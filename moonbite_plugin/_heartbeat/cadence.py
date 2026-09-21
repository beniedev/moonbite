"""Heartbeat cadence transitions using the existing public state owner.

The functions in this module receive an already constructed cadence owner.
They do not create paths, locks, or a second store; persistence and ownership
remain with :class:`moonbite_plugin.heartbeat.HeartbeatCadence`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from ..runtime_core import StateError, file_lock, isoformat
from .cadence_codec import (
    DAILY_ANCHOR_MAX,
    PRIVATE_CONTACT_MAX,
    SILENCE_BACKOFF_RECEIPT_MAX,
    VISIBLE_CONTACT_MAX,
    aware,
    compact_contacts,
    compact_tail,
    daily_anchor_kind,
    optional_time,
    strict_iso_date,
)


class _CadenceOwner(Protocol):
    path: Path
    lock_path: Path
    timezone: Any
    anchor_hour: int
    judge_interval: timedelta
    recent_contact_window: timedelta

    def _load(self) -> dict[str, Any]: ...

    def _save(self, state: Mapping[str, Any]) -> None: ...

    def _now(self, value: datetime | None = None) -> datetime: ...

    def _prune_contact_state(self, state: dict[str, Any], now: datetime) -> bool: ...

    def _clear_automatic_backoff(self, state: dict[str, Any]) -> None: ...

    def _prune_contact_state(self, state: dict[str, Any], now: datetime) -> bool: ...

    def _recent_from_state(
        self,
        state: Mapping[str, Any],
        now: datetime,
        *,
        include_verified_visible: bool = True,
    ) -> tuple[str | None, datetime | None]: ...

    def daily_anchor_epoch(self, now: datetime | None = None) -> str: ...


def prune_contact_state(
    owner: _CadenceOwner,
    state: dict[str, Any],
    now: datetime,
) -> bool:
    """Keep only exact source ids that can still affect the recent gate.

    The maps are intentionally bounded, but they never evict an id that is
    still inside the contact window merely to make room for another id. A
    single expiry marker represents the conservative overflow case until the
    window clears; this avoids lifetime probabilistic dedupe.
    """

    changed = False
    cutoff = now - owner.recent_contact_window
    for contacts_key, overflow_key, limit in (
        (
            "private_contacts",
            "private_contact_overflow_until",
            PRIVATE_CONTACT_MAX,
        ),
        (
            "verified_visible_contacts",
            "verified_visible_overflow_until",
            VISIBLE_CONTACT_MAX,
        ),
    ):
        contacts = state[contacts_key]
        kept: dict[str, str] = {}
        for key, raw in contacts.items():
            observed = optional_time(raw, f"{contacts_key}.{key}")
            if observed is not None and observed > cutoff:
                kept[key] = raw
        if len(kept) > limit:
            kept = compact_contacts(kept, limit)
        if kept != contacts:
            state[contacts_key] = kept
            changed = True
        overflow = optional_time(state.get(overflow_key), overflow_key)
        if overflow is not None and overflow <= now:
            state[overflow_key] = None
            changed = True
    return changed


def recent_from_state(
    owner: _CadenceOwner,
    state: Mapping[str, Any],
    now: datetime,
    *,
    include_verified_visible: bool = True,
) -> tuple[str | None, datetime | None]:
    items: list[tuple[str, datetime]] = []
    contact_sources = [("private_contacts", "recent_private_inbound")]
    overflow_sources = [("private_contact_overflow_until", "recent_private_inbound")]
    if include_verified_visible:
        contact_sources.append(
            ("verified_visible_contacts", "recent_verified_visible_contact")
        )
        overflow_sources.append(
            (
                "verified_visible_overflow_until",
                "recent_verified_visible_contact",
            )
        )
    for contacts_key, label in contact_sources:
        for raw in state[contacts_key].values():
            parsed = optional_time(raw, contacts_key)
            if parsed is not None and parsed + owner.recent_contact_window > now:
                items.append((label, parsed))
    for overflow_key, label in overflow_sources:
        expiry = optional_time(state.get(overflow_key), overflow_key)
        if expiry is not None and expiry > now:
            items.append((label, expiry - owner.recent_contact_window))
    if not items:
        return None, None
    return max(items, key=lambda item: item[1])


def snooze(owner: _CadenceOwner, minutes: int, *, manual: bool) -> datetime:
    if type(minutes) is not int or not 1 <= minutes <= 1440:
        raise ValueError("snooze minutes must be from 1 to 1440")
    until = owner._now() + timedelta(minutes=minutes)
    with file_lock(owner.lock_path):
        state = owner._load()
        state["manual_cooldown_until" if manual else "automatic_cooldown_until"] = (
            isoformat(until)
        )
        owner._save(state)
    return until


def resume(owner: _CadenceOwner) -> None:
    with file_lock(owner.lock_path):
        state = owner._load()
        state["manual_cooldown_until"] = state["manual_until"] = None
        state["automatic_cooldown_until"] = state["auto_until"] = None
        state["silence_backoff_streak"] = 0
        state["silence_backoff_last_completed_at"] = None
        owner._save(state)


def observe_private_reply(
    owner: _CadenceOwner,
    observed_at: datetime | None = None,
) -> None:
    observed = owner._now(observed_at)
    with file_lock(owner.lock_path):
        state = owner._load()
        previous = optional_time(
            state.get("last_private_contact_at"), "last_private_contact_at"
        )
        if previous is None or observed > previous:
            state["last_private_contact_at"] = isoformat(observed)
        owner._clear_automatic_backoff(state)
        owner._save(state)


def apply_silence_backoff(
    owner: _CadenceOwner,
    receipt: Any,
    *,
    receipt_type: type[Any],
    normalise_policy: Callable[[Mapping[str, Any]], dict[str, Any]],
    policy: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Atomically dedupe a settled silence and update cadence cooldown."""

    selected_policy = normalise_policy(policy)
    if not selected_policy["enabled"]:
        return {
            "status": "disabled",
            "applied": False,
            "processed": False,
            "streak": 0,
            "cooldown_until": None,
        }
    if not isinstance(receipt, receipt_type):
        raise TypeError("silence backoff requires HeartbeatSilenceReceipt")
    effective_now = owner._now(now)
    with file_lock(owner.lock_path):
        state = owner._load()
        changed = owner._prune_contact_state(state, effective_now)
        processed = dict(state["silence_backoff_processed_receipts"])
        if receipt.receipt_id in processed:
            if changed:
                owner._save(state)
            return {
                "status": "duplicate",
                "applied": False,
                "processed": True,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        if not receipt.settled:
            if changed:
                owner._save(state)
            return {
                "status": "pending_settlement",
                "applied": False,
                "processed": False,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        if "unknown" in {
            receipt.judge_terminal,
            receipt.wake_terminal,
            receipt.delivery_terminal,
        }:
            if changed:
                owner._save(state)
            return {
                "status": "pending_terminal",
                "applied": False,
                "processed": False,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        if receipt.completed_at > effective_now:
            if changed:
                owner._save(state)
            return {
                "status": "future_receipt",
                "applied": False,
                "processed": False,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        contact_watermarks = tuple(
            value
            for value in (
                optional_time(
                    state.get("last_private_contact_at"),
                    "last_private_contact_at",
                ),
                optional_time(
                    state.get("last_verified_visible_contact_at"),
                    "last_verified_visible_contact_at",
                ),
            )
            if value is not None
        )
        if contact_watermarks and receipt.completed_at <= max(contact_watermarks):
            processed[receipt.receipt_id] = "contact_after_receipt"
            state["silence_backoff_processed_receipts"] = compact_tail(
                processed, SILENCE_BACKOFF_RECEIPT_MAX
            )
            owner._save(state)
            return {
                "status": "contact_after_receipt",
                "applied": False,
                "processed": True,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        if not (
            receipt.profile == "routine"
            and receipt.intentional_silence
            and receipt.judge_terminal == "approved"
            and receipt.wake_terminal == "verified"
            and receipt.delivery_terminal == "not_requested"
            and not receipt.manual_override
        ):
            processed[receipt.receipt_id] = "ineligible"
            state["silence_backoff_processed_receipts"] = compact_tail(
                processed, SILENCE_BACKOFF_RECEIPT_MAX
            )
            owner._save(state)
            return {
                "status": "ineligible",
                "applied": False,
                "processed": True,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        previous_completed = optional_time(
            state.get("silence_backoff_last_completed_at"),
            "silence_backoff_last_completed_at",
        )
        if (
            previous_completed is not None
            and receipt.completed_at <= previous_completed
        ):
            processed[receipt.receipt_id] = "out_of_order"
            state["silence_backoff_processed_receipts"] = compact_tail(
                processed, SILENCE_BACKOFF_RECEIPT_MAX
            )
            owner._save(state)
            return {
                "status": "out_of_order",
                "applied": False,
                "processed": True,
                "streak": state["silence_backoff_streak"],
                "cooldown_until": state["automatic_cooldown_until"],
            }
        streak = min(
            state["silence_backoff_streak"] + 1,
            SILENCE_BACKOFF_RECEIPT_MAX,
        )
        duration = (
            selected_policy["first_minutes"]
            if streak == 1
            else selected_policy["repeat_minutes"]
        )
        duration = min(duration, selected_policy["max_minutes"])
        expiry = receipt.completed_at + timedelta(minutes=duration)
        current_until = optional_time(
            state.get("automatic_cooldown_until"),
            "automatic_cooldown_until",
        )
        if current_until is None or expiry > current_until:
            state["automatic_cooldown_until"] = state["auto_until"] = (
                isoformat(expiry) if expiry > effective_now else None
            )
        processed[receipt.receipt_id] = (
            "applied" if expiry > effective_now else "expired"
        )
        state["silence_backoff_processed_receipts"] = compact_tail(
            processed, SILENCE_BACKOFF_RECEIPT_MAX
        )
        state["silence_backoff_streak"] = streak
        state["silence_backoff_last_completed_at"] = isoformat(receipt.completed_at)
        owner._save(state)
        return {
            "status": "applied" if expiry > effective_now else "expired",
            "applied": expiry > effective_now,
            "processed": True,
            "streak": streak,
            "cooldown_until": state["automatic_cooldown_until"],
            "duration_minutes": duration,
        }


def cooldown(
    owner: _CadenceOwner,
    kind: str,
    *,
    supported_bypasses: frozenset[str],
    now: datetime | None = None,
    bypass: Iterable[str] | None = None,
) -> tuple[bool, str, datetime | None]:
    selected_bypass = frozenset() if bypass is None else frozenset(bypass)
    if not selected_bypass <= supported_bypasses:
        raise ValueError("heartbeat cooldown bypass is unsupported")
    effective_now = owner._now(now)
    if not owner.path.exists():
        return False, "open", None
    with file_lock(owner.lock_path):
        state = owner._load()
    for key, label, bypass_name in (
        ("manual_cooldown_until", "manual_snooze", "manual_snooze"),
        (
            "automatic_cooldown_until",
            "automatic_cadence",
            "automatic_cooldown",
        ),
    ):
        if bypass_name in selected_bypass:
            continue
        until = optional_time(state.get(key), key)
        if until is not None and until > effective_now:
            return True, label, until
    return False, "open", None


def anchor_epoch(owner: _CadenceOwner, now: datetime) -> str:
    local = aware(now).astimezone(owner.timezone)
    date = local.date()
    if local.hour < owner.anchor_hour:
        date -= timedelta(days=1)
    return date.isoformat()


def daily_anchor_due(
    owner: _CadenceOwner,
    now: datetime | None = None,
    *,
    kind: str = "daily_anchor",
) -> bool:
    selected_kind = daily_anchor_kind(kind)
    epoch = owner.daily_anchor_epoch(now)
    if not owner.path.exists():
        return True
    with file_lock(owner.lock_path):
        state = owner._load()
    if state["daily_anchor_legacy_epoch"] == epoch:
        return False
    return state["daily_anchor_epochs"].get(selected_kind) != epoch


def mark_daily_anchor(
    owner: _CadenceOwner,
    epoch: str | None = None,
    *,
    kind: str = "daily_anchor",
    now: datetime | None = None,
) -> str:
    selected_kind = daily_anchor_kind(kind)
    selected = epoch if epoch is not None else owner.daily_anchor_epoch(now)
    strict_iso_date(selected, "daily anchor epoch")
    with file_lock(owner.lock_path):
        state = owner._load()
        epochs = dict(state["daily_anchor_epochs"])
        if selected_kind not in epochs and len(epochs) >= DAILY_ANCHOR_MAX:
            raise StateError("heartbeat daily_anchor_epochs exceeds its bound")
        epochs[selected_kind] = selected
        state["daily_anchor_epochs"] = epochs
        state["daily_anchor_epoch"] = (
            epochs.get("daily_anchor") or state["daily_anchor_legacy_epoch"]
        )
        state["daily_anchor_completed"] = bool(state["daily_anchor_epoch"])
        owner._save(state)
    return selected


def next_judge_at(
    owner: _CadenceOwner,
    now: datetime | None = None,
) -> datetime:
    effective_now = owner._now(now)
    if not owner.path.exists():
        return effective_now
    with file_lock(owner.lock_path):
        state = owner._load()
    value = optional_time(state["next_judge_at"], "next_judge_at")
    if value is not None:
        return value
    last = optional_time(state["last_judge_at"], "last_judge_at")
    return effective_now if last is None else last + owner.judge_interval


def mark_judge(
    owner: _CadenceOwner,
    *,
    now: datetime | None = None,
    next_judge_at: datetime | str | None = None,
    cadence_minutes: int | None = None,
    anchor_epoch: str | None = None,
    anchor_kind: str | None = None,
) -> datetime:
    effective_now = owner._now(now)
    if anchor_kind is not None and anchor_epoch is None:
        raise ValueError("anchor_kind requires anchor_epoch")
    if anchor_epoch is not None:
        strict_iso_date(anchor_epoch, "anchor_epoch")
    selected_anchor_kind = (
        daily_anchor_kind(anchor_kind) if anchor_kind is not None else "daily_anchor"
    )
    if next_judge_at is not None:
        selected = optional_time(next_judge_at, "next_judge_at")
        assert selected is not None
        if selected <= effective_now:
            raise ValueError("next_judge_at must be later than now")
    elif cadence_minutes is not None:
        if type(cadence_minutes) is not int or not 1 <= cadence_minutes <= 10080:
            raise ValueError("cadence_minutes is out of bounds")
        selected = effective_now + timedelta(minutes=cadence_minutes)
    elif anchor_epoch is not None:
        local = effective_now.astimezone(owner.timezone)
        next_date = local.date() + timedelta(days=1)
        selected = datetime(
            next_date.year,
            next_date.month,
            next_date.day,
            owner.anchor_hour,
            tzinfo=owner.timezone,
        ).astimezone(effective_now.tzinfo)
    else:
        selected = effective_now + owner.judge_interval
    with file_lock(owner.lock_path):
        state = owner._load()
        state["last_judge_at"] = isoformat(effective_now)
        state["next_judge_at"] = isoformat(selected)
        if anchor_epoch is not None:
            epochs = dict(state["daily_anchor_epochs"])
            if selected_anchor_kind not in epochs and len(epochs) >= DAILY_ANCHOR_MAX:
                raise StateError("heartbeat daily_anchor_epochs exceeds its bound")
            epochs[selected_anchor_kind] = anchor_epoch
            state["daily_anchor_epochs"] = epochs
            state["daily_anchor_epoch"] = (
                epochs.get("daily_anchor") or state["daily_anchor_legacy_epoch"]
            )
            state["daily_anchor_completed"] = bool(state["daily_anchor_epoch"])
        owner._save(state)
    return selected


def record_private_contact(
    owner: _CadenceOwner,
    receipt: Any = None,
    *,
    receipt_type: type[Any],
    source_id: str | None = None,
    observed_at: datetime | None = None,
    fresh: bool = True,
    source_kind: str = "private_inbound",
) -> bool:
    if receipt is not None:
        if not isinstance(receipt, receipt_type):
            raise TypeError("receipt must be a SessionHookReceipt")
        context = receipt.context
        if not context.counts_as_private_contact:
            return False
        source_id, observed_at, fresh, source_kind = (
            context.source_id,
            context.observed_at,
            context.fresh,
            context.source_kind,
        )
    if (
        type(source_id) is not str
        or not source_id.strip()
        or type(fresh) is not bool
        or not fresh
        or source_kind != "private_inbound"
    ):
        return False
    effective_now = owner._now()
    observed = aware(effective_now if observed_at is None else observed_at)
    if observed > effective_now:
        return False
    with file_lock(owner.lock_path):
        state = owner._load()
        changed = owner._prune_contact_state(state, effective_now)
        contacts = dict(state["private_contacts"])
        if source_id in contacts:
            if changed:
                owner._save(state)
            return False
        watermark = optional_time(
            state.get("last_private_contact_at"), "last_private_contact_at"
        )
        if watermark is None or observed > watermark:
            state["last_private_contact_at"] = isoformat(observed)
        if observed + owner.recent_contact_window <= effective_now:
            owner._clear_automatic_backoff(state)
            owner._save(state)
            return False
        if len(contacts) >= PRIVATE_CONTACT_MAX:
            expiry = observed + owner.recent_contact_window
            current_expiry = optional_time(
                state.get("private_contact_overflow_until"),
                "private_contact_overflow_until",
            )
            if current_expiry is not None and current_expiry > expiry:
                expiry = current_expiry
            state["private_contact_overflow_until"] = isoformat(expiry)
            owner._clear_automatic_backoff(state)
            owner._save(state)
            return True
        contacts[source_id] = isoformat(observed)
        state["private_contacts"] = compact_contacts(contacts, PRIVATE_CONTACT_MAX)
        owner._clear_automatic_backoff(state)
        owner._save(state)
    return True


def record_verified_visible_contact(
    owner: _CadenceOwner,
    record: Any,
    receipt: Any = None,
    *,
    record_type: type[Any],
    receipt_type: type[Any],
) -> bool:
    """Project only a verified heartbeat delivery into contact state."""

    if not isinstance(record, record_type):
        raise TypeError("verified visible contact requires EffectRecord")
    if (
        record.kind != "heartbeat_delivery"
        or record.state != "verified"
        or not record.verified
        or not isinstance(record.receipt, receipt_type)
    ):
        return False
    selected = record.receipt if receipt is None else receipt
    if not isinstance(selected, receipt_type):
        raise TypeError("verified visible contact requires EffectReceipt")
    if selected != record.receipt or (
        selected.event_id != record.source_event_id
        or selected.content_sha256 != record.content_sha256
        or selected.content_length != record.content_length
        or selected.epoch_id != record.epoch_id
    ):
        return False
    key = record.effect_id
    observed = aware(selected.observed_at, "receipt observed_at")
    effective_now = owner._now()
    if observed > effective_now:
        return False
    with file_lock(owner.lock_path):
        state = owner._load()
        changed = owner._prune_contact_state(state, effective_now)
        contacts = dict(state["verified_visible_contacts"])
        if key in contacts:
            if changed:
                owner._save(state)
            return False
        watermark = optional_time(
            state.get("last_verified_visible_contact_at"),
            "last_verified_visible_contact_at",
        )
        watermark_changed = watermark is None or observed > watermark
        if watermark_changed:
            state["last_verified_visible_contact_at"] = isoformat(observed)
        if observed + owner.recent_contact_window <= effective_now:
            owner._clear_automatic_backoff(state)
            owner._save(state)
            return False
        if len(contacts) >= VISIBLE_CONTACT_MAX:
            expiry = observed + owner.recent_contact_window
            current_expiry = optional_time(
                state.get("verified_visible_overflow_until"),
                "verified_visible_overflow_until",
            )
            if current_expiry is not None and current_expiry > expiry:
                expiry = current_expiry
            state["verified_visible_overflow_until"] = isoformat(expiry)
            owner._clear_automatic_backoff(state)
            owner._save(state)
            return True
        contacts[key] = isoformat(observed)
        state["verified_visible_contacts"] = compact_contacts(
            contacts, VISIBLE_CONTACT_MAX
        )
        owner._clear_automatic_backoff(state)
        owner._save(state)
    return True


def recent_contact(
    owner: _CadenceOwner,
    *,
    now: datetime | None = None,
) -> tuple[str | None, datetime | None]:
    effective_now = owner._now(now)
    if not owner.path.exists():
        return None, None
    with file_lock(owner.lock_path):
        state = owner._load()
        owner._prune_contact_state(state, effective_now)
        return owner._recent_from_state(state, effective_now)


def recent_private_inbound(
    owner: _CadenceOwner,
    *,
    now: datetime | None = None,
) -> tuple[str | None, datetime | None]:
    """Return only recent user-originated private contact.

    Verified outbound delivery remains part of ``recent_contact`` so a
    heartbeat can avoid repeated messages, but it is not user-presence
    evidence for autonomy admission.
    """

    effective_now = owner._now(now)
    if not owner.path.exists():
        return None, None
    with file_lock(owner.lock_path):
        state = owner._load()
        owner._prune_contact_state(state, effective_now)
        return owner._recent_from_state(
            state,
            effective_now,
            include_verified_visible=False,
        )
