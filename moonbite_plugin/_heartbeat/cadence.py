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

from ..runtime_core import file_lock, isoformat
from .cadence_codec import (
    SILENCE_BACKOFF_RECEIPT_MAX,
    compact_tail,
    optional_time,
)


class _CadenceOwner(Protocol):
    path: Path
    lock_path: Path

    def _load(self) -> dict[str, Any]: ...

    def _save(self, state: Mapping[str, Any]) -> None: ...

    def _now(self, value: datetime | None = None) -> datetime: ...

    def _prune_contact_state(self, state: dict[str, Any], now: datetime) -> bool: ...

    def _clear_automatic_backoff(self, state: dict[str, Any]) -> None: ...


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
