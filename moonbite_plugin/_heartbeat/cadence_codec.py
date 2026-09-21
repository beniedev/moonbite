"""Pure Heartbeat cadence state decoding and serialisation.

This module owns no paths, locks, or persistence. The public cadence owner
continues to live in :mod:`moonbite_plugin.heartbeat` and delegates only the
schema transformations defined here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from ..runtime_core import StateError, isoformat, parse_time

CADENCE_SCHEMA_V1 = "moon.heartbeat.cadence.v1"
CADENCE_SCHEMA_V2 = "moon.heartbeat.cadence.v2"
CADENCE_SCHEMA_V3 = "moon.heartbeat.cadence.v3"
CADENCE_SCHEMA_V4 = "moon.heartbeat.cadence.v4"
HEARTBEAT_CADENCE_SCHEMA = CADENCE_SCHEMA_V4
CADENCE_SCHEMA = HEARTBEAT_CADENCE_SCHEMA

PRIVATE_CONTACT_MAX = 128
VISIBLE_CONTACT_MAX = 128
DAILY_ANCHOR_MAX = 128
EFFECT_TERMINAL_MAX = 256
EFFECT_REF_MAX = 256
SILENCE_BACKOFF_RECEIPT_MAX = 256
SILENCE_BACKOFF_RECEIPT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
HEARTBEAT_KIND_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


def aware(value: datetime, label: str = "timestamp") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    if value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value


def optional_time(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return aware(value, label)
    if type(value) is not str:
        raise ValueError(f"{label} must be an ISO timestamp or null")
    try:
        return parse_time(value)
    except (StateError, ValueError) as exc:
        raise ValueError(f"{label} must be an ISO timestamp") from exc


def strict_iso_date(value: Any, label: str) -> date:
    """Decode only the canonical ``YYYY-MM-DD`` representation."""

    if type(value) is not str or len(value) != 10:
        raise ValueError(f"{label} must be a strict ISO date")
    try:
        decoded = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a strict ISO date") from exc
    if decoded.isoformat() != value:
        raise ValueError(f"{label} must be a strict ISO date")
    return decoded


def daily_anchor_kind(value: Any, label: str = "daily anchor kind") -> str:
    if type(value) is not str or HEARTBEAT_KIND_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} has invalid syntax")
    return value


def json_time(value: datetime | None) -> str | None:
    return None if value is None else isoformat(value)


def decode_time(value: Any, label: str) -> str | None:
    return json_time(optional_time(value, label))


def compact_contacts(values: Mapping[str, str], limit: int) -> dict[str, str]:
    ordered = sorted(
        values.items(),
        key=lambda item: optional_time(item[1], "contact timestamp") or datetime.min,
        reverse=True,
    )
    return dict(ordered[:limit])


def compact_tail(values: Mapping[str, str], limit: int) -> dict[str, str]:
    return dict(list(values.items())[-limit:])


def empty_state() -> dict[str, Any]:
    return {
        "schema_version": CADENCE_SCHEMA_V4,
        "last_judge_at": None,
        "next_judge_at": None,
        "manual_cooldown_until": None,
        "automatic_cooldown_until": None,
        "last_effect_at": None,
        "daily_anchor_epoch": None,
        "daily_anchor_completed": False,
        "daily_anchor_epochs": {},
        "daily_anchor_legacy_epoch": None,
        "private_contacts": {},
        "verified_visible_contacts": {},
        "private_contact_overflow_until": None,
        "verified_visible_overflow_until": None,
        "effect_terminals": {},
        "effect_refs": {},
        "last_private_contact_at": None,
        "last_verified_visible_contact_at": None,
        "silence_backoff_processed_receipts": {},
        "silence_backoff_streak": 0,
        "silence_backoff_last_completed_at": None,
    }


def normalise_daily_anchor_state(
    raw: Mapping[str, Any],
) -> tuple[dict[str, str], str | None, bool]:
    """Decode exact-kind anchor evidence and the one-time legacy wildcard."""

    legacy_fields = {"daily_anchor_epoch", "daily_anchor_completed"}
    new_fields = {"daily_anchor_epochs", "daily_anchor_legacy_epoch"}
    fields = set(raw)
    schema = raw.get("schema_version")

    def normalise_epochs(value: Any) -> dict[str, str]:
        if not isinstance(value, Mapping):
            raise ValueError("heartbeat daily_anchor_epochs must be an object")
        if len(value) > DAILY_ANCHOR_MAX:
            raise ValueError("heartbeat daily_anchor_epochs exceeds its bound")
        selected: dict[str, str] = {}
        for kind, epoch in value.items():
            daily_anchor_kind(kind)
            strict_iso_date(epoch, f"daily_anchor_epochs.{kind}")
            selected[kind] = epoch
        return selected

    # Early per-kind hosts wrote an exact map under the v3 schema while keeping
    # the two old fields as a compatibility summary. Accept only the bounded,
    # internally consistent shape and let the next normal write persist v4.
    if schema == CADENCE_SCHEMA_V3 and "daily_anchor_epochs" in fields:
        if "daily_anchor_legacy_epoch" in fields:
            raise ValueError("heartbeat daily anchor state has a mixed schema")
        present_legacy = fields & legacy_fields
        if present_legacy not in (set(), legacy_fields):
            raise ValueError("heartbeat daily anchor transition is incomplete")
        selected = normalise_epochs(raw["daily_anchor_epochs"])
        if not present_legacy:
            return selected, None, False
        epoch = raw.get("daily_anchor_epoch")
        if epoch is not None:
            strict_iso_date(epoch, "heartbeat daily_anchor_epoch")
        completed = raw.get("daily_anchor_completed")
        if type(completed) is not bool:
            raise ValueError("heartbeat daily_anchor_completed is invalid")
        if completed != (epoch is not None):
            raise ValueError("heartbeat daily anchor summary is inconsistent")
        if selected:
            if not completed or epoch not in selected.values():
                raise ValueError("heartbeat daily anchor summary conflicts with map")
            return selected, None, False
        return selected, epoch, False

    if set(raw) & new_fields:
        if schema not in {None, CADENCE_SCHEMA_V4}:
            raise ValueError("heartbeat daily anchor state has a mixed schema")
        if set(raw) & legacy_fields:
            raise ValueError("heartbeat daily anchor state mixes schemas")
        selected = normalise_epochs(raw.get("daily_anchor_epochs", {}))
        legacy = raw.get("daily_anchor_legacy_epoch")
        if legacy is not None:
            strict_iso_date(legacy, "heartbeat daily_anchor_legacy_epoch")
        return selected, legacy, False

    if raw.get("schema_version") == CADENCE_SCHEMA_V4 and set(raw) & legacy_fields:
        raise ValueError("heartbeat daily anchor v4 requires per-kind state")
    epoch = raw.get("daily_anchor_epoch")
    if epoch is not None:
        strict_iso_date(epoch, "heartbeat daily_anchor_epoch")
        if "daily_anchor_completed" not in raw:
            raise ValueError("heartbeat daily_anchor_completed is required with epoch")
    completed = raw.get("daily_anchor_completed", False)
    if type(completed) is not bool:
        raise ValueError("heartbeat daily_anchor_completed is invalid")
    if completed and epoch is None:
        raise ValueError("heartbeat completed daily anchor requires an epoch")
    return {}, epoch if completed else None, True


def normalise_cadence_state(raw: Mapping[str, Any]) -> dict[str, Any]:
    if raw.get("schema_version") not in {
        None,
        CADENCE_SCHEMA_V1,
        CADENCE_SCHEMA_V2,
        CADENCE_SCHEMA_V3,
        CADENCE_SCHEMA_V4,
    }:
        raise StateError("heartbeat cadence state has an unsupported schema")
    allowed = {
        "schema_version",
        "auto_until",
        "manual_until",
        "automatic_cooldown_until",
        "manual_cooldown_until",
        "last_judge_at",
        "next_judge_at",
        "last_effect_at",
        "daily_anchor_epoch",
        "daily_anchor_completed",
        "daily_anchor_epochs",
        "daily_anchor_legacy_epoch",
        "private_contacts",
        "verified_visible_contacts",
        "private_contact_overflow_until",
        "verified_visible_overflow_until",
        "effect_terminals",
        "effect_refs",
        # These fields were written by an earlier v2 implementation. They are
        # accepted for safe migration, but never loaded or persisted; contact
        # dedupe is exact and window-bounded below.
        "private_contact_bloom",
        "verified_visible_bloom",
        "last_private_contact_at",
        "last_verified_visible_contact_at",
        "silence_backoff_processed_receipts",
        "silence_backoff_streak",
        "silence_backoff_last_completed_at",
    }
    if set(raw) - allowed:
        raise StateError("heartbeat cadence state has unsupported fields")
    state = empty_state()
    for key, aliases in {
        "last_judge_at": ("last_judge_at",),
        "next_judge_at": ("next_judge_at",),
        "last_effect_at": ("last_effect_at",),
        "automatic_cooldown_until": ("automatic_cooldown_until", "auto_until"),
        "manual_cooldown_until": ("manual_cooldown_until", "manual_until"),
        "private_contact_overflow_until": ("private_contact_overflow_until",),
        "verified_visible_overflow_until": ("verified_visible_overflow_until",),
        "last_private_contact_at": ("last_private_contact_at",),
        "last_verified_visible_contact_at": ("last_verified_visible_contact_at",),
        "silence_backoff_last_completed_at": ("silence_backoff_last_completed_at",),
    }.items():
        try:
            decoded = [
                decode_time(raw[alias], key) for alias in aliases if alias in raw
            ]
        except ValueError as exc:
            raise StateError(f"heartbeat cadence {key} has invalid timestamp") from exc
        if len(set(decoded)) > 1:
            raise StateError(f"heartbeat cadence {key} has conflicting timestamps")
        state[key] = decoded[0] if decoded else None
    try:
        anchor_epochs, legacy_epoch, _legacy_state = normalise_daily_anchor_state(raw)
    except ValueError as exc:
        raise StateError("heartbeat daily anchor state is invalid") from exc
    state["daily_anchor_epochs"] = anchor_epochs
    state["daily_anchor_legacy_epoch"] = legacy_epoch
    # Keep the old fields in the in-memory snapshot for direct callers. The
    # mapping and legacy epoch are the canonical v4 state.
    state["daily_anchor_epoch"] = anchor_epochs.get("daily_anchor") or legacy_epoch
    state["daily_anchor_completed"] = bool(
        anchor_epochs.get("daily_anchor") or legacy_epoch
    )
    for key in (
        "private_contacts",
        "verified_visible_contacts",
        "effect_terminals",
    ):
        value = raw.get(key, {})
        if not isinstance(value, Mapping):
            raise StateError(f"heartbeat {key} must be an object")
        copied: dict[str, Any] = {}
        for item_key, item_value in value.items():
            if type(item_key) is not str or not item_key.strip():
                raise StateError(f"heartbeat {key} has an invalid key")
            if key == "effect_terminals":
                if type(item_value) is not str or not item_value.strip():
                    raise StateError(f"heartbeat {key} has an invalid state")
                copied[item_key] = item_value
            else:
                try:
                    decoded = decode_time(item_value, f"{key}.{item_key}")
                    if decoded is None:
                        raise ValueError("contact timestamp must be non-null")
                    copied[item_key] = decoded
                except ValueError as exc:
                    raise StateError(
                        f"heartbeat {key} has an invalid timestamp"
                    ) from exc
        state[key] = copied
    refs = raw.get("effect_refs", {})
    if not isinstance(refs, Mapping):
        raise StateError("heartbeat effect_refs must be an object")
    state["effect_refs"] = {}
    for item_key, item_value in refs.items():
        if (
            type(item_key) is not str
            or not item_key.strip()
            or type(item_value) is not str
            or not item_value.strip()
        ):
            raise StateError("heartbeat effect_refs has an invalid entry")
        state["effect_refs"][item_key] = item_value
    state["private_contacts"] = compact_contacts(
        state["private_contacts"], PRIVATE_CONTACT_MAX
    )
    state["verified_visible_contacts"] = compact_contacts(
        state["verified_visible_contacts"], VISIBLE_CONTACT_MAX
    )
    state["effect_terminals"] = compact_tail(
        state["effect_terminals"], EFFECT_TERMINAL_MAX
    )
    state["effect_refs"] = compact_tail(state["effect_refs"], EFFECT_REF_MAX)
    for key in (
        "last_private_contact_at",
        "last_verified_visible_contact_at",
    ):
        values = list(
            state[
                "private_contacts"
                if key == "last_private_contact_at"
                else "verified_visible_contacts"
            ].values()
        )
        current = optional_time(state.get(key), key)
        mapped = max(
            (optional_time(value, key) for value in values),
            default=None,
        )
        if mapped is not None and (current is None or mapped > current):
            current = mapped
        state[key] = json_time(current)
    processed = raw.get("silence_backoff_processed_receipts", {})
    if not isinstance(processed, Mapping):
        raise StateError(
            "heartbeat silence_backoff_processed_receipts must be an object"
        )
    selected_processed: dict[str, str] = {}
    for receipt_id, status in processed.items():
        if (
            type(receipt_id) is not str
            or SILENCE_BACKOFF_RECEIPT_ID.fullmatch(receipt_id) is None
            or type(status) is not str
            or not status.strip()
            or len(status) > 64
        ):
            raise StateError(
                "heartbeat silence_backoff_processed_receipts has an invalid entry"
            )
        selected_processed[receipt_id] = status
    state["silence_backoff_processed_receipts"] = compact_tail(
        selected_processed, SILENCE_BACKOFF_RECEIPT_MAX
    )
    streak = raw.get("silence_backoff_streak", 0)
    if type(streak) is not int or not 0 <= streak <= SILENCE_BACKOFF_RECEIPT_MAX:
        raise StateError("heartbeat silence_backoff_streak is invalid")
    state["silence_backoff_streak"] = streak
    return state


def serialise_cadence_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": CADENCE_SCHEMA_V4,
        "last_judge_at": state.get("last_judge_at"),
        "next_judge_at": state.get("next_judge_at"),
        "manual_cooldown_until": state.get("manual_cooldown_until"),
        "automatic_cooldown_until": state.get("automatic_cooldown_until"),
        "manual_until": state.get("manual_cooldown_until"),
        "auto_until": state.get("automatic_cooldown_until"),
        "last_effect_at": state.get("last_effect_at"),
        "daily_anchor_epochs": dict(state.get("daily_anchor_epochs", {})),
        "daily_anchor_legacy_epoch": state.get("daily_anchor_legacy_epoch"),
        "private_contacts": dict(state.get("private_contacts", {})),
        "verified_visible_contacts": dict(state.get("verified_visible_contacts", {})),
        "private_contact_overflow_until": state.get("private_contact_overflow_until"),
        "verified_visible_overflow_until": state.get("verified_visible_overflow_until"),
        "effect_terminals": compact_tail(
            state.get("effect_terminals", {}), EFFECT_TERMINAL_MAX
        ),
        "effect_refs": compact_tail(state.get("effect_refs", {}), EFFECT_REF_MAX),
        "last_private_contact_at": state.get("last_private_contact_at"),
        "last_verified_visible_contact_at": state.get(
            "last_verified_visible_contact_at"
        ),
        "silence_backoff_processed_receipts": compact_tail(
            state.get("silence_backoff_processed_receipts", {}),
            SILENCE_BACKOFF_RECEIPT_MAX,
        ),
        "silence_backoff_streak": int(state.get("silence_backoff_streak", 0)),
        "silence_backoff_last_completed_at": state.get(
            "silence_backoff_last_completed_at"
        ),
    }
