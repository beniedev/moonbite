"""Durable runtime controls shared by Heartbeat and Autonomy."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .runtime_core import (
    JsonlLedger,
    StateError,
    ensure_bounded_json,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)

CONTROL_SCHEMA = "moon.runtime_control.v1"
CANONICAL_FEATURES = frozenset({"heartbeat", "autonomy", "proactive"})
FEATURE_ALIASES = {"background_costly": "proactive"}
FEATURES = CANONICAL_FEATURES | frozenset(FEATURE_ALIASES)
MODES = frozenset({"pause", "rest", "play_next", "quota_save"})
SOURCE_PRIORITY = {
    "scheduler": 10,
    "self": 20,
    "static": 30,
    "operator": 40,
}
_CONTROL_FIELDS = frozenset(
    {"control_id", "feature", "mode", "source", "created_at", "expires_at", "payload"}
)
_CONTROL_ENVELOPE_FIELDS = frozenset(
    {"schema_version", "event_id", "created_at", "action"}
)
_ACTION_FIELDS = {
    "put": frozenset({"control"}),
    "clear": frozenset({"feature", "source"}),
    "consume": frozenset({"control_id"}),
    "expire": frozenset({"control_ids"}),
}


def _nonempty_text(value: Any, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise StateError(f"{label} must be a non-empty string")
    return value


def canonical_feature(feature: str) -> str:
    if feature not in FEATURES:
        raise ValueError(f"unknown controlled feature: {feature}")
    return FEATURE_ALIASES.get(feature, feature)


def _aware_time(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.strip():
        raise StateError(f"{label} must be a timezone-aware timestamp")
    try:
        parsed = parse_time(value)
    except (StateError, TypeError, ValueError) as exc:
        raise StateError(f"{label} must be a timezone-aware timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StateError(f"{label} must be a timezone-aware timestamp")
    return parsed


def _observer_now(value: Any) -> datetime:
    """Validate a caller-supplied observer timestamp without side effects."""

    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception as exc:  # pragma: no cover - hostile tzinfo implementation
        raise ValueError("now must be timezone-aware") from exc
    if offset is None or not isinstance(offset, timedelta):
        raise ValueError("now must be timezone-aware")
    return value


def _lock_free_control_rows(path: Path) -> list[dict[str, Any]]:
    """Read controls JSONL without creating or acquiring its ledger lock."""

    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for index, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise StateError(
                    f"controls.jsonl row {index} contains invalid JSON"
                ) from exc
            if not isinstance(value, dict):
                raise StateError(f"controls.jsonl row {index} must contain an object")
            rows.append(value)
    return rows


@dataclass(frozen=True)
class ControlIntent:
    control_id: str
    feature: str
    mode: str
    source: str
    created_at: datetime
    expires_at: datetime | None = None
    payload: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "feature", canonical_feature(self.feature))
        if self.mode not in MODES:
            raise ValueError(f"unknown control mode: {self.mode}")
        if self.source not in SOURCE_PRIORITY:
            raise ValueError(f"unknown control source: {self.source}")
        if not self.control_id.strip():
            raise ValueError("control_id must be non-empty")
        if self.expires_at is not None and self.expires_at <= self.created_at:
            raise ValueError("expires_at must be later than created_at")
        if self.source == "self":
            if self.expires_at is None:
                raise ValueError("self controls require expires_at")
            if self.expires_at - self.created_at > timedelta(hours=24):
                raise ValueError("self controls may last at most 24 hours")
        ensure_bounded_json(
            {} if self.payload is None else dict(self.payload),
            "control payload",
            max_bytes=64 * 1024,
        )

    @property
    def priority(self) -> int:
        return SOURCE_PRIORITY[self.source]

    def to_dict(self) -> dict[str, Any]:
        return {
            "control_id": self.control_id,
            "feature": self.feature,
            "mode": self.mode,
            "source": self.source,
            "created_at": isoformat(self.created_at),
            "expires_at": None
            if self.expires_at is None
            else isoformat(self.expires_at),
            "payload": {} if self.payload is None else dict(self.payload),
        }

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any], *, redact_payload: bool = False
    ) -> ControlIntent:
        if not isinstance(value, Mapping) or set(value) != _CONTROL_FIELDS:
            raise StateError("control has invalid fields")
        control_id = _nonempty_text(value["control_id"], "control_id")
        feature = _nonempty_text(value["feature"], "control feature")
        mode = _nonempty_text(value["mode"], "control mode")
        source = _nonempty_text(value["source"], "control source")
        try:
            feature = canonical_feature(feature)
        except ValueError as exc:
            raise StateError(f"unknown controlled feature: {feature}") from exc
        if mode not in MODES:
            raise StateError(f"unknown control mode: {mode}")
        if source not in SOURCE_PRIORITY:
            raise StateError(f"unknown control source: {source}")
        created_at = _aware_time(value["created_at"], "control created_at")
        expires_at = (
            None
            if value["expires_at"] is None
            else _aware_time(value["expires_at"], "control expires_at")
        )
        payload = value["payload"]
        if not isinstance(payload, Mapping):
            raise StateError("control payload must be a mapping")
        try:
            return cls(
                control_id=control_id,
                feature=feature,
                mode=mode,
                source=source,
                created_at=created_at,
                expires_at=expires_at,
                # Observer replay deliberately does not copy or traverse the
                # untrusted payload; the metadata object must not retain it.
                payload={} if redact_payload else dict(payload),
            )
        except (TypeError, ValueError, StateError) as exc:
            raise StateError("control is invalid") from exc


@dataclass(frozen=True)
class ControlResolution:
    feature: str
    intent: ControlIntent | None

    @property
    def mode(self) -> str:
        return "none" if self.intent is None else self.intent.mode


@dataclass(frozen=True)
class GateResult:
    allowed: bool
    mode: str
    reason: str
    control_id: str | None = None


class ControlStore:
    """Append-only control ledger; the active view is rebuilt on every read."""

    def __init__(self, root: Path, *, clock: Callable[[], datetime] = utc_now):
        self.ledger = JsonlLedger(root / "controls.jsonl")
        self.clock = clock

    def _append(self, action: str, **payload: Any) -> None:
        self.ledger.append(
            {
                "schema_version": CONTROL_SCHEMA,
                "event_id": new_id("control_event"),
                "created_at": isoformat(self.clock()),
                "action": action,
                **payload,
            }
        )

    def put(
        self,
        *,
        feature: str,
        mode: str,
        source: str,
        expires_at: datetime | None = None,
        payload: Mapping[str, Any] | None = None,
        control_id: str | None = None,
    ) -> ControlIntent:
        intent = ControlIntent(
            control_id=control_id or new_id("control"),
            feature=feature,
            mode=mode,
            source=source,
            created_at=self.clock(),
            expires_at=expires_at,
            payload={} if payload is None else dict(payload),
        )
        self._append("put", control=intent.to_dict())
        return intent

    def clear(self, *, feature: str, source: str) -> None:
        if source not in SOURCE_PRIORITY:
            raise ValueError("clear requires a known feature and source")
        try:
            feature = canonical_feature(feature)
        except ValueError as exc:
            raise ValueError("clear requires a known feature and source") from exc
        self._append("clear", feature=feature, source=source)

    def consume(self, control_id: str) -> None:
        if not control_id.strip():
            raise ValueError("control_id must be non-empty")
        self._append("consume", control_id=control_id)

    def expire(self, *, now: datetime | None = None) -> list[str]:
        effective_now = self.clock() if now is None else now
        expired = sorted(
            control.control_id
            for control in self.active(now=effective_now, include_expired=True)
            if control.expires_at is not None and control.expires_at <= effective_now
        )
        if expired:
            self._append("expire", control_ids=expired)
        return expired

    @staticmethod
    def _replay_rows(
        rows: list[Mapping[str, Any]], *, redact_payload: bool = False
    ) -> dict[str, ControlIntent]:
        active: dict[str, ControlIntent] = {}
        event_ids: set[str] = set()
        for index, row in enumerate(rows, start=1):
            if not isinstance(row, Mapping):
                raise StateError(f"controls.jsonl row {index} is invalid")
            if row.get("schema_version") != CONTROL_SCHEMA:
                raise StateError(f"controls.jsonl row {index} has an unknown schema")
            action = row.get("action")
            if type(action) is not str or action not in _ACTION_FIELDS:
                raise StateError(
                    f"controls.jsonl row {index} has unknown action {action!r}"
                )
            expected_fields = _CONTROL_ENVELOPE_FIELDS | _ACTION_FIELDS[action]
            if set(row) != expected_fields:
                raise StateError(f"controls.jsonl row {index} is invalid")
            event_id = _nonempty_text(row["event_id"], "event_id")
            if event_id in event_ids:
                raise StateError(f"controls.jsonl row {index} has duplicate event_id")
            event_ids.add(event_id)
            _aware_time(row["created_at"], "created_at")

            if action == "put":
                raw = row.get("control")
                if not isinstance(raw, Mapping):
                    raise StateError(f"controls.jsonl row {index} is missing control")
                try:
                    intent = ControlIntent.from_dict(raw, redact_payload=redact_payload)
                except (KeyError, TypeError, ValueError, StateError) as exc:
                    raise StateError(f"controls.jsonl row {index} is invalid") from exc
                active = {
                    key: value
                    for key, value in active.items()
                    if not (
                        value.feature == intent.feature
                        and value.source == intent.source
                    )
                }
                active[intent.control_id] = intent
            elif action == "clear":
                feature = _nonempty_text(row["feature"], "clear feature")
                source = _nonempty_text(row["source"], "clear source")
                try:
                    feature = canonical_feature(feature)
                except ValueError as exc:
                    raise StateError(
                        f"controls.jsonl row {index} has invalid clear"
                    ) from exc
                if source not in SOURCE_PRIORITY:
                    raise StateError(f"controls.jsonl row {index} has invalid clear")
                active = {
                    key: value
                    for key, value in active.items()
                    if not (value.feature == feature and value.source == source)
                }
            elif action == "consume":
                active.pop(_nonempty_text(row["control_id"], "control_id"), None)
            elif action == "expire":
                ids = row.get("control_ids")
                if type(ids) is not list:
                    raise StateError(f"controls.jsonl row {index} has invalid expiry")
                control_ids: set[str] = set()
                for control_id in ids:
                    value = _nonempty_text(control_id, "control_id")
                    if value in control_ids:
                        raise StateError(
                            f"controls.jsonl row {index} has duplicate expiry"
                        )
                    control_ids.add(value)
                    active.pop(value, None)
        return active

    def _replay(self) -> dict[str, ControlIntent]:
        return self._replay_rows(self.ledger.rows())

    def observer_active(self, *, now: datetime) -> list[ControlIntent]:
        """Read active controls without creating or acquiring a lock.

        Unlike :meth:`expire`, this port never appends a terminal event.  A
        missing ledger is the pristine neutral state and returns an empty
        list; an existing ledger is strictly replayed and malformed history
        remains an error rather than becoming an apparently healthy result.
        """

        effective_now = _observer_now(now)
        if not self.ledger.path.exists():
            return []
        controls = list(
            self._replay_rows(
                _lock_free_control_rows(self.ledger.path), redact_payload=True
            ).values()
        )
        controls = [
            item
            for item in controls
            if item.expires_at is None or item.expires_at > effective_now
        ]
        return sorted(
            controls,
            key=lambda item: (
                item.feature,
                item.priority,
                item.created_at,
                item.control_id,
            ),
        )

    def active(
        self, *, now: datetime | None = None, include_expired: bool = False
    ) -> list[ControlIntent]:
        effective_now = self.clock() if now is None else now
        controls = list(self._replay().values())
        if not include_expired:
            controls = [
                item
                for item in controls
                if item.expires_at is None or item.expires_at > effective_now
            ]
        return sorted(
            controls,
            key=lambda item: (
                item.feature,
                item.priority,
                item.created_at,
                item.control_id,
            ),
        )

    def resolve(
        self, feature: str, *, now: datetime | None = None
    ) -> ControlResolution:
        feature = canonical_feature(feature)
        active = self.active(now=now)
        candidates = [item for item in active if item.feature == feature]
        if feature in {"heartbeat", "autonomy"}:
            candidates.extend(item for item in active if item.feature == "proactive")
        winner = max(
            candidates,
            key=lambda item: (
                item.priority,
                item.feature == "proactive",
                item.created_at,
                item.control_id,
            ),
            default=None,
        )
        return ControlResolution(feature, winner)


def evaluate_gate(resolution: ControlResolution) -> GateResult:
    intent = resolution.intent
    if intent is None:
        return GateResult(True, "none", "no_control")
    if intent.mode in {"pause", "rest", "quota_save"}:
        return GateResult(
            False,
            intent.mode,
            f"controlled_by:{intent.source}",
            intent.control_id,
        )
    if intent.mode == "play_next":
        return GateResult(True, intent.mode, "one_shot_requested", intent.control_id)
    return GateResult(
        False,
        intent.mode,
        f"unknown_mode_fail_closed:{intent.mode}",
        intent.control_id,
    )
