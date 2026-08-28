"""Typed owner-scoped daily-RAM panel.

The panel is deliberately a small state projection rather than a source of
truth. Records carry their owner and lifecycle policy so two producers can
use the same human-facing name without sharing reset or consumption state.
Read APIs never compact, expire, migrate, or roll the state file. All such
changes are explicit mutations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from .effects import EffectReceipt, EffectRecord
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    EventBus,
    SCHEMA_VERSION as EVENT_SCHEMA,
    StateError,
    atomic_json_write,
    ensure_bounded_json,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)


PANEL_SCHEMA = "moon.panel.v2"
LEGACY_PANEL_SCHEMA = "moon.panel.v1"
DEFAULT_OWNER = "default"
PANEL_LIFECYCLES = frozenset({"ttl", "persistent", "consume_once"})
PANEL_RESET_POLICIES = frozenset({"daily", "persistent"})
PANEL_CONSUME_POLICIES = frozenset({"repeatable", "once"})
AUTONOMY_COMPLETION_EFFECT_KIND = "autonomy_completion"

_FIELD_KEYS = frozenset(
    {
        "schema_version",
        "owner",
        "name",
        "value",
        "observed_at",
        "expires_at",
        "confidence",
        "source",
        "source_event_id",
        "epoch",
        "reset_policy",
        "persistence_policy",
        "consume_policy",
        "consumed_source_event_id",
        "consumed_at",
        "daily",
    }
)
_STATE_KEYS = frozenset(
    {"schema_version", "epoch", "owner_epochs", "fields", "bus_projections"}
)
_PROJECTION_KEYS = frozenset({"event_id", "kind", "source", "payload", "status"})
_PROJECTION_KINDS = frozenset({"panel.field_updated", "panel.field_consumed"})
_PROJECTION_STATUSES = frozenset({"pending", "delivered"})


class _PanelObserverSchemaError(StateError):
    """A panel or event schema cannot be interpreted by the observer."""


def _lock_free_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    """Read JSONL without going through :class:`JsonlLedger`.

    Observer probes must not create or acquire any owner lock.  This helper is
    intentionally private to the panel observer and validates only the
    envelope shape needed for integrity and projection matching.
    """

    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise StateError(f"observer JSONL row {line_number} is not an object")
            rows.append(value)
    return rows


def _lock_free_json_object(path: Path) -> dict[str, Any] | None:
    """Read the panel object without the normal mutation/read lock."""

    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise StateError("panel observer state is not an object")
    return value


def _panel_integrity_code(exc: Exception) -> str:
    """Return a stable, content-free integrity code."""

    if isinstance(exc, _PanelObserverSchemaError):
        return "panel_schema_error"
    return f"panel_integrity_error:{type(exc).__name__}"


def _panel_integrity_fact(
    *, target_date: date, now: datetime, code: str
) -> ObservationFact:
    return ObservationFact(
        key="panel:integrity",
        code=code,
        state="current" if code != "panel_integrity_ok" else "neutral",
        target_date=target_date,
        event_time=None if code == "panel_integrity_ok" else now,
        refs=("panel_state",),
        counts={"errors": 0 if code == "panel_integrity_ok" else 1},
    )


def _observer_field_metadata(raw: Mapping[str, Any]) -> tuple[str, str]:
    """Validate a field envelope without touching its ``value`` child."""

    if set(raw) != _FIELD_KEYS:
        raise StateError("panel observer field envelope has invalid fields")
    if raw["schema_version"] != PANEL_SCHEMA:
        raise _PanelObserverSchemaError("panel field schema is unsupported")
    owner = _nonempty_text(raw["owner"], "panel field owner")
    name = _nonempty_text(raw["name"], "panel field name")
    if "\x1f" in owner or "\x1f" in name:
        raise StateError("panel field owner/name is invalid")
    _nonempty_text(raw["source"], "panel field source", max_bytes=128)
    _nonempty_text(raw["source_event_id"], "panel field source_event_id")
    _nonempty_text(raw["epoch"], "panel field epoch", max_bytes=64)
    observed_at = _optional_time(raw["observed_at"], "panel field observed_at")
    if observed_at is None:
        raise StateError("panel field observed_at is invalid")
    expires_at = _optional_time(raw["expires_at"], "panel field expires_at")
    if expires_at is not None and expires_at <= observed_at:
        raise StateError("panel field expiry is invalid")
    confidence = raw["confidence"]
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise StateError("panel field confidence is invalid")
    if not 0 <= confidence <= 1:
        raise StateError("panel field confidence is invalid")
    if type(raw["daily"]) is not bool:
        raise StateError("panel field daily is invalid")
    if raw["reset_policy"] not in PANEL_RESET_POLICIES:
        raise StateError("panel field reset policy is invalid")
    if raw["persistence_policy"] not in PANEL_LIFECYCLES:
        raise StateError("panel field persistence policy is invalid")
    if raw["consume_policy"] not in PANEL_CONSUME_POLICIES:
        raise StateError("panel field consume policy is invalid")
    if raw["persistence_policy"] == "persistent" and expires_at is not None:
        raise StateError("persistent panel field has expiry")
    if raw["persistence_policy"] != "persistent" and expires_at is None:
        raise StateError("expiring panel field has no expiry")
    if raw["persistence_policy"] == "consume_once" and raw["consume_policy"] != "once":
        raise StateError("consume-once panel field has invalid policy")
    consumed_source_event_id = raw["consumed_source_event_id"]
    consumed_at = raw["consumed_at"]
    if consumed_source_event_id is None:
        if consumed_at is not None:
            raise StateError("unconsumed panel field has consumed_at")
    else:
        _nonempty_text(consumed_source_event_id, "panel consumed source_event_id")
        if raw["consume_policy"] != "once" or consumed_at is None:
            raise StateError("consumed panel field has invalid policy")
        _optional_time(consumed_at, "panel field consumed_at")
    # Deliberately only test for the key.  Reading, iterating, or serialising
    # this child could expose a PanelValue payload to the observer.
    if "value" not in raw:
        raise StateError("panel field value is missing")
    return owner, name


def _observer_projection_payload_metadata(
    kind: str, payload: Mapping[str, Any]
) -> tuple[str, str, str, str | None]:
    """Read only canonical projection scalars from a payload envelope."""

    if not isinstance(payload, Mapping):
        raise StateError("panel observer projection payload is invalid")
    expected_payload = (
        {"field", "owner", "source_event_id", "expires_at"}
        if kind == "panel.field_updated"
        else {"field", "owner", "source_event_id"}
    )
    if set(payload) != expected_payload:
        raise StateError("panel observer projection payload has invalid fields")
    field = _nonempty_text(payload["field"], "panel projection field")
    owner = _nonempty_text(payload["owner"], "panel projection owner")
    source_event_id = _nonempty_text(
        payload["source_event_id"], "panel projection source_event_id"
    )
    expires_at = None
    if kind == "panel.field_updated":
        expires_at = payload["expires_at"]
        _optional_time(expires_at, "panel projection expires_at")
    return field, owner, source_event_id, expires_at


def _observer_projection_metadata(
    key: Any, raw: Mapping[str, Any]
) -> tuple[str, str, str, str, tuple[str, str, str, str | None]]:
    """Validate projection metadata without touching field values."""

    if not isinstance(key, str) or not isinstance(raw, Mapping):
        raise StateError("panel observer projection is invalid")
    if set(raw) != _PROJECTION_KEYS:
        raise StateError("panel observer projection has invalid fields")
    event_id = _nonempty_text(raw["event_id"], "panel projection event_id")
    if key != event_id:
        raise StateError("panel observer projection id conflicts")
    kind = raw["kind"]
    if kind not in _PROJECTION_KINDS:
        raise StateError("panel observer projection kind is invalid")
    source = _nonempty_text(raw["source"], "panel projection source", max_bytes=128)
    status = raw["status"]
    if status not in _PROJECTION_STATUSES:
        raise StateError("panel observer projection status is invalid")
    payload = raw["payload"]
    if not isinstance(payload, Mapping):
        raise StateError("panel observer projection payload is invalid")
    payload_metadata = _observer_projection_payload_metadata(kind, payload)
    return event_id, kind, source, status, payload_metadata


def _observer_state_metadata(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate panel metadata without touching field values or payload bodies."""

    if set(raw) != _STATE_KEYS:
        raise StateError("panel observer state has invalid fields")
    if raw["schema_version"] != PANEL_SCHEMA:
        raise _PanelObserverSchemaError("panel state schema is unsupported")
    epoch = _nonempty_text(raw["epoch"], "panel observer epoch")
    raw_owner_epochs = raw["owner_epochs"]
    if not isinstance(raw_owner_epochs, Mapping):
        raise StateError("panel observer owner_epochs is invalid")
    owner_epochs: dict[str, str] = {}
    for owner, owner_epoch in raw_owner_epochs.items():
        owner_epochs[_nonempty_text(owner, "panel observer owner")] = _nonempty_text(
            owner_epoch, "panel observer owner epoch"
        )
    fields = raw["fields"]
    if not isinstance(fields, Mapping):
        raise StateError("panel observer fields are invalid")
    field_keys: set[str] = set()
    for key, field in fields.items():
        if not isinstance(key, str) or not isinstance(field, Mapping):
            raise StateError("panel observer field is invalid")
        owner, name = _observer_field_metadata(field)
        canonical = _storage_key(owner, name)
        if canonical in field_keys:
            raise StateError("panel observer has duplicate owner/name fields")
        field_keys.add(canonical)
    projections = raw["bus_projections"]
    if not isinstance(projections, Mapping):
        raise StateError("panel observer projections are invalid")
    projection_meta: dict[
        str, tuple[str, str, str, str, tuple[str, str, str, str | None]]
    ] = {}
    for key, projection in projections.items():
        event_id, kind, source, status, payload = _observer_projection_metadata(
            key, projection
        )
        if event_id in projection_meta:
            raise StateError("panel observer has duplicate projections")
        projection_meta[event_id] = (event_id, kind, source, status, payload)
    return {
        "schema_version": raw["schema_version"],
        "epoch": epoch,
        "owner_epochs": owner_epochs,
        "field_count": len(field_keys),
        "projections": projection_meta,
    }


def _nonempty_text(value: Any, label: str, *, max_bytes: int = 256) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    ensure_bounded_text(value, label, max_bytes=max_bytes)
    return value


def _optional_time(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError(f"{label} must be an ISO timestamp or null")
    return parse_time(value)


def _storage_key(owner: str, name: str) -> str:
    if "\x1f" in owner or "\x1f" in name:
        raise ValueError("panel owner and field name cannot contain the separator")
    return f"{owner}\x1f{name}"


def _projection_event_id(
    *, kind: str, owner: str, field: str, source_event_id: str
) -> str:
    canonical = json.dumps(
        [kind, owner, field, source_event_id],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    return f"panel_event_{digest}"


@dataclass(frozen=True)
class PanelValue:
    """A validated panel record.

    The first six positional fields retain the v1 constructor shape. New
    callers should use the named owner/policy/source-event fields.
    """

    value: Any
    observed_at: datetime
    expires_at: datetime | None
    confidence: float
    source: str
    daily: bool = True
    owner: str = DEFAULT_OWNER
    name: str = ""
    source_event_id: str = ""
    epoch: str = ""
    reset_policy: str = ""
    persistence_policy: str = "ttl"
    consume_policy: str = "repeatable"
    consumed_source_event_id: str | None = None
    consumed_at: datetime | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.observed_at, datetime)
            or self.observed_at.tzinfo is None
        ):
            raise ValueError("panel observed_at must be timezone-aware")
        if self.expires_at is not None:
            if (
                not isinstance(self.expires_at, datetime)
                or self.expires_at.tzinfo is None
            ):
                raise ValueError("panel expires_at must be timezone-aware")
            if self.expires_at <= self.observed_at:
                raise ValueError("panel value expiry must follow observation")
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, (int, float))
            or not 0 <= self.confidence <= 1
        ):
            raise ValueError("panel confidence must be between 0 and 1")
        if type(self.daily) is not bool:
            raise ValueError("panel daily must be boolean")
        owner = _nonempty_text(self.owner, "panel owner")
        name = _nonempty_text(self.name, "panel field name")
        source = _nonempty_text(self.source, "panel source", max_bytes=128)
        source_event_id = _nonempty_text(
            self.source_event_id or f"legacy:{source}:{name}",
            "panel source_event_id",
        )
        epoch = _nonempty_text(self.epoch or "unknown", "panel epoch", max_bytes=64)
        reset_policy = self.reset_policy or ("daily" if self.daily else "persistent")
        if reset_policy not in PANEL_RESET_POLICIES:
            raise ValueError("panel reset_policy is invalid")
        if self.persistence_policy not in PANEL_LIFECYCLES:
            raise ValueError("panel persistence_policy is invalid")
        if self.consume_policy not in PANEL_CONSUME_POLICIES:
            raise ValueError("panel consume_policy is invalid")
        if self.persistence_policy == "persistent" and self.expires_at is not None:
            raise ValueError("persistent panel values cannot have an expiry")
        if self.persistence_policy != "persistent" and self.expires_at is None:
            raise ValueError("expiring panel values require expires_at")
        if self.persistence_policy == "consume_once" and self.consume_policy != "once":
            raise ValueError("consume_once lifecycle requires consume_policy once")
        if self.consumed_source_event_id is not None:
            _nonempty_text(
                self.consumed_source_event_id,
                "panel consumed_source_event_id",
            )
            if self.consume_policy != "once":
                raise ValueError("only consume-once values can be consumed")
            if self.consumed_at is None:
                raise ValueError("consumed panel values require consumed_at")
        elif self.consumed_at is not None:
            raise ValueError("unconsumed panel values cannot have consumed_at")
        if self.consumed_at is not None and (
            not isinstance(self.consumed_at, datetime)
            or self.consumed_at.tzinfo is None
        ):
            raise ValueError("panel consumed_at must be timezone-aware")
        ensure_bounded_json(self.value, "panel value", max_bytes=64 * 1024)
        object.__setattr__(self, "owner", owner)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "source_event_id", source_event_id)
        object.__setattr__(self, "epoch", epoch)
        object.__setattr__(self, "reset_policy", reset_policy)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PANEL_SCHEMA,
            "owner": self.owner,
            "name": self.name,
            "value": self.value,
            "observed_at": isoformat(self.observed_at),
            "expires_at": (
                None if self.expires_at is None else isoformat(self.expires_at)
            ),
            "confidence": self.confidence,
            "source": self.source,
            "source_event_id": self.source_event_id,
            "epoch": self.epoch,
            "reset_policy": self.reset_policy,
            "persistence_policy": self.persistence_policy,
            "consume_policy": self.consume_policy,
            "consumed_source_event_id": self.consumed_source_event_id,
            "consumed_at": (
                None if self.consumed_at is None else isoformat(self.consumed_at)
            ),
            "daily": self.reset_policy == "daily",
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        owner: str | None = None,
        name: str | None = None,
        epoch: str | None = None,
    ) -> "PanelValue":
        """Parse either a strict v2 field or a legacy v1 field."""

        schema = value.get("schema_version")
        if schema == PANEL_SCHEMA:
            if set(value) != _FIELD_KEYS:
                raise StateError("panel v2 field has unsupported fields")
            confidence = value["confidence"]
            if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
                raise StateError("panel confidence must be numeric")
            daily = value["daily"]
            if type(daily) is not bool:
                raise StateError("panel daily must be boolean")
            try:
                return cls(
                    value=value["value"],
                    observed_at=parse_time(str(value["observed_at"])),
                    expires_at=_optional_time(value["expires_at"], "expires_at"),
                    confidence=float(confidence),
                    source=value["source"],
                    daily=daily,
                    owner=value["owner"],
                    name=value["name"],
                    source_event_id=value["source_event_id"],
                    epoch=value["epoch"],
                    reset_policy=value["reset_policy"],
                    persistence_policy=value["persistence_policy"],
                    consume_policy=value["consume_policy"],
                    consumed_source_event_id=value["consumed_source_event_id"],
                    consumed_at=_optional_time(value["consumed_at"], "consumed_at"),
                )
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError("panel v2 field is invalid") from exc
        if schema not in (None, LEGACY_PANEL_SCHEMA):
            raise StateError("panel field has an unsupported schema")
        confidence = value["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise StateError("panel confidence must be numeric")
        daily = value.get("daily", True)
        if type(daily) is not bool:
            raise StateError("panel daily must be boolean")
        try:
            legacy_name = name or "legacy"
            legacy_owner = owner or DEFAULT_OWNER
            return cls(
                value=value.get("value"),
                observed_at=parse_time(str(value["observed_at"])),
                expires_at=parse_time(str(value["expires_at"])),
                confidence=float(confidence),
                source=str(value["source"]),
                daily=daily,
                owner=legacy_owner,
                name=legacy_name,
                source_event_id=f"legacy:{legacy_owner}:{legacy_name}",
                epoch=epoch or "legacy",
                reset_policy="daily" if daily else "persistent",
                persistence_policy="ttl",
                consume_policy="repeatable",
            )
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise StateError("panel field is invalid") from exc


def epoch_id(now: datetime, *, timezone_name: str, anchor_hour: int) -> str:
    if not 0 <= anchor_hour <= 23:
        raise ValueError("anchor_hour must be from 0 to 23")
    local = now.astimezone(ZoneInfo(timezone_name))
    effective: date = local.date()
    if local.hour < anchor_hour:
        effective -= timedelta(days=1)
    return effective.isoformat()


class PanelStore:
    """Concurrent-safe owner-scoped panel state."""

    def __init__(
        self,
        root: Path,
        *,
        bus: EventBus,
        timezone_name: str,
        anchor_hour: int = 6,
        clock: Callable[[], datetime] = utc_now,
        owner_id: str = DEFAULT_OWNER,
        owner: str | None = None,
    ):
        self.root = Path(root)
        self.path = self.root / "panel.json"
        self.lock_path = self.root / "panel.lock"
        self.backup_path = self.root / "panel.v1.backup.json"
        self.bus = bus
        self.timezone_name = timezone_name
        self.anchor_hour = anchor_hour
        self.clock = clock
        if owner is not None:
            if owner_id != DEFAULT_OWNER and owner_id != owner:
                raise ValueError("panel owner aliases conflict")
            owner_id = owner
        self.owner_id = _nonempty_text(owner_id, "panel owner")
        ZoneInfo(timezone_name)

    def _empty(self, now: datetime) -> dict[str, Any]:
        return {
            "schema_version": PANEL_SCHEMA,
            "epoch": epoch_id(
                now, timezone_name=self.timezone_name, anchor_hour=self.anchor_hour
            ),
            "owner_epochs": {},
            "fields": {},
            "bus_projections": {},
        }

    def _read_json(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StateError("panel state is unreadable") from exc
        if not isinstance(value, dict):
            raise StateError("panel state must be an object")
        return value

    @staticmethod
    def _compare_epochs(left: str, right: str) -> int:
        """Compare reset epochs without allowing malformed state to guess."""

        if left == right:
            return 0
        try:
            left_date = date.fromisoformat(left)
            right_date = date.fromisoformat(right)
        except (TypeError, ValueError) as exc:
            raise StateError("panel epochs are not comparable") from exc
        return -1 if left_date < right_date else 1

    @staticmethod
    def _validate_projection(key: Any, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(key, str) or not isinstance(raw, Mapping):
            raise StateError("panel bus projection is invalid")
        if set(raw) != _PROJECTION_KEYS:
            raise StateError("panel bus projection has unsupported fields")
        event_id = _nonempty_text(raw["event_id"], "panel projection event_id")
        if key != event_id:
            raise StateError("panel bus projection key does not match event_id")
        kind = raw["kind"]
        if kind not in _PROJECTION_KINDS:
            raise StateError("panel bus projection kind is invalid")
        source = _nonempty_text(raw["source"], "panel projection source", max_bytes=128)
        status = raw["status"]
        if status not in _PROJECTION_STATUSES:
            raise StateError("panel bus projection status is invalid")
        payload = raw["payload"]
        if not isinstance(payload, Mapping):
            raise StateError("panel bus projection payload must be an object")
        expected_payload = (
            {"field", "owner", "source_event_id", "expires_at"}
            if kind == "panel.field_updated"
            else {"field", "owner", "source_event_id"}
        )
        if set(payload) != expected_payload:
            raise StateError("panel bus projection payload has unsupported fields")
        _nonempty_text(payload["field"], "panel projection field")
        _nonempty_text(payload["owner"], "panel projection owner")
        _nonempty_text(payload["source_event_id"], "panel projection source_event_id")
        if kind == "panel.field_updated" and payload["expires_at"] is not None:
            _optional_time(payload["expires_at"], "panel projection expires_at")
        ensure_bounded_json(
            dict(payload), "panel bus projection payload", max_bytes=4096
        )
        return {
            "event_id": event_id,
            "kind": kind,
            "source": source,
            "payload": dict(payload),
            "status": status,
        }

    @staticmethod
    def _new_projection(
        *,
        kind: str,
        source: str,
        field: str,
        owner: str,
        source_event_id: str,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        event_id = _projection_event_id(
            kind=kind,
            owner=owner,
            field=field,
            source_event_id=source_event_id,
        )
        payload: dict[str, Any] = {
            "field": field,
            "owner": owner,
            "source_event_id": source_event_id,
        }
        if kind == "panel.field_updated":
            payload["expires_at"] = expires_at
        return {
            "event_id": event_id,
            "kind": kind,
            "source": source,
            "payload": payload,
            "status": "pending",
        }

    @staticmethod
    def _find_projection(
        state: Mapping[str, Any],
        *,
        kind: str,
        field: str,
        owner: str,
        source_event_id: str,
    ) -> dict[str, Any] | None:
        found: dict[str, Any] | None = None
        for projection in state.get("bus_projections", {}).values():
            payload = projection["payload"]
            if (
                projection["kind"] == kind
                and payload["field"] == field
                and payload["owner"] == owner
                and payload["source_event_id"] == source_event_id
            ):
                if found is not None:
                    raise StateError("panel has duplicate bus projections")
                found = projection
        return found

    def _bus_projection_exists(self, projection: Mapping[str, Any]) -> bool:
        """Return whether the exact canonical event already reached the bus."""

        event_id = projection["event_id"]
        for event in self.bus.read_events():
            if event.event_id != event_id:
                continue
            if (
                event.kind != projection["kind"]
                or event.source != projection["source"]
                or dict(event.payload) != projection["payload"]
            ):
                raise StateError("panel bus projection conflicts with canonical event")
            return True
        return False

    def _deliver_projection(self, projection: dict[str, Any]) -> None:
        if projection["status"] == "delivered":
            return
        if not self._bus_projection_exists(projection):
            self.bus.emit(
                projection["kind"],
                source=projection["source"],
                event_id=projection["event_id"],
                payload=projection["payload"],
            )
        projection["status"] = "delivered"

    def _finish_projection(
        self, state: dict[str, Any], projection: dict[str, Any]
    ) -> None:
        """Deliver once, then compact the durable projection marker."""

        self._deliver_projection(projection)
        state["bus_projections"].pop(projection["event_id"], None)
        atomic_json_write(self.path, state)

    def _read_state(self, now: datetime) -> dict[str, Any]:
        value = self._read_json()
        if value is None:
            return self._empty(now)
        schema = value.get("schema_version")
        if schema == LEGACY_PANEL_SCHEMA:
            self._validate_legacy_state(value)
            raise StateError("legacy panel state requires explicit migrate_v1")
        if schema != PANEL_SCHEMA:
            raise StateError("panel state has an unsupported schema")
        if not set(value) <= _STATE_KEYS:
            raise StateError("panel state has unsupported fields")
        fields = value.get("fields")
        if not isinstance(fields, dict):
            raise StateError("panel fields must be an object")
        epoch = value.get("epoch")
        if type(epoch) is not str or not epoch.strip():
            raise StateError("panel epoch must be a non-empty string")
        owner_epochs = value.get("owner_epochs", {})
        if not isinstance(owner_epochs, dict):
            raise StateError("panel owner_epochs must be an object")
        for owner, owner_epoch in owner_epochs.items():
            if type(owner) is not str or not owner.strip():
                raise StateError("panel owner_epochs owner must be non-empty")
            if type(owner_epoch) is not str or not owner_epoch.strip():
                raise StateError("panel owner_epochs epoch must be non-empty")
        projections = value.get("bus_projections", {})
        if not isinstance(projections, dict):
            raise StateError("panel bus_projections must be an object")
        indexed_projections: dict[str, dict[str, Any]] = {}
        for key, raw in projections.items():
            try:
                projection = self._validate_projection(key, raw)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError("panel bus projection is invalid") from exc
            indexed_projections[key] = projection
        indexed: dict[str, dict[str, Any]] = {}
        for key, raw in fields.items():
            if not isinstance(key, str) or not isinstance(raw, Mapping):
                raise StateError("panel field is invalid")
            try:
                parsed = PanelValue.from_dict(raw)
                canonical = _storage_key(parsed.owner, parsed.name)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                label = raw.get("name", key) if isinstance(raw, Mapping) else key
                raise StateError(f"panel field {label!r} is invalid") from exc
            if canonical in indexed:
                raise StateError("panel contains duplicate owner/name records")
            indexed[canonical] = parsed.to_dict()
        return {
            "schema_version": PANEL_SCHEMA,
            "epoch": epoch,
            "owner_epochs": dict(owner_epochs),
            "fields": indexed,
            "bus_projections": indexed_projections,
        }

    def _validate_legacy_state(self, value: Mapping[str, Any]) -> None:
        fields = value.get("fields")
        if not isinstance(fields, dict):
            raise StateError("panel fields must be an object")
        epoch = value.get("epoch")
        if type(epoch) is not str or not epoch.strip():
            raise StateError("panel epoch must be a non-empty string")
        for key, raw in fields.items():
            if not isinstance(key, str) or not isinstance(raw, Mapping):
                raise StateError("panel field is invalid")
            try:
                PanelValue.from_dict(raw, owner=DEFAULT_OWNER, name=key, epoch=epoch)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"panel field {key!r} is invalid") from exc

    def _ensure_migration_backup(self, legacy: Mapping[str, Any]) -> None:
        """Create or verify the immutable v1 recovery copy."""

        if self.backup_path.exists():
            try:
                existing = json.loads(self.backup_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise StateError("panel migration backup is unreadable") from exc
            if existing != dict(legacy):
                raise StateError("panel migration backup conflicts with legacy state")
            return
        atomic_json_write(self.backup_path, legacy)

    def _parse_records(self, state: Mapping[str, Any]) -> dict[str, PanelValue]:
        records: dict[str, PanelValue] = {}
        for key, raw in state["fields"].items():
            try:
                records[key] = PanelValue.from_dict(raw)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"panel field {key!r} is invalid") from exc
        return records

    def _visible_records(
        self,
        state: Mapping[str, Any],
        *,
        now: datetime,
        owner: str | None,
    ) -> dict[str, PanelValue]:
        current_epoch = epoch_id(
            now, timezone_name=self.timezone_name, anchor_hour=self.anchor_hour
        )
        visible: dict[str, PanelValue] = {}
        for key, record in self._parse_records(state).items():
            if owner is not None and record.owner != owner:
                continue
            if record.expires_at is not None and record.expires_at <= now:
                continue
            owner_epochs = state.get("owner_epochs", {})
            # Every owner write records its own current epoch.  Missing entries
            # are only possible in older v2 files; their records remain hidden
            # on a natural-day change rather than changing visibility when a
            # different owner rolls over.
            target_epoch = owner_epochs.get(record.owner, current_epoch)
            if record.reset_policy == "daily" and record.epoch != target_epoch:
                continue
            if (
                record.consume_policy == "once"
                and record.consumed_source_event_id is not None
            ):
                continue
            visible[key] = record
        return visible

    def _display_key(
        self,
        record: PanelValue,
        *,
        owner_filter: str | None,
        used: set[str],
    ) -> str:
        if owner_filter is not None:
            candidate = record.name
        elif record.owner == self.owner_id and record.name not in used:
            candidate = record.name
        else:
            candidate = f"{record.owner}/{record.name}"
        if candidate in used:
            candidate = f"{record.owner}/{record.name}"
        while candidate in used:
            candidate = f"{candidate}:2"
        used.add(candidate)
        return candidate

    @staticmethod
    def _resolve_lifecycle(
        *,
        ttl: timedelta | None,
        daily: bool | None,
        reset_policy: str | None,
        persistence_policy: str | None,
        policy: str | None,
        persistence: str | None,
        lifecycle: str | None,
        consume_policy: str,
        consume_once: bool,
        persistent: bool,
    ) -> tuple[str, str, str, bool]:
        if ttl is not None and not isinstance(ttl, timedelta):
            raise ValueError("panel ttl must be a timedelta")
        policies = [
            candidate
            for candidate in (persistence_policy, policy, persistence, lifecycle)
            if candidate is not None
        ]
        if len(set(policies)) > 1:
            raise ValueError("panel lifecycle policy aliases conflict")
        lifecycle = policies[0] if policies else None
        if persistent:
            if lifecycle is not None and lifecycle != "persistent":
                raise ValueError("persistent flag conflicts with lifecycle policy")
            lifecycle = "persistent"
        normalized_consume = "once" if consume_once else consume_policy
        if normalized_consume == "consume_once":
            normalized_consume = "once"
        if normalized_consume not in PANEL_CONSUME_POLICIES:
            raise ValueError("panel consume_policy is invalid")
        if lifecycle is None:
            lifecycle = "consume_once" if normalized_consume == "once" else "ttl"
        if lifecycle not in PANEL_LIFECYCLES:
            raise ValueError("panel lifecycle policy is invalid")
        if lifecycle == "consume_once" and normalized_consume != "once":
            raise ValueError("consume_once lifecycle requires consume_policy once")
        if lifecycle != "consume_once" and normalized_consume == "once":
            raise ValueError("consume-once policy requires consume_once lifecycle")
        if lifecycle == "persistent":
            if ttl is not None:
                raise ValueError("persistent panel values cannot specify ttl")
        elif ttl is None or ttl <= timedelta(0):
            raise ValueError("panel lifecycle requires a positive ttl")
        if reset_policy is None:
            if daily is None:
                reset = "persistent" if lifecycle == "persistent" else "daily"
            else:
                if type(daily) is not bool:
                    raise ValueError("panel daily must be boolean")
                reset = "daily" if daily else "persistent"
        else:
            reset = reset_policy
            if daily is not None and reset != ("daily" if daily else "persistent"):
                raise ValueError("panel reset policy aliases conflict")
        if reset not in PANEL_RESET_POLICIES:
            raise ValueError("panel reset_policy is invalid")
        return lifecycle, reset, normalized_consume, reset == "daily"

    def set_field(
        self,
        name: str,
        value: Any,
        *,
        source: str,
        ttl: timedelta | None = None,
        confidence: float = 1.0,
        daily: bool | None = None,
        observed_at: datetime | None = None,
        owner: str | None = None,
        source_event_id: str | None = None,
        reset_policy: str | None = None,
        persistence_policy: str | None = None,
        policy: str | None = None,
        persistence: str | None = None,
        lifecycle: str | None = None,
        consume_policy: str = "repeatable",
        consume_once: bool = False,
        persistent: bool = False,
    ) -> PanelValue:
        owner_value = (
            self.owner_id if owner is None else _nonempty_text(owner, "panel owner")
        )
        name_value = _nonempty_text(name, "panel field name")
        source_value = _nonempty_text(source, "panel source", max_bytes=128)
        lifecycle, reset, consume, is_daily = self._resolve_lifecycle(
            ttl=ttl,
            daily=daily,
            reset_policy=reset_policy,
            persistence_policy=persistence_policy,
            policy=policy,
            persistence=persistence,
            lifecycle=lifecycle,
            consume_policy=consume_policy,
            consume_once=consume_once,
            persistent=persistent,
        )
        observed = self.clock() if observed_at is None else observed_at
        if not isinstance(observed, datetime) or observed.tzinfo is None:
            raise ValueError("panel observed_at must be timezone-aware")
        event_id = (
            new_id("panel")
            if source_event_id is None
            else _nonempty_text(source_event_id, "panel source_event_id")
        )
        current_epoch = epoch_id(
            observed, timezone_name=self.timezone_name, anchor_hour=self.anchor_hour
        )
        typed = PanelValue(
            value=value,
            observed_at=observed,
            expires_at=None if lifecycle == "persistent" else observed + ttl,  # type: ignore[operator]
            confidence=confidence,
            source=source_value,
            daily=is_daily,
            owner=owner_value,
            name=name_value,
            source_event_id=event_id,
            epoch=current_epoch,
            reset_policy=reset,
            persistence_policy=lifecycle,
            consume_policy=consume,
        )
        canonical = _storage_key(owner_value, name_value)
        with file_lock(self.lock_path):
            state = self._read_state(observed)
            existing = self._parse_records(state).get(canonical)
            if existing is not None and existing.source_event_id == event_id:
                if not self._same_projection(existing, typed):
                    raise ValueError("panel source event conflicts with existing field")
                projection = self._find_projection(
                    state,
                    kind="panel.field_updated",
                    field=name_value,
                    owner=owner_value,
                    source_event_id=event_id,
                )
                if projection is None:
                    projection = self._new_projection(
                        kind="panel.field_updated",
                        source=source_value,
                        field=name_value,
                        owner=owner_value,
                        source_event_id=event_id,
                        expires_at=(
                            None
                            if existing.expires_at is None
                            else isoformat(existing.expires_at)
                        ),
                    )
                    state["bus_projections"][projection["event_id"]] = projection
                    atomic_json_write(self.path, state)
                self._finish_projection(state, projection)
                return existing
            owner_epochs = state.setdefault("owner_epochs", {})
            stored_epoch = owner_epochs.get(owner_value)
            if stored_epoch is None and existing is not None:
                stored_epoch = existing.epoch
            if (
                stored_epoch is not None
                and self._compare_epochs(current_epoch, stored_epoch) < 0
            ):
                if existing is not None:
                    # An older source event is an explicit no-op: return the
                    # current record without touching state or the event bus.
                    return existing
                raise ValueError("panel source event is stale for owner epoch")
            if existing is not None:
                observation_order = (
                    -1
                    if typed.observed_at < existing.observed_at
                    else 1
                    if typed.observed_at > existing.observed_at
                    else 0
                )
                if observation_order < 0:
                    # The caller receives the durable current value as the
                    # explicit stale-event result; no projection is emitted.
                    return existing
                if observation_order == 0:
                    raise ValueError(
                        "panel source events conflict at the same observation time"
                    )
                if self._compare_epochs(typed.epoch, existing.epoch) < 0:
                    return existing
            if existing is not None and existing.consume_policy == "once":
                raise ValueError(
                    "cannot replace a consume-once panel field before consume"
                )
            owner_epochs[owner_value] = current_epoch
            state["fields"][canonical] = typed.to_dict()
            projection = self._new_projection(
                kind="panel.field_updated",
                source=source_value,
                field=name_value,
                owner=owner_value,
                source_event_id=event_id,
                expires_at=(
                    None if typed.expires_at is None else isoformat(typed.expires_at)
                ),
            )
            state["bus_projections"][projection["event_id"]] = projection
            atomic_json_write(self.path, state)
            self._finish_projection(state, projection)
        return typed

    @staticmethod
    def _same_projection(left: PanelValue, right: PanelValue) -> bool:
        return (
            left.owner == right.owner
            and left.name == right.name
            and left.value == right.value
            and left.source == right.source
            and left.confidence == right.confidence
            and left.reset_policy == right.reset_policy
            and left.persistence_policy == right.persistence_policy
            and left.consume_policy == right.consume_policy
        )

    def record_sensor(
        self,
        sensor: str,
        value: Any,
        *,
        ttl: timedelta,
        confidence: float = 1.0,
        observed_at: datetime | None = None,
        owner: str | None = None,
        source_event_id: str | None = None,
    ) -> PanelValue:
        observed = self.clock() if observed_at is None else observed_at
        event = self.bus.emit(
            "sensor.observation",
            source=sensor,
            payload={"sensor": sensor, "observed_at": isoformat(observed)},
            event_id=source_event_id,
        )
        event_id = getattr(event, "event_id", None)
        if event_id is None and isinstance(event, Mapping):
            event_id = event.get("event_id")
        return self.set_field(
            sensor,
            value,
            source=sensor,
            ttl=ttl,
            confidence=confidence,
            observed_at=observed,
            owner=owner,
            source_event_id=event_id or source_event_id,
        )

    def consume(
        self,
        name: str,
        *,
        source_event_id: str,
        owner: str | None = None,
        now: datetime | None = None,
    ) -> PanelValue:
        owner_value = (
            self.owner_id if owner is None else _nonempty_text(owner, "panel owner")
        )
        name_value = _nonempty_text(name, "panel field name")
        event_id = _nonempty_text(source_event_id, "panel source_event_id")
        effective_now = self.clock() if now is None else now
        canonical = _storage_key(owner_value, name_value)
        with file_lock(self.lock_path):
            state = self._read_state(effective_now)
            record = self._parse_records(state).get(canonical)
            if record is None:
                raise KeyError(f"panel field {name_value!r} is not present")
            if record.consume_policy != "once":
                raise ValueError("panel field is not consume-once")
            if event_id != record.source_event_id:
                raise ValueError("consume source event does not match canonical source")
            if record.consumed_source_event_id is not None:
                if record.consumed_source_event_id == event_id:
                    projection = self._find_projection(
                        state,
                        kind="panel.field_consumed",
                        field=name_value,
                        owner=owner_value,
                        source_event_id=event_id,
                    )
                    if projection is None:
                        projection = self._new_projection(
                            kind="panel.field_consumed",
                            source=record.source,
                            field=name_value,
                            owner=owner_value,
                            source_event_id=event_id,
                        )
                        state["bus_projections"][projection["event_id"]] = projection
                        atomic_json_write(self.path, state)
                    self._finish_projection(state, projection)
                    return record
                raise ValueError(
                    "consume-once panel field was consumed by another source"
                )
            if record.expires_at is not None and record.expires_at <= effective_now:
                raise ValueError("panel field has expired")
            target_epoch = state.get("owner_epochs", {}).get(
                record.owner,
                epoch_id(
                    effective_now,
                    timezone_name=self.timezone_name,
                    anchor_hour=self.anchor_hour,
                ),
            )
            if record.reset_policy == "daily" and record.epoch != target_epoch:
                raise ValueError("panel field is stale for the current epoch")
            consumed = PanelValue(
                value=record.value,
                observed_at=record.observed_at,
                expires_at=record.expires_at,
                confidence=record.confidence,
                source=record.source,
                daily=record.daily,
                owner=record.owner,
                name=record.name,
                source_event_id=record.source_event_id,
                epoch=record.epoch,
                reset_policy=record.reset_policy,
                persistence_policy=record.persistence_policy,
                consume_policy=record.consume_policy,
                consumed_source_event_id=event_id,
                consumed_at=effective_now,
            )
            state["fields"][canonical] = consumed.to_dict()
            projection = self._new_projection(
                kind="panel.field_consumed",
                source=record.source,
                field=name_value,
                owner=owner_value,
                source_event_id=event_id,
            )
            state["bus_projections"][projection["event_id"]] = projection
            atomic_json_write(self.path, state)
            self._finish_projection(state, projection)
        return consumed

    def consume_once(
        self,
        name: str,
        *,
        source_event_id: str,
        owner: str | None = None,
        now: datetime | None = None,
    ) -> PanelValue:
        return self.consume(name, source_event_id=source_event_id, owner=owner, now=now)

    def rollover(
        self, owner: str | None = None, *, now: datetime | None = None
    ) -> dict[str, Any]:
        """Persist a day-open reset for one owner only."""

        owner_value = (
            self.owner_id if owner is None else _nonempty_text(owner, "panel owner")
        )
        effective_now = self.clock() if now is None else now
        current_epoch = epoch_id(
            effective_now,
            timezone_name=self.timezone_name,
            anchor_hour=self.anchor_hour,
        )
        with file_lock(self.lock_path):
            state = self._read_state(effective_now)
            records = self._parse_records(state)
            if self._compare_epochs(current_epoch, state["epoch"]) < 0:
                return self.snapshot(owner=owner_value, now=effective_now)
            owner_epochs = state.setdefault("owner_epochs", {})
            owner_epoch = owner_epochs.get(owner_value)
            if (
                owner_epoch is not None
                and self._compare_epochs(current_epoch, owner_epoch) < 0
            ):
                return self.snapshot(owner=owner_value, now=effective_now)
            changed = self._compare_epochs(current_epoch, state["epoch"]) > 0
            if (
                owner_epoch is None
                or self._compare_epochs(current_epoch, owner_epoch) > 0
            ):
                owner_epochs[owner_value] = current_epoch
                changed = True
            for key, record in records.items():
                if (
                    record.owner == owner_value
                    and record.reset_policy == "daily"
                    and self._compare_epochs(record.epoch, current_epoch) < 0
                ):
                    del state["fields"][key]
                    changed = True
            if changed:
                state["epoch"] = current_epoch
                atomic_json_write(self.path, state)
        return self.snapshot(owner=owner_value, now=effective_now)

    def migrate_v1(
        self,
        owner_mapping: Mapping[str, str] | str | Callable[[str], str] | None = None,
        *,
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Explicitly migrate v1 and retain a JSON backup."""

        effective_now = self.clock()
        selected_owner = owner or (
            owner_mapping if isinstance(owner_mapping, str) else None
        )
        if selected_owner is not None:
            selected_owner = _nonempty_text(selected_owner, "panel owner")
        if (
            owner is not None
            and isinstance(owner_mapping, str)
            and owner != owner_mapping
        ):
            raise ValueError("migration owner aliases conflict")
        with file_lock(self.lock_path):
            raw = _lock_free_json_object(self.path)
            if raw is None:
                state = self._empty(effective_now)
                atomic_json_write(self.path, state)
                return self.snapshot(now=effective_now)
            if raw.get("schema_version") == PANEL_SCHEMA:
                self._read_state(effective_now)
                return self.snapshot(now=effective_now)
            if raw.get("schema_version") != LEGACY_PANEL_SCHEMA:
                raise StateError("panel state has an unsupported schema")
            self._validate_legacy_state(raw)
            legacy_epoch = raw["epoch"]
            converted: dict[str, Any] = {}
            for name, value in raw["fields"].items():
                if selected_owner is not None:
                    mapped_owner = selected_owner
                elif isinstance(owner_mapping, Mapping):
                    mapped_owner = owner_mapping.get(
                        name, owner_mapping.get("*", DEFAULT_OWNER)
                    )
                elif callable(owner_mapping):
                    mapped_owner = owner_mapping(name)
                else:
                    mapped_owner = DEFAULT_OWNER
                mapped_owner = _nonempty_text(mapped_owner, "panel owner")
                legacy = PanelValue.from_dict(
                    value, owner=mapped_owner, name=name, epoch=legacy_epoch
                )
                converted[_storage_key(mapped_owner, name)] = legacy.to_dict()
            state = {
                "schema_version": PANEL_SCHEMA,
                "epoch": legacy_epoch,
                "owner_epochs": {},
                "fields": converted,
                "bus_projections": {},
            }
            for record in converted.values():
                state["owner_epochs"].setdefault(record["owner"], legacy_epoch)
            self._ensure_migration_backup(raw)
            atomic_json_write(self.path, state)
        return self.snapshot(now=effective_now)

    def record_activity_afterglow(
        self,
        *,
        effect_record: EffectRecord,
        effect_receipt: EffectReceipt,
        canonical_event_id: str,
        summary: str,
        ttl: timedelta = timedelta(hours=3),
        owner: str | None = None,
    ) -> PanelValue:
        """Project one verified autonomy completion into activity afterglow."""

        if not isinstance(effect_record, EffectRecord):
            raise TypeError("activity afterglow requires a verified EffectRecord")
        if not isinstance(effect_receipt, EffectReceipt):
            raise TypeError("activity afterglow requires a verified EffectReceipt")
        if not effect_record.verified or effect_record.receipt != effect_receipt:
            raise ValueError("activity afterglow requires a matching verified receipt")
        if effect_record.kind != AUTONOMY_COMPLETION_EFFECT_KIND:
            raise ValueError(
                "activity afterglow requires autonomy_completion effect kind"
            )
        canonical = _nonempty_text(canonical_event_id, "canonical_event_id")
        if canonical != effect_record.source_event_id:
            raise ValueError("canonical event does not match effect source event")
        if (
            effect_receipt.event_id != canonical
            or effect_receipt.content_sha256 != effect_record.content_sha256
            or effect_receipt.content_length != effect_record.content_length
            or effect_receipt.epoch_id != effect_record.epoch_id
        ):
            raise ValueError("verified receipt does not match canonical effect")
        if type(summary) is not str:
            raise TypeError("activity afterglow summary must be text")
        ensure_bounded_text(summary, "activity afterglow summary", max_bytes=4096)
        return self.set_field(
            "activity_afterglow",
            {"event_id": canonical, "summary": summary},
            source="effect",
            ttl=ttl,
            confidence=1.0,
            daily=False,
            owner=owner,
            source_event_id=canonical,
        )

    @staticmethod
    def _observer_event(
        row: Mapping[str, Any],
    ) -> tuple[str, str, str, datetime, Mapping[str, Any]]:
        """Validate one event envelope without traversing its payload."""

        required = {
            "schema_version",
            "event_id",
            "created_at",
            "kind",
            "source",
            "payload",
        }
        if set(row) != required:
            raise StateError("panel event envelope has invalid fields")
        if row["schema_version"] != EVENT_SCHEMA:
            raise _PanelObserverSchemaError("panel event schema is unsupported")
        event_id = _nonempty_text(row["event_id"], "panel event_id")
        kind = _nonempty_text(row["kind"], "panel event kind", max_bytes=128)
        source = _nonempty_text(row["source"], "panel event source", max_bytes=128)
        created_at = parse_time(row["created_at"])
        if not isinstance(row["payload"], Mapping):
            raise StateError("panel event payload is invalid")
        return event_id, kind, source, created_at, row["payload"]

    def _observer_projection_evidence(
        self,
        projections: Mapping[
            str, tuple[str, str, str, str, tuple[str, str, str, str | None]]
        ],
    ) -> dict[str, datetime]:
        """Return explicit delivered evidence keyed by canonical projection id."""

        event_path = getattr(getattr(self.bus, "events", None), "path", None)
        if event_path is None:
            raise StateError("panel event ledger path is unavailable")
        events: dict[str, tuple[str, str, datetime, Mapping[str, Any]]] = {}
        for row in _lock_free_jsonl_rows(Path(event_path)):
            event_id, kind, source, created_at, payload = self._observer_event(row)
            if event_id in events:
                raise StateError("panel event ledger has duplicate event_id")
            events[event_id] = (kind, source, created_at, payload)

        delivered: dict[str, datetime] = {}
        for event_id, (
            _canonical,
            projection_kind,
            projection_source,
            _status,
            projection_payload,
        ) in projections.items():
            event = events.get(event_id)
            if event is None:
                continue
            kind, source, created_at, payload = event
            if kind != projection_kind or source != projection_source:
                raise StateError("panel projection delivery evidence conflicts")
            event_payload = _observer_projection_payload_metadata(
                projection_kind, payload
            )
            if event_payload != projection_payload:
                raise StateError("panel projection delivery evidence conflicts")
            delivered[event_id] = created_at
        return delivered

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return content-free panel health evidence without touching state.

        This path intentionally avoids :meth:`snapshot`, rollover, migration,
        event-bus readers, and every mutation lock.  A missing panel state is
        the healthy empty case and returns no facts, allowing ``Observer`` to
        classify it as neutral.
        """

        if not self.path.exists():
            return ()
        try:
            raw = _lock_free_json_object(self.path)
            if raw is None:
                return ()
            if raw.get("schema_version") != PANEL_SCHEMA:
                raise _PanelObserverSchemaError("panel state schema is unsupported")
            # Validate only metadata.  In particular, do not call snapshot or
            # PanelValue.from_dict: field values may contain private payloads.
            state = _observer_state_metadata(raw)
            delivered = self._observer_projection_evidence(state["projections"])
        except Exception as exc:
            return (
                _panel_integrity_fact(
                    target_date=target_date,
                    now=now,
                    code=_panel_integrity_code(exc),
                ),
            )

        owner_epochs = state["owner_epochs"]
        projections = state["projections"]
        pending_count = sum(
            projection[3] == "pending" for projection in projections.values()
        )
        delivered_count = sum(
            projection[3] == "delivered" for projection in projections.values()
        )
        facts: list[ObservationFact] = [
            ObservationFact(
                key="panel:state",
                code="panel_state_observed",
                state="neutral",
                target_date=target_date,
                event_time=None,
                refs=tuple(
                    [state["schema_version"], state["epoch"]]
                    + [
                        f"{owner}:{epoch}"
                        for owner, epoch in sorted(owner_epochs.items())
                    ]
                ),
                counts={
                    "schema": 1,
                    "epoch": 1,
                    "owner_epoch": len(owner_epochs),
                    "fields": state["field_count"],
                    "projections": len(projections),
                    "pending_projections": pending_count,
                    "delivered_projections": delivered_count,
                },
            ),
            _panel_integrity_fact(
                target_date=target_date,
                now=now,
                code="panel_integrity_ok",
            ),
        ]
        for event_id, (_canonical, _kind, _source, status, _payload) in sorted(
            projections.items()
        ):
            if status == "pending":
                evidence_time = delivered.get(event_id)
                if evidence_time is None:
                    facts.append(
                        ObservationFact(
                            key=f"panel:projection:{event_id}",
                            code="panel_projection_pending",
                            state="current",
                            target_date=target_date,
                            event_time=None,
                            refs=(event_id,),
                            counts={"pending": 1},
                        )
                    )
                else:
                    facts.append(
                        ObservationFact(
                            key=f"panel:projection:{event_id}",
                            code="panel_projection_recovered",
                            state="recovered_history",
                            target_date=target_date,
                            event_time=evidence_time,
                            refs=(event_id,),
                            counts={"recovered": 1},
                            recovery=RecoveryEvidence(
                                ref=event_id,
                                code="panel_projection_delivered",
                                recovered_at=evidence_time,
                            ),
                        )
                    )
            elif status == "delivered":
                evidence_time = delivered.get(event_id)
                if evidence_time is None:
                    facts.append(
                        ObservationFact(
                            key=f"panel:projection:{event_id}",
                            code="panel_projection_delivery_evidence_missing",
                            state="current",
                            target_date=target_date,
                            event_time=None,
                            refs=(event_id,),
                            counts={"delivery_evidence_missing": 1},
                        )
                    )
                else:
                    facts.append(
                        ObservationFact(
                            key=f"panel:projection:{event_id}",
                            code="panel_projection_delivered",
                            state="neutral",
                            target_date=target_date,
                            event_time=evidence_time,
                            refs=(event_id,),
                            counts={"delivered": 1},
                        )
                    )
        return tuple(sorted(facts, key=lambda fact: (fact.key, fact.code)))

    def snapshot(
        self, *, now: datetime | None = None, owner: str | None = None
    ) -> dict[str, Any]:
        """Read a projected view without writing expiry or rollover changes."""

        effective_now = self.clock() if now is None else now
        owner_value = None if owner is None else _nonempty_text(owner, "panel owner")
        state = self._read_state(effective_now)
        visible = self._visible_records(state, now=effective_now, owner=owner_value)
        fields: dict[str, Any] = {}
        used: set[str] = set()
        for record in visible.values():
            fields[self._display_key(record, owner_filter=owner_value, used=used)] = (
                record.to_dict()
            )
        owner_reset_policies: dict[str, set[str]] = {}
        for record in self._parse_records(state).values():
            owner_reset_policies.setdefault(record.owner, set()).add(
                record.reset_policy
            )
        return {
            "schema_version": PANEL_SCHEMA,
            "epoch": epoch_id(
                effective_now,
                timezone_name=self.timezone_name,
                anchor_hour=self.anchor_hour,
            ),
            "owner_epochs": dict(state.get("owner_epochs", {})),
            "owner_reset_policies": {
                owner_name: sorted(policies)
                for owner_name, policies in owner_reset_policies.items()
            },
            "fields": fields,
        }

    def render_markdown(
        self, *, now: datetime | None = None, owner: str | None = None
    ) -> str:
        snapshot = self.snapshot(now=now, owner=owner)
        lines = [f"# Moonbite daily RAM — {snapshot['epoch']}"]
        if not snapshot["fields"]:
            lines.append("\n_No fresh fields._")
        else:
            for name, value in sorted(snapshot["fields"].items()):
                lines.append(f"\n- **{name}**: {value['value']}")
        return "\n".join(lines) + "\n"
