"""Pure, topology-neutral incident lifecycle and fingerprint contracts.

The module accepts only bounded logical metadata and hashes.  It has no
filesystem, clock, network, target, log, or allowlist knowledge.  A host
sidecar owns durable persistence and supplies a cursor to the pure decision
function.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
import json
import re
from typing import Any, ClassVar

from .runtime_core import StateError, isoformat, parse_time


INCIDENT_SCHEMA = "moon.incident.v1"
INCIDENT_PROJECTION_SCHEMA = "moon.incident.projection.v1"
INCIDENT_SUMMARY_SCHEMA = "moon.incident.summary.v1"
INCIDENT_SEVERITIES = frozenset({"info", "warning", "error", "critical"})
INCIDENT_STATES = frozenset({"active", "recovered", "neutral"})
INCIDENT_LIFECYCLES = frozenset({"new", "current", "recovered_history", "neutral"})
INCIDENT_DECISIONS = frozenset(
    {
        "new",
        "current",
        "recovered_history",
        "neutral",
        "duplicate",
        "out_of_order",
        "clock_skew",
        "scope_mismatch",
    }
)
DEFAULT_INCIDENT_MAX_CLOCK_SKEW = timedelta(minutes=5)
MAX_INCIDENT_SEEN_EVENT_IDS = 256
MAX_INCIDENT_CODES = 8
MAX_INCIDENT_AGGREGATION = 256
MAX_INCIDENT_SEQUENCE = (1 << 63) - 1
MAX_INCIDENT_EVENT_ID_BYTES = 128
MAX_INCIDENT_LOGICAL_BYTES = 64
_LOGICAL = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_EVENT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")
_SEVERITY_RANK = {"info": 0, "warning": 1, "error": 2, "critical": 3}
_PROJECTION_KEYS = frozenset(
    {
        "schema_version",
        "source",
        "category",
        "reason",
        "event_id",
        "observed_at",
        "received_at",
        "source_sequence",
        "fingerprint",
        "severity",
        "state",
        "lifecycle",
        "recovery_ref",
    }
)
_SUMMARY_KEYS = frozenset(
    {
        "schema_version",
        "total_count",
        "counts",
        "active_severity",
        "active_severity_counts",
        "projections",
    }
)


def _bounded_text(value: Any, label: str, *, max_bytes: int) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{label} must be a bounded non-empty string")
    if "\r" in value or "\n" in value:
        raise ValueError(f"{label} must not contain newlines")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} bytes")
    return value


def _logical(value: Any, label: str) -> str:
    result = _bounded_text(value, label, max_bytes=MAX_INCIDENT_LOGICAL_BYTES)
    if _LOGICAL.fullmatch(result) is None:
        raise ValueError(f"{label} must be a lower-case logical name")
    return result


def _event_id(value: Any, label: str = "event_id") -> str:
    result = _bounded_text(value, label, max_bytes=MAX_INCIDENT_EVENT_ID_BYTES)
    if _EVENT_ID.fullmatch(result) is None:
        raise ValueError(f"{label} must be a stable event identifier")
    return result


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise ValueError(f"{label} must be timezone-aware")
    except Exception as exc:  # pragma: no cover - hostile tzinfo
        raise ValueError(f"{label} must be timezone-aware") from exc
    return value


def _optional_aware(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _aware(value, label)


def _sequence(value: Any, label: str = "source_sequence") -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= MAX_INCIDENT_SEQUENCE:
        raise ValueError(f"{label} must be a bounded non-negative integer or null")
    return value


def _fingerprint(value: Any, label: str = "fingerprint") -> str:
    if type(value) is not str or _FINGERPRINT.fullmatch(value) is None:
        raise ValueError(f"{label} must be exactly lowercase SHA256 hex")
    return value


def _duration(value: Any, label: str) -> timedelta:
    if (
        not isinstance(value, timedelta)
        or value < timedelta(0)
        or value > timedelta(days=7)
    ):
        raise ValueError(f"{label} must be a timedelta from zero through seven days")
    return value


def fingerprint_codes(codes: Sequence[str]) -> str:
    """Hash one to eight bounded logical codes in canonical order."""

    if type(codes) not in (list, tuple) or not 1 <= len(codes) <= MAX_INCIDENT_CODES:
        raise ValueError("codes must be a list or tuple of one to eight items")
    normalized = tuple(
        _logical(code, f"codes[{index}]") for index, code in enumerate(codes)
    )
    payload = json.dumps(
        normalized,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class IncidentEvidence:
    """A content-free incident or explicit recovery fact."""

    event_id: str
    source: str
    category: str
    reason: str
    observed_at: datetime
    received_at: datetime
    fingerprint: str
    severity: str
    state: str = "active"
    source_sequence: int | None = None
    recovery_ref: str | None = None
    schema_version: ClassVar[str] = INCIDENT_SCHEMA

    def __post_init__(self) -> None:
        _event_id(self.event_id)
        _logical(self.source, "source")
        _logical(self.category, "category")
        _logical(self.reason, "reason")
        _aware(self.observed_at, "observed_at")
        _aware(self.received_at, "received_at")
        _fingerprint(self.fingerprint)
        _sequence(self.source_sequence)
        if type(self.severity) is not str or self.severity not in INCIDENT_SEVERITIES:
            raise ValueError("severity is unsupported")
        if type(self.state) is not str or self.state not in INCIDENT_STATES:
            raise ValueError("state is unsupported")
        if self.state == "recovered":
            if self.recovery_ref != self.fingerprint:
                raise ValueError(
                    "recovered evidence requires recovery_ref equal to fingerprint"
                )
        elif self.recovery_ref is not None:
            raise ValueError("only recovered evidence may carry recovery_ref")
        if self.recovery_ref is not None:
            _fingerprint(self.recovery_ref, "recovery_ref")

    @property
    def scope(self) -> tuple[str, str, str]:
        return (self.source, self.category, self.fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "source": self.source,
            "category": self.category,
            "reason": self.reason,
            "observed_at": isoformat(self.observed_at),
            "received_at": isoformat(self.received_at),
            "source_sequence": self.source_sequence,
            "fingerprint": self.fingerprint,
            "severity": self.severity,
            "state": self.state,
            "recovery_ref": self.recovery_ref,
        }


@dataclass(frozen=True, slots=True)
class IncidentCursor:
    """Caller-owned durable cursor for one source/category/fingerprint scope."""

    source: str
    category: str
    fingerprint: str
    last_event_id: str | None = None
    last_source_sequence: int | None = None
    last_observed_at: datetime | None = None
    last_received_at: datetime | None = None
    seen_event_ids: tuple[str, ...] = ()
    active_seen: bool = False
    recovered_at: datetime | None = None

    def __post_init__(self) -> None:
        _logical(self.source, "cursor source")
        _logical(self.category, "cursor category")
        _fingerprint(self.fingerprint, "cursor fingerprint")
        if self.last_event_id is not None:
            _event_id(self.last_event_id, "last_event_id")
        _sequence(self.last_source_sequence, "last_source_sequence")
        _optional_aware(self.last_observed_at, "last_observed_at")
        _optional_aware(self.last_received_at, "last_received_at")
        if type(self.seen_event_ids) not in (tuple, list):
            raise ValueError("seen_event_ids must be a tuple or list")
        seen = tuple(_event_id(item, "seen_event_id") for item in self.seen_event_ids)
        if len(seen) != len(set(seen)):
            raise ValueError("seen_event_ids must be unique")
        if len(seen) > MAX_INCIDENT_SEEN_EVENT_IDS:
            raise ValueError(
                f"seen_event_ids exceeds {MAX_INCIDENT_SEEN_EVENT_IDS} entries"
            )
        if type(self.active_seen) is not bool:
            raise ValueError("active_seen must be a bool")
        recovered_at = _optional_aware(self.recovered_at, "recovered_at")
        if recovered_at is not None and not self.active_seen:
            raise ValueError("recovered_at requires prior active evidence")
        if (
            recovered_at is not None
            and self.last_observed_at is not None
            and recovered_at > self.last_observed_at
        ):
            raise ValueError("recovered_at cannot follow the cursor observed time")
        object.__setattr__(self, "seen_event_ids", seen)
        object.__setattr__(self, "recovered_at", recovered_at)

    @classmethod
    def empty(cls, source: str, category: str, fingerprint: str) -> "IncidentCursor":
        return cls(source=source, category=category, fingerprint=fingerprint)

    @property
    def scope(self) -> tuple[str, str, str]:
        return (self.source, self.category, self.fingerprint)

    def contains(self, event_id: str) -> bool:
        return event_id == self.last_event_id or event_id in self.seen_event_ids

    def advance(
        self,
        evidence: IncidentEvidence,
        *,
        recovery_accepted: bool = False,
    ) -> "IncidentCursor":
        if evidence.scope != self.scope:
            raise ValueError("incident does not belong to cursor scope")
        if self.contains(evidence.event_id):
            raise ValueError("duplicate incident event cannot advance cursor")
        if (
            self.last_observed_at is not None
            and evidence.observed_at <= self.last_observed_at
        ):
            raise ValueError("out-of-order incident cannot advance cursor")
        if (
            self.last_received_at is not None
            and evidence.received_at < self.last_received_at
        ):
            raise ValueError("received incident time cannot rewind cursor")
        if (
            evidence.source_sequence is not None
            and self.last_source_sequence is not None
            and evidence.source_sequence <= self.last_source_sequence
        ):
            raise ValueError("incident sequence cannot rewind cursor")
        seen = list(self.seen_event_ids)
        if evidence.event_id not in seen:
            seen.append(evidence.event_id)
        if len(seen) > MAX_INCIDENT_SEEN_EVENT_IDS:
            seen = seen[-MAX_INCIDENT_SEEN_EVENT_IDS:]
        return replace(
            self,
            last_event_id=evidence.event_id,
            last_source_sequence=(
                evidence.source_sequence
                if evidence.source_sequence is not None
                else self.last_source_sequence
            ),
            last_observed_at=evidence.observed_at,
            last_received_at=evidence.received_at,
            seen_event_ids=tuple(seen),
            active_seen=(True if evidence.state == "active" else self.active_seen),
            recovered_at=(
                evidence.observed_at
                if recovery_accepted
                else (None if evidence.state == "active" else self.recovered_at)
            ),
        )


@dataclass(frozen=True, slots=True)
class IncidentProjection:
    """Exact content-free lifecycle projection."""

    source: str
    category: str
    reason: str
    event_id: str
    observed_at: datetime
    received_at: datetime
    source_sequence: int | None
    fingerprint: str
    severity: str
    state: str
    lifecycle: str
    recovery_ref: str | None = None
    schema_version: ClassVar[str] = INCIDENT_PROJECTION_SCHEMA

    def __post_init__(self) -> None:
        _logical(self.source, "projection source")
        _logical(self.category, "projection category")
        _logical(self.reason, "projection reason")
        _event_id(self.event_id, "projection event_id")
        _aware(self.observed_at, "projection observed_at")
        _aware(self.received_at, "projection received_at")
        _sequence(self.source_sequence, "projection source_sequence")
        _fingerprint(self.fingerprint, "projection fingerprint")
        if type(self.severity) is not str or self.severity not in INCIDENT_SEVERITIES:
            raise ValueError("projection severity is unsupported")
        if type(self.state) is not str or self.state not in INCIDENT_STATES:
            raise ValueError("projection state is unsupported")
        if type(self.lifecycle) is not str or self.lifecycle not in INCIDENT_LIFECYCLES:
            raise ValueError("projection lifecycle is unsupported")
        if self.lifecycle in {"new", "current"} and self.state != "active":
            raise ValueError("active lifecycle must have active state")
        if self.lifecycle == "recovered_history" and self.state != "recovered":
            raise ValueError("recovered lifecycle must have recovered state")
        if self.lifecycle == "neutral" and self.state != "neutral":
            raise ValueError("neutral lifecycle must have neutral state")
        if self.state == "recovered":
            if self.recovery_ref != self.fingerprint:
                raise ValueError("recovered projection requires recovery_ref")
        elif self.recovery_ref is not None:
            raise ValueError("only recovered projection may carry recovery_ref")
        if self.recovery_ref is not None:
            _fingerprint(self.recovery_ref, "projection recovery_ref")

    @property
    def scope(self) -> tuple[str, str, str]:
        return (self.source, self.category, self.fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source": self.source,
            "category": self.category,
            "reason": self.reason,
            "event_id": self.event_id,
            "observed_at": isoformat(self.observed_at),
            "received_at": isoformat(self.received_at),
            "source_sequence": self.source_sequence,
            "fingerprint": self.fingerprint,
            "severity": self.severity,
            "state": self.state,
            "lifecycle": self.lifecycle,
            "recovery_ref": self.recovery_ref,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IncidentProjection":
        if not isinstance(value, Mapping) or set(value) != _PROJECTION_KEYS:
            raise ValueError("incident projection keys are invalid")
        if value["schema_version"] != INCIDENT_PROJECTION_SCHEMA:
            raise ValueError("incident projection schema is unsupported")
        try:
            observed_at = parse_time(value["observed_at"])
            received_at = parse_time(value["received_at"])
        except (StateError, TypeError, ValueError) as exc:
            raise ValueError("incident projection timestamps are invalid") from exc
        return cls(
            source=value["source"],
            category=value["category"],
            reason=value["reason"],
            event_id=value["event_id"],
            observed_at=observed_at,
            received_at=received_at,
            source_sequence=value["source_sequence"],
            fingerprint=value["fingerprint"],
            severity=value["severity"],
            state=value["state"],
            lifecycle=value["lifecycle"],
            recovery_ref=value["recovery_ref"],
        )


@dataclass(frozen=True, slots=True)
class IncidentDecision:
    """Pure lifecycle decision and optional durable cursor advancement."""

    status: str
    reason_code: str
    evidence: IncidentEvidence
    next_cursor: IncidentCursor | None = None
    projected: IncidentProjection | None = None

    def __post_init__(self) -> None:
        if self.status not in INCIDENT_DECISIONS:
            raise ValueError("incident decision status is unsupported")
        _logical(self.reason_code, "decision reason_code")
        if not isinstance(self.evidence, IncidentEvidence):
            raise TypeError("evidence must be IncidentEvidence")
        if self.next_cursor is not None:
            if not isinstance(self.next_cursor, IncidentCursor):
                raise TypeError("next_cursor must be IncidentCursor or None")
            if self.next_cursor.scope != self.evidence.scope:
                raise ValueError("next_cursor does not belong to evidence scope")
        if self.projected is not None and not isinstance(
            self.projected, IncidentProjection
        ):
            raise TypeError("projected must be IncidentProjection or None")
        if self.projected is not None and self.projected.scope != self.evidence.scope:
            raise ValueError("decision projection does not belong to evidence scope")
        accepted = {"new", "current", "recovered_history", "neutral"}
        if self.status in accepted:
            if self.next_cursor is None or self.projected is None:
                raise ValueError(
                    f"{self.status} decisions require cursor and projection"
                )
            if self.projected.lifecycle != self.status:
                raise ValueError("decision projection does not match status")
        elif self.next_cursor is not None or self.projected is not None:
            raise ValueError("rejected decisions cannot advance or project")

    @property
    def projection(self) -> dict[str, Any] | None:
        return None if self.projected is None else self.projected.to_dict()

    @property
    def projection_model(self) -> IncidentProjection | None:
        return self.projected

    @property
    def active(self) -> bool:
        return self.status in {"new", "current"}

    @property
    def neutral(self) -> bool:
        return self.status == "neutral"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": INCIDENT_SCHEMA,
            "status": self.status,
            "reason_code": self.reason_code,
            "active": self.active,
            "neutral": self.neutral,
            "projection": self.projection,
        }


def _decision(
    status: str,
    reason_code: str,
    evidence: IncidentEvidence,
    cursor: IncidentCursor | None = None,
    projected: IncidentProjection | None = None,
) -> IncidentDecision:
    return IncidentDecision(
        status=status,
        reason_code=reason_code,
        evidence=evidence,
        next_cursor=cursor,
        projected=projected,
    )


def _project(
    evidence: IncidentEvidence,
    *,
    lifecycle: str,
    state: str,
) -> IncidentProjection:
    return IncidentProjection(
        source=evidence.source,
        category=evidence.category,
        reason=evidence.reason,
        event_id=evidence.event_id,
        observed_at=evidence.observed_at,
        received_at=evidence.received_at,
        source_sequence=evidence.source_sequence,
        fingerprint=evidence.fingerprint,
        severity=evidence.severity,
        state=state,
        lifecycle=lifecycle,
        recovery_ref=(evidence.recovery_ref if state == "recovered" else None),
    )


def decide_incident(
    evidence: IncidentEvidence,
    *,
    now: datetime,
    cursor: IncidentCursor | None = None,
    max_clock_skew: timedelta = DEFAULT_INCIDENT_MAX_CLOCK_SKEW,
) -> IncidentDecision:
    """Classify one incident fact without retaining state or performing I/O."""

    if not isinstance(evidence, IncidentEvidence):
        raise TypeError("evidence must be IncidentEvidence")
    now = _aware(now, "now")
    max_clock_skew = _duration(max_clock_skew, "max_clock_skew")
    actual_cursor = IncidentCursor.empty(*evidence.scope) if cursor is None else cursor
    if not isinstance(actual_cursor, IncidentCursor):
        raise TypeError("cursor must be IncidentCursor or None")
    if actual_cursor.scope != evidence.scope:
        return _decision("scope_mismatch", "cursor_scope_mismatch", evidence)

    if actual_cursor.contains(evidence.event_id):
        return _decision("duplicate", "duplicate_event", evidence)

    if evidence.observed_at > now + max_clock_skew:
        return _decision("clock_skew", "observed_time_in_future", evidence)
    if evidence.received_at > now + max_clock_skew:
        return _decision("clock_skew", "received_time_in_future", evidence)
    if evidence.observed_at > evidence.received_at + max_clock_skew:
        return _decision("clock_skew", "sender_time_ahead", evidence)
    if (
        actual_cursor.last_received_at is not None
        and evidence.received_at < actual_cursor.last_received_at - max_clock_skew
    ):
        return _decision("clock_skew", "received_time_rewind", evidence)
    if (
        actual_cursor.last_received_at is not None
        and evidence.received_at < actual_cursor.last_received_at
    ):
        return _decision("out_of_order", "received_time_rewind", evidence)
    if (
        evidence.source_sequence is not None
        and actual_cursor.last_source_sequence is not None
        and evidence.source_sequence <= actual_cursor.last_source_sequence
    ):
        return _decision("out_of_order", "source_sequence_rewind", evidence)
    if (
        actual_cursor.last_observed_at is not None
        and evidence.observed_at <= actual_cursor.last_observed_at
    ):
        return _decision("out_of_order", "observed_time_rewind", evidence)

    if evidence.state == "active":
        lifecycle = (
            "new"
            if not actual_cursor.active_seen or actual_cursor.recovered_at is not None
            else "current"
        )
        next_cursor = actual_cursor.advance(evidence)
        return _decision(
            lifecycle,
            lifecycle,
            evidence,
            next_cursor,
            _project(evidence, lifecycle=lifecycle, state="active"),
        )

    if evidence.state == "recovered":
        if not actual_cursor.active_seen:
            reason = "orphan_recovery"
        elif actual_cursor.recovered_at is not None:
            reason = "stale_recovery"
        elif evidence.recovery_ref != actual_cursor.fingerprint:
            reason = "recovery_scope_mismatch"
        else:
            next_cursor = actual_cursor.advance(
                evidence,
                recovery_accepted=True,
            )
            return _decision(
                "recovered_history",
                "recovered_history",
                evidence,
                next_cursor,
                _project(
                    evidence,
                    lifecycle="recovered_history",
                    state="recovered",
                ),
            )
        next_cursor = actual_cursor.advance(evidence)
        return _decision(
            "neutral",
            reason,
            evidence,
            next_cursor,
            _project(evidence, lifecycle="neutral", state="neutral"),
        )

    next_cursor = actual_cursor.advance(evidence)
    return _decision(
        "neutral",
        "neutral",
        evidence,
        next_cursor,
        _project(evidence, lifecycle="neutral", state="neutral"),
    )


def _coerce_projection(
    value: IncidentProjection | Mapping[str, Any],
) -> IncidentProjection:
    if isinstance(value, IncidentProjection):
        return value
    if isinstance(value, Mapping):
        return IncidentProjection.from_dict(value)
    raise TypeError("aggregation accepts IncidentProjection values only")


def _projection_order(value: IncidentProjection) -> tuple[Any, ...]:
    return (
        value.observed_at,
        value.received_at,
        -1 if value.source_sequence is None else value.source_sequence,
        value.event_id,
    )


def aggregate_incidents(
    projections: Iterable[IncidentProjection | Mapping[str, Any]],
) -> dict[str, Any]:
    """Deduplicate lifecycle projections into a stable pure summary.

    A neutral projection is only a fallback for a scope with no non-neutral
    lifecycle evidence.  It must not erase an active or recovered history
    projection merely because its timestamp is newer.
    """

    latest_non_neutral: dict[tuple[str, str, str], IncidentProjection] = {}
    latest_neutral: dict[tuple[str, str, str], IncidentProjection] = {}
    count = 0
    for value in projections:
        count += 1
        if count > MAX_INCIDENT_AGGREGATION:
            raise ValueError(f"projections exceeds {MAX_INCIDENT_AGGREGATION} entries")
        projection = _coerce_projection(value)
        latest = (
            latest_neutral if projection.lifecycle == "neutral" else latest_non_neutral
        )
        previous = latest.get(projection.scope)
        if previous is None or _projection_order(projection) > _projection_order(
            previous
        ):
            latest[projection.scope] = projection

    selected_by_scope = dict(latest_neutral)
    selected_by_scope.update(latest_non_neutral)
    selected = sorted(
        selected_by_scope.values(),
        key=lambda item: (item.source, item.category, item.fingerprint),
    )
    counts = {name: 0 for name in ("new", "current", "recovered_history", "neutral")}
    active_severity_counts = {
        name: 0 for name in ("info", "warning", "error", "critical")
    }
    active_severity: str | None = None
    for projection in selected:
        counts[projection.lifecycle] += 1
        if projection.lifecycle in {"new", "current"}:
            active_severity_counts[projection.severity] += 1
            if (
                active_severity is None
                or _SEVERITY_RANK[projection.severity] > _SEVERITY_RANK[active_severity]
            ):
                active_severity = projection.severity
    return {
        "schema_version": INCIDENT_SUMMARY_SCHEMA,
        "total_count": len(selected),
        "counts": counts,
        "active_severity": active_severity,
        "active_severity_counts": active_severity_counts,
        "projections": [item.to_dict() for item in selected],
    }


__all__ = [
    "DEFAULT_INCIDENT_MAX_CLOCK_SKEW",
    "INCIDENT_DECISIONS",
    "INCIDENT_LIFECYCLES",
    "INCIDENT_PROJECTION_SCHEMA",
    "INCIDENT_SCHEMA",
    "INCIDENT_SEVERITIES",
    "INCIDENT_STATES",
    "INCIDENT_SUMMARY_SCHEMA",
    "IncidentCursor",
    "IncidentDecision",
    "IncidentEvidence",
    "IncidentProjection",
    "MAX_INCIDENT_AGGREGATION",
    "MAX_INCIDENT_CODES",
    "MAX_INCIDENT_SEEN_EVENT_IDS",
    "aggregate_incidents",
    "decide_incident",
    "fingerprint_codes",
]
