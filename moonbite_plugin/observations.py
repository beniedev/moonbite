"""Topology-neutral observation evidence and replay decisions.

This module is deliberately a pure contract. It does not read a source,
remember observations between calls, write an event, or project a panel
field. A host adapter supplies a durable :class:`ObservationCursor` and may
persist the returned cursor after it accepts evidence. Degraded evidence is
accepted for replay/health accounting but never becomes a user-state
candidate.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import math
import re
from typing import Any, ClassVar

from .runtime_core import isoformat


OBSERVATION_SCHEMA = "moon.observation.v1"
OBSERVATION_PROJECTION_SCHEMA = "moon.observation.projection.v1"
OBSERVATION_AVAILABILITIES = frozenset(
    {"available", "disabled", "not_configured", "offline", "error", "unknown"}
)
OBSERVATION_FRESHNESSES = frozenset({"fresh", "delayed", "stale", "unknown"})
OBSERVATION_DECISIONS = frozenset(
    {
        "candidate",
        "degraded",
        "neutral",
        "duplicate",
        "out_of_order",
        "clock_skew",
        "expired",
        "sender_clock_jump",
    }
)

DEFAULT_MAX_CLOCK_SKEW = timedelta(minutes=5)
DEFAULT_MAX_SENDER_CLOCK_JUMP = timedelta(hours=24)
MAX_EVENT_ID_BYTES = 256
MAX_REASON_CODE_BYTES = 64
MAX_LOGICAL_NAME_BYTES = 64
MAX_CURSOR_EVENT_IDS = 256
_LOGICAL_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_REASON_CODE = _LOGICAL_NAME


def _bounded_text(value: Any, label: str, *, max_bytes: int) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    if value != value.strip() or "\r" in value or "\n" in value:
        raise ValueError(f"{label} must not contain surrounding whitespace or newlines")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} bytes")
    return value


def _logical_name(value: Any, label: str) -> str:
    result = _bounded_text(value, label, max_bytes=MAX_LOGICAL_NAME_BYTES)
    if _LOGICAL_NAME.fullmatch(result) is None:
        raise ValueError(f"{label} must be a lower-case logical name")
    return result


def _reason_code(value: Any, label: str = "reason_code") -> str:
    result = _bounded_text(value, label, max_bytes=MAX_REASON_CODE_BYTES)
    if _REASON_CODE.fullmatch(result) is None:
        raise ValueError(f"{label} must be a bounded reason code")
    return result


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception as exc:  # pragma: no cover - hostile tzinfo implementation
        raise ValueError(f"{label} must be timezone-aware") from exc
    if offset is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value


def _optional_aware(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _aware(value, label)


def _sequence(value: Any, label: str = "source_sequence") -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer or null")
    return value


def _confidence(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("confidence must be a number from 0 to 1")
    result = float(value)
    if not math.isfinite(result) or not 0 <= result <= 1:
        raise ValueError("confidence must be a number from 0 to 1")
    return result


def _duration(value: Any, label: str) -> timedelta:
    if not isinstance(value, timedelta) or value < timedelta(0):
        raise ValueError(f"{label} must be a non-negative timedelta")
    return value


@dataclass(frozen=True, slots=True)
class ObservationEvidence:
    """A content-free fact supplied by a deployment adapter."""

    event_id: str
    source: str
    capability: str
    observed_at: datetime
    received_at: datetime
    expires_at: datetime | None
    availability: str
    freshness: str
    confidence: float
    source_sequence: int | None = None
    reason_code: str = "observed"
    schema_version: ClassVar[str] = OBSERVATION_SCHEMA

    def __post_init__(self) -> None:
        _bounded_text(self.event_id, "event_id", max_bytes=MAX_EVENT_ID_BYTES)
        _logical_name(self.source, "source")
        _logical_name(self.capability, "capability")
        observed_at = _aware(self.observed_at, "observed_at")
        received_at = _aware(self.received_at, "received_at")
        expires_at = _optional_aware(self.expires_at, "expires_at")
        if expires_at is not None and expires_at <= observed_at:
            raise ValueError("expires_at must follow observed_at")
        if self.availability not in OBSERVATION_AVAILABILITIES:
            raise ValueError("availability is unsupported")
        if self.freshness not in OBSERVATION_FRESHNESSES:
            raise ValueError("freshness is unsupported")
        if self.freshness == "fresh" and self.availability != "available":
            raise ValueError("only available observations may be fresh")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "confidence", _confidence(self.confidence))
        object.__setattr__(self, "source_sequence", _sequence(self.source_sequence))
        object.__setattr__(self, "reason_code", _reason_code(self.reason_code))

    @property
    def logical_source(self) -> str:
        return self.source

    def to_projection_payload(self) -> dict[str, Any]:
        """Return EventEnvelope/Panel metadata without source material."""

        return {
            "schema_version": OBSERVATION_PROJECTION_SCHEMA,
            "event_id": self.event_id,
            "source": self.source,
            "capability": self.capability,
            "source_sequence": self.source_sequence,
            "observed_at": isoformat(self.observed_at),
            "received_at": isoformat(self.received_at),
            "expires_at": (
                None if self.expires_at is None else isoformat(self.expires_at)
            ),
            "availability": self.availability,
            "freshness": self.freshness,
            "confidence": self.confidence,
            "reason_code": self.reason_code,
        }

    def to_event_payload(self) -> dict[str, Any]:
        return self.to_projection_payload()

    def to_dict(self) -> dict[str, Any]:
        """Return the evidence contract with its canonical schema version."""

        payload = self.to_projection_payload()
        payload["schema_version"] = OBSERVATION_SCHEMA
        return payload


@dataclass(frozen=True, slots=True)
class ObservationCursor:
    """Caller-owned durable replay/order cursor for one logical stream."""

    source: str
    capability: str
    last_event_id: str | None = None
    last_source_sequence: int | None = None
    last_observed_at: datetime | None = None
    last_received_at: datetime | None = None
    seen_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _logical_name(self.source, "cursor source")
        _logical_name(self.capability, "cursor capability")
        if self.last_event_id is not None:
            _bounded_text(
                self.last_event_id, "last_event_id", max_bytes=MAX_EVENT_ID_BYTES
            )
        _sequence(self.last_source_sequence, "last_source_sequence")
        _optional_aware(self.last_observed_at, "last_observed_at")
        _optional_aware(self.last_received_at, "last_received_at")
        if type(self.seen_event_ids) not in (tuple, list):
            raise ValueError("seen_event_ids must be a tuple or list")
        seen = tuple(
            _bounded_text(item, "seen_event_id", max_bytes=MAX_EVENT_ID_BYTES)
            for item in self.seen_event_ids
        )
        if len(set(seen)) != len(seen):
            raise ValueError("seen_event_ids must be unique")
        if len(seen) > MAX_CURSOR_EVENT_IDS:
            raise ValueError(f"seen_event_ids exceeds {MAX_CURSOR_EVENT_IDS} entries")
        object.__setattr__(self, "seen_event_ids", seen)

    @classmethod
    def empty(cls, source: str, capability: str) -> "ObservationCursor":
        return cls(source=source, capability=capability)

    def contains(self, event_id: str) -> bool:
        return event_id == self.last_event_id or event_id in self.seen_event_ids

    def advance(self, evidence: ObservationEvidence) -> "ObservationCursor":
        if (evidence.source, evidence.capability) != (self.source, self.capability):
            raise ValueError("observation does not belong to cursor stream")
        seen = list(self.seen_event_ids)
        if evidence.event_id not in seen:
            seen.append(evidence.event_id)
        last_observed_at = evidence.observed_at
        if self.last_observed_at is not None:
            last_observed_at = max(self.last_observed_at, evidence.observed_at)
        last_received_at = evidence.received_at
        if self.last_received_at is not None:
            last_received_at = max(self.last_received_at, evidence.received_at)
        if len(seen) > MAX_CURSOR_EVENT_IDS:
            seen = seen[-MAX_CURSOR_EVENT_IDS:]
        return replace(
            self,
            last_event_id=evidence.event_id,
            last_source_sequence=(
                evidence.source_sequence
                if evidence.source_sequence is not None
                else self.last_source_sequence
            ),
            last_observed_at=last_observed_at,
            last_received_at=last_received_at,
            seen_event_ids=tuple(seen),
        )


@dataclass(frozen=True, slots=True)
class ObservationDecision:
    """Pure result of evaluating one fact against a supplied cursor."""

    status: str
    reason_code: str
    evidence: ObservationEvidence
    next_cursor: ObservationCursor | None = None

    def __post_init__(self) -> None:
        if self.status not in OBSERVATION_DECISIONS:
            raise ValueError("observation decision status is unsupported")
        _reason_code(self.reason_code)
        if not isinstance(self.evidence, ObservationEvidence):
            raise TypeError("evidence must be ObservationEvidence")
        if self.next_cursor is not None:
            if not isinstance(self.next_cursor, ObservationCursor):
                raise TypeError("next_cursor must be ObservationCursor or None")
            if (
                self.next_cursor.source != self.evidence.source
                or self.next_cursor.capability != self.evidence.capability
            ):
                raise ValueError("next_cursor does not belong to evidence stream")
        if self.status in {"candidate", "degraded"} and self.next_cursor is None:
            raise ValueError(f"{self.status} decisions require next_cursor")
        if (
            self.status not in {"candidate", "degraded"}
            and self.next_cursor is not None
        ):
            raise ValueError("non-accepted decisions cannot advance the cursor")

    @property
    def projectable(self) -> bool:
        return self.status == "candidate"

    @property
    def candidate(self) -> bool:
        return self.projectable

    @property
    def degraded(self) -> bool:
        return self.status == "degraded"

    @property
    def health_projectable(self) -> bool:
        """Whether degraded metadata may feed an independent health surface."""

        return self.degraded

    @property
    def neutral(self) -> bool:
        return self.status == "neutral"

    @property
    def projection(self) -> dict[str, Any] | None:
        return self.evidence.to_projection_payload() if self.projectable else None

    @property
    def health_projection(self) -> dict[str, Any] | None:
        """Expose degraded metadata without making it user-state candidate data."""

        return (
            self.evidence.to_projection_payload() if self.health_projectable else None
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OBSERVATION_SCHEMA,
            "status": self.status,
            "reason_code": self.reason_code,
            "projectable": self.projectable,
            "projection": self.projection,
            "health_projectable": self.health_projectable,
            "health_projection": self.health_projection,
        }


def _decision(
    status: str,
    reason_code: str,
    evidence: ObservationEvidence,
    cursor: ObservationCursor | None = None,
) -> ObservationDecision:
    return ObservationDecision(
        status=status,
        reason_code=reason_code,
        evidence=evidence,
        next_cursor=cursor,
    )


def decide_observation(
    evidence: ObservationEvidence,
    *,
    now: datetime,
    cursor: ObservationCursor | None = None,
    max_clock_skew: timedelta = DEFAULT_MAX_CLOCK_SKEW,
    max_sender_clock_jump: timedelta = DEFAULT_MAX_SENDER_CLOCK_JUMP,
) -> ObservationDecision:
    """Evaluate evidence without retaining state or performing any I/O."""

    if not isinstance(evidence, ObservationEvidence):
        raise TypeError("evidence must be ObservationEvidence")
    now = _aware(now, "now")
    max_clock_skew = _duration(max_clock_skew, "max_clock_skew")
    max_sender_clock_jump = _duration(max_sender_clock_jump, "max_sender_clock_jump")
    actual_cursor = (
        ObservationCursor.empty(evidence.source, evidence.capability)
        if cursor is None
        else cursor
    )
    if not isinstance(actual_cursor, ObservationCursor):
        raise TypeError("cursor must be ObservationCursor or None")
    if (actual_cursor.source, actual_cursor.capability) != (
        evidence.source,
        evidence.capability,
    ):
        return _decision("out_of_order", "cursor_scope_mismatch", evidence)

    # Check duplicates first so the same fact remains a duplicate after expiry.
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
        evidence.source_sequence is not None
        and actual_cursor.last_source_sequence is not None
        and evidence.source_sequence <= actual_cursor.last_source_sequence
    ):
        return _decision("out_of_order", "source_sequence_rewind", evidence)
    if (
        evidence.source_sequence is None
        and actual_cursor.last_observed_at is not None
        and evidence.observed_at < actual_cursor.last_observed_at
    ):
        return _decision("out_of_order", "observed_time_rewind", evidence)

    if (
        actual_cursor.last_observed_at is not None
        and actual_cursor.last_received_at is not None
    ):
        sender_delta = evidence.observed_at - actual_cursor.last_observed_at
        receiver_delta = evidence.received_at - actual_cursor.last_received_at
        if abs(sender_delta - receiver_delta) > max_sender_clock_jump:
            return _decision("sender_clock_jump", "sender_clock_jump", evidence)

    if evidence.expires_at is not None and now >= evidence.expires_at:
        return _decision("expired", "expired", evidence)

    if evidence.availability in {"disabled", "not_configured"}:
        return _decision("neutral", evidence.availability, evidence)

    next_cursor = actual_cursor.advance(evidence)
    if evidence.availability != "available":
        return _decision("degraded", evidence.availability, evidence, next_cursor)
    if evidence.freshness != "fresh":
        return _decision("degraded", evidence.freshness, evidence, next_cursor)
    if evidence.confidence <= 0:
        return _decision("degraded", "confidence_zero", evidence, next_cursor)

    return _decision("candidate", "candidate", evidence, next_cursor)


__all__ = [
    "DEFAULT_MAX_CLOCK_SKEW",
    "DEFAULT_MAX_SENDER_CLOCK_JUMP",
    "MAX_CURSOR_EVENT_IDS",
    "OBSERVATION_AVAILABILITIES",
    "OBSERVATION_DECISIONS",
    "OBSERVATION_FRESHNESSES",
    "OBSERVATION_PROJECTION_SCHEMA",
    "OBSERVATION_SCHEMA",
    "ObservationCursor",
    "ObservationDecision",
    "ObservationEvidence",
    "decide_observation",
]
