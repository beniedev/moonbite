"""Host-neutral memory orchestration primitives for MB-50.

This module is deliberately a port layer.  A retriever can return opaque
references and bounded metadata, while an opener is called only after a
reference has been selected.  The durable part of the module is an
append-only exposure ledger: it records the reference and evidence
descriptor, never source material.  Memory records and writer effects are
delegated to their injected stores.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

from .effects import EffectLedger, EffectReceipt, EffectRecord, _valid_transition
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    JsonlLedger,
    StateError,
    as_utc,
    ensure_bounded_json,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)


ORCHESTRATION_SCHEMA = "moon.memory.orchestration.v1"
EXPOSURE_EVENTS = ("selected", "opened", "exposed", "used", "consumed")
EXPOSURE_STATES = frozenset((*EXPOSURE_EVENTS, "failed_to_open"))
_EXPOSURE_TRANSITIONS = {
    "selected": frozenset({"opened", "failed_to_open"}),
    "opened": frozenset({"exposed"}),
    "exposed": frozenset({"used"}),
    "used": frozenset({"consumed"}),
    "consumed": frozenset(),
    "failed_to_open": frozenset(),
}
WRITER_OPERATIONS = frozenset(
    {"turn_persistence", "flush", "diary", "consolidation", "maintenance"}
)

_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REFERENCE_BYTES = 1_024
_MAX_CLASS_BYTES = 256
_MAX_EVENT_ID_BYTES = 512
_MAX_METADATA_BYTES = 16 * 1024
_MAX_SOURCE_BYTES = 64 * 1024
_MAX_FAILURE_BYTES = 256
_MAX_REASON_BYTES = 4 * 1024
_PRIVATE_SOURCE_CLASSES = frozenset(
    {
        "private",
        "private_continuity",
        "self_brief",
        "identity",
        "relationship",
        "personal",
        "continuity",
    }
)
_SELF_BRIEF_CLASSES = frozenset(
    {"self_brief", "identity", "relationship", "private_continuity"}
)
_LEDGER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "event",
        "state",
        "exposure_id",
        "session_id",
        "lifecycle_id",
        "turn_id",
        "context_source_kind",
        "source_ref",
        "source_class",
        "source_event_time",
        "source_created_at",
        "source_expires_at",
        "content_sha256",
        "content_length",
        "created_at",
        "reply_use_id",
        "reply_content_sha256",
        "reply_content_length",
        "failure_code",
    }
)

_WRITER_CURRENT_STATES = frozenset(
    {"pending", "executed_unverified", "expired", "failed", "requeued"}
)
_OBSERVATION_STATE_RANK = {"neutral": 0, "recovered_history": 1, "current": 2}


def _observer_validate_context(target_date: date, now: datetime) -> datetime:
    """Validate observer inputs without consulting any durable owner."""

    if type(target_date) is not date:
        raise ValueError("target_date must be a date")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    try:
        effective = as_utc(now)
    except ValueError as exc:
        raise ValueError("now must be timezone-aware") from exc
    return effective


def _observer_jsonl_rows(path: Any) -> tuple[Mapping[str, Any], ...]:
    """Read an owner ledger directly, without creating or taking a lock.

    Observer probes intentionally avoid ``JsonlLedger.rows``.  That method
    acquires (and, on a pristine path, creates) a sibling lock file.  This
    helper only reads existing JSONL bytes and validates the outer object
    envelope; callers decide which content-free fields are safe to inspect.
    """

    if path is None:
        return ()
    path = Path(path)
    if not path.exists():
        return ()
    rows: list[Mapping[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for row_number, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise StateError(
                        f"observer ledger row {row_number} is not valid JSON"
                    ) from exc
                if not isinstance(value, Mapping):
                    raise StateError(
                        f"observer ledger row {row_number} is not an object"
                    )
                rows.append(value)
    except OSError as exc:
        raise StateError("observer ledger is unreadable") from exc
    return tuple(rows)


def _observer_integrity_fact(
    owner: str, *, target_date: date, code: str | None = None
) -> ObservationFact:
    """Return a redacted current integrity fact for malformed owner state."""

    return ObservationFact(
        key=f"memory.{owner}.integrity",
        code=code or f"{owner}_ledger_corrupt",
        state="current",
        target_date=target_date,
        refs=(owner,),
        counts={"integrity_errors": 1},
    )


def _observer_refs(*values: Any) -> tuple[str, ...]:
    """Keep only already validated, content-free reference strings."""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if type(value) is not str or not value.strip() or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _observer_merge_facts(
    facts: Iterable[ObservationFact],
) -> tuple[ObservationFact, ...]:
    """Dedupe an owner aggregate before the shared Observer sees it."""

    selected: dict[str, ObservationFact] = {}
    for fact in facts:
        previous = selected.get(fact.key)
        if previous is None:
            selected[fact.key] = fact
            continue
        candidate_rank = _OBSERVATION_STATE_RANK[fact.state]
        previous_rank = _OBSERVATION_STATE_RANK[previous.state]
        if candidate_rank > previous_rank or (
            candidate_rank == previous_rank
            and (fact.code, fact.refs, tuple(fact.counts.items()))
            < (previous.code, previous.refs, tuple(previous.counts.items()))
        ):
            selected[fact.key] = fact
    return tuple(
        sorted(
            selected.values(),
            key=lambda fact: (
                fact.key,
                -_OBSERVATION_STATE_RANK[fact.state],
                fact.code,
            ),
        )
    )


class OrchestrationError(RuntimeError):
    """Base error for unsafe orchestration operations."""


class MissingEvidenceError(OrchestrationError):
    """The selected opaque reference could not be opened."""


class ExpiredEvidenceError(OrchestrationError):
    """The source evidence is outside its declared freshness window."""


class PolicyDeniedError(PermissionError):
    """A source class was not authorized for the current context."""


class ExposureConflictError(StateError):
    """A replay/idempotency key attempted to change immutable identity."""


def _text(value: Any, label: str, *, limit: int = _MAX_REFERENCE_BYTES) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    result = value.strip()
    ensure_bounded_text(result, label, max_bytes=limit)
    return result


def _time(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        try:
            return as_utc(value)
        except ValueError as exc:
            raise ValueError(f"{label} must be timezone-aware") from exc
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    if type(value) is str:
        try:
            return parse_time(value)
        except StateError:
            try:
                return datetime.combine(
                    date.fromisoformat(value), datetime.min.time(), tzinfo=UTC
                )
            except ValueError as exc:
                raise ValueError(f"{label} must be an ISO date or timestamp") from exc
    raise ValueError(f"{label} must be a datetime or ISO date/timestamp")


def _optional_time(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _time(value, label)


def _hash(value: Any, label: str = "content_sha256") -> str:
    if type(value) is not str or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be exactly 64 lowercase hex characters")
    return value


def _length(value: Any, label: str = "content_length") -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("source metadata must be an object")

    def check(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, child in node.items():
                if type(key) is str and key.lower() in {
                    "body",
                    "content",
                    "text",
                    "raw",
                    "transcript",
                }:
                    raise ValueError("source metadata may not contain source material")
                check(child)
        elif isinstance(node, list):
            for child in node:
                check(child)

    check(value)
    ensure_bounded_json(dict(value), "source metadata", max_bytes=_MAX_METADATA_BYTES)
    return dict(value)


def _content_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, bytearray):
        payload = bytes(value)
    elif isinstance(value, str):
        payload = value.encode("utf-8")
    elif isinstance(value, Mapping) or isinstance(
        value, (list, tuple, int, float, bool)
    ):
        try:
            payload = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("writer content must be serializable") from exc
    elif hasattr(value, "to_dict") and callable(value.to_dict):
        return _content_bytes(value.to_dict())
    else:
        raise TypeError("writer content must be bytes, text, or JSON data")
    if not payload:
        raise ValueError("writer content must not be empty")
    return payload


def content_descriptor(value: Any) -> tuple[str, int]:
    """Return the only content information persisted by this module."""

    payload = _content_bytes(value)
    return hashlib.sha256(payload).hexdigest(), len(payload)


class SourceRetriever(Protocol):
    """Host-owned opaque candidate search port."""

    def retrieve(self, query: str, *, limit: int) -> Iterable["SourceCandidate"]: ...


class SourceOpener(Protocol):
    """Host-owned exact opener port for one selected opaque reference."""

    def open(self, source_ref: str, *, max_bytes: int) -> "SourceMaterial | None": ...


@dataclass(frozen=True, slots=True)
class SourceCandidate:
    """Opaque retriever output; it intentionally has no source body field."""

    source_ref: str
    source_class: str
    source_event_time: datetime
    created_at: datetime
    expires_at: datetime | None = None
    content_sha256: str | None = None
    content_length: int | None = None
    relevance: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_ref", _text(self.source_ref, "source_ref"))
        object.__setattr__(
            self,
            "source_class",
            _text(self.source_class, "source_class", limit=_MAX_CLASS_BYTES),
        )
        object.__setattr__(
            self,
            "source_event_time",
            _time(self.source_event_time, "source_event_time"),
        )
        object.__setattr__(
            self, "created_at", _time(self.created_at, "source created_at")
        )
        expires = _optional_time(self.expires_at, "source expires_at")
        if expires is not None and expires <= self.created_at:
            raise ValueError("source expires_at must be later than source created_at")
        object.__setattr__(self, "expires_at", expires)
        if (self.content_sha256 is None) != (self.content_length is None):
            raise ValueError("source content hash and length must be supplied together")
        if self.content_sha256 is not None:
            _hash(self.content_sha256)
            _length(self.content_length)
        if type(self.relevance) not in (int, float) or not math.isfinite(
            float(self.relevance)
        ):
            raise ValueError("source relevance must be finite")
        object.__setattr__(self, "metadata", _metadata(self.metadata))

    @property
    def source_kind(self) -> str:
        """Compatibility spelling for adapters that call classes kinds."""

        return self.source_class

    @property
    def event_time(self) -> datetime:
        return self.source_event_time

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceCandidate":
        if not isinstance(value, Mapping):
            raise ValueError("retriever candidate must be an object")
        if any(
            isinstance(key, str)
            and key.lower() in {"body", "content", "text", "raw", "transcript"}
            for key in value
        ):
            raise ValueError("retriever output may not contain source material")
        try:
            return cls(
                source_ref=value.get("source_ref", value.get("open_ref")),
                source_class=value.get("source_class", value.get("source_kind")),
                source_event_time=value.get(
                    "source_event_time", value.get("event_time")
                ),
                created_at=value["created_at"],
                expires_at=value.get("expires_at"),
                content_sha256=value.get("content_sha256", value.get("content_hash")),
                content_length=value.get("content_length"),
                relevance=value.get("relevance", value.get("score", 0.0)),
                metadata=value.get("metadata", {}),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("retriever candidate is invalid") from exc


@dataclass(frozen=True, slots=True)
class SourceMaterial:
    """Bounded transient result of an exact open; never written to a ledger."""

    source_ref: str
    source_class: str
    source_event_time: datetime
    created_at: datetime
    body: str | bytes
    expires_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    framing: str = "current"
    framing_date: date | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_ref", _text(self.source_ref, "source_ref"))
        object.__setattr__(
            self,
            "source_class",
            _text(self.source_class, "source_class", limit=_MAX_CLASS_BYTES),
        )
        object.__setattr__(
            self,
            "source_event_time",
            _time(self.source_event_time, "source_event_time"),
        )
        object.__setattr__(
            self, "created_at", _time(self.created_at, "source created_at")
        )
        expires = _optional_time(self.expires_at, "source expires_at")
        if expires is not None and expires <= self.created_at:
            raise ValueError("source expires_at must be later than source created_at")
        object.__setattr__(self, "expires_at", expires)
        if not isinstance(self.body, (str, bytes)):
            raise TypeError("source body must be text or bytes")
        body_bytes = _content_bytes(self.body)
        if not body_bytes:
            raise ValueError("source body must not be empty")
        if len(body_bytes) > _MAX_SOURCE_BYTES:
            raise ValueError(f"source body exceeds {_MAX_SOURCE_BYTES} UTF-8 bytes")
        object.__setattr__(self, "metadata", _metadata(self.metadata))
        if self.framing not in {"current", "historical"}:
            raise ValueError("source framing must be current or historical")
        frame_date = self.framing_date
        if frame_date is None:
            frame_date = self.source_event_time.date()
        elif isinstance(frame_date, datetime):
            frame_date = _time(frame_date, "framing_date").date()
        elif not isinstance(frame_date, date):
            raise ValueError("framing_date must be a date")
        object.__setattr__(self, "framing_date", frame_date)

    @property
    def content(self) -> str | bytes:
        return self.body

    @property
    def content_sha256(self) -> str:
        return content_descriptor(self.body)[0]

    @property
    def content_length(self) -> int:
        return content_descriptor(self.body)[1]

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        fallback: SourceCandidate,
    ) -> "SourceMaterial":
        if not isinstance(value, Mapping):
            raise MissingEvidenceError("exact opener returned a non-object")
        body = value.get("body", value.get("content"))
        if body is None:
            raise MissingEvidenceError("exact opener returned no source material")
        raw_frame_date = value.get("framing_date")
        if type(raw_frame_date) is str:
            try:
                raw_frame_date = date.fromisoformat(raw_frame_date)
            except ValueError as exc:
                raise MissingEvidenceError(
                    "exact opener returned invalid framing_date"
                ) from exc
        try:
            return cls(
                source_ref=value.get("source_ref", fallback.source_ref),
                source_class=value.get("source_class", fallback.source_class),
                source_event_time=value.get(
                    "source_event_time", fallback.source_event_time
                ),
                created_at=value.get("created_at", fallback.created_at),
                body=body,
                expires_at=value.get("expires_at", fallback.expires_at),
                metadata=value.get("metadata", {}),
                framing=value.get("framing", "current"),
                framing_date=raw_frame_date,
            )
        except (TypeError, ValueError) as exc:
            raise MissingEvidenceError(
                "exact opener returned invalid source material"
            ) from exc


@dataclass(frozen=True, slots=True)
class ExposedSource:
    """Transient exact-open material paired with its durable exposure receipt."""

    record: "ExposureRecord"
    material: SourceMaterial

    @property
    def exposure_id(self) -> str:
        return self.record.exposure_id

    @property
    def state(self) -> str:
        return self.record.state

    @property
    def body(self) -> str | bytes:
        return self.material.body

    @property
    def framing(self) -> str:
        return self.material.framing

    @property
    def framing_date(self) -> date:
        return self.material.framing_date


@dataclass(frozen=True, slots=True)
class ExposureContext:
    """The minimum lifecycle context needed to bind one exposure."""

    session_id: str
    lifecycle_id: str
    turn_id: str
    source_kind: str
    observed_at: datetime
    turn_index: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _text(self.session_id, "session_id"))
        object.__setattr__(
            self, "lifecycle_id", _text(self.lifecycle_id, "lifecycle_id")
        )
        object.__setattr__(self, "turn_id", _text(self.turn_id, "turn_id"))
        object.__setattr__(
            self,
            "source_kind",
            _text(self.source_kind, "context source_kind", limit=_MAX_CLASS_BYTES),
        )
        object.__setattr__(self, "observed_at", _time(self.observed_at, "observed_at"))
        if type(self.turn_index) is not int or self.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")

    @classmethod
    def from_session(
        cls,
        value: Any,
        *,
        observed_at: datetime | None = None,
        turn_index: int = 0,
    ) -> "ExposureContext":
        """Adapt a SessionContext or SessionHookReceipt without importing it."""

        if hasattr(value, "context"):
            value = value.context
        if isinstance(value, Mapping):
            get = value.get
        else:
            get = lambda key, default=None: getattr(value, key, default)
        observed = get("observed_at", observed_at)
        if observed is None:
            raise ValueError("session context observed_at is required")
        index = get("turn_index", turn_index)
        return cls(
            session_id=get("session_id"),
            lifecycle_id=get("lifecycle_id"),
            turn_id=get("turn_id"),
            source_kind=get("source_kind"),
            observed_at=observed,
            turn_index=index,
        )


@dataclass(frozen=True, slots=True)
class ReplyUseEvidence:
    """A reply-side proof that a selected source was actually used."""

    reply_use_id: str
    content_sha256: str
    content_length: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "reply_use_id", _text(self.reply_use_id, "reply_use_id")
        )
        _hash(self.content_sha256, "reply_content_sha256")
        _length(self.content_length, "reply_content_length")

    @classmethod
    def from_content(cls, reply_use_id: str, content: Any) -> "ReplyUseEvidence":
        digest, length = content_descriptor(content)
        return cls(reply_use_id, digest, length)


@dataclass(frozen=True, slots=True)
class ExposureRecord:
    exposure_id: str
    state: str
    session_id: str
    lifecycle_id: str
    turn_id: str
    context_source_kind: str
    source_ref: str
    source_class: str
    source_event_time: datetime
    source_created_at: datetime
    source_expires_at: datetime | None
    content_sha256: str | None
    content_length: int | None
    created_at: datetime
    events: tuple[str, ...] = ()
    last_event_id: str = ""
    reply_use_id: str | None = None
    reply_content_sha256: str | None = None
    reply_content_length: int | None = None
    failure_code: str | None = None

    @property
    def event_time(self) -> datetime:
        return self.source_event_time

    @property
    def used(self) -> bool:
        return self.state in {"used", "consumed"}

    @property
    def consumed(self) -> bool:
        return self.state == "consumed"

    @property
    def failed_to_open(self) -> bool:
        return self.state == "failed_to_open"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ORCHESTRATION_SCHEMA,
            "kind": "exposure",
            "exposure_id": self.exposure_id,
            "state": self.state,
            "session_id": self.session_id,
            "lifecycle_id": self.lifecycle_id,
            "turn_id": self.turn_id,
            "context_source_kind": self.context_source_kind,
            "source_ref": self.source_ref,
            "source_class": self.source_class,
            "source_event_time": isoformat(self.source_event_time),
            "source_created_at": isoformat(self.source_created_at),
            "source_expires_at": (
                None
                if self.source_expires_at is None
                else isoformat(self.source_expires_at)
            ),
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "created_at": isoformat(self.created_at),
            "events": list(self.events),
            "last_event_id": self.last_event_id,
            "reply_use_id": self.reply_use_id,
            "reply_content_sha256": self.reply_content_sha256,
            "reply_content_length": self.reply_content_length,
            "failure_code": self.failure_code,
        }


def _reply_tuple(
    evidence: ReplyUseEvidence | None,
) -> tuple[str | None, str | None, int | None]:
    if evidence is None:
        return None, None, None
    return evidence.reply_use_id, evidence.content_sha256, evidence.content_length


def _coerce_reply(
    evidence: ReplyUseEvidence | None = None,
    *,
    reply_use_id: str | None = None,
    reply_content_sha256: str | None = None,
    reply_content_length: int | None = None,
    reply_content: Any = None,
) -> ReplyUseEvidence | None:
    if evidence is not None and not isinstance(evidence, ReplyUseEvidence):
        raise TypeError("reply evidence must be a ReplyUseEvidence")
    if reply_content is not None:
        if reply_use_id is None:
            raise ValueError("reply_use_id is required with reply_content")
        generated = ReplyUseEvidence.from_content(reply_use_id, reply_content)
        if evidence is not None and evidence != generated:
            raise ExposureConflictError("reply evidence arguments conflict")
        evidence = generated
    supplied = (reply_use_id, reply_content_sha256, reply_content_length)
    if any(value is not None for value in supplied):
        if any(value is None for value in supplied):
            raise ValueError("reply evidence must include id, hash, and length")
        supplied_evidence = ReplyUseEvidence(
            reply_use_id,
            reply_content_sha256,
            reply_content_length,
        )
        if evidence is not None and evidence != supplied_evidence:
            raise ExposureConflictError("reply evidence arguments conflict")
        evidence = supplied_evidence
    return evidence


def _parse_ledger_row(value: Mapping[str, Any], row_number: int) -> dict[str, Any]:
    if set(value) != _LEDGER_FIELDS:
        raise StateError(
            f"memory orchestration row {row_number} has unsupported fields"
        )
    try:
        if (
            value["schema_version"] != ORCHESTRATION_SCHEMA
            or value["kind"] != "exposure_event"
        ):
            raise ValueError("unsupported orchestration schema")
        event = value["event"]
        state = value["state"]
        if event not in EXPOSURE_STATES or state != event:
            raise ValueError("invalid exposure event/state")
        event_id = _text(
            value["event_id"], "exposure event_id", limit=_MAX_EVENT_ID_BYTES
        )
        exposure_id = _text(value["exposure_id"], "exposure_id")
        session_id = _text(value["session_id"], "session_id")
        lifecycle_id = _text(value["lifecycle_id"], "lifecycle_id")
        turn_id = _text(value["turn_id"], "turn_id")
        context_source_kind = _text(
            value["context_source_kind"], "context source_kind", limit=_MAX_CLASS_BYTES
        )
        source_ref = _text(value["source_ref"], "source_ref")
        source_class = _text(
            value["source_class"], "source_class", limit=_MAX_CLASS_BYTES
        )
        source_event_time = _time(value["source_event_time"], "source_event_time")
        source_created_at = _time(value["source_created_at"], "source created_at")
        source_expires_at = _optional_time(
            value["source_expires_at"], "source expires_at"
        )
        if source_expires_at is not None and source_expires_at <= source_created_at:
            raise ValueError("source expires_at is not later than source created_at")
        content_values = (value["content_sha256"], value["content_length"])
        if all(item is None for item in content_values):
            content_sha256 = content_length = None
        elif any(item is None for item in content_values):
            raise ValueError(
                "source content hash and length must be all null or populated"
            )
        else:
            content_sha256 = _hash(value["content_sha256"])
            content_length = _length(value["content_length"])
        created_at = _time(value["created_at"], "created_at")
        reply_values = (
            value["reply_use_id"],
            value["reply_content_sha256"],
            value["reply_content_length"],
        )
        if all(item is None for item in reply_values):
            reply = None
        elif any(item is None for item in reply_values):
            raise ValueError("reply evidence must be all null or all populated")
        else:
            reply = ReplyUseEvidence(*reply_values)
        failure_code = value["failure_code"]
        if failure_code is not None:
            failure_code = _text(failure_code, "failure_code", limit=_MAX_FAILURE_BYTES)
        if event == "selected" and failure_code is not None:
            raise ValueError("selected event cannot contain failure_code")
        if event == "failed_to_open" and failure_code is None:
            raise ValueError("failed_to_open event requires failure_code")
        if event != "failed_to_open" and failure_code is not None:
            raise ValueError("only failed_to_open event may contain failure_code")
        if event in {"opened", "exposed", "used", "consumed"}:
            if content_sha256 is None or content_length is None:
                raise ValueError(
                    "opened/exposed events require source content descriptor"
                )
        if (
            event in {"selected", "opened", "exposed", "failed_to_open"}
            and reply is not None
        ):
            raise ValueError(f"{event} event cannot contain reply-use evidence")
        if event == "consumed" and reply is None:
            raise ValueError("consumed event requires reply-use evidence")
        return {
            "event": event,
            "event_id": event_id,
            "exposure_id": exposure_id,
            "session_id": session_id,
            "lifecycle_id": lifecycle_id,
            "turn_id": turn_id,
            "context_source_kind": context_source_kind,
            "source_ref": source_ref,
            "source_class": source_class,
            "source_event_time": source_event_time,
            "source_created_at": source_created_at,
            "source_expires_at": source_expires_at,
            "content_sha256": content_sha256,
            "content_length": content_length,
            "created_at": created_at,
            "reply": reply,
            "failure_code": failure_code,
        }
    except (KeyError, TypeError, ValueError, StateError) as exc:
        raise StateError(f"memory orchestration row {row_number} is invalid") from exc


class ExposureLedger:
    """Concurrent-safe append-only exposed/selected/used/consumed ledger."""

    def __init__(self, root: Path, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "memory_orchestration.jsonl")
        self.mutation_lock = self.root / "memory_orchestration.mutation.lock"
        self.mutation_lock_path = self.mutation_lock
        self.clock = clock

    @staticmethod
    def _identity(state: ExposureRecord | Mapping[str, Any]) -> tuple[Any, ...]:
        if isinstance(state, ExposureRecord):
            return (
                state.exposure_id,
                state.session_id,
                state.lifecycle_id,
                state.turn_id,
                state.context_source_kind,
                state.source_ref,
                state.source_class,
                state.source_event_time,
                state.source_created_at,
                state.source_expires_at,
            )
        return (
            state["exposure_id"],
            state["session_id"],
            state["lifecycle_id"],
            state["turn_id"],
            state["context_source_kind"],
            state["source_ref"],
            state["source_class"],
            state["source_event_time"],
            state["source_created_at"],
            state["source_expires_at"],
        )

    @staticmethod
    def _record(
        parsed: Mapping[str, Any], *, events: tuple[str, ...], last_event_id: str
    ) -> ExposureRecord:
        reply = parsed["reply"]
        reply_id, reply_hash, reply_length = _reply_tuple(reply)
        return ExposureRecord(
            exposure_id=parsed["exposure_id"],
            state=parsed["event"],
            session_id=parsed["session_id"],
            lifecycle_id=parsed["lifecycle_id"],
            turn_id=parsed["turn_id"],
            context_source_kind=parsed["context_source_kind"],
            source_ref=parsed["source_ref"],
            source_class=parsed["source_class"],
            source_event_time=parsed["source_event_time"],
            source_created_at=parsed["source_created_at"],
            source_expires_at=parsed["source_expires_at"],
            content_sha256=parsed["content_sha256"],
            content_length=parsed["content_length"],
            created_at=parsed["created_at"],
            events=events,
            last_event_id=last_event_id,
            reply_use_id=reply_id,
            reply_content_sha256=reply_hash,
            reply_content_length=reply_length,
            failure_code=parsed.get("failure_code"),
        )

    def _replay_unlocked(
        self,
    ) -> tuple[dict[str, ExposureRecord], dict[str, Mapping[str, Any]]]:
        states: dict[str, ExposureRecord] = {}
        event_rows: dict[str, Mapping[str, Any]] = {}
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            parsed = _parse_ledger_row(row, row_number)
            event_id = parsed["event_id"]
            if event_id in event_rows:
                raise StateError(f"duplicate memory orchestration event_id: {event_id}")
            event_rows[event_id] = dict(row)
            exposure_id = parsed["exposure_id"]
            previous = states.get(exposure_id)
            if previous is None:
                if parsed["event"] != "selected":
                    raise StateError(
                        f"memory orchestration row {row_number} starts after selected"
                    )
                current = self._record(
                    parsed, events=("selected",), last_event_id=event_id
                )
            else:
                if self._identity(previous) != self._identity(parsed):
                    raise StateError(
                        f"memory orchestration row {row_number} changes immutable identity"
                    )
                if parsed["event"] not in _EXPOSURE_TRANSITIONS[previous.state]:
                    raise StateError(
                        f"memory orchestration row {row_number} is out of order"
                    )
                if previous.content_sha256 is not None and (
                    parsed["content_sha256"] != previous.content_sha256
                    or parsed["content_length"] != previous.content_length
                ):
                    raise StateError(
                        f"memory orchestration row {row_number} changes content descriptor"
                    )
                if parsed["event"] == "consumed":
                    if parsed["reply"] is None:
                        raise StateError("consumed exposure has no reply-use evidence")
                    previous_reply = _reply_tuple(
                        None
                        if previous.reply_use_id is None
                        else ReplyUseEvidence(
                            previous.reply_use_id,
                            previous.reply_content_sha256,
                            previous.reply_content_length,
                        )
                    )
                    if previous_reply[0] is not None and previous_reply != _reply_tuple(
                        parsed["reply"]
                    ):
                        raise StateError(
                            "consumed exposure reply-use evidence conflicts"
                        )
                current = self._record(
                    parsed,
                    events=(*previous.events, parsed["event"]),
                    last_event_id=event_id,
                )
            states[exposure_id] = current
        return states, event_rows

    def _snapshot(self) -> dict[str, ExposureRecord]:
        with file_lock(self.mutation_lock):
            return self._replay_unlocked()[0]

    @staticmethod
    def _candidate_identity(candidate: SourceCandidate) -> tuple[Any, ...]:
        return (
            candidate.source_ref,
            candidate.source_class,
            candidate.source_event_time,
            candidate.created_at,
            candidate.expires_at,
        )

    @staticmethod
    def _candidate_descriptor(
        candidate: SourceCandidate,
    ) -> tuple[str | None, int | None]:
        return candidate.content_sha256, candidate.content_length

    def _new_row(
        self,
        *,
        event: str,
        event_id: str,
        exposure_id: str,
        context: ExposureContext,
        candidate: SourceCandidate,
        created_at: datetime,
        reply: ReplyUseEvidence | None = None,
        failure_code: str | None = None,
    ) -> dict[str, Any]:
        digest, length = self._candidate_descriptor(candidate)
        if event in {"opened", "exposed", "used", "consumed"} and (
            digest is None or length is None
        ):
            raise ValueError(f"{event} requires a source content descriptor")
        reply_id, reply_hash, reply_length = _reply_tuple(reply)
        return {
            "schema_version": ORCHESTRATION_SCHEMA,
            "kind": "exposure_event",
            "event_id": _text(event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES),
            "event": event,
            "state": event,
            "exposure_id": _text(exposure_id, "exposure_id"),
            "session_id": context.session_id,
            "lifecycle_id": context.lifecycle_id,
            "turn_id": context.turn_id,
            "context_source_kind": context.source_kind,
            "source_ref": candidate.source_ref,
            "source_class": candidate.source_class,
            "source_event_time": isoformat(candidate.source_event_time),
            "source_created_at": isoformat(candidate.created_at),
            "source_expires_at": None
            if candidate.expires_at is None
            else isoformat(candidate.expires_at),
            "content_sha256": digest,
            "content_length": length,
            "created_at": isoformat(created_at),
            "reply_use_id": reply_id,
            "reply_content_sha256": reply_hash,
            "reply_content_length": reply_length,
            "failure_code": failure_code,
        }

    @staticmethod
    def _validate_fresh(candidate: SourceCandidate, now: datetime) -> None:
        if candidate.expires_at is not None and now >= candidate.expires_at:
            raise ExpiredEvidenceError(
                f"source evidence has expired: {candidate.source_ref}"
            )

    def record_selected(
        self,
        candidate: SourceCandidate,
        *,
        context: ExposureContext,
        exposure_id: str | None = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        if not isinstance(candidate, SourceCandidate):
            raise TypeError("candidate must be a SourceCandidate")
        if not isinstance(context, ExposureContext):
            raise TypeError("context must be an ExposureContext")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_exposure_id = (
            exposure_id
            or hashlib.sha256(
                f"{context.session_id}|{context.lifecycle_id}|{context.turn_id}|{candidate.source_ref}".encode()
            ).hexdigest()[:32]
        )
        actual_event_id = event_id or new_id("memory_exposure")
        _text(actual_exposure_id, "exposure_id")
        _text(actual_event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES)
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            existing = states.get(actual_exposure_id)
            if existing is not None:
                if self._identity(existing) != (
                    actual_exposure_id,
                    context.session_id,
                    context.lifecycle_id,
                    context.turn_id,
                    context.source_kind,
                    *self._candidate_identity(candidate),
                ):
                    raise ExposureConflictError(
                        f"exposure identity conflicts: {actual_exposure_id}"
                    )
                if existing.content_sha256 is not None and self._candidate_descriptor(
                    candidate
                ) != (existing.content_sha256, existing.content_length):
                    raise ExposureConflictError(
                        f"exposure content descriptor conflicts: {actual_exposure_id}"
                    )
                if actual_event_id in event_rows:
                    row = event_rows[actual_event_id]
                    if (
                        row.get("exposure_id") != actual_exposure_id
                        or row.get("event") != "selected"
                    ):
                        raise ExposureConflictError("exposure event_id conflicts")
                elif event_id is not None:
                    raise ExposureConflictError(
                        "selection event_id does not match the replayed selection"
                    )
                return existing
            if actual_event_id in event_rows:
                raise ExposureConflictError(
                    f"exposure event_id already exists: {actual_event_id}"
                )
            row = self._new_row(
                event="selected",
                event_id=actual_event_id,
                exposure_id=actual_exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed, events=("selected",), last_event_id=actual_event_id
            )

    def record_opened(
        self,
        exposure_id: str,
        material: SourceMaterial,
        *,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        if not isinstance(material, SourceMaterial):
            raise TypeError("opened evidence must be SourceMaterial")
        exposure_id = _text(exposure_id, "exposure_id")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if (
                material.source_ref != current.source_ref
                or material.source_class != current.source_class
                or material.source_event_time != current.source_event_time
                or material.created_at != current.source_created_at
                or material.expires_at != current.source_expires_at
            ):
                raise ExposureConflictError(
                    "opened evidence identity conflicts with selected source"
                )
            if current.state in {"opened", "exposed", "used", "consumed"}:
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "opened"
                    ):
                        raise ExposureConflictError(
                            "open event_id does not match the replayed opening"
                        )
                if (
                    current.content_sha256 != material.content_sha256
                    or current.content_length != material.content_length
                ):
                    raise ExposureConflictError(
                        "opened evidence descriptor conflicts on replay"
                    )
                return current
            if current.state == "failed_to_open":
                raise ValueError(
                    "failed-to-open exposure cannot be opened without a new selection"
                )
            if current.state != "selected":
                raise ValueError(f"exposure cannot open from {current.state}")
            if current.content_sha256 is not None and (
                material.content_sha256 != current.content_sha256
                or material.content_length != current.content_length
            ):
                raise ExposureConflictError(
                    "opened evidence descriptor does not match selected candidate"
                )
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=material.content_sha256,
                content_length=material.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="opened",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "opened"),
                last_event_id=actual_event_id,
            )

    def record_open_failed(
        self,
        exposure_id: str,
        failure_code: str,
        *,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        failure_code = _text(failure_code, "failure_code", limit=_MAX_FAILURE_BYTES)
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state == "failed_to_open":
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "failed_to_open"
                    ):
                        raise ExposureConflictError(
                            "failure event_id does not match the replayed failure"
                        )
                if current.failure_code != failure_code:
                    raise ExposureConflictError("failed-to-open replay conflicts")
                return current
            if current.state != "selected":
                raise ValueError(f"exposure cannot fail open from {current.state}")
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="failed_to_open",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
                failure_code=failure_code,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "failed_to_open"),
                last_event_id=actual_event_id,
            )

    def record_exposed(
        self,
        exposure_id: str,
        *,
        event_id: str | None = None,
        exposure_cap: int | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        if exposure_cap is not None and (
            type(exposure_cap) is not int or exposure_cap <= 0
        ):
            raise ValueError("exposure_cap must be a positive integer")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state in {"exposed", "used", "consumed"}:
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "exposed"
                    ):
                        raise ExposureConflictError(
                            "exposure event_id does not match the replayed exposure"
                        )
                return current
            if current.state != "opened":
                raise ValueError(f"exposure cannot become exposed from {current.state}")
            if exposure_cap is not None:
                exposed_count = sum(
                    item.session_id == current.session_id
                    and item.state in {"exposed", "used", "consumed"}
                    for item in states.values()
                )
                if exposed_count >= exposure_cap:
                    raise ValueError("session exposure cap reached")
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="exposed",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "exposed"),
                last_event_id=actual_event_id,
            )

    def _transition(
        self,
        exposure_id: str,
        *,
        target: str,
        reply: ReplyUseEvidence | None = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        if target not in {"used", "consumed"}:
            raise ValueError("invalid exposure transition")
        if target == "consumed" and reply is None:
            raise ValueError("consumed requires matching reply-use evidence")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        _text(actual_event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES)
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state == target or (
                target == "used" and current.state in {"consumed"}
            ):
                if reply is not None and current.reply_use_id is not None:
                    if _reply_tuple(reply) != (
                        current.reply_use_id,
                        current.reply_content_sha256,
                        current.reply_content_length,
                    ):
                        raise ExposureConflictError(
                            "reply-use evidence conflicts on replay"
                        )
                if actual_event_id in event_rows:
                    row = event_rows[actual_event_id]
                    if (
                        row.get("exposure_id") != exposure_id
                        or row.get("event") != target
                    ):
                        raise ExposureConflictError("exposure event_id conflicts")
                elif event_id is not None:
                    raise ExposureConflictError(
                        "transition event_id does not match the replayed transition"
                    )
                return current
            if target not in _EXPOSURE_TRANSITIONS.get(current.state, frozenset()):
                raise ValueError(
                    f"exposure cannot transition from {current.state} to {target}"
                )
            if (
                target == "consumed"
                and current.reply_use_id is not None
                and _reply_tuple(reply)
                != (
                    current.reply_use_id,
                    current.reply_content_sha256,
                    current.reply_content_length,
                )
            ):
                raise ExposureConflictError(
                    "consumed reply-use evidence does not match used evidence"
                )
            if actual_event_id in event_rows:
                raise ExposureConflictError(
                    f"exposure event_id already exists: {actual_event_id}"
                )
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event=target,
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
                reply=reply,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, target),
                last_event_id=actual_event_id,
            )

    def record_used(
        self,
        exposure_id: str,
        evidence: ReplyUseEvidence | None = None,
        *,
        reply_use_id: str | None = None,
        reply_content_sha256: str | None = None,
        reply_content_length: int | None = None,
        reply_content: Any = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        reply = _coerce_reply(
            evidence,
            reply_use_id=reply_use_id,
            reply_content_sha256=reply_content_sha256,
            reply_content_length=reply_content_length,
            reply_content=reply_content,
        )
        return self._transition(
            exposure_id, target="used", reply=reply, event_id=event_id, now=now
        )

    def record_consumed(
        self,
        exposure_id: str,
        evidence: ReplyUseEvidence | None = None,
        *,
        reply_use_id: str | None = None,
        reply_content_sha256: str | None = None,
        reply_content_length: int | None = None,
        reply_content: Any = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        reply = _coerce_reply(
            evidence,
            reply_use_id=reply_use_id,
            reply_content_sha256=reply_content_sha256,
            reply_content_length=reply_content_length,
            reply_content=reply_content,
        )
        return self._transition(
            exposure_id, target="consumed", reply=reply, event_id=event_id, now=now
        )

    def get(self, exposure_id: str) -> ExposureRecord | None:
        exposure_id = _text(exposure_id, "exposure_id")
        return self._snapshot().get(exposure_id)

    def records_for_session(self, session_id: str) -> tuple[ExposureRecord, ...]:
        session_id = _text(session_id, "session_id")
        return tuple(
            sorted(
                (
                    item
                    for item in self._snapshot().values()
                    if item.session_id == session_id
                ),
                key=lambda item: item.exposure_id,
            )
        )

    def session_count(self, session_id: str) -> int:
        return sum(
            item.state in {"exposed", "used", "consumed"}
            for item in self.records_for_session(session_id)
        )

    def last_exposure_at(self, session_id: str, source_ref: str) -> datetime | None:
        session_id = _text(session_id, "session_id")
        source_ref = _text(source_ref, "source_ref")
        with file_lock(self.mutation_lock):
            self._replay_unlocked()
            values: list[datetime] = []
            for row in self.ledger.rows():
                parsed = _parse_ledger_row(row, 0)
                if (
                    parsed["event"] == "exposed"
                    and parsed["session_id"] == session_id
                    and parsed["source_ref"] == source_ref
                ):
                    values.append(parsed["created_at"])
            return max(values) if values else None

    def events(self) -> tuple[dict[str, Any], ...]:
        with file_lock(self.mutation_lock):
            self._replay_unlocked()
            return tuple(dict(row) for row in self.ledger.rows())

    def replay(self) -> tuple[ExposureRecord, ...]:
        return tuple(
            sorted(self._snapshot().values(), key=lambda item: item.exposure_id)
        )

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return content-free exposure facts without taking the mutation lock."""

        effective_now = _observer_validate_context(target_date, now)
        return _observer_exposure_facts(
            self,
            target_date=target_date,
            now=effective_now,
        )


def _observer_exposure_replay(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[ExposureRecord, ...]:
    """Replay exposure envelopes without ``JsonlLedger.rows`` or a lock."""

    states: dict[str, ExposureRecord] = {}
    event_rows: dict[str, Mapping[str, Any]] = {}
    for row_number, row in enumerate(rows, start=1):
        parsed = _parse_ledger_row(row, row_number)
        event_id = parsed["event_id"]
        if event_id in event_rows:
            raise StateError("duplicate memory orchestration event id")
        event_rows[event_id] = row
        exposure_id = parsed["exposure_id"]
        previous = states.get(exposure_id)
        if previous is None:
            if parsed["event"] != "selected":
                raise StateError("exposure ledger starts after selected")
            current = ExposureLedger._record(
                parsed, events=("selected",), last_event_id=event_id
            )
        else:
            identity = (
                parsed["exposure_id"],
                parsed["session_id"],
                parsed["lifecycle_id"],
                parsed["turn_id"],
                parsed["context_source_kind"],
                parsed["source_ref"],
                parsed["source_class"],
                parsed["source_event_time"],
                parsed["source_created_at"],
                parsed["source_expires_at"],
            )
            if ExposureLedger._identity(previous) != identity:
                raise StateError("exposure ledger changes immutable identity")
            if parsed["event"] not in _EXPOSURE_TRANSITIONS[previous.state]:
                raise StateError("exposure ledger contains an out-of-order event")
            if previous.content_sha256 is not None and (
                parsed["content_sha256"] != previous.content_sha256
                or parsed["content_length"] != previous.content_length
            ):
                raise StateError("exposure ledger changes content descriptor")
            if parsed["event"] == "consumed":
                reply = parsed["reply"]
                if reply is None:
                    raise StateError("consumed exposure has no reply evidence")
                if previous.reply_use_id is not None and _reply_tuple(reply) != (
                    previous.reply_use_id,
                    previous.reply_content_sha256,
                    previous.reply_content_length,
                ):
                    raise StateError("consumed exposure reply evidence conflicts")
            current = ExposureLedger._record(
                parsed,
                events=(*previous.events, parsed["event"]),
                last_event_id=event_id,
            )
        states[exposure_id] = current
    return tuple(sorted(states.values(), key=lambda item: item.exposure_id))


def _observer_exposure_facts(
    ledger: Any,
    *,
    target_date: date,
    now: datetime,
    policy: Any = None,
) -> tuple[ObservationFact, ...]:
    """Project exposure state and policy limits using only safe envelopes."""

    ledger_owner = getattr(ledger, "ledger", None)
    path = getattr(ledger_owner, "path", None)
    if path is None:
        return (
            ObservationFact(
                key="memory.exposure.adapter",
                code="exposure_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("exposure",),
            ),
        )
    try:
        rows = _observer_jsonl_rows(path)
        records = _observer_exposure_replay(rows)
    except Exception as exc:  # noqa: BLE001 - observer must fail closed
        del exc
        return (_observer_integrity_fact("exposure", target_date=target_date),)
    if not records:
        return ()

    exposure_positions = {
        row.get("exposure_id"): position
        for position, row in enumerate(rows)
        if type(row.get("exposure_id")) is str
    }
    facts: list[ObservationFact] = []
    success_states = {"exposed", "used", "consumed"}
    failed = [record for record in records if record.state == "failed_to_open"]
    for record in records:
        refs = _observer_refs(
            record.exposure_id,
            record.last_event_id,
            record.source_ref,
        )
        for event in record.events:
            if event == "failed_to_open":
                continue
            facts.append(
                ObservationFact(
                    key=f"memory.exposure.event:{record.exposure_id}:{event}",
                    code=f"exposure_{event}",
                    state="neutral",
                    target_date=target_date,
                    event_time=record.created_at,
                    refs=refs,
                    counts={f"exposure_{event}": 1},
                )
            )

    for record in failed:
        later = max(
            (
                candidate
                for candidate in records
                if candidate.source_ref == record.source_ref
                and candidate.state in success_states
                and exposure_positions.get(candidate.exposure_id, -1)
                > exposure_positions.get(record.exposure_id, -1)
            ),
            key=lambda candidate: candidate.created_at,
            default=None,
        )
        recovery = None
        state = "current"
        refs = _observer_refs(
            record.exposure_id,
            record.last_event_id,
            record.source_ref,
        )
        if later is not None:
            state = "recovered_history"
            recovery_ref = later.last_event_id or later.exposure_id
            recovery = RecoveryEvidence(
                recovery_ref,
                f"exposure_{later.state}",
                later.created_at,
            )
            refs = _observer_refs(
                *refs,
                later.exposure_id,
                later.last_event_id,
            )
        facts.append(
            ObservationFact(
                key=f"memory.exposure.open_failed:{record.source_ref}",
                code="exposure_open_failed",
                state=state,
                target_date=target_date,
                event_time=record.created_at,
                refs=refs,
                counts={"exposure_open_failed": 1},
                recovery=recovery,
            )
        )

    # A cap and source cooldown are policy observations, not ledger mutations.
    if policy is not None:
        cap = getattr(policy, "max_per_session", None)
        if type(cap) is int and cap > 0:
            by_session: dict[str, list[ExposureRecord]] = {}
            for record in records:
                if record.state in success_states:
                    by_session.setdefault(record.session_id, []).append(record)
            for session_id, exposed in sorted(by_session.items()):
                if len(exposed) >= cap:
                    facts.append(
                        ObservationFact(
                            key=f"memory.exposure.cap:{session_id}",
                            code="exposure_cap_reached",
                            state="neutral",
                            target_date=target_date,
                            event_time=max(item.created_at for item in exposed),
                            refs=_observer_refs(session_id),
                            counts={
                                "exposure_count": len(exposed),
                                "exposure_cap": cap,
                            },
                        )
                    )
        cooldown = getattr(policy, "source_cooldown", None)
        if isinstance(cooldown, timedelta) and cooldown > timedelta(0):
            latest: dict[tuple[str, str], ExposureRecord] = {}
            for record in records:
                if record.state not in success_states:
                    continue
                identity = (record.session_id, record.source_ref)
                previous = latest.get(identity)
                if previous is None or record.created_at > previous.created_at:
                    latest[identity] = record
            for (session_id, source_ref), record in sorted(latest.items()):
                if now - record.created_at < cooldown:
                    facts.append(
                        ObservationFact(
                            key=f"memory.exposure.cooldown:{session_id}:{source_ref}",
                            code="exposure_cooldown",
                            state="neutral",
                            target_date=target_date,
                            event_time=record.created_at,
                            refs=_observer_refs(
                                session_id,
                                source_ref,
                                record.last_event_id,
                            ),
                            counts={"cooldown_seconds": int(cooldown.total_seconds())},
                        )
                    )
    return _observer_merge_facts(facts)


@dataclass(frozen=True, slots=True)
class ExposurePolicy:
    """Deterministic selection limits and first/continuation reservoir rules."""

    max_per_session: int = 8
    source_cooldown: timedelta = timedelta(minutes=30)
    result_budget: int = 8
    exposure_cap: int | None = None

    def __post_init__(self) -> None:
        cap = self.max_per_session if self.exposure_cap is None else self.exposure_cap
        if type(cap) is not int or cap <= 0:
            raise ValueError("exposure cap must be a positive integer")
        if type(self.max_per_session) is not int or self.max_per_session <= 0:
            raise ValueError("max_per_session must be a positive integer")
        if type(self.result_budget) is not int or self.result_budget <= 0:
            raise ValueError("result_budget must be a positive integer")
        if not isinstance(
            self.source_cooldown, timedelta
        ) or self.source_cooldown < timedelta(0):
            raise ValueError("source_cooldown must be non-negative")
        object.__setattr__(self, "max_per_session", cap)
        object.__setattr__(self, "exposure_cap", cap)

    def choose(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext,
        ledger: ExposureLedger,
        now: datetime,
        continuity_policy: Callable[[str, str], bool] | None = None,
        first_turn: bool | None = None,
    ) -> "ExposurePlan":
        effective_now = _time(now, "now")
        incoming = list(candidates)
        if any(not isinstance(item, SourceCandidate) for item in incoming):
            raise TypeError("selection candidates must be SourceCandidate instances")
        is_first = context.turn_index == 0 if first_turn is None else first_turn
        prior = tuple(
            item
            for item in ledger.records_for_session(context.session_id)
            if item.state in {"exposed", "used", "consumed"}
        )
        remaining = self.max_per_session - len(prior)
        if remaining <= 0:
            mode = "self_brief" if is_first else "wider_reservoir"
            return ExposurePlan(mode, (), 0, self.result_budget)
        if is_first:
            mode = "self_brief"
            brief = [
                item
                for item in incoming
                if item.source_class.lower() in _SELF_BRIEF_CLASSES
            ]
            pool = brief or incoming
        else:
            previous_time = max(
                (item.source_event_time for item in prior), default=None
            )
            delta = [
                item
                for item in incoming
                if previous_time is None or item.source_event_time > previous_time
            ]
            mode = "continuation_delta" if delta else "wider_reservoir"
            pool = delta or incoming
        selected: list[SourceCandidate] = []
        seen: set[str] = set()
        for candidate in sorted(
            pool,
            key=lambda item: (
                -float(item.relevance),
                -item.source_event_time.timestamp(),
                item.source_ref,
            ),
        ):
            if candidate.source_ref in seen:
                continue
            seen.add(candidate.source_ref)
            if _is_private_class(candidate.source_class):
                if continuity_policy is None:
                    if context.source_kind != "private_inbound":
                        raise PolicyDeniedError(
                            f"private continuity is not authorized for {context.source_kind}"
                        )
                elif not continuity_policy(candidate.source_class, context.source_kind):
                    raise PolicyDeniedError(
                        f"source class is not authorized: {candidate.source_class}"
                    )
            last = ledger.last_exposure_at(context.session_id, candidate.source_ref)
            if last is not None and effective_now - last < self.source_cooldown:
                continue
            selected.append(candidate)
            if len(selected) >= min(remaining, self.result_budget):
                break
        return ExposurePlan(mode, tuple(selected), remaining, self.result_budget)


@dataclass(frozen=True, slots=True)
class ExposurePlan:
    mode: str
    candidates: tuple[SourceCandidate, ...]
    remaining_cap: int
    result_budget: int

    @property
    def selected(self) -> tuple[SourceCandidate, ...]:
        return self.candidates


def _is_private_class(source_class: str) -> bool:
    normalized = source_class.strip().lower()
    return normalized in _PRIVATE_SOURCE_CLASSES or normalized.startswith("private_")


class SourceRegistry:
    """Adapter that keeps retrieval opaque and exact opening bounded."""

    def __init__(self, retriever: Any = None, opener: Any = None) -> None:
        self.retriever = retriever
        self.opener = opener

    def retrieve(self, query: str, *, limit: int) -> tuple[SourceCandidate, ...]:
        query = _text(query, "query", limit=16 * 1024)
        if type(limit) is not int or limit <= 0:
            raise ValueError("retrieval limit must be a positive integer")
        if self.retriever is None:
            return ()
        method = getattr(self.retriever, "retrieve", None) or getattr(
            self.retriever, "search", None
        )
        if not callable(method):
            raise TypeError("retriever must provide retrieve or search")
        try:
            raw = method(query, limit=limit)
        except TypeError as first_error:
            try:
                raw = method(query, limit)
            except TypeError:
                raise first_error
        if raw is None:
            return ()
        result: list[SourceCandidate] = []
        for item in raw:
            candidate = (
                item
                if isinstance(item, SourceCandidate)
                else SourceCandidate.from_mapping(item)
            )
            result.append(candidate)
            if len(result) >= limit:
                break
        return tuple(result)

    def exact_open(
        self,
        candidate: SourceCandidate,
        *,
        max_bytes: int = _MAX_SOURCE_BYTES,
    ) -> SourceMaterial | None:
        if self.opener is None:
            raise MissingEvidenceError("no exact source opener is configured")
        if (
            type(max_bytes) is not int
            or max_bytes <= 0
            or max_bytes > _MAX_SOURCE_BYTES
        ):
            raise ValueError("max_bytes is outside the bounded source limit")
        method = getattr(self.opener, "open", None) or getattr(
            self.opener, "open_source", None
        )
        if not callable(method):
            raise TypeError("opener must provide open or open_source")
        try:
            raw = method(candidate.source_ref, max_bytes=max_bytes)
        except TypeError as first_error:
            try:
                raw = method(candidate.source_ref)
            except TypeError:
                raise first_error
        if raw is None:
            raise MissingEvidenceError(
                f"source reference is missing: {candidate.source_ref}"
            )
        if isinstance(raw, SourceMaterial):
            material = raw
        elif isinstance(raw, Mapping):
            material = SourceMaterial.from_mapping(raw, fallback=candidate)
        elif isinstance(raw, (str, bytes)):
            material = SourceMaterial(
                source_ref=candidate.source_ref,
                source_class=candidate.source_class,
                source_event_time=candidate.source_event_time,
                created_at=candidate.created_at,
                expires_at=candidate.expires_at,
                body=raw,
            )
        else:
            raise MissingEvidenceError(
                "exact opener returned unsupported source material"
            )
        if material.source_ref != candidate.source_ref:
            raise MissingEvidenceError("exact opener changed source reference")
        if material.source_class != candidate.source_class:
            raise MissingEvidenceError("exact opener changed source class")
        if material.source_event_time != candidate.source_event_time:
            raise MissingEvidenceError("exact opener changed source event time")
        if material.created_at != candidate.created_at:
            raise MissingEvidenceError("exact opener changed source created_at")
        if material.expires_at != candidate.expires_at:
            raise MissingEvidenceError("exact opener changed source expires_at")
        if candidate.content_sha256 is not None and (
            material.content_sha256 != candidate.content_sha256
            or material.content_length != candidate.content_length
        ):
            raise MissingEvidenceError(
                "exact source evidence does not match candidate descriptor"
            )
        if material.content_length > max_bytes:
            raise MissingEvidenceError(
                "exact source material exceeds the requested byte limit"
            )
        return material


class MaintenanceApprovalAdapter(Protocol):
    """Adapter-declared approval classification; core never infers it."""

    def approval_required(self, proposal: Mapping[str, Any]) -> bool: ...


_APPROVAL_SCHEMA = "moon.memory.approval.v1"
_APPROVAL_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event",
        "proposal_id",
        "required",
        "approved",
        "created_at",
        "evidence_sha256",
        "evidence_length",
    }
)
_MAINTENANCE_OBSERVER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "proposal_id",
        "request_id",
        "created_at",
        "operation",
        "evidence_refs",
        "evidence_sha256",
        "reason",
        "proposed_value",
        "status",
        "applied",
    }
)
_HISTORY_OBSERVER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "proposal_id",
        "request_id",
        "created_at",
        "operation",
        "activity",
        "permission",
        "evidence_sha256",
        "created_card",
        "archived_card_ids",
        "status",
    }
)


def _approval_descriptor(value: Any) -> tuple[str, int]:
    if isinstance(value, bool) or value is None:
        raise ValueError("approval evidence must be explicit non-boolean evidence")
    return content_descriptor(value)


def _approval_classification(adapter: Any, proposal: Mapping[str, Any]) -> bool:
    """Read only an adapter-declared approval classification."""

    if adapter is None:
        return False
    method = getattr(adapter, "approval_required", None)
    if method is None:
        method = getattr(adapter, "requires_approval", None)
    if callable(method):
        result = method(proposal)
    elif type(method) is bool:
        result = method
    elif callable(adapter):
        result = adapter(proposal)
    else:
        raise TypeError("approval adapter must provide an approval_required flag")
    if type(result) is not bool:
        raise ValueError("approval adapter must return a boolean")
    return result


class MemoryMaintenanceFacade:
    """Reference-only facade for proposal, apply, and archive semantics."""

    def __init__(
        self,
        memory_store: Any,
        *,
        approval_adapter: Any = None,
        root: Path | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.memory_store = memory_store
        self.approval_adapter = approval_adapter
        self.clock = clock
        maintenance_owner = getattr(memory_store, "maintenance", None)
        history_owner = getattr(memory_store, "history", None)
        self._maintenance_path = getattr(maintenance_owner, "path", None)
        self._history_path = getattr(history_owner, "path", None)
        if root is None:
            path = self._maintenance_path
            if path is not None:
                root = Path(path).parent
        self._root = None if root is None else Path(root)
        self._approval_ledger = (
            None
            if root is None
            else JsonlLedger(Path(root) / "memory_orchestration_approvals.jsonl")
        )
        self._approval_lock = (
            None
            if root is None
            else Path(root) / "memory_orchestration_approvals.mutation.lock"
        )
        self._approval_memory: dict[str, tuple[bool, bool, str | None, int | None]] = {}
        self._proposals: dict[str, Mapping[str, Any]] = {}

    def _approval_replay(self) -> dict[str, tuple[bool, bool, str | None, int | None]]:
        if self._approval_ledger is None:
            return dict(self._approval_memory)
        states: dict[str, tuple[bool, bool, str | None, int | None]] = {}
        for index, row in enumerate(self._approval_ledger.rows(), start=1):
            if set(row) != _APPROVAL_FIELDS:
                raise StateError(f"approval row {index} has unsupported fields")
            if row["schema_version"] != _APPROVAL_SCHEMA or row["kind"] != "approval":
                raise StateError(f"approval row {index} has an unsupported schema")
            proposal_id = _text(row["proposal_id"], "proposal_id")
            if type(row["required"]) is not bool or type(row["approved"]) is not bool:
                raise StateError(f"approval row {index} has invalid booleans")
            _time(row["created_at"], "approval created_at")
            digest = row["evidence_sha256"]
            length = row["evidence_length"]
            if digest is None or length is None:
                if digest is not None or length is not None:
                    raise StateError(f"approval row {index} has partial evidence")
            else:
                _hash(digest, "approval evidence hash")
                _length(length, "approval evidence length")
            if row["event"] == "pending":
                if row["approved"] or digest is not None:
                    raise StateError(f"approval row {index} has invalid pending state")
                if proposal_id in states:
                    raise StateError(f"duplicate approval pending event: {proposal_id}")
            elif row["event"] == "approved":
                if not row["required"] or not row["approved"] or digest is None:
                    raise StateError(f"approval row {index} has invalid approved state")
                previous = states.get(proposal_id)
                if previous is None or not previous[0] or previous[1]:
                    raise StateError(f"approval row {index} is out of order")
            else:
                raise StateError(f"approval row {index} has invalid event")
            states[proposal_id] = (row["required"], row["approved"], digest, length)
        return states

    def _approval_state(
        self,
        proposal_id: str,
    ) -> tuple[bool, bool, str | None, int | None] | None:
        with (
            file_lock(self._approval_lock)
            if self._approval_lock is not None
            else nullcontext()
        ):
            return self._approval_replay().get(proposal_id)

    def _append_approval(
        self,
        proposal_id: str,
        *,
        required: bool,
        approved: bool,
        digest: str | None = None,
        length: int | None = None,
    ) -> None:
        event = "approved" if approved else "pending"
        row = {
            "schema_version": _APPROVAL_SCHEMA,
            "kind": "approval",
            "event": event,
            "proposal_id": _text(proposal_id, "proposal_id"),
            "required": required,
            "approved": approved,
            "created_at": isoformat(self.clock()),
            "evidence_sha256": digest,
            "evidence_length": length,
        }
        if self._approval_ledger is None:
            states = self._approval_replay()
            if proposal_id in states and (not approved or states[proposal_id][1]):
                return
            self._approval_memory[proposal_id] = (
                required,
                approved,
                digest,
                length,
            )
            return
        self._approval_ledger.append(row)

    def _classify_approval(
        self,
        proposal: Mapping[str, Any],
        explicit: bool | None,
    ) -> bool:
        adapter_required = _approval_classification(self.approval_adapter, proposal)
        if explicit is not None:
            if type(explicit) is not bool:
                raise ValueError("approval_required must be a boolean")
            return adapter_required or explicit
        return adapter_required

    def propose(
        self,
        *,
        request_id: str,
        operation: str,
        evidence_refs: Iterable[str],
        reason: str,
        proposed_value: Any = None,
        approval_required: bool | None = None,
        sensitive: bool | None = None,
    ) -> Mapping[str, Any]:
        method = getattr(self.memory_store, "propose_maintenance", None)
        if not callable(method):
            raise TypeError("injected memory store lacks maintenance proposal support")
        if sensitive is not None:
            if type(sensitive) is not bool:
                raise ValueError("sensitive must be a boolean")
            if approval_required is not None and approval_required != sensitive:
                raise ValueError("sensitivity and approval classification conflict")
            approval_required = sensitive
        proposal = method(
            request_id=request_id,
            operation=operation,
            evidence_refs=evidence_refs,
            reason=reason,
            proposed_value=proposed_value,
        )
        proposal_id = _text(
            proposal.get("proposal_id") or f"proposal:{request_id}", "proposal_id"
        )
        required = self._classify_approval(proposal, approval_required)
        self._proposals[proposal_id] = dict(proposal)
        with (
            file_lock(self._approval_lock)
            if self._approval_lock is not None
            else nullcontext()
        ):
            existing = self._approval_replay().get(proposal_id)
            if existing is None:
                self._append_approval(proposal_id, required=required, approved=False)
            elif existing[0] != required:
                raise StateError("approval classification conflicts on replay")
        result = dict(proposal)
        result["approval_required"] = required
        result["approval_state"] = "pending" if required else "not_required"
        return result

    def apply(
        self,
        proposal_id: str,
        *,
        activity: str,
        permission: str,
        approval_evidence: Any = None,
        approval: bool | None = None,
    ) -> Mapping[str, Any]:
        proposal_id = _text(proposal_id, "proposal_id")
        state = self._approval_state(proposal_id)
        if state is None:
            raise ValueError(f"maintenance proposal is not registered: {proposal_id}")
        if state is not None and state[0] and not state[1]:
            if approval is not None and type(approval) is not bool:
                raise ValueError("approval must be a boolean")
            if approval is False:
                approval_evidence = None
            if approval_evidence is None:
                return {
                    "proposal_id": proposal_id,
                    "status": "pending",
                    "reason": "approval_required",
                    "approval_requested": True,
                    "write_performed": False,
                }
            digest, length = _approval_descriptor(approval_evidence)
            with (
                file_lock(self._approval_lock)
                if self._approval_lock is not None
                else nullcontext()
            ):
                state = self._approval_replay().get(proposal_id)
                if state is None or not state[0]:
                    raise StateError("approval state disappeared")
                if not state[1]:
                    self._append_approval(
                        proposal_id,
                        required=True,
                        approved=True,
                        digest=digest,
                        length=length,
                    )
        method = getattr(self.memory_store, "apply_maintenance", None)
        if not callable(method):
            raise TypeError("injected memory store lacks maintenance apply support")
        return method(proposal_id, activity=activity, permission=permission)

    def archive(
        self,
        *,
        request_id: str,
        evidence_refs: Iterable[str],
        reason: str,
    ) -> Mapping[str, Any]:
        return self.propose(
            request_id=request_id,
            operation="retire",
            evidence_refs=evidence_refs,
            reason=reason,
        )

    def approval_required(self, proposal: Mapping[str, Any]) -> bool:
        declared = False
        if isinstance(proposal, Mapping):
            raw_declared = proposal.get("approval_required")
            if raw_declared is not None and type(raw_declared) is not bool:
                raise ValueError("proposal approval_required must be a boolean")
            declared = raw_declared is True
        adapter_required = _approval_classification(self.approval_adapter, proposal)
        return adapter_required or declared

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Read proposal/approval envelopes without opening memory content."""

        _observer_validate_context(target_date, now)
        return _observer_maintenance_facts(
            self,
            target_date=target_date,
            now=now,
        )


def _observer_maintenance_proposal(
    row: Mapping[str, Any], row_number: int
) -> dict[str, Any]:
    if set(row) != _MAINTENANCE_OBSERVER_FIELDS:
        raise StateError(
            f"maintenance observer row {row_number} has unsupported fields"
        )
    if row["schema_version"] != "moon.memory.maintenance.v1":
        raise StateError("maintenance observer row has an unsupported schema")
    if row["kind"] != "maintenance_proposal":
        raise StateError("maintenance observer row has an unsupported kind")
    proposal_id = _text(row["proposal_id"], "proposal_id")
    request_id = _text(row["request_id"], "request_id")
    created_at = _time(row["created_at"], "created_at")
    operation = row["operation"]
    if operation not in {"merge", "retire", "distill"}:
        raise StateError("maintenance observer row has an invalid operation")
    evidence_refs = row["evidence_refs"]
    if not isinstance(evidence_refs, list) or any(
        type(ref) is not str or not ref.strip() for ref in evidence_refs
    ):
        raise StateError("maintenance observer row has invalid evidence refs")
    digest = row["evidence_sha256"]
    if type(digest) is not str or _HASH_RE.fullmatch(digest) is None:
        raise StateError("maintenance observer row has an invalid evidence hash")
    if row["status"] != "proposed" or row["applied"] is not False:
        raise StateError("maintenance observer row has an invalid state")
    # Deliberately do not inspect ``reason`` or ``proposed_value``.  They may
    # contain memory/source material and are not observer evidence.
    return {
        "proposal_id": proposal_id,
        "request_id": request_id,
        "created_at": created_at,
        "operation": operation,
        "evidence_refs": tuple(evidence_refs),
        "evidence_sha256": digest,
    }


def _observer_maintenance_history(
    row: Mapping[str, Any], row_number: int
) -> dict[str, Any]:
    if set(row) != _HISTORY_OBSERVER_FIELDS:
        raise StateError(f"maintenance history row {row_number} has unsupported fields")
    if row["schema_version"] != "moon.memory.history.v1":
        raise StateError("maintenance history has an unsupported schema")
    if row["kind"] != "maintenance_applied" or row["status"] != "applied":
        raise StateError("maintenance history has an invalid state")
    operation = row["operation"]
    if operation not in {"merge", "retire", "distill"}:
        raise StateError("maintenance history has an invalid operation")
    request_id = _text(row["request_id"], "history request_id")
    _text(row["activity"], "history activity")
    _text(row["permission"], "history permission")
    evidence_sha256 = _hash(row["evidence_sha256"], "history evidence hash")
    archived_card_ids = row["archived_card_ids"]
    if not isinstance(archived_card_ids, list) or any(
        type(value) is not str or not value.strip() for value in archived_card_ids
    ):
        raise StateError("maintenance history has invalid archive refs")
    return {
        "event_id": _text(row["event_id"], "history event_id"),
        "proposal_id": _text(row["proposal_id"], "history proposal_id"),
        "request_id": request_id,
        "created_at": _time(row["created_at"], "history created_at"),
        "operation": operation,
        "evidence_sha256": evidence_sha256,
    }


def _observer_approval_row(row: Mapping[str, Any], row_number: int) -> dict[str, Any]:
    if set(row) != _APPROVAL_FIELDS:
        raise StateError(f"approval observer row {row_number} has unsupported fields")
    if row["schema_version"] != _APPROVAL_SCHEMA or row["kind"] != "approval":
        raise StateError("approval observer row has an unsupported schema")
    proposal_id = _text(row["proposal_id"], "approval proposal_id")
    required = row["required"]
    approved = row["approved"]
    if type(required) is not bool or type(approved) is not bool:
        raise StateError("approval observer row has invalid booleans")
    created_at = _time(row["created_at"], "approval created_at")
    digest = row["evidence_sha256"]
    length = row["evidence_length"]
    if digest is None or length is None:
        if digest is not None or length is not None:
            raise StateError("approval observer row has partial evidence")
    else:
        _hash(digest, "approval evidence hash")
        _length(length, "approval evidence length")
    event = row["event"]
    if event == "pending":
        if approved or digest is not None or length is not None:
            raise StateError("approval pending row has evidence")
    elif event == "approved":
        if not required or not approved or digest is None or length is None:
            raise StateError("approval approved row is invalid")
    else:
        raise StateError("approval observer row has an invalid event")
    return {
        "proposal_id": proposal_id,
        "required": required,
        "approved": approved,
        "created_at": created_at,
        "digest": digest,
        "length": length,
        "event": event,
    }


def _observer_maintenance_facts(
    facade: MemoryMaintenanceFacade,
    *,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Project approval/proposal receipts without reading card/diary content."""

    maintenance_rows: tuple[Mapping[str, Any], ...] = ()
    history_rows: tuple[Mapping[str, Any], ...] = ()
    approval_rows: tuple[Mapping[str, Any], ...] = ()
    try:
        if facade._maintenance_path is not None:
            maintenance_rows = _observer_jsonl_rows(facade._maintenance_path)
        if facade._history_path is not None:
            history_rows = _observer_jsonl_rows(facade._history_path)
        if facade._approval_ledger is not None:
            approval_rows = _observer_jsonl_rows(facade._approval_ledger.path)
        proposals: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(maintenance_rows, start=1):
            proposal = _observer_maintenance_proposal(row, row_number)
            proposal_id = proposal["proposal_id"]
            if proposal_id in proposals:
                raise StateError("duplicate maintenance proposal")
            proposals[proposal_id] = proposal

        # An in-memory proposal is a useful owner only when no durable
        # maintenance ledger exists.  Read its envelope fields exclusively;
        # never touch proposed_value/reason or recursively inspect the map.
        if not maintenance_rows:
            for value in facade._proposals.values():
                if not isinstance(value, Mapping):
                    continue
                proposal_id = value.get("proposal_id")
                if type(proposal_id) is not str or not proposal_id.strip():
                    continue
                operation = value.get("operation")
                if operation not in {"merge", "retire", "distill"}:
                    continue
                request_id = value.get("request_id", proposal_id)
                if type(request_id) is not str or not request_id.strip():
                    request_id = proposal_id
                else:
                    request_id = request_id.strip()
                created_at_value = value.get("created_at")
                created_at = (
                    None
                    if created_at_value is None
                    else _time(created_at_value, "in-memory proposal created_at")
                )
                proposals.setdefault(
                    proposal_id,
                    {
                        "proposal_id": proposal_id,
                        "request_id": request_id,
                        # An in-memory proposal has no durable timestamp or
                        # apply receipt to project into observer evidence.
                        "created_at": created_at,
                        "operation": operation,
                        "evidence_refs": (),
                        "evidence_sha256": None,
                    },
                )

        applied: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(history_rows, start=1):
            event = _observer_maintenance_history(row, row_number)
            proposal = proposals.get(event["proposal_id"])
            if proposal is None:
                raise StateError("maintenance history references an unknown proposal")
            if (
                event["proposal_id"] != proposal["proposal_id"]
                or event["operation"] != proposal["operation"]
                or event["request_id"] != proposal["request_id"]
                or event["evidence_sha256"] != proposal["evidence_sha256"]
            ):
                raise StateError("maintenance history does not match proposal")
            if event["proposal_id"] in applied:
                raise StateError("duplicate maintenance history event")
            applied[event["proposal_id"]] = event

        approvals: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(approval_rows, start=1):
            approval = _observer_approval_row(row, row_number)
            proposal_id = approval["proposal_id"]
            if proposal_id not in proposals:
                raise StateError("approval references an unknown proposal")
            previous = approvals.get(proposal_id)
            if previous is None and approval["event"] == "approved":
                raise StateError("approval approval event starts without pending")
            if previous is not None:
                if approval["event"] != "approved" or previous["approved"]:
                    raise StateError("approval events are out of order")
                if not previous["required"]:
                    raise StateError("approval classification changed")
            approvals[proposal_id] = approval
        if not approvals and facade._approval_memory:
            for proposal_id, state in facade._approval_memory.items():
                if proposal_id not in proposals:
                    raise StateError("approval references an unknown proposal")
                required, approved, digest, length = state
                approvals[proposal_id] = {
                    "proposal_id": proposal_id,
                    "required": required,
                    "approved": approved,
                    # The in-memory approval state has no durable timestamp.
                    "created_at": None,
                    "digest": digest,
                    "length": length,
                    "event": "approved" if approved else "pending",
                }
    except Exception as exc:  # noqa: BLE001 - observer fails closed
        del exc
        return (_observer_integrity_fact("maintenance", target_date=target_date),)

    facts: list[ObservationFact] = []
    for proposal_id, proposal in proposals.items():
        applied_event = applied.get(proposal_id)
        if applied_event is not None:
            code = (
                "maintenance_archive_applied"
                if proposal["operation"] == "retire"
                else "maintenance_applied"
            )
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.proposal:{proposal_id}",
                    code=code,
                    state="neutral",
                    target_date=target_date,
                    event_time=applied_event["created_at"],
                    refs=_observer_refs(proposal_id, applied_event["event_id"]),
                    counts={"maintenance_applied": 1},
                )
            )
            continue
        facts.append(
            ObservationFact(
                key=f"memory.maintenance.proposal:{proposal_id}",
                code="maintenance_proposal_pending",
                state="current",
                target_date=target_date,
                event_time=proposal["created_at"],
                refs=_observer_refs(proposal_id, proposal["request_id"]),
                counts={"maintenance_proposals": 1},
            )
        )
        if proposal["operation"] == "retire":
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.archive:{proposal_id}",
                    code="maintenance_archive_proposal",
                    state="neutral",
                    target_date=target_date,
                    event_time=proposal["created_at"],
                    refs=_observer_refs(proposal_id),
                    counts={"maintenance_archives": 1},
                )
            )

    for proposal_id, approval in approvals.items():
        refs = _observer_refs(proposal_id)
        if approval["digest"] is not None:
            refs = _observer_refs(*refs, approval["digest"])
        if approval["required"] and not approval["approved"]:
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.approval:{proposal_id}",
                    code="maintenance_approval_pending",
                    state="current",
                    target_date=target_date,
                    event_time=approval["created_at"],
                    refs=refs,
                    counts={"approval_pending": 1},
                )
            )
        elif approval["approved"]:
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.approval:{proposal_id}",
                    code="maintenance_approval_verified",
                    state="neutral",
                    target_date=target_date,
                    event_time=approval["created_at"],
                    refs=refs,
                    counts={
                        "approval_verified": 1,
                        "approval_evidence_length": approval["length"] or 0,
                    },
                )
            )
    return _observer_merge_facts(facts)


@dataclass(frozen=True, slots=True)
class WriterRequest:
    """Typed, transient handoff envelope; only its descriptor is durable."""

    effect_id: str
    operation: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    content_sha256: str
    content_length: int
    attempt: int
    content: Any


@dataclass(frozen=True, slots=True)
class WriterHandoff:
    operation: str
    effect_id: str
    record: Any
    error_type: str | None = None
    request: WriterRequest | None = None

    @property
    def queued(self) -> bool:
        return self.record.state == "executed_unverified"

    @property
    def verified(self) -> bool:
        return self.record.state == "verified"

    @property
    def failed(self) -> bool:
        return self.record.state == "failed"


class WriterCoordinator:
    """Create receipt-backed writer intents before handing work to an adapter."""

    def __init__(
        self, effect_ledger: Any, *, clock: Callable[[], datetime] = utc_now
    ) -> None:
        required = (
            "get",
            "begin_intent",
            "mark_pending",
            "mark_queue_accepted",
            "verify",
            "fail",
        )
        if any(not callable(getattr(effect_ledger, name, None)) for name in required):
            raise TypeError("effect_ledger does not provide the required port")
        self.effect_ledger = effect_ledger
        self.clock = clock
        self._handoff_thread_lock = Lock()
        lock_path = getattr(effect_ledger, "mutation_lock_path", None)
        if lock_path is None:
            lock_path = getattr(effect_ledger, "mutation_lock", None)
        self._handoff_lock_path = (
            None
            if lock_path is None
            else Path(lock_path).with_name(f"{Path(lock_path).name}.writer")
        )

    @contextmanager
    def _claim_lock(self):
        """Serialize the intent-to-pending claim across threads/processes."""

        with self._handoff_thread_lock:
            if self._handoff_lock_path is None:
                yield
            else:
                with file_lock(self._handoff_lock_path):
                    yield

    def create_intent(
        self,
        operation: str,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> EffectRecord:
        if operation not in WRITER_OPERATIONS:
            raise ValueError(f"unsupported memory writer operation: {operation}")
        digest, length = content_descriptor(content)
        created_at = self.clock()
        deadline = (
            _time(expires_at, "expires_at")
            if expires_at is not None
            else created_at + ttl
        )
        return self.effect_ledger.begin_intent(
            effect_id,
            kind=operation,
            source_event_id=_text(source_event_id, "source_event_id"),
            idempotency_key=_text(idempotency_key, "idempotency_key"),
            epoch_id=_text(epoch_id, "epoch_id"),
            content_sha256=digest,
            content_length=length,
            expires_at=deadline,
            created_at=created_at,
        )

    def handoff(
        self,
        effect_id: str,
        writer: Any,
        *,
        content: Any = None,
        operation: str | None = None,
    ) -> WriterHandoff:
        with self._claim_lock():
            record = self.effect_ledger.get(effect_id)
            if record is None:
                raise ValueError(f"writer effect does not exist: {effect_id}")
            actual_operation = record.kind if operation is None else operation
            if record.state not in {"intent", "requeued"}:
                return WriterHandoff(actual_operation, effect_id, record)
            if content is None:
                raise ValueError("first writer handoff requires transient content")
            digest, length = content_descriptor(content)
            if digest != record.content_sha256 or length != record.content_length:
                raise ValueError("writer content does not match effect descriptor")
            record = self.effect_ledger.mark_pending(effect_id)
            if record.state != "pending":
                raise StateError("effect ledger did not claim writer intent")
            request = WriterRequest(
                effect_id=record.effect_id,
                operation=record.kind,
                source_event_id=record.source_event_id,
                idempotency_key=record.idempotency_key,
                epoch_id=record.epoch_id,
                content_sha256=record.content_sha256,
                content_length=record.content_length,
                attempt=record.attempt,
                content=content,
            )
        try:
            method = writer if callable(writer) else getattr(writer, "write", None)
            if method is None and not callable(writer):
                method = getattr(writer, "persist", None)
            if not callable(method):
                raise TypeError("writer must be callable or provide write/persist")
            result = method(request)
            if isinstance(result, EffectReceipt):
                try:
                    record = self.effect_ledger.verify(effect_id, result)
                except Exception as exc:  # noqa: BLE001 - receipt mismatch is visible
                    record = self.effect_ledger.fail(
                        effect_id,
                        f"writer receipt mismatch: {type(exc).__name__}",
                        retryable=True,
                    )
                    return WriterHandoff(
                        actual_operation, effect_id, record, type(exc).__name__, request
                    )
            else:
                record = self.effect_ledger.mark_queue_accepted(effect_id)
        except Exception as exc:  # noqa: BLE001 - failure must become visible ledger state
            record = self.effect_ledger.fail(
                effect_id,
                f"writer handoff failed: {type(exc).__name__}",
                retryable=True,
            )
            return WriterHandoff(
                actual_operation, effect_id, record, type(exc).__name__, request
            )
        return WriterHandoff(actual_operation, effect_id, record, request=request)

    def verify(self, effect_id: str, receipt: EffectReceipt) -> WriterHandoff:
        record = self.effect_ledger.verify(effect_id, receipt)
        return WriterHandoff(record.kind, effect_id, record)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project effect state without reconciliation, expiry, or writer calls."""

        effective_now = _observer_validate_context(target_date, now)
        del effective_now
        return _observer_writer_facts(
            self.effect_ledger,
            target_date=target_date,
            now=now,
        )

    def submit(
        self,
        operation: str,
        writer: Any,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> WriterHandoff:
        intent = self.create_intent(
            operation,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            content=content,
            expires_at=expires_at,
            ttl=ttl,
            effect_id=effect_id,
        )
        return self.handoff(
            intent.effect_id, writer, content=content, operation=operation
        )


def _observer_writer_facts(
    effect_ledger: Any,
    *,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Fallback effect projection for a path-backed injected effect port."""

    owner = getattr(effect_ledger, "ledger", None)
    path = getattr(owner, "path", None)
    if path is None:
        return (
            ObservationFact(
                key="memory.writer.adapter",
                code="writer_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("writer",),
            ),
        )
    try:
        raw_rows = _observer_jsonl_rows(path)
        records: list[tuple[str, EffectRecord]] = []
        by_effect: dict[str, list[EffectRecord]] = {}
        idempotency: dict[str, str] = {}
        for row_number, row in enumerate(raw_rows, start=1):
            operation = row.get("operation")
            if operation not in {
                "begin_intent",
                "mark_pending",
                "mark_queue_accepted",
                "verify",
                "fail",
                "expire",
                "requeue",
            }:
                raise StateError(f"writer row {row_number} has an invalid operation")
            record = EffectRecord.from_dict(row)
            records.append((operation, record))
            previous = by_effect.get(record.effect_id, [])
            if not previous:
                if operation != "begin_intent" or record.state != "intent":
                    raise StateError("writer ledger starts after intent")
                if record.attempt != 1:
                    raise StateError("writer ledger has an invalid initial attempt")
            else:
                prior = previous[-1]
                if (
                    prior.effect_id != record.effect_id
                    or prior.kind != record.kind
                    or prior.source_event_id != record.source_event_id
                    or prior.idempotency_key != record.idempotency_key
                    or prior.epoch_id != record.epoch_id
                    or prior.created_at != record.created_at
                    or prior.content_sha256 != record.content_sha256
                    or prior.content_length != record.content_length
                ):
                    raise StateError("writer ledger changes immutable identity")
                if not _valid_transition(prior, record, operation):
                    raise StateError("writer ledger contains an out-of-order event")
            previous_effect = idempotency.get(record.idempotency_key)
            if previous_effect is not None and previous_effect != record.effect_id:
                raise StateError("writer ledger reuses an idempotency key")
            idempotency[record.idempotency_key] = record.effect_id
            by_effect.setdefault(record.effect_id, []).append(record)
    except Exception as exc:  # noqa: BLE001 - content-free fail-closed status
        del exc
        return (_observer_integrity_fact("writer", target_date=target_date),)
    if not records:
        return ()

    facts: list[ObservationFact] = []
    for history in by_effect.values():
        current = history[-1]
        projected_state = current.state
        if (
            current.state in {"pending", "executed_unverified"}
            and current.expires_at < now
        ):
            # Projection only: never call EffectLedger.expire().
            projected_state = "expired"
        refs = [
            f"effect:{current.effect_id}",
            f"source:{current.source_event_id}",
            f"sha256:{current.content_sha256}",
        ]
        counts = {
            "effects": 1,
            "attempt": current.attempt,
            "content_length": current.content_length,
            f"state_{projected_state}": 1,
        }
        if current.receipt is not None:
            refs.append(f"receipt:{current.receipt.receipt_id}")
        if current.state == "verified":
            prior_states = {record.state for record in history[:-1]}
            recovery = None
            fact_state = "neutral"
            if prior_states & _WRITER_CURRENT_STATES:
                receipt = current.receipt
                if receipt is not None:
                    recovery = RecoveryEvidence(
                        f"receipt:{receipt.receipt_id}",
                        "effect_verified",
                        receipt.observed_at,
                    )
                    fact_state = "recovered_history"
            facts.append(
                ObservationFact(
                    key=f"memory.writer.effect:{current.effect_id}",
                    code="effect_verified",
                    state=fact_state,
                    target_date=target_date,
                    event_time=(current.observed_at or current.created_at),
                    refs=tuple(refs),
                    counts=counts,
                    recovery=recovery,
                )
            )
            continue
        if projected_state in _WRITER_CURRENT_STATES:
            fact_state = "current"
        else:
            fact_state = "neutral"
        facts.append(
            ObservationFact(
                key=f"memory.writer.effect:{current.effect_id}",
                code=f"effect_{projected_state}",
                state=fact_state,
                target_date=target_date,
                event_time=(
                    current.expires_at
                    if projected_state == "expired"
                    else current.created_at
                ),
                refs=tuple(refs),
                counts=counts,
            )
        )
    return _observer_merge_facts(facts)


class MemoryOrchestrator:
    """Facade joining source ports, exposure ledger, injected stores, and effects."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        memory_store: Any = None,
        session_store: Any = None,
        effect_ledger: EffectLedger | None = None,
        retriever: Any = None,
        opener: Any = None,
        source_registry: SourceRegistry | None = None,
        exposure_ledger: ExposureLedger | None = None,
        policy: ExposurePolicy | None = None,
        continuity_policy: Callable[[str, str], bool] | None = None,
        approval_adapter: Any = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.clock = clock
        self.memory_store = memory_store
        self.session_store = session_store
        self.effect_ledger = effect_ledger
        if root is None and memory_store is not None:
            cards = getattr(memory_store, "cards", None)
            path = getattr(cards, "path", None)
            if path is not None:
                root = Path(path).parent
        if exposure_ledger is not None:
            self.exposures = exposure_ledger
        else:
            if root is None:
                raise ValueError("root or exposure_ledger is required")
            self.exposures = ExposureLedger(root, clock=clock)
        self.policy = policy or ExposurePolicy()
        self.continuity_policy = continuity_policy
        self.sources = source_registry or SourceRegistry(retriever, opener)
        self.writer = (
            None
            if effect_ledger is None
            else WriterCoordinator(effect_ledger, clock=clock)
        )
        self.maintenance = (
            None
            if memory_store is None
            else MemoryMaintenanceFacade(
                memory_store,
                approval_adapter=approval_adapter,
                root=root,
                clock=clock,
            )
        )

    @staticmethod
    def _context(
        value: Any, *, observed_at: datetime | None = None, turn_index: int = 0
    ) -> ExposureContext:
        if isinstance(value, ExposureContext):
            return value
        return ExposureContext.from_session(
            value, observed_at=observed_at, turn_index=turn_index
        )

    def retrieve(
        self,
        query: str,
        *,
        context: ExposureContext | Any,
        limit: int | None = None,
    ) -> tuple[SourceCandidate, ...]:
        self._context(context)
        budget = (
            self.policy.result_budget
            if limit is None
            else min(limit, self.policy.result_budget)
        )
        return self.sources.retrieve(query, limit=budget)

    def plan(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        now: datetime | None = None,
    ) -> ExposurePlan:
        actual_context = self._context(context)
        return self.policy.choose(
            candidates,
            context=actual_context,
            ledger=self.exposures,
            now=self.clock() if now is None else now,
            continuity_policy=self.continuity_policy,
            first_turn=first_turn,
        )

    def expose_candidates(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        now: datetime | None = None,
    ) -> tuple[ExposedSource, ...]:
        actual_context = self._context(context)
        plan = self.plan(
            candidates, context=actual_context, first_turn=first_turn, now=now
        )
        results: list[ExposedSource] = []
        for candidate in plan.candidates:
            selected = self.exposures.record_selected(
                candidate, context=actual_context, now=now
            )
            material = self.open_selected(
                selected.exposure_id, context=actual_context, now=now
            )
            exposed = self.exposures.record_exposed(
                selected.exposure_id,
                exposure_cap=self.policy.max_per_session,
                now=now,
            )
            results.append(ExposedSource(exposed, material))
        return tuple(results)

    def expose_query(
        self,
        query: str,
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        limit: int | None = None,
        now: datetime | None = None,
    ) -> tuple[ExposedSource, ...]:
        candidates = self.retrieve(query, context=context, limit=limit)
        return self.expose_candidates(
            candidates, context=context, first_turn=first_turn, now=now
        )

    def open_selected(
        self,
        exposure_id: str,
        *,
        candidate: SourceCandidate | None = None,
        opener: Any = None,
        max_bytes: int = _MAX_SOURCE_BYTES,
        context: ExposureContext | Any | None = None,
        now: datetime | None = None,
    ) -> SourceMaterial:
        state = self.exposures.get(exposure_id)
        if state is None:
            raise MissingEvidenceError(f"exposure does not exist: {exposure_id}")
        if state.state not in {"selected", "opened", "exposed", "used", "consumed"}:
            raise ValueError(
                "exact open requires a selected or previously opened exposure"
            )
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_context = (
            self._context(context)
            if context is not None
            else ExposureContext(
                state.session_id,
                state.lifecycle_id,
                state.turn_id,
                state.context_source_kind,
                effective_now,
            )
        )
        if (
            actual_context.session_id != state.session_id
            or actual_context.lifecycle_id != state.lifecycle_id
            or actual_context.turn_id != state.turn_id
            or actual_context.source_kind != state.context_source_kind
        ):
            raise ExposureConflictError("exact-open context does not match selection")
        source_candidate = candidate or SourceCandidate(
            source_ref=state.source_ref,
            source_class=state.source_class,
            source_event_time=state.source_event_time,
            created_at=state.source_created_at,
            expires_at=state.source_expires_at,
            content_sha256=state.content_sha256,
            content_length=state.content_length,
        )
        if ExposureLedger._candidate_identity(source_candidate) != (
            state.source_ref,
            state.source_class,
            state.source_event_time,
            state.source_created_at,
            state.source_expires_at,
        ):
            raise ExposureConflictError("exact-open candidate does not match exposure")
        if state.content_sha256 is not None and (
            source_candidate.content_sha256 != state.content_sha256
            or source_candidate.content_length != state.content_length
        ):
            raise ExposureConflictError(
                "exact-open content descriptor does not match exposure"
            )
        try:
            if (
                source_candidate.expires_at is not None
                and effective_now >= source_candidate.expires_at
            ):
                raise ExpiredEvidenceError(
                    f"source evidence has expired: {state.source_ref}"
                )
            registry = self.sources if opener is None else SourceRegistry(opener=opener)
            material = registry.exact_open(source_candidate, max_bytes=max_bytes)
            if material.expires_at is not None and effective_now >= material.expires_at:
                raise ExpiredEvidenceError(
                    f"opened source evidence has expired: {state.source_ref}"
                )
            historical = (
                material.source_event_time.date() < actual_context.observed_at.date()
            )
            material = replace(
                material,
                framing="historical" if historical else "current",
                framing_date=material.source_event_time.date(),
            )
            self.exposures.record_opened(exposure_id, material, now=effective_now)
            return material
        except (MissingEvidenceError, ExpiredEvidenceError, ValueError) as exc:
            if state.state == "selected":
                self.exposures.record_open_failed(
                    exposure_id,
                    type(exc).__name__.lower(),
                    now=effective_now,
                )
            raise

    def mark_used(self, *args: Any, **kwargs: Any) -> ExposureRecord:
        return self.exposures.record_used(*args, **kwargs)

    def mark_consumed(self, *args: Any, **kwargs: Any) -> ExposureRecord:
        return self.exposures.record_consumed(*args, **kwargs)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Aggregate owner facts exactly once, with no orchestration side effects."""

        effective_now = _observer_validate_context(target_date, now)
        facts: list[ObservationFact] = []
        if isinstance(self.exposures, ExposureLedger):
            facts.extend(
                _observer_exposure_facts(
                    self.exposures,
                    target_date=target_date,
                    now=effective_now,
                    policy=self.policy,
                )
            )
        else:
            exposure_adapter = getattr(self.exposures, "observer_status", None)
            if callable(exposure_adapter):
                result = exposure_adapter(target_date=target_date, now=now)
                if isinstance(result, (str, bytes, bytearray, Mapping)):
                    raise TypeError("exposure observer result must be an iterable")
                values = tuple(result)
                if any(not isinstance(item, ObservationFact) for item in values):
                    raise TypeError(
                        "exposure observer result contains a malformed fact"
                    )
                facts.extend(values)
            else:
                facts.extend(
                    _observer_exposure_facts(
                        self.exposures,
                        target_date=target_date,
                        now=effective_now,
                        policy=self.policy,
                    )
                )
        if self.writer is not None:
            facts.extend(self.writer.observer_status(target_date=target_date, now=now))
        if self.maintenance is not None:
            facts.extend(
                self.maintenance.observer_status(
                    target_date=target_date,
                    now=now,
                )
            )
        if self.memory_store is not None:
            adapter = getattr(self.memory_store, "observer_status", None)
            if callable(adapter):
                result = adapter(target_date=target_date, now=now)
                if isinstance(result, (str, bytes, bytearray, Mapping)):
                    raise TypeError("memory observer result must be an iterable")
                values = tuple(result)
                if any(not isinstance(item, ObservationFact) for item in values):
                    raise TypeError("memory observer result contains a malformed fact")
                facts.extend(values)
            else:
                facts.append(
                    ObservationFact(
                        key="memory.store.adapter",
                        code="memory_adapter_unavailable",
                        state="neutral",
                        target_date=target_date,
                        refs=("memory_store",),
                    )
                )
        return _observer_merge_facts(facts)


__all__ = [
    "EXPOSURE_EVENTS",
    "EXPOSURE_STATES",
    "ORCHESTRATION_SCHEMA",
    "WRITER_OPERATIONS",
    "ExpiredEvidenceError",
    "ExposureConflictError",
    "ExposureContext",
    "ExposureLedger",
    "ExposurePlan",
    "ExposurePolicy",
    "ExposureRecord",
    "ExposedSource",
    "MemoryMaintenanceFacade",
    "MemoryOrchestrator",
    "MaintenanceApprovalAdapter",
    "MissingEvidenceError",
    "OrchestrationError",
    "PolicyDeniedError",
    "ReplyUseEvidence",
    "SourceCandidate",
    "SourceMaterial",
    "SourceOpener",
    "SourceRegistry",
    "SourceRetriever",
    "WriterCoordinator",
    "WriterHandoff",
    "WriterRequest",
    "content_descriptor",
]
