"""Shared contracts and validation for memory orchestration.

These types carry no durable owner and create no state on their own.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Protocol

from ..runtime_core import (
    StateError,
    as_utc,
    ensure_bounded_json,
    ensure_bounded_text,
    isoformat,
    parse_time,
)

ORCHESTRATION_SCHEMA = "moon.memory.orchestration.v1"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REFERENCE_BYTES = 1_024
_MAX_CLASS_BYTES = 256
_MAX_METADATA_BYTES = 16 * 1024
_MAX_SOURCE_BYTES = 64 * 1024


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


__all__ = ()
