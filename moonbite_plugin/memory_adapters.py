"""Portable adapters that expose local memory through the source port.

The adapter deliberately keeps the memory store behind a small structural port.
Retrieval rebuilds every candidate from an exact local open, while opening a
selected reference reconstructs the same identity and returns only bounded,
transient source material.  No ledger or other durable state is written here.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
import math
from typing import Any, Protocol

from .memory import ExternalHit
from .memory_orchestration import (
    MissingEvidenceError,
    SourceCandidate,
    SourceMaterial,
    content_descriptor,
)
from .runtime_core import StateError, as_utc, ensure_bounded_text, parse_time


PRIVATE_CONTINUITY_SOURCE_CLASS = "private_continuity"
MAX_SOURCE_BYTES = 64 * 1024
MAX_REFERENCE_BYTES = 1 * 1024
MAX_QUERY_BYTES = 16 * 1024


class MemoryStorePort(Protocol):
    """Minimal read-only surface required from a memory store."""

    def open(self, open_ref: str) -> Mapping[str, Any] | None: ...

    def lexical_recall(self, query: str, *, limit: int) -> Iterable[Any]: ...


class ExternalRetrieverPort(Protocol):
    """Legacy host-owned search surface returning opaque references only."""

    def search(self, query: str, *, limit: int) -> Iterable[Any]: ...


@dataclass(frozen=True, slots=True)
class _SourceRecord:
    source_ref: str
    source_event_time: datetime
    created_at: datetime
    body: str


def _validate_query(query: str) -> str:
    if type(query) is not str or not query.strip():
        raise ValueError("query must be a non-empty string")
    ensure_bounded_text(query, "query", max_bytes=MAX_QUERY_BYTES)
    return query


def _validate_limit(limit: int) -> int:
    if type(limit) is not int or limit <= 0:
        raise ValueError("limit must be a positive integer")
    return limit


def _parse_source_ref(source_ref: Any) -> tuple[str, str] | None:
    if type(source_ref) is not str or not source_ref:
        return None
    try:
        ensure_bounded_text(
            source_ref,
            "source_ref",
            max_bytes=MAX_REFERENCE_BYTES,
        )
    except ValueError:
        return None
    prefix, separator, identifier = source_ref.partition(":")
    if not separator or not identifier or prefix not in {"card", "diary"}:
        return None
    return prefix, identifier


def _timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        try:
            return as_utc(value)
        except ValueError as exc:
            raise ValueError(f"{label} must be timezone-aware") from exc
    if type(value) is str:
        try:
            return parse_time(value)
        except (StateError, ValueError) as exc:
            raise ValueError(f"{label} must be an ISO timestamp") from exc
    raise ValueError(f"{label} must be a timezone-aware timestamp")


def _event_timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        return _timestamp(value, label)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if type(value) is str:
        try:
            return parse_time(value)
        except (StateError, ValueError):
            try:
                return datetime.combine(date.fromisoformat(value), time.min, tzinfo=UTC)
            except ValueError as exc:
                raise ValueError(
                    f"{label} must be an ISO date or timezone-aware timestamp"
                ) from exc
    raise ValueError(f"{label} must be an ISO date or timezone-aware timestamp")


def _diary_timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        raise ValueError(f"{label} must be an ISO date")
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if type(value) is str:
        try:
            return datetime.combine(date.fromisoformat(value), time.min, tzinfo=UTC)
        except ValueError as exc:
            raise ValueError(f"{label} must be an ISO date") from exc
    raise ValueError(f"{label} must be an ISO date")


def _text_field(record: Mapping[str, Any], key: str, label: str) -> str:
    value = record.get(key)
    if type(value) is not str or not value.strip():
        raise StateError(f"memory record {label} is invalid")
    return value


def _coerce_record(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, Mapping):
            return result
    raise StateError("memory store open returned an invalid record")


def _score(value: Any, label: str) -> float:
    if value is None:
        return 0.0
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise StateError(f"{label} must be a finite number or null")
    return float(value)


def _opaque_hit(value: Any, *, label: str) -> tuple[str, float]:
    if isinstance(value, ExternalHit):
        return value.open_ref, _score(value.score, f"{label} score")
    if isinstance(value, Mapping):
        try:
            if set(value) - {"open_ref", "score"}:
                raise StateError(
                    f"{label} contains source material or unsupported fields"
                )
            hit = ExternalHit.from_dict(value)
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise StateError(f"{label} is invalid") from exc
        return hit.open_ref, _score(hit.score, f"{label} score")

    open_ref = getattr(value, "open_ref", None)
    if open_ref is None:
        raise StateError(f"{label} is invalid")
    score = getattr(value, "score", None)
    if type(open_ref) is not str or not open_ref.strip():
        raise StateError(f"{label} open_ref is invalid")
    return open_ref, _score(score, f"{label} score")


def _diary_body(record: Mapping[str, Any]) -> str:
    title = _text_field(record, "title", "title")
    body = _text_field(record, "body", "body")
    # This is the stable, bounded format used for diary source material.
    return f"{title}: {body}"


class MemoryStoreSourceAdapter:
    """Expose card and diary records as opaque source-port values.

    ``external_retriever`` is optional and is intentionally treated as an
    opaque reference index.  Local exact opens remain authoritative for both
    candidates and material.
    """

    def __init__(
        self,
        memory_store: MemoryStorePort,
        external_retriever: ExternalRetrieverPort | None = None,
    ) -> None:
        if memory_store is None:
            raise TypeError("memory_store is required")
        self.memory_store = memory_store
        self.external_retriever = external_retriever

    def _open_record(self, source_ref: str) -> Mapping[str, Any] | None:
        # The caller has already validated the prefix and passes this exact
        # reference through without normalization or reconstruction.
        return _coerce_record(self.memory_store.open(source_ref))

    def _source_from_record(
        self,
        source_ref: str,
        record: Mapping[str, Any] | None,
    ) -> _SourceRecord | None:
        parsed = _parse_source_ref(source_ref)
        if parsed is None or record is None:
            return None
        kind, identifier = parsed
        if record.get("kind") != kind:
            return None

        if kind == "card":
            if record.get("card_id") != identifier:
                return None
            if (
                record.get("lifecycle_status") != "active"
                or record.get("history_status") != "current"
            ):
                return None
            event_time = _event_timestamp(
                record.get("event_time"),
                "card event_time",
            )
            body = _text_field(record, "summary", "summary")
        else:
            if record.get("entry_id") != identifier:
                return None
            event_time = _diary_timestamp(record.get("day"), "diary day")
            body = _diary_body(record)

        created_at = _timestamp(record.get("created_at"), "memory created_at")
        return _SourceRecord(source_ref, event_time, created_at, body)

    def _candidate(
        self,
        source_ref: str,
        relevance: float,
    ) -> SourceCandidate | None:
        if _parse_source_ref(source_ref) is None:
            return None
        source = self._source_from_record(
            source_ref,
            self._open_record(source_ref),
        )
        if source is None:
            return None
        digest, length = content_descriptor(source.body)
        return SourceCandidate(
            source_ref=source.source_ref,
            source_class=PRIVATE_CONTINUITY_SOURCE_CLASS,
            source_event_time=source.source_event_time,
            created_at=source.created_at,
            content_sha256=digest,
            content_length=length,
            relevance=relevance,
        )

    def _candidates(
        self,
        hits: Iterable[tuple[str, float]],
        *,
        limit: int,
    ) -> tuple[SourceCandidate, ...]:
        result: list[SourceCandidate] = []
        seen: set[str] = set()
        for source_ref, relevance in hits:
            if source_ref in seen:
                continue
            seen.add(source_ref)
            candidate = self._candidate(source_ref, relevance)
            if candidate is None:
                continue
            result.append(candidate)
            if len(result) >= limit:
                break
        return tuple(result)

    def _external_hits(
        self,
        query: str,
        *,
        limit: int,
    ) -> list[tuple[str, float]]:
        if self.external_retriever is None:
            return []
        search = getattr(self.external_retriever, "search", None)
        if not callable(search):
            raise TypeError("external retriever must provide search")
        # Provider exceptions intentionally propagate to the caller.
        raw_hits = search(query, limit=limit)
        try:
            iterator = iter(raw_hits)
        except TypeError as exc:
            raise StateError("external retriever returned a non-iterable") from exc
        result: list[tuple[str, float]] = []
        for index, value in enumerate(iterator):
            if index >= limit:
                break
            result.append(_opaque_hit(value, label="external hit"))
        return result

    def _lexical_hits(self, query: str, *, limit: int) -> list[tuple[str, float]]:
        lexical_recall = getattr(self.memory_store, "lexical_recall", None)
        if not callable(lexical_recall):
            raise TypeError("memory store must provide lexical_recall")
        raw_hits = lexical_recall(query, limit=limit)
        try:
            iterator = iter(raw_hits)
        except TypeError as exc:
            raise StateError(
                "memory store lexical_recall returned a non-iterable"
            ) from exc
        result: list[tuple[str, float]] = []
        for index, value in enumerate(iterator):
            if index >= limit:
                break
            result.append(_opaque_hit(value, label="lexical hit"))
        return result

    def retrieve(self, query: str, *, limit: int) -> tuple[SourceCandidate, ...]:
        """Return deterministic, descriptor-only candidates in source order."""

        query = _validate_query(query)
        limit = _validate_limit(limit)

        external = self._candidates(
            self._external_hits(query, limit=limit),
            limit=limit,
        )
        if external:
            return external
        return self._candidates(
            self._lexical_hits(query, limit=limit),
            limit=limit,
        )

    def open(
        self,
        source_ref: str,
        *,
        max_bytes: int,
    ) -> SourceMaterial | None:
        """Open one exact memory reference with a strict UTF-8 byte bound."""

        if type(max_bytes) is not int or max_bytes <= 0 or max_bytes > MAX_SOURCE_BYTES:
            raise ValueError("max_bytes is outside the bounded source limit")
        if _parse_source_ref(source_ref) is None:
            return None
        source = self._source_from_record(source_ref, self._open_record(source_ref))
        if source is None:
            return None
        encoded_length = len(source.body.encode("utf-8"))
        if encoded_length > max_bytes:
            raise MissingEvidenceError(
                "exact memory source exceeds the requested byte limit"
            )
        return SourceMaterial(
            source_ref=source.source_ref,
            source_class=PRIVATE_CONTINUITY_SOURCE_CLASS,
            source_event_time=source.source_event_time,
            created_at=source.created_at,
            body=source.body,
        )


# Short aliases make the adapter discoverable without coupling callers to one
# deployment-specific name.
MemorySourceAdapter = MemoryStoreSourceAdapter
MemorySourceRegistryAdapter = MemoryStoreSourceAdapter


__all__ = [
    "ExternalRetrieverPort",
    "MAX_QUERY_BYTES",
    "MAX_REFERENCE_BYTES",
    "MAX_SOURCE_BYTES",
    "MemorySourceAdapter",
    "MemorySourceRegistryAdapter",
    "MemoryStorePort",
    "MemoryStoreSourceAdapter",
    "PRIVATE_CONTINUITY_SOURCE_CLASS",
]
