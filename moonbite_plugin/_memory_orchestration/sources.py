"""Opaque source retrieval and bounded exact-open adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .contracts import (
    MissingEvidenceError,
    SourceCandidate,
    SourceMaterial,
    _MAX_SOURCE_BYTES,
    _text,
)


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


__all__ = ()
