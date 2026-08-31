"""Append-only memory records, evidence-backed recall, and proposals."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

from .observer import ObservationFact
from .runtime_core import (
    JsonlLedger,
    StateError,
    ensure_bounded_json,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)

MEMORY_SCHEMA = "moon.memory.v1"
MEMORY_CARD_SCHEMA = "moon.memory.card.v2"
MAINTENANCE_SCHEMA = "moon.memory.maintenance.v1"
HISTORY_SCHEMA = "moon.memory.history.v1"
PROVENANCE = frozenset({"user_explicit", "agent_observation", "agent_inference"})
HISTORY_STATUSES = frozenset({"current", "historical", "corrected"})
LIFECYCLE_STATUSES = frozenset({"active", "archived"})
SUPERSESSION_KINDS = frozenset({"evolution", "correction", "dedupe"})
APPLY_PERMISSION_LEVEL = {"safe": 1, "reporting": 2, "manual": 3}
APPLY_REQUIRED_PERMISSION = {
    "merge": "safe",
    "retire": "reporting",
    "distill": "manual",
}

MAX_OPEN_REF_BYTES = 4 * 1024
MAX_RECALL_EXCERPT_BYTES = 2 * 1024
MAX_RECALL_REASON_BYTES = 4 * 1024
MAX_MAINTENANCE_PROPOSED_VALUE_BYTES = 16 * 1024
_CJK_FALLBACK_SCORE = 1
_CJK_CODEPOINT_RANGES = (
    (0x1100, 0x11FF),  # Hangul Jamo
    (0x3040, 0x309F),  # Hiragana
    (0x30A0, 0x30FF),  # Katakana
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # Han
    (0xAC00, 0xD7AF),  # Hangul syllables
    (0xF900, 0xFAFF),  # CJK compatibility ideographs
)


def _validated_event_time(value: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError("memory event_time is required")
    normalized = value.strip()
    ensure_bounded_text(normalized, "memory event_time", max_bytes=128)
    try:
        if len(normalized) == 10:
            date.fromisoformat(normalized)
        else:
            parse_time(normalized)
    except (ValueError, StateError) as exc:
        raise ValueError("memory event_time must be an ISO date or timestamp") from exc
    return normalized


def _validated_card_ids(values: Iterable[str], label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} must be an iterable of card ids")
    try:
        iterator = iter(values)
    except TypeError as exc:
        raise TypeError(f"{label} must be an iterable of card ids") from exc
    result: list[str] = []
    seen: set[str] = set()
    for value in iterator:
        if type(value) is not str or not value.strip():
            raise ValueError(f"{label} must contain non-empty card ids")
        card_id = value.strip()
        ensure_bounded_text(card_id, label, max_bytes=256)
        if card_id in seen:
            raise ValueError(f"{label} must contain unique card ids")
        seen.add(card_id)
        result.append(card_id)
    if len(result) > 64:
        raise ValueError(f"{label} supports at most 64 card ids")
    return tuple(result)


def _validated_text_items(
    values: Iterable[str],
    label: str,
    *,
    max_items: int,
    max_bytes: int,
    lowercase: bool = False,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} must be an iterable of strings")
    try:
        iterator = iter(values)
    except TypeError as exc:
        raise TypeError(f"{label} must be an iterable of strings") from exc
    result: list[str] = []
    seen: set[str] = set()
    for value in iterator:
        if type(value) is not str or not value.strip():
            raise ValueError(f"{label} must contain non-empty strings")
        normalized = value.strip()
        if lowercase:
            normalized = normalized.lower()
        ensure_bounded_text(normalized, label, max_bytes=max_bytes)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    if len(result) > max_items:
        raise ValueError(f"{label} supports at most {max_items} items")
    return tuple(result)


def _tokens(value: str) -> set[str]:
    return {part.lower() for part in re.findall(r"[\w\-]{2,}", value, re.UNICODE)}


def _has_cjk_query(value: str) -> bool:
    return (
        sum(
            any(start <= ord(character) <= end for start, end in _CJK_CODEPOINT_RANGES)
            for character in value
        )
        >= 2
    )


@dataclass(frozen=True)
class MemoryCard:
    card_id: str
    summary: str
    provenance: str
    source_ref: str
    created_at: datetime
    tags: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    event_time: str = ""
    entities: tuple[str, ...] = ()
    state_key: str | None = None
    history_status: str = "current"
    lifecycle_status: str = "active"
    supersedes: tuple[str, ...] = ()
    supersession_kind: str | None = None
    related_cards: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.provenance not in PROVENANCE:
            raise ValueError(f"invalid memory provenance: {self.provenance}")
        if (
            not self.card_id.strip()
            or not self.summary.strip()
            or not self.source_ref.strip()
        ):
            raise ValueError("memory card id, summary, and source_ref are required")
        ensure_bounded_text(self.card_id, "memory card id", max_bytes=256)
        ensure_bounded_text(self.summary, "memory summary", max_bytes=16 * 1024)
        ensure_bounded_text(self.source_ref, "memory source_ref", max_bytes=4 * 1024)
        _validated_event_time(self.event_time or isoformat(self.created_at))
        if len(self.tags) > 64:
            raise ValueError("memory card supports at most 64 tags")
        for tag in self.tags:
            ensure_bounded_text(tag, "memory tag", max_bytes=128)
        if len(self.entities) > 64:
            raise ValueError("memory card supports at most 64 entities")
        if len(set(self.entities)) != len(self.entities):
            raise ValueError("memory entities must be unique")
        for entity in self.entities:
            if type(entity) is not str or not entity.strip():
                raise ValueError("memory entities must be non-empty strings")
            ensure_bounded_text(entity, "memory entity", max_bytes=256)
        if self.state_key is not None:
            if type(self.state_key) is not str or not self.state_key.strip():
                raise ValueError("memory state_key must be null or a non-empty string")
            ensure_bounded_text(self.state_key, "memory state_key", max_bytes=512)
        if self.history_status not in HISTORY_STATUSES:
            raise ValueError(f"invalid memory history_status: {self.history_status}")
        if self.lifecycle_status not in LIFECYCLE_STATUSES:
            raise ValueError(
                f"invalid memory lifecycle_status: {self.lifecycle_status}"
            )
        _validated_card_ids(self.supersedes, "memory supersedes")
        _validated_card_ids(self.related_cards, "memory related_cards")
        if self.supersedes and self.supersession_kind not in SUPERSESSION_KINDS:
            raise ValueError("memory supersession_kind is required with supersedes")
        if not self.supersedes and self.supersession_kind is not None:
            raise ValueError("memory supersession_kind requires supersedes")
        ensure_bounded_json(self.metadata, "memory metadata", max_bytes=64 * 1024)

    @property
    def open_ref(self) -> str:
        return f"card:{self.card_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": MEMORY_CARD_SCHEMA,
            "kind": "card",
            "card_id": self.card_id,
            "summary": self.summary,
            "provenance": self.provenance,
            "source_ref": self.source_ref,
            "created_at": isoformat(self.created_at),
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
            "event_time": self.event_time or isoformat(self.created_at),
            "entities": list(self.entities),
            "state_key": self.state_key,
            "history_status": self.history_status,
            "lifecycle_status": self.lifecycle_status,
            "supersedes": list(self.supersedes),
            "supersession_kind": self.supersession_kind,
            "related_cards": list(self.related_cards),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MemoryCard:
        schema = value.get("schema_version")
        if schema not in {MEMORY_SCHEMA, MEMORY_CARD_SCHEMA}:
            raise StateError("memory card has an unsupported schema")
        tags = value.get("tags", [])
        if not isinstance(tags, list) or any(type(item) is not str for item in tags):
            raise StateError("memory tags must be a list of strings")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise StateError("memory metadata must be a mapping")
        entities = value.get("entities", [])
        supersedes = value.get("supersedes", [])
        related_cards = value.get("related_cards", [])
        for label, collection in (
            ("entities", entities),
            ("supersedes", supersedes),
            ("related_cards", related_cards),
        ):
            if not isinstance(collection, list) or any(
                type(item) is not str for item in collection
            ):
                raise StateError(f"memory {label} must be a list of strings")
        created_at = parse_time(str(value["created_at"]))
        return cls(
            card_id=str(value["card_id"]),
            summary=str(value["summary"]),
            provenance=str(value["provenance"]),
            source_ref=str(value["source_ref"]),
            created_at=created_at,
            tags=tuple(tags),
            metadata=dict(metadata),
            event_time=_validated_event_time(
                str(value.get("event_time") or isoformat(created_at))
            ),
            entities=tuple(entities),
            state_key=(
                None if value.get("state_key") is None else str(value.get("state_key"))
            ),
            history_status=str(value.get("history_status") or "current"),
            lifecycle_status=str(value.get("lifecycle_status") or "active"),
            supersedes=tuple(supersedes),
            supersession_kind=(
                None
                if value.get("supersession_kind") is None
                else str(value.get("supersession_kind"))
            ),
            related_cards=tuple(related_cards),
        )


@dataclass(frozen=True)
class DiaryEntry:
    entry_id: str
    day: date
    title: str
    body: str
    source_ref: str
    created_at: datetime

    def __post_init__(self) -> None:
        if (
            not self.entry_id.strip()
            or not self.title.strip()
            or not self.body.strip()
            or not self.source_ref.strip()
        ):
            raise ValueError("diary id, title, body, and source_ref are required")
        ensure_bounded_text(self.entry_id, "diary id", max_bytes=256)
        ensure_bounded_text(self.title, "diary title", max_bytes=1024)
        ensure_bounded_text(self.body, "diary body", max_bytes=128 * 1024)
        ensure_bounded_text(self.source_ref, "diary source_ref", max_bytes=16 * 1024)

    @property
    def open_ref(self) -> str:
        return f"diary:{self.entry_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": MEMORY_SCHEMA,
            "kind": "diary",
            "entry_id": self.entry_id,
            "day": self.day.isoformat(),
            "title": self.title,
            "body": self.body,
            "source_ref": self.source_ref,
            "created_at": isoformat(self.created_at),
        }


@dataclass(frozen=True)
class SearchHit:
    open_ref: str
    score: int
    excerpt: str
    source_ref: str
    provenance: str | None


def _validated_score(value: Any, label: str) -> int | float | None:
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ValueError(f"{label} must be a finite number or null")
    return value


def _validated_open_ref(value: Any, label: str = "open_ref") -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} is required")
    ensure_bounded_text(value, label, max_bytes=MAX_OPEN_REF_BYTES)
    return value


@dataclass(frozen=True)
class RecallCandidate:
    """A bounded, evidence-backed memory candidate.

    Candidates are transient.  They intentionally carry only a reference and
    a short, locally reconstructed excerpt; callers must use ``open_ref`` to
    retrieve the full evidence before adopting it.
    """

    open_ref: str
    score: int | float | None
    excerpt: str
    provenance: str | None

    def __post_init__(self) -> None:
        _validated_open_ref(self.open_ref)
        _validated_score(self.score, "recall score")
        if type(self.excerpt) is not str or not self.excerpt.strip():
            raise ValueError("recall excerpt is required")
        ensure_bounded_text(
            self.excerpt,
            "recall excerpt",
            max_bytes=MAX_RECALL_EXCERPT_BYTES,
        )
        if self.provenance is not None and (
            type(self.provenance) is not str or self.provenance not in PROVENANCE
        ):
            raise ValueError(f"invalid recall provenance: {self.provenance}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": MEMORY_SCHEMA,
            "kind": "recall_candidate",
            "open_ref": self.open_ref,
            "score": self.score,
            "excerpt": self.excerpt,
            "provenance": self.provenance,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RecallCandidate:
        if not isinstance(value, Mapping):
            raise StateError("recall candidate row must be an object")
        if (
            value.get("schema_version") != MEMORY_SCHEMA
            or value.get("kind") != "recall_candidate"
        ):
            raise StateError("recall candidate row has an unsupported schema")
        try:
            return cls(
                open_ref=value["open_ref"],
                score=value["score"],
                excerpt=value["excerpt"],
                provenance=value["provenance"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StateError("recall candidate row is invalid") from exc


@dataclass(frozen=True)
class ExternalHit:
    """The intentionally narrow result accepted from an external retriever."""

    open_ref: str
    score: float | None = None

    def __post_init__(self) -> None:
        _validated_open_ref(self.open_ref, "external open_ref")
        _validated_score(self.score, "external score")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ExternalHit:
        if not isinstance(value, Mapping):
            raise StateError("external hit must be an object")
        if set(value) - {"open_ref", "score"}:
            raise StateError("external hit contains unsupported fields")
        try:
            return cls(open_ref=value["open_ref"], score=value.get("score"))
        except (KeyError, TypeError, ValueError) as exc:
            raise StateError("external hit is invalid") from exc


class ExternalRetriever(Protocol):
    """Optional host-owned retriever port; no provider is selected here."""

    def search(self, query: str, *, limit: int) -> Iterable[ExternalHit]: ...


@dataclass(frozen=True)
class ResurfaceCandidate:
    """A pure, expiring suggestion; it never sends or wakes a host."""

    candidate_id: str
    open_ref: str
    created_at: datetime
    expires_at: datetime
    reason: str
    relevance: int | float

    def __post_init__(self) -> None:
        if type(self.candidate_id) is not str or not self.candidate_id.strip():
            raise ValueError("resurface candidate id is required")
        ensure_bounded_text(
            self.candidate_id,
            "resurface candidate id",
            max_bytes=256,
        )
        _validated_open_ref(self.open_ref)
        # isoformat validates timezone awareness before comparing timestamps.
        isoformat(self.created_at)
        isoformat(self.expires_at)
        if self.expires_at <= self.created_at:
            raise ValueError("resurface candidate must have a positive TTL")
        if type(self.reason) is not str or not self.reason.strip():
            raise ValueError("resurface candidate reason is required")
        ensure_bounded_text(
            self.reason,
            "resurface candidate reason",
            max_bytes=MAX_RECALL_REASON_BYTES,
        )
        if type(self.relevance) not in (int, float) or not math.isfinite(
            float(self.relevance)
        ):
            raise ValueError("resurface candidate relevance must be finite")

    def is_expired(self, at: datetime) -> bool:
        isoformat(at)
        return at >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": MEMORY_SCHEMA,
            "kind": "resurface_candidate",
            "candidate_id": self.candidate_id,
            "open_ref": self.open_ref,
            "created_at": isoformat(self.created_at),
            "expires_at": isoformat(self.expires_at),
            "reason": self.reason,
            "relevance": self.relevance,
        }


@dataclass(frozen=True)
class DiaryDraft:
    title: str
    body: str
    reason: str

    def __post_init__(self) -> None:
        if not self.title.strip() or not self.body.strip() or not self.reason.strip():
            raise ValueError("diary draft title, body, and reason are required")


class DiaryWriter(Protocol):
    def synthesize(
        self,
        *,
        day: date,
        evidence: list[Mapping[str, Any]],
        title_hint: str,
    ) -> DiaryDraft: ...


class MemoryStore:
    def __init__(self, root: Path, *, clock: Callable[[], datetime] = utc_now):
        self.cards = JsonlLedger(root / "memory_cards.jsonl")
        self.diary = JsonlLedger(root / "diary.jsonl")
        self.proposals = JsonlLedger(root / "hot_memory_proposals.jsonl")
        self.maintenance = JsonlLedger(root / "memory_maintenance.jsonl")
        self.history = JsonlLedger(root / "memory_history.jsonl")
        self.maintenance_request_lock = root / "memory_maintenance.request.lock"
        self.card_mutation_lock = root / "memory_cards.mutation.lock"
        self.clock = clock

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Report the card/diary adapter boundary without opening content.

        The native card and diary ledgers contain summaries and bodies.  This
        owner has no separate content-free telemetry envelope, so an observer
        probe must remain neutral instead of parsing those ledgers.
        """

        if type(target_date) is not date:
            raise ValueError("target_date must be a date")
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return (
            ObservationFact(
                key="memory.store.adapter",
                code="memory_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("memory_cards", "diary"),
            ),
        )

    def _base_card_rows(self) -> list[MemoryCard]:
        result: list[MemoryCard] = []
        seen: set[str] = set()
        for index, row in enumerate(self.cards.rows(), start=1):
            if (
                row.get("schema_version") not in {MEMORY_SCHEMA, MEMORY_CARD_SCHEMA}
                or row.get("kind") != "card"
            ):
                raise StateError(f"memory card row {index} has an unsupported schema")
            try:
                card = MemoryCard.from_dict(row)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"memory card row {index} is invalid") from exc
            if card.card_id in seen:
                raise StateError(f"duplicate memory card id: {card.card_id}")
            seen.add(card.card_id)
            result.append(card)
        return result

    def _history_rows(self) -> list[dict[str, Any]]:
        expected_fields = {
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
        result: list[dict[str, Any]] = []
        seen_events: set[str] = set()
        seen_proposals: set[str] = set()
        for index, row in enumerate(self.history.rows(), start=1):
            try:
                if set(row) != expected_fields:
                    raise ValueError("unsupported history fields")
                if (
                    row["schema_version"] != HISTORY_SCHEMA
                    or row["kind"] != "maintenance_applied"
                    or row["status"] != "applied"
                ):
                    raise ValueError("unsupported history schema")
                event_id = _validated_open_ref(row["event_id"], "history event_id")
                proposal_id = _validated_open_ref(
                    row["proposal_id"], "history proposal_id"
                )
                _validated_open_ref(row["request_id"], "history request_id")
                parse_time(row["created_at"])
                operation = row["operation"]
                if operation not in {"merge", "retire", "distill"}:
                    raise ValueError("unsupported history operation")
                activity = row["activity"]
                permission = row["permission"]
                if type(activity) is not str or not activity.strip():
                    raise ValueError("history activity is required")
                ensure_bounded_text(activity, "history activity", max_bytes=256)
                if permission not in APPLY_PERMISSION_LEVEL:
                    raise ValueError("history permission is invalid")
                digest = row["evidence_sha256"]
                if (
                    type(digest) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                ):
                    raise ValueError("history evidence_sha256 is invalid")
                archived = row["archived_card_ids"]
                if not isinstance(archived, list):
                    raise TypeError("history archived_card_ids must be a list")
                _validated_card_ids(archived, "history archived_card_ids")
                created = row["created_card"]
                if created is not None:
                    if not isinstance(created, Mapping):
                        raise TypeError(
                            "history created_card must be an object or null"
                        )
                    if created.get("schema_version") != MEMORY_CARD_SCHEMA:
                        raise ValueError(
                            "history created_card must use the v2 card schema"
                        )
                    MemoryCard.from_dict(created)
                if operation == "retire" and (created is not None or not archived):
                    raise ValueError("retire history must archive cards only")
                if operation in {"merge", "distill"} and (created is None or archived):
                    raise ValueError("merge/distill history must create one card only")
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"memory history row {index} is invalid") from exc
            if event_id in seen_events:
                raise StateError(f"duplicate memory history event id: {event_id}")
            if proposal_id in seen_proposals:
                raise StateError(
                    f"duplicate applied maintenance proposal id: {proposal_id}"
                )
            seen_events.add(event_id)
            seen_proposals.add(proposal_id)
            result.append(row)
        return result

    def _card_rows(self) -> list[MemoryCard]:
        result = self._base_card_rows()
        seen = {card.card_id for card in result}
        for index, event in enumerate(self._history_rows(), start=1):
            value = event["created_card"]
            if value is None:
                continue
            try:
                card = MemoryCard.from_dict(value)
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(
                    f"memory history row {index} created_card is invalid"
                ) from exc
            if card.card_id in seen:
                raise StateError(f"duplicate memory card id: {card.card_id}")
            seen.add(card.card_id)
            result.append(card)
        return result

    def _card_views(self) -> list[dict[str, Any]]:
        cards = self._card_rows()
        views = {card.card_id: card.to_dict() for card in cards}
        for view in views.values():
            view["superseded_by"] = None
        for card in cards:
            derived = (
                "corrected" if card.supersession_kind == "correction" else "historical"
            )
            for superseded_id in card.supersedes:
                if superseded_id not in views:
                    raise StateError(
                        f"memory card {card.card_id} supersedes missing card: {superseded_id}"
                    )
                if views[superseded_id]["superseded_by"] is not None:
                    raise StateError(
                        f"memory card has multiple superseders: {superseded_id}"
                    )
                views[superseded_id]["history_status"] = derived
                views[superseded_id]["superseded_by"] = card.card_id
            for related_id in card.related_cards:
                if related_id not in views:
                    raise StateError(
                        f"memory card {card.card_id} relates to missing card: {related_id}"
                    )
        for event in self._history_rows():
            for archived_id in event["archived_card_ids"]:
                if archived_id not in views:
                    raise StateError(
                        f"memory history archives missing card: {archived_id}"
                    )
                views[archived_id]["lifecycle_status"] = "archived"
        return [views[card.card_id] for card in cards]

    def _open_raw(self, open_ref: str) -> dict[str, Any] | None:
        prefix, separator, identifier = open_ref.partition(":")
        if not separator or not identifier:
            return None
        if prefix == "card":
            self._card_rows()
            for row in self.cards.rows():
                if row.get("kind") == "card" and row.get("card_id") == identifier:
                    return dict(row)
            for event in self._history_rows():
                created = event["created_card"]
                if created is not None and created.get("card_id") == identifier:
                    return dict(created)
            return None
        if prefix == "diary":
            self._diary_rows()
            for row in self.diary.rows():
                if row.get("kind") == "diary" and row.get("entry_id") == identifier:
                    return dict(row)
            return None
        return None

    def _diary_rows(self) -> list[DiaryEntry]:
        result: list[DiaryEntry] = []
        seen: set[str] = set()
        for index, row in enumerate(self.diary.rows(), start=1):
            if row.get("schema_version") != MEMORY_SCHEMA or row.get("kind") != "diary":
                raise StateError(f"diary row {index} has an unsupported schema")
            try:
                entry = DiaryEntry(
                    entry_id=str(row["entry_id"]),
                    day=date.fromisoformat(str(row["day"])),
                    title=str(row["title"]),
                    body=str(row["body"]),
                    source_ref=str(row["source_ref"]),
                    created_at=parse_time(str(row["created_at"])),
                )
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"diary row {index} is invalid") from exc
            if entry.entry_id in seen:
                raise StateError(f"duplicate diary entry id: {entry.entry_id}")
            seen.add(entry.entry_id)
            result.append(entry)
        return result

    def add_card(
        self,
        summary: str,
        *,
        provenance: str,
        source_ref: str,
        tags: Iterable[str] = (),
        metadata: Mapping[str, Any] | None = None,
        card_id: str | None = None,
        event_time: str | None = None,
        entities: Iterable[str] = (),
        state_key: str | None = None,
        history_status: str = "current",
        lifecycle_status: str = "active",
        supersedes: Iterable[str] = (),
        supersession_kind: str | None = None,
        related_cards: Iterable[str] = (),
    ) -> MemoryCard:
        with file_lock(self.card_mutation_lock):
            card = self._build_card(
                summary,
                provenance=provenance,
                source_ref=source_ref,
                tags=tags,
                metadata=metadata,
                card_id=card_id,
                event_time=event_time,
                entities=entities,
                state_key=state_key,
                history_status=history_status,
                lifecycle_status=lifecycle_status,
                supersedes=supersedes,
                supersession_kind=supersession_kind,
                related_cards=related_cards,
            )
            self.cards.append(card.to_dict())
            return card

    def _build_card(
        self,
        summary: str,
        *,
        provenance: str,
        source_ref: str,
        tags: Iterable[str] = (),
        metadata: Mapping[str, Any] | None = None,
        card_id: str | None = None,
        event_time: str | None = None,
        entities: Iterable[str] = (),
        state_key: str | None = None,
        history_status: str = "current",
        lifecycle_status: str = "active",
        supersedes: Iterable[str] = (),
        supersession_kind: str | None = None,
        related_cards: Iterable[str] = (),
    ) -> MemoryCard:
        views = {card["card_id"]: card for card in self._card_views()}
        existing = set(views)
        for label, value in (
            ("memory summary", summary),
            ("memory provenance", provenance),
            ("memory source_ref", source_ref),
        ):
            if type(value) is not str:
                raise TypeError(f"{label} must be a string")
        if card_id is not None and (type(card_id) is not str or not card_id.strip()):
            raise ValueError("memory card_id must be null or a non-empty string")
        if state_key is not None and (
            type(state_key) is not str or not state_key.strip()
        ):
            raise ValueError("memory state_key must be null or a non-empty string")
        created_at = self.clock()
        resolved_card_id = card_id.strip() if card_id is not None else new_id("card")
        superseded_ids = _validated_card_ids(supersedes, "memory supersedes")
        related_ids = _validated_card_ids(related_cards, "memory related_cards")
        referenced = set((*superseded_ids, *related_ids))
        missing = sorted(referenced - existing)
        if missing:
            raise ValueError(f"memory relationship references missing cards: {missing}")
        if resolved_card_id in referenced:
            raise ValueError("memory card cannot relate to or supersede itself")
        if superseded_ids and history_status != "current":
            raise ValueError("a superseding memory card must be current")
        for superseded_id in superseded_ids:
            target = views[superseded_id]
            if (
                target["history_status"] != "current"
                or target["lifecycle_status"] != "active"
            ):
                raise ValueError(
                    f"memory card is not current and active: {superseded_id}"
                )
        card = MemoryCard(
            card_id=resolved_card_id,
            summary=summary,
            provenance=provenance,
            source_ref=source_ref,
            created_at=created_at,
            tags=tuple(
                sorted(
                    _validated_text_items(
                        tags,
                        "memory tags",
                        max_items=64,
                        max_bytes=128,
                        lowercase=True,
                    )
                )
            ),
            metadata={} if metadata is None else dict(metadata),
            event_time=_validated_event_time(event_time or isoformat(created_at)),
            entities=_validated_text_items(
                entities,
                "memory entities",
                max_items=64,
                max_bytes=256,
            ),
            state_key=None if state_key is None else state_key.strip(),
            history_status=history_status,
            lifecycle_status=lifecycle_status,
            supersedes=superseded_ids,
            supersession_kind=supersession_kind,
            related_cards=related_ids,
        )
        if card.card_id in existing:
            raise ValueError(f"memory card already exists: {card.card_id}")
        return card

    def append_diary(
        self,
        *,
        day: date,
        title: str,
        body: str,
        source_ref: str,
        entry_id: str | None = None,
    ) -> DiaryEntry:
        if not title.strip() or not body.strip() or not source_ref.strip():
            raise ValueError("diary title, body, and source_ref are required")
        entry = DiaryEntry(
            entry_id or new_id("diary"),
            day,
            title,
            body,
            source_ref,
            self.clock(),
        )
        self.diary.append(entry.to_dict())
        return entry

    def search(
        self,
        query: str,
        *,
        limit: int = 8,
        include_archived: bool = False,
        include_historical: bool = False,
    ) -> list[SearchHit]:
        terms = _tokens(query)
        literal_query = query.strip()
        cjk_fallback = _has_cjk_query(literal_query)
        if not 1 <= limit <= 100 or (not terms and not cjk_fallback):
            return []
        hits: list[SearchHit] = []
        for card in self._card_views():
            if not include_archived and card["lifecycle_status"] != "active":
                continue
            if not include_historical and card["history_status"] != "current":
                continue
            text = " ".join((card["summary"], *card["tags"]))
            haystack = _tokens(text)
            score = len(terms & haystack)
            if not score and cjk_fallback and literal_query in text:
                score = _CJK_FALLBACK_SCORE
            if score:
                hits.append(
                    SearchHit(
                        f"card:{card['card_id']}",
                        score,
                        card["summary"],
                        card["source_ref"],
                        card["provenance"],
                    )
                )
        for entry in self._diary_rows():
            text = f"{entry.title} {entry.body}"
            score = len(terms & _tokens(text))
            if not score and cjk_fallback and literal_query in text:
                score = _CJK_FALLBACK_SCORE
            if score:
                hits.append(
                    SearchHit(
                        entry.open_ref,
                        score,
                        entry.title,
                        entry.source_ref,
                        None,
                    )
                )
        return sorted(hits, key=lambda item: (-item.score, item.open_ref))[:limit]

    def _candidate_from_open(
        self,
        open_ref: str,
        *,
        score: float | None,
    ) -> RecallCandidate | None:
        """Rebuild a candidate from the local record, never from a hit excerpt."""

        _validated_open_ref(open_ref)
        record = self.open(open_ref)
        if record is None:
            return None
        kind = record.get("kind")
        if kind == "card":
            if (
                record.get("lifecycle_status") != "active"
                or record.get("history_status") != "current"
            ):
                return None
            excerpt = record.get("summary")
            provenance = record.get("provenance")
        elif kind == "diary":
            title = record.get("title")
            body = record.get("body")
            if type(title) is not str or type(body) is not str:
                raise StateError("memory record has an invalid excerpt")
            excerpt = f"{title}: {body}"
            provenance = None
        else:
            raise StateError("memory record has an unsupported kind")
        if type(excerpt) is not str:
            raise StateError("memory record has an invalid excerpt")
        if provenance is not None and type(provenance) is not str:
            raise StateError("memory record has an invalid provenance")
        normalized = excerpt.strip()
        encoded = normalized.encode("utf-8")
        if len(encoded) > MAX_RECALL_EXCERPT_BYTES:
            normalized = (
                encoded[:MAX_RECALL_EXCERPT_BYTES]
                .decode("utf-8", errors="ignore")
                .rstrip()
            )
        return RecallCandidate(open_ref, score, normalized, provenance)

    def lexical_recall(self, query: str, *, limit: int = 8) -> list[RecallCandidate]:
        """Return lexical candidates after exact local evidence reconstruction."""

        candidates: list[RecallCandidate] = []
        seen: set[str] = set()
        for hit in self.search(query, limit=limit):
            candidate = self._candidate_from_open(hit.open_ref, score=hit.score)
            if candidate is None or candidate.open_ref in seen:
                continue
            seen.add(candidate.open_ref)
            candidates.append(candidate)
        return candidates

    @staticmethod
    def _coerce_external_hit(value: Any) -> ExternalHit:
        if isinstance(value, ExternalHit):
            return value
        if isinstance(value, Mapping):
            return ExternalHit.from_dict(value)
        raise StateError("external retriever returned an invalid hit")

    def search_external(
        self,
        query: str,
        *,
        retriever: ExternalRetriever,
        limit: int = 8,
    ) -> list[ExternalHit]:
        """Call the external port only; local evidence validation is separate."""

        if type(query) is not str:
            raise ValueError("memory query must be a string")
        if not 1 <= limit <= 100 or not _tokens(query):
            return []
        # Do not catch exceptions from the provider: callers need to
        # distinguish provider failure from local StateError while opening a
        # returned reference.
        raw_hits = retriever.search(query, limit=limit)
        try:
            iterator = iter(raw_hits)
        except TypeError as exc:
            raise StateError("external retriever returned a non-iterable") from exc
        hits: list[ExternalHit] = []
        for index, value in enumerate(iterator, start=1):
            if index > limit:
                break
            hits.append(self._coerce_external_hit(value))
        return hits

    def candidates_from_external_hits(
        self,
        hits: Iterable[ExternalHit],
        *,
        limit: int = 8,
    ) -> list[RecallCandidate]:
        """Exact-open external hits; invalid refs are filtered, state errors rise."""

        if not 1 <= limit <= 100:
            return []
        if isinstance(hits, (str, bytes)):
            raise StateError("external hits must be an iterable of hits")
        candidates: list[RecallCandidate] = []
        seen: set[str] = set()
        for value in hits:
            hit = self._coerce_external_hit(value)
            if hit.open_ref in seen:
                continue
            candidate = self._candidate_from_open(hit.open_ref, score=hit.score)
            if candidate is None:
                # An external index may be stale.  It cannot become evidence
                # unless the current local store opens the exact reference.
                continue
            seen.add(candidate.open_ref)
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        return candidates

    def recall(
        self,
        query: str,
        *,
        limit: int = 8,
        retriever: ExternalRetriever | None = None,
    ) -> list[RecallCandidate]:
        """Prefer exact-open external hits, with lexical fallback when empty.

        Provider exceptions intentionally propagate so the host can audit them
        before choosing a lexical fallback. Local exact-open failures also
        propagate and must not be mistaken for provider failures.
        """

        if retriever is None:
            return self.lexical_recall(query, limit=limit)
        external_hits = self.search_external(
            query,
            retriever=retriever,
            limit=limit,
        )
        candidates = self.candidates_from_external_hits(external_hits, limit=limit)
        if candidates:
            return candidates
        return self.lexical_recall(query, limit=limit)

    def open(self, open_ref: str) -> dict[str, Any] | None:
        prefix, separator, identifier = open_ref.partition(":")
        if not separator or not identifier:
            return None
        if prefix == "card":
            return next(
                (item for item in self._card_views() if item["card_id"] == identifier),
                None,
            )
        if prefix == "diary":
            entry = next(
                (item for item in self._diary_rows() if item.entry_id == identifier),
                None,
            )
            return None if entry is None else entry.to_dict()
        return None

    def history_chain(self, open_ref: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return an exact, bounded card relation component around one card."""

        if not 1 <= limit <= 200:
            raise ValueError("memory history limit must be between 1 and 200")
        prefix, separator, identifier = open_ref.partition(":")
        if prefix != "card" or not separator or not identifier:
            raise ValueError("memory history requires a card open_ref")
        views = {card["card_id"]: card for card in self._card_views()}
        if identifier not in views:
            return []
        adjacent: dict[str, set[str]] = {card_id: set() for card_id in views}
        for card in views.values():
            card_id = card["card_id"]
            for related_id in (*card["supersedes"], *card["related_cards"]):
                adjacent[card_id].add(related_id)
                adjacent[related_id].add(card_id)
        queue = [identifier]
        seen: set[str] = set()
        while queue and len(seen) < limit:
            card_id = queue.pop(0)
            if card_id in seen:
                continue
            seen.add(card_id)
            queue.extend(sorted(adjacent[card_id] - seen))
        return sorted(
            (views[card_id] for card_id in seen),
            key=lambda card: (card["event_time"], card["created_at"], card["card_id"]),
        )

    def propose_hot_memory_change(
        self,
        *,
        current_text: str,
        operation: str,
        reason: str,
        target: str,
    ) -> dict[str, Any]:
        if operation not in {"add", "replace", "retire"}:
            raise ValueError("hot-memory operation must be add, replace, or retire")
        proposal = {
            "schema_version": MEMORY_SCHEMA,
            "kind": "hot_memory_proposal",
            "proposal_id": new_id("memory_proposal"),
            "created_at": isoformat(self.clock()),
            "expected_sha256": hashlib.sha256(current_text.encode("utf-8")).hexdigest(),
            "operation": operation,
            "target": target,
            "reason": reason,
            "applied": False,
        }
        self.proposals.append(proposal)
        return proposal

    @staticmethod
    def _normalize_evidence_refs(evidence_refs: Iterable[str]) -> list[str]:
        if isinstance(evidence_refs, (str, bytes)):
            raise TypeError("maintenance evidence_refs must be an iterable")
        try:
            refs = list(evidence_refs)
        except TypeError as exc:
            raise ValueError("maintenance evidence_refs must be an iterable") from exc
        if not 1 <= len(refs) <= 20:
            raise ValueError("maintenance evidence_refs must contain 1 to 20 refs")
        normalized: list[str] = []
        seen: set[str] = set()
        for ref in refs:
            _validated_open_ref(ref, "maintenance evidence ref")
            if ref in seen:
                raise ValueError("maintenance evidence_refs must be unique")
            seen.add(ref)
            normalized.append(ref)
        return normalized

    @staticmethod
    def _canonical_json_value(value: Any) -> Any:
        ensure_bounded_json(
            value,
            "maintenance proposed_value",
            max_bytes=MAX_MAINTENANCE_PROPOSED_VALUE_BYTES,
        )
        # Round-trip through canonical JSON so a caller cannot mutate the
        # object after proposal creation and so replay comparisons are stable.
        return json.loads(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )

    @staticmethod
    def _canonical_evidence_sha256(
        evidence_refs: list[str], evidence: list[Mapping[str, Any]]
    ) -> str:
        payload = [
            {"open_ref": open_ref, "record": record}
            for open_ref, record in zip(evidence_refs, evidence, strict=True)
        ]
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _maintenance_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            row["operation"],
            tuple(row["evidence_refs"]),
            row["reason"],
            row["proposed_value"],
        )

    def _maintenance_rows(self) -> list[dict[str, Any]]:
        expected_fields = {
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
        result: list[dict[str, Any]] = []
        seen_proposals: set[str] = set()
        seen_requests: set[str] = set()
        for index, row in enumerate(self.maintenance.rows(), start=1):
            if set(row) != expected_fields:
                raise StateError(f"maintenance row {index} has an unsupported schema")
            try:
                if (
                    row["schema_version"] != MAINTENANCE_SCHEMA
                    or row["kind"] != "maintenance_proposal"
                ):
                    raise StateError("unsupported maintenance schema")
                proposal_id = row["proposal_id"]
                request_id = row["request_id"]
                _validated_open_ref(proposal_id, "maintenance proposal_id")
                _validated_open_ref(request_id, "maintenance request_id")
                parse_time(row["created_at"])
                operation = row["operation"]
                if operation not in {"merge", "retire", "distill"}:
                    raise ValueError("unsupported maintenance operation")
                refs = row["evidence_refs"]
                if not isinstance(refs, list):
                    raise TypeError("maintenance evidence_refs must be a list")
                self._normalize_evidence_refs(refs)
                digest = row["evidence_sha256"]
                if (
                    type(digest) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                ):
                    raise ValueError("maintenance evidence_sha256 is invalid")
                reason = row["reason"]
                if type(reason) is not str or not reason.strip():
                    raise ValueError("maintenance reason is required")
                ensure_bounded_text(
                    reason,
                    "maintenance reason",
                    max_bytes=MAX_RECALL_REASON_BYTES,
                )
                proposed_value = self._canonical_json_value(row["proposed_value"])
                if operation in {"merge", "distill"} and proposed_value is None:
                    raise ValueError(
                        "merge and distill proposals require proposed_value"
                    )
                evidence: list[Mapping[str, Any]] = []
                for ref in refs:
                    record = self._open_raw(ref)
                    if record is None:
                        raise ValueError("maintenance evidence ref is not openable")
                    evidence.append(record)
                if self._canonical_evidence_sha256(refs, evidence) != digest:
                    raise ValueError("maintenance evidence hash does not match")
                if row["status"] != "proposed" or row["applied"] is not False:
                    raise ValueError("maintenance proposal must remain unapplied")
            except (KeyError, TypeError, ValueError, StateError) as exc:
                raise StateError(f"maintenance row {index} is invalid") from exc
            if proposal_id in seen_proposals:
                raise StateError(f"duplicate maintenance proposal id: {proposal_id}")
            if request_id in seen_requests:
                raise StateError(f"duplicate maintenance request id: {request_id}")
            seen_proposals.add(proposal_id)
            seen_requests.add(request_id)
            result.append(row)
        return result

    def propose_maintenance(
        self,
        *,
        request_id: str,
        operation: str,
        evidence_refs: Iterable[str],
        reason: str,
        proposed_value: Any = None,
    ) -> dict[str, Any]:
        """Append an auditable, proposal-only maintenance request."""

        if type(request_id) is not str or not request_id.strip():
            raise ValueError("maintenance request_id is required")
        _validated_open_ref(request_id, "maintenance request_id")
        if operation not in {"merge", "retire", "distill"}:
            raise ValueError("maintenance operation must be merge, retire, or distill")
        if type(reason) is not str or not reason.strip():
            raise ValueError("maintenance reason is required")
        ensure_bounded_text(
            reason,
            "maintenance reason",
            max_bytes=MAX_RECALL_REASON_BYTES,
        )
        refs = self._normalize_evidence_refs(evidence_refs)
        value = self._canonical_json_value(proposed_value)
        if operation in {"merge", "distill"} and value is None:
            raise ValueError("merge and distill proposals require proposed_value")

        with (
            file_lock(self.maintenance_request_lock),
            file_lock(self.card_mutation_lock),
        ):
            existing_rows = self._maintenance_rows()
            for existing in existing_rows:
                if existing["request_id"] != request_id:
                    continue
                requested_identity = (operation, tuple(refs), reason, value)
                if self._maintenance_identity(existing) != requested_identity:
                    raise StateError(f"maintenance request_id conflict: {request_id}")
                return existing

            evidence: list[Mapping[str, Any]] = []
            for ref in refs:
                record = self._open_raw(ref)
                if record is None:
                    raise ValueError(f"maintenance evidence ref is not openable: {ref}")
                evidence.append(record)
            proposal = {
                "schema_version": MAINTENANCE_SCHEMA,
                "kind": "maintenance_proposal",
                "proposal_id": new_id("maintenance_proposal"),
                "request_id": request_id,
                "created_at": isoformat(self.clock()),
                "operation": operation,
                "evidence_refs": refs,
                "evidence_sha256": self._canonical_evidence_sha256(refs, evidence),
                "reason": reason,
                "proposed_value": value,
                "status": "proposed",
                "applied": False,
            }
            self.maintenance.append(proposal)
            return proposal

    def _card_from_maintenance(self, proposal: Mapping[str, Any]) -> MemoryCard:
        operation = proposal["operation"]
        value = proposal["proposed_value"]
        if not isinstance(value, Mapping):
            raise ValueError("maintenance proposed_value must be an object")
        allowed = {
            "summary",
            "provenance",
            "source_ref",
            "tags",
            "metadata",
            "card_id",
            "event_time",
            "entities",
            "state_key",
            "history_status",
            "lifecycle_status",
            "related_cards",
            "supersession_kind",
        }
        extra = set(value) - allowed
        if extra:
            raise ValueError(
                f"maintenance proposed_value contains unsupported fields: {sorted(extra)}"
            )
        if type(value.get("summary")) is not str or not value["summary"].strip():
            raise ValueError("maintenance proposed_value summary is required")
        if type(value.get("provenance")) is not str or not value["provenance"].strip():
            raise ValueError("maintenance proposed_value provenance is required")
        if value.get("source_ref") is not None and (
            type(value["source_ref"]) is not str or not value["source_ref"].strip()
        ):
            raise ValueError("maintenance proposed_value source_ref must be a string")
        evidence_card_ids = [
            ref.removeprefix("card:")
            for ref in proposal["evidence_refs"]
            if ref.startswith("card:")
        ]
        metadata = value.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ValueError("maintenance proposed_value metadata must be an object")
        metadata = {
            **dict(metadata),
            "maintenance_proposal_id": proposal["proposal_id"],
            "evidence_refs": list(proposal["evidence_refs"]),
        }
        if operation == "merge":
            if len(evidence_card_ids) != len(proposal["evidence_refs"]):
                raise ValueError("merge maintenance evidence must contain cards only")
            supersedes = evidence_card_ids
            supersession_kind = str(value.get("supersession_kind") or "dedupe")
            related_cards = value.get("related_cards", [])
        else:
            if value.get("supersession_kind") is not None:
                raise ValueError("distill maintenance cannot set supersession_kind")
            supersedes = []
            supersession_kind = None
            related_cards = list(
                dict.fromkeys((*value.get("related_cards", []), *evidence_card_ids))
            )
        return self._build_card(
            value["summary"],
            provenance=value["provenance"],
            source_ref=str(value.get("source_ref") or proposal["evidence_refs"][0]),
            tags=value.get("tags", []),
            metadata=metadata,
            card_id=value.get("card_id"),
            event_time=value.get("event_time"),
            entities=value.get("entities", []),
            state_key=value.get("state_key"),
            history_status=str(value.get("history_status") or "current"),
            lifecycle_status=str(value.get("lifecycle_status") or "active"),
            supersedes=supersedes,
            supersession_kind=supersession_kind,
            related_cards=related_cards,
        )

    def apply_maintenance(
        self,
        proposal_id: str,
        *,
        activity: str,
        permission: str,
    ) -> dict[str, Any]:
        """Apply one proposal as a single append-only history receipt."""

        _validated_open_ref(proposal_id, "maintenance proposal_id")
        if type(activity) is not str or not activity.strip():
            raise ValueError("maintenance apply activity is required")
        ensure_bounded_text(activity, "maintenance apply activity", max_bytes=256)
        if permission not in APPLY_PERMISSION_LEVEL:
            raise ValueError("maintenance apply permission is invalid")
        with (
            file_lock(self.maintenance_request_lock),
            file_lock(self.card_mutation_lock),
        ):
            for event in self._history_rows():
                if event["proposal_id"] == proposal_id:
                    if (
                        event["activity"] != activity.strip()
                        or event["permission"] != permission
                    ):
                        raise StateError(
                            f"maintenance proposal replay conflict: {proposal_id}"
                        )
                    return event
            proposal = next(
                (
                    row
                    for row in self._maintenance_rows()
                    if row["proposal_id"] == proposal_id
                ),
                None,
            )
            if proposal is None:
                raise ValueError(f"maintenance proposal not found: {proposal_id}")
            evidence = []
            for ref in proposal["evidence_refs"]:
                record = self._open_raw(ref)
                if record is None:
                    raise ValueError(f"maintenance evidence ref is not openable: {ref}")
                evidence.append(record)
            if (
                self._canonical_evidence_sha256(proposal["evidence_refs"], evidence)
                != proposal["evidence_sha256"]
            ):
                raise StateError("maintenance evidence changed before apply")

            created_card: dict[str, Any] | None = None
            archived_card_ids: list[str] = []
            operation = proposal["operation"]
            required_permission = APPLY_REQUIRED_PERMISSION[operation]
            if (
                APPLY_PERMISSION_LEVEL[permission]
                < APPLY_PERMISSION_LEVEL[required_permission]
            ):
                raise PermissionError(
                    f"{operation} maintenance requires {required_permission} permission"
                )
            if operation == "retire":
                views = {card["card_id"]: card for card in self._card_views()}
                for ref in proposal["evidence_refs"]:
                    if not ref.startswith("card:"):
                        raise ValueError(
                            "retire maintenance evidence must contain cards only"
                        )
                    card_id = ref.removeprefix("card:")
                    if views[card_id]["lifecycle_status"] != "archived":
                        archived_card_ids.append(card_id)
                if not archived_card_ids:
                    raise ValueError("maintenance cards are already archived")
            else:
                card = self._card_from_maintenance(proposal)
                created_card = card.to_dict()

            event = {
                "schema_version": HISTORY_SCHEMA,
                "kind": "maintenance_applied",
                "event_id": new_id("history_event"),
                "proposal_id": proposal["proposal_id"],
                "request_id": proposal["request_id"],
                "created_at": isoformat(self.clock()),
                "operation": operation,
                "activity": activity.strip(),
                "permission": permission,
                "evidence_sha256": proposal["evidence_sha256"],
                "created_card": created_card,
                "archived_card_ids": archived_card_ids,
                "status": "applied",
            }
            self.history.append(event)
            return event
