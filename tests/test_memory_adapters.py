from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from moonbite_plugin.memory import ExternalHit, MemoryStore
from moonbite_plugin.memory_adapters import (
    PRIVATE_CONTINUITY_SOURCE_CLASS,
    MemoryStoreSourceAdapter,
)
from moonbite_plugin.memory_orchestration import (
    MissingEvidenceError,
    SourceRegistry,
    content_descriptor,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class FixtureRetriever:
    def __init__(self, hits, error: Exception | None = None):
        self.hits = hits
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, *, limit: int):
        self.calls.append((query, limit))
        if self.error is not None:
            raise self.error
        return self.hits


class MappingStore:
    def __init__(self, records, lexical_hits=()):
        self.records = records
        self.lexical_hits = list(lexical_hits)
        self.open_calls: list[str] = []
        self.lexical_calls: list[tuple[str, int]] = []

    def open(self, open_ref: str):
        self.open_calls.append(open_ref)
        return self.records.get(open_ref)

    def lexical_recall(self, query: str, *, limit: int):
        self.lexical_calls.append((query, limit))
        return self.lexical_hits


def test_retrieve_is_opaque_and_source_registry_can_exact_open(tmp_path: Path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "A card summary",
        provenance="user_explicit",
        source_ref="event:card",
        card_id="fixture-card",
        event_time="2026-08-20",
    )
    diary = memory.append_diary(
        day=date(2026, 8, 21),
        title="A diary title",
        body="A diary body",
        source_ref="event:diary",
        entry_id="fixture-diary",
    )
    retriever = FixtureRetriever(
        [ExternalHit(diary.open_ref, score=4), ExternalHit(card.open_ref, score=2)]
    )
    adapter = MemoryStoreSourceAdapter(memory, retriever)
    registry = SourceRegistry(retriever=adapter, opener=adapter)

    candidates = registry.retrieve("fixture", limit=4)

    assert [item.source_ref for item in candidates] == [diary.open_ref, card.open_ref]
    assert all(
        not hasattr(item, field)
        for item in candidates
        for field in ("body", "text", "raw", "content")
    )
    assert all(
        item.source_class == PRIVATE_CONTINUITY_SOURCE_CLASS for item in candidates
    )
    assert candidates[0].source_event_time == datetime(2026, 8, 21, tzinfo=UTC)
    assert candidates[1].source_event_time == datetime(2026, 8, 20, tzinfo=UTC)
    assert candidates[0].created_at == NOW
    assert candidates[1].created_at == NOW
    assert (
        candidates[0].content_sha256
        == content_descriptor("A diary title: A diary body")[0]
    )

    diary_material = registry.exact_open(candidates[0], max_bytes=128)
    card_material = registry.exact_open(candidates[1], max_bytes=128)
    assert diary_material is not None
    assert card_material is not None
    assert diary_material.body == "A diary title: A diary body"
    assert card_material.body == "A card summary"


def test_external_stale_refs_fall_back_to_lexical_and_provider_errors_propagate(
    tmp_path: Path,
):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Fallback fixture card",
        provenance="agent_observation",
        source_ref="event:fallback",
        card_id="fallback-card",
    )
    retriever = FixtureRetriever([ExternalHit("card:missing", score=99)])
    adapter = MemoryStoreSourceAdapter(memory, retriever)

    candidates = adapter.retrieve("fallback", limit=2)

    assert [item.source_ref for item in candidates] == [card.open_ref]
    assert retriever.calls == [("fallback", 2)]

    failing = FixtureRetriever([], error=RuntimeError("provider unavailable"))
    with pytest.raises(RuntimeError, match="provider unavailable"):
        MemoryStoreSourceAdapter(memory, failing).retrieve("fallback", limit=2)


def test_archived_and_historical_cards_are_not_exposed():
    records = {
        "card:archived": {
            "kind": "card",
            "card_id": "archived",
            "summary": "archived card",
            "event_time": "2026-08-20",
            "created_at": NOW.isoformat(),
            "history_status": "current",
            "lifecycle_status": "archived",
        },
        "card:historical": {
            "kind": "card",
            "card_id": "historical",
            "summary": "historical card",
            "event_time": "2026-08-20",
            "created_at": NOW.isoformat(),
            "history_status": "historical",
            "lifecycle_status": "active",
        },
        "card:current": {
            "kind": "card",
            "card_id": "current",
            "summary": "current card",
            "event_time": "2026-08-20",
            "created_at": NOW.isoformat(),
            "history_status": "current",
            "lifecycle_status": "active",
        },
    }
    hits = [
        ExternalHit("card:archived", score=3),
        ExternalHit("card:historical", score=2),
        ExternalHit("card:current", score=1),
    ]

    candidates = MemoryStoreSourceAdapter(
        MappingStore(records), FixtureRetriever(hits)
    ).retrieve("fixture", limit=3)

    assert [item.source_ref for item in candidates] == ["card:current"]


def test_historical_framing_is_left_to_orchestrator():
    store = MappingStore(
        {
            "diary:old": {
                "kind": "diary",
                "entry_id": "old",
                "day": "2026-01-01",
                "title": "Old title",
                "body": "Old body",
                "created_at": NOW.isoformat(),
            }
        },
        lexical_hits=[ExternalHit("diary:old", score=1)],
    )
    material = MemoryStoreSourceAdapter(store).open("diary:old", max_bytes=128)

    assert material is not None
    assert material.framing == "current"
    assert material.framing_date == date(2026, 1, 1)


def test_max_bytes_is_strict_on_actual_utf8_value():
    body = "éé"
    store = MappingStore(
        {
            "card:utf8": {
                "kind": "card",
                "card_id": "utf8",
                "summary": body,
                "event_time": "2026-08-20",
                "created_at": NOW.isoformat(),
                "history_status": "current",
                "lifecycle_status": "active",
            }
        }
    )
    adapter = MemoryStoreSourceAdapter(store)

    with pytest.raises(MissingEvidenceError, match="byte limit"):
        adapter.open("card:utf8", max_bytes=3)
    material = adapter.open("card:utf8", max_bytes=4)
    assert material is not None
    assert len(material.body.encode("utf-8")) == 4


def test_bad_timestamps_fail_closed_without_forging_candidates():
    store = MappingStore(
        {
            "card:bad": {
                "kind": "card",
                "card_id": "bad",
                "summary": "bad timestamp",
                "event_time": "2026-08-20T12:00:00",
                "created_at": NOW.isoformat(),
                "history_status": "current",
                "lifecycle_status": "active",
            }
        },
    )
    adapter = MemoryStoreSourceAdapter(
        store,
        FixtureRetriever([ExternalHit("card:bad", score=1)]),
    )

    with pytest.raises(ValueError, match="event_time"):
        adapter.retrieve("bad", limit=1)
    assert adapter.open("card:missing", max_bytes=64) is None


def test_retrieve_and_open_do_not_write_pristine_store(tmp_path: Path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Read-only fixture",
        provenance="user_explicit",
        source_ref="event:readonly",
        card_id="readonly-card",
    )
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.jsonl")}
    adapter = MemoryStoreSourceAdapter(
        memory,
        FixtureRetriever([ExternalHit(card.open_ref, score=1)]),
    )

    adapter.retrieve("readonly", limit=1)
    adapter.open(card.open_ref, max_bytes=64)
    adapter.open("card:missing", max_bytes=64)
    adapter.open("not-a-memory-ref", max_bytes=64)

    after = {path.name: path.read_bytes() for path in tmp_path.glob("*.jsonl")}
    assert after == before
