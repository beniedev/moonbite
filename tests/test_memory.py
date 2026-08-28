from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta

import pytest

from moonbite_plugin.memory import (
    HISTORY_SCHEMA,
    MAINTENANCE_SCHEMA,
    MEMORY_CARD_SCHEMA,
    MAX_OPEN_REF_BYTES,
    MAX_RECALL_EXCERPT_BYTES,
    ExternalHit,
    MemoryStore,
    RecallCandidate,
    ResurfaceCandidate,
)
from moonbite_plugin.runtime_core import JsonlLedger, StateError

NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class FixtureRetriever:
    def __init__(self, hits):
        self.hits = hits
        self.calls = []

    def search(self, query, *, limit):
        self.calls.append((query, limit))
        return self.hits


def test_cards_and_diary_return_exact_open_references(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "User likes moonlit walks",
        provenance="user_explicit",
        source_ref="conversation:fixture",
        card_id="fixture-card",
    )
    diary = memory.append_diary(
        day=date(2026, 8, 22),
        title="Moonlit walk",
        body="A calm evening outside.",
        source_ref="event:fixture",
        entry_id="fixture-diary",
    )

    refs = {hit.open_ref for hit in memory.search("moonlit")}
    assert refs == {card.open_ref, diary.open_ref}
    assert memory.open(card.open_ref)["source_ref"] == "conversation:fixture"
    assert memory.open(diary.open_ref)["source_ref"] == "event:fixture"


def test_recall_rebuilds_exact_candidates_and_preserves_external_order(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Moonlit card evidence",
        provenance="user_explicit",
        source_ref="conversation:fixture",
        card_id="fixture-card",
    )
    diary = memory.append_diary(
        day=date(2026, 8, 22),
        title="Moonlit diary evidence",
        body="The exact diary body is local evidence.",
        source_ref="event:fixture",
        entry_id="fixture-diary",
    )

    hits = [
        ExternalHit(diary.open_ref, score=4),
        ExternalHit(card.open_ref, score=3),
        ExternalHit("card:stale-index-ref", score=99),
        ExternalHit(diary.open_ref, score=1),
    ]
    retriever = FixtureRetriever(hits)
    external_hits = memory.search_external("moonlit", retriever=retriever, limit=8)
    candidates = memory.candidates_from_external_hits(external_hits)

    assert retriever.calls == [("moonlit", 8)]
    assert [candidate.open_ref for candidate in candidates] == [
        diary.open_ref,
        card.open_ref,
    ]
    assert candidates[0].excerpt == (
        "Moonlit diary evidence: The exact diary body is local evidence."
    )
    assert candidates[0].provenance is None
    assert candidates[1].excerpt == "Moonlit card evidence"
    assert candidates[1].provenance == "user_explicit"

    lexical = memory.lexical_recall("moonlit")
    assert all(memory.open(candidate.open_ref) is not None for candidate in lexical)


def test_recall_candidate_and_external_ref_byte_boundaries(tmp_path):
    with pytest.raises(ValueError, match="recall excerpt exceeds"):
        RecallCandidate(
            "card:fixture",
            1,
            "é" * (MAX_RECALL_EXCERPT_BYTES // 2 + 1),
            "user_explicit",
        )

    with pytest.raises(ValueError, match="external open_ref exceeds"):
        ExternalHit("x" * (MAX_OPEN_REF_BYTES + 1))


def test_recall_truncates_exact_local_excerpt_at_utf8_boundary(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "é" * (MAX_RECALL_EXCERPT_BYTES // 2 + 100),
        provenance="user_explicit",
        source_ref="conversation:fixture",
        card_id="fixture-card",
    )

    candidate = memory._candidate_from_open(card.open_ref, score=1)

    assert candidate is not None
    assert len(candidate.excerpt.encode("utf-8")) <= MAX_RECALL_EXCERPT_BYTES
    assert candidate.excerpt


def test_resurface_candidate_requires_positive_ttl_and_expires(tmp_path):
    candidate = ResurfaceCandidate(
        candidate_id="resurface-fixture",
        open_ref="card:fixture",
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
        reason="active chat fixture",
        relevance=1,
    )

    assert not candidate.is_expired(NOW)
    assert candidate.is_expired(NOW + timedelta(minutes=30))
    with pytest.raises(ValueError, match="positive TTL"):
        ResurfaceCandidate(
            candidate_id="resurface-invalid",
            open_ref="card:fixture",
            created_at=NOW,
            expires_at=NOW,
            reason="invalid fixture",
            relevance=1,
        )


def test_maintenance_proposal_replay_conflict_and_source_immutability(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Stable fixture memory",
        provenance="agent_observation",
        source_ref="event:fixture",
        card_id="fixture-card",
    )
    diary = memory.append_diary(
        day=date(2026, 8, 22),
        title="Stable fixture diary",
        body="Evidence remains append-only.",
        source_ref="event:fixture",
        entry_id="fixture-diary",
    )
    cards_before = memory.cards.path.read_bytes()
    diary_before = memory.diary.path.read_bytes()
    card_rows_before = memory.cards.rows()
    diary_rows_before = memory.diary.rows()

    proposal = memory.propose_maintenance(
        request_id="request-fixture-1",
        operation="distill",
        evidence_refs=[card.open_ref, diary.open_ref],
        reason="bounded fixture proposal",
        proposed_value={"summary": "stable fixture"},
    )
    replay = memory.propose_maintenance(
        request_id="request-fixture-1",
        operation="distill",
        evidence_refs=[card.open_ref, diary.open_ref],
        reason="bounded fixture proposal",
        proposed_value={"summary": "stable fixture"},
    )

    assert replay == proposal
    assert proposal["schema_version"] == MAINTENANCE_SCHEMA
    assert proposal["status"] == "proposed"
    assert proposal["applied"] is False
    assert len(proposal["evidence_sha256"]) == 64
    assert memory.maintenance.rows() == [proposal]
    assert memory.cards.path.read_bytes() == cards_before
    assert memory.diary.path.read_bytes() == diary_before
    assert memory.cards.rows() == card_rows_before
    assert memory.diary.rows() == diary_rows_before

    with pytest.raises(StateError, match="maintenance request_id conflict"):
        memory.propose_maintenance(
            request_id="request-fixture-1",
            operation="retire",
            evidence_refs=[card.open_ref],
            reason="conflicting fixture proposal",
        )


def test_concurrent_maintenance_request_replays_one_proposal(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Concurrent fixture memory",
        provenance="agent_observation",
        source_ref="event:fixture",
        card_id="fixture-card",
    )

    def propose(_index):
        return memory.propose_maintenance(
            request_id="request-concurrent-fixture",
            operation="distill",
            evidence_refs=[card.open_ref],
            reason="concurrent fixture proposal",
            proposed_value={"summary": "bounded"},
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        proposals = list(pool.map(propose, range(8)))

    assert len({proposal["proposal_id"] for proposal in proposals}) == 1
    assert len(memory.maintenance.rows()) == 1

    with pytest.raises(ValueError, match="require proposed_value"):
        memory.propose_maintenance(
            request_id="request-fixture-2",
            operation="merge",
            evidence_refs=[card.open_ref],
            reason="missing merged value",
        )


def test_corrupt_maintenance_row_fails_closed(tmp_path):
    memory = MemoryStore(tmp_path)
    JsonlLedger(tmp_path / "memory_maintenance.jsonl").append(
        {
            "schema_version": MAINTENANCE_SCHEMA,
            "kind": "unknown-maintenance-kind",
        }
    )

    with pytest.raises(StateError, match="maintenance row 1"):
        memory.propose_maintenance(
            request_id="request-fixture-1",
            operation="merge",
            evidence_refs=["card:missing"],
            reason="fixture",
            proposed_value={"summary": "fixture"},
        )


def test_invalid_provenance_is_rejected(tmp_path):
    memory = MemoryStore(tmp_path)
    with pytest.raises(ValueError, match="invalid memory provenance"):
        memory.add_card("fixture", provenance="guess", source_ref="event:fixture")


def test_hot_memory_change_is_only_a_sha_bound_proposal(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    proposal = memory.propose_hot_memory_change(
        current_text="stable memory",
        operation="replace",
        reason="fixture",
        target="rule:fixture",
    )
    assert proposal["expected_sha256"] == hashlib.sha256(b"stable memory").hexdigest()
    assert proposal["applied"] is False


def test_corrupt_memory_row_is_normalized_to_state_error(tmp_path):
    JsonlLedger(tmp_path / "memory_cards.jsonl").append(
        {
            "schema_version": "moon.memory.v1",
            "kind": "card",
            "card_id": "missing-required-fields",
        }
    )

    with pytest.raises(StateError, match="memory card row 1 is invalid"):
        MemoryStore(tmp_path).search("fixture")


def test_memory_summary_size_is_bounded_before_persistence(tmp_path):
    memory = MemoryStore(tmp_path)

    with pytest.raises(ValueError, match="memory summary exceeds"):
        memory.add_card(
            "x" * (16 * 1024 + 1),
            provenance="agent_observation",
            source_ref="event:fixture",
        )

    assert memory.cards.rows() == []


@pytest.mark.parametrize(
    ("field", "value"),
    [("tags", "not-an-array"), ("tags", ["ok", 1]), ("metadata", [])],
)
def test_corrupt_card_collection_types_are_rejected(field, value, tmp_path):
    row = {
        "schema_version": "moon.memory.v1",
        "kind": "card",
        "card_id": "fixture-card",
        "summary": "fixture",
        "provenance": "agent_observation",
        "source_ref": "event:fixture",
        "created_at": NOW.isoformat(),
        "tags": [],
        "metadata": {},
    }
    row[field] = value
    JsonlLedger(tmp_path / "memory_cards.jsonl").append(row)

    with pytest.raises(StateError, match="memory card row 1 is invalid"):
        MemoryStore(tmp_path).search("fixture")


def test_duplicate_diary_entry_ids_are_rejected(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    for title in ("first", "second"):
        memory.append_diary(
            day=date(2026, 8, 22),
            title=title,
            body="fixture",
            source_ref="event:fixture",
            entry_id="duplicate-entry",
        )

    with pytest.raises(StateError, match="duplicate diary entry id"):
        memory.search("fixture")


def test_legacy_cards_load_with_history_defaults(tmp_path):
    JsonlLedger(tmp_path / "memory_cards.jsonl").append(
        {
            "schema_version": "moon.memory.v1",
            "kind": "card",
            "card_id": "legacy-card",
            "summary": "Legacy compatible fixture",
            "provenance": "agent_observation",
            "source_ref": "event:legacy",
            "created_at": NOW.isoformat(),
            "tags": [],
            "metadata": {},
        }
    )

    opened = MemoryStore(tmp_path).open("card:legacy-card")

    assert opened["schema_version"] == MEMORY_CARD_SCHEMA
    assert opened["event_time"] == NOW.isoformat().replace("+00:00", "Z")
    assert opened["history_status"] == "current"
    assert opened["lifecycle_status"] == "active"


def test_history_relations_project_current_corrected_and_medium_context(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    original = memory.add_card(
        "Fixture role was alpha",
        provenance="user_explicit",
        source_ref="conversation:alpha",
        card_id="role-alpha",
        event_time="2026-01-01",
        entities=["fixture-user"],
        state_key="fixture.role",
    )
    evolved = memory.add_card(
        "Fixture role became beta",
        provenance="user_explicit",
        source_ref="conversation:beta",
        card_id="role-beta",
        event_time="2026-04-01",
        entities=["fixture-user"],
        state_key="fixture.role",
        supersedes=[original.card_id],
        supersession_kind="evolution",
    )
    corrected = memory.add_card(
        "Fixture role is gamma",
        provenance="user_explicit",
        source_ref="conversation:correction",
        card_id="role-gamma",
        event_time="2026-04-01",
        entities=["fixture-user"],
        state_key="fixture.role",
        supersedes=[evolved.card_id],
        supersession_kind="correction",
    )

    assert memory.open(original.open_ref)["history_status"] == "historical"
    assert memory.open(original.open_ref)["superseded_by"] == evolved.card_id
    assert memory.open(evolved.open_ref)["history_status"] == "corrected"
    assert memory.open(evolved.open_ref)["superseded_by"] == corrected.card_id
    assert memory.open(corrected.open_ref)["history_status"] == "current"
    assert [hit.open_ref for hit in memory.search("fixture role")] == [
        corrected.open_ref
    ]
    assert {
        hit.open_ref for hit in memory.search("fixture role", include_historical=True)
    } == {original.open_ref, evolved.open_ref, corrected.open_ref}
    assert [card["card_id"] for card in memory.history_chain(corrected.open_ref)] == [
        "role-alpha",
        "role-beta",
        "role-gamma",
    ]


def test_retire_apply_archives_without_deleting_and_replays(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Archive boundary fixture",
        provenance="agent_observation",
        source_ref="event:fixture",
        card_id="archive-fixture",
    )
    proposal = memory.propose_maintenance(
        request_id="request-archive-fixture",
        operation="retire",
        evidence_refs=[card.open_ref],
        reason="fixture archive",
    )

    with pytest.raises(PermissionError, match="requires reporting"):
        memory.apply_maintenance(
            proposal["proposal_id"],
            activity="fixture_archive",
            permission="safe",
        )
    assert memory.history.rows() == []
    receipt = memory.apply_maintenance(
        proposal["proposal_id"],
        activity="fixture_archive",
        permission="reporting",
    )
    replay = memory.apply_maintenance(
        proposal["proposal_id"],
        activity="fixture_archive",
        permission="reporting",
    )

    assert receipt == replay
    assert receipt["schema_version"] == HISTORY_SCHEMA
    assert receipt["operation"] == "retire"
    assert receipt["activity"] == "fixture_archive"
    assert receipt["permission"] == "reporting"
    assert receipt["archived_card_ids"] == [card.card_id]
    assert "delete" not in str(receipt).casefold()
    assert memory.search("archive boundary") == []
    assert memory.open(card.open_ref)["lifecycle_status"] == "archived"

    with pytest.raises(StateError, match="replay conflict"):
        memory.apply_maintenance(
            proposal["proposal_id"],
            activity="different_activity",
            permission="manual",
        )
    assert memory._open_raw(card.open_ref) is not None
    assert len(memory.history.rows()) == 1


def test_merge_apply_creates_one_card_and_keeps_superseded_evidence(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    cards = [
        memory.add_card(
            f"Duplicate fixture {suffix}",
            provenance="agent_observation",
            source_ref=f"event:{suffix}",
            card_id=f"duplicate-{suffix}",
        )
        for suffix in ("a", "b")
    ]
    proposal = memory.propose_maintenance(
        request_id="request-merge-fixture",
        operation="merge",
        evidence_refs=[card.open_ref for card in cards],
        reason="fixture merge",
        proposed_value={
            "summary": "Merged duplicate fixture",
            "provenance": "agent_observation",
            "card_id": "duplicate-merged",
        },
    )

    receipt = memory.apply_maintenance(
        proposal["proposal_id"],
        activity="fixture_merge",
        permission="manual",
    )

    assert receipt["created_card"]["card_id"] == "duplicate-merged"
    assert receipt["created_card"]["supersession_kind"] == "dedupe"
    assert set(receipt["created_card"]["supersedes"]) == {
        "duplicate-a",
        "duplicate-b",
    }
    assert all(
        memory.open(card.open_ref)["history_status"] == "historical" for card in cards
    )
    assert memory.open("card:duplicate-merged")["history_status"] == "current"
    assert len(memory.cards.rows()) == 2
    assert len(memory.history.rows()) == 1


def test_concurrent_apply_appends_one_history_receipt(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card = memory.add_card(
        "Concurrent archive fixture",
        provenance="agent_observation",
        source_ref="event:fixture",
        card_id="concurrent-archive",
    )
    proposal = memory.propose_maintenance(
        request_id="request-concurrent-archive",
        operation="retire",
        evidence_refs=[card.open_ref],
        reason="fixture archive",
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(
            pool.map(
                lambda _index: memory.apply_maintenance(
                    proposal["proposal_id"],
                    activity="fixture_concurrent",
                    permission="manual",
                ),
                range(8),
            )
        )

    assert len({receipt["event_id"] for receipt in receipts}) == 1
    assert len(memory.history.rows()) == 1


def test_relationships_reject_missing_or_already_superseded_cards(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    with pytest.raises(ValueError, match="missing cards"):
        memory.add_card(
            "Missing relation fixture",
            provenance="agent_observation",
            source_ref="event:fixture",
            supersedes=["missing-card"],
            supersession_kind="evolution",
        )
    original = memory.add_card(
        "Original relation fixture",
        provenance="agent_observation",
        source_ref="event:original",
        card_id="relation-original",
    )
    memory.add_card(
        "Current relation fixture",
        provenance="agent_observation",
        source_ref="event:current",
        card_id="relation-current",
        supersedes=[original.card_id],
        supersession_kind="evolution",
    )
    with pytest.raises(ValueError, match="not current and active"):
        memory.add_card(
            "Conflicting relation fixture",
            provenance="agent_observation",
            source_ref="event:conflict",
            supersedes=[original.card_id],
            supersession_kind="correction",
        )


def test_concurrent_superseders_cannot_fork_one_current_state(tmp_path):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    original = memory.add_card(
        "Concurrent state original",
        provenance="agent_observation",
        source_ref="event:original",
        card_id="concurrent-state-original",
    )

    def supersede(index):
        try:
            return memory.add_card(
                f"Concurrent state replacement {index}",
                provenance="agent_observation",
                source_ref=f"event:replacement-{index}",
                card_id=f"concurrent-state-{index}",
                supersedes=[original.card_id],
                supersession_kind="evolution",
            ).card_id
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(supersede, range(2)))

    assert sum(result is not None for result in results) == 1
    assert memory.open(original.open_ref)["history_status"] == "historical"


def test_history_ledger_rejects_delete_operations(tmp_path):
    JsonlLedger(tmp_path / "memory_history.jsonl").append(
        {
            "schema_version": HISTORY_SCHEMA,
            "kind": "maintenance_applied",
            "event_id": "history-delete-fixture",
            "proposal_id": "proposal-delete-fixture",
            "request_id": "request-delete-fixture",
            "created_at": NOW.isoformat(),
            "operation": "delete",
            "activity": "fixture",
            "permission": "manual",
            "evidence_sha256": "0" * 64,
            "created_card": None,
            "archived_card_ids": [],
            "status": "applied",
        }
    )

    with pytest.raises(StateError, match="memory history row 1"):
        MemoryStore(tmp_path).search("fixture")


def test_memory_observer_status_is_neutral_without_opening_card_or_diary_body(
    tmp_path,
):
    memory = MemoryStore(tmp_path, clock=lambda: NOW)
    card_path = tmp_path / "memory_cards.jsonl"
    diary_path = tmp_path / "diary.jsonl"
    card_path.write_text(
        json.dumps(
            {
                "schema_version": MEMORY_CARD_SCHEMA,
                "kind": "card",
                "card_id": "fixture-card",
                "summary": "synthetic private summary",
                "provenance": "user_explicit",
                "source_ref": "fixture-source",
                "created_at": NOW.isoformat(),
                "body": "synthetic private body",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    diary_path.write_text(
        json.dumps(
            {
                "schema_version": "moon.memory.v1",
                "kind": "diary",
                "entry_id": "fixture-diary",
                "day": NOW.date().isoformat(),
                "title": "synthetic title",
                "body": "synthetic diary body",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in (card_path, diary_path)
    }
    facts = memory.observer_status(target_date=NOW.date(), now=NOW)
    after = {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in (card_path, diary_path)
    }
    assert len(facts) == 1
    assert facts[0].state == "neutral"
    assert facts[0].code == "memory_adapter_unavailable"
    assert "synthetic private" not in repr(facts)
    assert before == after
    assert not (tmp_path / "memory_cards.jsonl.lock").exists()
    assert not (tmp_path / "diary.jsonl.lock").exists()
