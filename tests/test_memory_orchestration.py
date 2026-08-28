from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.memory_orchestration import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureLedger,
    ExposurePolicy,
    MemoryMaintenanceFacade,
    MemoryOrchestrator,
    MissingEvidenceError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceRegistry,
    WriterCoordinator,
    content_descriptor,
)
from moonbite_plugin.runtime_core import JsonlLedger, StateError


NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def make_candidate(
    ref: str = "card:one",
    *,
    source_class: str = "private_continuity",
    body: str = "opaque source body",
    event_time: datetime = NOW - timedelta(days=1),
    created_at: datetime = NOW - timedelta(hours=1),
    expires_at: datetime | None = NOW + timedelta(hours=1),
) -> SourceCandidate:
    digest, length = content_descriptor(body)
    return SourceCandidate(
        source_ref=ref,
        source_class=source_class,
        source_event_time=event_time,
        created_at=created_at,
        expires_at=expires_at,
        content_sha256=digest,
        content_length=length,
        relevance=1,
    )


def context(
    *, source_kind: str = "private_inbound", turn_index: int = 0
) -> ExposureContext:
    return ExposureContext(
        "session-1",
        "lifecycle-1",
        f"turn-{turn_index + 1}",
        source_kind,
        NOW,
        turn_index,
    )


def _maintenance_observer_rows(
    tmp_path: Path,
    *,
    proposal_id: str = "proposal:observer",
    request_id: str = "request:observer",
    operation: str = "merge",
    evidence_sha256: str = "a" * 64,
    include_history: bool = True,
    history_overrides: dict[str, object] | None = None,
) -> tuple[Path, Path]:
    maintenance_path = tmp_path / "memory_maintenance.jsonl"
    history_path = tmp_path / "memory_history.jsonl"
    JsonlLedger(maintenance_path).append(
        {
            "schema_version": "moon.memory.maintenance.v1",
            "kind": "maintenance_proposal",
            "proposal_id": proposal_id,
            "request_id": request_id,
            "created_at": NOW.isoformat(),
            "operation": operation,
            "evidence_refs": [],
            "evidence_sha256": evidence_sha256,
            "reason": "private observer body must stay unreachable",
            "proposed_value": {"summary": "private observer body"},
            "status": "proposed",
            "applied": False,
        }
    )
    if include_history:
        history = {
            "schema_version": "moon.memory.history.v1",
            "kind": "maintenance_applied",
            "event_id": "history:observer",
            "proposal_id": proposal_id,
            "request_id": request_id,
            "created_at": NOW.isoformat(),
            "operation": operation,
            "activity": "observer fixture",
            "permission": "manual",
            "evidence_sha256": evidence_sha256,
            "created_card": None,
            "archived_card_ids": [],
            "status": "applied",
        }
        history.update(history_overrides or {})
        JsonlLedger(history_path).append(history)
    for lock_path in tmp_path.glob("*.lock"):
        lock_path.unlink()
    return maintenance_path, history_path


class Opener:
    def __init__(self, body: str | None):
        self.body = body
        self.calls: list[str] = []

    def open(self, source_ref: str, *, max_bytes: int):
        self.calls.append(source_ref)
        if self.body is None:
            return None
        candidate = make_candidate(source_ref, body=self.body)
        return SourceMaterial(
            source_ref=source_ref,
            source_class=candidate.source_class,
            source_event_time=candidate.source_event_time,
            created_at=candidate.created_at,
            expires_at=candidate.expires_at,
            body=self.body,
        )


def test_exact_open_is_after_selection_and_has_bounded_transient_body(tmp_path: Path):
    candidate = make_candidate()
    opener = Opener("opaque source body")
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=opener),
        clock=lambda: NOW,
    )

    exposed = orchestrator.expose_candidates([candidate], context=context(), now=NOW)[0]
    assert opener.calls == [candidate.source_ref]
    assert exposed.state == "exposed"
    material = exposed.material
    assert material.body == "opaque source body"
    assert opener.calls == [candidate.source_ref]
    rows = (tmp_path / "memory_orchestration.jsonl").read_text(encoding="utf-8")
    assert "opaque source body" not in rows
    assert "source_event_time" in rows and "source_created_at" in rows


def test_index_without_descriptor_is_exact_opened_only_after_policy_selection(
    tmp_path: Path,
):
    full = make_candidate()
    candidate = SourceCandidate(
        source_ref=full.source_ref,
        source_class=full.source_class,
        source_event_time=full.source_event_time,
        created_at=full.created_at,
        expires_at=full.expires_at,
    )
    opener = Opener("opaque source body")
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=opener),
        clock=lambda: NOW,
    )
    records = orchestrator.expose_candidates([candidate], context=context(), now=NOW)
    assert len(records) == 1 and opener.calls == [candidate.source_ref]
    assert records[0].material.content_sha256 == full.content_sha256


def test_exact_open_rejects_missing_or_expired_evidence(tmp_path: Path):
    missing = make_candidate()
    missing_orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path / "missing", clock=lambda: NOW),
        source_registry=SourceRegistry(opener=Opener(None)),
        clock=lambda: NOW,
    )
    with pytest.raises(MissingEvidenceError):
        missing_orchestrator.expose_candidates([missing], context=context(), now=NOW)
    failed = missing_orchestrator.exposures.replay()[0]
    assert failed.state == "failed_to_open"
    assert failed.failed_to_open
    assert not failed.used and not failed.consumed

    expired_dir = tmp_path / "expired"
    expired = make_candidate(expires_at=NOW - timedelta(seconds=1))
    expired_orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(expired_dir, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=Opener("opaque source body")),
        clock=lambda: NOW,
    )
    with pytest.raises(ExpiredEvidenceError):
        expired_orchestrator.expose_candidates([expired], context=context(), now=NOW)
    assert expired_orchestrator.exposures.replay()[0].state == "failed_to_open"

    mismatch_dir = tmp_path / "mismatch"
    mismatch = make_candidate()
    mismatch_orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(mismatch_dir, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=Opener("different source body")),
        clock=lambda: NOW,
    )
    with pytest.raises(MissingEvidenceError):
        mismatch_orchestrator.expose_candidates([mismatch], context=context(), now=NOW)
    mismatch_record = mismatch_orchestrator.exposures.replay()[0]
    assert mismatch_record.state == "failed_to_open"
    assert not mismatch_record.consumed


def test_replayed_exposure_reopens_transient_body_without_new_receipt(tmp_path: Path):
    candidate = make_candidate()
    opener = Opener("opaque source body")
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=opener),
        policy=ExposurePolicy(source_cooldown=timedelta(0)),
        clock=lambda: NOW,
    )
    first = orchestrator.expose_candidates([candidate], context=context(), now=NOW)
    second = orchestrator.expose_candidates([candidate], context=context(), now=NOW)
    assert first[0].state == second[0].state == "exposed"
    assert second[0].body == first[0].body
    assert len(orchestrator.exposures.events()) == 3


def test_policy_modes_cap_and_cooldown_are_deterministic(tmp_path: Path):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    policy = ExposurePolicy(
        max_per_session=2, source_cooldown=timedelta(minutes=5), result_budget=3
    )
    first = context(turn_index=0)
    candidates = [
        make_candidate("card:delta", source_class="wider_reservoir", event_time=NOW),
        make_candidate(
            "card:brief", source_class="self_brief", event_time=NOW - timedelta(days=2)
        ),
        make_candidate(
            "card:other", source_class="public", event_time=NOW - timedelta(days=3)
        ),
    ]
    plan = policy.choose(candidates, context=first, ledger=ledger, now=NOW)
    assert plan.mode == "self_brief"
    assert [item.source_ref for item in plan.candidates] == ["card:brief"]
    selected = ledger.record_selected(plan.candidates[0], context=first, now=NOW)
    body = "opaque source body"
    opened = SourceMaterial(
        source_ref=plan.candidates[0].source_ref,
        source_class=plan.candidates[0].source_class,
        source_event_time=plan.candidates[0].source_event_time,
        created_at=plan.candidates[0].created_at,
        expires_at=plan.candidates[0].expires_at,
        body=body,
    )
    ledger.record_opened(selected.exposure_id, opened, now=NOW)
    ledger.record_exposed(selected.exposure_id, now=NOW)
    continuation = ExposureContext(
        "session-1", "lifecycle-1", "turn-2", "private_inbound", NOW, 1
    )
    delta_plan = policy.choose(candidates, context=continuation, ledger=ledger, now=NOW)
    assert delta_plan.mode == "continuation_delta"
    assert [item.source_ref for item in delta_plan.candidates] == ["card:delta"]
    selected_delta = ledger.record_selected(
        delta_plan.candidates[0], context=continuation, now=NOW
    )
    opened_delta = SourceMaterial(
        source_ref=delta_plan.candidates[0].source_ref,
        source_class=delta_plan.candidates[0].source_class,
        source_event_time=delta_plan.candidates[0].source_event_time,
        created_at=delta_plan.candidates[0].created_at,
        expires_at=delta_plan.candidates[0].expires_at,
        body=body,
    )
    ledger.record_opened(selected_delta.exposure_id, opened_delta, now=NOW)
    ledger.record_exposed(selected_delta.exposure_id, now=NOW)
    assert (
        policy.choose(
            candidates, context=continuation, ledger=ledger, now=NOW
        ).candidates
        == ()
    )


def test_exposure_states_are_distinct_and_consumed_requires_matching_reply(
    tmp_path: Path,
):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    candidate = make_candidate()
    record = ledger.record_selected(
        candidate, context=context(), exposure_id="exposure-1", event_id="event-1"
    )
    assert record.state == "selected"
    with pytest.raises(ValueError):
        ledger.record_used("exposure-1")
    material = SourceMaterial(
        source_ref=candidate.source_ref,
        source_class=candidate.source_class,
        source_event_time=candidate.source_event_time,
        created_at=candidate.created_at,
        expires_at=candidate.expires_at,
        body="opaque source body",
    )
    opened = ledger.record_opened("exposure-1", material, event_id="event-2")
    assert opened.state == "opened"
    exposed = ledger.record_exposed("exposure-1", event_id="event-3")
    assert exposed.state == "exposed"
    used = ledger.record_used("exposure-1", event_id="event-4")
    assert used.state == "used"
    evidence = ReplyUseEvidence.from_content("reply-1", "reply evidence")
    consumed = ledger.record_consumed("exposure-1", evidence, event_id="event-5")
    assert consumed.state == "consumed"
    with pytest.raises(ExposureConflictError):
        ledger.record_consumed(
            "exposure-1",
            ReplyUseEvidence.from_content("reply-other", "reply evidence"),
        )
    assert [row["event"] for row in ledger.events()] == [
        "selected",
        "opened",
        "exposed",
        "used",
        "consumed",
    ]


def test_duplicate_replay_and_concurrent_exposure_are_idempotent(tmp_path: Path):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    candidate = make_candidate()
    ctx = context()

    def expose(_index: int):
        return ledger.record_selected(
            candidate, context=ctx, exposure_id="same", event_id="same-event"
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(expose, range(12)))
    assert {item.exposure_id for item in results} == {"same"}
    assert len(ledger.events()) == 1
    assert ExposureLedger(tmp_path, clock=lambda: NOW).get("same") == results[0]

    with pytest.raises(ExposureConflictError):
        ledger.record_selected(make_candidate("other"), context=ctx, exposure_id="same")


def test_exposure_cap_is_atomic_and_failed_selection_does_not_consume_it(
    tmp_path: Path,
):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    candidates = [
        make_candidate(f"card:{index}", source_class="public")
        for index in ("one", "two")
    ]
    exposure_ids = []
    for index, (candidate, turn_index) in enumerate(zip(candidates, (0, 1))):
        selected = ledger.record_selected(
            candidate,
            context=context(turn_index=turn_index),
            exposure_id=f"exposure-{index}",
        )
        exposure_ids.append(selected.exposure_id)
        ledger.record_opened(
            selected.exposure_id,
            SourceMaterial(
                source_ref=candidate.source_ref,
                source_class=candidate.source_class,
                source_event_time=candidate.source_event_time,
                created_at=candidate.created_at,
                expires_at=candidate.expires_at,
                body="opaque source body",
            ),
        )

    def expose(exposure_id: str):
        try:
            return ledger.record_exposed(exposure_id, exposure_cap=1)
        except ValueError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(expose, exposure_ids))

    assert sum(item.state == "exposed" for item in ledger.replay()) == 1
    assert sum(item.state == "opened" for item in ledger.replay()) == 1
    assert sum(isinstance(item, ValueError) for item in results) == 1

    failed_candidate = make_candidate("card:failed", source_class="public")
    failed = ledger.record_selected(
        failed_candidate,
        context=context(turn_index=2),
        exposure_id="exposure-failed",
    )
    ledger.record_open_failed(failed.exposure_id, "opener_unavailable")
    assert ledger.session_count("session-1") == 1


def test_exact_open_enforces_returned_body_byte_limit(tmp_path: Path):
    candidate = SourceCandidate(
        source_ref="card:large",
        source_class="public",
        source_event_time=NOW - timedelta(days=1),
        created_at=NOW - timedelta(hours=1),
        expires_at=NOW + timedelta(hours=1),
    )

    class LargeOpener:
        def open(self, source_ref: str, *, max_bytes: int):
            return SourceMaterial(
                source_ref=source_ref,
                source_class=candidate.source_class,
                source_event_time=candidate.source_event_time,
                created_at=candidate.created_at,
                expires_at=candidate.expires_at,
                body="x" * 100,
            )

    with pytest.raises(MissingEvidenceError, match="byte limit"):
        SourceRegistry(opener=LargeOpener()).exact_open(candidate, max_bytes=10)


def test_exact_open_rejects_context_source_kind_change(tmp_path: Path):
    candidate = make_candidate()
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=Opener("opaque source body")),
        clock=lambda: NOW,
    )
    exposed = orchestrator.expose_candidates([candidate], context=context(), now=NOW)[0]
    system_context = ExposureContext(
        "session-1", "lifecycle-1", "turn-1", "system", NOW, 0
    )

    with pytest.raises(ExposureConflictError, match="context"):
        orchestrator.open_selected(
            exposed.exposure_id,
            context=system_context,
            now=NOW,
        )


def test_private_continuity_isolated_from_nonprivate_context_by_default(tmp_path: Path):
    candidate = make_candidate()
    policy = ExposurePolicy(result_budget=2)
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    with pytest.raises(PolicyDeniedError):
        policy.choose(
            [candidate],
            context=context(source_kind="system"),
            ledger=ledger,
            now=NOW,
        )
    allowed = policy.choose(
        [candidate],
        context=context(source_kind="system"),
        ledger=ledger,
        now=NOW,
        continuity_policy=lambda source_class, source_kind: (
            source_class == "private_continuity" and source_kind == "system"
        ),
    )
    assert allowed.candidates == (candidate,)


def test_writer_intent_queue_receipt_and_failure_are_visible(tmp_path: Path):
    effects = EffectLedger(tmp_path, clock=lambda: NOW)
    coordinator = WriterCoordinator(effects, clock=lambda: NOW)
    calls: list[object] = []

    def writer(request):
        calls.append(request)

    queued = coordinator.submit(
        "flush",
        writer,
        source_event_id="turn-event",
        idempotency_key="flush-1",
        epoch_id="epoch-1",
        content="flush body",
    )
    assert queued.record.state == "executed_unverified"
    assert len(calls) == 1
    assert calls[0].effect_id == queued.effect_id
    assert calls[0].idempotency_key == "flush-1"
    assert calls[0].source_event_id == "turn-event"
    assert calls[0].epoch_id == "epoch-1"
    assert calls[0].content == "flush body"
    reopened = coordinator.submit(
        "flush",
        writer,
        source_event_id="turn-event",
        idempotency_key="flush-1",
        epoch_id="epoch-1",
        content="flush body",
    )
    assert reopened.record.state == "executed_unverified"
    assert len(calls) == 1
    receipt = EffectReceipt(
        receipt_id="receipt-1",
        event_id="turn-event",
        observed_at=NOW,
        content_sha256=queued.record.content_sha256,
        content_length=queued.record.content_length,
        epoch_id="epoch-1",
    )
    assert coordinator.verify(queued.effect_id, receipt).verified
    with pytest.raises(ValueError):
        coordinator.verify(
            queued.effect_id,
            EffectReceipt(
                receipt_id="receipt-other",
                event_id="turn-event",
                observed_at=NOW,
                content_sha256="b" * 64,
                content_length=1,
                epoch_id="epoch-1",
            ),
        )
    failed = coordinator.submit(
        "diary",
        lambda _request: (_ for _ in ()).throw(RuntimeError("writer unavailable")),
        source_event_id="diary-event",
        idempotency_key="diary-1",
        epoch_id="epoch-1",
        content="diary body",
    )
    assert failed.failed and failed.record.retryable is True


def test_writer_can_directly_return_matching_receipt_and_reopen_does_not_repeat(
    tmp_path: Path,
):
    effects = EffectLedger(tmp_path, clock=lambda: NOW)
    coordinator = WriterCoordinator(effects, clock=lambda: NOW)
    calls = 0

    def writer(request):
        nonlocal calls
        calls += 1
        return EffectReceipt(
            receipt_id="direct-receipt",
            event_id=request.source_event_id,
            observed_at=NOW,
            content_sha256=request.content_sha256,
            content_length=request.content_length,
            epoch_id=request.epoch_id,
        )

    first = coordinator.submit(
        "diary",
        writer,
        source_event_id="diary-event",
        idempotency_key="diary-idem",
        epoch_id="epoch-1",
        content={"title": "fixture"},
    )
    assert first.verified and calls == 1
    second = coordinator.submit(
        "diary",
        writer,
        source_event_id="diary-event",
        idempotency_key="diary-idem",
        epoch_id="epoch-1",
        content={"title": "fixture"},
    )
    assert second.verified and calls == 1


def test_maintenance_facade_is_reference_only_and_archive_is_proposal(tmp_path: Path):
    class Store:
        def propose_maintenance(self, **kwargs):
            return {"operation": kwargs["operation"], "status": "proposed"}

        def apply_maintenance(self, proposal_id, **kwargs):
            return {"proposal_id": proposal_id, "status": "applied"}

    facade = MemoryMaintenanceFacade(Store(), approval_adapter=lambda _proposal: True)
    proposal = facade.archive(
        request_id="archive-1", evidence_refs=["card:one"], reason="fixture"
    )
    assert proposal["operation"] == "retire"
    assert facade.approval_required(proposal) is True
    assert (
        facade.apply(
            "proposal:archive-1",
            activity="manual",
            permission="manual",
            approval_evidence="owner-approved",
        )["status"]
        == "applied"
    )
    forbidden = ("de" + "lete", "pur" + "ge")
    assert not any(name in dir(facade) for name in forbidden)


def test_approval_required_stays_pending_until_explicit_evidence(tmp_path: Path):
    class Store:
        def __init__(self):
            self.propose_calls = 0
            self.apply_calls = 0

        def propose_maintenance(self, **kwargs):
            self.propose_calls += 1
            return {"proposal_id": "proposal-sensitive", "status": "proposed"}

        def apply_maintenance(self, proposal_id, **kwargs):
            self.apply_calls += 1
            return {"proposal_id": proposal_id, "status": "applied"}

    store = Store()
    facade = MemoryMaintenanceFacade(store, root=tmp_path, clock=lambda: NOW)
    proposal = facade.propose(
        request_id="request-sensitive",
        operation="distill",
        evidence_refs=["card:one"],
        reason="sensitive relationship fixture",
        approval_required=True,
    )
    assert proposal["approval_state"] == "pending"
    blocked = facade.apply("proposal-sensitive", activity="manual", permission="manual")
    assert blocked["status"] == "pending"
    assert store.apply_calls == 0
    applied = facade.apply(
        "proposal-sensitive",
        activity="manual",
        permission="manual",
        approval_evidence={"approval_id": "owner-1", "decision": "approve"},
    )
    assert applied["status"] == "applied"
    assert store.apply_calls == 1


def test_explicit_false_cannot_downgrade_adapter_approval(tmp_path: Path):
    class Store:
        def __init__(self):
            self.apply_calls = 0

        def propose_maintenance(self, **kwargs):
            return {"proposal_id": "proposal-sensitive", "status": "proposed"}

        def apply_maintenance(self, proposal_id, **kwargs):
            self.apply_calls += 1
            return {"proposal_id": proposal_id, "status": "applied"}

    store = Store()
    facade = MemoryMaintenanceFacade(
        store,
        root=tmp_path,
        approval_adapter=lambda _proposal: True,
        clock=lambda: NOW,
    )
    proposal = facade.propose(
        request_id="request-sensitive",
        operation="distill",
        evidence_refs=["card:one"],
        reason="sensitive relationship fixture",
        approval_required=False,
    )
    assert proposal["approval_required"] is True
    assert facade.approval_required({"approval_required": False}) is True
    blocked = facade.apply(
        "proposal-sensitive",
        activity="manual",
        permission="manual",
    )
    assert blocked["status"] == "pending"
    assert store.apply_calls == 0


def test_unknown_maintenance_apply_fails_closed_before_backend_root_and_memory(
    tmp_path: Path,
):
    class Store:
        def __init__(self):
            self.apply_calls = 0

        def propose_maintenance(self, **kwargs):
            return {"proposal_id": "known-proposal", "status": "proposed"}

        def apply_maintenance(self, proposal_id, **kwargs):
            self.apply_calls += 1
            return {"proposal_id": proposal_id, "status": "applied"}

    store = Store()
    root = tmp_path / "durable"
    facade = MemoryMaintenanceFacade(store, root=root, clock=lambda: NOW)
    facade.propose(
        request_id="known-request",
        operation="retire",
        evidence_refs=[],
        reason="fixture",
        approval_required=False,
    )
    restarted = MemoryMaintenanceFacade(store, root=root, clock=lambda: NOW)
    with pytest.raises(ValueError, match="not registered"):
        restarted.apply("unknown-proposal", activity="manual", permission="manual")
    assert store.apply_calls == 0

    in_memory = MemoryMaintenanceFacade(store, clock=lambda: NOW)
    with pytest.raises(ValueError, match="not registered"):
        in_memory.apply("unknown-proposal", activity="manual", permission="manual")
    assert store.apply_calls == 0


def test_historical_open_returns_machine_readable_event_framing(tmp_path: Path):
    old = make_candidate(event_time=NOW - timedelta(days=4))

    class HistoricalOpener:
        def open(self, source_ref: str, *, max_bytes: int):
            return SourceMaterial(
                source_ref=source_ref,
                source_class=old.source_class,
                source_event_time=old.source_event_time,
                created_at=old.created_at,
                expires_at=old.expires_at,
                body="opaque source body",
            )

    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        source_registry=SourceRegistry(opener=HistoricalOpener()),
        clock=lambda: NOW,
    )
    result = orchestrator.expose_candidates([old], context=context(), now=NOW)[0]
    assert result.state == "exposed"
    assert result.framing == "historical"
    assert result.framing_date == old.source_event_time.date()
    assert result.material.created_at != result.material.source_event_time


def test_corrupt_ledger_fails_closed(tmp_path: Path):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    ledger.ledger.append({"schema_version": "wrong"})
    with pytest.raises(StateError):
        ledger.replay()
    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)
    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "exposure_ledger_corrupt"


def test_exposure_observer_is_lock_free_and_marks_open_failure_recovery(
    tmp_path: Path,
):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    assert ledger.observer_status(target_date=NOW.date(), now=NOW) == ()
    assert not ledger.mutation_lock.exists()

    candidate = SourceCandidate(
        source_ref="card:observer",
        source_class="public",
        source_event_time=NOW - timedelta(days=1),
        created_at=NOW - timedelta(hours=1),
        expires_at=NOW + timedelta(hours=1),
    )
    first = ledger.record_selected(
        candidate,
        context=context(),
        exposure_id="failed-exposure",
        event_id="failed-selected",
        now=NOW,
    )
    ledger.record_open_failed(first.exposure_id, "opener_unavailable", now=NOW)
    ledger.mutation_lock.unlink()
    failed_facts = ledger.observer_status(target_date=NOW.date(), now=NOW)
    failed = next(item for item in failed_facts if item.code == "exposure_open_failed")
    assert failed.state == "current"
    before = ledger.ledger.path.stat().st_mtime_ns
    assert not ledger.mutation_lock.exists()

    later = NOW + timedelta(minutes=1)
    second = ledger.record_selected(
        candidate,
        context=context(turn_index=1),
        exposure_id="recovered-exposure",
        event_id="recovered-selected",
        now=later,
    )
    material = SourceMaterial(
        source_ref=candidate.source_ref,
        source_class=candidate.source_class,
        source_event_time=candidate.source_event_time,
        created_at=candidate.created_at,
        body="synthetic source body",
        expires_at=candidate.expires_at,
    )
    ledger.record_opened(second.exposure_id, material, now=later)
    ledger.record_exposed(second.exposure_id, now=later)
    ledger.mutation_lock.unlink()
    before_recovery = (
        ledger.ledger.path.stat().st_mtime_ns,
        ledger.ledger.path.read_bytes(),
    )
    facts = ledger.observer_status(target_date=later.date(), now=later)
    recovered = next(item for item in facts if item.code == "exposure_open_failed")
    assert recovered.state == "recovered_history"
    assert recovered.recovery is not None
    assert "synthetic source body" not in repr(facts)
    assert ledger.ledger.path.stat().st_mtime_ns != before
    assert (
        ledger.ledger.path.stat().st_mtime_ns,
        ledger.ledger.path.read_bytes(),
    ) == before_recovery
    assert not ledger.mutation_lock.exists()


def test_writer_observer_pending_to_verified_is_read_only(tmp_path: Path):
    effects = EffectLedger(tmp_path, clock=lambda: NOW)
    effects.begin_intent(
        "observer-effect",
        kind="flush",
        source_event_id="observer-source",
        idempotency_key="observer-idempotency",
        epoch_id="observer-epoch",
        content_sha256="a" * 64,
        content_length=8,
        expires_at=NOW + timedelta(hours=1),
        created_at=NOW,
    )
    effects.mark_pending("observer-effect")
    path = tmp_path / "effects.jsonl"
    effects.mutation_lock.unlink()
    before = (path.stat().st_mtime_ns, path.read_bytes())
    coordinator = WriterCoordinator(effects, clock=lambda: NOW)
    pending = coordinator.observer_status(target_date=NOW.date(), now=NOW)
    assert any(
        item.code == "effect_pending" and item.state == "current" for item in pending
    )
    assert (path.stat().st_mtime_ns, path.read_bytes()) == before
    assert not effects.mutation_lock.exists()
    assert not coordinator._handoff_lock_path.exists()

    effects.verify(
        "observer-effect",
        EffectReceipt(
            receipt_id="observer-receipt",
            event_id="observer-source",
            observed_at=NOW + timedelta(minutes=1),
            content_sha256="a" * 64,
            content_length=8,
            epoch_id="observer-epoch",
        ),
    )
    effects.mutation_lock.unlink()
    verified_before = (path.stat().st_mtime_ns, path.read_bytes())
    verified = coordinator.observer_status(
        target_date=NOW.date(), now=NOW + timedelta(minutes=1)
    )
    fact = next(item for item in verified if item.code == "effect_verified")
    assert fact.state == "recovered_history"
    assert fact.recovery is not None
    assert "writer body" not in repr(verified)
    assert (path.stat().st_mtime_ns, path.read_bytes()) == verified_before
    assert not effects.mutation_lock.exists()


def test_exposure_cap_and_cooldown_are_neutral_policy_facts(tmp_path: Path):
    ledger = ExposureLedger(tmp_path, clock=lambda: NOW)
    candidate = SourceCandidate(
        source_ref="card:policy",
        source_class="public",
        source_event_time=NOW - timedelta(days=1),
        created_at=NOW - timedelta(hours=1),
        expires_at=NOW + timedelta(hours=1),
    )
    selected = ledger.record_selected(candidate, context=context(), now=NOW)
    ledger.record_opened(
        selected.exposure_id,
        SourceMaterial(
            source_ref=candidate.source_ref,
            source_class=candidate.source_class,
            source_event_time=candidate.source_event_time,
            created_at=candidate.created_at,
            body="synthetic policy body",
            expires_at=candidate.expires_at,
        ),
        now=NOW,
    )
    ledger.record_exposed(selected.exposure_id, now=NOW)
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ledger,
        policy=ExposurePolicy(max_per_session=1, source_cooldown=timedelta(minutes=5)),
        clock=lambda: NOW,
    )
    facts = orchestrator.observer_status(target_date=NOW.date(), now=NOW)
    policy_facts = [
        item
        for item in facts
        if item.code in {"exposure_cap_reached", "exposure_cooldown"}
    ]
    assert policy_facts
    assert all(item.state == "neutral" for item in policy_facts)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("operation", "retire"),
        ("request_id", "request:other"),
        ("evidence_sha256", "b" * 64),
    ],
)
def test_maintenance_observer_rejects_history_identity_mismatch(
    tmp_path: Path, field: str, value: str
):
    maintenance_path, history_path = _maintenance_observer_rows(
        tmp_path, history_overrides={field: value}
    )

    class Store:
        def __init__(self):
            self.maintenance = JsonlLedger(maintenance_path)
            self.history = JsonlLedger(history_path)

    facade = MemoryMaintenanceFacade(Store(), root=tmp_path, clock=lambda: NOW)
    owner_files = (maintenance_path, history_path)
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    }
    facts = facade.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "maintenance_ledger_corrupt"
    assert "private observer body" not in repr(facts)
    assert {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    } == before
    assert not tuple(tmp_path.glob("*.lock"))


def test_maintenance_observer_accepts_matching_history_as_neutral(tmp_path: Path):
    maintenance_path, history_path = _maintenance_observer_rows(tmp_path)

    class Store:
        def __init__(self):
            self.maintenance = JsonlLedger(maintenance_path)
            self.history = JsonlLedger(history_path)

    facade = MemoryMaintenanceFacade(Store(), root=tmp_path, clock=lambda: NOW)
    owner_files = (maintenance_path, history_path)
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    }
    facts = facade.observer_status(target_date=NOW.date(), now=NOW)
    proposal_fact = next(
        item
        for item in facts
        if item.key == "memory.maintenance.proposal:proposal:observer"
    )

    assert proposal_fact.state == "neutral"
    assert proposal_fact.code == "maintenance_applied"
    assert "private observer body" not in repr(facts)
    assert {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    } == before
    assert not tuple(tmp_path.glob("*.lock"))


def test_maintenance_observer_orphan_history_fails_closed(tmp_path: Path):
    maintenance_path, history_path = _maintenance_observer_rows(
        tmp_path, include_history=False
    )
    maintenance_path.unlink()
    JsonlLedger(history_path).append(
        {
            "schema_version": "moon.memory.history.v1",
            "kind": "maintenance_applied",
            "event_id": "history:orphan",
            "proposal_id": "proposal:orphan",
            "request_id": "request:orphan",
            "created_at": NOW.isoformat(),
            "operation": "merge",
            "activity": "observer fixture",
            "permission": "manual",
            "evidence_sha256": "a" * 64,
            "created_card": None,
            "archived_card_ids": [],
            "status": "applied",
        }
    )
    for lock_path in tmp_path.glob("*.lock"):
        lock_path.unlink()

    class Store:
        def __init__(self):
            self.maintenance = JsonlLedger(maintenance_path)
            self.history = JsonlLedger(history_path)

    facade = MemoryMaintenanceFacade(Store(), root=tmp_path, clock=lambda: NOW)
    facts = facade.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "maintenance_ledger_corrupt"
    assert "proposal:orphan" not in repr(facts)


def test_maintenance_observer_unknown_approval_fails_closed(tmp_path: Path):
    maintenance_path, history_path = _maintenance_observer_rows(
        tmp_path, include_history=False
    )
    approval_path = tmp_path / "memory_orchestration_approvals.jsonl"
    JsonlLedger(approval_path).append(
        {
            "schema_version": "moon.memory.approval.v1",
            "kind": "approval",
            "event": "pending",
            "proposal_id": "proposal:unknown",
            "required": True,
            "approved": False,
            "created_at": NOW.isoformat(),
            "evidence_sha256": None,
            "evidence_length": None,
        }
    )
    for lock_path in tmp_path.glob("*.lock"):
        lock_path.unlink()

    class Store:
        def __init__(self):
            self.maintenance = JsonlLedger(maintenance_path)
            self.history = JsonlLedger(history_path)

    facade = MemoryMaintenanceFacade(Store(), root=tmp_path, clock=lambda: NOW)
    facts = facade.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "maintenance_ledger_corrupt"
    assert "proposal:unknown" not in repr(facts)


def test_maintenance_observer_in_memory_fallback_never_infers_applied():
    class Store:
        def propose_maintenance(self, **kwargs):
            return {
                "proposal_id": "proposal:memory",
                "request_id": "request:memory",
                "operation": kwargs["operation"],
                "status": "proposed",
            }

        def apply_maintenance(self, proposal_id, **kwargs):
            return {"proposal_id": proposal_id, "status": "applied"}

    facade = MemoryMaintenanceFacade(Store(), clock=lambda: NOW)
    proposal = facade.propose(
        request_id="request:memory",
        operation="retire",
        evidence_refs=[],
        reason="private in-memory body",
    )
    assert (
        facade.apply(proposal["proposal_id"], activity="fixture", permission="manual")[
            "status"
        ]
        == "applied"
    )

    facts = facade.observer_status(target_date=NOW.date(), now=NOW)
    proposal_fact = next(
        item
        for item in facts
        if item.key == "memory.maintenance.proposal:proposal:memory"
    )
    assert proposal_fact.state == "current"
    assert proposal_fact.code == "maintenance_proposal_pending"
    assert proposal_fact.event_time is None
    assert "private in-memory body" not in repr(facts)


def test_maintenance_observer_reports_pending_then_applied_without_card_body(
    tmp_path: Path,
):
    from moonbite_plugin.memory import MemoryStore

    store = MemoryStore(tmp_path, clock=lambda: NOW)
    card = store.add_card(
        "synthetic card body must stay private",
        provenance="user_explicit",
        source_ref="observer-source",
        card_id="observer-card",
    )
    facade = MemoryMaintenanceFacade(
        store,
        root=tmp_path,
        approval_adapter=lambda _proposal: True,
        clock=lambda: NOW,
    )
    proposal = facade.propose(
        request_id="observer-request",
        operation="retire",
        evidence_refs=[card.open_ref],
        reason="synthetic observer fixture",
    )
    pending = facade.observer_status(target_date=NOW.date(), now=NOW)
    assert any(item.code == "maintenance_proposal_pending" for item in pending)
    assert any(item.code == "maintenance_approval_pending" for item in pending)
    assert "synthetic card body" not in repr(pending)

    facade.apply(
        proposal["proposal_id"],
        activity="observer fixture",
        permission="manual",
        approval_evidence="owner approval evidence",
    )
    for lock_path in tmp_path.glob("*.lock"):
        lock_path.unlink()
    owner_files = tuple(sorted(tmp_path.glob("*.jsonl")))
    before_owner_files = {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    }
    applied = facade.observer_status(target_date=NOW.date(), now=NOW)
    fact = next(
        item
        for item in applied
        if item.key == f"memory.maintenance.proposal:{proposal['proposal_id']}"
    )
    assert fact.state == "neutral"
    assert fact.recovery is None
    assert "owner approval evidence" not in repr(applied)
    assert {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in owner_files
    } == before_owner_files
    assert not tuple(tmp_path.glob("*.lock"))


def test_memory_orchestrator_dedupes_owner_fact_keys(tmp_path: Path):
    orchestrator = MemoryOrchestrator(
        exposure_ledger=ExposureLedger(tmp_path, clock=lambda: NOW),
        memory_store=None,
        clock=lambda: NOW,
    )
    facts = orchestrator.observer_status(target_date=NOW.date(), now=NOW)
    assert len({item.key for item in facts}) == len(facts)
    assert facts == ()
