from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import (
    EffectResult,
    HeartbeatCadence,
    HeartbeatCandidate,
    HeartbeatEngine,
    JudgeDecision,
)
from moonbite_plugin.runtime_core import EventBus, FileRuntimeLocks


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class FixedJudge:
    def __init__(self, decision: JudgeDecision):
        self.decision = decision
        self.calls = 0

    def decide(self, _candidate: HeartbeatCandidate) -> JudgeDecision:
        self.calls += 1
        return self.decision


class QueueSink:
    def __init__(self, *, delivery=None, wake=None):
        self.delivery_value = delivery
        self.wake_value = wake
        self.calls: list[str] = []

    def deliver(self, _candidate, _decision, intent=None):
        self.calls.append("deliver")
        value = self.delivery_value
        return (
            value(intent) if callable(value) else value or EffectResult(True, "queued")
        )

    def wake(self, _candidate, _decision, intent=None):
        self.calls.append("wake")
        value = self.wake_value
        return (
            value(intent) if callable(value) else value or EffectResult(True, "queued")
        )


def make_engine(
    tmp_path,
    decision: JudgeDecision,
    sink,
    *,
    kind_policies=None,
    clock=None,
    effect_ttl=None,
):
    if clock is None:
        clock = lambda: NOW
    judge = FixedJudge(decision)
    ledger = EffectLedger(tmp_path, clock=clock)
    cadence = HeartbeatCadence(tmp_path, clock=clock)
    engine = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=clock),
        controls=ControlStore(tmp_path, clock=clock),
        cadence=cadence,
        judge=judge,
        sink=sink,
        effect_ledger=ledger,
        effect_ttl=effect_ttl,
        kind_policies=kind_policies,
    )
    return engine, judge, ledger


def candidate(kind: str, source: str, **changes) -> HeartbeatCandidate:
    context = {
        "events": [f"event-{source}"],
        "due": True,
        "source_event_id": source,
    }
    context.update(changes)
    return HeartbeatCandidate(kind, context, candidate_id=source)


def policy(*, profile: str, host_only: bool, bypass=()):
    return {
        "enabled": True,
        "profile": profile,
        "judge": "required",
        "host_only": host_only,
        "bypass": list(bypass),
    }


class PathlessCadence:
    """Delegate a real cadence while hiding its durable path from the host."""

    def __init__(self, cadence: HeartbeatCadence):
        self._cadence = cadence

    @property
    def path(self):
        raise AttributeError("path intentionally unavailable")

    def __getattr__(self, name):
        if name == "path":
            raise AttributeError("path intentionally unavailable")
        return getattr(self._cadence, name)


def receipt_for(value, **changes) -> EffectReceipt:
    receipt = EffectReceipt(
        receipt_id=f"receipt-{value.effect_id}",
        event_id=value.source_event_id,
        observed_at=value.created_at,
        content_sha256=value.content_sha256,
        content_length=value.content_length,
        epoch_id=value.epoch_id,
    )
    return replace(receipt, **changes)


@pytest.mark.parametrize(
    ("allow_autonomy", "expected_status", "expected_reason"),
    (
        (True, "allowed", "allowed"),
        (None, "skipped", "silent"),
    ),
)
def test_no_effect_decisions_skip_effect_plan_source_validation(
    tmp_path, allow_autonomy, expected_status, expected_reason
):
    """Public run preserves no-effect behavior even with a truthy non-string source."""

    sink = QueueSink()
    engine, judge, ledger = make_engine(
        tmp_path,
        JudgeDecision(
            False,
            False,
            "silent",
            allow_autonomy=allow_autonomy,
        ),
        sink,
    )
    heartbeat_candidate = HeartbeatCandidate(
        "care_poke",
        {
            "events": ["synthetic-event"],
            "due": True,
            "source_event_id": 12345,
        },
        candidate_id="non-string-source-candidate",
    )

    result = engine.run(heartbeat_candidate)

    assert (result.status, result.reason) == (expected_status, expected_reason)
    assert judge.calls == 1
    assert sink.calls == []
    assert not ledger.records()
    assert not (tmp_path / "heartbeat_effect_plans.jsonl").exists()


@pytest.mark.parametrize("effect", ("wake", "delivery"))
def test_effect_with_pathless_cadence_fails_before_mark_judge(tmp_path, effect):
    """Effect-bearing hosts need a durable cadence root before any cadence write."""

    cadence = HeartbeatCadence(tmp_path / "cadence", clock=lambda: NOW)
    cadence_path = cadence.path
    before_cadence = cadence_path.read_bytes() if cadence_path.exists() else None
    sink = QueueSink()
    decision = (
        JudgeDecision(True, False, "wake")
        if effect == "wake"
        else JudgeDecision(False, True, "contact", "hello")
    )
    judge = FixedJudge(decision)
    ledger = EffectLedger(tmp_path / "ledger", clock=lambda: NOW)
    engine = HeartbeatEngine(
        bus=EventBus(tmp_path / "bus", clock=lambda: NOW),
        controls=ControlStore(tmp_path / "controls", clock=lambda: NOW),
        cadence=PathlessCadence(cadence),
        judge=judge,
        sink=sink,
        effect_ledger=ledger,
        locks=FileRuntimeLocks(tmp_path / "locks"),
    )

    result = engine.run(candidate("care_poke", f"pathless-{effect}"))

    assert result.status == "failed"
    assert result.reason == "effect_replay_error"
    assert judge.calls == 1
    assert sink.calls == []
    assert not ledger.records()
    after_cadence = cadence_path.read_bytes() if cadence_path.exists() else None
    assert after_cadence == before_cadence


def test_pending_effect_from_other_kind_blocks_urgent_contact_bypass(tmp_path):
    """Pending reconciliation is global across heartbeat kinds by contract."""

    sink = QueueSink()
    engine, judge, ledger = make_engine(
        tmp_path,
        JudgeDecision(True, False, "wake"),
        sink,
        kind_policies={
            "routine_signal": policy(profile="routine", host_only=False),
            "urgent_signal": policy(
                profile="urgent",
                host_only=True,
                bypass=("recent_contact", "active_chat"),
            ),
        },
    )

    first = engine.run(candidate("routine_signal", "routine-source"))
    second = engine.run(
        candidate(
            "urgent_signal",
            "urgent-source",
            recent_private_inbound_at=NOW,
            active_chat=True,
        )
    )

    assert first.status == "pending"
    assert first.wake is not None
    assert second.status == "pending"
    assert second.reason == "awaiting_receipt"
    assert second.wake is None
    assert judge.calls == 1
    assert sink.calls == ["wake"]
    assert [record.source_event_id for record in ledger.records()] == ["routine-source"]


class BadReceiptSink:
    def __init__(self, **changes):
        self.changes = changes

    def deliver(self, _candidate, _decision, intent=None):
        changes = {
            key: value(intent) if callable(value) else value
            for key, value in self.changes.items()
        }
        return receipt_for(intent, **changes)


def test_sync_and_delegated_delivery_reject_the_same_identity_mismatch(tmp_path):
    """Both entry paths delegate identity checking to EffectLedger.verify."""

    sync_engine, _judge, sync_ledger = make_engine(
        tmp_path / "sync",
        JudgeDecision(False, True, "contact", "hello"),
        BadReceiptSink(event_id="wrong-source"),
    )
    sync_result = sync_engine.run(candidate("care_poke", "identity-source"))
    assert sync_result.status == "failed"
    assert sync_result.delivery is not None
    assert sync_result.delivery.status == "receipt_mismatch"
    assert sync_ledger.get(sync_result.delivery.effect_id).state == "failed"

    delegated_sink = QueueSink()
    delegated_engine, _judge, delegated_ledger = make_engine(
        tmp_path / "delegated",
        JudgeDecision(
            True,
            True,
            "contact",
            "hello",
            delivery_mode="delegated",
        ),
        delegated_sink,
    )
    delegated_result = delegated_engine.run(candidate("care_poke", "identity-source"))
    delegated_record = delegated_ledger.get(delegated_result.delivery.effect_id)
    with pytest.raises(ValueError, match="receipt mismatch"):
        delegated_engine.reconcile_heartbeat_delivery(
            delegated_result.delivery.effect_id,
            "verified",
            receipt_for(delegated_record, event_id="wrong-source"),
        )
    assert delegated_ledger.get(delegated_result.delivery.effect_id).state == (
        "executed_unverified"
    )


@pytest.mark.parametrize("boundary", ("before_created", "at_expiry"))
def test_all_heartbeat_receipt_entry_points_reject_out_of_lifetime_receipts(
    tmp_path, boundary
):
    """Every heartbeat receipt seam enforces the half-open effect lifetime."""

    def observed_at(value):
        return (
            value.created_at - timedelta(microseconds=1)
            if boundary == "before_created"
            else value.expires_at
        )

    sync_engine, _judge, sync_ledger = make_engine(
        tmp_path / "sync",
        JudgeDecision(False, True, "contact", "hello"),
        BadReceiptSink(observed_at=observed_at),
    )
    sync_result = sync_engine.run(candidate("care_poke", "time-source"))
    assert sync_result.status == "failed"
    assert sync_result.delivery is not None
    assert sync_result.delivery.status == "receipt_mismatch"
    assert sync_ledger.get(sync_result.delivery.effect_id).state == "failed"

    delegated_engine, _judge, delegated_ledger = make_engine(
        tmp_path / "delegated",
        JudgeDecision(
            True,
            True,
            "contact",
            "hello",
            delivery_mode="delegated",
        ),
        QueueSink(),
    )
    delegated_result = delegated_engine.run(candidate("care_poke", "time-source"))
    delegated_record = delegated_ledger.get(delegated_result.delivery.effect_id)
    with pytest.raises(ValueError, match="outside the effect lifetime"):
        delegated_engine.reconcile_heartbeat_delivery(
            delegated_result.delivery.effect_id,
            "verified",
            receipt_for(delegated_record, observed_at=observed_at(delegated_record)),
        )
    assert delegated_ledger.get(delegated_result.delivery.effect_id).state == (
        "executed_unverified"
    )

    wake_engine, _judge, wake_ledger = make_engine(
        tmp_path / "wake",
        JudgeDecision(True, False, "wake"),
        QueueSink(),
    )
    wake_result = wake_engine.run(candidate("care_poke", "time-source"))
    wake_record = wake_ledger.get(wake_result.wake.effect_id)
    with pytest.raises(ValueError, match="outside the effect lifetime"):
        wake_engine.reconcile_heartbeat_wake(
            wake_result.wake.effect_id,
            receipt_for(wake_record, observed_at=observed_at(wake_record)),
        )
    assert wake_ledger.get(wake_result.wake.effect_id).state == ("executed_unverified")


def test_heartbeat_receipts_at_created_at_are_valid_for_all_entry_points(tmp_path):
    """The inclusive lower bound is valid at each heartbeat receipt seam."""

    sync_engine, _judge, sync_ledger = make_engine(
        tmp_path / "sync",
        JudgeDecision(False, True, "contact", "hello"),
        BadReceiptSink(observed_at=lambda value: value.created_at),
    )
    sync_result = sync_engine.run(candidate("care_poke", "lower-bound-sync"))
    assert sync_result.status == "completed"
    assert sync_result.delivery is not None and sync_result.delivery.verified
    assert sync_ledger.get(sync_result.delivery.effect_id).state == "verified"

    delegated_engine, _judge, delegated_ledger = make_engine(
        tmp_path / "delegated",
        JudgeDecision(
            True,
            True,
            "contact",
            "hello",
            delivery_mode="delegated",
        ),
        QueueSink(),
    )
    delegated_result = delegated_engine.run(
        candidate("care_poke", "lower-bound-delegated")
    )
    delegated_record = delegated_ledger.get(delegated_result.delivery.effect_id)
    delegated_reconciled = delegated_engine.reconcile_heartbeat_delivery(
        delegated_result.delivery.effect_id,
        "verified",
        receipt_for(delegated_record, observed_at=delegated_record.created_at),
    )
    assert delegated_reconciled.status == "verified"
    assert delegated_ledger.get(delegated_result.delivery.effect_id).state == (
        "verified"
    )

    wake_engine, _judge, wake_ledger = make_engine(
        tmp_path / "wake",
        JudgeDecision(True, False, "wake"),
        QueueSink(),
    )
    wake_result = wake_engine.run(candidate("care_poke", "lower-bound-wake"))
    wake_record = wake_ledger.get(wake_result.wake.effect_id)
    wake_reconciled = wake_engine.reconcile_heartbeat_wake(
        wake_result.wake.effect_id,
        receipt_for(wake_record, observed_at=wake_record.created_at),
    )
    assert wake_reconciled.status == "verified"
    assert wake_ledger.get(wake_result.wake.effect_id).state == "verified"


@pytest.mark.parametrize("kind", ("delegated_delivery", "wake"))
def test_reconciliation_accepts_in_lifetime_receipt_after_clock_passes_expiry(
    tmp_path, kind
):
    """Receipt time is checked against the record, not reconciliation wall time."""

    current = [NOW]
    clock = lambda: current[0]
    if kind == "delegated_delivery":
        engine, _judge, ledger = make_engine(
            tmp_path,
            JudgeDecision(
                True,
                True,
                "contact",
                "hello",
                delivery_mode="delegated",
            ),
            QueueSink(),
            clock=clock,
            effect_ttl=timedelta(minutes=1),
        )
        result = engine.run(candidate("care_poke", "late-delegated"))
        effect_id = result.delivery.effect_id
    else:
        engine, _judge, ledger = make_engine(
            tmp_path,
            JudgeDecision(True, False, "wake"),
            QueueSink(),
            clock=clock,
            effect_ttl=timedelta(minutes=1),
        )
        result = engine.run(candidate("care_poke", "late-wake"))
        effect_id = result.wake.effect_id

    record = ledger.get(effect_id)
    assert record.state == "executed_unverified"
    current[0] = record.expires_at + timedelta(microseconds=1)
    assert ledger.get(effect_id).state == "executed_unverified"
    receipt = receipt_for(record, observed_at=record.created_at)

    if kind == "delegated_delivery":
        reconciled = engine.reconcile_heartbeat_delivery(effect_id, "verified", receipt)
    else:
        reconciled = engine.reconcile_heartbeat_wake(effect_id, receipt)

    assert reconciled.status == "verified"
    assert ledger.get(effect_id).state == "verified"


class DeliveryFailure(RuntimeError):
    pass


class WakeFailure(RuntimeError):
    pass


class DualFailureSink:
    def __init__(self):
        self.calls: list[str] = []

    def deliver(self, _candidate, _decision, _intent=None):
        self.calls.append("deliver")
        raise DeliveryFailure("delivery transport")

    def wake(self, _candidate, _decision, _intent=None):
        self.calls.append("wake")
        raise WakeFailure("wake transport")


def test_two_effect_failures_are_attempted_and_retained_in_terminal_diagnostics(
    tmp_path,
):
    sink = DualFailureSink()
    engine, _judge, ledger = make_engine(
        tmp_path,
        JudgeDecision(True, True, "contact", "hello"),
        sink,
    )

    result = engine.run(candidate("care_poke", "dual-failure"))

    assert result.status == "failed"
    assert result.reason == "effect_failed"
    assert sink.calls == ["deliver", "wake"]
    assert result.delivery is not None
    assert result.delivery.status == "delivery_error:DeliveryFailure"
    assert result.wake is not None
    assert result.wake.status == "wake_error:WakeFailure"
    records = {record.kind: record for record in ledger.records()}
    assert records["heartbeat_delivery"].state == "failed"
    assert records["heartbeat_wake"].state == "failed"

    terminals = [
        event
        for event in engine.bus.read_audit()
        if event.payload.get("occurrence_id") == "dual-failure"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 1
    payload = terminals[0].payload
    assert payload["effect_ids"] == sorted(records[item].effect_id for item in records)
    assert payload["delivery"]["status"] == "delivery_error:DeliveryFailure"
    assert payload["wake"]["status"] == "wake_error:WakeFailure"

    # A terminal replay is intentionally summary-only; the audit row retains
    # the per-effect evidence above.
    replay = engine.run(candidate("care_poke", "dual-failure"))
    assert (replay.status, replay.reason) == ("failed", "failed")
    assert replay.delivery is None and replay.wake is None
