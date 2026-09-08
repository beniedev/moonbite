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
from moonbite_plugin.runtime_core import EventBus
from moonbite_plugin.service import MoonbiteRuntime


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class WakeJudge:
    def decide(self, _candidate):
        return JudgeDecision(True, False, "synthetic wake")


class DeliveryJudge:
    def decide(self, _candidate):
        return JudgeDecision(False, True, "synthetic delivery", "synthetic text")


class QueueSink:
    def __init__(self):
        self.wakes = 0

    def wake(self, _candidate, _decision):
        self.wakes += 1
        return EffectResult(True, "queued")


class NoVisibleContactCadence(HeartbeatCadence):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.visible_contact_calls = 0

    def record_verified_visible_contact(self, *args, **kwargs):
        self.visible_contact_calls += 1
        raise AssertionError("heartbeat wake must not project visible contact")


def make_engine(tmp_path, *, cadence_type=HeartbeatCadence):
    cadence = cadence_type(tmp_path, clock=lambda: NOW)
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    sink = QueueSink()
    engine = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=ControlStore(tmp_path, clock=lambda: NOW),
        cadence=cadence,
        judge=WakeJudge(),
        sink=sink,
        effect_ledger=ledger,
    )
    return engine, cadence, ledger, sink


def pending_wake(
    engine, cadence, ledger, source="synthetic-source", epoch="synthetic-epoch"
):
    result = engine.run(
        HeartbeatCandidate(
            "care_poke",
            {
                "events": ["synthetic-event"],
                "due": True,
                "source_event_id": source,
                "epoch_id": epoch,
            },
        )
    )
    effect_id = cadence.effect_ref(source, "heartbeat_wake", epoch_id=epoch)
    assert effect_id is not None
    record = ledger.get(effect_id)
    assert record is not None
    assert record.kind == "heartbeat_wake"
    assert record.state == "executed_unverified"
    assert result.wake is not None and not result.wake.verified
    return effect_id, record


def receipt_for(record, **changes):
    receipt = EffectReceipt(
        receipt_id="synthetic-receipt",
        event_id=record.source_event_id,
        observed_at=NOW,
        content_sha256=record.content_sha256,
        content_length=record.content_length,
        epoch_id=record.epoch_id,
    )
    return replace(receipt, **changes)


def test_wake_receipt_verifies_and_only_projects_cadence(tmp_path):
    engine, cadence, ledger, sink = make_engine(
        tmp_path, cadence_type=NoVisibleContactCadence
    )
    effect_id, record = pending_wake(engine, cadence, ledger)

    result = engine.reconcile_heartbeat_wake(effect_id, receipt_for(record))

    assert result.status == "verified"
    assert result.verified is True
    assert result.receipt == receipt_for(record)
    assert ledger.get(effect_id).state == "verified"
    assert cadence.snapshot()["effect_terminals"][effect_id] == "verified"
    assert cadence.snapshot()["verified_visible_effects"] == []
    assert cadence.visible_contact_calls == 0
    assert sink.wakes == 1


def test_wake_replay_is_idempotent_across_new_engine(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path)
    effect_id, record = pending_wake(engine, cadence, ledger)
    receipt = receipt_for(record)
    first = engine.reconcile_heartbeat_wake(effect_id, receipt)
    ledger_rows_after_first = ledger.ledger.path.read_text(
        encoding="utf-8"
    ).splitlines()

    restarted, restarted_cadence, restarted_ledger, sink = make_engine(tmp_path)
    replay = restarted.reconcile_heartbeat_wake(effect_id, receipt)

    assert replay.to_dict() == first.to_dict()
    assert restarted_ledger.get(effect_id).state == "verified"
    assert restarted_cadence.snapshot()["effect_terminals"][effect_id] == "verified"
    assert restarted_cadence.snapshot()["verified_visible_effects"] == []
    assert (
        ledger.ledger.path.read_text(encoding="utf-8").splitlines()
        == ledger_rows_after_first
    )
    assert sink.wakes == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "other-source"),
        ("content_sha256", "0" * 64),
        ("content_length", 1),
        ("epoch_id", "other-epoch"),
    ],
)
def test_wake_receipt_requires_exact_effect_identity(tmp_path, field, value):
    engine, cadence, ledger, _sink = make_engine(tmp_path)
    effect_id, record = pending_wake(engine, cadence, ledger)

    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_wake(
            effect_id, receipt_for(record, **{field: value})
        )

    assert ledger.get(effect_id).state == "executed_unverified"
    assert cadence.snapshot()["effect_terminals"][effect_id] == "executed_unverified"


@pytest.mark.parametrize("offset", (-timedelta(microseconds=1), timedelta(hours=1)))
def test_wake_receipt_must_be_observed_during_effect_lifetime(tmp_path, offset):
    engine, cadence, ledger, _sink = make_engine(tmp_path)
    effect_id, record = pending_wake(engine, cadence, ledger)

    with pytest.raises(ValueError, match="outside the effect lifetime"):
        engine.reconcile_heartbeat_wake(
            effect_id,
            receipt_for(record, observed_at=record.created_at + offset),
        )

    assert ledger.get(effect_id).state == "executed_unverified"
    assert cadence.snapshot()["effect_terminals"][effect_id] == "executed_unverified"


def test_wake_reconciliation_rejects_wrong_kind_and_conflicting_state(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path)
    wake_id, wake_record = pending_wake(engine, cadence, ledger)
    source = "delivery-source"
    delivery = HeartbeatEngine(
        bus=EventBus(tmp_path / "delivery", clock=lambda: NOW),
        controls=ControlStore(tmp_path / "delivery", clock=lambda: NOW),
        cadence=HeartbeatCadence(tmp_path / "delivery", clock=lambda: NOW),
        judge=DeliveryJudge(),
        sink=QueueSink(),
        effect_ledger=EffectLedger(tmp_path / "delivery", clock=lambda: NOW),
    )
    # The engine accepts a Judge-like object; this call only creates a direct
    # delivery effect so the wake seam can prove its kind gate.
    delivery_result = delivery.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["synthetic-event"], "due": True, "source_event_id": source},
        )
    )
    assert delivery_result.delivery is not None
    delivery_id = delivery.cadence.effect_ref(source, "heartbeat_delivery")
    assert delivery_id is not None
    delivery_record = delivery.effect_ledger.get(delivery_id)
    assert delivery_record is not None
    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_wake(delivery_id, receipt_for(delivery_record))

    receipt = receipt_for(wake_record)
    engine.reconcile_heartbeat_wake(wake_id, receipt)
    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_wake(
            wake_id, receipt_for(wake_record, receipt_id="other-receipt")
        )


def test_runtime_facade_forwards_wake_reconciliation(tmp_path):
    runtime = MoonbiteRuntime({"state": {"directory": str(tmp_path / "runtime")}})
    engine, cadence, ledger, _sink = make_engine(tmp_path / "engine")
    runtime.heartbeat = engine
    effect_id, record = pending_wake(engine, cadence, ledger, "runtime-source")

    result = runtime.reconcile_heartbeat_wake(effect_id, receipt_for(record))

    assert result.verified is True
    assert cadence.snapshot()["verified_visible_effects"] == []
