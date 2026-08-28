from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest

from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import (
    EffectResult,
    HeartbeatCadence,
    HeartbeatCandidate,
    HeartbeatEngine,
    JudgeDecision,
    _effect_body,
)
from moonbite_plugin.runtime_core import EventBus
from moonbite_plugin.service import MoonbiteRuntime


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class Judge:
    def __init__(self, decision):
        self.decision = decision

    def decide(self, _candidate):
        return self.decision


class QueueSink:
    def __init__(self):
        self.deliveries = 0
        self.wakes = 0

    def deliver(self, _candidate, _decision):
        self.deliveries += 1
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision):
        self.wakes += 1
        return EffectResult(True, "queued")


def make_engine(tmp_path, decision, sink=None):
    sink = QueueSink() if sink is None else sink
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    engine = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=ControlStore(tmp_path, clock=lambda: NOW),
        cadence=cadence,
        judge=Judge(decision),
        sink=sink,
        effect_ledger=ledger,
    )
    return engine, cadence, ledger, sink


def delegated_decision(instruction="host instruction"):
    return JudgeDecision(
        True,
        True,
        "delegate",
        instruction,
        delivery_mode="delegated",
    )


def pending_delivery(engine, cadence, source="source-1"):
    result = engine.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["heartbeat-event"], "due": True, "source_event_id": source},
        )
    )
    effect_id = cadence.effect_ref(source, "heartbeat_delivery")
    assert effect_id is not None
    return result, effect_id


def receipt_for(record, receipt_id="delivery-receipt"):
    return EffectReceipt(
        receipt_id=receipt_id,
        event_id=record.source_event_id,
        observed_at=NOW,
        content_sha256=record.content_sha256,
        content_length=record.content_length,
        epoch_id=record.epoch_id,
    )


def test_delivery_mode_is_strict_and_mapping_round_trips():
    direct = JudgeDecision(True, True, "reason", "message")
    assert direct.delivery_mode == "direct"
    assert direct.to_dict()["delivery_mode"] == "direct"
    delegated = JudgeDecision(
        True, True, "reason", "instruction", delivery_mode="delegated"
    )
    assert delegated.to_dict()["delivery_mode"] == "delegated"
    with pytest.raises(ValueError):
        JudgeDecision(False, True, "reason", "instruction", delivery_mode="delegated")
    with pytest.raises(ValueError):
        JudgeDecision(True, True, "reason", "", delivery_mode="delegated")
    with pytest.raises(ValueError):
        JudgeDecision(True, False, "reason", delivery_mode="unknown")


def test_delegated_obligation_is_stable_and_content_free(tmp_path):
    decision = delegated_decision("private host instruction")
    candidate = HeartbeatCandidate("care_poke", {}, "candidate-1")
    body = _effect_body("delivery", candidate, decision)
    decoded = json.loads(body)
    assert set(decoded) == {
        "schema",
        "mode",
        "candidate_id",
        "kind",
        "instruction_sha256",
        "instruction_length",
    }
    assert decoded["mode"] == "delegated"
    assert "private host instruction" not in body.decode()
    assert (
        decoded["instruction_sha256"]
        == hashlib.sha256(decision.message.encode()).hexdigest()
    )
    assert body == _effect_body("delivery", candidate, decision)

    engine, cadence, ledger, sink = make_engine(tmp_path, decision)
    result, effect_id = pending_delivery(engine, cadence)
    record = ledger.get(effect_id)
    assert record is not None
    assert record.state == "executed_unverified"
    assert record.idempotency_key.endswith(":delegated")
    assert sink.deliveries == 1
    assert result.delivery is not None
    assert result.delivery.verified is False
    assert cadence.snapshot()["verified_visible_effects"] == []
    assert "private host instruction" not in "\n".join(
        path.read_text(encoding="utf-8")
        for path in tmp_path.rglob("*")
        if path.is_file()
    )


def test_verified_host_receipt_is_the_only_visible_projection(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path, delegated_decision())
    result, effect_id = pending_delivery(engine, cadence)
    assert result.delivery is not None and not result.delivery.verified
    record = ledger.get(effect_id)
    assert record is not None
    verified = engine.reconcile_heartbeat_delivery(
        effect_id, "verified", receipt_for(record)
    )
    assert verified.status == "verified"
    assert verified.verified is True
    assert cadence.snapshot()["verified_visible_effects"] == [effect_id]
    replay = engine.reconcile_heartbeat_delivery(
        effect_id, terminal="verified", receipt=receipt_for(record, "delivery-receipt")
    )
    assert replay.status == "verified"


def test_intentional_silence_is_durable_without_contact_or_receipt(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path, delegated_decision())
    _result, effect_id = pending_delivery(engine, cadence, "silence-source")
    settled = engine.reconcile_heartbeat_delivery(effect_id, "intentional_silence")
    assert settled.status == "intentional_silence"
    assert settled.terminal == "intentional_silence"
    assert settled.receipt is None
    record = ledger.get(effect_id)
    assert record is not None
    assert (record.state, record.reason, record.retryable) == (
        "failed",
        "intentional_silence",
        False,
    )
    assert cadence.snapshot()["verified_visible_effects"] == []
    replay = engine.reconcile_heartbeat_delivery(effect_id, "intentional_silence")
    assert replay.status == "intentional_silence"


def test_unknown_keeps_unverified_and_failed_is_fixed_terminal(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path, delegated_decision())
    _result, unknown_id = pending_delivery(engine, cadence, "unknown-source")
    unknown = engine.reconcile_heartbeat_delivery(unknown_id, "unknown")
    assert unknown.status == "unknown"
    assert unknown.terminal == "executed_unverified"
    assert ledger.get(unknown_id).state == "executed_unverified"
    assert cadence.snapshot()["verified_visible_effects"] == []

    failed_engine, failed_cadence, failed_ledger, _failed_sink = make_engine(
        tmp_path / "failed", delegated_decision()
    )
    _result, failed_id = pending_delivery(
        failed_engine, failed_cadence, "failed-source"
    )
    failed = failed_engine.reconcile_heartbeat_delivery(failed_id, "failed")
    assert failed.status == "failed"
    assert failed.terminal == "failed"
    assert failed.receipt is None
    failed_record = failed_ledger.get(failed_id)
    assert failed_record is not None
    assert failed_record.reason == "delegated_delivery_failed"
    assert failed_record.retryable is False
    assert (
        failed_engine.reconcile_heartbeat_delivery(failed_id, "failed").status
        == "failed"
    )


def test_reconciliation_rejects_wrong_kind_state_and_receipt(tmp_path):
    engine, cadence, ledger, _sink = make_engine(tmp_path, delegated_decision())
    _result, delegated_id = pending_delivery(engine, cadence, "mismatch-source")
    record = ledger.get(delegated_id)
    assert record is not None
    wrong = EffectReceipt(
        receipt_id="wrong",
        event_id=record.source_event_id,
        observed_at=NOW,
        content_sha256="0" * 64,
        content_length=record.content_length,
        epoch_id=record.epoch_id,
    )
    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_delivery(delegated_id, "verified", wrong)
    assert ledger.get(delegated_id).state == "executed_unverified"
    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_delivery(delegated_id, "intentional_silence", wrong)

    direct_engine, direct_cadence, _direct_ledger, _direct_sink = make_engine(
        tmp_path / "direct", JudgeDecision(True, True, "direct", "message")
    )
    _result, direct_id = pending_delivery(
        direct_engine, direct_cadence, "direct-source"
    )
    with pytest.raises(ValueError):
        direct_engine.reconcile_heartbeat_delivery(direct_id, "unknown")
    with pytest.raises(ValueError):
        engine.reconcile_heartbeat_delivery("missing-effect", "unknown")


def test_runtime_facade_forwards_host_reconciliation(tmp_path):
    runtime = MoonbiteRuntime({"state": {"directory": str(tmp_path)}})
    engine, cadence, ledger, _sink = make_engine(
        tmp_path / "engine", delegated_decision()
    )
    runtime.heartbeat = engine
    _result, effect_id = pending_delivery(engine, cadence, "runtime-source")
    record = ledger.get(effect_id)
    assert record is not None
    result = runtime.reconcile_heartbeat_delivery(
        effect_id, "verified", receipt_for(record)
    )
    assert result.verified is True
