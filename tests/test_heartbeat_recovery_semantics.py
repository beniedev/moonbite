from __future__ import annotations

import argparse
from copy import deepcopy
import json
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import (
    EffectResult,
    HEARTBEAT_EFFECT_PLAN_SCHEMA,
    HeartbeatCadence,
    HeartbeatCandidate,
    HeartbeatEngine,
    JudgeDecision,
)
from moonbite_plugin.components import RuntimeComponents
from moonbite_plugin.conversation import ConversationBridge
from moonbite_plugin.memory import MemoryStore
from moonbite_plugin.panel import PanelStore
from moonbite_plugin.plugin import register
from moonbite_plugin.runtime_core import EventBus, FileRuntimeLocks, StateError
from moonbite_plugin.session import SessionLifecycleStore


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class FixedJudge:
    def __init__(self, decision: JudgeDecision):
        self.decision = decision
        self.calls = 0

    def decide(self, _candidate: HeartbeatCandidate) -> JudgeDecision:
        self.calls += 1
        return self.decision


class ReceiptWakeSink:
    @staticmethod
    def _receipt(intent, prefix: str) -> EffectReceipt:
        return EffectReceipt(
            receipt_id=f"{prefix}-{intent.effect_id}",
            event_id=intent.source_event_id,
            observed_at=intent.created_at,
            content_sha256=intent.content_sha256,
            content_length=intent.content_length,
            epoch_id=intent.epoch_id,
        )

    def deliver(self, _candidate, _decision, intent=None):
        return self._receipt(intent, "delivery")

    def wake(self, _candidate, _decision, intent=None):
        return self._receipt(intent, "wake")


class CountingReceiptWakeSink(ReceiptWakeSink):
    def __init__(self):
        self.calls: list[str] = []

    def deliver(self, candidate, decision, intent=None):
        self.calls.append("deliver")
        return super().deliver(candidate, decision, intent)

    def wake(self, candidate, decision, intent=None):
        self.calls.append("wake")
        return super().wake(candidate, decision, intent)


class IntentInterrupted(BaseException):
    """Synthetic crash raised at the narrow durable-intent seam."""


class InterruptingWakeSink(ReceiptWakeSink):
    def wake(self, _candidate, _decision, _intent=None):
        raise IntentInterrupted("synthetic wake interruption")


class RaisingWakeSink:
    def wake(self, _candidate, _decision, _intent=None):
        raise RuntimeError("synthetic wake failure")


def rewrite_terminal_audit(audit_path: Path, occurrence_id: str, mutate) -> None:
    rows = []
    found = False
    for raw in audit_path.read_bytes().splitlines():
        row = json.loads(raw)
        payload = row.get("payload", {})
        if (
            payload.get("occurrence_id") == occurrence_id
            and payload.get("terminal") is not None
        ):
            mutate(payload)
            found = True
        rows.append(row)
    assert found
    audit_path.write_bytes(
        (
            "\n".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows
            )
            + "\n"
        ).encode("utf-8")
    )


def effect_result_snapshot(record, *, status: str) -> dict:
    if record.state == "verified":
        result = EffectResult(
            True,
            "verified",
            record.receipt,
            True,
            record.effect_id,
            "verified",
        )
    else:
        result = EffectResult(
            True,
            status,
            effect_id=record.effect_id,
            terminal=record.state,
        )
    return result.to_dict()


class InterruptingLedger(EffectLedger):
    def __init__(self, root: Path, *, fail_kind: str):
        super().__init__(root, clock=lambda: NOW)
        self.fail_kind = fail_kind
        self.interrupted = False
        self.begin_kinds: list[str | None] = []

    def begin_intent(self, *args, **kwargs):
        kind = kwargs.get("kind")
        self.begin_kinds.append(kind)
        if kind == self.fail_kind and not self.interrupted:
            self.interrupted = True
            raise IntentInterrupted("synthetic intent interruption")
        return super().begin_intent(*args, **kwargs)


def make_engine(tmp_path: Path, decision: JudgeDecision, sink=None):
    return HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=ControlStore(tmp_path, clock=lambda: NOW),
        cadence=HeartbeatCadence(tmp_path, clock=lambda: NOW),
        judge=FixedJudge(decision),
        sink=sink or ReceiptWakeSink(),
        effect_ledger=EffectLedger(tmp_path, clock=lambda: NOW),
    )


class PathlessCadence:
    @property
    def path(self):
        raise AssertionError("pathless test cadence must not expose a root")

    def blocked(self, _kind, **_kwargs):
        return False, "open"

    def clock(self):
        return NOW


class TestLocks:
    @contextmanager
    def try_exclusive(self, _name):
        yield True


def candidate(
    source: str = "recovery-source",
    *,
    epoch: str | None = None,
    **changes,
):
    context = {
        "events": ["synthetic-event"],
        "due": True,
        "source_event_id": source,
    }
    if epoch is not None:
        context["epoch_id"] = epoch
    context.update(changes)
    return HeartbeatCandidate("care_poke", context, candidate_id=source)


def test_reconcile_reuses_sync_terminal_audit_without_identity_conflict(tmp_path):
    engine = make_engine(tmp_path, JudgeDecision(True, True, "contact", "hello"))

    initial = engine.run(candidate())

    assert initial.status == "completed"
    assert initial.delivery is not None and initial.delivery.verified
    assert initial.wake is not None and initial.wake.verified
    audit_before = engine.bus.audit.path.read_bytes()
    replay = engine.reconcile_heartbeat_wake(
        initial.wake.effect_id,
        initial.wake.receipt,
    )

    assert replay.verified is True
    terminals = [
        event
        for event in engine.bus.read_audit()
        if event.payload.get("occurrence_id") == "recovery-source"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 1
    assert terminals[0].payload["effect_ids"] == sorted(
        [initial.delivery.effect_id, initial.wake.effect_id]
    )
    assert engine.bus.audit.path.read_bytes() == audit_before


def test_legacy_singleton_reconcile_stays_unclosed_without_plan(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    judge = FixedJudge(JudgeDecision(False, False, "silent"))
    engine = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=ControlStore(tmp_path, clock=lambda: NOW),
        cadence=PathlessCadence(),
        judge=judge,
        sink=ReceiptWakeSink(),
        locks=TestLocks(),
        effect_ledger=ledger,
    )
    intent = ledger.begin_intent(
        kind="heartbeat_delivery",
        source_event_id="legacy-singleton",
        idempotency_key="heartbeat:legacy-singleton:delivery:delegated",
        epoch_id="heartbeat",
        content_sha256="0" * 64,
        content_length=1,
        created_at=NOW,
        expires_at=NOW.replace(hour=20),
    )
    ledger.mark_pending(intent.effect_id)

    settled = engine.reconcile_heartbeat_delivery(
        intent.effect_id,
        "intentional_silence",
    )

    assert settled.status == "intentional_silence"
    assert ledger.get(intent.effect_id).state == "failed"
    assert [
        event
        for event in engine.bus.read_audit()
        if event.payload.get("terminal") is not None
    ] == []
    replay = engine.run(candidate("legacy-singleton"))
    assert (replay.status, replay.reason) == ("pending", "awaiting_effect_plan")
    assert judge.calls == 0


class QueueingWakeSink:
    def wake(self, _candidate, _decision, intent=None):
        assert intent is not None
        return EffectResult(True, "queued")


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
@pytest.mark.parametrize(
    "gate",
    ["control", "cadence", "cooldown", "contact", "active_chat", "activity"],
)
def test_pending_effect_reconciliation_precedes_every_admission_gate(
    tmp_path, epoch, gate
):
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, False, "wake"),
        sink=QueueingWakeSink(),
    )
    first = engine.run(candidate("pending-source", epoch=epoch))
    assert first.status == "pending"
    assert first.wake is not None

    if gate == "control":
        engine.controls.put(feature="heartbeat", mode="pause", source="operator")
    changes = {}
    if gate == "cadence":
        changes["due"] = False
    elif gate == "cooldown":
        changes["cooldown"] = True
    elif gate == "contact":
        changes["recent_private_inbound_at"] = NOW
    elif gate == "active_chat":
        changes["active_chat"] = True
    elif gate == "activity":
        changes["activity_busy"] = True
    blocked = engine.run(candidate("pending-source", epoch=epoch, **changes))

    assert blocked.status == "pending"
    assert blocked.reason == "awaiting_receipt"
    assert blocked.wake is not None
    assert blocked.wake.effect_id == first.wake.effect_id
    assert blocked.wake.status == "queued_unverified"
    assert engine.effect_ledger.get(first.wake.effect_id).state == (
        "executed_unverified"
    )


class QueueingDelegatedSink:
    def deliver(self, _candidate, _decision, intent=None):
        assert intent is not None
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision, intent=None):
        assert intent is not None
        return EffectResult(True, "queued")


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
def test_verified_effect_reconciliation_survives_control_gate(tmp_path, epoch):
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, False, "wake"),
        sink=QueueingWakeSink(),
    )
    first = engine.run(candidate("verified-before-gate", epoch=epoch))
    assert first.wake is not None
    engine.controls.put(feature="heartbeat", mode="pause", source="operator")

    pending = engine.run(candidate("verified-before-gate", epoch=epoch))
    assert pending.status == "pending"
    receipt = ReceiptWakeSink._receipt(
        engine.effect_ledger.get(first.wake.effect_id), "wake"
    )
    verified = engine.reconcile_heartbeat_wake(first.wake.effect_id, receipt)

    assert verified.verified is True
    assert engine.effect_ledger.get(first.wake.effect_id).state == "verified"
    replay = engine.run(candidate("verified-before-gate", epoch=epoch))
    assert replay.status == "completed"


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
def test_failed_effect_reconciliation_survives_control_gate(tmp_path, epoch):
    engine = make_engine(
        tmp_path,
        JudgeDecision(
            True,
            True,
            "contact",
            "hello",
            delivery_mode="delegated",
        ),
        sink=QueueingDelegatedSink(),
    )
    first = engine.run(candidate("failed-before-gate", epoch=epoch))
    assert first.delivery is not None and first.wake is not None
    engine.controls.put(feature="heartbeat", mode="pause", source="operator")

    failed = engine.reconcile_heartbeat_delivery(
        first.delivery.effect_id,
        "failed",
    )

    assert failed.status == "failed"
    assert failed.verified is False
    assert engine.effect_ledger.get(first.delivery.effect_id).state == "failed"
    replay = engine.run(candidate("failed-before-gate", epoch=epoch))
    assert replay.status == "failed"


def _terminal_audits(engine, source):
    return [
        event
        for event in engine.bus.read_audit()
        if event.payload.get("occurrence_id") == source
        and event.payload.get("terminal") is not None
    ]


def _settle_delivery_as_silence(engine, first):
    """Verify the wake, then fail the delivery durably without a projection
    pass, leaving the cadence marker stale."""
    ledger = engine.effect_ledger
    engine.reconcile_heartbeat_wake(
        first.wake.effect_id,
        ReceiptWakeSink._receipt(ledger.get(first.wake.effect_id), "wake"),
    )
    ledger.fail(first.delivery.effect_id, "intentional_silence", retryable=False)


def test_failed_marker_replays_durable_silence_as_silence(tmp_path):
    """A cadence "failed" marker left by an earlier projection must not turn a
    durable intentional_silence record into a failed/effect_error replay."""
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, True, "contact", "hello", delivery_mode="delegated"),
        sink=QueueingDelegatedSink(),
    )
    first = engine.run(candidate("marker-failed-silence"))
    assert first.status == "pending"
    _settle_delivery_as_silence(engine, first)
    engine.cadence.record_effect_terminal(first.delivery.effect_id, "failed")

    replay = engine.run(candidate("marker-failed-silence"))

    assert replay.status == "intentional_silence"
    assert replay.reason_code.value == "denied"
    audits = _terminal_audits(engine, "marker-failed-silence")
    assert len(audits) == 1
    assert audits[0].payload["terminal"] == "intentional_silence"
    assert audits[0].payload["delivery"]["terminal"] == "intentional_silence"


def test_stale_unverified_marker_replays_durable_silence_as_silence(tmp_path):
    """A stale executed_unverified marker must not expire or requeue a record
    the durable ledger already holds as failed."""
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, True, "contact", "hello", delivery_mode="delegated"),
        sink=QueueingDelegatedSink(),
    )
    first = engine.run(candidate("marker-stale-silence"))
    assert first.status == "pending"
    _settle_delivery_as_silence(engine, first)
    assert (
        engine.cadence.snapshot()["effect_terminals"][first.delivery.effect_id]
        == "executed_unverified"
    )

    replay = engine.run(candidate("marker-stale-silence"))

    assert replay.status == "intentional_silence"
    assert replay.reason_code.value == "denied"
    assert engine.effect_ledger.get(first.delivery.effect_id).state == "failed"
    audits = _terminal_audits(engine, "marker-stale-silence")
    assert len(audits) == 1
    assert audits[0].payload["terminal"] == "intentional_silence"


def test_failed_marker_replays_durable_failure_with_its_reason(tmp_path):
    """A durable failure with a real reason keeps failing on marker replay."""
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, True, "contact", "hello", delivery_mode="delegated"),
        sink=QueueingDelegatedSink(),
    )
    first = engine.run(candidate("marker-failed-error"))
    assert first.status == "pending"
    ledger = engine.effect_ledger
    engine.reconcile_heartbeat_wake(
        first.wake.effect_id,
        ReceiptWakeSink._receipt(ledger.get(first.wake.effect_id), "wake"),
    )
    ledger.fail(first.delivery.effect_id, "effect_error", retryable=False)
    engine.cadence.record_effect_terminal(first.delivery.effect_id, "failed")

    replay = engine.run(candidate("marker-failed-error"))

    assert replay.status == "failed"
    assert replay.reason_code.value == "effect_error"


def test_snapshot_marker_replay_projects_durable_silence(tmp_path):
    """Without an effect-plan row, cadence-marker replay must still project
    the durable record's own terminal."""
    engine = make_engine(
        tmp_path,
        JudgeDecision(True, False, "wake"),
        sink=QueueingDelegatedSink(),
    )
    first = engine.run(candidate("marker-snapshot-silence"))
    assert first.status == "pending"
    assert first.wake is not None
    engine.effect_ledger.fail(
        first.wake.effect_id, "intentional_silence", retryable=False
    )
    # A projection pass writes the canonical occurrence terminal.
    replay = engine.run(candidate("marker-snapshot-silence"))
    assert replay.status == "intentional_silence"
    # Lose the plan ledger; cadence refs and terminal markers remain.
    (tmp_path / "heartbeat_effect_plans.jsonl").unlink()
    engine.cadence.record_effect_terminal(first.wake.effect_id, "failed")

    existing = engine._candidate_existing_effects(
        candidate("marker-snapshot-silence"), NOW
    )

    assert existing is not None
    assert existing[0] is None
    assert existing[1] is not None
    assert existing[1].terminal == "intentional_silence"


def test_exception_effect_failure_status_replays_through_registered_cli(
    tmp_path, capsys
):
    source = "exception-failure-status"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        RaisingWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )

    assert first_code == 1
    assert first_output["status"] == "failed"
    assert first_output["wake"]["status"] == "wake_error:RuntimeError"
    assert first_output["wake"]["terminal"] == "failed"
    record = EffectLedger(tmp_path / "state", clock=lambda: NOW).get(
        first_output["wake"]["effect_id"]
    )
    assert record is not None
    assert record.reason == "adapter_error:RuntimeError"

    source_state = tmp_path / "state"
    recovered_root = tmp_path / "recovered"
    recovered_state = recovered_root / "state"
    recovered_state.mkdir(parents=True)
    for filename in ("effects.jsonl", "heartbeat_effect_plans.jsonl"):
        (recovered_state / filename).write_bytes((source_state / filename).read_bytes())
    recovery_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    recovery_sink = CountingReceiptWakeSink()
    recovery_context, _recovery_runtime = registered_runtime(
        recovered_root,
        EffectLedger(recovered_state, clock=lambda: NOW),
        recovery_judge,
        recovery_sink,
    )
    recovery_code, recovery_output = invoke_registered_heartbeat(
        recovery_context, payload, capsys
    )
    assert recovery_code == 1
    assert recovery_output["wake"]["status"] == "adapter_error:RuntimeError"
    assert recovery_judge.calls == 0
    assert recovery_sink.calls == []

    audit_path = tmp_path / "state" / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )

    assert (replay_code, replay_output["status"]) == (1, "failed")
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before

    recovered_audit_path = recovered_state / "audit.jsonl"
    recovered_audit_before = recovered_audit_path.read_bytes()
    recovered_replay_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    recovered_replay_sink = CountingReceiptWakeSink()
    recovered_replay_context, _recovered_replay_runtime = registered_runtime(
        recovered_root,
        EffectLedger(recovered_state, clock=lambda: NOW),
        recovered_replay_judge,
        recovered_replay_sink,
    )
    recovered_replay_code, recovered_replay_output = invoke_registered_heartbeat(
        recovered_replay_context, payload, capsys
    )
    assert (recovered_replay_code, recovered_replay_output["status"]) == (1, "failed")
    assert recovered_replay_judge.calls == 0
    assert recovered_replay_sink.calls == []
    assert recovered_audit_path.read_bytes() == recovered_audit_before


class EarlyDelegatedReceiptSink:
    def __init__(self):
        self.engine = None
        self.ready = threading.Event()
        self.release = threading.Event()
        self.intent = None
        self.reconcile_result = None

    def deliver(self, _candidate, _decision, intent=None):
        self.intent = intent
        self.ready.set()
        if not self.release.wait(timeout=5):
            raise AssertionError("delivery barrier was not released")
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision, intent=None):
        return ReceiptWakeSink._receipt(intent, "wake")


def test_closed_effect_set_blocks_early_receipt_until_second_intent_exists(tmp_path):
    sink = EarlyDelegatedReceiptSink()
    engine = make_engine(
        tmp_path,
        JudgeDecision(
            True,
            True,
            "contact",
            "hello",
            delivery_mode="delegated",
        ),
        sink=sink,
    )
    candidate_value = candidate("ordered-effects")
    outcome = []

    def run_engine():
        outcome.append(engine.run(candidate_value))

    worker = threading.Thread(target=run_engine)
    worker.start()
    try:
        assert sink.ready.wait(timeout=5)
        assert sink.intent is not None
        intents = engine.effect_ledger.records()
        assert {record.kind for record in intents} == {
            "heartbeat_delivery",
            "heartbeat_wake",
        }
        receipt = ReceiptWakeSink._receipt(sink.intent, "delivery")
        sink.reconcile_result = engine.reconcile_heartbeat_delivery(
            sink.intent.effect_id,
            "verified",
            receipt,
        )
        assert sink.reconcile_result.verified is True
        assert [
            event
            for event in engine.bus.read_audit()
            if event.payload.get("occurrence_id") == "ordered-effects"
            and event.payload.get("terminal") is not None
        ] == []
    finally:
        sink.release.set()
        worker.join(timeout=5)

    assert worker.is_alive() is False
    assert len(outcome) == 1
    result = outcome[0]
    assert result.status == "completed"
    assert result.delivery is not None and result.delivery.verified
    assert result.wake is not None and result.wake.verified
    terminals = [
        event
        for event in engine.bus.read_audit()
        if event.payload.get("occurrence_id") == "ordered-effects"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 1
    assert len(terminals[0].payload["effect_ids"]) == 2


class RegisteredHost:
    def __init__(self, config):
        self.config = config
        self.llm = object()
        self.cli = None
        self.commands = {}
        self.tools = {}
        self.hooks = {}
        self.auxiliary_tasks = {}

    def get_config(self, key, default=None):
        return self.config if key == "config" else default

    def register_cli_command(self, **kwargs):
        self.cli = kwargs

    def register_command(self, name, handler, description="", args_hint=""):
        self.commands[name] = {
            "handler": handler,
            "description": description,
            "args_hint": args_hint,
        }

    def register_tool(self, **kwargs):
        self.tools[kwargs["name"]] = kwargs

    def register_hook(self, name, handler):
        self.hooks[name] = handler

    def register_auxiliary_task(self, key, **kwargs):
        self.auxiliary_tasks[key] = kwargs


def registered_config(tmp_path: Path):
    return {
        "state": {"directory": str(tmp_path / "state")},
        "modules": {"heartbeat": True, "autonomy": False},
        "heartbeat": {
            "kinds": {
                "care_poke": {
                    "enabled": True,
                    "profile": "routine",
                    "judge": "required",
                    "host_only": False,
                    "bypass": [],
                }
            }
        },
    }


def injected_components(root: Path, effects: EffectLedger) -> RuntimeComponents:
    root.mkdir(parents=True, exist_ok=True)
    bus = EventBus(root, clock=lambda: NOW)
    controls = ControlStore(root, clock=lambda: NOW)
    cadence = HeartbeatCadence(root, clock=lambda: NOW)
    panel = PanelStore(root, bus=bus, timezone_name="UTC", clock=lambda: NOW)
    memory = MemoryStore(root, clock=lambda: NOW)
    session = SessionLifecycleStore(root)
    locks = FileRuntimeLocks(root)
    return RuntimeComponents.injected(
        "synthetic-host",
        bus=bus,
        controls=controls,
        cadence=cadence,
        panel=panel,
        memory=memory,
        session=session,
        effects=effects,
        locks=locks,
        state_root=root,
    )


def registered_runtime(
    tmp_path: Path,
    effects: EffectLedger,
    judge: FixedJudge,
    sink: ReceiptWakeSink,
):
    context = RegisteredHost(registered_config(tmp_path))
    components = injected_components(tmp_path / "state", effects)
    bridge = ConversationBridge(
        tmp_path / "state",
        session_store=components.session,
        effect_ledger=components.effects,
        clock=lambda: NOW,
    )
    runtime = register(
        context,
        components=components,
        heartbeat_judge=judge,
        wake_sink=sink,
        conversation_bridge=bridge,
    )
    return context, runtime


def invoke_registered_heartbeat(context, payload, capsys):
    parser = argparse.ArgumentParser()
    context.cli["setup_fn"](parser)
    args = parser.parse_args(
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(payload, ensure_ascii=False),
        ]
    )
    code = context.cli["handler_fn"](args)
    return code, json.loads(capsys.readouterr().out)


def test_registered_cli_reconcile_uses_same_heartbeat_terminal_identity(
    tmp_path, capsys
):
    context = RegisteredHost(registered_config(tmp_path))
    runtime = register(
        context,
        heartbeat_judge=FixedJudge(JudgeDecision(True, False, "contact", "hello")),
        wake_sink=ReceiptWakeSink(),
    )
    parser = argparse.ArgumentParser()
    context.cli["setup_fn"](parser)
    args = parser.parse_args(
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {
                    "events": ["registered-event"],
                    "due": True,
                    "source_event_id": "registered-source",
                }
            ),
        ]
    )

    code = context.cli["handler_fn"](args)
    output = json.loads(capsys.readouterr().out)

    assert code == 0
    assert output["status"] == "completed"
    replay = runtime.reconcile_heartbeat_wake(
        output["wake"]["effect_id"],
        EffectReceipt.from_dict(output["wake"]["receipt"]),
    )

    assert replay.verified is True
    terminals = [
        event
        for event in runtime.bus.read_audit()
        if event.payload.get("occurrence_id") == "registered-source"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 1
    assert len(terminals[0].payload["effect_ids"]) == 1


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
def test_registered_create_interruption_keeps_closed_plan_for_pending_recovery(
    tmp_path, capsys, epoch
):
    state_root = tmp_path / "state"
    fault_ledger = InterruptingLedger(state_root, fail_kind="heartbeat_wake")
    first_judge = FixedJudge(JudgeDecision(True, True, "contact", "hello"))
    first_sink = CountingReceiptWakeSink()
    context, runtime = registered_runtime(
        tmp_path, fault_ledger, first_judge, first_sink
    )
    assert runtime.heartbeat.effect_ledger is fault_ledger
    payload = {
        "events": ["registered-interruption"],
        "due": True,
        "active_chat": False,
        "source_event_id": "interrupted-source",
    }
    if epoch is not None:
        payload["epoch_id"] = epoch

    with pytest.raises(IntentInterrupted, match="synthetic intent interruption"):
        invoke_registered_heartbeat(context, payload, capsys)
    capsys.readouterr()

    plan_path = state_root / "heartbeat_effect_plans.jsonl"
    effects_path = state_root / "effects.jsonl"
    plan_rows = [
        json.loads(line)
        for line in plan_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(plan_rows) == 1
    assert plan_rows[0]["closed"] is True
    assert len(plan_rows[0]["effects"]) == 2
    records = fault_ledger.records()
    assert len(records) == 1
    assert records[0].kind == "heartbeat_delivery"
    assert records[0].state == "intent"
    assert first_sink.calls == []
    assert first_judge.calls == 1
    plan_before = plan_path.read_bytes()
    effects_before = effects_path.read_bytes()

    replay_judge = FixedJudge(JudgeDecision(True, True, "contact", "hello"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path, EffectLedger(state_root, clock=lambda: NOW), replay_judge, replay_sink
    )
    code, output = invoke_registered_heartbeat(replay_context, payload, capsys)

    assert code == 0
    assert output["status"] == "pending"
    assert output["reason"] == "awaiting_effect_intent"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert plan_path.read_bytes() == plan_before
    assert effects_path.read_bytes() == effects_before


def legacy_history_engine(
    tmp_path: Path, epoch: str | None, mutation: str | None = None
):
    source_root = tmp_path / "source"
    source_engine = make_engine(
        source_root,
        JudgeDecision(True, True, "contact", "hello"),
    )
    initial = source_engine.run(candidate("legacy-nested", epoch=epoch))
    assert initial.status == "completed"
    assert initial.delivery is not None and initial.delivery.verified
    assert initial.wake is not None and initial.wake.verified

    legacy_root = tmp_path / "legacy"
    legacy_root.mkdir(parents=True)
    (legacy_root / "effects.jsonl").write_bytes(
        (source_root / "effects.jsonl").read_bytes()
    )
    bus = EventBus(legacy_root, clock=lambda: NOW)
    payload = deepcopy(initial.to_dict())
    payload["occurrence_id"] = "legacy-nested"
    payload["terminal"] = "verified"
    if mutation == "source":
        payload["source_event_id"] = "wrong-source"
    elif mutation == "effect_ids":
        payload["effect_ids"] = ["unknown-effect"]
    elif mutation == "nested_receipt":
        payload["wake"]["receipt"]["receipt_id"] = "tampered-receipt"
    elif mutation == "status":
        payload["status"] = "failed"
    bus.record_audit(
        "heartbeat",
        status="completed",
        source="heartbeat",
        details=payload,
    )
    ledger = EffectLedger(legacy_root, clock=lambda: NOW)
    engine = HeartbeatEngine(
        bus=bus,
        controls=ControlStore(legacy_root, clock=lambda: NOW),
        cadence=HeartbeatCadence(legacy_root, clock=lambda: NOW),
        judge=FixedJudge(JudgeDecision(True, True, "contact", "hello")),
        sink=CountingReceiptWakeSink(),
        effect_ledger=ledger,
    )
    return initial, engine, bus.audit.path


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
def test_legacy_nested_canonical_replay_is_byte_stable(tmp_path, epoch):
    initial, engine, audit_path = legacy_history_engine(tmp_path, epoch)
    before = audit_path.read_bytes()

    replay = engine.run(candidate("legacy-nested", epoch=epoch))
    duplicate = engine.reconcile_heartbeat_wake(
        initial.wake.effect_id,
        initial.wake.receipt,
    )

    assert replay.status == "completed"
    assert replay.reason == "verified"
    assert duplicate.verified is True
    assert engine.judge.calls == 0
    assert engine.sink.calls == []
    assert audit_path.read_bytes() == before


@pytest.mark.parametrize("epoch", [None, "epoch-1"], ids=["legacy", "explicit"])
@pytest.mark.parametrize(
    "mutation",
    ["source", "effect_ids", "nested_receipt", "status"],
)
def test_legacy_nested_identity_mutations_fail_closed(tmp_path, epoch, mutation):
    _initial, engine, audit_path = legacy_history_engine(tmp_path, epoch, mutation)
    audit_before = audit_path.read_bytes()

    with pytest.raises(StateError):
        engine.run(candidate("legacy-nested", epoch=epoch))
    assert engine.judge.calls == 0
    assert engine.sink.calls == []
    assert audit_path.read_bytes() == audit_before


@pytest.mark.parametrize(
    ("first_epoch", "second_epoch"),
    [(None, "heartbeat"), ("heartbeat", None)],
    ids=["legacy-then-explicit", "explicit-then-legacy"],
)
def test_registered_heartbeat_epoch_identities_do_not_collide(
    tmp_path, capsys, first_epoch, second_epoch
):
    first_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    first_sink = CountingReceiptWakeSink()
    context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        first_judge,
        first_sink,
    )
    first_payload = {
        "events": ["epoch-collision"],
        "due": True,
        "active_chat": False,
        "source_event_id": "epoch-collision-source",
    }
    if first_epoch is not None:
        first_payload["epoch_id"] = first_epoch

    first_code, first_output = invoke_registered_heartbeat(
        context, first_payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "completed"
    assert first_judge.calls == 1
    assert first_sink.calls == ["wake"]

    second_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    second_sink = CountingReceiptWakeSink()
    second_context, second_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        second_judge,
        second_sink,
    )
    second_payload = dict(first_payload)
    if second_epoch is None:
        second_payload.pop("epoch_id", None)
    else:
        second_payload["epoch_id"] = second_epoch

    second_code, second_output = invoke_registered_heartbeat(
        second_context, second_payload, capsys
    )
    assert second_code == 0
    assert second_output["status"] == "completed"
    assert second_judge.calls == 1
    assert second_sink.calls == ["wake"]
    terminals = [
        event
        for event in second_runtime.bus.read_audit()
        if event.payload.get("occurrence_id") == "epoch-collision-source"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 2
    assert {event.payload.get("epoch_id") for event in terminals} == {
        first_epoch,
        second_epoch,
    }


def test_legacy_plan_explicit_epoch_replays_without_rewrite(tmp_path, capsys):
    payload = {
        "events": ["legacy-plan-epoch"],
        "due": True,
        "active_chat": False,
        "source_event_id": "legacy-plan-source",
        "epoch_id": "heartbeat",
    }
    first_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    first_sink = CountingReceiptWakeSink()
    context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        first_judge,
        first_sink,
    )
    code, output = invoke_registered_heartbeat(context, payload, capsys)
    assert code == 0
    assert output["status"] == "completed"

    plan_path = tmp_path / "state" / "heartbeat_effect_plans.jsonl"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["public_epoch_id"] == "heartbeat"
    plan.pop("public_epoch_id")
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n", encoding="utf-8")
    plan_before = plan_path.read_bytes()
    audit_path = tmp_path / "state" / "audit.jsonl"
    audit_before = audit_path.read_bytes()

    replay_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )

    assert replay_code == 0
    assert replay_output["status"] == "completed"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert plan_path.read_bytes() == plan_before
    assert audit_path.read_bytes() == audit_before


def test_legacy_plan_epoch_inference_uses_dynamic_matcher(monkeypatch):
    calls: list[str | None] = []
    legacy_plan = {
        "schema_version": HEARTBEAT_EFFECT_PLAN_SCHEMA,
        "candidate_id": "candidate-legacy-matcher",
        "source_event_id": "source-legacy-matcher",
        "epoch_id": "heartbeat",
        "closed": True,
        "effects": [
            {
                "effect_id": "effect-legacy-matcher",
                "kind": "heartbeat_wake",
                "source_event_id": "source-legacy-matcher",
                "epoch_id": "heartbeat",
                "idempotency_key": "host-custom-key",
                "content_sha256": "0" * 64,
                "content_length": 1,
            }
        ],
    }
    original = deepcopy(legacy_plan)

    def custom_matcher(effect, public_epoch_id):
        calls.append(public_epoch_id)
        return (
            effect["idempotency_key"] == "host-custom-key" and public_epoch_id is None
        )

    monkeypatch.setattr(
        HeartbeatEngine,
        "_plan_effect_key_matches",
        staticmethod(custom_matcher),
    )
    validated = HeartbeatEngine._validate_effect_plan(legacy_plan)

    assert validated["public_epoch_id"] is None
    assert validated["effects"] == legacy_plan["effects"]
    assert calls == [None, "heartbeat", None]
    assert legacy_plan == original

    def failing_matcher(_effect, public_epoch_id):
        calls.append(public_epoch_id)
        raise RuntimeError("synthetic dynamic matcher failure")

    calls.clear()
    monkeypatch.setattr(
        HeartbeatEngine,
        "_plan_effect_key_matches",
        staticmethod(failing_matcher),
    )
    with pytest.raises(RuntimeError, match="synthetic dynamic matcher failure"):
        HeartbeatEngine._validate_effect_plan(legacy_plan)
    assert calls == [None]
    assert legacy_plan == original


def test_plan_public_epoch_tamper_fails_closed(tmp_path, capsys):
    payload = {
        "events": ["tampered-plan-epoch"],
        "due": True,
        "active_chat": False,
        "source_event_id": "tampered-plan-source",
        "epoch_id": "heartbeat",
    }
    first_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        first_judge,
        QueueingWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "pending"

    plan_path = tmp_path / "state" / "heartbeat_effect_plans.jsonl"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["public_epoch_id"] = None
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n", encoding="utf-8")
    plan_before = plan_path.read_bytes()

    replay_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )

    assert replay_code == 1
    assert replay_output["status"] == "failed"
    assert replay_output["reason"] == "effect_replay_error"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert plan_path.read_bytes() == plan_before


def test_legacy_audit_replay_separates_public_epochs_and_rejects_cross_epoch_ids(
    tmp_path,
):
    source_root = tmp_path / "source"
    source_engine = make_engine(
        source_root,
        JudgeDecision(True, False, "wake"),
        sink=CountingReceiptWakeSink(),
    )
    legacy = source_engine.run(candidate("legacy-audit-epochs"))
    explicit = source_engine.run(candidate("legacy-audit-epochs", epoch="heartbeat"))
    assert legacy.status == "completed"
    assert explicit.status == "completed"

    legacy_root = tmp_path / "legacy"
    legacy_root.mkdir()
    for name in ("effects.jsonl", "audit.jsonl"):
        (legacy_root / name).write_bytes((source_root / name).read_bytes())
    audit_path = legacy_root / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    replay_sink = CountingReceiptWakeSink()
    replay_engine = make_engine(
        legacy_root,
        replay_judge,
        sink=replay_sink,
    )

    legacy_replay = replay_engine.run(candidate("legacy-audit-epochs"))
    explicit_replay = replay_engine.run(
        candidate("legacy-audit-epochs", epoch="heartbeat")
    )
    assert legacy_replay.status == "completed"
    assert explicit_replay.status == "completed"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before

    tampered_root = tmp_path / "tampered"
    tampered_root.mkdir()
    tampered_audit = []
    for line in audit_before.splitlines():
        row = json.loads(line)
        payload = row["payload"]
        if payload.get("epoch_id") == "heartbeat":
            legacy_ids = next(
                json.loads(item)["payload"]["effect_ids"]
                for item in audit_before.splitlines()
                if json.loads(item)["payload"].get("epoch_id") is None
            )
            payload["effect_ids"] = legacy_ids
        tampered_audit.append(json.dumps(row, sort_keys=True))
    (tampered_root / "effects.jsonl").write_bytes(
        (legacy_root / "effects.jsonl").read_bytes()
    )
    tampered_audit_path = tampered_root / "audit.jsonl"
    tampered_audit_path.write_text("\n".join(tampered_audit) + "\n", encoding="utf-8")
    tampered_before = tampered_audit_path.read_bytes()
    tampered_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    tampered_sink = CountingReceiptWakeSink()
    tampered_engine = make_engine(
        tampered_root,
        tampered_judge,
        sink=tampered_sink,
    )

    with pytest.raises(StateError):
        tampered_engine.run(candidate("legacy-audit-epochs", epoch="heartbeat"))
    assert tampered_judge.calls == 0
    assert tampered_sink.calls == []
    assert tampered_audit_path.read_bytes() == tampered_before


def test_verified_terminal_without_effect_file_fails_closed(tmp_path, capsys):
    payload = {
        "events": ["missing-effects-file"],
        "due": True,
        "active_chat": False,
        "source_event_id": "missing-effects-source",
    }
    source_root = tmp_path / "source"
    source_context, _source_runtime = registered_runtime(
        source_root,
        EffectLedger(source_root / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        CountingReceiptWakeSink(),
    )
    source_code, source_output = invoke_registered_heartbeat(
        source_context, payload, capsys
    )
    assert source_code == 0
    assert source_output["status"] == "completed"

    missing_root = tmp_path / "missing-effects"
    missing_state = missing_root / "state"
    missing_state.mkdir(parents=True)
    (missing_state / "audit.jsonl").write_bytes(
        (source_root / "state" / "audit.jsonl").read_bytes()
    )
    replay_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        missing_root,
        EffectLedger(missing_state, clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    audit_path = missing_state / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_verified_legacy_nested_terminal_without_matching_effect_fails_closed(
    tmp_path, capsys
):
    payload = {
        "events": ["empty-effects-ledger"],
        "due": True,
        "active_chat": False,
        "source_event_id": "empty-effects-source",
    }
    source_root = tmp_path / "source"
    source_context, _source_runtime = registered_runtime(
        source_root,
        EffectLedger(source_root / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        CountingReceiptWakeSink(),
    )
    source_code, source_output = invoke_registered_heartbeat(
        source_context, payload, capsys
    )
    assert source_code == 0
    assert source_output["status"] == "completed"

    empty_root = tmp_path / "empty-ledger"
    empty_state = empty_root / "state"
    empty_state.mkdir(parents=True)
    legacy_details = {
        "candidate_id": source_output["candidate_id"],
        "occurrence_id": source_output["candidate_id"],
        "terminal": "verified",
        "wake": deepcopy(source_output["wake"]),
    }
    EventBus(empty_state, clock=lambda: NOW).record_audit(
        "heartbeat",
        status="completed",
        source="heartbeat",
        details=legacy_details,
    )
    (empty_state / "effects.jsonl").write_bytes(b"")
    replay_judge = FixedJudge(JudgeDecision(True, False, "wake"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        empty_root,
        EffectLedger(empty_state, clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    audit_path = empty_state / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


@pytest.mark.parametrize(
    "epoch",
    [None, "heartbeat"],
    ids=["legacy", "literal-heartbeat"],
)
def test_legacy_terminal_effect_ids_must_cover_all_siblings(tmp_path, capsys, epoch):
    source = "partial-terminal-effects"
    payload = {
        "events": ["partial-terminal-effects"],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    if epoch is not None:
        payload["epoch_id"] = epoch

    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, True, "contact", "hello")),
        InterruptingWakeSink(),
    )
    with pytest.raises(IntentInterrupted, match="synthetic wake interruption"):
        invoke_registered_heartbeat(first_context, payload, capsys)
    capsys.readouterr()

    state_root = tmp_path / "state"
    ledger = EffectLedger(state_root, clock=lambda: NOW)
    records = {record.kind: record for record in ledger.records()}
    delivery = records["heartbeat_delivery"]
    wake = records["heartbeat_wake"]
    assert delivery.state == "verified"
    assert wake.state == "pending"

    plan_path = state_root / "heartbeat_effect_plans.jsonl"
    assert plan_path.exists()
    legacy_root = tmp_path / "legacy-history"
    legacy_state = legacy_root / "state"
    legacy_state.mkdir(parents=True)
    for name in ("effects.jsonl", "audit.jsonl"):
        source_path = state_root / name
        if source_path.exists():
            (legacy_state / name).write_bytes(source_path.read_bytes())

    details = {
        "candidate_id": source,
        "source_event_id": source,
        "decision": JudgeDecision(True, True, "contact", "hello").to_dict(),
        "delivery": effect_result_snapshot(delivery, status="verified"),
        "wake": effect_result_snapshot(wake, status="pending"),
        "effect_ids": [delivery.effect_id],
    }
    terminal_kwargs = {
        "occurrence_id": source,
        "terminal": "verified",
        "status": "completed",
        "source": "heartbeat",
        "details": details,
    }
    if epoch is not None:
        terminal_kwargs["epoch_id"] = epoch
    EventBus(legacy_state, clock=lambda: NOW).record_audit_terminal(
        "heartbeat", **terminal_kwargs
    )

    replay_judge = FixedJudge(JudgeDecision(True, True, "contact", "hello"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        legacy_root,
        EffectLedger(legacy_state, clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    audit_path = legacy_state / "audit.jsonl"
    audit_before = audit_path.read_bytes()

    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


@pytest.mark.parametrize(
    ("node", "field"),
    [
        ("delivery", "effect_id"),
        ("delivery", "receipt"),
        ("delivery", "missing_effect_id"),
        ("delivery", "non_mapping"),
        ("wake", "effect_id"),
        ("wake", "receipt"),
        ("wake", "missing_effect_id"),
        ("wake", "non_mapping"),
    ],
    ids=[
        "delivery-effect-id",
        "delivery-receipt",
        "delivery-missing-effect-id",
        "delivery-non-mapping",
        "wake-effect-id",
        "wake-receipt",
        "wake-missing-effect-id",
        "wake-non-mapping",
    ],
)
def test_terminal_top_effect_ids_do_not_override_nested_identity(
    tmp_path, capsys, node, field
):
    source = "nested-terminal-identity"
    payload = {
        "events": ["nested-terminal-identity"],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
        "epoch_id": "heartbeat",
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, True, "contact", "hello")),
        ReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "completed"

    audit_path = tmp_path / "state" / "audit.jsonl"
    audit_rows = [json.loads(line) for line in audit_path.read_bytes().splitlines()]
    terminal = next(
        row["payload"]
        for row in audit_rows
        if row["payload"].get("occurrence_id") == source
        and row["payload"].get("terminal") is not None
    )
    assert set(terminal["effect_ids"]) == {
        first_output["delivery"]["effect_id"],
        first_output["wake"]["effect_id"],
    }

    def mutate(payload):
        nested = payload[node]
        if field == "effect_id":
            nested["effect_id"] = "tampered-effect-id"
        elif field == "receipt":
            nested["receipt"]["receipt_id"] = "tampered-receipt"
        elif field == "missing_effect_id":
            nested["effect_id"] = None
        else:
            payload[node] = ["invalid-nested-effect"]

    rewrite_terminal_audit(audit_path, source, mutate)
    audit_before = audit_path.read_bytes()

    replay_judge = FixedJudge(JudgeDecision(True, True, "contact", "hello"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_terminal_nested_effect_roles_cannot_swap(tmp_path, capsys):
    source = "nested-terminal-roles"
    payload = {
        "events": ["nested-terminal-roles"],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
        "epoch_id": "heartbeat",
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, True, "contact", "hello")),
        ReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "completed"

    audit_path = tmp_path / "state" / "audit.jsonl"

    def mutate(payload):
        delivery = deepcopy(payload["delivery"])
        payload["delivery"] = deepcopy(payload["wake"])
        payload["wake"] = delivery

    rewrite_terminal_audit(audit_path, source, mutate)
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(JudgeDecision(True, True, "contact", "hello"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


@pytest.mark.parametrize(
    ("decision", "expected_status", "expected_terminal"),
    [
        (
            JudgeDecision(
                False,
                False,
                "free-allowed-reason",
                allow_autonomy=True,
            ),
            "allowed",
            "allowed",
        ),
        (JudgeDecision(False, False, "free-skipped-reason"), "skipped", "denied"),
        (None, "failed", "failed"),
    ],
    ids=["allowed", "skipped", "failed"],
)
def test_effectless_canonical_terminals_replay_without_effects(
    tmp_path, capsys, decision, expected_status, expected_terminal
):
    source = f"effectless-{expected_status}"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    first_judge = FixedJudge(decision)
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        first_judge,
        CountingReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == (1 if expected_status == "failed" else 0)
    assert first_output["status"] == expected_status
    assert first_output.get("delivery") is None
    assert first_output.get("wake") is None

    audit_path = tmp_path / "state" / "audit.jsonl"
    terminal = next(
        row["payload"]
        for row in (json.loads(line) for line in audit_path.read_bytes().splitlines())
        if row["payload"].get("occurrence_id") == source
        and row["payload"].get("terminal") is not None
    )
    assert terminal["status"] == expected_status
    assert terminal["terminal"] == expected_terminal
    assert "effect_ids" not in terminal

    replay_judge = FixedJudge(decision)
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    audit_before = audit_path.read_bytes()
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )
    assert replay_code == (1 if expected_status == "failed" else 0)
    assert replay_output["status"] == expected_status
    assert replay_output["reason"] == expected_terminal
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_effectless_completed_legacy_terminal_remains_replayable(tmp_path, capsys):
    source = "effectless-completed"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(
            JudgeDecision(False, False, "free-allowed-reason", allow_autonomy=True)
        ),
        CountingReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "allowed"

    audit_path = tmp_path / "state" / "audit.jsonl"
    rewrite_terminal_audit(
        audit_path,
        source,
        lambda row: row.update(status="completed", terminal="completed"),
    )
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(
        JudgeDecision(False, False, "unused", allow_autonomy=True)
    )
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )
    assert replay_code == 0
    assert replay_output["status"] == "completed"
    assert replay_output["reason"] == "completed"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


@pytest.mark.parametrize(
    ("mutated_status", "mutated_terminal"),
    [
        ("completed", "failed"),
        ("completed", "intentional_silence"),
        ("skipped", "completed"),
        ("allowed", "failed"),
    ],
    ids=[
        "completed-failed",
        "completed-intentional-silence",
        "skipped-completed",
        "allowed-failed",
    ],
)
def test_effectless_status_terminal_conflicts_fail_closed(
    tmp_path, capsys, mutated_status, mutated_terminal
):
    source = "effectless-conflict"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(
            JudgeDecision(False, False, "free-allowed-reason", allow_autonomy=True)
        ),
        CountingReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "allowed"

    audit_path = tmp_path / "state" / "audit.jsonl"
    rewrite_terminal_audit(
        audit_path,
        source,
        lambda row: row.update(
            status=mutated_status,
            terminal=mutated_terminal,
        ),
    )
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(
        JudgeDecision(False, False, "unused", allow_autonomy=True)
    )
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_effectless_declared_effect_without_durable_evidence_fails_closed(
    tmp_path, capsys
):
    source = "effectless-declared-effect"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    first_context, _runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(
            JudgeDecision(False, False, "free-allowed-reason", allow_autonomy=True)
        ),
        CountingReceiptWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "allowed"

    audit_path = tmp_path / "state" / "audit.jsonl"
    rewrite_terminal_audit(
        audit_path,
        source,
        lambda row: row.update(
            status="completed",
            terminal="failed",
            effect_ids=["missing-effect-evidence"],
        ),
    )
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(
        JudgeDecision(False, False, "unused", allow_autonomy=True)
    )
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_nested_effect_without_ledger_is_not_effectless_completion(tmp_path, capsys):
    source = "nested-only-no-ledger"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
    }
    source_root = tmp_path / "source"
    source_context, _source_runtime = registered_runtime(
        source_root,
        EffectLedger(source_root / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        ReceiptWakeSink(),
    )
    source_code, source_output = invoke_registered_heartbeat(
        source_context, payload, capsys
    )
    assert source_code == 0
    assert source_output["status"] == "completed"

    nested_root = tmp_path / "nested-only"
    nested_state = nested_root / "state"
    nested_state.mkdir(parents=True)
    source_audit = source_root / "state" / "audit.jsonl"
    (nested_state / "audit.jsonl").write_bytes(source_audit.read_bytes())
    rewrite_terminal_audit(
        nested_state / "audit.jsonl",
        source,
        lambda row: (
            row.update(terminal="completed"),
            row.pop("effect_ids", None),
            row.pop("effect_id", None),
        ),
    )
    audit_path = nested_state / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        nested_root,
        EffectLedger(nested_state, clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_persisted_plan_without_ledger_is_not_effectless_completion(tmp_path, capsys):
    source = "plan-only-no-ledger"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
        "epoch_id": "heartbeat",
    }
    source_root = tmp_path / "source"
    source_context, _source_runtime = registered_runtime(
        source_root,
        EffectLedger(source_root / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        ReceiptWakeSink(),
    )
    source_code, source_output = invoke_registered_heartbeat(
        source_context, payload, capsys
    )
    assert source_code == 0
    assert source_output["status"] == "completed"

    plan_root = tmp_path / "plan-only"
    plan_state = plan_root / "state"
    plan_state.mkdir(parents=True)
    source_state = source_root / "state"
    (plan_state / "heartbeat_effect_plans.jsonl").write_bytes(
        (source_state / "heartbeat_effect_plans.jsonl").read_bytes()
    )
    (plan_state / "audit.jsonl").write_bytes(
        (source_state / "audit.jsonl").read_bytes()
    )
    rewrite_terminal_audit(
        plan_state / "audit.jsonl",
        source,
        lambda row: (
            row.update(terminal="completed"),
            row.pop("effect_ids", None),
            row.pop("effect_id", None),
            row.pop("delivery", None),
            row.pop("wake", None),
        ),
    )
    audit_path = plan_state / "audit.jsonl"
    audit_before = audit_path.read_bytes()
    replay_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        plan_root,
        EffectLedger(plan_state, clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    with pytest.raises(StateError):
        invoke_registered_heartbeat(replay_context, payload, capsys)
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
    assert audit_path.read_bytes() == audit_before


def test_literal_heartbeat_pending_receipt_replay_keeps_identity(tmp_path, capsys):
    source = "literal-heartbeat-pending"
    payload = {
        "events": [source],
        "due": True,
        "active_chat": False,
        "source_event_id": source,
        "epoch_id": "heartbeat",
    }
    first_context, runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        FixedJudge(JudgeDecision(True, False, "wake")),
        QueueingWakeSink(),
    )
    first_code, first_output = invoke_registered_heartbeat(
        first_context, payload, capsys
    )
    assert first_code == 0
    assert first_output["status"] == "pending"
    wake_id = first_output["wake"]["effect_id"]
    record = runtime.effect_ledger.get(wake_id)
    assert record is not None
    receipt = ReceiptWakeSink._receipt(record.to_intent(), "wake")
    settled = runtime.reconcile_heartbeat_wake(wake_id, receipt)
    assert settled.verified is True

    replay_judge = FixedJudge(JudgeDecision(True, False, "unused"))
    replay_sink = CountingReceiptWakeSink()
    replay_context, _replay_runtime = registered_runtime(
        tmp_path,
        EffectLedger(tmp_path / "state", clock=lambda: NOW),
        replay_judge,
        replay_sink,
    )
    replay_code, replay_output = invoke_registered_heartbeat(
        replay_context, payload, capsys
    )
    assert replay_code == 0
    assert replay_output["status"] == "completed"
    assert replay_output["reason"] == "verified"
    assert replay_judge.calls == 0
    assert replay_sink.calls == []
