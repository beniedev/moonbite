from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import validate as validate_jsonschema

from moonbite_plugin.autonomy import ActivityProvider, AutonomyDecision
from moonbite_plugin.components import RuntimeComponents
from moonbite_plugin.conversation import ConversationBridge
from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import (
    EffectResult,
    HeartbeatCadence,
    JudgeDecision,
)
from moonbite_plugin.hermes_adapter import (
    HermesAutonomyJudge,
)
from moonbite_plugin.memory import MemoryStore
from moonbite_plugin.panel import PanelStore
from moonbite_plugin.plugin import register
from moonbite_plugin.runtime_core import EventBus, FileRuntimeLocks
from moonbite_plugin.session import SessionLifecycleStore


class HostContext:
    """Small public host registry used by the real plugin registration path."""

    def __init__(self, config):
        self.config = config
        self.scenario_pack = None
        self.llm = object()
        self.cli = None
        self.commands = {}
        self.tools = {}
        self.hooks = {}
        self.auxiliary_tasks = {}

    def get_config(self, key, default=None):
        if key == "config":
            return self.config
        if key == "scenario_pack":
            return self.scenario_pack
        return default

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


def _config(tmp_path: Path, *, heartbeat=False, autonomy=False):
    config = {
        "state": {"directory": str(tmp_path / "state")},
        "modules": {"heartbeat": heartbeat, "autonomy": autonomy},
    }
    if heartbeat:
        config["heartbeat"] = {
            "kinds": {
                "care_poke": {
                    "enabled": True,
                    "profile": "routine",
                    "judge": "required",
                    "host_only": False,
                    "bypass": [],
                }
            }
        }
    if autonomy:
        config["autonomy"] = {"providers": {"fixture": {"enabled": True, "weight": 1}}}
    return config


def _routed_config(tmp_path: Path, *, heartbeat=False, autonomy=False):
    config = _config(tmp_path, heartbeat=heartbeat, autonomy=autonomy)
    config["model_routes"] = {
        "schema_version": "moon.model_route_bindings.v1",
        "main": {"alias": "moon_main"},
        "heartbeat": {"alias": "moon_support"},
        "hippocampus": {"alias": "moon_hippocampus"},
    }
    return config


def _cli(ctx: HostContext):
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    return parser


def _run_cli(ctx: HostContext, parser, argv, capsys):
    code = ctx.cli["handler_fn"](parser.parse_args(argv))
    output = json.loads(capsys.readouterr().out)
    return code, output


class FixedHeartbeatJudge:
    def __init__(self, decision: JudgeDecision):
        self.decision = decision

    def decide(self, _candidate):
        return self.decision


class ReceiptWakeSink:
    def deliver(self, _candidate, _decision, intent=None):
        return _receipt_for_intent(intent, prefix="delivery")

    def wake(self, _candidate, _decision, intent=None):
        return _receipt_for_intent(intent, prefix="wake")


class RejectingWakeSink:
    def deliver(self, _candidate, _decision, intent=None):
        return EffectResult(False, "rejected")

    def wake(self, _candidate, _decision, intent=None):
        return EffectResult(False, "rejected")


class QueueingWakeSink:
    def deliver(self, _candidate, _decision, intent=None):
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision, intent=None):
        return EffectResult(True, "queued")


def _receipt_for_intent(intent, *, prefix: str):
    assert intent is not None
    return EffectReceipt(
        receipt_id=f"{prefix}-{intent.source_event_id}",
        event_id=intent.source_event_id,
        observed_at=intent.created_at,
        content_sha256=intent.content_sha256,
        content_length=intent.content_length,
        epoch_id=intent.epoch_id,
    )


@pytest.mark.parametrize(
    ("decision", "sink_type", "expected_status", "expected_exit"),
    (
        (
            JudgeDecision(False, True, "contact", "hello"),
            RejectingWakeSink,
            "failed",
            1,
        ),
        (
            JudgeDecision(False, True, "contact", "hello"),
            ReceiptWakeSink,
            "completed",
            0,
        ),
        (
            JudgeDecision(False, False, "quiet"),
            ReceiptWakeSink,
            "skipped",
            0,
        ),
        (
            JudgeDecision(False, True, "contact", "hello"),
            QueueingWakeSink,
            "pending",
            0,
        ),
    ),
    ids=("effect-failed", "normal-text", "normal-skip", "async-pending"),
)
def test_registered_heartbeat_cli_uses_business_failure_only(
    tmp_path, capsys, decision, sink_type, expected_status, expected_exit
):
    ctx = HostContext(_config(tmp_path, heartbeat=True))
    register(
        ctx,
        heartbeat_judge=FixedHeartbeatJudge(decision),
        wake_sink=sink_type(),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {
                    "events": ["fixture-event"],
                    "due": True,
                    "source_event_id": expected_status,
                }
            ),
        ],
        capsys,
    )

    assert output["status"] == expected_status
    assert code == expected_exit


class DelegatingQueueSink:
    def __init__(self):
        self.deliveries = 0
        self.wakes = 0

    def deliver(self, _candidate, _decision, intent=None):
        assert intent is not None
        self.deliveries += 1
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision, intent=None):
        assert intent is not None
        self.wakes += 1
        return EffectResult(True, "queued")


def test_registered_heartbeat_cli_replays_formal_delegated_terminals(tmp_path, capsys):
    """Unknown remains pending; intentional silence becomes a terminal replay."""

    def registered(root):
        sink = DelegatingQueueSink()
        ctx = HostContext(_config(root, heartbeat=True))
        runtime = register(
            ctx,
            heartbeat_judge=FixedHeartbeatJudge(
                JudgeDecision(
                    True,
                    True,
                    "delegate",
                    "fixture instruction",
                    delivery_mode="delegated",
                )
            ),
            wake_sink=sink,
        )
        return ctx, runtime, _cli(ctx), sink

    ctx, runtime, parser, sink = registered(tmp_path)

    def invoke(source_event_id):
        return _run_cli(
            ctx,
            parser,
            [
                "heartbeat",
                "care_poke",
                "--context",
                json.dumps(
                    {
                        "events": ["fixture-event"],
                        "due": True,
                        "source_event_id": source_event_id,
                    }
                ),
            ],
            capsys,
        )

    unknown_code, unknown_output = invoke("formal-unknown")
    unknown_effect_id = unknown_output["delivery"]["effect_id"]
    unknown_reconciliation = runtime.reconcile_heartbeat_delivery(
        unknown_effect_id, "unknown"
    )
    replay_code, replay_output = invoke("formal-unknown")

    assert (unknown_reconciliation.status, unknown_reconciliation.terminal) == (
        "unknown",
        "executed_unverified",
    )
    assert (unknown_code, replay_code) == (0, 0)
    assert replay_output["status"] == "pending"
    assert replay_output["delivery"]["effect_id"] == unknown_effect_id
    assert sink.deliveries == sink.wakes == 1

    silence_ctx, silence_runtime, silence_parser, silence_sink = registered(
        tmp_path / "silence"
    )
    silence_code, silence_output = _run_cli(
        silence_ctx,
        silence_parser,
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {
                    "events": ["fixture-event"],
                    "due": True,
                    "source_event_id": "formal-silence",
                }
            ),
        ],
        capsys,
    )
    silence_delivery_id = silence_output["delivery"]["effect_id"]
    silence_wake_id = silence_output["wake"]["effect_id"]
    silence_reconciliation = silence_runtime.reconcile_heartbeat_delivery(
        silence_delivery_id, "intentional_silence"
    )
    wake_record = silence_runtime.effects.get(silence_wake_id)
    assert wake_record is not None
    wake_reconciliation = silence_runtime.reconcile_heartbeat_wake(
        silence_wake_id,
        _receipt_for_record(wake_record, receipt_id="formal-wake"),
    )
    terminal_code, terminal_output = _run_cli(
        silence_ctx,
        silence_parser,
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {
                    "events": ["fixture-event"],
                    "due": True,
                    "source_event_id": "formal-silence",
                }
            ),
        ],
        capsys,
    )

    assert (silence_reconciliation.status, silence_reconciliation.terminal) == (
        "intentional_silence",
        "intentional_silence",
    )
    assert (wake_reconciliation.status, wake_reconciliation.verified) == (
        "verified",
        True,
    )
    assert (silence_code, terminal_code) == (0, 0)
    assert terminal_output["status"] == "completed"
    assert terminal_output["reason"] == "intentional_silence"
    assert silence_sink.deliveries == silence_sink.wakes == 1


def _receipt_for_record(record, *, receipt_id):
    return EffectReceipt(
        receipt_id=receipt_id,
        event_id=record.source_event_id,
        observed_at=record.created_at,
        content_sha256=record.content_sha256,
        content_length=record.content_length,
        epoch_id=record.epoch_id,
    )


class FixedAutonomyJudge:
    def __init__(self, decision: AutonomyDecision):
        self.decision = decision

    def decide(self, _context):
        return self.decision


def verified_provider(request):
    return EffectReceipt(
        receipt_id=f"autonomy-{request.source_event_id}",
        event_id=request.source_event_id,
        observed_at=request.context.now,
        content_sha256=request.content_sha256,
        content_length=request.content_length,
        epoch_id=request.epoch_id,
    )


def pending_provider(_request):
    return "accepted-by-host-queue"


def failing_provider(_request):
    raise RuntimeError("synthetic provider failure")


@pytest.mark.parametrize(
    ("decision", "provider", "expected_status", "expected_exit"),
    (
        (AutonomyDecision(True, "run"), verified_provider, "completed", 0),
        (AutonomyDecision(True, "run"), pending_provider, "executed_unverified", 0),
        (AutonomyDecision(True, "run"), failing_provider, "failed", 1),
        (AutonomyDecision(False, "quiet"), verified_provider, "skipped", 0),
        (AutonomyDecision(False, "unknown"), verified_provider, "skipped", 0),
        (
            AutonomyDecision(False, "intentional_silence"),
            verified_provider,
            "skipped",
            0,
        ),
    ),
    ids=("completed", "pending", "failed", "skip", "unknown", "intentional-silence"),
)
def test_registered_autonomy_cli_uses_business_failure_only(
    tmp_path, capsys, decision, provider, expected_status, expected_exit
):
    ctx = HostContext(_config(tmp_path, autonomy=True))
    register(
        ctx,
        autonomy_judge=FixedAutonomyJudge(decision),
        activity_providers=(ActivityProvider("fixture", provider),),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        [
            "autonomy",
            "--facts",
            json.dumps({"source_event_id": expected_status}),
        ],
        capsys,
    )

    assert output["status"] == expected_status
    assert code == expected_exit


class DegradedAuditBus(EventBus):
    def record_audit(self, *args, **kwargs):
        raise OSError("synthetic audit projection failure")

    def record_audit_terminal(self, *args, **kwargs):
        raise OSError("synthetic terminal projection failure")


class ConflictAuditBus(EventBus):
    def __init__(self, root):
        super().__init__(root)
        self.inject_conflict = True

    def record_audit_terminal(
        self,
        action,
        *,
        occurrence_id,
        epoch_id=None,
        terminal,
        status,
        source,
        details=None,
    ):
        if self.inject_conflict:
            self.inject_conflict = False
            super().record_audit_terminal(
                action,
                occurrence_id=occurrence_id,
                epoch_id=epoch_id,
                terminal="failed" if terminal != "failed" else "verified",
                status="failed" if terminal != "failed" else "completed",
                source=source,
            )
        return super().record_audit_terminal(
            action,
            occurrence_id=occurrence_id,
            epoch_id=epoch_id,
            terminal=terminal,
            status=status,
            source=source,
            details=details,
        )


def _injected_components(tmp_path, bus_type):
    root = tmp_path / "components"
    root.mkdir()
    bus = bus_type(root)
    controls = ControlStore(root)
    cadence = HeartbeatCadence(root, timezone_name="UTC", anchor_hour=6)
    panel = PanelStore(root, bus=bus, timezone_name="UTC", anchor_hour=6)
    memory = MemoryStore(root)
    session = SessionLifecycleStore(root)
    effects = EffectLedger(root)
    locks = FileRuntimeLocks(root)
    return RuntimeComponents.injected(
        "cli-semantics-test",
        bus,
        controls,
        cadence,
        panel,
        memory,
        locks,
        session=session,
        effects=effects,
        state_root=root,
    )


def _degraded_components(tmp_path):
    return _injected_components(tmp_path, DegradedAuditBus)


def _conflict_components(tmp_path):
    return _injected_components(tmp_path, ConflictAuditBus)


def test_heartbeat_cli_ignores_degraded_side_channel_after_verified_effect(
    tmp_path, capsys
):
    ctx = HostContext(_config(tmp_path, heartbeat=True))
    components = _degraded_components(tmp_path)
    register(
        ctx,
        components=components,
        conversation_bridge=ConversationBridge(
            components.state_root,
            session_store=components.session,
            effect_ledger=components.effects,
        ),
        heartbeat_judge=FixedHeartbeatJudge(
            JudgeDecision(False, True, "contact", "hello")
        ),
        wake_sink=ReceiptWakeSink(),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {"events": ["fixture"], "due": True, "source_event_id": "degraded-hb"}
            ),
        ],
        capsys,
    )

    assert output["status"] == "partial"
    assert output["degraded"] is True
    assert output["delivery"]["verified"] is True
    assert code == 0


def test_heartbeat_cli_reports_real_terminal_conflict_as_failure(tmp_path, capsys):
    ctx = HostContext(_config(tmp_path, heartbeat=True))
    components = _conflict_components(tmp_path)
    register(
        ctx,
        components=components,
        conversation_bridge=ConversationBridge(
            components.state_root,
            session_store=components.session,
            effect_ledger=components.effects,
        ),
        heartbeat_judge=FixedHeartbeatJudge(
            JudgeDecision(False, True, "contact", "hello")
        ),
        wake_sink=ReceiptWakeSink(),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {"events": ["fixture"], "due": True, "source_event_id": "conflict"}
            ),
        ],
        capsys,
    )

    assert output["status"] == "failed"
    assert output["reason"] == "terminal_conflict"
    assert output["delivery"]["verified"] is True
    assert output["degraded"] is True
    assert code == 1


def test_autonomy_cli_ignores_degraded_side_channel_after_verified_effect(
    tmp_path, capsys
):
    ctx = HostContext(_config(tmp_path, autonomy=True))
    components = _degraded_components(tmp_path)
    register(
        ctx,
        components=components,
        conversation_bridge=ConversationBridge(
            components.state_root,
            session_store=components.session,
            effect_ledger=components.effects,
        ),
        autonomy_judge=FixedAutonomyJudge(AutonomyDecision(True, "run")),
        activity_providers=(ActivityProvider("fixture", verified_provider),),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps({"source_event_id": "degraded-auto"})],
        capsys,
    )

    assert output["status"] == "completed"
    assert output["audit_status"] == "degraded"
    assert output["audit_error"].startswith("audit_error:")
    assert code == 0


class StructuredLlm:
    def __init__(self, parsed):
        self.parsed = parsed
        self.calls = []

    def complete_structured(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(parsed=self.parsed)


@pytest.mark.parametrize(
    ("allowed", "reason", "expected_reason"),
    (
        (True, "a" * 128, "a" * 128),
        (False, "a" * 129, "a" * 128),
        (True, "a" * 127 + "é", "a" * 127),
        (False, "🙂" * 32 + "é", "🙂" * 32),
    ),
    ids=("ascii-128-true", "ascii-129-false", "utf8-cut-true", "emoji-cut-false"),
)
def test_autonomy_reason_is_utf8_bounded_at_real_adapter_and_cli_boundary(
    tmp_path, capsys, allowed, reason, expected_reason
):
    llm = StructuredLlm({"allowed": allowed, "reason": reason})
    ctx = HostContext(_routed_config(tmp_path, autonomy=True))
    ctx.llm = llm
    runtime = register(
        ctx,
        activity_providers=(ActivityProvider("fixture", verified_provider),),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps({"source_event_id": "reason-boundary"})],
        capsys,
    )

    assert isinstance(runtime.autonomy.judge, HermesAutonomyJudge)
    if allowed:
        assert output["status"] == "completed"
    else:
        assert output["status"] == "skipped"
        assert output["reason"] == expected_reason
    assert code == 0

    call = llm.calls[0]
    reason_schema = call["json_schema"]["properties"]["reason"]
    prompt = call["instructions"]
    validate_jsonschema({"allowed": allowed, "reason": reason}, call["json_schema"])
    assert "128" in reason_schema["description"]
    assert "UTF-8" in reason_schema["description"]
    assert "128" in prompt
    assert "UTF-8" in prompt


@pytest.mark.parametrize(
    "parsed",
    (
        {"allowed": 1, "reason": "valid text"},
        {"allowed": "false", "reason": "valid text"},
        {"allowed": None, "reason": "valid text"},
        {"allowed": True, "reason": ""},
        {"allowed": False, "reason": "   "},
    ),
    ids=(
        "numeric-allowed",
        "string-allowed",
        "null-allowed",
        "empty-reason",
        "whitespace-reason",
    ),
)
def test_autonomy_judge_invalid_fields_fail_closed_through_registered_cli(
    tmp_path, capsys, parsed
):
    llm = StructuredLlm(parsed)
    ctx = HostContext(_routed_config(tmp_path, autonomy=True))
    ctx.llm = llm
    runtime = register(
        ctx,
        activity_providers=(ActivityProvider("fixture", verified_provider),),
    )
    parser = _cli(ctx)

    code, output = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps({"source_event_id": "invalid-judge"})],
        capsys,
    )

    assert output["status"] == "failed"
    assert output["reason"].startswith("judge_error:")
    assert code == 1
    assert isinstance(runtime.autonomy.judge, HermesAutonomyJudge)
