from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from moonbite_plugin.components import RuntimeComponents
from moonbite_plugin.control import ControlStore
from moonbite_plugin.conversation import ConversationBridge
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import HeartbeatCadence, JudgeDecision
from moonbite_plugin.memory import MemoryStore
from moonbite_plugin.panel import PanelStore
from moonbite_plugin.plugin import register
from moonbite_plugin.runtime_core import EventBus, FileRuntimeLocks, JsonlLedger
from moonbite_plugin.session import SessionLifecycleStore


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)


class AnchorWriteCrash(BaseException):
    """Synthetic process crash after the cadence storage boundary."""


class CrashAfterAnchorWriteCadence(HeartbeatCadence):
    """Inject a crash after mark_judge's durable write completes."""

    def mark_judge(self, *args, **kwargs):
        selected = super().mark_judge(*args, **kwargs)
        if kwargs.get("anchor_epoch") is not None:
            raise AnchorWriteCrash("daily anchor storage boundary interrupted")
        return selected


class PlanWriteCrash(BaseException):
    """Synthetic process crash at the closed-plan storage boundary."""


class FailingPlanLedger(JsonlLedger):
    def get_or_append(self, value, *, matcher):
        del value, matcher
        raise PlanWriteCrash("closed effect plan storage interrupted")


class FixedJudge:
    def __init__(self, decision: JudgeDecision):
        self.decision = decision
        self.calls = 0

    def decide(self, _candidate):
        self.calls += 1
        return self.decision


class CountingWakeSink:
    def __init__(self):
        self.calls: list[str] = []

    def deliver(self, _candidate, _decision, _intent=None):
        self.calls.append("deliver")
        raise AssertionError("delivery was not part of this synthetic decision")

    def wake(self, _candidate, _decision, intent=None):
        self.calls.append("wake")
        return EffectReceipt(
            receipt_id=f"wake-{intent.effect_id}",
            event_id=intent.source_event_id,
            observed_at=intent.created_at,
            content_sha256=intent.content_sha256,
            content_length=intent.content_length,
            epoch_id=intent.epoch_id,
        )


class RegisteredHost:
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


def _config(state_root: Path) -> dict:
    return {
        "state": {"directory": str(state_root)},
        "modules": {"heartbeat": True, "autonomy": False},
        "heartbeat": {
            "kinds": {
                "daily_anchor": {
                    "enabled": True,
                    "profile": "daily_anchor",
                    "judge": "required",
                    "host_only": True,
                    "bypass": [],
                }
            }
        },
    }


def _components(
    state_root: Path, cadence: HeartbeatCadence, effects: EffectLedger
) -> RuntimeComponents:
    state_root.mkdir(parents=True, exist_ok=True)
    bus = EventBus(state_root, clock=lambda: NOW)
    return RuntimeComponents.injected(
        "synthetic-host",
        bus=bus,
        controls=ControlStore(state_root, clock=lambda: NOW),
        cadence=cadence,
        panel=PanelStore(state_root, bus=bus, timezone_name="UTC", clock=lambda: NOW),
        memory=MemoryStore(state_root, clock=lambda: NOW),
        session=SessionLifecycleStore(state_root),
        effects=effects,
        locks=FileRuntimeLocks(state_root),
        state_root=state_root,
    )


def _registered(
    state_root: Path,
    cadence_type: type[HeartbeatCadence],
    judge: FixedJudge,
    sink: CountingWakeSink,
):
    cadence = cadence_type(state_root, clock=lambda: NOW)
    effects = EffectLedger(state_root, clock=lambda: NOW)
    context = RegisteredHost(_config(state_root))
    components = _components(state_root, cadence, effects)
    runtime = register(
        context,
        components=components,
        heartbeat_judge=judge,
        wake_sink=sink,
        conversation_bridge=ConversationBridge(
            state_root,
            session_store=components.session,
            effect_ledger=components.effects,
            clock=lambda: NOW,
        ),
    )
    return context, runtime


def _invoke(context, payload, capsys):
    parser = argparse.ArgumentParser()
    context.cli["setup_fn"](parser)
    args = parser.parse_args(
        [
            "heartbeat",
            "daily_anchor",
            "--context",
            json.dumps(payload, ensure_ascii=False),
        ]
    )
    code = context.cli["handler_fn"](args)
    return code, json.loads(capsys.readouterr().out)


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_daily_anchor_crash_after_cadence_write_preserves_effect_recovery(
    tmp_path, capsys
):
    state_root = tmp_path / "state"
    decision = JudgeDecision(True, False, "synthetic-anchor", "synthetic wake")
    first_judge = FixedJudge(decision)
    first_sink = CountingWakeSink()
    first_context, _first_runtime = _registered(
        state_root, CrashAfterAnchorWriteCadence, first_judge, first_sink
    )
    payload = {
        "source_event_id": "synthetic-daily-anchor",
        "active_chat": False,
    }

    with pytest.raises(AnchorWriteCrash, match="storage boundary"):
        _invoke(first_context, payload, capsys)
    capsys.readouterr()

    cadence_state = json.loads(
        (state_root / "heartbeat_cadence.json").read_text(encoding="utf-8")
    )
    plan_rows = _jsonl(state_root / "heartbeat_effect_plans.jsonl")
    effect_rows = _jsonl(state_root / "effects.jsonl")

    replay_judge = FixedJudge(decision)
    replay_sink = CountingWakeSink()
    replay_context, _replay_runtime = _registered(
        state_root, HeartbeatCadence, replay_judge, replay_sink
    )
    replay_code, replay_output = _invoke(replay_context, payload, capsys)

    observed = {
        "anchor_epoch": cadence_state["daily_anchor_epochs"].get("daily_anchor"),
        "plan_rows": len(plan_rows),
        "effect_intents": sum(
            row.get("operation") == "begin_intent" for row in effect_rows
        ),
        "first_judge_calls": first_judge.calls,
        "first_sink_calls": first_sink.calls,
        "replay_code": replay_code,
        "replay_status": replay_output["status"],
        "replay_reason": replay_output["reason"],
        "replay_judge_calls": replay_judge.calls,
        "replay_sink_calls": replay_sink.calls,
    }
    assert observed == {
        "anchor_epoch": "2026-08-22",
        "plan_rows": 1,
        "effect_intents": 0,
        "first_judge_calls": 1,
        "first_sink_calls": [],
        "replay_code": 0,
        "replay_status": "pending",
        "replay_reason": "awaiting_effect_intent",
        "replay_judge_calls": 0,
        "replay_sink_calls": [],
    }


def test_daily_anchor_plan_write_crash_does_not_consume_cadence(tmp_path, capsys):
    state_root = tmp_path / "state"
    decision = JudgeDecision(True, False, "synthetic-anchor", "synthetic wake")
    judge = FixedJudge(decision)
    sink = CountingWakeSink()
    context, runtime = _registered(state_root, HeartbeatCadence, judge, sink)
    runtime.heartbeat._effect_plans = FailingPlanLedger(
        state_root / "heartbeat_effect_plans.jsonl"
    )
    payload = {
        "source_event_id": "synthetic-plan-write-failure",
        "active_chat": False,
    }

    with pytest.raises(PlanWriteCrash, match="closed effect plan storage"):
        _invoke(context, payload, capsys)
    capsys.readouterr()

    cadence_path = state_root / "heartbeat_cadence.json"
    plan_path = state_root / "heartbeat_effect_plans.jsonl"
    effects_path = state_root / "effects.jsonl"
    assert not cadence_path.exists()
    assert not plan_path.exists()
    assert not effects_path.exists()
    assert HeartbeatCadence(state_root, clock=lambda: NOW).daily_anchor_due(
        NOW, kind="daily_anchor"
    )
    assert judge.calls == 1
    assert sink.calls == []
