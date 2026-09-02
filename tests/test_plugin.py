from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from moonbite_plugin.autonomy import ActivityProvider, AllowAutonomyJudge
from moonbite_plugin.config import ConfigError
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.plugin import (
    RegistrationPlan,
    TOOL_NAMES,
    build_runtime,
    register,
    register_runtime,
)
from moonbite_plugin.session import HOOK_ORDER, SessionContext
from moonbite_plugin.service import MoonbiteRuntime


class FakeContext:
    def __init__(self, config=None, scenario_pack=None):
        self.config = {} if config is None else config
        self.scenario_pack = scenario_pack
        self.cli = None
        self.slash = None
        self.tools = {}
        self.hooks = {}
        self.auxiliary_tasks = {}
        self.llm = object()

    def get_config(self, key, default=None):
        if key == "config":
            return self.config
        if key == "scenario_pack":
            return self.scenario_pack
        return default

    def register_cli_command(self, **kwargs):
        self.cli = kwargs

    def register_command(self, name, handler, description="", args_hint=""):
        self.slash = {
            "name": name,
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


class RecordingConversationBridge:
    def __init__(self):
        self.receipts = []

    def observe(self, receipt):
        self.receipts.append(receipt)


def test_register_exposes_runtime_tools_and_operator_surfaces(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    runtime = register(ctx)
    assert isinstance(runtime, MoonbiteRuntime)
    assert ctx.cli["name"] == "moonbite"
    assert ctx.slash["name"] == "moon"
    assert set(ctx.tools) == {
        "moonbite_status",
        "control_moonbite_runtime",
        "record_moonbite_event",
        "run_moonbite_heartbeat",
        "run_moonbite_autonomy",
        "get_moonbite_panel",
        "search_moonbite_memory",
        "open_moonbite_memory",
        "capture_moonbite_memory_card",
        "synthesize_moonbite_diary",
    }
    assert set(ctx.hooks) == set(HOOK_ORDER)
    assert ctx.auxiliary_tasks == {}
    report = json.loads(ctx.slash["handler"]("doctor"))
    assert report["ok"] is True
    assert report["network_probe"] == "not_performed"


def test_register_passes_injected_conversation_bridge(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    bridge = RecordingConversationBridge()
    ctx = FakeContext({"state": {"directory": str(tmp_path / "state")}})

    register(ctx, conversation_bridge=bridge)
    ctx.hooks["on_session_start"](session_id="bridge-session")

    assert len(bridge.receipts) == 1
    assert bridge.receipts[0].context.session_id == "bridge-session"


def test_registered_subagent_stop_closes_child_turn(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    runtime = register(ctx)

    ctx.hooks["on_session_start"](session_id="child-session")
    ctx.hooks["pre_llm_call"](
        session_id="child-session",
        turn_id="child-turn",
    )
    ctx.hooks["subagent_stop"](
        child_session_id="child-session",
        child_status="timeout",
        parent_turn_id="ignored",
        summary="ignored",
        goal="ignored",
        tool_history="ignored",
    )

    snapshot = runtime.session.snapshot("child-session")
    assert snapshot.open_turn_id is None
    assert snapshot.abandoned_turn_ids == ("child-turn",)
    assert snapshot.settled_turn_ids == ()
    assert [
        row["hook"] for row in runtime.session.ledger.rows() if row["kind"] == "hook"
    ] == [
        "on_session_start",
        "pre_llm_call",
        "subagent_stop",
    ]
    assert [
        row["reason"]
        for row in runtime.session.ledger.rows()
        if row["kind"] == "turn_terminal"
    ] == [
        "host_turn_failed",
    ]


def test_register_passes_memory_injection_seams(monkeypatch, tmp_path):
    import moonbite_plugin.plugin as plugin_module

    captured = {}
    original_runtime = plugin_module.MoonbiteRuntime

    def runtime_factory(*args, **kwargs):
        captured.update(kwargs)
        return original_runtime(*args, **kwargs)

    monkeypatch.setattr(plugin_module, "MoonbiteRuntime", runtime_factory)
    memory_orchestrator = object()
    source_registry = object()
    approval_adapter = object()
    ctx = FakeContext()

    register(
        ctx,
        memory_orchestrator=memory_orchestrator,
        source_registry=source_registry,
        approval_adapter=approval_adapter,
    )

    assert captured["memory_orchestrator"] is memory_orchestrator
    assert captured["source_registry"] is source_registry
    assert captured["approval_adapter"] is approval_adapter


def test_build_runtime_uses_explicit_host_policy_ports(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    heartbeat_judge = object()
    autonomy_judge = object()
    wake_sink = object()
    diary_writer = object()
    ctx = FakeContext(
        {
            "state": {"directory": str(tmp_path / "state")},
            "delivery": {"adapter": "hermes_session", "target": "main"},
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "moon_main"},
                "heartbeat": {"alias": "moon_support"},
                "hippocampus": {"alias": "moon_hippocampus"},
            },
        }
    )

    runtime = build_runtime(
        ctx,
        heartbeat_judge=heartbeat_judge,
        autonomy_judge=autonomy_judge,
        wake_sink=wake_sink,
        diary_writer=diary_writer,
    )

    assert runtime.heartbeat.judge is heartbeat_judge
    assert runtime.heartbeat.sink is wake_sink
    assert runtime.autonomy.judge is autonomy_judge
    assert runtime.diary_writer is diary_writer


def test_register_forwards_explicit_host_policy_ports(monkeypatch, tmp_path):
    import moonbite_plugin.plugin as plugin_module

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    captured = {}
    original_build_runtime = plugin_module.build_runtime

    def recording_build_runtime(*args, **kwargs):
        captured.update(kwargs)
        return original_build_runtime(*args, **kwargs)

    monkeypatch.setattr(plugin_module, "build_runtime", recording_build_runtime)
    ports = {
        "heartbeat_judge": object(),
        "autonomy_judge": object(),
        "wake_sink": object(),
        "diary_writer": object(),
    }

    register(FakeContext(), plan=RegistrationPlan.shadow(), **ports)

    for name, value in ports.items():
        assert captured[name] is value


def test_invalid_config_aborts_registration():
    ctx = FakeContext({"model_routes": {"schema_version": "wrong"}})
    with pytest.raises(ConfigError):
        register(ctx)
    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}


def test_routes_register_unique_host_owned_auxiliary_tasks(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext(
        {
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "moon_main"},
                "heartbeat": {"alias": "moon_support"},
                "hippocampus": {"alias": "moon_support"},
            }
        }
    )
    register(ctx)
    assert set(ctx.auxiliary_tasks) == {"moon_main", "moon_support"}
    assert all(
        settings["defaults"]
        == {
            "provider": "auto",
            "model": "",
            "timeout": 60,
        }
        for settings in ctx.auxiliary_tasks.values()
    )


def test_shadow_plan_builds_runtime_without_surfaces_or_state_writes(
    monkeypatch, tmp_path
):
    state_root = tmp_path / "state"
    hermes_home = tmp_path / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    ctx = FakeContext(
        {
            "state": {"directory": str(state_root)},
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "moon_main"},
                "heartbeat": {"alias": "moon_support"},
                "hippocampus": {"alias": "moon_support"},
            },
        }
    )

    runtime = register(ctx, plan=RegistrationPlan.shadow())

    assert isinstance(runtime, MoonbiteRuntime)
    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.auxiliary_tasks == {}
    assert not state_root.exists()
    assert not hermes_home.exists()
    assert "model_reflection" in runtime.providers.names()


def test_registration_plan_selects_exact_surfaces(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    plan = RegistrationPlan(
        tool_names=frozenset({"moonbite_status", "get_moonbite_panel"}),
        hook_names=frozenset({"pre_llm_call", "on_session_finalize"}),
        cli=True,
        slash=False,
        auxiliary_tasks=False,
    )

    register(ctx, plan=plan)

    assert set(ctx.tools) == {"moonbite_status", "get_moonbite_panel"}
    assert set(ctx.hooks) == {"pre_llm_call", "on_session_finalize"}
    assert ctx.cli["name"] == "moonbite"
    assert ctx.slash is None
    assert ctx.auxiliary_tasks == {}


def test_invalid_plan_fails_before_any_host_registration(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()

    with pytest.raises(TypeError, match="RegistrationPlan"):
        register(ctx, plan=object())

    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.auxiliary_tasks == {}

    with pytest.raises(ValueError, match="unknown Moonbite tools"):
        RegistrationPlan(
            tool_names=frozenset({"not_moonbite"}),
            hook_names=frozenset(),
            cli=False,
            slash=False,
            auxiliary_tasks=False,
        )

    with pytest.raises(ValueError, match="only strings"):
        RegistrationPlan(
            tool_names=frozenset({1}),
            hook_names=frozenset(),
            cli=False,
            slash=False,
            auxiliary_tasks=False,
        )


@pytest.mark.parametrize(
    "missing_capability",
    ("register_tool", "register_hook", "register_cli_command", "register_command"),
)
def test_missing_registration_capability_fails_before_host_registration(
    monkeypatch, tmp_path, missing_capability
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    setattr(ctx, missing_capability, None)

    with pytest.raises(TypeError, match=missing_capability):
        register(ctx)

    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.auxiliary_tasks == {}


def test_missing_auxiliary_capability_is_plan_scoped(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    config = {
        "model_routes": {
            "schema_version": "moon.model_route_bindings.v1",
            "main": {"alias": "moon_main"},
            "heartbeat": {"alias": "moon_support"},
            "hippocampus": {"alias": "moon_support"},
        }
    }
    ctx = FakeContext(config)
    ctx.register_auxiliary_task = None

    with pytest.raises(TypeError, match="register_auxiliary_task"):
        register(ctx, plan=RegistrationPlan.all())
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.cli is None
    assert ctx.slash is None

    ctx = FakeContext(config)
    ctx.register_auxiliary_task = None
    no_auxiliary_tasks = RegistrationPlan(
        tool_names=frozenset(TOOL_NAMES),
        hook_names=frozenset(HOOK_ORDER),
        cli=True,
        slash=True,
        auxiliary_tasks=False,
    )
    register(ctx, plan=no_auxiliary_tasks)
    assert set(ctx.tools) == set(TOOL_NAMES)
    assert set(ctx.hooks) == set(HOOK_ORDER)
    assert tuple(ctx.hooks) == HOOK_ORDER
    assert ctx.cli["name"] == "moonbite"
    assert ctx.slash["name"] == "moon"


def test_prebuilt_runtime_registers_exact_surfaces(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    runtime = build_runtime(ctx)

    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert register_runtime(ctx, runtime) is None
    assert set(ctx.tools) == set(TOOL_NAMES)
    assert set(ctx.hooks) == set(HOOK_ORDER)
    assert ctx.cli["name"] == "moonbite"
    assert ctx.slash["name"] == "moon"


def test_prebuilt_runtime_rejects_type_or_config_mismatch_before_registration(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()

    with pytest.raises(TypeError, match="MoonbiteRuntime"):
        register_runtime(ctx, object())
    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.auxiliary_tasks == {}

    owner_ctx = FakeContext()
    runtime = build_runtime(owner_ctx)
    other_ctx = FakeContext()
    with pytest.raises(ValueError, match="different Hermes context"):
        register_runtime(other_ctx, runtime)
    assert other_ctx.cli is None
    assert other_ctx.slash is None
    assert other_ctx.tools == {}
    assert other_ctx.hooks == {}
    assert other_ctx.auxiliary_tasks == {}

    ctx = FakeContext({"state": {"directory": "state-b"}})
    runtime = build_runtime(ctx, raw_config={"state": {"directory": "state-a"}})
    with pytest.raises(ValueError, match="config does not match"):
        register_runtime(ctx, runtime)
    assert ctx.cli is None
    assert ctx.slash is None
    assert ctx.tools == {}
    assert ctx.hooks == {}
    assert ctx.auxiliary_tasks == {}


def test_panel_hooks_are_safe_when_module_is_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    register(ctx)
    assert set(ctx.hooks) == set(HOOK_ORDER)
    for handler in ctx.hooks.values():
        handler()
    assert not (tmp_path / "hermes-home").exists()


def test_fresh_activity_afterglow_is_ephemeral_chat_context(monkeypatch, tmp_path):
    from moonbite_plugin.panel import PanelStore
    from moonbite_plugin.runtime_core import EventBus

    state_root = tmp_path / "state"
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext(
        {
            "state": {"directory": str(state_root)},
            "modules": {"panel": True},
        }
    )
    register(ctx)
    panel = PanelStore(state_root, bus=EventBus(state_root), timezone_name="UTC")
    created_at = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    ledger = EffectLedger(state_root)
    effect = ledger.begin_intent(
        kind="autonomy_completion",
        source_event_id="event_fixture",
        idempotency_key="idempotency:event_fixture",
        epoch_id="epoch-fixture",
        content_sha256="a" * 64,
        content_length=1,
        created_at=created_at,
        expires_at=created_at + timedelta(hours=1),
    )
    pending = ledger.mark_pending(effect.effect_id)
    receipt = EffectReceipt(
        receipt_id="receipt_fixture",
        event_id="event_fixture",
        observed_at=created_at,
        content_sha256=pending.content_sha256,
        content_length=pending.content_length,
        epoch_id=pending.epoch_id,
    )
    verified = ledger.verify(effect.effect_id, receipt)
    panel.record_activity_afterglow(
        effect_record=verified,
        effect_receipt=receipt,
        canonical_event_id="event_fixture",
        summary="A synthetic paper topic",
    )

    injected = ctx.hooks["pre_llm_call"]()

    assert injected is not None
    assert "A synthetic paper topic" in injected["context"]
    assert "Mention it only when it fits" in injected["context"]


def test_disabled_module_tools_fail_visibly(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    register(ctx)
    response = json.loads(ctx.tools["get_moonbite_panel"]["handler"]({}))
    assert response["ok"] is False
    assert response["error"] == "RuntimeError"
    assert response["code"] == "module_disabled"
    assert response["message"] == "Panel is disabled."
    assert "modules.panel" in response["remediation"]


def test_pre_llm_memory_requires_typed_private_session_receipt(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    state_root = tmp_path / "state"
    ctx = FakeContext(
        {
            "state": {"directory": str(state_root)},
            "modules": {"memory": True},
            "memory": {"recall_enabled": True},
        }
    )
    register(ctx)
    ctx.tools["capture_moonbite_memory_card"]["handler"](
        {
            "summary": "Private typed memory body",
            "provenance": "agent_observation",
            "source_ref": "event:typed-private",
        }
    )

    assert ctx.hooks["pre_llm_call"](user_message="typed memory") is None

    ctx.hooks["on_session_start"](session_id="system-session")
    assert (
        ctx.hooks["pre_llm_call"](
            session_id="system-session",
            turn_id="system-turn",
            user_message="typed memory",
        )
        is None
    )
    assert not (state_root / "memory_orchestration.jsonl").exists()


def test_pre_llm_private_receipt_exposes_transient_memory_body(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    state_root = tmp_path / "state"
    ctx = FakeContext(
        {
            "state": {"directory": str(state_root)},
            "modules": {"memory": True},
            "memory": {"recall_enabled": True},
        }
    )

    def resolver(hook, kwargs, supported_hooks):
        source_kind = {
            "pre_gateway_dispatch": "private_inbound",
            "on_session_start": "session_start",
            "pre_llm_call": "private_inbound",
            "post_llm_call": "assistant_response",
            "on_session_finalize": "system",
        }[hook]
        source_id = (
            "private-gateway" if hook == "pre_gateway_dispatch" else "private-session"
        )
        return SessionContext(
            session_id="private-session",
            lifecycle_id="private-session",
            source_id=source_id,
            turn_id=kwargs.get("turn_id"),
            source_kind=source_kind,
            observed_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
            fresh=hook in {"pre_gateway_dispatch", "pre_llm_call"},
            supported_hooks=frozenset(supported_hooks),
        )

    register(ctx, session_context_resolver=resolver)
    captured = json.loads(
        ctx.tools["capture_moonbite_memory_card"]["handler"](
            {
                "summary": "Private typed memory body",
                "provenance": "agent_observation",
                "source_ref": "event:typed-private",
            }
        )
    )["result"]

    ctx.hooks["pre_gateway_dispatch"](
        event=SimpleNamespace(internal=False), gateway=object()
    )
    ctx.hooks["on_session_start"](session_id="private-session")
    injected = ctx.hooks["pre_llm_call"](
        session_id="private-session",
        turn_id="private-turn",
        user_message="typed memory",
    )

    assert injected is not None
    assert "Private typed memory body" in injected["context"]
    assert "exposures_json" in injected["context"]
    exposure_ledger = state_root / "memory_orchestration.jsonl"
    rows = exposure_ledger.read_text(encoding="utf-8")
    assert captured["card_id"] in rows
    assert "Private typed memory body" not in rows


def test_model_tools_cannot_assert_trusted_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"modules": {"heartbeat": True, "memory": True}})
    register(ctx)

    event = json.loads(
        ctx.tools["record_moonbite_event"]["handler"](
            {"kind": "fixture", "source": "operator", "payload": {}}
        )
    )
    assert event["result"]["source"] == "model_tool"
    assert (
        "source"
        not in ctx.tools["record_moonbite_event"]["schema"]["parameters"]["properties"]
    )

    memory = json.loads(
        ctx.tools["capture_moonbite_memory_card"]["handler"](
            {
                "summary": "fixture",
                "provenance": "user_explicit",
                "source_ref": "fixture:1",
            }
        )
    )
    assert memory["ok"] is False
    assert memory["error"] == "ValueError"
    assert memory["code"] == "invalid_request"
    provenance = ctx.tools["capture_moonbite_memory_card"]["schema"]["parameters"][
        "properties"
    ]["provenance"]["enum"]
    assert "user_explicit" not in provenance

    heartbeat = json.loads(
        ctx.tools["run_moonbite_heartbeat"]["handler"]({"kind": "critical_ops"})
    )
    assert heartbeat["ok"] is True
    assert heartbeat["result"]["reason_code"] == "candidate_invalid"

    autonomy_schema = ctx.tools["run_moonbite_autonomy"]["schema"]["parameters"]
    assert autonomy_schema["properties"] == {}
    assert autonomy_schema["additionalProperties"] is False


def test_model_tools_block_configured_routine_host_only_kind(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext(
        {
            "modules": {"heartbeat": True},
            "heartbeat": {
                "kinds": {
                    "routine": {
                        "enabled": True,
                        "profile": "routine",
                        "judge": "required",
                        "host_only": True,
                        "bypass": [],
                    }
                }
            },
        }
    )
    register(ctx)

    event = json.loads(
        ctx.tools["record_moonbite_event"]["handler"](
            {"kind": "routine", "payload": {}}
        )
    )
    heartbeat = json.loads(
        ctx.tools["run_moonbite_heartbeat"]["handler"]({"kind": "routine"})
    )

    assert event["code"] == "invalid_request"
    assert heartbeat["code"] == "invalid_request"


def test_model_event_rejects_invalid_kind_but_allows_valid_unknown(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"modules": {"heartbeat": True}})
    register(ctx)

    invalid = json.loads(
        ctx.tools["record_moonbite_event"]["handler"](
            {"kind": "Bad.Kind", "payload": {}}
        )
    )
    unknown = json.loads(
        ctx.tools["record_moonbite_event"]["handler"](
            {"kind": "ordinary.event", "payload": {}}
        )
    )

    assert invalid["ok"] is False
    assert invalid["error"] == "ValueError"
    assert invalid["code"] == "invalid_request"
    assert unknown["ok"] is True
    assert unknown["result"]["kind"] == "ordinary.event"


def test_model_memory_tool_wires_history_fields_and_exact_chain(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"modules": {"memory": True}})
    register(ctx)
    old = json.loads(
        ctx.tools["capture_moonbite_memory_card"]["handler"](
            {
                "summary": "Tool history old fixture",
                "provenance": "agent_observation",
                "source_ref": "event:old",
                "event_time": "2026-01-01",
                "entities": ["fixture-entity"],
                "state_key": "fixture.state",
            }
        )
    )["result"]
    new = json.loads(
        ctx.tools["capture_moonbite_memory_card"]["handler"](
            {
                "summary": "Tool history new fixture",
                "provenance": "agent_observation",
                "source_ref": "event:new",
                "event_time": "2026-02-01",
                "entities": ["fixture-entity"],
                "state_key": "fixture.state",
                "supersedes": [old["card_id"]],
                "supersession_kind": "evolution",
            }
        )
    )["result"]

    opened = json.loads(
        ctx.tools["open_moonbite_memory"]["handler"](
            {"open_ref": f"card:{new['card_id']}", "include_history": True}
        )
    )["result"]

    assert opened["record"]["card_id"] == new["card_id"]
    assert {card["card_id"] for card in opened["history"]} == {
        old["card_id"],
        new["card_id"],
    }


@pytest.mark.parametrize(
    ("kind", "rejected"),
    [("urgent_care", True), ("critical_ops", False), ("connector.failed", False)],
)
def test_model_tool_uses_configured_host_boundary_for_event(
    kind, rejected, monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"modules": {"heartbeat": True}})
    register(ctx)

    result = json.loads(
        ctx.tools["record_moonbite_event"]["handler"]({"kind": kind, "payload": {}})
    )

    if rejected:
        assert result["ok"] is False
        assert result["error"] == "ValueError"
        assert result["code"] == "invalid_request"
    else:
        assert result["ok"] is True
        assert result["result"]["kind"] == kind


def test_model_status_redacts_state_root_but_cli_keeps_operator_detail(
    capsys, monkeypatch, tmp_path
):
    state_root = tmp_path / "private-state"
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"state": {"directory": str(state_root)}})
    register(ctx)

    tool_result = json.loads(ctx.tools["moonbite_status"]["handler"]({}))
    assert tool_result["result"]["state_root"] == "private"

    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    assert ctx.cli["handler_fn"](parser.parse_args(["status"])) == 0
    assert json.loads(capsys.readouterr().out)["state_root"] == str(state_root)


def test_cli_parser_and_handler_round_trip(capsys, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    register(ctx)
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    args = parser.parse_args(["doctor"])
    assert ctx.cli["handler_fn"](args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_register_passes_host_activity_providers_to_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    provider = ActivityProvider("host_activity", lambda _request: "accepted")

    runtime = register(FakeContext(), activity_providers=(provider,))

    assert runtime.providers.get("host_activity") is provider


def test_autonomy_cli_accepts_stable_host_occurrence_identity(
    capsys, monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext(
        {
            "modules": {"autonomy": True},
            "autonomy": {
                "providers": {"host_activity": {"enabled": True, "weight": 1}}
            },
        }
    )
    register(
        ctx,
        autonomy_judge=AllowAutonomyJudge(),
        activity_providers=(
            ActivityProvider("host_activity", lambda _request: "accepted"),
        ),
    )
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)

    result = ctx.cli["handler_fn"](
        parser.parse_args(
            [
                "autonomy",
                "--occurrence-id",
                "occurrence-1",
                "--epoch-id",
                "epoch-1",
            ]
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert result == 0
    assert output["source_event_id"] == "occurrence-1"


def test_proactive_control_cli_slash_and_legacy_alias_share_state(
    capsys, monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    register(ctx)
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)

    assert (
        ctx.cli["handler_fn"](
            parser.parse_args(["control", "pause", "background_costly"])
        )
        == 0
    )
    paused = json.loads(capsys.readouterr().out)
    assert paused["control"]["feature"] == "proactive"

    status = json.loads(ctx.slash["handler"]("status proactive"))
    assert status["feature"] == "proactive"
    assert [item["feature"] for item in status["controls"]] == ["proactive"]

    resumed = json.loads(ctx.slash["handler"]("resume background_costly"))
    assert resumed["feature"] == "proactive"
    assert json.loads(ctx.slash["handler"]("status proactive"))["controls"] == []


def test_session_status_and_exact_repair_cli(capsys, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    state_root = tmp_path / "state"
    ctx = FakeContext({"state": {"directory": str(state_root)}})
    register(ctx)
    ctx.hooks["on_session_start"](session_id="session-cli")
    ctx.hooks["pre_llm_call"](
        session_id="session-cli",
        turn_id="turn-cli",
    )

    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    assert ctx.cli["handler_fn"](parser.parse_args(["session", "status"])) == 0
    status = json.loads(capsys.readouterr().out)
    assert status == {
        "ok": True,
        "open_turns": [
            {
                "lifecycle_id": "session-cli",
                "session_id": "session-cli",
                "turn_id": "turn-cli",
            }
        ],
    }

    assert (
        ctx.cli["handler_fn"](
            parser.parse_args(
                [
                    "session",
                    "repair",
                    "--lifecycle-id",
                    "session-cli",
                    "--turn-id",
                    "turn-cli",
                ]
            )
        )
        == 0
    )
    repaired = json.loads(capsys.readouterr().out)
    assert repaired["ok"] is True
    assert repaired["status"] == "repaired"
    assert repaired["outcome"] == "abandoned"
    assert repaired["reason"] == "operator_repair"
    assert repaired["turn_id"] == "turn-cli"
    assert "settled" not in repaired

    assert (
        ctx.cli["handler_fn"](
            parser.parse_args(
                [
                    "session",
                    "repair",
                    "--lifecycle-id",
                    "session-cli",
                    "--turn-id",
                    "turn-cli",
                ]
            )
        )
        == 0
    )
    already = json.loads(capsys.readouterr().out)
    assert already["status"] == "already_repaired"
    assert already["outcome"] == "abandoned"


def test_cli_and_slash_doctor_receive_registered_runtime(capsys, monkeypatch, tmp_path):
    import moonbite_plugin.plugin as plugin_module

    calls = []
    original = plugin_module.doctor_report

    def capture(raw, **kwargs):
        calls.append(kwargs.get("runtime"))
        return original(raw, **kwargs)

    monkeypatch.setattr(plugin_module, "doctor_report", capture)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext()
    register(ctx)

    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    assert ctx.cli["handler_fn"](parser.parse_args(["doctor"])) == 0
    capsys.readouterr()
    slash_report = json.loads(ctx.slash["handler"]("doctor"))

    assert slash_report["health"]["available"] is True
    assert len(calls) == 2
    assert all(runtime is not None for runtime in calls)


def test_memory_lifecycle_cli_and_hook_surfaces(capsys, monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    state_root = tmp_path / "state"
    ctx = FakeContext(
        {
            "state": {"directory": str(state_root)},
            "modules": {"memory": True},
            "memory": {
                "recall_enabled": True,
                "resurfacing_enabled": True,
                "maintenance_enabled": True,
                "resurfacing_limit": 1,
            },
        }
    )
    register(ctx)
    captured = json.loads(
        ctx.tools["capture_moonbite_memory_card"]["handler"](
            {
                "summary": "A CLI fixture memory",
                "provenance": "agent_observation",
                "source_ref": "event:fixture",
            }
        )
    )["result"]

    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)

    recall_args = parser.parse_args(["memory-recall", "CLI fixture", "--limit", "1"])
    assert ctx.cli["handler_fn"](recall_args) == 0
    recalled = json.loads(capsys.readouterr().out)
    assert recalled[0]["open_ref"] == f"card:{captured['card_id']}"
    assert "source_ref" not in recalled[0]

    injected = ctx.hooks["pre_llm_call"](user_message="CLI fixture")
    assert injected is None

    resurface_args = parser.parse_args(
        ["memory-resurface", "CLI fixture", "--active-chat", "--limit", "1"]
    )
    assert ctx.cli["handler_fn"](resurface_args) == 0
    assert json.loads(capsys.readouterr().out) == []

    ctx.hooks["on_session_start"](session_id="cli-session")
    ctx.hooks["pre_llm_call"](
        session_id="cli-session",
        turn_id="cli-turn",
        user_message="CLI fixture",
    )
    assert ctx.cli["handler_fn"](resurface_args) == 0
    resurfaced = json.loads(capsys.readouterr().out)
    assert resurfaced[0]["open_ref"] == f"card:{captured['card_id']}"

    maintenance_args = parser.parse_args(
        [
            "memory-maintenance-propose",
            "distill",
            "--request-id",
            "request-cli-fixture",
            "--evidence-ref",
            f"card:{captured['card_id']}",
            "--reason",
            "CLI fixture proposal",
            "--proposed-value",
            '{"summary":"bounded","provenance":"agent_observation"}',
        ]
    )
    assert ctx.cli["handler_fn"](maintenance_args) == 0
    proposal = json.loads(capsys.readouterr().out)
    assert proposal["status"] == "proposed"
    assert proposal["applied"] is False

    apply_args = parser.parse_args(
        [
            "memory-maintenance-apply",
            "--proposal-id",
            proposal["proposal_id"],
            "--permission",
            "manual",
        ]
    )
    assert ctx.cli["handler_fn"](apply_args) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "applied"
    assert receipt["operation"] == "distill"

    open_args = parser.parse_args(
        ["memory-open", f"card:{receipt['created_card']['card_id']}", "--history"]
    )
    assert ctx.cli["handler_fn"](open_args) == 0
    opened = json.loads(capsys.readouterr().out)
    assert opened["record"]["card_id"] == receipt["created_card"]["card_id"]


def test_memory_surfaces_expose_archive_but_never_delete(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    ctx = FakeContext({"modules": {"memory": True}})
    register(ctx)
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)

    with pytest.raises(SystemExit):
        parser.parse_args(
            ["memory-maintenance-apply", "--proposal-id", "proposal-fixture"]
        )

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "memory-maintenance-propose",
                "delete",
                "--request-id",
                "request-delete",
                "--evidence-ref",
                "card:fixture",
                "--reason",
                "forbidden",
                "--proposed-value",
                "null",
            ]
        )
    public_contract = json.dumps(
        {
            "tool_names": sorted(ctx.tools),
            "schemas": [tool["schema"] for tool in ctx.tools.values()],
        },
        sort_keys=True,
    ).casefold()
    assert "delete" not in public_contract
    assert "purge" not in public_contract


def test_root_entrypoint_can_load_as_a_top_level_module():
    root_entrypoint = Path(__file__).parents[1] / "__init__.py"
    spec = importlib.util.spec_from_file_location("__init__", root_entrypoint)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.register is register
