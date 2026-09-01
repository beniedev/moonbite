from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from moonbite_plugin.components import (
    REQUIRED_STATE_DOMAINS,
    RuntimeComponents,
    RuntimeComponentsError,
)
from moonbite_plugin.autonomy import AllowAutonomyJudge
from moonbite_plugin.conversation import ConversationBridge
from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger
from moonbite_plugin.heartbeat import HeartbeatCadence
from moonbite_plugin.memory import MemoryStore
from moonbite_plugin.memory_orchestration import (
    ExposureLedger,
    MemoryOrchestrator,
)
from moonbite_plugin.panel import PanelStore
from moonbite_plugin.plugin import register
from moonbite_plugin.runtime_core import EventBus
from moonbite_plugin.service import MoonbiteRuntime
from moonbite_plugin.session import (
    HOOK_ORDER,
    SessionContext,
    SessionLifecycleError,
    SessionLifecycleStore,
)


class RecordingLocks:
    def __init__(self):
        self.exclusive_names: list[str] = []
        self.try_exclusive_names: list[str] = []

    @contextmanager
    def exclusive(self, name: str):
        self.exclusive_names.append(name)
        yield

    @contextmanager
    def try_exclusive(self, name: str):
        self.try_exclusive_names.append(name)
        yield True


def injected_bundle(root: Path) -> tuple[RuntimeComponents, RecordingLocks, EventBus]:
    bus = EventBus(root)
    controls = ControlStore(root)
    cadence = HeartbeatCadence(root)
    panel = PanelStore(root, bus=bus, timezone_name="UTC")
    memory = MemoryStore(root)
    session = SessionLifecycleStore(root)
    effects = EffectLedger(root)
    locks = RecordingLocks()
    return (
        RuntimeComponents.injected(
            "fixture-owner",
            bus=bus,
            controls=controls,
            cadence=cadence,
            panel=panel,
            memory=memory,
            session=session,
            effects=effects,
            locks=locks,
        ),
        locks,
        bus,
    )


def test_standalone_keeps_legacy_event_state_readable(tmp_path):
    root = tmp_path / "standalone"
    legacy_bus = EventBus(root)
    legacy_bus.emit("fixture.legacy", source="host")

    runtime = MoonbiteRuntime({}, root=root)

    assert runtime.bus.read_events()[0].kind == "fixture.legacy"
    assert {"local_reflection", "paper_browse", "x_browse"} <= set(
        runtime.providers.names()
    )


def test_standalone_memory_orchestrator_shares_component_owners(tmp_path):
    runtime = MoonbiteRuntime({"modules": {"memory": True}}, root=tmp_path)

    assert runtime.memory_orchestrator is not None
    assert runtime.memory_orchestrator.memory_store is runtime.memory
    assert runtime.memory_orchestrator.session_store is runtime.session
    assert runtime.memory_orchestrator.effect_ledger is runtime.effects
    assert runtime.memory_orchestrator.sources is runtime.source_registry
    assert runtime.source_registry.retriever is runtime.source_registry.opener


def test_root_and_components_are_mutually_exclusive_before_path_work(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"

    with pytest.raises(RuntimeComponentsError, match="multiple_state_writers"):
        MoonbiteRuntime({}, components=components, root=trap_root)

    assert not trap_root.exists()


def test_injected_runtime_uses_exact_components_and_host_bus(tmp_path):
    components, locks, bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"
    runtime = MoonbiteRuntime(
        {"state": {"directory": str(trap_root)}}, components=components
    )

    assert runtime.components is components
    assert runtime.bus is components.bus
    assert runtime.controls is components.controls
    assert runtime.cadence is components.cadence
    assert runtime.panel is components.panel
    assert runtime.memory is components.memory
    assert runtime.session is components.session
    assert runtime.session_store is components.session
    assert runtime.effects is components.effects
    assert runtime.effect_ledger is components.effects
    assert runtime.heartbeat.locks is locks
    assert runtime.autonomy.locks is locks
    assert runtime.heartbeat.effect_ledger is components.effects
    assert runtime.autonomy.effect_ledger is components.effects
    provider_names = set(runtime.providers.names())
    assert {"local_reflection", "x_browse"} <= provider_names
    assert "paper_browse" not in provider_names

    runtime.emit_event("fixture.host_event", source="host")

    assert bus.read_events()[0].kind == "fixture.host_event"
    assert not trap_root.exists()
    assert runtime.status(include_private_paths=True)["state_root"] == "host_owned"


def test_injected_runtime_without_bridge_fails_closed_without_local_bridge_state(
    tmp_path,
):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"
    runtime = MoonbiteRuntime(
        {
            "state": {"directory": str(trap_root)},
            "modules": {"heartbeat": True, "autonomy": True},
        },
        components=components,
        autonomy_judge=AllowAutonomyJudge(),
    )

    assert runtime.conversation_bridge is None
    assert runtime._conversation_active_chat() is True
    runtime.status()
    runtime.control("status")
    assert not trap_root.exists()
    assert not (tmp_path / "host" / "conversation_bridge.jsonl").exists()


def test_injected_memory_surfaces_fail_closed_without_orchestrator(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"
    runtime = MoonbiteRuntime(
        {
            "state": {"directory": str(trap_root)},
            "modules": {"memory": True},
            "memory": {"recall_enabled": True, "maintenance_enabled": True},
        },
        components=components,
    )

    assert runtime.memory_orchestrator is None
    assert runtime.memory_prompt_context("fixture") is None
    with pytest.raises(RuntimeError, match="orchestration is unavailable"):
        runtime.propose_memory_maintenance(
            request_id="fixture-request",
            operation="retire",
            evidence_refs=[],
            reason="fixture",
        )
    with pytest.raises(RuntimeError, match="writer orchestration is unavailable"):
        runtime.submit_memory_write(
            "flush",
            lambda _request: None,
            source_event_id="fixture-event",
            idempotency_key="fixture-idempotency",
            epoch_id="fixture-epoch",
            content="fixture",
        )
    assert not trap_root.exists()
    assert not (tmp_path / "host" / "memory_orchestration.jsonl").exists()


def test_concrete_orchestrator_must_use_injected_bundle_owners(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    other_root = tmp_path / "other"
    foreign = MemoryOrchestrator(
        other_root,
        memory_store=MemoryStore(other_root),
        session_store=SessionLifecycleStore(other_root),
        effect_ledger=EffectLedger(other_root),
        exposure_ledger=ExposureLedger(other_root),
    )

    with pytest.raises(
        RuntimeComponentsError, match="memory orchestrator memory_store"
    ):
        MoonbiteRuntime(
            {},
            components=components,
            memory_orchestrator=foreign,
        )

    partial = MemoryOrchestrator(exposure_ledger=ExposureLedger(tmp_path / "partial"))
    with pytest.raises(
        RuntimeComponentsError, match="memory orchestrator memory_store"
    ):
        MoonbiteRuntime(
            {},
            components=components,
            memory_orchestrator=partial,
        )


def test_concrete_conversation_bridge_must_use_injected_session_and_effect_owners(
    tmp_path,
):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    other_root = tmp_path / "other-bridge"
    foreign = ConversationBridge(
        other_root,
        session_store=SessionLifecycleStore(other_root),
        effect_ledger=EffectLedger(other_root),
    )

    with pytest.raises(
        RuntimeComponentsError, match="conversation bridge session_store"
    ):
        MoonbiteRuntime(
            {},
            components=components,
            conversation_bridge=foreign,
        )


def test_caller_active_chat_cannot_lower_durable_active_chat_gate(tmp_path):
    runtime = MoonbiteRuntime(
        {
            "modules": {"heartbeat": True, "autonomy": True},
            "heartbeat": {
                "kinds": {
                    "fixture": {
                        "enabled": True,
                        "profile": "routine",
                        "judge": "required",
                        "host_only": False,
                        "bypass": [],
                    }
                }
            },
        },
        root=tmp_path,
        autonomy_judge=AllowAutonomyJudge(),
    )
    runtime.record_session_hook("on_session_start", {"session_id": "active"})
    runtime.record_session_hook(
        "pre_llm_call", {"session_id": "active", "turn_id": "turn-active"}
    )

    heartbeat = runtime.run_heartbeat(
        "fixture", context={"source_event_id": "event-active", "active_chat": False}
    )
    autonomy = runtime.run_autonomy(facts={"active_chat": False, "chat_active": False})

    assert heartbeat.reason == "active_chat"
    assert autonomy.reason == "active_chat"


def test_injected_bus_write_failure_propagates_without_local_fallback(
    tmp_path, monkeypatch
):
    components, _locks, bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"
    runtime = MoonbiteRuntime(
        {"state": {"directory": str(trap_root)}}, components=components
    )

    def fail(*_args: Any, **_kwargs: Any):
        raise OSError("fixture bus failure")

    monkeypatch.setattr(bus, "emit", fail)

    with pytest.raises(OSError, match="fixture bus failure"):
        runtime.emit_event("fixture.failed", source="host")

    assert bus.read_events() == []
    assert not trap_root.exists()


def test_injected_resurface_uses_supplied_lock_provider(tmp_path):
    components, locks, _bus = injected_bundle(tmp_path / "host")
    runtime = MoonbiteRuntime(
        {
            "modules": {"memory": True},
            "memory": {"recall_enabled": True, "resurfacing_enabled": True},
        },
        components=components,
    )
    runtime.add_memory_card(
        "Fixture resurface card",
        provenance="agent_observation",
        source_ref="fixture:event",
    )

    result = runtime.resurface_memory("resurface card", active_chat=True)

    assert len(result) == 1
    assert locks.exclusive_names == ["memory_resurface.request"]


def test_injected_paper_provider_is_rejected_without_state_writes(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"

    with pytest.raises(RuntimeComponentsError, match="paper_browse"):
        MoonbiteRuntime(
            {
                "state": {"directory": str(trap_root)},
                "autonomy": {"providers": {"paper_browse": {"enabled": True}}},
            },
            components=components,
        )

    assert not trap_root.exists()
    assert not (tmp_path / "host" / "paper_browse_seen.jsonl").exists()


def test_provided_standalone_bundle_is_not_an_injection_seam(tmp_path):
    components = RuntimeComponents.standalone(
        tmp_path / "standalone", "UTC", anchor_hour=6
    )

    with pytest.raises(RuntimeComponentsError, match="injected_components_required"):
        MoonbiteRuntime({}, components=components)


class RecordingContext:
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = {} if config is None else config
        self.scenario_pack = None
        self.llm = object()
        self.tools: dict[str, dict[str, Any]] = {}
        self.hooks: dict[str, Any] = {}
        self.auxiliary_tasks: dict[str, dict[str, Any]] = {}
        self.registered: list[str] = []
        self.hook_order: list[str] = []

    def get_config(self, key: str, default=None):
        if key == "config":
            return self.config
        if key == "scenario_pack":
            return self.scenario_pack
        return default

    def register_tool(self, **kwargs: Any):
        self.registered.append("tool")
        self.tools[kwargs["name"]] = kwargs

    def register_hook(self, name: str, handler):
        self.registered.append("hook")
        self.hook_order.append(name)
        self.hooks[name] = handler

    def register_auxiliary_task(self, key: str, **kwargs: Any):
        self.registered.append("auxiliary")
        self.auxiliary_tasks[key] = kwargs

    def register_cli_command(self, **kwargs: Any):
        self.registered.append("cli")
        self.cli = kwargs

    def register_command(self, name: str, **kwargs: Any):
        self.registered.append("slash")
        self.slash = {"name": name, **kwargs}


def test_plugin_injected_tool_writes_host_bus(tmp_path):
    components, _locks, bus = injected_bundle(tmp_path / "host")
    trap_root = tmp_path / "trap"
    context = RecordingContext({"state": {"directory": str(trap_root)}})

    register(context, components=components)
    result = json.loads(
        context.tools["record_moonbite_event"]["handler"](
            {"kind": "fixture.plugin_event", "payload": {}}
        )
    )

    assert result["ok"] is True
    assert bus.read_events()[0].kind == "fixture.plugin_event"
    assert not trap_root.exists()


def test_invalid_bundle_rejects_before_plugin_registration(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    forged = replace(components, writer_count=1)
    object.__setattr__(forged, "writer_count", 0)
    context = RecordingContext(
        {
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "fixture_main"},
                "heartbeat": {"alias": "fixture_heartbeat"},
                "hippocampus": {"alias": "fixture_memory"},
            }
        }
    )

    with pytest.raises(RuntimeComponentsError, match="writer_count"):
        register(context, components=forged)

    assert context.registered == []
    assert context.auxiliary_tasks == {}


def test_partial_domain_bundle_rejects_before_plugin_registration(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    object.__setattr__(
        components,
        "owned_domains",
        REQUIRED_STATE_DOMAINS - {"effect"},
    )
    context = RecordingContext(
        {
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "fixture_main"},
                "heartbeat": {"alias": "fixture_heartbeat"},
                "hippocampus": {"alias": "fixture_memory"},
            }
        }
    )

    with pytest.raises(RuntimeComponentsError, match="owned state domains"):
        register(context, components=components)

    assert context.registered == []
    assert context.auxiliary_tasks == {}


def test_partial_bundle_rejects_as_runtime_components_error(tmp_path):
    _components, _locks, _bus = injected_bundle(tmp_path / "host")
    partial = object.__new__(RuntimeComponents)
    context = RecordingContext()

    with pytest.raises(RuntimeComponentsError, match="invalid runtime components"):
        register(context, components=partial)

    assert context.registered == []


def test_session_effect_owners_are_validated_before_registration(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    object.__setattr__(components, "session", object())
    context = RecordingContext()

    with pytest.raises(RuntimeComponentsError, match="session owner"):
        register(context, components=components)

    assert context.registered == []
    assert context.hook_order == []


def test_registered_session_hooks_are_ordered_and_default_source_isolation(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    context = RecordingContext()

    register(context, components=components)

    assert context.hook_order == list(HOOK_ORDER)
    context.hooks["pre_gateway_dispatch"](
        event=SimpleNamespace(internal=False), gateway=object(), session_store=None
    )
    assert components.session.ledger.rows() == []

    context.hooks["on_session_start"](
        session_id="session-fixture", model="fixture", platform="cli"
    )
    context.hooks["pre_llm_call"](
        session_id="session-fixture",
        task_id="task-fixture",
        turn_id="turn-fixture",
        user_message="private body must not be persisted",
        conversation_history=["private body must not be persisted"],
        model="fixture",
        platform="cli",
    )
    context.hooks["post_llm_call"](
        session_id="session-fixture",
        task_id="task-fixture",
        turn_id="turn-fixture",
        user_message="private body must not be persisted",
        assistant_response="private body must not be persisted",
        conversation_history=[],
        model="fixture",
        platform="cli",
    )
    context.hooks["on_session_finalize"](
        session_id="session-fixture", platform="cli", reason="fixture"
    )

    snapshot = components.session.snapshot("session-fixture")
    assert snapshot is not None
    assert snapshot.hooks == HOOK_ORDER[1:]
    assert snapshot.private_contact_count == 0
    assert not components.panel.path.exists()
    assert "private body" not in components.session.ledger.path.read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize(
    ("reason", "complete"),
    (("session_expired", False), ("session_expired", True)),
)
def test_plugin_session_expired_finalize_closes_open_turn(tmp_path, reason, complete):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    context = RecordingContext()
    register(context, components=components)

    context.hooks["on_session_start"](session_id="session-fixture")
    context.hooks["pre_llm_call"](session_id="session-fixture", turn_id="turn-fixture")
    if complete:
        context.hooks["post_llm_call"](
            session_id="session-fixture", turn_id="turn-fixture"
        )
    context.hooks["on_session_finalize"](session_id="session-fixture", reason=reason)

    snapshot = components.session.snapshot("session-fixture")
    assert snapshot is not None
    assert snapshot.finalized is True
    assert snapshot.open_turn_id is None
    assert snapshot.settled_turn_ids == (("turn-fixture",) if complete else ())
    assert snapshot.abandoned_turn_ids == (() if complete else ("turn-fixture",))
    assert [
        row["reason"]
        for row in components.session.ledger.rows()
        if row["kind"] == "turn_terminal"
    ] == ([] if complete else ["host_session_finalized"])


def test_plugin_shutdown_finalize_is_noop_and_successor_pre_repairs_old_turn(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    context = RecordingContext()
    register(context, components=components)

    context.hooks["on_session_start"](session_id="session-fixture")
    context.hooks["pre_llm_call"](session_id="session-fixture", turn_id="turn-1")
    context.hooks["on_session_finalize"](
        session_id="session-fixture", reason="shutdown"
    )
    context.hooks["pre_llm_call"](session_id="session-fixture", turn_id="turn-2")

    snapshot = components.session.snapshot("session-fixture")
    assert snapshot is not None
    assert snapshot.finalized is False
    assert snapshot.open_turn_id == "turn-2"
    assert snapshot.abandoned_turn_ids == ("turn-1",)
    assert [
        row["hook"] for row in components.session.ledger.rows() if row["kind"] == "hook"
    ] == ["on_session_start", "pre_llm_call", "pre_llm_call"]
    assert [
        row["reason"]
        for row in components.session.ledger.rows()
        if row["kind"] == "turn_terminal"
    ] == ["superseded_by_new_pre"]


@pytest.mark.parametrize("reason", (None, "new_session", "other", ["shutdown"]))
def test_plugin_non_definitive_hermes_finalize_keeps_bare_fail_closed_behavior(
    tmp_path, reason
):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    context = RecordingContext()
    register(context, components=components)

    context.hooks["on_session_start"](session_id="session-fixture")
    context.hooks["pre_llm_call"](session_id="session-fixture", turn_id="turn-fixture")
    with pytest.raises(SessionLifecycleError, match="settled"):
        context.hooks["on_session_finalize"](
            session_id="session-fixture", reason=reason
        )

    rows = components.session.ledger.rows()
    assert len(rows) == 2
    assert all(row["kind"] != "turn_terminal" for row in rows)


def test_session_expired_falls_back_for_legacy_session_owner(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    underlying_session = components.session
    legacy_session = SimpleNamespace(
        record_hook=underlying_session.record_hook,
        snapshot=underlying_session.snapshot,
        replay=underlying_session.replay,
    )
    components = replace(components, session=legacy_session)
    context = RecordingContext()
    register(context, components=components)

    context.hooks["on_session_start"](session_id="session-fixture")
    context.hooks["pre_llm_call"](session_id="session-fixture", turn_id="turn-fixture")
    with pytest.raises(SessionLifecycleError, match="settled"):
        context.hooks["on_session_finalize"](
            session_id="session-fixture", reason="session_expired"
        )

    rows = underlying_session.ledger.rows()
    assert len(rows) == 2
    assert all(row["kind"] != "turn_terminal" for row in rows)


def test_resolver_can_supply_authorized_private_gateway_context(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    context = RecordingContext()

    def resolver(hook, kwargs, supported_hooks):
        source_kind = {
            "pre_gateway_dispatch": "private_inbound",
            "on_session_start": "session_start",
            "pre_llm_call": "system",
            "post_llm_call": "assistant_response",
            "on_session_finalize": "system",
        }[hook]
        source_id = (
            "private-message:fixture"
            if hook == "pre_gateway_dispatch"
            else f"{hook}:fixture"
        )
        return SessionContext(
            session_id="session-fixture",
            lifecycle_id="session-fixture",
            source_id=source_id,
            turn_id=kwargs.get("turn_id"),
            source_kind=source_kind,
            observed_at=datetime.now(UTC),
            fresh=hook == "pre_gateway_dispatch",
            supported_hooks=frozenset(supported_hooks),
        )

    register(context, components=components, session_context_resolver=resolver)
    context.hooks["pre_gateway_dispatch"](
        event=SimpleNamespace(internal=True, message_id="internal-fixture"),
        gateway=object(),
        session_store=None,
    )
    assert components.session.ledger.rows() == []
    assert components.session.snapshot("session-fixture") is None

    context.hooks["pre_gateway_dispatch"](
        event=SimpleNamespace(internal=False, message_id="fixture-message"),
        gateway=object(),
        session_store=None,
    )
    assert components.session.snapshot("session-fixture").private_contact_count == 1


def test_only_typed_private_receipt_projects_contact_and_wake_only_is_neutral(
    tmp_path,
):
    components, _locks, _bus = injected_bundle(tmp_path / "host")

    def resolver(hook, _kwargs, _supported_hooks):
        is_private = hook == "pre_gateway_dispatch"
        session_id = "session-private" if is_private else "session-ordinary"
        return SessionContext(
            session_id=session_id,
            lifecycle_id=session_id,
            source_id=("private:fixture" if is_private else f"ordinary:{hook}"),
            source_kind="private_inbound" if is_private else "session_start",
            observed_at=datetime.now(UTC),
            fresh=is_private,
            supported_hooks=frozenset({hook}),
        )

    runtime = MoonbiteRuntime(
        {"modules": {"heartbeat": True}},
        components=components,
        session_context_resolver=resolver,
    )
    ordinary = runtime.record_session_hook("on_session_start")
    assert ordinary.context.counts_as_private_contact is False
    assert runtime.cadence.snapshot()["private_contact_sources"] == []

    runtime.run_heartbeat(
        "wake-only",
        context={"source_event_id": "wake-only", "session_receipt": ordinary},
    )
    assert runtime.cadence.snapshot()["private_contact_sources"] == []

    private = runtime.record_session_hook(
        "pre_gateway_dispatch",
        {"event": SimpleNamespace(internal=False)},
    )
    assert private.context.counts_as_private_contact is True
    assert runtime.cadence.snapshot()["private_contact_sources"] == ["private:fixture"]


def test_session_append_failure_is_visible_without_local_fallback(
    tmp_path, monkeypatch
):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    runtime = MoonbiteRuntime({}, components=components)

    def fail(*_args, **_kwargs):
        raise OSError("fixture session append failure")

    monkeypatch.setattr(components.session, "record_hook", fail)

    with pytest.raises(OSError, match="fixture session append failure"):
        runtime.record_session_hook(
            "on_session_start", {"session_id": "session-fixture"}
        )

    assert runtime.last_session_hook_error == {
        "hook": "on_session_start",
        "error": "OSError",
    }
    assert runtime.status()["last_session_hook_error"] == {
        "hook": "on_session_start",
        "error": "OSError",
    }
    assert not (tmp_path / "trap").exists()


def test_default_finalize_without_existing_lifecycle_is_neutral(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    runtime = MoonbiteRuntime({}, components=components)

    assert (
        runtime.record_session_hook(
            "on_session_finalize", {"session_id": "never-started"}
        )
        is None
    )
    assert components.session.ledger.rows() == []
    assert runtime.last_session_hook_error is None


def test_default_hook_missing_turn_id_is_explicit_non_mutating_rejection(tmp_path):
    components, _locks, _bus = injected_bundle(tmp_path / "host")
    runtime = MoonbiteRuntime({}, components=components)

    assert (
        runtime.record_session_hook("pre_llm_call", {"session_id": "fixture"}) is None
    )
    assert components.session.ledger.rows() == []
    assert runtime.last_session_hook_error == {
        "hook": "pre_llm_call",
        "error": "SessionHookMappingError",
    }
