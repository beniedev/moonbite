"""Verify Moonbite against public Hermes plugin APIs and its real loader."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import inspect
import json
import os
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

import yaml
from hermes_cli.hooks import _DEFAULT_PAYLOADS
from hermes_cli.plugins import (
    PluginContext,
    PluginManager,
    PluginManifest,
    VALID_HOOKS,
)

from moonbite_plugin.config import normalize_config
from moonbite_plugin.config import ConfigError
from moonbite_plugin.hermes_adapter import HermesHostAdapter, HermesSessionWakeSink
from moonbite_plugin.heartbeat import HeartbeatCandidate, JudgeDecision
from moonbite_plugin.plugin import TOOL_NAMES
from moonbite_plugin.session import HOOK_ORDER
from moonbite_plugin.session import SessionLifecycleStore
from moonbite_plugin.scenarios import resolve_config


ROOT = Path(__file__).parents[1]
PRESETS = ("core-only", "panel-only", "memory-only", "full-companion")


def _turn_exit_contract() -> None:
    """Exercise the exact turn-terminal payload published by this Hermes."""

    now = datetime(2026, 8, 24, 19, 0, tzinfo=UTC)
    adapter = HermesHostAdapter(clock=lambda: now)
    supported = frozenset(HOOK_ORDER[1:])
    assert "on_session_end" in VALID_HOOKS
    assert "subagent_stop" in VALID_HOOKS
    host_end = dict(_DEFAULT_PAYLOADS["on_session_end"])
    assert {
        "session_id",
        "task_id",
        "turn_id",
        "completed",
        "failed",
        "interrupted",
        "turn_exit_reason",
    } <= host_end.keys()

    host_stop = dict(_DEFAULT_PAYLOADS["subagent_stop"])
    assert "child_status" in host_stop

    def start_turn(
        store: SessionLifecycleStore,
        hooks: frozenset[str] = supported,
    ) -> None:
        store.record_hook(
            adapter.session_context(
                "on_session_start",
                {"session_id": "test-session"},
                hooks,
            ),
            "on_session_start",
        )
        store.record_hook(
            adapter.session_context(
                "pre_llm_call",
                {
                    "session_id": "test-session",
                    "task_id": "test-task",
                    "turn_id": "test-turn",
                },
                hooks,
            ),
            "pre_llm_call",
        )

    with tempfile.TemporaryDirectory(prefix="moonbite-session-contract-") as root:
        normal = SessionLifecycleStore(Path(root) / "normal")
        start_turn(normal)
        normal.record_hook(
            adapter.session_context(
                "post_llm_call",
                {
                    "session_id": "test-session",
                    "task_id": "test-task",
                    "turn_id": "test-turn",
                },
                supported,
            ),
            "post_llm_call",
            settled=True,
        )
        normal_end = adapter.turn_terminal(host_end, supported_hooks=supported)
        normal.record_host_turn_end(normal_end.context, normal_end.reason)
        assert normal.snapshot("test-session").settled_turn_ids == ("test-turn",)

        rotated = SessionLifecycleStore(Path(root) / "rotated")
        start_turn(rotated)
        rotated_post = adapter.correlate_turn(
            adapter.session_context(
                "post_llm_call",
                {
                    "session_id": "compressed-session",
                    "task_id": "test-task",
                    "turn_id": "test-turn",
                },
                supported,
            ),
            rotated.replay(),
        )
        rotated.record_hook(rotated_post, "post_llm_call", settled=True)
        rotated_payload = {**host_end, "session_id": "compressed-session"}
        rotated_end = adapter.turn_terminal(
            rotated_payload,
            supported_hooks=supported,
            context=adapter.correlate_turn(
                adapter.session_context("on_session_end", rotated_payload, supported),
                rotated.replay(),
            ),
        )
        rotated.record_host_turn_end(rotated_end.context, rotated_end.reason)
        assert rotated.snapshot("compressed-session") is None
        assert rotated.snapshot("test-session").settled_turn_ids == ("test-turn",)

        continuation = adapter.pre_turn_context(
            adapter.session_context(
                "pre_llm_call",
                {
                    "session_id": "compressed-session",
                    "task_id": "next-task",
                    "turn_id": "next-turn",
                },
                supported,
            ),
            rotated.replay(),
        )
        rotated.record_hook(continuation, "pre_llm_call")
        continuation_end_payload = {
            **host_end,
            "session_id": "compressed-session",
            "task_id": "next-task",
            "turn_id": "next-turn",
            "completed": False,
            "failed": True,
        }
        continuation_end = adapter.turn_terminal(
            continuation_end_payload,
            supported_hooks=continuation.supported_hooks,
            context=adapter.correlate_turn(
                adapter.session_context(
                    "on_session_end", continuation_end_payload, supported
                ),
                rotated.replay(),
            ),
        )
        rotated.record_host_turn_end(continuation_end.context, continuation_end.reason)
        assert "on_session_start" not in continuation.supported_hooks
        assert rotated.snapshot("compressed-session").abandoned_turn_ids == (
            "next-turn",
        )

        exits = (
            (
                {"completed": False, "failed": True, "interrupted": False},
                "host_turn_failed",
            ),
            (
                {"completed": False, "failed": False, "interrupted": True},
                "host_turn_interrupted",
            ),
            (
                {"completed": True, "failed": False, "interrupted": False},
                "host_turn_completed",
            ),
        )
        for flags, expected_reason in exits:
            store = SessionLifecycleStore(Path(root) / expected_reason)
            start_turn(store)
            payload = {**host_end, **flags}
            terminal = adapter.turn_terminal(payload, supported_hooks=supported)
            receipt = store.record_host_turn_end(terminal.context, terminal.reason)
            assert receipt.snapshot.open_turn_id is None
            assert receipt.snapshot.abandoned_turn_ids == ("test-turn",)
            assert receipt.snapshot.settled_turn_ids == ()
            assert terminal.reason == expected_reason

        shutdown_fallbacks = (
            {
                "session_id": "test-session",
                "task_id": "",
                "turn_id": "",
                "api_request_id": "",
                "completed": False,
                "interrupted": True,
                "reason": "keyboard_interrupt",
                "platform": "cli",
            },
            {
                "session_id": "test-session",
                "completed": False,
                "interrupted": True,
                "reason": "shutdown",
                "platform": "cli",
            },
            {
                "session_id": "test-session",
                "completed": False,
                "interrupted": True,
                "platform": "tui",
            },
        )
        for index, payload in enumerate(shutdown_fallbacks):
            store = SessionLifecycleStore(Path(root) / f"shutdown-fallback-{index}")
            start_turn(store)
            context = adapter.session_end_shutdown_fallback(
                payload,
                supported_hooks=supported,
            )
            assert context is not None
            assert context.turn_id is None
            context = adapter.correlate_lifecycle(context, store.replay())
            receipt = store.record_host_shutdown(context)
            store.record_host_shutdown(context)
            assert receipt.snapshot.open_turn_id is None
            assert receipt.snapshot.finalized is False
            assert receipt.snapshot.abandoned_turn_ids == ("test-turn",)
            terminal_rows = [
                row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
            ]
            assert len(terminal_rows) == 1
            assert terminal_rows[0]["turn_id"] == "test-turn"
            assert terminal_rows[0]["reason"] == "host_shutdown"

        child = SessionLifecycleStore(Path(root) / "child-interrupted")
        start_turn(child)
        child_terminal = adapter.subagent_stop_terminal(
            {
                **host_stop,
                "child_session_id": "test-session",
                "child_status": "interrupted",
            }
        )
        assert child_terminal is not None
        child_receipt = child.record_host_child_stop(
            child_terminal.child_session_id,
            child_terminal.reason,
            now,
        )
        child_replay = child.record_host_child_stop(
            child_terminal.child_session_id,
            child_terminal.reason,
            now,
        )
        assert child_receipt.snapshot.open_turn_id is None
        assert child_receipt.snapshot.settled_turn_ids == ()
        assert child_replay.deduplicated is True
        late_end = adapter.turn_terminal(
            {
                **host_end,
                "completed": False,
                "failed": False,
                "interrupted": True,
            },
            supported_hooks=supported,
        )
        child.record_host_turn_end(late_end.context, late_end.reason)
        assert (
            len([row for row in child.ledger.rows() if row["kind"] == "turn_terminal"])
            == 1
        )

        timed_out = SessionLifecycleStore(Path(root) / "child-timeout")
        start_turn(timed_out)
        timeout_terminal = adapter.subagent_stop_terminal(
            {
                **host_stop,
                "child_session_id": "test-session",
                "child_status": "timeout",
            }
        )
        assert timeout_terminal is not None
        timeout_receipt = timed_out.record_host_child_stop(
            timeout_terminal.child_session_id,
            timeout_terminal.reason,
            now,
        )
        assert timeout_terminal.reason == "host_turn_failed"
        assert timeout_receipt.snapshot.open_turn_id is None
        timeout_terminals = [
            row for row in timed_out.ledger.rows() if row["kind"] == "turn_terminal"
        ]
        assert len(timeout_terminals) == 1
        assert timeout_terminals[0]["reason"] == "host_turn_failed"
        assert (
            adapter.subagent_stop_terminal(
                {
                    **host_stop,
                    "child_session_id": "test-session",
                    "child_status": "completed",
                }
            )
            is None
        )

        legacy_hooks = supported - {"subagent_stop"}
        legacy_child = SessionLifecycleStore(Path(root) / "legacy-child")
        start_turn(legacy_child, legacy_hooks)
        legacy_receipt = legacy_child.record_host_child_stop(
            "test-session",
            "host_turn_failed",
            now,
        )
        assert legacy_receipt.snapshot.supported_hooks == legacy_hooks
        assert "subagent_stop" not in legacy_receipt.snapshot.hooks


def _metadata_contract() -> None:
    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert manifest["manifest_version"] == 1
    assert manifest["kind"] == "standalone"
    assert tuple(manifest["provides_tools"]) == TOOL_NAMES
    assert tuple(manifest["provides_hooks"]) == HOOK_ORDER
    assert manifest["version"] == project["project"]["version"]

    required_public_methods = {
        "register_tool": ({"name", "toolset", "schema", "handler"},),
        "register_hook": (
            {"hook_name", "callback"},
            {"name", "handler"},
        ),
        "register_cli_command": ({"name", "help", "setup_fn"},),
        "register_command": ({"name", "handler"},),
        "register_auxiliary_task": ({"key"},),
        "get_config": ({"key"},),
    }
    for method_name, accepted_parameter_sets in required_public_methods.items():
        method = getattr(PluginContext, method_name)
        actual = set(inspect.signature(method).parameters) - {"self"}
        assert any(required <= actual for required in accepted_parameter_sets), (
            method_name,
            sorted(actual),
        )

    settings_example = yaml.safe_load(
        (ROOT / "config" / "example.yaml").read_text(encoding="utf-8")
    )
    effective_example = yaml.safe_load(
        (ROOT / "config" / "effective-config.example.yaml").read_text(encoding="utf-8")
    )
    assert "enabled" not in settings_example["plugins"]
    assert effective_example["plugins"]["enabled"] == ["moonbite"]
    for example in (settings_example, effective_example):
        entry = example["plugins"]["entries"]["moonbite"]
        assert "enabled" not in entry
        normalize_config(entry["settings"]["config"])

    for relative in (
        "README.md",
        "README.zh-CN.md",
        "CHANGELOG.md",
        "COMPATIBILITY.md",
        "SETUP.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        for hook in HOOK_ORDER:
            assert hook in text, (relative, hook)


@contextmanager
def _wake_home(*, allow_gateway_injection: bool):
    """Give the real Hermes manager a synthetic, isolated grant profile."""

    previous = os.environ.get("HERMES_HOME")
    with tempfile.TemporaryDirectory(prefix="moonbite-wake-") as temporary:
        home = Path(temporary)
        (home / "config.yaml").write_text(
            yaml.safe_dump(
                {
                    "plugins": {
                        "entries": {
                            "moonbite": {
                                "allow_gateway_injection": allow_gateway_injection
                            }
                        }
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        os.environ["HERMES_HOME"] = str(home)
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous


def _wake_context() -> tuple[PluginContext, PluginManager]:
    """Construct the official context and manager used by the wake adapter."""

    manager = PluginManager()
    manifest = PluginManifest(name="moonbite", key="moonbite", source=ROOT)
    return PluginContext(manifest, manager), manager


def _wake_candidate() -> tuple[HeartbeatCandidate, JudgeDecision]:
    return (
        HeartbeatCandidate("care_poke", candidate_id="contract-wake"),
        JudgeDecision(True, False, "contract wake"),
    )


def _wake_result(sink: HermesSessionWakeSink):
    candidate, decision = _wake_candidate()
    return sink.wake(candidate, decision)


def _wake_contract() -> None:
    """Exercise targeted wake admission through real Hermes public APIs."""

    inject_signature = inspect.signature(PluginContext.inject_message)
    inject_parameters = inject_signature.parameters
    assert {"self", "content", "role"} <= set(inject_parameters)
    session_parameter = inject_parameters.get("session_key")
    supports_target = session_parameter is not None or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in inject_parameters.values()
    )
    assert supports_target, inject_signature

    missing_target = {"delivery": {"adapter": "hermes_session"}}
    try:
        resolve_config(missing_target)
    except ConfigError as exc:
        assert str(exc) == "delivery.target is required for hermes_session"
    else:
        raise AssertionError("hermes_session without a target was accepted")

    target = "synthetic-session"
    calls: list[dict[str, object]] = []

    with _wake_home(allow_gateway_injection=False):
        context, manager = _wake_context()

        def grant_injector(**kwargs: object) -> bool:
            calls.append(kwargs)
            return True

        manager.set_gateway_message_injector(object(), grant_injector)
        assert manager.has_gateway_message_injector is True
        assert context.inject_message("no target", role="system") is False
        assert calls == []
        result = _wake_result(HermesSessionWakeSink(context, session_key=target))
        assert result.ok is False
        assert result.status == "rejected"
        assert calls == []

    calls.clear()
    with _wake_home(allow_gateway_injection=True):
        context, manager = _wake_context()
        result = _wake_result(HermesSessionWakeSink(context, session_key=target))
        assert manager.has_gateway_message_injector is False
        assert result.ok is False
        assert result.status == "rejected"
        assert calls == []

    calls.clear()
    with _wake_home(allow_gateway_injection=True):
        context, manager = _wake_context()

        def rejected_injector(**kwargs: object) -> bool:
            calls.append(kwargs)
            return False

        manager.set_gateway_message_injector(object(), rejected_injector)
        result = _wake_result(HermesSessionWakeSink(context, session_key=target))
        assert result.ok is False
        assert result.status == "rejected"
        assert len(calls) == 1

    calls.clear()
    with _wake_home(allow_gateway_injection=True):
        context, manager = _wake_context()

        def accepting_injector(**kwargs: object) -> bool:
            calls.append(kwargs)
            return True

        manager.set_gateway_message_injector(object(), accepting_injector)
        result = _wake_result(HermesSessionWakeSink(context, session_key=target))
        assert result.ok is True
        assert result.status == "queued_unverified"
        assert result.verified is False
        assert len(calls) == 1
        call = calls[0]
        assert call["session_key"] == target
        assert call["plugin_id"] == "moonbite"
        content = str(call["content"])
        assert content.startswith("[system] ")
        assert json.loads(content.removeprefix("[system] ")) == {
            "candidate_id": "contract-wake",
            "event_type": "moonbite_heartbeat_wake",
            "kind": "care_poke",
            "schema_version": "moon.wake_packet.v1",
        }
        assert call.get("role") is None

    manager = PluginManager()
    public_cli_attach_methods = sorted(
        name
        for name in dir(manager)
        if not name.startswith("_")
        and ("cli" in name.lower() or "attach" in name.lower())
    )
    print(
        json.dumps(
            {
                "inject_message_signature": str(inject_signature),
                "gateway_cases": [
                    "missing_target",
                    "missing_grant",
                    "no_gateway_injector",
                    "gateway_reject",
                    "gateway_accept_queued_unverified",
                ],
                "public_cli_attach_methods": public_cli_attach_methods,
                "cli_targeting": (
                    "unavailable_via_public_manager_api"
                    if not public_cli_attach_methods
                    else "requires_host_public_attach_contract"
                ),
            },
            sort_keys=True,
        )
    )


def _loaded_example_contract(loaded_plugin: object) -> None:
    """Prove the runnable example consumes the loader's own module tree."""

    host_example = import_module("examples.heartbeat_host")
    package_name, heartbeat_module, effects_module, plugin_module = (
        host_example._implementation_modules(loaded_plugin)
    )
    assert package_name in {
        loaded_plugin.__name__,
        f"{loaded_plugin.__name__}.moonbite_plugin",
    }
    register = getattr(loaded_plugin, "register")
    assert plugin_module.register is register
    assert heartbeat_module.EffectResult.__module__ == f"{package_name}.heartbeat"
    assert effects_module.EffectReceipt.__module__ == f"{package_name}.effects"

    submitted: list[object] = []

    class ContractJudge:
        calls = 0

        def decide(self, _candidate: object) -> object:
            self.calls += 1
            return heartbeat_module.JudgeDecision(True, False, "loader example")

    def submit_wake(_candidate: object, _decision: object, intent: object) -> bool:
        submitted.append(intent)
        return True

    with tempfile.TemporaryDirectory(prefix="moonbite-example-") as temporary:
        state_root = Path(temporary)
        ctx = host_example.SyntheticHostContext(
            host_example._example_config(state_root)
        )
        judge = ContractJudge()
        runtime = host_example.register_with_host_wake(
            ctx,
            loaded_plugin,
            submit_wake,
            heartbeat_judge=judge,
        )
        result = runtime.run_heartbeat(
            "care_poke",
            context={
                "events": ["loader-example-event"],
                "due": True,
                "source_event_id": "loader-example-source",
            },
        )
        assert result.wake is not None
        assert type(result.wake) is heartbeat_module.EffectResult
        assert result.wake.status == "queued_unverified"
        assert result.wake.verified is False
        assert judge.calls == 1
        assert len(submitted) == 1
        intent = submitted[0]
        receipt = effects_module.EffectReceipt(
            receipt_id=f"loader-example-receipt:{intent.effect_id}",
            event_id=intent.source_event_id,
            observed_at=intent.created_at,
            content_sha256=intent.content_sha256,
            content_length=intent.content_length,
            epoch_id=intent.epoch_id,
        )
        settled = runtime.heartbeat.reconcile_heartbeat_wake(intent.effect_id, receipt)
        assert settled.status == "verified"
        assert type(settled.receipt) is effects_module.EffectReceipt


def _write_loader_home(home: Path, config: dict) -> None:
    plugins = home / "plugins"
    bundled = home / "bundled"
    plugins.mkdir(parents=True)
    bundled.mkdir()
    (plugins / "moonbite").symlink_to(ROOT, target_is_directory=True)
    # An editable install can leave entry-point metadata in ROOT. Keep only
    # importable source on PYTHONPATH so it cannot shadow directory discovery.
    imports = home / "imports"
    imports.mkdir()
    for name in ("moonbite_plugin", "examples"):
        (imports / name).symlink_to(ROOT / name, target_is_directory=True)
    host_config = {
        "plugins": {
            "enabled": ["moonbite"],
            "entries": {
                "moonbite": {
                    "allow_gateway_injection": False,
                    "settings": {"config": config},
                }
            },
        }
    }
    (home / "config.yaml").write_text(
        yaml.safe_dump(host_config, sort_keys=False), encoding="utf-8"
    )


def _loader_probe() -> None:
    manager = PluginManager()
    manager.discover_and_load()
    row = next(item for item in manager.list_plugins() if item["name"] == "moonbite")
    assert row["enabled"] is True
    assert row["kind"] == "standalone"
    assert row["tools"] == len(TOOL_NAMES)
    assert row["hooks"] == len(HOOK_ORDER)
    assert row["commands"] == 1
    assert row["error"] is None
    loaded_namespace = sys.modules.get("hermes_plugins.moonbite")
    assert loaded_namespace is not None, (
        "directory plugin was shadowed by an entry point"
    )
    register = getattr(loaded_namespace, "register", None)
    assert callable(register)
    assert register.__module__ == "hermes_plugins.moonbite.moonbite_plugin.plugin"
    register_source = inspect.getsourcefile(register)
    assert register_source is not None
    assert ROOT in Path(register_source).resolve().parents
    _loaded_example_contract(loaded_namespace)
    assert not (Path(os.environ["HERMES_HOME"]) / "moonbite").exists()
    print(
        json.dumps(
            {
                "tools": len(TOOL_NAMES),
                "hooks": len(HOOK_ORDER),
                "register_module": register.__module__,
                "register_source_matches_repo": True,
                "loaded_namespace": "hermes_plugins.moonbite",
                "loaded_example_flow": "queued_unverified_to_verified",
            }
        )
    )


def _loader_contract() -> None:
    for preset in PRESETS:
        config = yaml.safe_load(
            (ROOT / "config" / "presets" / f"{preset}.yaml").read_text(encoding="utf-8")
        )
        normalize_config(config)
        with tempfile.TemporaryDirectory(prefix=f"moonbite-{preset}-") as temporary:
            home = Path(temporary)
            _write_loader_home(home, config)
            env = {
                **os.environ,
                "HERMES_HOME": str(home),
                "HERMES_BUNDLED_PLUGINS": str(home / "bundled"),
                "HERMES_ENABLE_PROJECT_PLUGINS": "0",
                "PYTHONPATH": os.pathsep.join(
                    (
                        str(home / "imports"),
                        str(Path(inspect.getfile(PluginContext)).resolve().parents[1]),
                    )
                ),
            }
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--loader-probe"],
                cwd=home,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            assert completed.returncode == 0, (preset, completed.stderr)
            cli = subprocess.run(
                [str(Path(sys.executable).with_name("hermes")), "moonbite", "doctor"],
                cwd=home,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            assert cli.returncode == 0, (preset, cli.stderr)
            assert json.loads(cli.stdout)["ok"] is True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loader-probe", action="store_true")
    args = parser.parse_args()
    if args.loader_probe:
        _loader_probe()
        return 0
    _turn_exit_contract()
    _metadata_contract()
    _wake_contract()
    _loader_contract()
    print(
        f"Hermes public API contract: manifest {len(TOOL_NAMES)} tools/"
        f"{len(HOOK_ORDER)} hooks, standalone opt-in loader, four inert presets, "
        "CLI/slash registration, versions and docs aligned"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
