"""Verify Moonbite against public Hermes plugin APIs and its real loader."""

from __future__ import annotations

import argparse
import inspect
import json
import os
from datetime import UTC, datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

import yaml
from hermes_cli.hooks import _DEFAULT_PAYLOADS
from hermes_cli.plugins import PluginContext, VALID_HOOKS

from moonbite_plugin.config import normalize_config
from moonbite_plugin.hermes_adapter import HermesHostAdapter
from moonbite_plugin.plugin import TOOL_NAMES
from moonbite_plugin.session import HOOK_ORDER
from moonbite_plugin.session import SessionLifecycleStore


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
                "host_turn_completed_without_post",
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


def _write_loader_home(home: Path, config: dict) -> None:
    plugins = home / "plugins"
    bundled = home / "bundled"
    plugins.mkdir(parents=True)
    bundled.mkdir()
    (plugins / "moonbite").symlink_to(ROOT, target_is_directory=True)
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
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    manager.discover_and_load()
    row = next(item for item in manager.list_plugins() if item["name"] == "moonbite")
    assert row["enabled"] is True
    assert row["kind"] == "standalone"
    assert row["tools"] == len(TOOL_NAMES)
    assert row["hooks"] == len(HOOK_ORDER)
    assert row["commands"] == 1
    assert row["error"] is None
    assert not (Path(os.environ["HERMES_HOME"]) / "moonbite").exists()
    print(json.dumps({"tools": len(TOOL_NAMES), "hooks": len(HOOK_ORDER)}))


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
    _loader_contract()
    print(
        f"Hermes public API contract: manifest {len(TOOL_NAMES)} tools/"
        f"{len(HOOK_ORDER)} hooks, standalone opt-in loader, four inert presets, "
        "CLI/slash registration, versions and docs aligned"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
