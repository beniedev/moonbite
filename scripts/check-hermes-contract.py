"""Verify Moonbite against public Hermes plugin APIs and its real loader."""

from __future__ import annotations

import argparse
import inspect
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib

import yaml
from hermes_cli.plugins import PluginContext

from moonbite_plugin.config import normalize_config
from moonbite_plugin.plugin import TOOL_NAMES
from moonbite_plugin.session import HOOK_ORDER
from moonbite_plugin.session import SessionContext, SessionLifecycleStore


ROOT = Path(__file__).parents[1]
PRESETS = ("core-only", "panel-only", "memory-only", "full-companion")


def _session_context(
    source_id: str,
    *,
    turn_id: str | None = None,
    source_kind: str = "system",
    observed_at: datetime,
) -> SessionContext:
    return SessionContext(
        session_id="contract-session",
        lifecycle_id="contract-session",
        source_id=source_id,
        turn_id=turn_id,
        source_kind=source_kind,
        observed_at=observed_at,
        fresh=False,
        supported_hooks=frozenset({"pre_llm_call", "post_llm_call"}),
    )


def _turn_exit_contract() -> None:
    """Exercise host call sequences without depending on a Hermes checkout."""

    now = datetime(2026, 8, 24, 19, 0, tzinfo=UTC)
    with tempfile.TemporaryDirectory(prefix="moonbite-session-contract-") as root:
        normal = SessionLifecycleStore(Path(root) / "normal")
        normal.record_hook(
            _session_context("pre-normal", turn_id="turn-normal", observed_at=now),
            "pre_llm_call",
        )
        normal.record_hook(
            _session_context(
                "post-normal",
                turn_id="turn-normal",
                source_kind="assistant_response",
                observed_at=now + timedelta(seconds=1),
            ),
            "post_llm_call",
            settled=True,
        )
        assert normal.snapshot("contract-session").settled_turn_ids == ("turn-normal",)

        for exit_reason in ("interrupted", "empty-final-response"):
            store = SessionLifecycleStore(Path(root) / exit_reason)
            store.record_hook(
                _session_context(
                    f"pre-{exit_reason}",
                    turn_id="turn-abandoned",
                    observed_at=now,
                ),
                "pre_llm_call",
            )
            # Hermes may omit post_llm_call for this host exit.  A successor
            # pre must still recover the old ownership without a fake post.
            successor = store.record_hook(
                _session_context(
                    f"pre-successor-{exit_reason}",
                    turn_id="turn-successor",
                    observed_at=now + timedelta(seconds=1),
                ),
                "pre_llm_call",
            )
            assert successor.snapshot.abandoned_turn_ids == ("turn-abandoned",)
            assert successor.snapshot.open_turn_id == "turn-successor"
            assert successor.snapshot.settled_turn_ids == ()


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
