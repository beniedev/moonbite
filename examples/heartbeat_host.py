"""Minimal synthetic host composition for Moonbite Heartbeat wakes.

The host owns submission and completion evidence. Moonbite only receives
acceptance at enqueue time. This offline demo simulates later completion;
a real host must wait for actual completion evidence before issuing a receipt.
"""

from __future__ import annotations

import argparse
import importlib
import json
import tempfile
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any


class SyntheticHostContext:
    """Small Hermes-shaped context used by the runnable synthetic example."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.llm = object()
        self.cli: dict[str, Any] | None = None
        self.slash: dict[str, Any] | None = None
        self.tools: dict[str, Any] = {}
        self.hooks: dict[str, Any] = {}
        self.auxiliary_tasks: dict[str, Any] = {}

    def get_config(self, key: str, default: Any = None) -> Any:
        if key == "config":
            return self.config
        if key == "scenario_pack":
            return None
        return default

    def register_cli_command(self, **kwargs: Any) -> None:
        self.cli = kwargs

    def register_command(
        self, name: str, handler: Any, description: str = "", args_hint: str = ""
    ) -> None:
        self.slash = {
            "name": name,
            "handler": handler,
            "description": description,
            "args_hint": args_hint,
        }

    def register_tool(self, **kwargs: Any) -> None:
        self.tools[kwargs["name"]] = kwargs

    def register_hook(self, name: str, handler: Any) -> None:
        self.hooks[name] = handler

    def register_auxiliary_task(self, key: str, **kwargs: Any) -> None:
        self.auxiliary_tasks[key] = kwargs


def _implementation_modules(loaded_plugin: Any) -> tuple[str, Any, Any, Any]:
    """Load all host-facing types from the package owning ``register``."""

    register = getattr(loaded_plugin, "register", None)
    register_module_name = getattr(register, "__module__", None)
    if not callable(register) or not isinstance(register_module_name, str):
        raise TypeError("loaded_plugin must expose a public register function")
    package_name, separator, _module_name = register_module_name.rpartition(".")
    if not separator or not package_name:
        raise TypeError("register.__module__ must identify an implementation package")

    heartbeat_module = importlib.import_module(f"{package_name}.heartbeat")
    effects_module = importlib.import_module(f"{package_name}.effects")
    plugin_module = importlib.import_module(f"{package_name}.plugin")
    package_register = getattr(plugin_module, "register", None)
    if (
        not callable(package_register)
        or getattr(package_register, "__module__", None) != register_module_name
    ):
        raise ImportError(
            "loaded plugin register does not match its implementation tree"
        )
    return package_name, heartbeat_module, effects_module, plugin_module


class _HostWakeSink:
    """Translate host queue acceptance into an unverified effect result."""

    def __init__(self, heartbeat_module: Any, effects_module: Any, submit_wake: Any):
        self._effect_result = heartbeat_module.EffectResult
        self._receipt = effects_module.EffectReceipt
        self._submit_wake = submit_wake

    def wake(self, candidate: Any, decision: Any, intent: Any = None) -> Any:
        accepted = self._submit_wake(candidate, decision, intent)
        if isinstance(accepted, self._receipt):
            raise TypeError(
                "submit_wake returns acceptance; receipt settlement is separate"
            )
        if type(accepted) is not bool:
            raise TypeError("submit_wake must return bool")
        return self._effect_result(
            accepted,
            "queued_unverified" if accepted else "rejected",
            verified=False,
        )

    def deliver(self, _candidate: Any, _decision: Any, _intent: Any = None) -> Any:
        return self._effect_result(False, "rejected", verified=False)


def register_with_host_wake(
    ctx: Any,
    loaded_plugin: Any,
    submit_wake: Any,
    **registration_options: Any,
) -> Any:
    """Register one loaded Moonbite plugin with a host-owned wake queue.

    ``submit_wake`` must return a strict ``bool``. It does not create a
    receipt. After verified host completion, translate real evidence into a
    receipt from the loaded plugin's types and reconcile the returned runtime.
    Only the offline demo below manufactures synthetic completion evidence.
    """

    if "wake_sink" in registration_options:
        raise TypeError("register_with_host_wake owns wake_sink")
    _package_name, heartbeat_module, effects_module, _plugin_module = (
        _implementation_modules(loaded_plugin)
    )
    sink = _HostWakeSink(heartbeat_module, effects_module, submit_wake)
    return loaded_plugin.register(ctx, wake_sink=sink, **registration_options)


def _example_config(state_dir: Path) -> dict[str, Any]:
    return {
        "modules": {"heartbeat": True},
        "state": {"directory": str(state_dir)},
        "heartbeat": {
            "kinds": {
                "care_poke": {
                    "enabled": True,
                    "profile": "routine",
                    "judge": "required",
                    "host_only": True,
                    "bypass": [],
                }
            }
        },
    }


def main(state_dir: str | Path | None = None) -> None:
    """Run the synthetic host flow and print its actual CLI terminal states."""

    state_root = (
        Path(state_dir)
        if state_dir is not None
        else Path(tempfile.mkdtemp(prefix="moonbite-host-example-"))
    )
    state_root.mkdir(parents=True, exist_ok=True)

    loaded_plugin = importlib.import_module("moonbite_plugin")
    _package_name, heartbeat_module, effects_module, _plugin_module = (
        _implementation_modules(loaded_plugin)
    )

    class SyntheticWakeJudge:
        def __init__(self):
            self.calls = 0

        def decide(self, _candidate: Any) -> Any:
            self.calls += 1
            return heartbeat_module.JudgeDecision(True, False, "synthetic wake")

    submitted: list[Any] = []

    def submit_wake(_candidate: Any, _decision: Any, intent: Any) -> bool:
        submitted.append(intent)
        return True

    ctx = SyntheticHostContext(_example_config(state_root))
    judge = SyntheticWakeJudge()
    runtime = register_with_host_wake(
        ctx,
        loaded_plugin,
        submit_wake,
        heartbeat_judge=judge,
    )
    if ctx.cli is None:
        raise RuntimeError("public register did not install the CLI surface")

    parser = argparse.ArgumentParser(prog="synthetic-hermes-moonbite")
    ctx.cli["setup_fn"](parser)
    cli_args = parser.parse_args(
        [
            "heartbeat",
            "care_poke",
            "--context",
            json.dumps(
                {
                    "events": ["synthetic-event"],
                    "due": True,
                    "source_event_id": "synthetic-source",
                }
            ),
        ]
    )

    def run_cli() -> tuple[int, dict[str, Any]]:
        output = StringIO()
        with redirect_stdout(output):
            code = ctx.cli["handler_fn"](cli_args)
        return code, json.loads(output.getvalue())

    first_code, first = run_cli()
    if first_code != 0 or first.get("status") != "pending" or len(submitted) != 1:
        raise RuntimeError(f"synthetic enqueue did not remain pending: {first!r}")

    intent = submitted[0]
    # Simulate completion only for this offline demonstration. Acceptance
    # alone must never produce a receipt in a real host integration.
    receipt = effects_module.EffectReceipt(
        receipt_id=f"synthetic-host-receipt:{intent.effect_id}",
        event_id=intent.source_event_id,
        observed_at=intent.created_at,
        content_sha256=intent.content_sha256,
        content_length=intent.content_length,
        epoch_id=intent.epoch_id,
    )
    settled = runtime.heartbeat.reconcile_heartbeat_wake(intent.effect_id, receipt)
    if settled.status != "verified":
        raise RuntimeError(f"synthetic receipt did not verify: {settled!r}")

    second_code, second = run_cli()
    if second_code != 0:
        raise RuntimeError(f"synthetic terminal replay failed: {second!r}")
    print(f"synthetic host wake: {first['status']} -> {second['status']}")
    print(f"synthetic judge calls: {judge.calls}; host submissions: {len(submitted)}")


if __name__ == "__main__":
    main()
