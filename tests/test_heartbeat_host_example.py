from __future__ import annotations

import argparse
import importlib
import json
import sys
import types
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pytest

from examples.heartbeat_host import (
    SyntheticHostContext,
    _implementation_modules,
    _example_config,
    main,
    register_with_host_wake,
)


def _cli_call(ctx: SyntheticHostContext, context: dict[str, object]):
    if ctx.cli is None:
        raise AssertionError("public register did not install CLI")
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    args = parser.parse_args(
        ["heartbeat", "care_poke", "--context", json.dumps(context)]
    )
    output = StringIO()
    with redirect_stdout(output):
        code = ctx.cli["handler_fn"](args)
    return code, json.loads(output.getvalue())


def _register(
    tmp_path: Path,
    loaded_plugin,
    heartbeat_module,
    submit_wake,
):
    ctx = SyntheticHostContext(_example_config(tmp_path))
    judge = type(
        "CountingWakeJudge",
        (),
        {
            "__init__": lambda self: setattr(self, "calls", 0),
            "decide": lambda self, _candidate: (
                setattr(self, "calls", self.calls + 1)
                or heartbeat_module.JudgeDecision(True, False, "synthetic wake")
            ),
        },
    )()
    runtime = register_with_host_wake(
        ctx,
        loaded_plugin,
        submit_wake,
        heartbeat_judge=judge,
    )
    return ctx, judge, runtime


def _receipt(effects_module, intent, *, event_id=None):
    return effects_module.EffectReceipt(
        receipt_id=f"synthetic-test-receipt:{intent.effect_id}",
        event_id=intent.source_event_id if event_id is None else event_id,
        observed_at=intent.created_at,
        content_sha256=intent.content_sha256,
        content_length=intent.content_length,
        epoch_id=intent.epoch_id,
    )


def test_canonical_public_register_cli_queue_receipt_and_replay(tmp_path):
    loaded_plugin = importlib.import_module("moonbite_plugin")
    package_name, heartbeat_module, effects_module, plugin_module = (
        _implementation_modules(loaded_plugin)
    )
    assert package_name == "moonbite_plugin"
    assert plugin_module.register is loaded_plugin.register
    assert effects_module.EffectReceipt.__module__ == "moonbite_plugin.effects"

    submitted = []

    def submit_wake(_candidate, _decision, intent):
        submitted.append(intent)
        return True

    ctx, judge, runtime = _register(
        tmp_path, loaded_plugin, heartbeat_module, submit_wake
    )
    context = {
        "events": ["synthetic-event"],
        "due": True,
        "source_event_id": "canonical-source",
    }

    code, pending = _cli_call(ctx, context)
    assert code == 0
    assert pending["status"] == "pending"
    assert pending["wake"]["status"] == "queued_unverified"
    assert pending["wake"]["verified"] is False
    assert pending["wake"]["receipt"] is None
    assert judge.calls == 1
    assert len(submitted) == 1

    intent = submitted[0]
    record = runtime.effect_ledger.get(intent.effect_id)
    assert record is not None
    assert record.state == "executed_unverified"
    with pytest.raises(ValueError, match="receipt mismatch"):
        runtime.heartbeat.reconcile_heartbeat_wake(
            intent.effect_id,
            _receipt(effects_module, intent, event_id="wrong-synthetic-source"),
        )
    assert runtime.effect_ledger.get(intent.effect_id).state == ("executed_unverified")

    settled = runtime.heartbeat.reconcile_heartbeat_wake(
        intent.effect_id, _receipt(effects_module, intent)
    )
    assert settled.status == "verified"
    assert settled.verified is True
    assert type(settled.receipt) is effects_module.EffectReceipt

    code, completed = _cli_call(ctx, context)
    assert code == 0
    assert completed["status"] == "completed"
    assert completed["wake"] is None
    assert judge.calls == 1
    assert len(submitted) == 1


def test_host_queue_rejection_is_unverified_and_has_no_receipt(tmp_path):
    loaded_plugin = importlib.import_module("moonbite_plugin")
    _package_name, heartbeat_module, _effects_module, _plugin_module = (
        _implementation_modules(loaded_plugin)
    )
    submitted = []

    def submit_wake(_candidate, _decision, intent):
        submitted.append(intent)
        return False

    ctx, judge, runtime = _register(
        tmp_path, loaded_plugin, heartbeat_module, submit_wake
    )
    code, result = _cli_call(
        ctx,
        {
            "events": ["synthetic-event"],
            "due": True,
            "source_event_id": "rejected-source",
        },
    )

    assert code == 1
    assert result["status"] == "failed"
    assert result["wake"]["status"] == "adapter_rejected"
    assert result["wake"]["verified"] is False
    assert result["wake"]["receipt"] is None
    assert runtime.effect_ledger.get(submitted[0].effect_id).state == "failed"
    assert judge.calls == 1
    assert len(submitted) == 1


def test_runnable_demo_reports_actual_pending_to_completed_flow(tmp_path, capsys):
    main(tmp_path / "demo-state")

    output = capsys.readouterr().out
    assert "synthetic host wake: pending -> completed" in output
    assert "synthetic judge calls: 1; host submissions: 1" in output


def test_directory_namespace_loader_reuses_its_own_effect_types(
    tmp_path, monkeypatch, request
):
    existing_modules = set(sys.modules)

    def remove_example_modules():
        for name in set(sys.modules) - existing_modules:
            if name.startswith("hermes_plugins."):
                sys.modules.pop(name, None)

    request.addfinalizer(remove_example_modules)
    canonical = importlib.import_module("moonbite_plugin")
    canonical_heartbeat = importlib.import_module("moonbite_plugin.heartbeat")
    canonical_effects = importlib.import_module("moonbite_plugin.effects")
    source_package = Path(canonical.__file__).parent
    package_name = "hermes_plugins.moonbite.moonbite_plugin"
    for parent_name in ("hermes_plugins", "hermes_plugins.moonbite"):
        parent = types.ModuleType(parent_name)
        parent.__path__ = []
        parent.__package__ = parent_name
        parent.__spec__ = importlib.machinery.ModuleSpec(
            parent_name, loader=None, is_package=True
        )
        monkeypatch.setitem(sys.modules, parent_name, parent)
    package_spec = importlib.util.spec_from_file_location(
        package_name,
        source_package / "__init__.py",
        submodule_search_locations=[str(source_package)],
    )
    assert package_spec is not None and package_spec.loader is not None
    package_module = importlib.util.module_from_spec(package_spec)
    monkeypatch.setitem(sys.modules, package_name, package_module)
    package_spec.loader.exec_module(package_module)

    loaded_plugin = importlib.import_module(package_name)
    loaded_package_name, heartbeat_module, effects_module, plugin_module = (
        _implementation_modules(loaded_plugin)
    )
    assert loaded_package_name == package_name
    assert plugin_module.register is loaded_plugin.register
    assert heartbeat_module.EffectResult is not canonical_heartbeat.EffectResult
    assert effects_module.EffectReceipt is not canonical_effects.EffectReceipt

    submitted = []

    def submit_wake(_candidate, _decision, intent):
        submitted.append(intent)
        return True

    ctx, _judge, runtime = _register(
        tmp_path / "state", loaded_plugin, heartbeat_module, submit_wake
    )
    result = runtime.run_heartbeat(
        "care_poke",
        context={
            "events": ["namespace-event"],
            "due": True,
            "source_event_id": "namespace-source",
        },
    )
    assert type(result.wake) is heartbeat_module.EffectResult
    assert result.wake.status == "queued_unverified"
    assert ctx.cli is not None
    intent = submitted[0]
    settled = runtime.heartbeat.reconcile_heartbeat_wake(
        intent.effect_id, _receipt(effects_module, intent)
    )
    assert type(settled.receipt) is effects_module.EffectReceipt
    assert settled.status == "verified"
