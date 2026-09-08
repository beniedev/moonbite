"""Receipt ordering across processes through the registered host boundary."""

from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal

import pytest

import moonbite_plugin
from moonbite_plugin.heartbeat import EffectResult, JudgeDecision
from moonbite_plugin.plugin import register

from test_cli_result_semantics import (
    FixedHeartbeatJudge,
    HostContext,
    _cli,
    _config,
    _receipt_for_intent,
    _run_cli,
)


class NoReplaySink:
    def deliver(self, *_args, **_kwargs):
        raise AssertionError("reconciliation called delivery again")

    def wake(self, *_args, **_kwargs):
        raise AssertionError("reconciliation called wake again")


def registered(root, sink):
    ctx = HostContext(_config(root, heartbeat=True))
    runtime = register(
        ctx,
        heartbeat_judge=FixedHeartbeatJudge(
            JudgeDecision(
                True,
                True,
                "contact",
                "synthetic instruction",
                delivery_mode="delegated",
            )
        ),
        wake_sink=sink,
    )
    return ctx, runtime, _cli(ctx)


class CrossProcessReceiptSink:
    def __init__(self, root, wake_outcome):
        self.root = root
        self.wake_outcome = wake_outcome
        self.deliveries = 0
        self.wakes = 0
        self.child_result = None

    def deliver(self, _candidate, _decision, intent=None):
        self.deliveries += 1
        assert intent is not None
        reader, writer = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(reader)
            try:
                # A new registered runtime reads the shared durable state;
                # it does not reuse the parent's runtime object.
                _, runtime, _ = registered(self.root, NoReplaySink())
                result = runtime.reconcile_heartbeat_delivery(
                    intent.effect_id,
                    "verified",
                    _receipt_for_intent(intent, prefix="child-delivery"),
                )
                terminals = [
                    event
                    for event in runtime.bus.read_audit()
                    if event.payload.get("occurrence_id") == intent.source_event_id
                    and event.payload.get("terminal") is not None
                ]
                proof = {
                    "verified": result.verified,
                    "terminal_count": len(terminals),
                    "package_source": str(Path(moonbite_plugin.__file__).resolve()),
                }
                os.write(writer, json.dumps(proof).encode())
                os.close(writer)
                os._exit(0)
            except BaseException as exc:
                os.write(
                    writer, json.dumps({"error_class": type(exc).__name__}).encode()
                )
                os.close(writer)
                os._exit(7)
        os.close(writer)
        try:
            ready, _, _ = select.select([reader], [], [], 10)
            if not ready:
                os.kill(pid, signal.SIGKILL)
            raw = os.read(reader, 8192) if ready else b""
        finally:
            os.close(reader)
            _, status = os.waitpid(pid, 0)
        self.child_result = json.loads(raw) if raw else {"error_class": "ChildTimeout"}
        assert os.waitstatus_to_exitcode(status) == 0, self.child_result
        assert self.child_result["verified"] is True
        return EffectResult(True, "queued")

    def wake(self, _candidate, _decision, intent=None):
        self.wakes += 1
        assert intent is not None
        if self.wake_outcome == "failed":
            return EffectResult(False, "synthetic_failure")
        return EffectResult(True, "queued")


@pytest.mark.parametrize("epoch", [None, "explicit-epoch"])
@pytest.mark.parametrize("wake_outcome", ["pending", "failed"])
def test_registered_cross_process_receipt_waits_for_complete_occurrence(
    tmp_path, capsys, epoch, wake_outcome
):
    sink = CrossProcessReceiptSink(tmp_path, wake_outcome)
    ctx, _runtime, parser = registered(tmp_path, sink)
    context = {"source_event_id": "cross-process", "events": ["synthetic"], "due": True}
    if epoch is not None:
        context["epoch_id"] = epoch
    argv = ["heartbeat", "care_poke", "--context", json.dumps(context)]
    code, initial = _run_cli(ctx, parser, argv, capsys)

    assert sink.child_result is not None
    assert "error_class" not in sink.child_result, sink.child_result
    assert sink.child_result["package_source"] == str(
        Path(moonbite_plugin.__file__).resolve()
    )
    assert sink.child_result["terminal_count"] == 0
    assert initial["status"] == wake_outcome
    assert code == (1 if wake_outcome == "failed" else 0)
    assert (sink.deliveries, sink.wakes) == (1, 1)

    replay_ctx, restarted, replay_parser = registered(tmp_path, NoReplaySink())
    replay_code, replay = _run_cli(replay_ctx, replay_parser, argv, capsys)
    assert (replay_code, replay["status"]) == (code, wake_outcome)
    wake_id = initial["wake"]["effect_id"]
    if wake_outcome == "pending":
        wake = restarted.effects.get(wake_id)
        assert wake is not None
        receipt = _receipt_for_intent(wake, prefix="completed-wake")
        assert restarted.reconcile_heartbeat_wake(wake_id, receipt).verified
        # Repeated formal evidence is idempotent even after reassembly.
        assert restarted.reconcile_heartbeat_wake(wake_id, receipt).verified
        final_code, final = _run_cli(replay_ctx, replay_parser, argv, capsys)
        assert (final_code, final["status"]) == (0, "completed")
    else:
        assert restarted.effects.get(wake_id).state == "failed"

    terminals = [
        event
        for event in restarted.bus.read_audit()
        if event.payload.get("occurrence_id") == "cross-process"
        and event.payload.get("terminal") is not None
    ]
    assert len(terminals) == 1
    if epoch is None:
        assert "epoch_id" not in terminals[0].payload
    else:
        assert terminals[0].payload["epoch_id"] == epoch
    assert set(terminals[0].payload["effect_ids"]) == {
        initial["delivery"]["effect_id"],
        wake_id,
    }
