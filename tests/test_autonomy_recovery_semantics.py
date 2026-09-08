from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from moonbite_plugin.autonomy import (
    ActivityProvider,
    AllowAutonomyJudge,
    AutonomyDecision,
    AutonomyEngine,
    AutonomyExecutionRequest,
    ProviderRegistry,
)
from moonbite_plugin.control import ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.plugin import register
from moonbite_plugin.runtime_core import EventBus


NOW = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)


class HostContext:
    """Minimal public host surface used by the real registration path."""

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


class FixedAutonomyJudge:
    def __init__(self, decision):
        self.decision = decision

    def decide(self, _context):
        return self.decision


def _config(tmp_path: Path):
    return {
        "state": {"directory": str(tmp_path / "state")},
        "modules": {"autonomy": True},
        "autonomy": {"providers": {"fixture": {"enabled": True, "weight": 1}}},
    }


def _parser(ctx: HostContext):
    parser = argparse.ArgumentParser()
    ctx.cli["setup_fn"](parser)
    return parser


def _run_cli(ctx: HostContext, parser, argv, capsys):
    code = ctx.cli["handler_fn"](parser.parse_args(argv))
    return code, json.loads(capsys.readouterr().out)


def _receipt(request: AutonomyExecutionRequest, *, receipt_id="receipt-1"):
    return EffectReceipt(
        receipt_id=receipt_id,
        event_id=request.source_event_id,
        observed_at=request.context.now,
        content_sha256=request.content_sha256,
        content_length=request.content_length,
        epoch_id=request.epoch_id,
    )


def _engine(tmp_path, providers, *, bus=None, judge=None, clock=lambda: NOW):
    registry = ProviderRegistry()
    for provider in providers:
        registry.register(provider)
    controls = ControlStore(tmp_path, clock=clock)
    ledger = EffectLedger(tmp_path, clock=clock)
    engine = AutonomyEngine(
        bus=bus or EventBus(tmp_path, clock=clock),
        controls=controls,
        registry=registry,
        judge=judge or AllowAutonomyJudge(),
        rng=random.Random(0),
        clock=clock,
        effect_ledger=ledger,
    )
    return engine, controls, ledger


class CrashAfterIntentBus(EventBus):
    def emit(self, *_args, **_kwargs):
        raise SystemExit("synthetic crash after begin_intent")


@pytest.mark.parametrize(
    "block",
    ("enabled", "source", "channel", "budget", "provider"),
)
def test_begin_intent_recovery_rechecks_current_provider_authority(tmp_path, block):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        bus=CrashAfterIntentBus(tmp_path),
    )
    facts = {
        "source_event_id": "intent-recovery",
        "epoch_id": "epoch-1",
        "source": "scheduler",
        "channel": "private",
        "cost_budget_remaining": 1,
    }
    with pytest.raises(SystemExit, match="synthetic crash"):
        first.run_once({"chosen": {"enabled": True, "weight": 1}}, facts=facts)
    intent = ledger.records()[0]
    assert intent.state == "intent"

    current = {"chosen": {"enabled": True, "weight": 1}}
    if block == "enabled":
        current["chosen"]["enabled"] = False
    elif block == "source":
        current["chosen"]["allowed_sources"] = {"other"}
    elif block == "channel":
        current["chosen"]["allowed_channels"] = {"other"}
    elif block == "budget":
        current["chosen"].update({"cost": 2, "cost_budget": 2})
        facts["cost_budget_remaining"] = 1
    else:
        current["chosen"]["enabled"] = True

    eligible = block != "provider"
    recovery_provider = ActivityProvider(
        "chosen", provider, eligible=lambda _context: eligible
    )

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("recovery must keep the persisted selection")

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [recovery_provider],
        judge=RejectUnexpectedJudge(),
    )
    result = recovered.run_once(current, facts=facts)

    assert (result.status, result.reason, calls) == (
        "skipped",
        "no_eligible_provider",
        [],
    )
    assert recovered.effect_ledger.get(intent.effect_id).state == "intent"


def test_begin_intent_recovery_keeps_selection_and_excludes_own_limits(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [
            ActivityProvider(
                "alpha",
                provider,
                cooldown=3600,
                daily_limit=1,
                cost_budget=1,
            ),
            ActivityProvider("beta", provider),
        ],
        bus=CrashAfterIntentBus(tmp_path),
    )
    facts = {
        "source_event_id": "intent-recovery-eligible",
        "epoch_id": "epoch-1",
        "cost_budget_remaining": 1,
    }
    with pytest.raises(SystemExit, match="synthetic crash"):
        first.run_once(
            {
                "alpha": {
                    "enabled": True,
                    "weight": 1,
                    "cooldown": 3600,
                    "daily_limit": 1,
                    "cost": 1,
                    "cost_budget": 1,
                },
                "beta": {"enabled": False, "weight": 1},
            },
            facts=facts,
        )
    intent = ledger.records()[0]
    assert intent.state == "intent"

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("recovery must keep the persisted selection")

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [
            ActivityProvider(
                "alpha",
                provider,
                cooldown=3600,
                daily_limit=1,
                cost_budget=1,
            ),
            ActivityProvider("beta", provider),
        ],
        judge=RejectUnexpectedJudge(),
    )
    result = recovered.run_once(
        {
            "alpha": {
                "enabled": True,
                "weight": 1,
                "cooldown": 3600,
                "daily_limit": 1,
                "cost": 1,
                "cost_budget": 1,
            },
            "beta": {"enabled": False, "weight": 1},
        },
        facts=facts,
    )

    assert result.status == "completed"
    assert result.provider == "alpha"
    assert len(calls) == 1
    assert recovered.effect_ledger.get(intent.effect_id).state == "verified"


@pytest.mark.parametrize("initial_state", ("intent", "executed_unverified"))
def test_cross_day_implicit_retry_reuses_persisted_selection(tmp_path, initial_state):
    current = [NOW]
    calls = []

    def provider(request):
        calls.append(request)
        if initial_state == "executed_unverified":
            return "accepted-by-host-queue"
        return _receipt(request)

    first_bus = CrashAfterIntentBus(tmp_path) if initial_state == "intent" else None
    settings = {"chosen": {"enabled": True, "effect_ttl": 3 * 24 * 60 * 60}}
    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        bus=first_bus,
        clock=lambda: current[0],
    )
    facts = {"source_event_id": "cross-day-implicit"}
    if initial_state == "intent":
        with pytest.raises(SystemExit, match="synthetic crash"):
            first.run_once(settings, facts=facts)
        initial = ledger.records()[0]
    else:
        initial = first.run_once(settings, facts=facts).effect_record
    assert initial.state == initial_state
    current[0] = NOW + timedelta(days=1)

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("implicit retry must keep persisted selection")

    recovered_provider = ActivityProvider(
        "chosen",
        provider,
    )
    recovered, _controls, _ledger = _engine(
        tmp_path,
        [recovered_provider],
        judge=RejectUnexpectedJudge(),
        clock=lambda: current[0],
    )
    result = recovered.run_once(settings, facts=facts)

    if initial_state == "intent":
        assert (result.status, result.reason, result.effect_id) == (
            "awaiting_reconciliation",
            "implicit_identity_unavailable",
            None,
        )
        assert len(calls) == 0
        assert recovered.effect_ledger.get(initial.effect_id).state == "intent"
        assert not any(
            event.payload.get("occurrence_id") == "cross-day-implicit"
            and event.payload.get("terminal") is not None
            for event in recovered.bus.read_audit()
        )
    else:
        assert result.effect_id == initial.effect_id
        assert len(calls) == 1
        assert (result.status, result.reason) == (
            "awaiting_reconciliation",
            "awaiting_reconciliation",
        )
        assert recovered.effect_ledger.get(initial.effect_id).state == (
            "executed_unverified"
        )


def test_cross_day_implicit_retry_with_custom_idempotency_key_reuses_effect(
    tmp_path,
):
    current = [NOW]
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        clock=lambda: current[0],
    )
    initial = first.run_once(
        {"chosen": {"enabled": True}},
        facts={
            "source_event_id": "cross-day-custom-key",
            "idempotency_key": "legacy-custom-key",
        },
    )
    current[0] = NOW + timedelta(days=1)

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("implicit retry must keep persisted selection")

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("not called"))],
        judge=RejectUnexpectedJudge(),
        clock=lambda: current[0],
    )
    result = recovered.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "cross-day-custom-key"},
    )

    assert initial.status == "completed"
    assert (result.status, result.reason, result.effect_id) == (
        "completed",
        "already_verified",
        initial.effect_id,
    )
    assert ledger.records()[0].idempotency_key == "legacy-custom-key"
    assert len(calls) == 1


def test_explicit_effect_by_idempotency_requires_public_epoch_on_retry(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
    )
    initial = first.run_once(
        {"chosen": {"enabled": True}},
        facts={
            "source_event_id": "explicit-key-retry",
            "epoch_id": "explicit-public-epoch",
        },
    )
    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("not called"))],
    )

    result = recovered.run_once(
        {"chosen": {"enabled": True}},
        facts={
            "source_event_id": "explicit-key-retry",
            "idempotency_key": initial.idempotency_key,
        },
    )

    assert initial.status == "completed"
    assert (result.status, result.reason, result.effect_id) == (
        "awaiting_reconciliation",
        "implicit_identity_unavailable",
        None,
    )
    assert len(calls) == 1
    assert len(ledger.records()) == 1


def test_implicit_retry_fails_closed_on_unproven_epoch_collision(tmp_path):
    current = [NOW]
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        bus=CrashAfterIntentBus(tmp_path),
        clock=lambda: current[0],
    )
    explicit_facts = {
        "source_event_id": "cross-day-explicit",
        "epoch_id": f"autonomy:{NOW.date().isoformat()}",
    }
    with pytest.raises(SystemExit, match="synthetic crash"):
        first.run_once({"chosen": {"enabled": True}}, facts=explicit_facts)
    assert ledger.records()[0].epoch_id == explicit_facts["epoch_id"]
    current[0] = NOW + timedelta(days=1)

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        judge=AllowAutonomyJudge(),
        clock=lambda: current[0],
    )
    result = recovered.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "cross-day-explicit"},
    )

    assert (result.status, result.reason, result.effect_id) == (
        "awaiting_reconciliation",
        "implicit_identity_unavailable",
        None,
    )
    assert calls == []
    assert len(recovered.effect_ledger.records()) == 1
    assert not any(
        event.payload.get("occurrence_id") == "cross-day-explicit"
        and event.payload.get("terminal") is not None
        for event in recovered.bus.read_audit()
    )


@pytest.mark.parametrize(
    ("explicit_epoch", "retry_time"),
    (
        (f"autonomy:{NOW.date().isoformat()}", NOW),
        ("autonomy:2026-09-08", NOW + timedelta(days=1)),
    ),
)
def test_known_explicit_date_epoch_blocks_implicit_retry(
    tmp_path, explicit_epoch, retry_time
):
    current = [NOW]
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        clock=lambda: current[0],
    )
    explicit_facts = {
        "source_event_id": "known-explicit-collision",
        "epoch_id": explicit_epoch,
    }
    initial = first.run_once({"chosen": {"enabled": True}}, facts=explicit_facts)
    current[0] = retry_time
    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("not called"))],
        clock=lambda: current[0],
    )

    result = recovered.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "known-explicit-collision"},
    )

    assert initial.status == "completed"
    assert (result.status, result.reason, result.effect_id) == (
        "awaiting_reconciliation",
        "implicit_identity_unavailable",
        None,
    )
    assert len(calls) == 1
    assert len(ledger.records()) == 1
    assert not any(
        event.payload.get("occurrence_id") == "known-explicit-collision"
        and event.payload.get("terminal") is not None
        and event.payload.get("epoch_id") is None
        for event in recovered.bus.read_audit()
    )


def test_explicit_date_epoch_then_next_day_implicit_is_independent(
    tmp_path,
):
    current = [NOW]
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request, receipt_id=f"receipt-{len(calls)}")

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        clock=lambda: current[0],
    )
    day_one_facts = {
        "source_event_id": "date-epoch-independent",
        "epoch_id": f"autonomy:{NOW.date().isoformat()}",
    }
    first_result = first.run_once({"chosen": {"enabled": True}}, facts=day_one_facts)
    current[0] = NOW + timedelta(days=1)
    second_result = first.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "date-epoch-independent"},
    )

    assert first_result.status == second_result.status == "completed"
    assert first_result.effect_id != second_result.effect_id
    assert [record.epoch_id for record in ledger.records()] == [
        f"autonomy:{NOW.date().isoformat()}",
        f"autonomy:{current[0].date().isoformat()}",
    ]
    assert len(calls) == 2

    class RejectUnexpectedProvider:
        def __call__(self, _request):
            raise AssertionError("identity replay must not call provider")

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("identity replay must not invoke Judge")

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", RejectUnexpectedProvider())],
        judge=RejectUnexpectedJudge(),
        clock=lambda: current[0],
    )
    explicit_replay = recovered.run_once(
        {"chosen": {"enabled": True}}, facts=day_one_facts
    )
    implicit_replay = recovered.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "date-epoch-independent"},
    )

    assert (explicit_replay.status, explicit_replay.effect_id) == (
        "completed",
        first_result.effect_id,
    )
    assert (implicit_replay.status, implicit_replay.effect_id) == (
        "completed",
        second_result.effect_id,
    )
    assert len(calls) == 2


def test_known_implicit_date_epoch_blocks_explicit_retry(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
    )
    initial = first.run_once(
        {"chosen": {"enabled": True}},
        facts={"source_event_id": "known-implicit-collision"},
    )
    explicit_facts = {
        "source_event_id": "known-implicit-collision",
        "epoch_id": f"autonomy:{NOW.date().isoformat()}",
    }
    recovered, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("not called"))],
    )

    result = recovered.run_once({"chosen": {"enabled": True}}, facts=explicit_facts)

    assert initial.status == "completed"
    assert (result.status, result.reason, result.effect_id) == (
        "awaiting_reconciliation",
        "implicit_identity_unavailable",
        None,
    )
    assert len(calls) == 1
    assert len(ledger.records()) == 1


@pytest.mark.parametrize("block", ("pause", "active_chat", "provider"))
def test_revoked_intent_skip_is_nonterminal_and_resumes_same_selection(tmp_path, block):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        bus=CrashAfterIntentBus(tmp_path),
    )
    facts = {"source_event_id": "revoked-then-restored", "epoch_id": "epoch-1"}
    with pytest.raises(SystemExit, match="synthetic crash"):
        first.run_once({"chosen": {"enabled": True}}, facts=facts)
    intent = ledger.records()[0]
    if block == "pause":
        controls.put(feature="autonomy", mode="pause", source="operator")

    revoked, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
    )
    blocked_facts = {**facts, "active_chat": block == "active_chat"}
    blocked_settings = {"chosen": {"enabled": block != "provider"}}
    skipped = revoked.run_once(blocked_settings, facts=blocked_facts)
    expected_reason = {
        "pause": "controlled_by:operator",
        "active_chat": "active_chat",
        "provider": "no_eligible_provider",
    }[block]
    assert (skipped.status, skipped.reason) == ("skipped", expected_reason)
    assert revoked.effect_ledger.get(intent.effect_id).state == "intent"
    assert all("terminal" not in event.payload for event in revoked.bus.read_audit())
    if block == "pause":
        controls.clear(feature="autonomy", source="operator")

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("restored intent must keep persisted selection")

    restored, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        judge=RejectUnexpectedJudge(),
    )
    result = restored.run_once({"chosen": {"enabled": True}}, facts=facts)

    assert (result.status, result.provider, result.effect_id) == (
        "completed",
        "chosen",
        intent.effect_id,
    )
    assert len(calls) == 1
    assert restored.effect_ledger.get(intent.effect_id).state == "verified"


@pytest.mark.parametrize("block", ("pause", "active_chat"))
@pytest.mark.parametrize("settle", ("verify", "fail"))
@pytest.mark.parametrize("epoch_id", (None, "epoch-1"))
def test_pending_retry_gate_then_settle_never_calls_provider_again(
    tmp_path, capsys, block, settle, epoch_id
):
    calls = []

    def provider(request):
        calls.append(request)
        return "accepted-by-host-queue"

    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=FixedAutonomyJudge(AutonomyDecision(True, "run")),
        activity_providers=(ActivityProvider("fixture", provider),),
    )
    parser = _parser(ctx)
    facts = {"source_event_id": "pending-gate-settle"}
    if epoch_id is not None:
        facts["epoch_id"] = epoch_id

    first_code, first = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )
    if block == "pause":
        runtime.control("pause", feature="autonomy", source="operator")
        retry_facts = facts
    else:
        retry_facts = {**facts, "active_chat": True}
    retry_code, retry = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(retry_facts)],
        capsys,
    )
    record = runtime.effects.records()[0]

    assert (first["status"], first_code) == ("executed_unverified", 0)
    assert (retry["status"], retry["reason"], retry_code) == (
        "awaiting_reconciliation",
        "awaiting_reconciliation",
        0,
    )
    if settle == "verify":
        settled = runtime.reconcile_autonomy(record.effect_id, _receipt(calls[0]))
        assert (settled.status, settled.reason) == (
            "completed",
            "verified_reconciliation",
        )
        assert settled.effect_record.state == "verified"
    else:
        settled = runtime.fail_autonomy(record.effect_id, "host_failed")
        assert (settled.status, settled.reason) == ("failed", "host_failed")
        assert settled.effect_record.state == "failed"
    assert len(calls) == 1


@pytest.mark.parametrize("epoch_id", (None, "public-epoch"))
def test_effectless_skip_terminal_replays_by_registered_cli(tmp_path, capsys, epoch_id):
    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=FixedAutonomyJudge(AutonomyDecision(True, "run")),
        activity_providers=(
            ActivityProvider("fixture", lambda _request: pytest.fail("not called")),
        ),
    )
    parser = _parser(ctx)
    facts = {"source_event_id": "legacy-terminal", "active_chat": True}
    if epoch_id is not None:
        facts["epoch_id"] = epoch_id

    first_code, first = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )
    before = runtime.bus.audit.path.read_bytes()
    second_code, second = _run_cli(
        ctx,
        parser,
        [
            "autonomy",
            "--facts",
            json.dumps(
                {
                    "source_event_id": "legacy-terminal",
                    **({} if epoch_id is None else {"epoch_id": epoch_id}),
                }
            ),
        ],
        capsys,
    )

    assert (first["status"], first_code) == ("skipped", 0)
    assert (second["status"], second["reason"], second_code) == (
        "skipped",
        "active_chat",
        0,
    )
    assert runtime.effects.records() == ()
    assert runtime.bus.audit.path.read_bytes() == before


@pytest.mark.parametrize("reason", ("verified", "completed"))
def test_effectless_skip_reason_named_like_success_replays_by_registered_cli(
    tmp_path, capsys, reason
):
    judge_calls = []
    provider_calls = []

    class CountingDenyJudge:
        def decide(self, _context):
            judge_calls.append(True)
            return AutonomyDecision(False, reason)

    def provider(request):
        provider_calls.append(request)
        raise AssertionError("denied skip must stop before provider")

    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=CountingDenyJudge(),
        activity_providers=(ActivityProvider("fixture", provider),),
    )
    parser = _parser(ctx)
    facts = {"source_event_id": f"named-skip-{reason}"}
    first_code, first = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )
    before = runtime.bus.audit.path.read_bytes()
    second_code, second = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )

    assert (first["status"], first["reason"], first_code) == (
        "skipped",
        reason,
        0,
    )
    assert (second["status"], second["reason"], second_code) == (
        "skipped",
        reason,
        0,
    )
    assert judge_calls == [True]
    assert provider_calls == []
    assert runtime.effects.records() == ()
    assert runtime.bus.audit.path.read_bytes() == before


@pytest.mark.parametrize("epoch_id", (None, "public-epoch"))
def test_effectless_failed_terminal_replays_by_registered_cli(
    tmp_path, capsys, epoch_id
):
    judge_calls = []
    provider_calls = []

    class FailingJudge:
        def decide(self, _context):
            judge_calls.append(True)
            raise RuntimeError("synthetic")

    def provider(request):
        provider_calls.append(request)
        raise AssertionError("early failure must stop before provider")

    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=FailingJudge(),
        activity_providers=(ActivityProvider("fixture", provider),),
    )
    parser = _parser(ctx)
    facts = {"source_event_id": "early-failed-terminal"}
    if epoch_id is not None:
        facts["epoch_id"] = epoch_id

    first_code, first = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )
    before = runtime.bus.audit.path.read_bytes()
    second_code, second = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )

    assert (first["status"], first_code) == ("failed", 1)
    assert first["reason"] == "judge_error:RuntimeError"
    assert (second["status"], second["reason"], second_code) == (
        "failed",
        "judge_error:RuntimeError",
        1,
    )
    assert judge_calls == [True]
    assert provider_calls == []
    assert runtime.effects.records() == ()
    assert runtime.bus.audit.path.read_bytes() == before


def test_existing_skip_replays_before_gate_change_without_new_terminal(
    tmp_path, capsys
):
    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=FixedAutonomyJudge(AutonomyDecision(True, "run")),
        activity_providers=(
            ActivityProvider("fixture", lambda _request: pytest.fail("not called")),
        ),
    )
    parser = _parser(ctx)
    facts = {"source_event_id": "stable-skip", "active_chat": True}

    first_code, first = _run_cli(
        ctx,
        parser,
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )
    terminals_before = [
        event
        for event in runtime.bus.read_audit()
        if event.payload.get("occurrence_id") == "stable-skip"
        and event.payload.get("terminal") is not None
    ]
    runtime.control("pause", feature="autonomy", source="operator")
    second_code, second = _run_cli(
        ctx,
        parser,
        [
            "autonomy",
            "--facts",
            json.dumps({"source_event_id": "stable-skip"}),
        ],
        capsys,
    )
    terminals_after = [
        event
        for event in runtime.bus.read_audit()
        if event.payload.get("occurrence_id") == "stable-skip"
        and event.payload.get("terminal") is not None
    ]

    assert (first["status"], first["reason"], first_code) == (
        "skipped",
        "active_chat",
        0,
    )
    assert (second["status"], second["reason"], second_code) == (
        "skipped",
        "active_chat",
        0,
    )
    assert len(terminals_before) == len(terminals_after) == 1
    assert runtime.effects.records() == ()


@pytest.mark.parametrize(
    ("epoch_id", "terminal", "status"),
    (
        (None, "verified", "completed"),
        ("public-epoch", "verified", "completed"),
        (None, "failed", "invalid-status"),
    ),
)
def test_effectless_completed_terminal_is_not_replayed_by_registered_cli(
    tmp_path, capsys, epoch_id, terminal, status
):
    judge_calls = []
    provider_calls = []

    class CountingJudge:
        def decide(self, _context):
            judge_calls.append(True)
            return AutonomyDecision(True, "run")

    def provider(request):
        provider_calls.append(request)
        raise AssertionError("effectless terminal must stop before provider")

    ctx = HostContext(_config(tmp_path))
    runtime = register(
        ctx,
        autonomy_judge=CountingJudge(),
        activity_providers=(ActivityProvider("fixture", provider),),
    )
    occurrence_id = f"effectless-completed-{epoch_id or 'legacy'}"
    terminal_kwargs = {
        "occurrence_id": occurrence_id,
        "terminal": terminal,
        "status": status,
        "source": "autonomy",
        "details": {"provider": "fixture"},
    }
    if epoch_id is not None:
        terminal_kwargs["epoch_id"] = epoch_id
    runtime.bus.record_audit_terminal("autonomy", **terminal_kwargs)
    before = runtime.bus.audit.path.read_bytes()

    facts = {"source_event_id": occurrence_id}
    if epoch_id is not None:
        facts["epoch_id"] = epoch_id
    code, output = _run_cli(
        ctx,
        _parser(ctx),
        ["autonomy", "--facts", json.dumps(facts)],
        capsys,
    )

    assert (output["status"], output["reason"], code) == (
        "failed",
        "terminal_conflict",
        1,
    )
    assert judge_calls == []
    assert provider_calls == []
    assert runtime.effects.records() == ()
    assert runtime.bus.audit.path.read_bytes() == before


def test_verified_terminal_replay_precedes_current_active_chat_gate(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
    )
    facts = {"source_event_id": "verified-gated", "epoch_id": "epoch-1"}
    completed = first.run_once({"chosen": {"enabled": True}}, facts=facts)
    gated, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("not called"))],
    )

    result = gated.run_once(
        {"chosen": {"enabled": True}}, facts={**facts, "active_chat": True}
    )

    assert completed.status == "completed"
    assert (result.status, result.reason) == ("completed", "already_verified")
    assert len(calls) == 1
    assert ledger.get(completed.effect_id).state == "verified"


def test_implicit_and_explicit_epochs_replay_exact_effects(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request, receipt_id=f"receipt-{len(calls)}")

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
    )
    legacy_facts = {"source_event_id": "epoch-isolation"}
    explicit_facts = {"source_event_id": "epoch-isolation", "epoch_id": "epoch-1"}
    legacy = first.run_once({"chosen": {"enabled": True}}, facts=legacy_facts)
    explicit = first.run_once({"chosen": {"enabled": True}}, facts=explicit_facts)

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("terminal replay must not invoke Judge")

    replay, _controls, _ledger = _engine(
        tmp_path,
        [],
        judge=RejectUnexpectedJudge(),
    )
    legacy_replay = replay.run_once({}, facts=legacy_facts)
    explicit_replay = replay.run_once({}, facts=explicit_facts)

    assert legacy.status == explicit.status == "completed"
    assert (legacy_replay.status, legacy_replay.effect_id) == (
        "completed",
        legacy.effect_id,
    )
    assert (explicit_replay.status, explicit_replay.effect_id) == (
        "completed",
        explicit.effect_id,
    )
    assert legacy.effect_id != explicit.effect_id
    assert len(calls) == 2
    assert ledger.get(legacy.effect_id).epoch_id == f"autonomy:{NOW.date().isoformat()}"
    assert ledger.get(explicit.effect_id).epoch_id == "epoch-1"


def test_legacy_effectless_audit_history_still_counts_toward_limits(tmp_path):
    calls = []

    def provider(_request):
        calls.append(True)
        return pytest.fail("legacy history should block before provider")

    engine, _controls, _ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider, daily_limit=1)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="completed",
        source="autonomy",
        details={"provider": "chosen"},
    )

    result = engine.run_once(
        {"chosen": {"enabled": True, "daily_limit": 1}},
        facts={"source_event_id": "legacy-history"},
    )

    assert (result.status, result.reason, calls) == (
        "skipped",
        "no_eligible_provider",
        [],
    )


def test_skipped_audit_cannot_mask_verified_effect(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return _receipt(request)

    first, _controls, ledger = _engine(
        tmp_path,
        [ActivityProvider("chosen", provider)],
        bus=CrashAfterIntentBus(tmp_path),
    )
    facts = {"source_event_id": "poisoned-skip", "epoch_id": "epoch-1"}
    with pytest.raises(SystemExit, match="synthetic crash"):
        first.run_once({"chosen": {"enabled": True}}, facts=facts)
    intent = ledger.records()[0]
    first.bus.record_audit_terminal(
        "autonomy",
        occurrence_id=intent.source_event_id,
        epoch_id=intent.epoch_id,
        terminal="no_eligible_provider",
        status="skipped",
        source="autonomy",
        details={
            "provider": "chosen",
            "effect_id": intent.effect_id,
            "source_event_id": intent.source_event_id,
            "epoch_id": intent.epoch_id,
            "idempotency_key": intent.idempotency_key,
        },
    )
    receipt = EffectReceipt(
        receipt_id="receipt-verified",
        event_id=intent.source_event_id,
        observed_at=NOW,
        content_sha256=intent.content_sha256,
        content_length=intent.content_length,
        epoch_id=intent.epoch_id,
    )
    ledger.mark_pending(intent.effect_id)
    ledger.mark_queue_accepted(intent.effect_id)
    ledger.verify(intent.effect_id, receipt)

    class RejectUnexpectedJudge:
        def decide(self, _context):
            raise AssertionError("poisoned terminal must fail before Judge")

    recovered, _controls, _ledger = _engine(
        tmp_path,
        [],
        judge=RejectUnexpectedJudge(),
    )
    result = recovered.run_once({}, facts=facts)

    assert (result.status, result.reason) == ("failed", "terminal_conflict")
    assert calls == []
    assert recovered.effect_ledger.get(intent.effect_id).state == "verified"
