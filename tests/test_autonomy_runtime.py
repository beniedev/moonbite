from __future__ import annotations

import json
import random
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from moonbite_plugin.autonomy import (
    ActivityProvider,
    AllowAutonomyJudge,
    AutonomyDecision,
    AutonomyEngine,
    AutonomyExecutionRequest,
    ProviderRegistry,
)
from moonbite_plugin.control import ControlResolution, ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.runtime_core import EventBus

NOW = datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)


def receipt_for(request: AutonomyExecutionRequest, *, receipt_id: str = "receipt-1"):
    return EffectReceipt(
        receipt_id=receipt_id,
        event_id=request.source_event_id,
        observed_at=request.context.now,
        content_sha256=request.content_sha256,
        content_length=request.content_length,
        epoch_id=request.epoch_id,
    )


class FakeLocks:
    def __init__(self):
        self.names = []

    @contextmanager
    def try_exclusive(self, name):
        self.names.append(name)
        yield True


class PathlessControls:
    @property
    def ledger(self):
        raise AssertionError("injected state must not inspect controls.ledger")

    def resolve(self, feature):
        return ControlResolution(feature, None)


class PublicEffectPort:
    """Test adapter exposing the explicit public effect-port surface."""

    def __init__(self, root, *, clock):
        self.inner = EffectLedger(root, clock=clock)
        self._effect_ids: dict[str, str] = {}

    def begin_intent(self, **kwargs):
        record = self.inner.begin_intent(**kwargs)
        self._effect_ids[record.idempotency_key] = record.effect_id
        return record

    def mark_pending(self, effect_id):
        return self.inner.mark_pending(effect_id)

    def mark_queue_accepted(self, effect_id):
        return self.inner.mark_queue_accepted(effect_id)

    def verify(self, effect_id, receipt):
        return self.inner.verify(effect_id, receipt)

    def fail(self, effect_id, reason, retryable):
        return self.inner.fail(effect_id, reason, retryable)

    def get(self, effect_id):
        return self.inner.get(effect_id)

    def find_by_idempotency(self, key):
        effect_id = self._effect_ids.get(key)
        return None if effect_id is None else self.inner.get(effect_id)

    def records(self):
        return [
            self.inner.get(effect_id)
            for effect_id in self._effect_ids.values()
            if self.inner.get(effect_id) is not None
        ]

    def __getattr__(self, name):
        return getattr(self.inner, name)


def make_engine(
    tmp_path,
    providers,
    *,
    judge=None,
    clock=lambda: NOW,
    locks=None,
    effect_ledger=None,
):
    registry = ProviderRegistry()
    for provider in providers:
        registry.register(provider)
    controls = ControlStore(tmp_path, clock=clock)
    if effect_ledger is None:
        effect_ledger = PublicEffectPort(tmp_path, clock=clock)
    engine = AutonomyEngine(
        bus=EventBus(tmp_path, clock=clock),
        controls=controls,
        registry=registry,
        judge=judge or AllowAutonomyJudge(),
        rng=random.Random(0),
        clock=clock,
        locks=locks,
        effect_ledger=effect_ledger,
    )
    return engine, controls


def settings(name="chosen", **values):
    return {name: {"enabled": True, "weight": 1, **values}}


def test_provider_descriptor_is_bounded_and_registry_injectable():
    provider = ActivityProvider(
        "chosen",
        lambda _request: None,
        capabilities={"read", "bounded"},
        cost_class="medium",
        allowed_sources={"scheduler"},
        allowed_channels={"private"},
        cooldown=60,
        daily_limit=2,
        repeat_limit=1,
    )
    assert provider.capabilities == frozenset({"read", "bounded"})
    assert provider.evidence_contract == "effect_receipt"
    assert provider.allowed_sources == frozenset({"scheduler"})
    registry = ProviderRegistry()
    registry.register(provider)
    assert registry.get("chosen") == provider
    with pytest.raises(ValueError):
        ActivityProvider("", lambda _request: None)
    with pytest.raises(ValueError):
        ActivityProvider("bad", lambda _request: None, daily_limit=0)
    with pytest.raises(ValueError):
        ActivityProvider("bad", lambda _request: None, capabilities=["x"] * 33)


def test_pathless_controls_require_an_injected_effect_port(tmp_path):
    with pytest.raises(TypeError, match="effect_ledger_required"):
        AutonomyEngine(
            bus=EventBus(tmp_path, clock=lambda: NOW),
            controls=PathlessControls(),
            registry=ProviderRegistry(),
            judge=AllowAutonomyJudge(),
            clock=lambda: NOW,
        )


def test_effect_port_requires_public_records_method(tmp_path):
    class WithoutRecords(PublicEffectPort):
        records = None

    port = WithoutRecords(tmp_path, clock=lambda: NOW)
    with pytest.raises(TypeError, match="records"):
        AutonomyEngine(
            bus=EventBus(tmp_path, clock=lambda: NOW),
            controls=PathlessControls(),
            registry=ProviderRegistry(),
            judge=AllowAutonomyJudge(),
            clock=lambda: NOW,
            effect_ledger=port,
        )


def test_plain_provider_return_is_executed_unverified(tmp_path):
    engine, _controls = make_engine(
        tmp_path, [ActivityProvider("chosen", lambda _request: {"body": "secret"})]
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    assert (result.status, result.reason, result.verified) == (
        "executed_unverified",
        "awaiting_receipt",
        False,
    )
    record = engine.effect_ledger.get(result.effect_id)
    assert record.state == "executed_unverified"
    audit = engine.bus.read_audit()[-1].payload
    assert audit["evidence"]["state"] == "executed_unverified"
    assert "body" not in json.dumps(audit)


def test_strict_receipt_is_the_only_completed_result(tmp_path):
    seen = []

    def run(request):
        seen.append(request)
        return receipt_for(request)

    engine, _controls = make_engine(tmp_path, [ActivityProvider("chosen", run)])
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    assert result.status == "completed"
    assert result.evidence["state"] == "verified"
    assert result.evidence["receipt_id"] == "receipt-1"
    assert result.effect_record.verified is True
    assert result.canonical_event_id == "source-1"
    assert isinstance(seen[0], AutonomyExecutionRequest)
    assert seen[0].effect_id == result.effect_id
    assert seen[0].idempotency_key
    assert seen[0].source_event_id == "source-1"
    assert seen[0].epoch_id == "epoch-1"
    assert seen[0].content_length > 0
    assert seen[0].attempt == 1


def test_started_event_is_after_effect_intent_and_failure_does_not_run_provider(
    tmp_path, monkeypatch
):
    calls = []
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: calls.append(True))],
    )

    def fail_emit(kind, **_kwargs):
        assert kind == "autonomy.started"
        records = engine.effect_ledger.records()
        assert len(records) == 1
        assert records[0].state == "intent"
        raise OSError("event unavailable")

    monkeypatch.setattr(engine.bus, "emit", fail_emit)
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )

    assert result.status == "failed"
    assert result.reason == "started_event_error:OSError"
    assert calls == []
    assert result.effect_record.state == "intent"
    assert engine.bus.read_events() == []


def test_audit_failure_degrades_projection_but_preserves_verified_effect(
    tmp_path, monkeypatch
):
    engine, _controls = make_engine(
        tmp_path, [ActivityProvider("chosen", lambda request: receipt_for(request))]
    )

    def fail_audit(*_args, **_kwargs):
        raise OSError("audit unavailable")

    monkeypatch.setattr(engine.bus, "record_audit", fail_audit)
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    assert (result.status, result.audit_status, result.degraded) == (
        "completed",
        "degraded",
        True,
    )
    assert result.effect_record.state == "verified"
    assert result.audit_error == "audit_error:OSError"


def test_durable_effect_records_enforce_limits_when_audit_write_fails(
    tmp_path, monkeypatch
):
    calls = []

    def run(request):
        calls.append(request)
        return receipt_for(request, receipt_id=f"receipt-{len(calls)}")

    engine, _controls = make_engine(
        tmp_path, [ActivityProvider("chosen", run, daily_limit=1)]
    )

    def fail_audit(*_args, **_kwargs):
        raise OSError("audit unavailable")

    monkeypatch.setattr(engine.bus, "record_audit", fail_audit)
    first = engine.run_once(
        settings(), facts={"source_event_id": "s1", "epoch_id": "e1"}
    )
    second = engine.run_once(
        settings(), facts={"source_event_id": "s2", "epoch_id": "e1"}
    )

    assert first.status == "completed"
    assert first.audit_status == "degraded"
    assert (second.status, second.reason) == (
        "skipped",
        "no_eligible_provider",
    )
    assert len(calls) == 1


def test_selected_failure_never_rerolls(tmp_path):
    calls = []

    def fail(_request):
        calls.append("first")
        raise RuntimeError("fixture")

    def alternate(_request):
        calls.append("alternate")

    engine, _controls = make_engine(
        tmp_path,
        [
            ActivityProvider("first", fail),
            ActivityProvider("alternate", alternate),
        ],
    )
    result = engine.run_once(
        {
            "first": {"enabled": True, "weight": 100},
            "alternate": {"enabled": True, "weight": 1},
        },
        facts={"source_event_id": "source-1", "epoch_id": "epoch-1"},
    )
    assert (result.status, result.provider, calls) == ("failed", "first", ["first"])


def test_control_and_active_chat_are_before_judge(tmp_path):
    class CountingJudge:
        calls = 0

        def decide(self, _context):
            self.calls += 1
            return AutonomyDecision(True, "allow")

    judge = CountingJudge()
    engine, controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("runner called"))],
        judge=judge,
    )
    assert (
        engine.run_once(settings(), facts={"active_chat": True}).reason == "active_chat"
    )
    assert judge.calls == 0
    controls.put(feature="autonomy", mode="pause", source="operator")
    assert engine.run_once(settings()).status == "skipped"
    assert judge.calls == 0
    assert engine.bus.read_events() == []


def test_malformed_judge_decision_fails_closed_before_runner(tmp_path):
    calls = []

    class MalformedJudge:
        def decide(self, _context):
            malformed = object.__new__(AutonomyDecision)
            object.__setattr__(malformed, "allowed", "yes")
            object.__setattr__(malformed, "reason", "allow")
            return malformed

    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: calls.append(True))],
        judge=MalformedJudge(),
    )
    result = engine.run_once(settings(), facts={"source_event_id": "s1"})

    assert (result.status, result.reason, calls) == (
        "failed",
        "judge_invalid_result",
        [],
    )
    assert engine.bus.read_audit()[-1].payload["status"] == "failed"


@pytest.mark.parametrize("chat_key", ["active_chat", "chat_active"])
def test_invalid_active_chat_type_fails_closed_before_judge(tmp_path, chat_key):
    judge_calls = []

    class Judge:
        def decide(self, _context):
            judge_calls.append(True)
            return AutonomyDecision(True, "allow")

    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: pytest.fail("runner called"))],
        judge=Judge(),
    )
    result = engine.run_once(settings(), facts={chat_key: "true"})

    assert (result.status, result.reason, judge_calls) == (
        "failed",
        f"{chat_key}_invalid",
        [],
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_event_id", ""),
        ("event_id", None),
        ("epoch_id", ""),
        ("epoch", None),
        ("idempotency_key", ""),
    ],
)
def test_invalid_explicit_identity_fields_fail_closed(tmp_path, field, value):
    calls = []
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: calls.append(True))],
    )
    result = engine.run_once(settings(), facts={field: value})

    assert (result.status, result.reason, calls) == (
        "failed",
        f"invalid_{field}",
        [],
    )
    assert engine.effect_ledger.records() == []


def test_invalid_alias_identity_field_is_not_hidden_by_preferred_alias(tmp_path):
    engine, _controls = make_engine(
        tmp_path, [ActivityProvider("chosen", lambda _request: None)]
    )

    result = engine.run_once(
        settings(), facts={"source_event_id": "valid", "event_id": ""}
    )

    assert (result.status, result.reason) == ("failed", "invalid_event_id")


def test_play_next_unverified_preserves_control_and_duplicate_skips_judge(tmp_path):
    calls = []
    judge_calls = []

    class Judge:
        def decide(self, _context):
            judge_calls.append(True)
            return AutonomyDecision(True, "allow")

    def run(request):
        calls.append(request)
        return "queued"

    engine, controls = make_engine(
        tmp_path, [ActivityProvider("chosen", run)], judge=Judge()
    )
    intent = controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "chosen"},
    )
    facts = {"source_event_id": "source-1", "epoch_id": "epoch-1"}
    first = engine.run_once(settings(), facts=facts)
    second = engine.run_once(settings(), facts=facts)
    assert first.status == "executed_unverified"
    assert (second.status, second.reason) == (
        "awaiting_reconciliation",
        "awaiting_reconciliation",
    )
    assert len(calls) == 1
    assert len(judge_calls) == 1
    assert controls.resolve("autonomy").intent == intent


def test_explicit_reconcile_verifies_without_rerunning_and_consumes_play_next(tmp_path):
    calls = []

    def run(request):
        calls.append(request)
        return "accepted"

    engine, controls = make_engine(tmp_path, [ActivityProvider("chosen", run)])
    controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "chosen"},
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    completed = engine.reconcile(
        result.effect_id, receipt_for(calls[0], receipt_id="receipt-2")
    )
    assert completed.status == "completed"
    assert len(calls) == 1
    assert controls.resolve("autonomy").intent is None


def test_late_reconcile_after_expiry_does_not_verify_or_consume_control(tmp_path):
    current = [NOW]
    calls = []

    def run(request):
        calls.append(request)
        return "accepted"

    engine, controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", run)],
        clock=lambda: current[0],
    )
    intent = controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "chosen"},
    )
    result = engine.run_once(
        settings(effect_ttl=1),
        facts={"source_event_id": "source-1", "epoch_id": "epoch-1"},
    )
    current[0] = NOW + timedelta(seconds=2)
    late = engine.reconcile(result.effect_id, receipt_for(calls[0]))

    assert (late.status, late.reason) == (
        "awaiting_reconciliation",
        "expired_requeue_required",
    )
    assert engine.effect_ledger.get(result.effect_id).state == "executed_unverified"
    assert controls.resolve("autonomy").intent.control_id == intent.control_id


def test_late_reconcile_never_consumes_a_new_play_next_control(tmp_path):
    calls = []

    def run(request):
        calls.append(request)
        return "accepted"

    engine, controls = make_engine(tmp_path, [ActivityProvider("chosen", run)])
    old = controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "chosen"},
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    new = controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "chosen"},
    )
    completed = engine.reconcile(
        result.effect_id,
        receipt_for(calls[0], receipt_id="receipt-late"),
        control_id=new.control_id,
    )
    assert (completed.status, completed.effect_record.state) == (
        "completed",
        "verified",
    )
    assert controls.resolve("autonomy").intent.control_id == new.control_id
    assert old.control_id != new.control_id


def test_receipt_mismatch_fails_closed(tmp_path):
    def run(request):
        return EffectReceipt(
            receipt_id="bad",
            event_id=request.source_event_id,
            observed_at=request.context.now,
            content_sha256="0" * 64,
            content_length=request.content_length,
            epoch_id=request.epoch_id,
        )

    engine, _controls = make_engine(tmp_path, [ActivityProvider("chosen", run)])
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    assert result.status == "failed"
    assert "receipt_mismatch" in result.reason
    assert engine.effect_ledger.get(result.effect_id).state == "failed"


def test_play_next_fallback_is_explicit_and_deterministic(tmp_path):
    calls = []
    engine, controls = make_engine(
        tmp_path,
        [
            ActivityProvider(
                "blocked",
                lambda _request: calls.append("blocked"),
                eligible=lambda _ctx: False,
            ),
            ActivityProvider("fallback", lambda _request: calls.append("fallback")),
        ],
    )
    controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "blocked", "fallback_provider": "fallback"},
    )
    result = engine.run_once(
        {"blocked": {"enabled": True}, "fallback": {"enabled": True}},
        facts={"source_event_id": "source-1", "epoch_id": "epoch-1"},
    )
    assert (result.provider, calls) == ("fallback", ["fallback"])


@pytest.mark.parametrize(
    "requested_settings",
    [{}, {"requested": {"enabled": False}}],
)
def test_play_next_fallback_handles_missing_or_disabled_requested_provider(
    tmp_path, requested_settings
):
    calls = []
    engine, controls = make_engine(
        tmp_path,
        [
            ActivityProvider("requested", lambda _request: calls.append("requested")),
            ActivityProvider("fallback", lambda _request: calls.append("fallback")),
        ],
    )
    controls.put(
        feature="autonomy",
        mode="play_next",
        source="operator",
        payload={"provider": "requested", "fallback_provider": "fallback"},
    )
    config = {**requested_settings, "fallback": {"enabled": True}}

    result = engine.run_once(config, facts={"source_event_id": "s1"})

    assert (result.provider, result.status, calls) == (
        "fallback",
        "executed_unverified",
        ["fallback"],
    )


def test_source_and_channel_gates_are_exact(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [
            ActivityProvider(
                "chosen",
                lambda _request: "queued",
                allowed_sources={"scheduler"},
                allowed_channels={"private"},
            )
        ],
    )
    wrong = engine.run_once(
        settings(),
        facts={"source": "scheduler", "channel": "public", "source_event_id": "s1"},
    )
    right = engine.run_once(
        settings(),
        facts={"source": "scheduler", "channel": "private", "source_event_id": "s2"},
    )
    assert wrong.reason == "no_eligible_provider"
    assert wrong.status == "skipped"
    assert right.status == "executed_unverified"


@pytest.mark.parametrize(
    ("provider_settings", "expected_reason"),
    [
        ({"enabled": "yes"}, "invalid_provider_settings:enabled"),
        (
            {"enabled": True, "cooldown": "not-a-duration"},
            "invalid_provider_settings:cooldown",
        ),
        ({"enabled": True, "weight": 0}, "invalid_provider_settings:weight"),
    ],
)
def test_invalid_provider_settings_return_typed_failure(
    tmp_path, provider_settings, expected_reason
):
    calls = []
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: calls.append(True))],
    )
    result = engine.run_once({"chosen": provider_settings})

    assert (result.status, result.reason, calls) == (
        "failed",
        expected_reason,
        [],
    )
    assert engine.bus.read_audit()[-1].payload["status"] == "failed"


def test_cooldown_and_daily_limit_use_eventbus_audit(tmp_path):
    calls = []

    def run(request):
        calls.append(request)
        return receipt_for(request, receipt_id=f"r-{len(calls)}")

    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", run, cooldown=3600, daily_limit=1)],
    )
    first = engine.run_once(
        settings(), facts={"source_event_id": "s1", "epoch_id": "e1"}
    )
    second = engine.run_once(
        settings(), facts={"source_event_id": "s2", "epoch_id": "e1"}
    )
    assert first.status == "completed"
    assert second.status == "skipped"
    assert second.reason == "no_eligible_provider"
    assert len(calls) == 1
    assert len(engine.bus.read_audit()) == 2


def test_repeat_and_cost_limits_are_bounded(tmp_path):
    calls = []

    def run(request):
        calls.append(request)
        return "queued"

    engine, _controls = make_engine(
        tmp_path,
        [
            ActivityProvider(
                "chosen", run, repeat_limit=1, cost_class="medium", cost_budget=2
            )
        ],
    )
    first = engine.run_once(
        settings(),
        facts={"source_event_id": "same", "epoch_id": "e1", "cost_budget_remaining": 2},
    )
    repeated = engine.run_once(
        settings(),
        facts={"source_event_id": "same", "epoch_id": "e2", "cost_budget_remaining": 2},
    )
    cost_blocked = engine.run_once(
        settings(),
        facts={
            "source_event_id": "other",
            "epoch_id": "e3",
            "cost_budget_remaining": 1,
        },
    )
    assert first.status == "executed_unverified"
    assert repeated.status == "skipped"
    assert cost_blocked.status == "skipped"
    assert len(calls) == 1


def test_expired_effect_requires_explicit_requeue(tmp_path):
    current = [NOW]
    calls = []

    def run(request):
        calls.append(request)
        return "queued"

    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", run)],
        clock=lambda: current[0],
    )
    facts = {"source_event_id": "s1", "epoch_id": "e1"}
    first = engine.run_once(settings(effect_ttl=1), facts=facts)
    current[0] = NOW + timedelta(seconds=2)
    engine.effect_ledger.expire(first.effect_id, now=current[0])
    second = engine.run_once(settings(effect_ttl=1), facts=facts)
    assert (second.status, second.reason) == (
        "awaiting_reconciliation",
        "expired_requeue_required",
    )
    assert len(calls) == 1


def test_injected_effect_port_and_locks_need_no_controls_path(tmp_path):
    class EffectPort:
        def __init__(self):
            self.inner = EffectLedger(tmp_path, clock=lambda: NOW)
            self._effect_ids = {}

        def begin_intent(self, **kwargs):
            record = self.inner.begin_intent(**kwargs)
            self._effect_ids[record.idempotency_key] = record.effect_id
            return record

        def mark_pending(self, effect_id):
            return self.inner.mark_pending(effect_id)

        def mark_queue_accepted(self, effect_id):
            return self.inner.mark_queue_accepted(effect_id)

        def verify(self, effect_id, receipt):
            return self.inner.verify(effect_id, receipt)

        def fail(self, effect_id, reason, retryable):
            return self.inner.fail(effect_id, reason, retryable)

        def get(self, effect_id):
            return self.inner.get(effect_id)

        def find_by_idempotency(self, key):
            effect_id = self._effect_ids.get(key)
            return None if effect_id is None else self.inner.get(effect_id)

        def records(self):
            return [
                self.inner.get(effect_id)
                for effect_id in self._effect_ids.values()
                if self.inner.get(effect_id) is not None
            ]

    locks = FakeLocks()
    port = EffectPort()
    engine = AutonomyEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=PathlessControls(),
        registry=ProviderRegistry(),
        judge=AllowAutonomyJudge(),
        locks=locks,
        clock=lambda: NOW,
        effect_ledger=port,
    )
    engine.registry.register(
        ActivityProvider("chosen", lambda request: receipt_for(request))
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "s1", "epoch_id": "e1"}
    )
    assert result.status == "completed"
    assert locks.names == ["autonomy_execution"]
    assert engine.execution_lock_path is None


def test_observer_status_discards_unrelated_and_private_audit_payloads(tmp_path):
    bus = EventBus(tmp_path, clock=lambda: NOW)
    bus.record_audit(
        "unrelated",
        status="ignored",
        source="test",
        details={
            "output": {"body": "unrelated-secret"},
            "facts": {"raw": "unrelated-secret"},
            "nested": [{"reason": "do-not-retain"}],
        },
    )
    bus.record_audit(
        "autonomy",
        status="failed",
        source="test",
        details={
            "provider": "chosen",
            "effect_id": "audit-effect",
            "source_event_id": "source-1",
            "idempotency_key": "idempotency-1",
            "reason": "provider_error:RuntimeError",
            "output": {"body": "autonomy-secret"},
            "facts": {"raw": "autonomy-secret"},
            "gate": {"reason": "private-gate-reason"},
            "evidence": {"receipt_id": "receipt-1", "output": "private"},
        },
    )

    audit_path = bus.audit.path
    audit_lock = bus.audit.lock_path
    before = audit_path.read_bytes()
    before_mtime = audit_path.stat().st_mtime_ns
    if audit_lock.exists():
        audit_lock.unlink()

    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    facts = engine.observer_status(target_date=NOW.date(), now=NOW)
    encoded = [fact.to_dict() for fact in facts]

    failure = [
        fact for fact in encoded if fact["code"] == "autonomy_provider_failure:failure"
    ]
    assert len(failure) == 1
    serialized = json.dumps(encoded, sort_keys=True)
    for private_value in (
        "unrelated-secret",
        "autonomy-secret",
        "private-gate-reason",
        "provider_error:RuntimeError",
    ):
        assert private_value not in serialized
    assert all(
        key not in serialized for key in ("output", "facts", "body", "gate", "reason")
    )
    assert audit_path.read_bytes() == before
    assert audit_path.stat().st_mtime_ns == before_mtime
    assert not audit_lock.exists()


def test_observer_audit_only_failed_to_completed_stays_unverified(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    details = {
        "provider": "chosen",
        "effect_id": "audit-effect",
        "source_event_id": "source-1",
        "idempotency_key": "audit-idempotency",
    }
    engine.bus.record_audit("autonomy", status="failed", source="test", details=details)
    engine.bus.record_audit(
        "autonomy", status="completed", source="test", details=details
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    completion = next(
        fact for fact in facts if fact.code == "autonomy_completion_unverified"
    )
    assert completion.state == "current"
    assert completion.recovery is None
    assert all(fact.state == "current" for fact in facts)


def test_observer_audit_completed_without_receipt_is_not_verified(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="completed",
        source="test",
        details={
            "provider": "chosen",
            "effect_id": "audit-effect",
            "source_event_id": "source-1",
            "idempotency_key": "audit-idempotency",
        },
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].code == "autonomy_completion_unverified"
    assert facts[0].state == "current"
    assert facts[0].recovery is None


def test_observer_audit_conflicts_do_not_override_verified_effect(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda request: receipt_for(request))],
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )
    engine.bus.record_audit(
        "autonomy",
        status="completed",
        source="test",
        details={
            "provider": "chosen",
            "effect_id": "wrong-effect",
            "source_event_id": result.source_event_id,
            "idempotency_key": result.idempotency_key,
            "evidence": {"receipt_id": "wrong-receipt"},
        },
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    verified = next(fact for fact in facts if fact.code == "autonomy_verified")
    assert verified.state == "recovered_history"
    assert verified.recovery is not None
    assert verified.recovery.ref == result.effect_record.receipt.receipt_id
    assert any(
        fact.state == "current"
        and fact.code == "autonomy_integrity_error:audit_effect_conflict"
        for fact in facts
    )


def test_observer_duplicate_audit_event_id_is_integrity(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="completed",
        source="test",
        details={"provider": "chosen"},
    )
    engine.bus.audit.append(engine.bus.audit.rows()[0])

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert any(
        fact.code == "autonomy_integrity_error:duplicate_event_id"
        and fact.state == "current"
        for fact in facts
    )


def test_observer_skip_without_provider_is_ignored(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="skipped",
        source="test",
        details={"provider": None},
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert facts == ()


def test_observer_failed_without_provider_is_global_current(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="failed",
        source="test",
        details={"provider": None, "reason": "judge_error:private"},
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "autonomy_provider_failure:failure"
    assert facts[0].refs == ()
    assert "private" not in str(facts)


def test_observer_unknown_audit_status_is_integrity(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    engine.bus.record_audit(
        "autonomy",
        status="unknown-terminal",
        source="test",
        details={"provider": "chosen"},
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code.startswith("autonomy_integrity_error:")


def test_observer_verified_effect_uses_receipt_for_recovery(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda request: receipt_for(request))],
    )
    result = engine.run_once(
        settings(), facts={"source_event_id": "source-1", "epoch_id": "epoch-1"}
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].code == "autonomy_verified"
    assert facts[0].state == "recovered_history"
    assert facts[0].recovery is not None
    assert facts[0].recovery.ref == result.effect_record.receipt.receipt_id


def test_observer_custom_idempotency_uses_exact_audit_provider_label(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda request: receipt_for(request))],
    )
    result = engine.run_once(
        settings(),
        facts={
            "source_event_id": "source-1",
            "epoch_id": "epoch-1",
            "idempotency_key": "custom-key",
        },
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert result.status == "completed"
    assert len(facts) == 1
    assert facts[0].code == "autonomy_verified"
    assert facts[0].state == "recovered_history"
    assert "provider:chosen" in facts[0].refs
    assert facts[0].recovery is not None
    assert facts[0].recovery.ref == result.effect_record.receipt.receipt_id


def test_observer_custom_verified_effect_without_audit_is_generic_verified(
    tmp_path, monkeypatch
):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda request: receipt_for(request))],
    )
    monkeypatch.setattr(engine.bus, "record_audit", lambda *_args, **_kwargs: None)
    result = engine.run_once(
        settings(),
        facts={
            "source_event_id": "source-1",
            "epoch_id": "epoch-1",
            "idempotency_key": "custom-key",
        },
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert result.status == "completed"
    assert len(facts) == 1
    assert facts[0].code == "autonomy_verified"
    assert facts[0].state == "recovered_history"
    assert "provider:chosen" not in facts[0].refs
    assert facts[0].recovery is not None
    assert facts[0].recovery.ref == result.effect_record.receipt.receipt_id


def test_observer_custom_unverified_effect_without_provider_is_integrity(tmp_path):
    engine, _controls = make_engine(
        tmp_path,
        [ActivityProvider("chosen", lambda _request: None)],
    )
    result = engine.run_once(
        settings(),
        facts={
            "source_event_id": "source-1",
            "epoch_id": "epoch-1",
            "idempotency_key": "custom-key",
        },
    )

    facts = engine.observer_status(target_date=NOW.date(), now=NOW)

    assert result.status == "executed_unverified"
    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code == "autonomy_integrity_error:provider_unavailable"
