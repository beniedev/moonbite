from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from moonbite_plugin.control import ControlResolution, ControlStore
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import (
    EffectResult,
    HeartbeatCadence,
    HeartbeatCandidate,
    HeartbeatEngine,
    HeartbeatReasonCode,
    JudgeDecision,
)
from moonbite_plugin.runtime_core import EventBus, StateError, atomic_json_write
from moonbite_plugin.observer import ObservationFact
from moonbite_plugin.session import SessionContext, SessionLifecycleStore

NOW = datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)


class Judge:
    def __init__(self, decision=None, error=None):
        self.decision = decision
        self.error = error
        self.calls = 0

    def decide(self, candidate):
        self.calls += 1
        if self.error:
            raise self.error
        return self.decision


class Sink:
    def __init__(
        self,
        *,
        delivery_ok=True,
        wake_ok=True,
        delivery_error=None,
        wake_error=None,
        delivery_verified=True,
        wake_verified=True,
    ):
        self.delivery_ok = delivery_ok
        self.wake_ok = wake_ok
        self.delivery_error = delivery_error
        self.wake_error = wake_error
        self.delivery_verified = delivery_verified
        self.wake_verified = wake_verified
        self.deliveries = 0
        self.wakes = 0

    def deliver(self, candidate, decision):
        self.deliveries += 1
        if self.delivery_error:
            raise self.delivery_error
        return EffectResult(
            self.delivery_ok,
            "sent" if self.delivery_ok else "rejected",
            verified=self.delivery_ok and self.delivery_verified,
        )

    def wake(self, candidate, decision):
        self.wakes += 1
        if self.wake_error:
            raise self.wake_error
        return EffectResult(
            self.wake_ok,
            "accepted" if self.wake_ok else "rejected",
            verified=self.wake_ok and self.wake_verified,
        )


def engine(tmp_path, judge, sink, *, locks=None, kind_policies=None):
    bus = EventBus(tmp_path, clock=lambda: NOW)
    controls = ControlStore(tmp_path, clock=lambda: NOW)
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    return (
        HeartbeatEngine(
            bus=bus,
            controls=controls,
            cadence=cadence,
            judge=judge,
            sink=sink,
            locks=locks,
            kind_policies=kind_policies,
        ),
        controls,
        bus,
        cadence,
    )


class FakeLocks:
    def __init__(self):
        self.names = []

    @contextmanager
    def try_exclusive(self, name):
        self.names.append(name)
        yield True


class PathlessCadence:
    @property
    def path(self):
        raise AssertionError("injected locks must not inspect cadence.path")

    def blocked(self, kind):
        return False, "open"

    def clock(self):
        return NOW


class PathlessControls:
    @property
    def ledger(self):
        raise AssertionError("injected locks must not inspect controls.ledger")

    def resolve(self, feature):
        return ControlResolution(feature, None)


def test_injected_heartbeat_locks_are_used(tmp_path):
    locks = FakeLocks()
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=PathlessControls(),
        cadence=PathlessCadence(),
        judge=Judge(JudgeDecision(False, False, "silent")),
        sink=Sink(),
        locks=locks,
    )

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert result.reason == "silent"
    assert locks.names == ["heartbeat_execution"]
    assert runtime.execution_lock_path is None


def test_default_heartbeat_execution_lock_filename_is_unchanged(tmp_path):
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, False, "silent")),
        Sink(),
    )

    runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert runtime.execution_lock_path == tmp_path / "heartbeat_execution.lock"
    assert runtime.execution_lock_path.exists()


def test_control_gate_runs_before_judge(tmp_path):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, controls, bus, _cadence = engine(tmp_path, judge, sink)
    controls.put(feature="heartbeat", mode="pause", source="operator")

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert (result.status, judge.calls, sink.deliveries, sink.wakes) == (
        "skipped",
        0,
        0,
        0,
    )
    assert bus.read_audit()[-1].payload["status"] == "skipped"


def test_judge_error_is_fail_closed_and_audited(tmp_path):
    judge = Judge(error=RuntimeError("fixture"))
    sink = Sink()
    runtime, _controls, bus, _cadence = engine(tmp_path, judge, sink)

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert (result.status, result.reason) == ("failed", "judge_error:RuntimeError")
    assert sink.deliveries == sink.wakes == 0
    assert bus.read_audit()[-1].payload["status"] == "failed"


def test_effect_failure_is_not_reported_as_success(tmp_path):
    judge = Judge(JudgeDecision(True, True, "contact", "hello"))
    sink = Sink(delivery_ok=True, wake_ok=False)
    runtime, _controls, _bus, _cadence = engine(tmp_path, judge, sink)

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert (result.status, result.reason) == ("failed", "effect_failed")
    assert sink.deliveries == sink.wakes == 1


def test_effect_exception_still_produces_terminal_audit(tmp_path):
    judge = Judge(JudgeDecision(True, True, "contact", "hello"))
    sink = Sink(delivery_error=RuntimeError("fixture"))
    runtime, _controls, bus, _cadence = engine(tmp_path, judge, sink)

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert (result.status, result.reason) == ("failed", "effect_failed")
    assert result.delivery.status == "delivery_error:RuntimeError"
    assert result.wake.ok is True
    assert bus.read_audit()[-1].payload["status"] == "failed"


def test_accepted_but_unverified_wake_is_pending(tmp_path):
    judge = Judge(JudgeDecision(True, False, "contact", "untrusted text"))
    sink = Sink(wake_verified=False)
    runtime, _controls, bus, _cadence = engine(tmp_path, judge, sink)

    result = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))

    assert (result.status, result.reason) == (
        "pending",
        "effects_accepted_unverified",
    )
    assert result.wake.ok is True
    assert result.wake.verified is False
    assert bus.read_audit()[-1].payload["status"] == "pending"


def test_manual_snooze_bypass_is_policy_scoped(tmp_path):
    judge = Judge(JudgeDecision(False, False, "silent"))
    sink = Sink()
    policies = {
        "care_poke": {
            "enabled": True,
            "profile": "routine",
            "judge": "required",
            "host_only": False,
            "bypass": [],
        },
        "critical_ops": {
            "enabled": True,
            "profile": "urgent",
            "judge": "required",
            "host_only": True,
            "bypass": ["manual_snooze"],
        },
    }
    runtime, _controls, _bus, cadence = engine(
        tmp_path, judge, sink, kind_policies=policies
    )
    cadence.snooze(60, manual=True)

    blocked = runtime.run(HeartbeatCandidate("care_poke", {"events": ["fixture"]}))
    passthrough = runtime.run(
        HeartbeatCandidate("critical_ops", {"events": ["fixture"]})
    )

    assert (blocked.status, blocked.reason) == ("skipped", "manual_snooze")
    assert passthrough.reason == "silent"
    assert judge.calls == 1


def test_naive_cadence_timestamp_is_normalized_to_state_error(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    atomic_json_write(
        cadence.path,
        {"auto_until": "2026-08-22T20:00:00", "manual_until": None},
    )

    with pytest.raises(StateError, match="invalid timestamp"):
        cadence.blocked("care_poke")


def test_expired_candidate_skips_before_judge(tmp_path):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    runtime, _controls, _bus, _cadence = engine(tmp_path, judge, Sink())

    result = runtime.run(
        HeartbeatCandidate(
            "check_in",
            {"expires_at": "2026-08-22T18:59:00+00:00"},
        )
    )

    assert (result.status, result.reason, judge.calls) == (
        "skipped",
        "candidate_expired",
        0,
    )


def test_overlapping_heartbeat_does_not_duplicate_effects(tmp_path):
    started = threading.Event()
    release = threading.Event()

    class BlockingSink(Sink):
        def deliver(self, candidate, decision):
            self.deliveries += 1
            started.set()
            assert release.wait(timeout=5)
            return EffectResult(True, "sent", verified=True)

    judge = Judge(JudgeDecision(True, True, "contact", "hello"))
    sink = BlockingSink()
    runtime, _controls, _bus, _cadence = engine(tmp_path, judge, sink)
    candidate = HeartbeatCandidate(
        "care_poke", {"events": ["fixture"]}, candidate_id="candidate_fixture"
    )
    first_results = []
    worker = threading.Thread(
        target=lambda: first_results.append(runtime.run(candidate))
    )
    worker.start()
    assert started.wait(timeout=5)

    overlapping = runtime.run(candidate)
    release.set()
    worker.join(timeout=5)

    assert worker.is_alive() is False
    # A provider's legacy verified hint is not a delivery receipt.
    assert first_results[0].status == "pending"
    assert (overlapping.status, overlapping.reason) == (
        "skipped",
        "execution_in_progress",
    )
    assert (sink.deliveries, sink.wakes, judge.calls) == (1, 1, 1)


def test_no_event_and_not_due_are_neutral_typed_skips(tmp_path):
    judge = Judge(JudgeDecision(True, False, "would_run"))
    runtime, _controls, _bus, _cadence = engine(tmp_path, judge, Sink())

    no_event = runtime.run(HeartbeatCandidate("care_poke", {"events": []}))
    not_due = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["event"], "due": False})
    )

    assert (no_event.status, no_event.reason_code) == (
        "neutral",
        HeartbeatReasonCode.NO_EVENT,
    )
    assert (not_due.status, not_due.reason_code, judge.calls) == (
        "skipped",
        HeartbeatReasonCode.NOT_DUE,
        0,
    )


def test_no_event_reconciles_global_pending_before_neutral(tmp_path):
    class QueueSink:
        def deliver(self, candidate, decision, intent=None):
            return EffectResult(True, "queued")

    runtime, _controls, bus, _cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, True, "contact", "hello")),
        QueueSink(),
    )
    first = runtime.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["event"], "due": True, "source_event_id": "source"},
        )
    )
    pending = runtime.run(
        HeartbeatCandidate("care_poke", {"events": []}, candidate_id="no-event")
    )

    assert first.status == "pending"
    assert (pending.status, pending.reason) == ("pending", "awaiting_receipt")
    assert pending.snapshot["pending_effects"]
    assert len(bus.read_audit()) == 2


def test_pristine_status_and_neutral_probe_do_not_create_state_files(tmp_path):
    runtime, _controls, bus, cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, False, "silent")),
        Sink(),
    )
    before = sorted(path.name for path in tmp_path.iterdir())

    snapshot = runtime.status()
    no_event = runtime.run(HeartbeatCandidate("care_poke", {"events": []}))
    cadence.snapshot()
    cadence.recent_contact()
    cadence.daily_anchor_due()
    cadence.next_judge_at()

    after = sorted(path.name for path in tmp_path.iterdir())
    assert before == after == []
    assert snapshot["pending_effects"] == []
    assert no_event.status == "neutral"
    assert bus.read_audit() == []


def test_kind_without_event_evidence_is_neutral_without_persistent_work(tmp_path):
    locks = FakeLocks()
    judge = Judge(JudgeDecision(True, False, "would_run"))
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=PathlessControls(),
        cadence=PathlessCadence(),
        judge=judge,
        sink=Sink(),
        locks=locks,
    )

    result = runtime.run(HeartbeatCandidate("care_poke"))

    assert (result.status, result.reason_code, judge.calls) == (
        "neutral",
        HeartbeatReasonCode.NO_EVENT,
        0,
    )
    assert list(tmp_path.iterdir()) == []


def test_automatic_cooldown_and_active_chat_gate_before_judge(tmp_path):
    judge = Judge(JudgeDecision(True, False, "would_run"))
    runtime, _controls, _bus, cadence = engine(tmp_path, judge, Sink())
    cadence.snooze(60, manual=False)

    cooldown = runtime.run(HeartbeatCandidate("care_poke", {"events": ["event"]}))
    cadence.resume()
    active = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["event"], "active_chat": True})
    )

    assert cooldown.reason_code is HeartbeatReasonCode.COOLDOWN
    assert active.reason_code is HeartbeatReasonCode.ACTIVE_CHAT
    assert judge.calls == 0


def _policy(
    *, enabled=True, profile="routine", judge="required", host_only=False, bypass=None
):
    return {
        "enabled": enabled,
        "profile": profile,
        "judge": judge,
        "host_only": host_only,
        "bypass": [] if bypass is None else bypass,
    }


@pytest.mark.parametrize(
    "bypass,recent,active,expected_reason,expected_calls",
    [
        ([], True, False, "recent_private_inbound", 0),
        ([], False, True, "active_chat", 0),
        (["recent_contact"], True, False, "silent", 1),
        (["active_chat"], False, True, "silent", 1),
        (["recent_contact"], True, True, "active_chat", 0),
        (["active_chat"], True, True, "recent_private_inbound", 0),
        (["recent_contact", "active_chat"], True, True, "silent", 1),
    ],
)
def test_contact_guard_bypass_is_explicit_and_policy_scoped(
    tmp_path,
    bypass,
    recent,
    active,
    expected_reason,
    expected_calls,
):
    judge = Judge(JudgeDecision(False, False, "silent"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            "urgent_signal": _policy(
                profile="urgent",
                host_only=True,
                bypass=bypass,
            )
        },
    )
    context = {"events": ["fixture"], "due": True, "active_chat": active}
    if recent:
        context["recent_private_inbound_at"] = NOW

    result = runtime.run(HeartbeatCandidate("urgent_signal", context))

    assert (result.reason, judge.calls) == (expected_reason, expected_calls)
    assert sink.deliveries == sink.wakes == 0


def test_daily_anchor_profile_owns_due_state_without_context_flags(tmp_path):
    judge = Judge(JudgeDecision(False, False, "anchor_seen"))
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        judge,
        Sink(),
        kind_policies={
            "day_open": _policy(
                profile="daily_anchor",
                host_only=True,
            )
        },
    )
    candidate = HeartbeatCandidate(
        "day_open",
        {},
        candidate_id="fixture-day-open",
    )

    first = runtime.run(candidate)
    replay = runtime.run(
        HeartbeatCandidate(
            "day_open",
            {
                # Legacy hints cannot force an already-completed durable
                # anchor to become due again.
                "daily_anchor": True,
                "anchor_completed_for_epoch": False,
                "due": True,
            },
            candidate_id="fixture-day-open-replay",
        )
    )

    assert (first.status, first.reason) == ("skipped", "anchor_seen")
    assert (replay.status, replay.reason_code) == (
        "neutral",
        HeartbeatReasonCode.NO_EVENT,
    )
    assert judge.calls == 1
    assert cadence.snapshot()["daily_anchor_epochs"] == {"day_open": "2026-08-22"}


@pytest.mark.parametrize(
    "kind,policy,context",
    [
        (
            "routine",
            _policy(),
            {"events": ["event"], "daily_anchor": True},
        ),
        (
            "day_open",
            _policy(profile="daily_anchor", host_only=True),
            {"daily_anchor": False},
        ),
    ],
)
def test_candidate_context_cannot_change_configured_anchor_profile(
    tmp_path, kind, policy, context
):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={kind: policy},
    )

    result = runtime.run(HeartbeatCandidate(kind, context))

    assert (result.status, result.reason) == ("failed", "heartbeat_input_error")
    assert (judge.calls, sink.deliveries, sink.wakes) == (0, 0, 0)


def test_unknown_and_disabled_kind_fail_closed_before_judge_or_effect(tmp_path):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            "disabled": _policy(enabled=False),
            "invalid": _policy(bypass=["automatic_cooldown"]),
        },
    )

    unknown = runtime.run(HeartbeatCandidate("unknown", {"events": ["event"]}))
    disabled = runtime.run(HeartbeatCandidate("disabled", {"events": ["event"]}))
    invalid = runtime.run(HeartbeatCandidate("invalid", {"events": ["event"]}))

    assert unknown.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
    assert disabled.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
    assert invalid.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
    assert (unknown.reason, disabled.reason, invalid.reason) == (
        "kind_unconfigured",
        "kind_disabled",
        "kind_invalid",
    )
    assert (judge.calls, sink.deliveries, sink.wakes) == (0, 0, 0)


def test_direct_policy_descriptors_require_exact_config_shape(tmp_path):
    routine = _policy()
    missing = _policy()
    missing.pop("host_only")
    invalid_descriptors = [
        {**routine, "extra": True},
        missing,
        {**routine, "bypass": ("manual_snooze",)},
        {**routine, "bypass": "manual_snooze"},
        {**routine, "bypass": ["manual_snooze", "manual_snooze"]},
    ]
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            f"invalid_{index}": descriptor
            for index, descriptor in enumerate(invalid_descriptors)
        },
    )

    results = [
        runtime.run(
            HeartbeatCandidate(
                f"invalid_{index}",
                {"events": ["event"], "due": True},
            )
        )
        for index in range(len(invalid_descriptors))
    ]

    assert all(
        result.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
        for result in results
    )
    assert (judge.calls, sink.deliveries, sink.wakes) == (0, 0, 0)


@pytest.mark.parametrize("kind", ["Bad", "bad kind", "-bad", "bad/bad", "a" * 65])
def test_invalid_kind_syntax_is_candidate_invalid_before_judge(tmp_path, kind):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(tmp_path, judge, sink)

    result = runtime.run(HeartbeatCandidate(kind, {"events": ["event"], "due": True}))

    assert result.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
    assert (judge.calls, sink.deliveries, sink.wakes) == (0, 0, 0)


def test_routine_host_only_policy_is_valid_for_host_execution(tmp_path):
    judge = Judge(JudgeDecision(False, False, "silent"))
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        Sink(),
        kind_policies={"routine": _policy(host_only=True)},
    )

    result = runtime.run(
        HeartbeatCandidate("routine", {"events": ["event"], "due": True})
    )

    assert result.reason == "silent"
    assert judge.calls == 1


def test_cooldown_bypass_is_exact_and_context_cannot_remove_configured_block(
    tmp_path,
):
    judge = Judge(JudgeDecision(False, False, "silent"))
    sink = Sink()
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            "manual": _policy(
                profile="urgent",
                host_only=True,
                bypass=["manual_snooze"],
            )
        },
    )
    cadence.snooze(60, manual=True)
    bypassed = runtime.run(
        HeartbeatCandidate(
            "manual", {"events": ["event"], "cooldown": False, "due": True}
        )
    )
    assert bypassed.reason == "silent"

    cadence.resume()
    cadence.snooze(60, manual=False)
    blocked = runtime.run(
        HeartbeatCandidate(
            "manual", {"events": ["event"], "cooldown": False, "due": True}
        )
    )
    assert blocked.reason == "effect_cooldown"

    runtime, _controls, _bus, cadence = engine(
        tmp_path / "extra",
        judge,
        sink,
        kind_policies={"routine": _policy()},
    )
    extra = runtime.run(
        HeartbeatCandidate(
            "routine", {"events": ["event"], "cooldown": True, "due": True}
        )
    )
    assert extra.reason == "effect_cooldown"


def test_unsupported_cadence_bypass_fails_closed(tmp_path):
    class LegacyCadence:
        def blocked(self, _kind):
            return False, "open"

        def clock(self):
            return NOW

    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=PathlessControls(),
        cadence=LegacyCadence(),
        judge=judge,
        sink=Sink(),
        locks=FakeLocks(),
        kind_policies={
            "routine": _policy(
                profile="urgent",
                host_only=True,
                bypass=["manual_snooze"],
            )
        },
    )

    result = runtime.run(HeartbeatCandidate("routine", {"events": ["event"]}))

    assert result.reason_code is HeartbeatReasonCode.CANDIDATE_INVALID
    assert judge.calls == 0


def test_maintenance_skip_is_allowed_without_judge_or_effect(tmp_path):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            "maintenance": _policy(
                profile="maintenance",
                judge="skip",
                host_only=True,
            )
        },
    )

    result = runtime.run(
        HeartbeatCandidate(
            "maintenance",
            {"events": ["maintenance"], "due": True},
        )
    )

    assert (result.status, result.reason) == ("allowed", "maintenance")
    assert result.decision.maintenance is True
    assert result.effects == ()
    assert (judge.calls, sink.deliveries, sink.wakes) == (0, 0, 0)


def test_maintenance_required_uses_judge_and_effect(tmp_path):
    judge = Judge(JudgeDecision(True, False, "maintenance_wake"))
    sink = Sink()
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        sink,
        kind_policies={
            "maintenance": _policy(
                profile="maintenance",
                judge="required",
                host_only=True,
            )
        },
    )

    result = runtime.run(
        HeartbeatCandidate(
            "maintenance",
            {"events": ["maintenance"], "due": True},
        )
    )

    assert result.status in {"completed", "pending"}
    assert result.wake is not None
    assert judge.calls == 1
    assert sink.wakes == 1


def test_maintenance_skip_still_respects_active_chat(tmp_path):
    judge = Judge(JudgeDecision(True, True, "would_run", "hello"))
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        judge,
        Sink(),
        kind_policies={
            "maintenance": _policy(
                profile="maintenance",
                judge="skip",
                host_only=True,
            )
        },
    )

    result = runtime.run(
        HeartbeatCandidate(
            "maintenance",
            {"events": ["maintenance"], "due": True, "active_chat": True},
        )
    )

    assert result.reason_code is HeartbeatReasonCode.ACTIVE_CHAT
    assert judge.calls == 0


def test_only_unique_fresh_private_contact_is_recent(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)

    assert not cadence.record_private_contact(
        source_id="assistant-1",
        source_kind="assistant_response",
    )
    assert cadence.recent_contact(now=NOW) == (None, None)
    assert cadence.record_private_contact(source_id="private-1")
    assert not cadence.record_private_contact(source_id="private-1")

    kind, observed = cadence.recent_contact(now=NOW)
    assert kind == "recent_private_inbound"
    assert observed == NOW
    assert cadence.snapshot()["private_contact_sources"] == ["private-1"]
    assert cadence.snapshot()["verified_visible_effects"] == []


def test_session_hook_receipt_refreshes_private_contact_only(tmp_path):
    session = SessionLifecycleStore(tmp_path)
    receipt = session.record_hook(
        SessionContext(
            session_id="session-1",
            lifecycle_id="lifecycle-1",
            source_id="private-event-1",
            source_kind="private_inbound",
            observed_at=NOW,
            fresh=True,
            supported_hooks=frozenset({"pre_gateway_dispatch"}),
        ),
        "pre_gateway_dispatch",
    )
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)

    assert cadence.record_private_contact(receipt)
    assert cadence.snapshot()["private_contact_sources"] == ["private-event-1"]


def test_daily_anchor_is_durable_and_schedules_next_anchor(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW, anchor_hour=6)
    epoch = cadence.daily_anchor_epoch(NOW)

    cadence.mark_judge(now=NOW, anchor_epoch=epoch)
    snapshot = cadence.snapshot(now=NOW)

    assert snapshot["daily_anchor_epoch"] == epoch
    assert snapshot["daily_anchor_completed"] is True
    assert snapshot["next_judge_at"].startswith("2026-08-23T06:00:00")


def test_daily_anchor_state_is_independent_per_kind(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW, anchor_hour=6)
    epoch = cadence.daily_anchor_epoch(NOW)

    assert cadence.daily_anchor_due(NOW, kind="day_open") is True
    assert cadence.daily_anchor_due(NOW, kind="day_close") is True

    cadence.mark_daily_anchor(epoch, kind="day_open")

    assert cadence.daily_anchor_due(NOW, kind="day_open") is False
    assert cadence.daily_anchor_due(NOW, kind="day_close") is True

    cadence.mark_judge(now=NOW, anchor_epoch=epoch, anchor_kind="day_close")
    snapshot = cadence.snapshot(now=NOW)

    assert cadence.daily_anchor_due(NOW, kind="day_open") is False
    assert cadence.daily_anchor_due(NOW, kind="day_close") is False
    assert snapshot["daily_anchor_epochs"] == {
        "day_close": epoch,
        "day_open": epoch,
    }
    assert json.loads(cadence.path.read_text(encoding="utf-8"))["schema_version"] == (
        "moon.heartbeat.cadence.v4"
    )


def test_heartbeat_engine_settles_exact_daily_anchor_kind(tmp_path):
    judge = Judge(JudgeDecision(False, False, "silent"))
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        judge,
        Sink(),
        kind_policies={
            "day_open": _policy(profile="daily_anchor", host_only=True),
            "day_close": _policy(profile="daily_anchor", host_only=True),
        },
    )

    first = runtime.run(HeartbeatCandidate("day_open", {}))
    second = runtime.run(HeartbeatCandidate("day_close", {}))

    assert (first.status, second.status, judge.calls) == (
        "skipped",
        "skipped",
        2,
    )
    assert cadence.snapshot()["daily_anchor_epochs"] == {
        "day_close": "2026-08-22",
        "day_open": "2026-08-22",
    }


def test_daily_anchor_gates_and_judge_failure_do_not_settle(tmp_path):
    judge = Judge(error=RuntimeError("fixture"))
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        judge,
        Sink(),
        kind_policies={
            "day_open": _policy(profile="daily_anchor", host_only=True),
        },
    )
    cadence.snooze(60, manual=True)

    snoozed = runtime.run(HeartbeatCandidate("day_open", {}))
    cadence.resume()
    active = runtime.run(HeartbeatCandidate("day_open", {"active_chat": True}))
    recent = runtime.run(
        HeartbeatCandidate("day_open", {"recent_private_inbound_at": NOW})
    )
    failed = runtime.run(HeartbeatCandidate("day_open", {}))

    assert [result.reason for result in (snoozed, active, recent)] == [
        "manual_snooze",
        "active_chat",
        "recent_private_inbound",
    ]
    assert (failed.status, failed.reason, judge.calls) == (
        "failed",
        "judge_error:RuntimeError",
        1,
    )
    assert cadence.snapshot()["daily_anchor_epochs"] == {}
    assert cadence.daily_anchor_due(NOW, kind="day_open") is True


def test_daily_anchor_effect_failure_does_not_reopen_kind(tmp_path):
    judge = Judge(JudgeDecision(False, True, "contact", "hello"))
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        judge,
        Sink(delivery_ok=False),
        kind_policies={
            "day_open": _policy(profile="daily_anchor", host_only=True),
            "day_close": _policy(profile="daily_anchor", host_only=True),
        },
    )

    failed = runtime.run(HeartbeatCandidate("day_open", {}))
    duplicate = runtime.run(HeartbeatCandidate("day_open", {}))

    assert (failed.status, failed.reason) == ("failed", "effect_failed")
    assert (duplicate.status, duplicate.reason_code, judge.calls) == (
        "neutral",
        HeartbeatReasonCode.NO_EVENT,
        1,
    )
    assert cadence.daily_anchor_due(NOW, kind="day_open") is False
    assert cadence.daily_anchor_due(NOW, kind="day_close") is True


@pytest.mark.parametrize(
    "schema_version",
    (
        "moon.heartbeat.cadence.v1",
        "moon.heartbeat.cadence.v2",
        "moon.heartbeat.cadence.v3",
    ),
)
def test_legacy_daily_anchor_completion_is_a_migrated_wildcard(
    tmp_path, schema_version
):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW, anchor_hour=6)
    cadence.path.write_text(
        json.dumps(
            {
                "schema_version": schema_version,
                "daily_anchor_epoch": "2026-08-22",
                "daily_anchor_completed": True,
            }
        ),
        encoding="utf-8",
    )

    assert cadence.daily_anchor_due(NOW, kind="day_open") is False
    assert cadence.daily_anchor_due(NOW, kind="day_close") is False
    assert cadence.snapshot(now=NOW)["daily_anchor_legacy_epoch"] == "2026-08-22"

    cadence.mark_daily_anchor(
        "2026-08-23", kind="day_open", now=NOW + timedelta(days=1)
    )
    snapshot = cadence.snapshot(now=NOW + timedelta(days=1))

    assert snapshot["daily_anchor_epochs"] == {"day_open": "2026-08-23"}
    assert cadence.daily_anchor_due(NOW + timedelta(days=1), kind="day_close") is True
    assert json.loads(cadence.path.read_text(encoding="utf-8"))["schema_version"] == (
        "moon.heartbeat.cadence.v4"
    )


def test_daily_anchor_mapping_validation_fails_closed(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(
        json.dumps(
            {
                "schema_version": "moon.heartbeat.cadence.v4",
                "daily_anchor_epochs": {"Day_Open": "2026-08-22"},
                "daily_anchor_legacy_epoch": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(StateError, match="daily anchor state"):
        cadence.daily_anchor_due(NOW, kind="day_open")

    for fields in (
        {
            "daily_anchor_epochs": {
                f"kind_{index}": "2026-08-22" for index in range(129)
            },
            "daily_anchor_legacy_epoch": None,
        },
        {
            "daily_anchor_epochs": {},
            "daily_anchor_legacy_epoch": None,
            "daily_anchor_epoch": "2026-08-22",
            "daily_anchor_completed": True,
        },
        {
            "daily_anchor_epochs": {},
            "daily_anchor_legacy_epoch": None,
            "daily_anchor_unknown": True,
        },
    ):
        cadence.path.write_text(
            json.dumps({"schema_version": "moon.heartbeat.cadence.v4", **fields}),
            encoding="utf-8",
        )
        with pytest.raises(StateError):
            cadence.daily_anchor_due(NOW, kind="day_open")

    with pytest.raises(ValueError, match="strict ISO date"):
        cadence.mark_judge(
            now=NOW,
            next_judge_at=NOW + timedelta(hours=1),
            anchor_epoch="2026-8-22",
            anchor_kind="day_open",
        )


def test_cadence_observer_projects_exact_anchor_kinds(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(
        json.dumps(
            {
                "schema_version": "moon.heartbeat.cadence.v4",
                "daily_anchor_epochs": {
                    "day_open": "2026-08-22",
                    "day_close": "2026-08-21",
                },
                "daily_anchor_legacy_epoch": None,
            }
        ),
        encoding="utf-8",
    )

    facts = {
        fact.key: fact
        for fact in cadence.observer_status(target_date=NOW.date(), now=NOW)
    }

    assert facts["heartbeat:anchor:day_open"].code == "heartbeat_anchor_completed"
    assert "completion:exact" in facts["heartbeat:anchor:day_open"].refs
    assert facts["heartbeat:anchor:day_close"].code == (
        "heartbeat_anchor_outside_target"
    )


def test_queued_effect_is_pending_and_duplicate_reuses_intent(tmp_path):
    class QueueSink:
        def __init__(self):
            self.calls = 0
            self.intents = []

        def wake(self, candidate, decision, intent=None):
            self.calls += 1
            self.intents.append(intent)
            return EffectResult(True, "queued")

    judge = Judge(JudgeDecision(True, False, "contact"))
    sink = QueueSink()
    runtime, _controls, _bus, cadence = engine(tmp_path, judge, sink)
    candidate = HeartbeatCandidate(
        "care_poke",
        {"events": ["event"], "due": True, "source_event_id": "source-1"},
        candidate_id="candidate-1",
    )

    first = runtime.run(candidate)
    duplicate = runtime.run(candidate)
    record = runtime.effect_ledger.get(first.wake.effect_id)

    assert (first.status, first.wake.terminal, first.wake.verified) == (
        "pending",
        "executed_unverified",
        False,
    )
    assert (duplicate.status, duplicate.reason) == ("pending", "awaiting_receipt")
    assert (sink.calls, judge.calls) == (1, 1)
    assert record is not None
    assert record.content_sha256 and record.content_length > 0
    assert "message" not in record.to_dict()
    assert cadence.snapshot()["verified_visible_effects"] == []


def test_visible_contact_projection_rejects_cross_domain_record(tmp_path):
    effects = EffectLedger(tmp_path / "effects", clock=lambda: NOW)
    intent = effects.begin_intent(
        kind="memory_checkpoint",
        source_event_id="memory-source",
        idempotency_key="memory:checkpoint",
        epoch_id="epoch",
        content_sha256="a" * 64,
        content_length=1,
        created_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    effects.mark_pending(intent.effect_id)
    receipt = EffectReceipt(
        receipt_id="memory-receipt",
        event_id="memory-source",
        observed_at=NOW,
        content_sha256="a" * 64,
        content_length=1,
        epoch_id="epoch",
    )
    verified = effects.verify(intent.effect_id, receipt)
    cadence = HeartbeatCadence(tmp_path / "cadence", clock=lambda: NOW)

    assert cadence.record_verified_visible_contact(verified, receipt) is False
    assert cadence.snapshot()["verified_visible_effects"] == []


def test_delivery_only_refreshes_visible_contact_wake_only_does_not(tmp_path):
    class ReceiptSink:
        def __init__(self, mismatch=False):
            self.mismatch = mismatch

        @staticmethod
        def _receipt(intent, event_id):
            return EffectReceipt(
                receipt_id=f"receipt-{intent.kind}",
                event_id=event_id,
                observed_at=NOW,
                content_sha256=intent.content_sha256,
                content_length=intent.content_length,
                epoch_id=intent.epoch_id,
            )

        def deliver(self, candidate, decision, intent=None):
            event_id = "wrong-source" if self.mismatch else intent.source_event_id
            return self._receipt(intent, event_id)

        def wake(self, candidate, decision, intent=None):
            return self._receipt(intent, intent.source_event_id)

    candidate = HeartbeatCandidate(
        "care_poke",
        {"events": ["event"], "due": True, "source_event_id": "source-1"},
        candidate_id="candidate-1",
    )
    wake_runtime, _controls, _bus, wake_cadence = engine(
        tmp_path / "wake",
        Judge(JudgeDecision(True, False, "contact")),
        ReceiptSink(),
    )
    wake = wake_runtime.run(candidate)
    assert (wake.status, wake.wake.verified) == ("completed", True)
    assert wake_cadence.snapshot()["verified_visible_effects"] == []

    delivery_runtime, _controls, _bus, delivery_cadence = engine(
        tmp_path / "delivery",
        Judge(JudgeDecision(False, True, "contact", "hello")),
        ReceiptSink(),
    )
    delivery = delivery_runtime.run(candidate)
    assert (delivery.status, delivery.delivery.verified) == ("completed", True)
    assert delivery_cadence.snapshot()["verified_visible_effects"]

    mixed_runtime, _controls, _bus, mixed_cadence = engine(
        tmp_path / "mixed",
        Judge(JudgeDecision(True, True, "contact", "hello")),
        ReceiptSink(),
    )
    mixed = mixed_runtime.run(candidate)
    assert (mixed.status, mixed.delivery.verified, mixed.wake.verified) == (
        "completed",
        True,
        True,
    )
    assert len(mixed_cadence.snapshot()["verified_visible_effects"]) == 1

    mismatch_runtime, _controls, _bus, mismatch_cadence = engine(
        tmp_path / "mismatch",
        Judge(JudgeDecision(False, True, "contact", "hello")),
        ReceiptSink(mismatch=True),
    )
    mismatch = mismatch_runtime.run(candidate)
    assert (mismatch.status, mismatch.reason_code) == (
        "failed",
        HeartbeatReasonCode.EFFECT_ERROR,
    )
    assert mismatch_cadence.snapshot()["verified_visible_effects"] == []


def test_expired_pending_is_requeued_without_replay(tmp_path):
    current = [NOW]

    class QueueSink:
        def __init__(self):
            self.calls = 0

        def wake(self, candidate, decision, intent=None):
            self.calls += 1
            return EffectResult(True, "queued")

    cadence = HeartbeatCadence(
        tmp_path,
        clock=lambda: current[0],
        effect_ttl=timedelta(minutes=1),
    )
    ledger = EffectLedger(tmp_path, clock=lambda: current[0])
    sink = QueueSink()
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: current[0]),
        controls=ControlStore(tmp_path, clock=lambda: current[0]),
        cadence=cadence,
        judge=Judge(JudgeDecision(True, False, "contact")),
        sink=sink,
        effect_ledger=ledger,
    )
    candidate = HeartbeatCandidate(
        "care_poke",
        {"events": ["event"], "due": True, "source_event_id": "source-1"},
        candidate_id="candidate-1",
    )

    first = runtime.run(candidate)
    current[0] += timedelta(minutes=2)
    expired = runtime.run(candidate)

    assert first.wake.effect_id is not None
    assert (expired.status, expired.reason) == ("requeued", "expired_effect")
    assert sink.calls == 1
    assert cadence.snapshot()["effect_terminals"][first.wake.effect_id] == "requeued"


def test_malformed_judge_and_unavailable_adapter_fail_closed(tmp_path):
    class MalformedJudge:
        def decide(self, candidate):
            return {"wake_main": True, "dm_user": False, "reason": "bad", "extra": 1}

    malformed_runtime, _controls, _bus, _cadence = engine(
        tmp_path / "judge", MalformedJudge(), Sink()
    )
    malformed = malformed_runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["event"], "due": True})
    )
    assert malformed.reason_code is HeartbeatReasonCode.JUDGE_MALFORMED

    unavailable_runtime, _controls, _bus, _cadence = engine(
        tmp_path / "adapter",
        Judge(JudgeDecision(True, False, "contact")),
        Sink(),
    )
    unavailable_runtime.sink = type(
        "Unavailable",
        (),
        {
            "wake": lambda self, candidate, decision, intent=None: EffectResult(
                False, "unavailable"
            )
        },
    )()
    unavailable = unavailable_runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["event"], "due": True})
    )
    assert unavailable.reason_code is HeartbeatReasonCode.ADAPTER_UNAVAILABLE


def test_non_heartbeat_pending_and_expired_effects_do_not_block_or_reconcile(tmp_path):
    current = [NOW]
    ledger = EffectLedger(tmp_path, clock=lambda: current[0])
    external = ledger.begin_intent(
        kind="memory_checkpoint",
        source_event_id="memory-source",
        idempotency_key="memory:checkpoint:1",
        epoch_id="memory",
        content_sha256="a" * 64,
        content_length=1,
        created_at=NOW - timedelta(minutes=5),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending(external.effect_id)
    cadence = HeartbeatCadence(tmp_path, clock=lambda: current[0])
    judge = Judge(JudgeDecision(False, False, "silent", allow_autonomy=True))
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: current[0]),
        controls=ControlStore(tmp_path, clock=lambda: current[0]),
        cadence=cadence,
        judge=judge,
        sink=Sink(),
        effect_ledger=ledger,
    )

    result = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["heartbeat-event"], "due": True})
    )

    unchanged = ledger.get(external.effect_id)
    assert (result.status, result.reason) == ("allowed", "allowed")
    assert judge.calls == 1
    assert unchanged is not None
    assert (unchanged.state, unchanged.attempt, unchanged.expires_at) == (
        "pending",
        1,
        NOW - timedelta(minutes=1),
    )
    assert all(
        item["effect_id"] != external.effect_id
        for item in runtime.status()["pending_effects"]
    )


def test_audit_redacts_result_and_judge_text_but_keeps_reason_code(tmp_path):
    reason = "HEARTBEAT_REASON_SECRET_MARKER"
    message = "HEARTBEAT_MESSAGE_SECRET_MARKER"
    runtime, _controls, bus, _cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, False, reason, message)),
        Sink(),
    )

    result = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["heartbeat-event"], "due": True})
    )
    audit = bus.read_audit()[-1].payload

    assert result.reason == reason
    assert audit["reason_code"] == HeartbeatReasonCode.DENIED.value
    assert "reason" not in audit
    assert audit["reason_length"] == len(reason.encode("utf-8"))
    assert "reason" not in audit["decision"]
    assert "message" not in audit["decision"]
    assert audit["decision"]["reason_length"] == len(reason.encode("utf-8"))
    assert audit["decision"]["message_length"] == len(message.encode("utf-8"))
    raw = (tmp_path / "audit.jsonl").read_text(encoding="utf-8")
    assert reason not in raw
    assert message not in raw


def test_audit_and_effect_state_never_persist_message_body(tmp_path):
    private_body = "HEARTBEAT_PRIVATE_BODY_MARKER_9e52"
    runtime, _controls, bus, _cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, True, "contact", private_body)),
        Sink(delivery_verified=False),
    )
    result = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["heartbeat-event"], "due": True})
    )

    assert result.status == "pending"
    assert private_body not in "\n".join(
        path.read_text(encoding="utf-8")
        for path in tmp_path.rglob("*")
        if path.is_file()
    )
    audit = bus.read_audit()[-1].payload
    assert "message" not in audit["decision"]
    assert audit["decision"]["message_length"] == len(private_body.encode("utf-8"))


def test_future_private_and_visible_contacts_are_rejected(tmp_path):
    cadence = HeartbeatCadence(tmp_path / "cadence", clock=lambda: NOW)
    future = NOW + timedelta(minutes=1)

    assert (
        cadence.record_private_contact(
            source_id="future-private",
            observed_at=future,
            source_kind="private_inbound",
        )
        is False
    )

    effects = EffectLedger(tmp_path / "effects", clock=lambda: NOW)
    intent = effects.begin_intent(
        kind="heartbeat_delivery",
        source_event_id="future-source",
        idempotency_key="heartbeat:future",
        epoch_id="epoch",
        content_sha256="b" * 64,
        content_length=1,
        created_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )
    effects.mark_pending(intent.effect_id)
    receipt = EffectReceipt(
        receipt_id="future-receipt",
        event_id="future-source",
        observed_at=future,
        content_sha256="b" * 64,
        content_length=1,
        epoch_id="epoch",
    )
    verified = effects.verify(intent.effect_id, receipt)

    assert cadence.record_verified_visible_contact(verified, receipt) is False
    snapshot = cadence.snapshot()
    assert snapshot["private_contact_sources"] == []
    assert snapshot["verified_visible_effects"] == []


def test_duplicate_private_source_is_noop_even_with_later_timestamp(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    later = NOW + timedelta(minutes=10)

    assert cadence.record_private_contact(
        source_id="private-source", observed_at=NOW, source_kind="private_inbound"
    )
    assert not cadence.record_private_contact(
        source_id="private-source",
        observed_at=later,
        source_kind="private_inbound",
    )
    for source_kind in (
        "session_start",
        "assistant_response",
        "system",
        "cron",
        "workroom",
        "tool",
    ):
        assert not cadence.record_private_contact(
            source_id=f"ignored-{source_kind}", source_kind=source_kind
        )

    snapshot = cadence.snapshot(now=later)
    assert snapshot["private_contact_sources"] == ["private-source"]
    assert snapshot["last_private_contact_at"] == NOW.isoformat().replace("+00:00", "Z")


def test_contact_read_probes_prune_in_memory_without_writing(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    assert cadence.record_private_contact(
        source_id="expires-in-window", source_kind="private_inbound"
    )
    before = cadence.path.read_bytes()
    before_mtime = cadence.path.stat().st_mtime_ns
    later = NOW + timedelta(minutes=31)

    assert cadence.recent_contact(now=later) == (None, None)
    snapshot = cadence.snapshot(now=later)
    assert snapshot["private_contact_sources"] == []
    assert cadence.path.read_bytes() == before
    assert cadence.path.stat().st_mtime_ns == before_mtime


def test_cadence_retention_is_bounded_with_safe_overflow_rotation(tmp_path):
    current = [NOW]
    cadence = HeartbeatCadence(
        tmp_path,
        clock=lambda: current[0],
        recent_contact_window=timedelta(minutes=30),
    )
    for index in range(128):
        assert cadence.record_private_contact(
            source_id=f"private-{index}",
            observed_at=NOW,
            source_kind="private_inbound",
        )
    assert not cadence.record_private_contact(
        source_id="private-127",
        observed_at=NOW + timedelta(minutes=1),
        source_kind="private_inbound",
    )
    # A new id cannot evict an active exact id.  The bounded overflow marker
    # conservatively keeps the recent-contact gate closed for the window.
    assert cadence.record_private_contact(
        source_id="private-new-at-capacity",
        observed_at=NOW,
        source_kind="private_inbound",
    )
    saturated = cadence.snapshot(now=NOW)
    assert len(saturated["private_contact_sources"]) == 128
    assert saturated["private_contact_overflow_until"] == (
        NOW + timedelta(minutes=30)
    ).isoformat().replace("+00:00", "Z")

    judge = Judge(JudgeDecision(True, False, "would_run"))
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: current[0]),
        controls=ControlStore(tmp_path, clock=lambda: current[0]),
        cadence=cadence,
        judge=judge,
        sink=Sink(),
    )
    blocked = runtime.run(
        HeartbeatCandidate("care_poke", {"events": ["event"], "due": True})
    )
    assert blocked.reason_code is HeartbeatReasonCode.RECENT_CONTACT
    assert judge.calls == 0

    # Rotation clears old exact ids and the overflow marker; a genuinely old
    # source id is no longer retained forever and can be recorded again.
    current[0] += timedelta(minutes=31)
    assert cadence.recent_contact(now=current[0]) == (None, None)
    assert cadence.record_private_contact(
        source_id="private-after-rotation",
        observed_at=current[0],
        source_kind="private_inbound",
    )
    assert cadence.record_private_contact(
        source_id="private-0",
        observed_at=current[0],
        source_kind="private_inbound",
    )
    for index in range(300):
        cadence.record_effect_terminal(f"effect-{index}", "executed_unverified")
        cadence.remember_effect_ref("source", f"heartbeat_wake_{index}", f"id-{index}")

    snapshot = cadence.snapshot()
    assert len(snapshot["private_contact_sources"]) <= 128
    assert len(snapshot["effect_terminals"]) <= 256
    assert snapshot["effect_reference_count"] <= 256


def test_duplicate_pending_uses_public_effect_port_without_private_snapshot(tmp_path):
    class PublicLedger(EffectLedger):
        def _snapshot(self):
            raise AssertionError("heartbeat must not use private ledger replay")

    class QueueSink:
        def __init__(self):
            self.calls = 0

        def wake(self, candidate, decision, intent=None):
            self.calls += 1
            return EffectResult(True, "queued")

    ledger = PublicLedger(tmp_path, clock=lambda: NOW)
    sink = QueueSink()
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=ControlStore(tmp_path, clock=lambda: NOW),
        cadence=HeartbeatCadence(tmp_path, clock=lambda: NOW),
        judge=Judge(JudgeDecision(True, False, "wake")),
        sink=sink,
        effect_ledger=ledger,
    )
    candidate = HeartbeatCandidate(
        "care_poke",
        {"events": ["event"], "due": True, "source_event_id": "source"},
        candidate_id="candidate",
    )

    first = runtime.run(candidate)
    duplicate = runtime.run(candidate)

    assert first.status == "pending"
    assert duplicate.reason == "awaiting_receipt"
    assert sink.calls == 1


def test_projection_failures_are_degraded_without_erasing_verified_truth(tmp_path):
    class ReceiptSink:
        def deliver(self, candidate, decision, intent=None):
            return EffectReceipt(
                receipt_id="receipt",
                event_id=intent.source_event_id,
                observed_at=NOW,
                content_sha256=intent.content_sha256,
                content_length=intent.content_length,
                epoch_id=intent.epoch_id,
            )

    runtime, _controls, bus, cadence = engine(
        tmp_path / "contact",
        Judge(JudgeDecision(False, True, "contact", "hello")),
        ReceiptSink(),
    )
    cadence.record_verified_visible_contact = lambda *args, **kwargs: (
        _ for _ in ()
    ).throw(OSError("contact projection"))
    contact_failure = runtime.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["event"], "due": True, "source_event_id": "source"},
        )
    )
    assert contact_failure.status == "partial"
    assert contact_failure.delivery.verified is True
    assert contact_failure.degraded is True
    assert any(
        "visible_contact_write" in item for item in contact_failure.projection_errors
    )

    queue_runtime, _controls, queue_bus, queue_cadence = engine(
        tmp_path / "terminal",
        Judge(JudgeDecision(True, False, "wake")),
        Sink(wake_verified=False),
    )
    queue_cadence.record_effect_terminal = lambda *args, **kwargs: (
        _ for _ in ()
    ).throw(OSError("terminal projection"))
    terminal_failure = queue_runtime.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["event"], "due": True, "source_event_id": "source"},
        )
    )
    assert terminal_failure.status == "pending"
    assert terminal_failure.degraded is True
    assert any(
        "effect_terminal_write" in item for item in terminal_failure.projection_errors
    )

    audit_runtime, _controls, audit_bus, _cadence = engine(
        tmp_path / "audit",
        Judge(JudgeDecision(False, True, "contact", "hello")),
        ReceiptSink(),
    )
    audit_bus.record_audit = lambda *args, **kwargs: (_ for _ in ()).throw(
        OSError("audit projection")
    )
    audit_failure = audit_runtime.run(
        HeartbeatCandidate(
            "care_poke",
            {"events": ["event"], "due": True, "source_event_id": "source"},
        )
    )
    assert audit_failure.status == "partial"
    assert audit_failure.degraded is True
    assert any("audit_write" in item for item in audit_failure.projection_errors)


def test_observer_status_pristine_and_corrupt_cadence_are_read_only(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)

    assert cadence.observer_status(target_date=NOW.date(), now=NOW) == ()
    assert list(tmp_path.iterdir()) == []

    cadence.path.write_text(
        '{"schema_version":"moon.heartbeat.unknown"}', encoding="utf-8"
    )
    before = cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns
    facts = cadence.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert "integrity" in facts[0].code
    assert (cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns) == before
    assert not cadence.lock_path.exists()


def test_engine_observer_separates_visible_delivery_from_internal_wake(tmp_path):
    runtime, _controls, _bus, _cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, False, "idle")),
        Sink(),
    )
    ledger = runtime.effect_ledger
    delivery = ledger.begin_intent(
        "delivery-observe",
        kind="heartbeat_delivery",
        source_event_id="delivery-source",
        idempotency_key="heartbeat:delivery-source:delivery",
        epoch_id="epoch",
        content_sha256="d" * 64,
        content_length=4,
        expires_at=NOW + timedelta(minutes=5),
    )
    wake = ledger.begin_intent(
        "wake-observe",
        kind="heartbeat_wake",
        source_event_id="wake-source",
        idempotency_key="heartbeat:wake-source:wake",
        epoch_id="epoch",
        content_sha256="e" * 64,
        content_length=4,
        expires_at=NOW + timedelta(minutes=5),
    )
    ledger.mark_pending(delivery.effect_id)
    ledger.mark_pending(wake.effect_id)
    ledger.verify(
        delivery.effect_id,
        EffectReceipt(
            receipt_id="delivery-receipt",
            event_id="delivery-source",
            observed_at=NOW,
            content_sha256="d" * 64,
            content_length=4,
            epoch_id="epoch",
        ),
    )
    runtime.run = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("observer must not run heartbeat")
    )
    runtime._reconcile_pending = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("observer must not reconcile heartbeat")
    )

    facts = runtime.observer_status(target_date=NOW.date(), now=NOW)
    by_key = {fact.key: fact for fact in facts}

    assert by_key["heartbeat:contact:verified_visible"].state == "neutral"
    assert "heartbeat:contact:wake" not in by_key
    assert by_key["heartbeat:effect:wake:wake-observe"].state == "current"
    assert all(
        "expected" not in fact.code and "missed" not in fact.code for fact in facts
    )


@pytest.mark.parametrize("port_kind", ("mapping", "string", "non_iterable", "mixed"))
def test_engine_observer_rejects_malformed_cadence_port(tmp_path, port_kind):
    runtime, _controls, _bus, cadence = engine(
        tmp_path,
        Judge(JudgeDecision(False, False, "idle")),
        Sink(),
    )
    valid = ObservationFact(
        key="fixture:cadence",
        code="fixture_observed",
        state="neutral",
        target_date=NOW.date(),
    )
    results = {
        "mapping": {"fact": valid},
        "string": "malformed",
        "non_iterable": 42,
        "mixed": [valid, "malformed"],
    }
    cadence.observer_status = lambda **_kwargs: results[port_kind]

    facts = runtime.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code.startswith("heartbeat_integrity_error:port_")
    assert "content" not in facts[0].to_dict()
    assert not (tmp_path / "heartbeat_cadence.lock").exists()
    assert not (tmp_path / "effects.jsonl").exists()


@pytest.mark.parametrize(
    "payload",
    (
        "{}",
        '{"schema_version":"moon.heartbeat.cadence.v1"}',
        '{"auto_until":null}',
    ),
)
def test_cadence_observer_reports_existing_legacy_empty_or_partial_state(
    tmp_path, payload
):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(payload, encoding="utf-8")
    before = cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns

    facts = cadence.observer_status(target_date=NOW.date(), now=NOW)
    state = next(fact for fact in facts if fact.key == "heartbeat:cadence:state")

    assert state.state == "neutral"
    assert state.code in {
        "heartbeat_cadence_uninitialized",
        "heartbeat_cadence_observed",
    }
    assert (cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns) == before
    assert not cadence.lock_path.exists()


def test_cadence_observer_marks_stale_anchor_outside_target_without_conclusion(
    tmp_path,
):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(
        '{"daily_anchor_epoch":"2026-08-21","daily_anchor_completed":true}',
        encoding="utf-8",
    )
    before = cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns

    facts = cadence.observer_status(target_date=NOW.date(), now=NOW)
    anchor = next(fact for fact in facts if fact.key == "heartbeat:anchor")

    assert anchor.state == "neutral"
    assert anchor.code == "heartbeat_anchor_outside_target"
    assert "completed" not in anchor.code
    assert "pending" not in anchor.code
    assert "missed" not in anchor.code
    assert "migration:legacy" in anchor.refs
    assert anchor.counts["legacy"] == 1
    assert (cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns) == before
    assert not cadence.lock_path.exists()


def test_cadence_observer_rejects_noncanonical_anchor_epoch(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(
        '{"daily_anchor_epoch":"2026-8-22","daily_anchor_completed":true}',
        encoding="utf-8",
    )
    before = cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns

    facts = cadence.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert "integrity" in facts[0].code
    assert (cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns) == before
    assert not cadence.lock_path.exists()


def test_cadence_observer_rejects_conflicting_effect_reference_kinds(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.write_text(
        '{"effect_refs":'
        '{"heartbeat_delivery:source-a":"effect-1",'
        '"heartbeat_wake:source-b":"effect-1"}}',
        encoding="utf-8",
    )
    before = cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns

    facts = cadence.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert "integrity" in facts[0].code
    assert (cadence.path.read_bytes(), cadence.path.stat().st_mtime_ns) == before
    assert not cadence.lock_path.exists()


def test_engine_observer_missing_cadence_port_is_neutral_and_read_only(tmp_path):
    class MissingCadence:
        def __init__(self, path):
            self.path = path

        def snapshot(self):
            raise AssertionError("observer must not call cadence snapshot")

        def clock(self):
            return NOW

    cadence = MissingCadence(tmp_path / "heartbeat_cadence.json")
    locks = FakeLocks()
    runtime = HeartbeatEngine(
        bus=EventBus(tmp_path, clock=lambda: NOW),
        controls=PathlessControls(),
        cadence=cadence,
        judge=Judge(JudgeDecision(False, False, "idle")),
        sink=Sink(),
        locks=locks,
    )
    before = tuple(
        sorted(
            (path.name, path.stat().st_mtime_ns)
            for path in tmp_path.iterdir()
            if path.is_file()
        )
    )

    facts = runtime.observer_status(target_date=NOW.date(), now=NOW)
    unavailable = next(
        fact for fact in facts if fact.code == "heartbeat_cadence_observer_unavailable"
    )

    assert unavailable.key == "heartbeat:cadence:observer"
    assert unavailable.state == "neutral"
    assert "content" not in unavailable.to_dict()
    assert locks.names == []
    assert not (tmp_path / "effects.jsonl").exists()
    after = tuple(
        sorted(
            (path.name, path.stat().st_mtime_ns)
            for path in tmp_path.iterdir()
            if path.is_file()
        )
    )
    assert after == before
