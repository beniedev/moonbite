from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from moonbite_plugin.autonomy import AutonomyContext
from moonbite_plugin.heartbeat import HeartbeatCandidate, JudgeDecision
from moonbite_plugin.hermes_adapter import (
    HermesDiaryWriter,
    HermesHostAdapter,
    HermesModelReflection,
    HermesSessionWakeSink,
)
from moonbite_plugin.session import HOOK_ORDER, SessionContext, SessionLifecycleSnapshot


HERMES_TURN_END_PAYLOAD = {
    "session_id": "contract-session",
    "task_id": "contract-task",
    "turn_id": "contract-turn",
    "completed": False,
    "failed": True,
    "interrupted": False,
    "turn_exit_reason": "provider_error",
    "model": "contract-model",
    "platform": "cli",
}


class Llm:
    def __init__(self):
        self.calls = []

    def complete(self, **kwargs):
        self.calls.append(("complete", kwargs))
        return SimpleNamespace(
            text="A grounded reflection.", provider="fixture", model="fixture-model"
        )

    def complete_structured(self, **kwargs):
        self.calls.append(("structured", kwargs))
        return SimpleNamespace(
            parsed={"title": "Day", "body": "Grounded body.", "reason": "evidence"}
        )


def test_host_adapter_normalizes_official_turn_end_payload():
    adapter = HermesHostAdapter()

    terminal = adapter.turn_terminal(
        HERMES_TURN_END_PAYLOAD,
        supported_hooks=frozenset(HOOK_ORDER),
    )

    assert terminal.context.session_id == "contract-session"
    assert terminal.context.lifecycle_id == "contract-session"
    assert terminal.context.turn_id == "contract-turn"
    assert terminal.reason == "host_turn_failed"


@pytest.mark.parametrize(
    ("status", "reason"),
    (
        ("interrupted", "host_turn_interrupted"),
        ("failed", "host_turn_failed"),
        ("error", "host_turn_failed"),
        ("timeout", "host_turn_failed"),
    ),
)
def test_host_adapter_normalizes_child_stop_status(status, reason):
    adapter = HermesHostAdapter()

    terminal = adapter.subagent_stop_terminal(
        {
            "child_session_id": "child-session",
            "child_status": status,
            "parent_turn_id": "must-not-be-read",
            "summary": "must-not-be-read",
            "goal": "must-not-be-read",
            "tool_history": "must-not-be-read",
        }
    )

    assert terminal.child_session_id == "child-session"
    assert terminal.reason == reason


def test_host_adapter_child_stop_completed_is_noop():
    adapter = HermesHostAdapter()

    assert (
        adapter.subagent_stop_terminal(
            {"child_session_id": "child-session", "child_status": "completed"}
        )
        is None
    )


def test_host_adapter_child_stop_rejects_unknown_status():
    adapter = HermesHostAdapter()

    with pytest.raises(ValueError, match="unsupported child_status"):
        adapter.subagent_stop_terminal(
            {"child_session_id": "child-session", "child_status": "cancelled"}
        )


def test_host_adapter_correlates_rotated_session_from_durable_turn():
    adapter = HermesHostAdapter()
    context = adapter.session_context(
        "post_llm_call",
        {
            "session_id": "session-new",
            "task_id": "contract-task",
            "turn_id": "contract-turn",
        },
        frozenset(HOOK_ORDER),
    )
    snapshot = SessionLifecycleSnapshot(
        session_id="session-old",
        lifecycle_id="session-old",
        supported_hooks=frozenset(HOOK_ORDER),
        hooks=("on_session_start", "pre_llm_call"),
        settled_turn_ids=(),
        open_turn_id="contract-turn",
        finalized=False,
        private_contact_count=0,
    )

    correlated = adapter.correlate_turn(context, (snapshot,))

    assert correlated.session_id == "session-old"
    assert correlated.lifecycle_id == "session-old"
    assert correlated.turn_id == "contract-turn"


def test_host_adapter_rejects_ambiguous_turn_correlation():
    adapter = HermesHostAdapter()
    context = SessionContext(
        session_id="session-new",
        lifecycle_id="session-new",
        source_id="contract-task",
        turn_id="contract-turn",
        source_kind="system",
        observed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        fresh=False,
        supported_hooks=frozenset(HOOK_ORDER),
    )

    def snapshot(session_id):
        return SessionLifecycleSnapshot(
            session_id=session_id,
            lifecycle_id=session_id,
            supported_hooks=frozenset(HOOK_ORDER),
            hooks=("pre_llm_call",),
            settled_turn_ids=(),
            open_turn_id="contract-turn",
            finalized=False,
            private_contact_count=0,
        )

    with __import__("pytest").raises(
        ValueError, match="multiple Moonbite session lifecycles"
    ):
        adapter.correlate_turn(context, (snapshot("first"), snapshot("second")))


def test_model_reflection_uses_main_task_key():
    llm = Llm()
    reflection = HermesModelReflection(llm, task="moon_main")
    result = reflection(
        AutonomyContext(
            datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc),
            {"fixture": True},
        )
    )
    assert result["text"] == "A grounded reflection."
    assert llm.calls[0][1]["task"] == "moon_main"


def test_diary_writer_uses_hippocampus_task_key():
    llm = Llm()
    writer = HermesDiaryWriter(llm, task="moon_hippocampus")
    draft = writer.synthesize(
        day=date(2026, 8, 22),
        evidence=[{"open_ref": "card:fixture", "record": {"summary": "fact"}}],
        title_hint="",
    )
    assert (draft.title, draft.body) == ("Day", "Grounded body.")
    assert llm.calls[0][1]["task"] == "moon_hippocampus"


def test_session_wake_is_typed_system_data_and_not_a_delivery_receipt():
    class Context:
        def __init__(self):
            self.call = None

        def inject_message(self, content, role="user", *, session_key=None):
            self.call = (content, role, session_key)
            return True

    context = Context()
    sink = HermesSessionWakeSink(context, session_key="fixture-session")
    result = sink.wake(
        HeartbeatCandidate("care_poke", candidate_id="candidate-1"),
        JudgeDecision(True, False, "model reason", "model suggested message"),
    )

    packet = __import__("json").loads(context.call[0])
    assert context.call[1:] == ("system", "fixture-session")
    assert packet == {
        "candidate_id": "candidate-1",
        "event_type": "moonbite_heartbeat_wake",
        "kind": "care_poke",
        "schema_version": "moon.wake_packet.v1",
    }
    assert (result.ok, result.verified, result.status) == (
        True,
        False,
        "queued_unverified",
    )


def test_official_hermes_shape_rejects_unsupported_targeted_wake():
    class OfficialContextShape:
        def __init__(self):
            self.called = False

        def inject_message(self, content, role="user"):
            self.called = True
            return True

    context = OfficialContextShape()
    sink = HermesSessionWakeSink(context, session_key="fixture-session")

    result = sink.wake(
        HeartbeatCandidate("care_poke", candidate_id="candidate-1"),
        JudgeDecision(True, False, "model reason", "untrusted message"),
    )

    assert (result.ok, result.verified, result.status) == (
        False,
        False,
        "targeted_wake_adapter_unavailable",
    )
    assert context.called is False
