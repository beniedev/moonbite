from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from moonbite_plugin.autonomy import AutonomyContext
from moonbite_plugin.heartbeat import HeartbeatCandidate, JudgeDecision
from moonbite_plugin.hermes_adapter import (
    HermesDiaryWriter,
    HermesModelReflection,
    HermesSessionWakeSink,
)


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
