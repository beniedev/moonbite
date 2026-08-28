"""Adapters for the public Hermes plugin surfaces used by Moonbite."""

from __future__ import annotations

import json
from datetime import date
import inspect
from typing import Any, Mapping

from .autonomy import AutonomyContext, AutonomyDecision
from .heartbeat import (
    EffectResult,
    HeartbeatCandidate,
    JudgeDecision,
)
from .memory import DiaryDraft


HEARTBEAT_DECISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["wake_main", "dm_user", "reason", "message"],
    "properties": {
        "wake_main": {"type": "boolean"},
        "dm_user": {"type": "boolean"},
        "reason": {"type": "string", "minLength": 1},
        "message": {"type": "string"},
    },
}

AUTONOMY_DECISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["allowed", "reason"],
    "properties": {
        "allowed": {"type": "boolean"},
        "reason": {"type": "string", "minLength": 1},
    },
}

DIARY_DRAFT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "body", "reason"],
    "properties": {
        "title": {"type": "string", "minLength": 1},
        "body": {"type": "string", "minLength": 1},
        "reason": {"type": "string", "minLength": 1},
    },
}


def _parsed_mapping(result: Any) -> Mapping[str, Any]:
    parsed = getattr(result, "parsed", None)
    if not isinstance(parsed, Mapping):
        raise RuntimeError("host LLM returned no validated structured decision")
    return parsed


class HermesHeartbeatJudge:
    def __init__(self, llm: Any, *, task: str):
        self.llm = llm
        self.task = task

    def decide(self, candidate: HeartbeatCandidate) -> JudgeDecision:
        result = self.llm.complete_structured(
            instructions=(
                "Decide whether this companion heartbeat candidate should wake the "
                "main agent and/or send a short direct message. Fail closed when "
                "evidence is insufficient. Fresh activity is not proof of availability; "
                "silence is not proof of sleep. Return only the requested schema."
            ),
            input=[
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "candidate_id": candidate.candidate_id,
                            "kind": candidate.kind,
                            "context": dict(candidate.context),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            ],
            json_schema=HEARTBEAT_DECISION_SCHEMA,
            schema_name="moonbite_heartbeat_decision",
            temperature=0,
            max_tokens=600,
            purpose="moonbite heartbeat judge",
            task=self.task,
        )
        parsed = _parsed_mapping(result)
        if (
            type(parsed.get("wake_main")) is not bool
            or type(parsed.get("dm_user")) is not bool
        ):
            raise RuntimeError("heartbeat Judge returned invalid booleans")
        reason, message = parsed.get("reason"), parsed.get("message")
        if (
            not isinstance(reason, str)
            or not reason.strip()
            or not isinstance(message, str)
        ):
            raise RuntimeError("heartbeat Judge returned invalid text fields")
        return JudgeDecision(
            parsed["wake_main"], parsed["dm_user"], reason.strip(), message.strip()
        )


class HermesAutonomyJudge:
    def __init__(self, llm: Any, *, task: str):
        self.llm = llm
        self.task = task

    def decide(self, context: AutonomyContext) -> AutonomyDecision:
        result = self.llm.complete_structured(
            instructions=(
                "Decide whether one optional autonomous activity may run now. "
                "Respect active conversation, explicit controls, quiet/sleep evidence, "
                "and insufficient context. Return only the requested schema."
            ),
            input=[
                {
                    "type": "text",
                    "text": json.dumps(
                        {"now": context.now.isoformat(), "facts": dict(context.facts)},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            ],
            json_schema=AUTONOMY_DECISION_SCHEMA,
            schema_name="moonbite_autonomy_decision",
            temperature=0,
            max_tokens=300,
            purpose="moonbite autonomy judge",
            task=self.task,
        )
        parsed = _parsed_mapping(result)
        if type(parsed.get("allowed")) is not bool:
            raise RuntimeError("autonomy Judge returned invalid allowed value")
        reason = parsed.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise RuntimeError("autonomy Judge returned invalid reason")
        return AutonomyDecision(parsed["allowed"], reason.strip())


class HermesModelReflection:
    def __init__(self, llm: Any, *, task: str):
        self.llm = llm
        self.task = task

    def __call__(self, context: AutonomyContext) -> dict[str, Any]:
        result = self.llm.complete(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Write one bounded private reflection from the supplied facts. "
                        "Do not claim facts that are absent and do not contact the user."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {"now": context.now.isoformat(), "facts": dict(context.facts)},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                },
            ],
            temperature=0.4,
            max_tokens=900,
            purpose="moonbite model reflection",
            task=self.task,
        )
        text = getattr(result, "text", None)
        if not isinstance(text, str) or not text.strip():
            raise RuntimeError("host LLM returned an empty reflection")
        return {
            "text": text.strip(),
            "route": {
                "provider": str(getattr(result, "provider", "")),
                "model": str(getattr(result, "model", "")),
                "task": self.task,
            },
        }


class HermesDiaryWriter:
    def __init__(self, llm: Any, *, task: str):
        self.llm = llm
        self.task = task

    def synthesize(
        self,
        *,
        day: date,
        evidence: list[Mapping[str, Any]],
        title_hint: str,
    ) -> DiaryDraft:
        result = self.llm.complete_structured(
            instructions=(
                "Write a concise private diary entry grounded only in the supplied "
                "evidence. Preserve uncertainty and source attribution. Return only "
                "the requested schema."
            ),
            input=[
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "day": day.isoformat(),
                            "title_hint": title_hint,
                            "evidence": evidence,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            ],
            json_schema=DIARY_DRAFT_SCHEMA,
            schema_name="moonbite_diary_draft",
            temperature=0.2,
            max_tokens=1600,
            purpose="moonbite diary synthesis",
            task=self.task,
        )
        parsed = _parsed_mapping(result)
        fields = [parsed.get(name) for name in ("title", "body", "reason")]
        if not all(isinstance(value, str) and value.strip() for value in fields):
            raise RuntimeError("diary writer returned invalid text fields")
        return DiaryDraft(*(value.strip() for value in fields))


class HermesSessionWakeSink:
    """Optional main-session wake adapter; direct-message delivery stays host-owned."""

    def __init__(self, ctx: Any, *, session_key: str):
        self.ctx = ctx
        self.session_key = session_key

    def deliver(
        self, candidate: HeartbeatCandidate, decision: JudgeDecision
    ) -> EffectResult:
        return EffectResult(False, "direct_message_adapter_not_configured")

    def wake(
        self, candidate: HeartbeatCandidate, decision: JudgeDecision
    ) -> EffectResult:
        marker = json.dumps(
            {
                "schema_version": "moon.wake_packet.v1",
                "event_type": "moonbite_heartbeat_wake",
                "candidate_id": candidate.candidate_id,
                "kind": candidate.kind,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        inject_message = self.ctx.inject_message
        try:
            parameters = inspect.signature(inject_message).parameters.values()
        except (TypeError, ValueError):
            return EffectResult(False, "targeted_wake_adapter_unavailable")
        supports_target = any(
            parameter.name == "session_key"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
        if not supports_target:
            # Official pinned Hermes can inject only into an active CLI
            # conversation and exposes no targeted gateway/session wake API.
            # A deployment adapter may provide the explicit extension.
            return EffectResult(False, "targeted_wake_adapter_unavailable")
        accepted = inject_message(marker, role="system", session_key=self.session_key)
        return EffectResult(
            bool(accepted),
            "queued_unverified" if accepted else "rejected",
            verified=False,
        )
