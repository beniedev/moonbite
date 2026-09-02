"""Adapters for the public Hermes plugin surfaces used by Moonbite."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date
import inspect
from typing import Any, Callable, Mapping

from .autonomy import AutonomyContext, AutonomyDecision
from .heartbeat import (
    EffectResult,
    HeartbeatCandidate,
    JudgeDecision,
)
from .memory import DiaryDraft
from .runtime_core import ensure_bounded_text, utc_now
from .session import SessionContext, SessionLifecycleSnapshot


_DEFINITIVE_FINALIZE_REASONS = frozenset({"new_session", "session_expired"})
_TURN_TERMINAL_REASONS = frozenset(
    {
        "host_turn_completed_without_post",
        "host_turn_failed",
        "host_turn_incomplete",
        "host_turn_interrupted",
    }
)
_CHILD_STOP_STATUSES = frozenset(
    {
        "completed",
        "interrupted",
        "failed",
        "error",
        "timeout",
    }
)


class SessionHookMappingError(ValueError):
    """Raised when public Hermes hook kwargs cannot form canonical evidence."""


def _hook_reference(value: Any, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise SessionHookMappingError(f"{label} is required for session hook mapping")
    try:
        ensure_bounded_text(value, label, max_bytes=256)
    except ValueError as exc:
        raise SessionHookMappingError(str(exc)) from exc
    return value


def _optional_hook_reference(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _hook_reference(value, label)


@dataclass(frozen=True, slots=True)
class HermesTurnTerminal:
    """Canonical terminal classification derived from one Hermes turn end."""

    context: SessionContext
    reason: str

    def __post_init__(self) -> None:
        if self.reason not in _TURN_TERMINAL_REASONS:
            raise ValueError("unsupported Hermes turn terminal reason")


@dataclass(frozen=True, slots=True)
class HermesChildStop:
    """Bounded child-stop evidence accepted from Hermes."""

    child_session_id: str
    reason: str

    @property
    def terminal_reason(self) -> str:
        return self.reason


class HermesHostAdapter:
    """The sole raw Hermes lifecycle compatibility boundary for Moonbite."""

    def __init__(self, *, clock: Callable[[], Any] = utc_now):
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.clock = clock

    def session_context(
        self,
        hook: str,
        kwargs: Mapping[str, Any],
        supported_hooks: frozenset[str],
    ) -> SessionContext | None:
        if hook == "pre_gateway_dispatch":
            # This hook runs before authorization and has no stable session ID.
            return None

        session_id = _hook_reference(kwargs.get("session_id"), "session_id")
        observed_at = self.clock()

        if hook == "on_session_start":
            return SessionContext(
                session_id=session_id,
                lifecycle_id=session_id,
                source_id=session_id,
                source_kind="session_start",
                observed_at=observed_at,
                fresh=True,
                supported_hooks=supported_hooks,
            )

        if hook in {"pre_llm_call", "post_llm_call", "on_session_end"}:
            turn_id = _hook_reference(kwargs.get("turn_id"), "turn_id")
            task_id = _optional_hook_reference(kwargs.get("task_id"), "task_id")
            return SessionContext(
                session_id=session_id,
                lifecycle_id=session_id,
                source_id=task_id or turn_id,
                turn_id=turn_id,
                source_kind=(
                    "assistant_response" if hook == "post_llm_call" else "system"
                ),
                observed_at=observed_at,
                fresh=False,
                supported_hooks=supported_hooks,
            )

        if hook == "on_session_finalize":
            return SessionContext(
                session_id=session_id,
                lifecycle_id=session_id,
                source_id=session_id,
                source_kind="system",
                observed_at=observed_at,
                fresh=False,
                supported_hooks=supported_hooks,
            )

        raise SessionHookMappingError(f"unsupported session hook: {hook!r}")

    def turn_terminal(
        self,
        kwargs: Mapping[str, Any],
        *,
        supported_hooks: frozenset[str],
        context: SessionContext | None = None,
    ) -> HermesTurnTerminal:
        if context is None:
            context = self.session_context("on_session_end", kwargs, supported_hooks)
        if not isinstance(context, SessionContext):
            raise SessionHookMappingError("on_session_end requires session context")
        for field in ("completed", "failed", "interrupted"):
            if type(kwargs.get(field)) is not bool:
                raise SessionHookMappingError(
                    f"{field} is required for on_session_end mapping"
                )
        _hook_reference(kwargs.get("turn_exit_reason"), "turn_exit_reason")
        if kwargs["interrupted"]:
            reason = "host_turn_interrupted"
        elif kwargs["failed"]:
            reason = "host_turn_failed"
        elif kwargs["completed"]:
            reason = "host_turn_completed_without_post"
        else:
            reason = "host_turn_incomplete"
        return HermesTurnTerminal(context=context, reason=reason)

    def subagent_stop_terminal(
        self, kwargs: Mapping[str, Any]
    ) -> HermesChildStop | None:
        """Normalize Hermes child-stop status without inspecting child content."""

        child_session_id = _hook_reference(
            kwargs.get("child_session_id"), "child_session_id"
        )
        child_status = _hook_reference(kwargs.get("child_status"), "child_status")
        if child_status not in _CHILD_STOP_STATUSES:
            raise SessionHookMappingError(f"unsupported child_status: {child_status!r}")
        if child_status == "completed":
            return None
        reason = (
            "host_turn_interrupted"
            if child_status == "interrupted"
            else "host_turn_failed"
        )
        return HermesChildStop(child_session_id=child_session_id, reason=reason)

    @staticmethod
    def correlate_turn(
        context: SessionContext,
        snapshots: tuple[SessionLifecycleSnapshot, ...],
    ) -> SessionContext:
        """Resolve a rotated Hermes session ID from durable turn evidence."""

        if context.turn_id is None:
            return context
        candidates = [
            snapshot
            for snapshot in snapshots
            if context.turn_id == snapshot.open_turn_id
            or context.turn_id in snapshot.terminal_turn_ids
            or context.turn_id in snapshot.settled_turn_ids
            or context.turn_id in snapshot.abandoned_turn_ids
        ]
        if not candidates:
            return context
        if len(candidates) != 1:
            raise SessionHookMappingError(
                "turn_id matches multiple Moonbite session lifecycles"
            )
        snapshot = candidates[0]
        return replace(
            context,
            session_id=snapshot.session_id,
            lifecycle_id=snapshot.lifecycle_id,
            supported_hooks=snapshot.supported_hooks,
        )

    @staticmethod
    def pre_turn_context(
        context: SessionContext,
        snapshots: tuple[SessionLifecycleSnapshot, ...],
    ) -> SessionContext:
        """Reuse an existing lifecycle or attach after a host session rotation."""

        matches = [
            snapshot
            for snapshot in snapshots
            if snapshot.lifecycle_id == context.lifecycle_id
        ]
        if len(matches) > 1:
            raise SessionHookMappingError(
                "session_id matches multiple Moonbite session lifecycles"
            )
        if matches:
            snapshot = matches[0]
            return replace(
                context,
                session_id=snapshot.session_id,
                lifecycle_id=snapshot.lifecycle_id,
                supported_hooks=snapshot.supported_hooks,
            )
        # Hermes legacy compression can rotate session_id without firing a
        # matching on_session_start. A pre-model hook is enough to start the
        # new canonical lifecycle; no synthetic start callback is recorded.
        return replace(
            context,
            supported_hooks=context.supported_hooks - {"on_session_start"},
        )

    @staticmethod
    def finalize_disposition(kwargs: Mapping[str, Any]) -> str:
        reason = kwargs.get("reason")
        if reason is None:
            return "ordinary"
        reason = _hook_reference(reason, "reason")
        if reason == "shutdown":
            return "shutdown"
        if reason in _DEFINITIVE_FINALIZE_REASONS:
            return "definitive"
        return "ordinary"


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
