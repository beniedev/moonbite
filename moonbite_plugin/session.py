"""Portable, append-only session lifecycle state for Moonbite."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

from .runtime_core import (
    JsonlLedger,
    StateError,
    as_utc,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)

SESSION_LIFECYCLE_SCHEMA = "moon.session.lifecycle.v1"
SESSION_SCHEMA = SESSION_LIFECYCLE_SCHEMA
LIFECYCLE_SCHEMA = SESSION_LIFECYCLE_SCHEMA
SCHEMA_VERSION = SESSION_LIFECYCLE_SCHEMA
SESSION_LIFECYCLE_KIND = "hook"
SESSION_TURN_TERMINAL_SCHEMA = "moon.session.turn_terminal.v1"
SESSION_TURN_TERMINAL_KIND = "turn_terminal"
TURN_TERMINAL_OUTCOMES = frozenset({"abandoned"})
TURN_TERMINAL_REASONS = frozenset(
    {
        "superseded_by_new_pre",
        "operator_repair",
        "host_session_finalized",
        "host_shutdown",
        "host_turn_completed_without_post",
        "host_turn_failed",
        "host_turn_incomplete",
        "host_turn_interrupted",
    }
)
CHILD_STOP_TERMINAL_REASONS = frozenset(
    {
        "host_turn_failed",
        "host_turn_interrupted",
    }
)

HOOK_ORDER = (
    "pre_gateway_dispatch",
    "on_session_start",
    "pre_llm_call",
    "post_llm_call",
    "on_session_end",
    "on_session_finalize",
    "subagent_stop",
)
HOOKS = frozenset(HOOK_ORDER)
SUPPORTED_HOOKS = HOOKS

SOURCE_KINDS = frozenset(
    {
        "private_inbound",
        "cron",
        "workroom",
        "system",
        "tool",
        "assistant_response",
        "session_start",
    }
)
SESSION_SOURCE_KINDS = SOURCE_KINDS
SUPPORTED_SOURCE_KINDS = SOURCE_KINDS

_MAX_REFERENCE_BYTES = 256
_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "session_id",
        "lifecycle_id",
        "source_id",
        "turn_id",
        "source_kind",
        "observed_at",
        "fresh",
        "supported_hooks",
        "hook",
        "settled",
    }
)
_TERMINAL_CALLBACK_ROW_FIELDS = _ROW_FIELDS | {"terminal_reason"}
_TERMINAL_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "session_id",
        "lifecycle_id",
        "turn_id",
        "outcome",
        "reason",
        "superseded_by_turn_id",
        "observed_at",
    }
)


class SessionLifecycleError(StateError):
    """Raised when a session lifecycle callback or ledger is unsafe."""


def _reference(value: str, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    ensure_bounded_text(value, label, max_bytes=_MAX_REFERENCE_BYTES)
    return value


def _ordered_hooks(hooks: frozenset[str]) -> tuple[str, ...]:
    return tuple(hook for hook in HOOK_ORDER if hook in hooks)


@dataclass(frozen=True, slots=True)
class SessionContext:
    """Caller-owned context for one lifecycle callback.

    This intentionally contains no message, content, or transcript field.
    """

    session_id: str
    lifecycle_id: str
    source_id: str
    source_kind: str
    observed_at: datetime
    fresh: bool
    supported_hooks: frozenset[str]
    turn_id: str | None = None

    schema_version: ClassVar[str] = SESSION_LIFECYCLE_SCHEMA

    def __post_init__(self) -> None:
        _reference(self.session_id, "session_id")
        _reference(self.lifecycle_id, "lifecycle_id")
        _reference(self.source_id, "source_id")
        if type(self.source_kind) is not str or self.source_kind not in SOURCE_KINDS:
            raise ValueError(f"unsupported source_kind: {self.source_kind!r}")
        if not isinstance(self.observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        object.__setattr__(self, "observed_at", as_utc(self.observed_at))
        if type(self.fresh) is not bool:
            raise ValueError("fresh must be a bool")
        try:
            capabilities = frozenset(self.supported_hooks)
        except TypeError as exc:
            raise ValueError("supported_hooks must be a set of hooks") from exc
        if not capabilities:
            raise ValueError("supported_hooks must be non-empty")
        if any(type(hook) is not str or hook not in HOOKS for hook in capabilities):
            raise ValueError("supported_hooks contains an unknown hook")
        object.__setattr__(self, "supported_hooks", capabilities)
        if self.turn_id is not None:
            _reference(self.turn_id, "turn_id")

    @property
    def counts_as_private_contact(self) -> bool:
        return self.fresh and self.source_kind == "private_inbound"

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "lifecycle_id": self.lifecycle_id,
            "source_id": self.source_id,
            "turn_id": self.turn_id,
            "source_kind": self.source_kind,
            "observed_at": isoformat(self.observed_at),
            "fresh": self.fresh,
            "supported_hooks": list(_ordered_hooks(self.supported_hooks)),
        }


@dataclass(frozen=True, slots=True)
class SessionLifecycleSnapshot:
    """Read-only derived state for one session lifecycle."""

    session_id: str
    lifecycle_id: str
    supported_hooks: frozenset[str]
    hooks: tuple[str, ...]
    settled_turn_ids: tuple[str, ...]
    open_turn_id: str | None
    finalized: bool
    private_contact_count: int
    terminal_turn_ids: tuple[str, ...] = ()
    abandoned_turn_ids: tuple[str, ...] = ()

    schema_version: ClassVar[str] = SESSION_LIFECYCLE_SCHEMA

    @property
    def completed_hooks(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(self.hooks))

    @property
    def completed_hook_set(self) -> frozenset[str]:
        return frozenset(self.hooks)

    @property
    def hook_order(self) -> tuple[str, ...]:
        return self.hooks

    @property
    def recorded_hooks(self) -> tuple[str, ...]:
        return self.hooks

    @property
    def current_turn_id(self) -> str | None:
        return self.open_turn_id

    @property
    def has_open_turn(self) -> bool:
        return self.open_turn_id is not None

    @property
    def contact_count(self) -> int:
        return self.private_contact_count

    @property
    def contact_established(self) -> bool:
        return self.private_contact_count > 0

    @property
    def is_finalized(self) -> bool:
        return self.finalized

    @property
    def seen_hooks(self) -> frozenset[str]:
        return self.completed_hook_set


@dataclass(frozen=True, slots=True)
class SessionHookReceipt:
    """Receipt returned after a callback is accepted or deduplicated."""

    schema_version: str
    event_id: str
    lifecycle_id: str
    hook: str
    source_id: str
    turn_id: str | None
    settled: bool
    deduplicated: bool
    context: SessionContext
    snapshot: SessionLifecycleSnapshot

    @property
    def is_duplicate(self) -> bool:
        return self.deduplicated

    @property
    def duplicate(self) -> bool:
        return self.deduplicated

    @property
    def state(self) -> SessionLifecycleSnapshot:
        return self.snapshot

    @property
    def hooks(self) -> tuple[str, ...]:
        return self.snapshot.hooks

    @property
    def private_contact_count(self) -> int:
        return self.snapshot.private_contact_count


SessionLifecycleReceipt = SessionHookReceipt
SessionLifecycleState = SessionLifecycleSnapshot


@dataclass(frozen=True, slots=True)
class SessionTurnTerminal:
    """Immutable non-hook terminal evidence for one session turn."""

    schema_version: str
    event_id: str
    session_id: str
    lifecycle_id: str
    turn_id: str
    outcome: str
    reason: str
    superseded_by_turn_id: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.schema_version != SESSION_TURN_TERMINAL_SCHEMA:
            raise ValueError("unsupported turn terminal schema")
        _reference(self.event_id, "event_id")
        _reference(self.session_id, "session_id")
        _reference(self.lifecycle_id, "lifecycle_id")
        _reference(self.turn_id, "turn_id")
        if self.outcome not in TURN_TERMINAL_OUTCOMES:
            raise ValueError("unsupported turn terminal outcome")
        if self.reason not in TURN_TERMINAL_REASONS:
            raise ValueError("unsupported turn terminal reason")
        if self.reason == "superseded_by_new_pre":
            if self.superseded_by_turn_id is None:
                raise ValueError("superseded terminal requires successor turn_id")
            _reference(self.superseded_by_turn_id, "superseded_by_turn_id")
            if self.superseded_by_turn_id == self.turn_id:
                raise ValueError("turn cannot supersede itself")
        elif self.superseded_by_turn_id is not None:
            raise ValueError("only a superseded terminal can name a successor turn")
        if not isinstance(self.observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        object.__setattr__(self, "observed_at", as_utc(self.observed_at))

    def to_row(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": SESSION_TURN_TERMINAL_KIND,
            "event_id": self.event_id,
            "session_id": self.session_id,
            "lifecycle_id": self.lifecycle_id,
            "turn_id": self.turn_id,
            "outcome": self.outcome,
            "reason": self.reason,
            "superseded_by_turn_id": self.superseded_by_turn_id,
            "observed_at": isoformat(self.observed_at),
        }


@dataclass(frozen=True, slots=True)
class SessionTurnTerminalReceipt:
    """Receipt returned after a turn terminal is appended or replayed."""

    schema_version: str
    event_id: str
    session_id: str
    lifecycle_id: str
    turn_id: str
    outcome: str
    reason: str
    superseded_by_turn_id: str | None
    observed_at: datetime
    deduplicated: bool
    snapshot: SessionLifecycleSnapshot

    @property
    def already_repaired(self) -> bool:
        return self.deduplicated

    @property
    def is_duplicate(self) -> bool:
        return self.deduplicated

    @property
    def state(self) -> SessionLifecycleSnapshot:
        return self.snapshot


@dataclass(frozen=True, slots=True)
class _Callback:
    context: SessionContext
    hook: str
    settled: bool
    event_id: str
    terminal_reason: str | None = None

    @property
    def key(self) -> tuple[str, str, str | None]:
        return (self.hook, self.context.source_id, self.context.turn_id)

    def to_row(self) -> dict[str, Any]:
        row = {
            "schema_version": SESSION_LIFECYCLE_SCHEMA,
            "kind": SESSION_LIFECYCLE_KIND,
            "event_id": self.event_id,
            **self.context.to_dict(),
            "hook": self.hook,
            "settled": self.settled,
        }
        if self.terminal_reason is not None:
            row["terminal_reason"] = self.terminal_reason
        return row


@dataclass(slots=True)
class _TurnState:
    outcome: str | None = None


@dataclass
class _LifecycleState:
    session_id: str
    lifecycle_id: str
    supported_hooks: frozenset[str]
    callbacks: dict[tuple[str, str, str | None], _Callback] = field(
        default_factory=dict
    )
    hooks: list[str] = field(default_factory=list)
    gates: set[str] = field(default_factory=set)
    turns: dict[str, _TurnState] = field(default_factory=dict)
    terminals: dict[str, SessionTurnTerminal] = field(default_factory=dict)
    terminal_turn_ids: list[str] = field(default_factory=list)
    abandoned_turn_ids: list[str] = field(default_factory=list)
    current_turn_id: str | None = None
    settled_turn_ids: list[str] = field(default_factory=list)
    finalized: bool = False
    private_contact_count: int = 0
    private_source_ids: set[str] = field(default_factory=set)


def _snapshot(state: _LifecycleState) -> SessionLifecycleSnapshot:
    return SessionLifecycleSnapshot(
        session_id=state.session_id,
        lifecycle_id=state.lifecycle_id,
        supported_hooks=state.supported_hooks,
        hooks=tuple(state.hooks),
        settled_turn_ids=tuple(state.settled_turn_ids),
        terminal_turn_ids=tuple(state.terminal_turn_ids),
        abandoned_turn_ids=tuple(state.abandoned_turn_ids),
        open_turn_id=state.current_turn_id,
        finalized=state.finalized,
        private_contact_count=state.private_contact_count,
    )


def _same_callback_identity(left: _Callback, right: _Callback) -> bool:
    """Compare callback identity while treating observed_at as audit metadata."""

    return (
        left.hook == right.hook
        and left.context.session_id == right.context.session_id
        and left.context.lifecycle_id == right.context.lifecycle_id
        and left.context.source_id == right.context.source_id
        and left.context.turn_id == right.context.turn_id
        and left.context.source_kind == right.context.source_kind
        and left.context.fresh == right.context.fresh
        and left.context.supported_hooks == right.context.supported_hooks
        and left.settled == right.settled
        and left.terminal_reason == right.terminal_reason
    )


class SessionLifecycleStore:
    """Append-only, replayable session lifecycle state.

    The mutation lock covers replay plus validation plus append, making the
    check-then-append idempotency decision atomic across store instances.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "session_lifecycle.jsonl")
        self.mutation_lock = self.root / "session_lifecycle.mutation.lock"
        self.mutation_lock_path = self.mutation_lock

    def _new_state(self, context: SessionContext) -> _LifecycleState:
        return _LifecycleState(
            session_id=context.session_id,
            lifecycle_id=context.lifecycle_id,
            supported_hooks=context.supported_hooks,
        )

    @staticmethod
    def _validate_inputs(context: SessionContext, hook: str, settled: bool) -> None:
        if not isinstance(context, SessionContext):
            raise TypeError("context must be a SessionContext")
        if type(hook) is not str or hook not in HOOKS:
            raise SessionLifecycleError(f"unsupported session lifecycle hook: {hook!r}")
        if type(settled) is not bool:
            raise SessionLifecycleError("settled must be a bool")
        if settled and hook != "post_llm_call":
            raise SessionLifecycleError("only post_llm_call may be settled")
        if hook == "on_session_start" and context.source_kind != "session_start":
            raise SessionLifecycleError(
                "on_session_start requires source_kind=session_start"
            )
        if hook == "post_llm_call" and context.source_kind != "assistant_response":
            raise SessionLifecycleError(
                "post_llm_call requires source_kind=assistant_response"
            )
        if hook == "on_session_end" and context.source_kind != "system":
            raise SessionLifecycleError("on_session_end requires source_kind=system")
        if hook == "on_session_finalize" and context.source_kind != "system":
            raise SessionLifecycleError(
                "on_session_finalize requires source_kind=system"
            )
        if hook == "subagent_stop" and context.source_kind != "system":
            raise SessionLifecycleError("subagent_stop requires source_kind=system")
        if hook in {"pre_gateway_dispatch", "pre_llm_call"} and context.source_kind in {
            "session_start",
            "assistant_response",
        }:
            raise SessionLifecycleError(
                f"{hook} rejects source_kind={context.source_kind}"
            )
        if context.source_kind == "private_inbound" and not context.fresh:
            raise SessionLifecycleError(
                "stale private inbound cannot enter a lifecycle"
            )

    def _apply(
        self, state: _LifecycleState, callback: _Callback, *, allow_duplicate: bool
    ) -> bool:
        context = callback.context
        if context.lifecycle_id != state.lifecycle_id:
            raise SessionLifecycleError("callback lifecycle_id does not match state")
        if context.session_id != state.session_id:
            raise SessionLifecycleError("session_id changed within a lifecycle")
        if context.supported_hooks != state.supported_hooks:
            raise SessionLifecycleError("supported_hooks changed within a lifecycle")
        self._validate_inputs(context, callback.hook, callback.settled)

        existing = state.callbacks.get(callback.key)
        if existing is not None:
            if allow_duplicate and _same_callback_identity(existing, callback):
                return True
            raise SessionLifecycleError(
                "callback identity conflicts with an existing lifecycle callback"
            )

        if state.finalized:
            raise SessionLifecycleError("session lifecycle is already finalized")

        if callback.hook not in context.supported_hooks:
            raise SessionLifecycleError(
                f"hook {callback.hook!r} is not supported by this lifecycle"
            )

        if callback.hook == "pre_gateway_dispatch":
            if "pre_gateway_dispatch" in state.gates:
                raise SessionLifecycleError("pre_gateway_dispatch already recorded")
            state.gates.add(callback.hook)

        elif callback.hook == "on_session_start":
            if "on_session_start" in state.gates:
                raise SessionLifecycleError("on_session_start already recorded")
            if (
                "pre_gateway_dispatch" in context.supported_hooks
                and "pre_gateway_dispatch" not in state.gates
            ):
                raise SessionLifecycleError(
                    "on_session_start requires pre_gateway_dispatch"
                )
            state.gates.add(callback.hook)

        elif callback.hook == "pre_llm_call":
            turn_id = context.turn_id
            if turn_id is None:
                raise SessionLifecycleError("pre_llm_call requires turn_id")
            if (
                "pre_gateway_dispatch" in context.supported_hooks
                and "pre_gateway_dispatch" not in state.gates
            ):
                raise SessionLifecycleError(
                    "pre_llm_call requires pre_gateway_dispatch"
                )
            if (
                "on_session_start" in context.supported_hooks
                and "on_session_start" not in state.gates
            ):
                raise SessionLifecycleError("pre_llm_call requires on_session_start")
            if state.current_turn_id is not None:
                raise SessionLifecycleError("previous turn is not settled")
            if turn_id in state.turns:
                raise SessionLifecycleError("turn_id was already used")
            state.turns[turn_id] = _TurnState()
            state.current_turn_id = turn_id

        elif callback.hook == "post_llm_call":
            turn_id = context.turn_id
            if turn_id is None:
                raise SessionLifecycleError("post_llm_call requires turn_id")
            turn = state.turns.get(turn_id)
            if turn is not None and turn.outcome is not None:
                raise SessionLifecycleError(
                    f"turn is already terminal ({turn.outcome})"
                )
            if state.current_turn_id != turn_id:
                raise SessionLifecycleError(
                    "post_llm_call must match the current pre_llm_call turn"
                )
            assert turn is not None
            if callback.settled:
                turn.outcome = "completed"
                state.terminal_turn_ids.append(turn_id)
                state.settled_turn_ids.append(turn_id)
                state.current_turn_id = None

        elif callback.hook == "on_session_end":
            turn_id = context.turn_id
            if turn_id is None:
                raise SessionLifecycleError("on_session_end requires turn_id")
            turn = state.turns.get(turn_id)
            if turn is None:
                raise SessionLifecycleError("on_session_end requires pre_llm_call")
            if turn.outcome is None:
                raise SessionLifecycleError(
                    "on_session_end requires canonical turn terminal evidence"
                )

        elif callback.hook == "subagent_stop":
            turn_id = context.turn_id
            if turn_id is None:
                raise SessionLifecycleError("subagent_stop requires turn_id")
            if callback.terminal_reason not in CHILD_STOP_TERMINAL_REASONS:
                raise SessionLifecycleError(
                    "subagent_stop requires a child terminal reason"
                )
            turn = state.turns.get(turn_id)
            terminal = state.terminals.get(turn_id)
            if turn is None or terminal is None or turn.outcome != "abandoned":
                raise SessionLifecycleError(
                    "subagent_stop requires canonical non-success terminal evidence"
                )
            if terminal.reason != callback.terminal_reason:
                raise SessionLifecycleError(
                    "subagent_stop conflicts with existing terminal evidence"
                )

        elif callback.hook == "on_session_finalize":
            if state.finalized:
                raise SessionLifecycleError("on_session_finalize already recorded")
            self._validate_finalize(state)
            state.finalized = True

        state.callbacks[callback.key] = callback
        state.hooks.append(callback.hook)
        if context.counts_as_private_contact:
            state.private_source_ids.add(context.source_id)
            state.private_contact_count = len(state.private_source_ids)
        return False

    @staticmethod
    def _validate_finalize(state: _LifecycleState) -> None:
        supported = state.supported_hooks
        if (
            "pre_gateway_dispatch" in supported
            and "pre_gateway_dispatch" not in state.gates
        ):
            raise SessionLifecycleError(
                "on_session_finalize requires pre_gateway_dispatch"
            )
        if "on_session_start" in supported and "on_session_start" not in state.gates:
            raise SessionLifecycleError("on_session_finalize requires on_session_start")
        if "pre_llm_call" in supported and not state.turns:
            raise SessionLifecycleError("on_session_finalize requires pre_llm_call")
        if state.current_turn_id is not None or any(
            turn.outcome is None for turn in state.turns.values()
        ):
            raise SessionLifecycleError(
                "on_session_finalize requires all turns to be terminal or settled"
            )

    @staticmethod
    def _callback_from_row(row: Mapping[str, Any]) -> _Callback:
        row_fields = set(row)
        if row_fields != _ROW_FIELDS and row_fields != _TERMINAL_CALLBACK_ROW_FIELDS:
            raise SessionLifecycleError("session lifecycle row has invalid fields")
        if row["schema_version"] != SESSION_LIFECYCLE_SCHEMA:
            raise SessionLifecycleError(
                "session lifecycle row has an unsupported schema"
            )
        if row["kind"] != SESSION_LIFECYCLE_KIND:
            raise SessionLifecycleError("session lifecycle row has an unsupported kind")
        event_id = row["event_id"]
        hook = row["hook"]
        if type(event_id) is not str:
            raise SessionLifecycleError("session lifecycle event_id is invalid")
        try:
            _reference(event_id, "event_id")
        except (TypeError, ValueError) as exc:
            raise SessionLifecycleError(
                "session lifecycle event_id is invalid"
            ) from exc
        if type(hook) is not str or hook not in HOOKS:
            raise SessionLifecycleError("session lifecycle row hook is invalid")
        terminal_reason = row.get("terminal_reason")
        if terminal_reason is not None and (
            hook not in {"on_session_end", "subagent_stop"}
            or type(terminal_reason) is not str
            or terminal_reason not in TURN_TERMINAL_REASONS
            or not terminal_reason.startswith("host_turn_")
        ):
            raise SessionLifecycleError("session lifecycle terminal_reason is invalid")
        if (
            hook == "subagent_stop"
            and terminal_reason not in CHILD_STOP_TERMINAL_REASONS
        ):
            raise SessionLifecycleError(
                "session lifecycle child terminal_reason is invalid"
            )
        raw_turn_id = row["turn_id"]
        if raw_turn_id is not None and type(raw_turn_id) is not str:
            raise SessionLifecycleError("session lifecycle row turn_id is invalid")
        raw_capabilities = row["supported_hooks"]
        if not isinstance(raw_capabilities, list):
            raise SessionLifecycleError(
                "session lifecycle row supported_hooks is invalid"
            )
        if any(type(hook_name) is not str for hook_name in raw_capabilities):
            raise SessionLifecycleError(
                "session lifecycle row supported_hooks is invalid"
            )
        if len(raw_capabilities) != len(set(raw_capabilities)):
            raise SessionLifecycleError(
                "session lifecycle row supported_hooks is invalid"
            )
        if type(row["observed_at"]) is not str:
            raise SessionLifecycleError("session lifecycle row observed_at is invalid")
        if type(row["fresh"]) is not bool or type(row["settled"]) is not bool:
            raise SessionLifecycleError("session lifecycle row booleans are invalid")
        try:
            context = SessionContext(
                session_id=row["session_id"],
                lifecycle_id=row["lifecycle_id"],
                source_id=row["source_id"],
                turn_id=raw_turn_id,
                source_kind=row["source_kind"],
                observed_at=parse_time(row["observed_at"]),
                fresh=row["fresh"],
                supported_hooks=frozenset(raw_capabilities),
            )
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise SessionLifecycleError(
                "session lifecycle row context is invalid"
            ) from exc
        return _Callback(
            context=context,
            hook=hook,
            settled=row["settled"],
            event_id=event_id,
            terminal_reason=terminal_reason,
        )

    @staticmethod
    def _terminal_from_row(row: Mapping[str, Any]) -> SessionTurnTerminal:
        if set(row) != _TERMINAL_ROW_FIELDS:
            raise SessionLifecycleError("turn terminal row has invalid fields")
        if row["schema_version"] != SESSION_TURN_TERMINAL_SCHEMA:
            raise SessionLifecycleError("turn terminal row has an unsupported schema")
        if row["kind"] != SESSION_TURN_TERMINAL_KIND:
            raise SessionLifecycleError("turn terminal row has an unsupported kind")
        if type(row["observed_at"]) is not str:
            raise SessionLifecycleError("turn terminal observed_at is invalid")
        if (
            row["superseded_by_turn_id"] is not None
            and type(row["superseded_by_turn_id"]) is not str
        ):
            raise SessionLifecycleError(
                "turn terminal superseded_by_turn_id is invalid"
            )
        try:
            return SessionTurnTerminal(
                schema_version=row["schema_version"],
                event_id=row["event_id"],
                session_id=row["session_id"],
                lifecycle_id=row["lifecycle_id"],
                turn_id=row["turn_id"],
                outcome=row["outcome"],
                reason=row["reason"],
                superseded_by_turn_id=row["superseded_by_turn_id"],
                observed_at=parse_time(row["observed_at"]),
            )
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise SessionLifecycleError("turn terminal row is invalid") from exc

    @staticmethod
    def _validate_state_identity(
        state: _LifecycleState, context: SessionContext
    ) -> None:
        if context.lifecycle_id != state.lifecycle_id:
            raise SessionLifecycleError("callback lifecycle_id does not match state")
        if context.session_id != state.session_id:
            raise SessionLifecycleError("session_id changed within a lifecycle")
        if context.supported_hooks != state.supported_hooks:
            raise SessionLifecycleError("supported_hooks changed within a lifecycle")

    @staticmethod
    def _validate_new_pre(state: _LifecycleState, callback: _Callback) -> None:
        context = callback.context
        if state.finalized:
            raise SessionLifecycleError("session lifecycle is already finalized")
        if callback.hook not in context.supported_hooks:
            raise SessionLifecycleError(
                f"hook {callback.hook!r} is not supported by this lifecycle"
            )
        turn_id = context.turn_id
        if turn_id is None:
            raise SessionLifecycleError("pre_llm_call requires turn_id")
        if (
            "pre_gateway_dispatch" in context.supported_hooks
            and "pre_gateway_dispatch" not in state.gates
        ):
            raise SessionLifecycleError("pre_llm_call requires pre_gateway_dispatch")
        if (
            "on_session_start" in context.supported_hooks
            and "on_session_start" not in state.gates
        ):
            raise SessionLifecycleError("pre_llm_call requires on_session_start")
        if turn_id in state.turns:
            raise SessionLifecycleError("turn_id was already used")

    @staticmethod
    def _apply_terminal(state: _LifecycleState, terminal: SessionTurnTerminal) -> None:
        if terminal.lifecycle_id != state.lifecycle_id:
            raise SessionLifecycleError("terminal lifecycle_id does not match state")
        if terminal.session_id != state.session_id:
            raise SessionLifecycleError(
                "terminal session_id changed within a lifecycle"
            )
        if state.finalized:
            raise SessionLifecycleError("session lifecycle is already finalized")
        if terminal.turn_id in state.terminals:
            raise SessionLifecycleError("turn already has a terminal")
        turn = state.turns.get(terminal.turn_id)
        if turn is None:
            raise SessionLifecycleError("terminal references an unknown turn")
        if turn.outcome is not None:
            raise SessionLifecycleError(f"turn is already terminal ({turn.outcome})")
        if state.current_turn_id != terminal.turn_id:
            raise SessionLifecycleError(
                "terminal must match the current pre_llm_call turn"
            )
        turn.outcome = terminal.outcome
        state.terminals[terminal.turn_id] = terminal
        state.terminal_turn_ids.append(terminal.turn_id)
        if terminal.outcome == "abandoned":
            state.abandoned_turn_ids.append(terminal.turn_id)
        state.current_turn_id = None

    def _replay_unlocked(self) -> dict[str, _LifecycleState]:
        states: dict[str, _LifecycleState] = {}
        event_ids: set[str] = set()
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            try:
                kind = row.get("kind")
                if kind == SESSION_LIFECYCLE_KIND:
                    callback = self._callback_from_row(row)
                    event_id = callback.event_id
                elif kind == SESSION_TURN_TERMINAL_KIND:
                    terminal = self._terminal_from_row(row)
                    event_id = terminal.event_id
                else:
                    raise SessionLifecycleError(
                        "session lifecycle row has an unsupported kind"
                    )
                if event_id in event_ids:
                    raise SessionLifecycleError("duplicate session lifecycle event_id")
                event_ids.add(event_id)
                if kind == SESSION_LIFECYCLE_KIND:
                    state = states.get(callback.context.lifecycle_id)
                    if state is None:
                        state = self._new_state(callback.context)
                        states[callback.context.lifecycle_id] = state
                    self._apply(state, callback, allow_duplicate=False)
                else:
                    state = states.get(terminal.lifecycle_id)
                    if state is None:
                        raise SessionLifecycleError(
                            "turn terminal references an unknown lifecycle"
                        )
                    self._apply_terminal(state, terminal)
            except SessionLifecycleError as exc:
                raise SessionLifecycleError(
                    f"session_lifecycle.jsonl row {row_number} is invalid"
                ) from exc
        return states

    def _receipt(
        self,
        callback: _Callback,
        state: _LifecycleState,
        *,
        deduplicated: bool,
    ) -> SessionHookReceipt:
        return SessionHookReceipt(
            schema_version=SESSION_LIFECYCLE_SCHEMA,
            event_id=callback.event_id,
            lifecycle_id=callback.context.lifecycle_id,
            hook=callback.hook,
            source_id=callback.context.source_id,
            turn_id=callback.context.turn_id,
            settled=callback.settled,
            deduplicated=deduplicated,
            context=callback.context,
            snapshot=_snapshot(state),
        )

    @staticmethod
    def _terminal_receipt(
        terminal: SessionTurnTerminal,
        state: _LifecycleState,
        *,
        deduplicated: bool,
    ) -> SessionTurnTerminalReceipt:
        return SessionTurnTerminalReceipt(
            schema_version=terminal.schema_version,
            event_id=terminal.event_id,
            session_id=terminal.session_id,
            lifecycle_id=terminal.lifecycle_id,
            turn_id=terminal.turn_id,
            outcome=terminal.outcome,
            reason=terminal.reason,
            superseded_by_turn_id=terminal.superseded_by_turn_id,
            observed_at=terminal.observed_at,
            deduplicated=deduplicated,
            snapshot=_snapshot(state),
        )

    def record_hook(
        self,
        context: SessionContext,
        hook: str,
        *,
        settled: bool = False,
    ) -> SessionHookReceipt:
        """Record one supported callback, atomically and idempotently."""

        self._validate_inputs(context, hook, settled)
        callback = _Callback(
            context=context,
            hook=hook,
            settled=settled,
            event_id=new_id("session_hook"),
        )
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(context.lifecycle_id)
            if state is None:
                state = self._new_state(context)
                states[context.lifecycle_id] = state
            existing = state.callbacks.get(callback.key)
            if existing is not None:
                # Let the normal identity validator handle duplicate and
                # conflicting callbacks before any recovery path runs.
                self._apply(state, callback, allow_duplicate=True)
                callback = existing
                deduplicated = True
            else:
                if hook == "pre_llm_call":
                    # Validate the successor before closing the old turn. This
                    # keeps rejected pre callbacks from mutating durable state.
                    self._validate_state_identity(state, context)
                    self._validate_new_pre(state, callback)
                if hook == "pre_llm_call" and state.current_turn_id is not None:
                    old_turn_id = state.current_turn_id
                    assert old_turn_id is not None
                    terminal = SessionTurnTerminal(
                        schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                        event_id=new_id("session_turn_terminal"),
                        session_id=state.session_id,
                        lifecycle_id=state.lifecycle_id,
                        turn_id=old_turn_id,
                        outcome="abandoned",
                        reason="superseded_by_new_pre",
                        superseded_by_turn_id=context.turn_id,
                        observed_at=context.observed_at,
                    )
                    self._apply_terminal(state, terminal)
                    # If this append fails, the successor pre is not opened.
                    # A later retry replays the durable terminal and proceeds.
                    self.ledger.append(terminal.to_row())
                # JsonlLedger propagates all write errors. State is rebuilt from
                # disk on the next retry, so a failed append cannot leak a
                # partially accepted callback into this store instance.
                self._apply(state, callback, allow_duplicate=True)
                self.ledger.append(callback.to_row())
                deduplicated = False
            return self._receipt(
                callback,
                state,
                deduplicated=deduplicated,
            )

    def snapshot(self, lifecycle_id: str) -> SessionLifecycleSnapshot | None:
        _reference(lifecycle_id, "lifecycle_id")
        with file_lock(self.mutation_lock):
            state = self._replay_unlocked().get(lifecycle_id)
            return None if state is None else _snapshot(state)

    def abandon_open_turn(
        self,
        lifecycle_id: str,
        turn_id: str,
        observed_at: datetime | None = None,
    ) -> SessionTurnTerminalReceipt:
        """Append an exact operator repair for the current open turn.

        This is a compare-and-set operation under the same mutation lock as
        hook recording.  A previously abandoned turn is returned as-is,
        while unknown, completed, or no-longer-current turns are rejected.
        """

        _reference(lifecycle_id, "lifecycle_id")
        _reference(turn_id, "turn_id")
        effective_observed_at = utc_now() if observed_at is None else observed_at
        if not isinstance(effective_observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        effective_observed_at = as_utc(effective_observed_at)
        with file_lock(self.mutation_lock):
            state = self._replay_unlocked().get(lifecycle_id)
            if state is None:
                raise SessionLifecycleError("lifecycle_id was not found")
            existing = state.terminals.get(turn_id)
            if existing is not None:
                if existing.outcome != "abandoned":
                    raise SessionLifecycleError("turn is already terminal")
                return self._terminal_receipt(existing, state, deduplicated=True)
            turn = state.turns.get(turn_id)
            if turn is None:
                raise SessionLifecycleError("turn_id was not found")
            if turn.outcome is not None:
                raise SessionLifecycleError(
                    f"turn is already terminal ({turn.outcome})"
                )
            if state.current_turn_id != turn_id:
                raise SessionLifecycleError(
                    "turn_id does not match the current open turn"
                )
            terminal = SessionTurnTerminal(
                schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                event_id=new_id("session_turn_terminal"),
                session_id=state.session_id,
                lifecycle_id=state.lifecycle_id,
                turn_id=turn_id,
                outcome="abandoned",
                reason="operator_repair",
                superseded_by_turn_id=None,
                observed_at=effective_observed_at,
            )
            self._apply_terminal(state, terminal)
            self.ledger.append(terminal.to_row())
            return self._terminal_receipt(terminal, state, deduplicated=False)

    def record_host_turn_end(
        self,
        context: SessionContext,
        terminal_reason: str,
    ) -> SessionHookReceipt | SessionTurnTerminalReceipt | None:
        """Record an exact host turn end, abandoning only a still-open turn."""

        self._validate_inputs(context, "on_session_end", False)
        if (
            terminal_reason not in TURN_TERMINAL_REASONS
            or not terminal_reason.startswith("host_turn_")
        ):
            raise SessionLifecycleError("unsupported host turn terminal reason")
        callback = _Callback(
            context=context,
            hook="on_session_end",
            settled=False,
            event_id=new_id("session_hook"),
            terminal_reason=terminal_reason,
        )
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(context.lifecycle_id)
            if state is None:
                return None

            turn_id = context.turn_id
            assert turn_id is not None
            turn = state.turns.get(turn_id)
            if turn is None:
                raise SessionLifecycleError("on_session_end requires pre_llm_call")

            records_hook = "on_session_end" in state.supported_hooks
            existing_callback = next(
                (
                    item
                    for item in state.callbacks.values()
                    if item.hook == "on_session_end" and item.context.turn_id == turn_id
                ),
                None,
            )
            if existing_callback is not None:
                if not _same_callback_identity(existing_callback, callback):
                    raise SessionLifecycleError(
                        "on_session_end conflicts with existing terminal evidence"
                    )
                return self._receipt(existing_callback, state, deduplicated=True)

            existing_terminal = state.terminals.get(turn_id)
            if existing_terminal is not None:
                if existing_terminal.reason != terminal_reason:
                    raise SessionLifecycleError(
                        "on_session_end conflicts with existing terminal evidence"
                    )
                if not records_hook:
                    return self._terminal_receipt(
                        existing_terminal, state, deduplicated=True
                    )

            if (
                turn.outcome == "completed"
                and terminal_reason != "host_turn_completed_without_post"
            ):
                raise SessionLifecycleError(
                    "on_session_end conflicts with settled turn evidence"
                )

            terminal: SessionTurnTerminal | None = None
            if turn.outcome is None:
                if state.current_turn_id != turn_id:
                    raise SessionLifecycleError(
                        "on_session_end turn does not match the current open turn"
                    )
                terminal = SessionTurnTerminal(
                    schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                    event_id=new_id("session_turn_terminal"),
                    session_id=state.session_id,
                    lifecycle_id=state.lifecycle_id,
                    turn_id=turn_id,
                    outcome="abandoned",
                    reason=terminal_reason,
                    superseded_by_turn_id=None,
                    observed_at=context.observed_at,
                )
                self._apply_terminal(state, terminal)

            if not records_hook:
                if terminal is None:
                    return None
                self.ledger.append(terminal.to_row())
                return self._terminal_receipt(terminal, state, deduplicated=False)

            self._apply(state, callback, allow_duplicate=False)
            if terminal is not None:
                self.ledger.append(terminal.to_row())
            self.ledger.append(callback.to_row())
            return self._receipt(callback, state, deduplicated=False)

    def record_host_child_stop(
        self,
        child_session_id: str,
        terminal_reason: str,
        observed_at: datetime | None = None,
    ) -> SessionHookReceipt | SessionTurnTerminalReceipt | None:
        """Close the unique open child turn reported by Hermes.

        A child stop has no parent turn identity.  The child session id is
        therefore resolved against exactly one durable lifecycle while holding
        the same mutation lock used by every other lifecycle mutation.
        """

        _reference(child_session_id, "child_session_id")
        if terminal_reason not in CHILD_STOP_TERMINAL_REASONS:
            raise SessionLifecycleError("unsupported child terminal reason")
        effective_observed_at = utc_now() if observed_at is None else observed_at
        if not isinstance(effective_observed_at, datetime):
            raise TypeError("observed_at must be a datetime")
        effective_observed_at = as_utc(effective_observed_at)

        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            matches = [
                state
                for state in states.values()
                if state.session_id == child_session_id
            ]
            if len(matches) > 1:
                raise SessionLifecycleError(
                    "child_session_id matches multiple Moonbite session lifecycles"
                )
            if not matches:
                return None
            state = matches[0]

            child_callbacks = [
                callback
                for callback in state.callbacks.values()
                if callback.hook == "subagent_stop"
            ]
            if len(child_callbacks) > 1:
                raise SessionLifecycleError(
                    "child_session_id has multiple subagent_stop callbacks"
                )
            if child_callbacks:
                callback = child_callbacks[0]
                if callback.terminal_reason != terminal_reason:
                    raise SessionLifecycleError(
                        "subagent_stop conflicts with existing terminal evidence"
                    )
                turn_id = callback.context.turn_id
                if turn_id is None:
                    raise SessionLifecycleError(
                        "subagent_stop callback has no child turn"
                    )
                terminal = state.terminals.get(turn_id)
                turn = state.turns.get(turn_id)
                if (
                    terminal is None
                    or turn is None
                    or turn.outcome != "abandoned"
                    or terminal.reason != terminal_reason
                ):
                    raise SessionLifecycleError(
                        "subagent_stop callback conflicts with terminal evidence"
                    )
                return self._receipt(callback, state, deduplicated=True)

            turn_id = state.current_turn_id
            if turn_id is None:
                if len(state.turns) != 1:
                    raise SessionLifecycleError(
                        "child_session_id does not identify a unique one-shot turn"
                    )
                turn_id = next(iter(state.turns))
            turn = state.turns.get(turn_id)
            if turn is None:
                raise SessionLifecycleError("child turn is not present in lifecycle")
            terminal = state.terminals.get(turn_id)
            terminal_created = False
            if turn.outcome == "completed":
                raise SessionLifecycleError(
                    "subagent_stop conflicts with settled turn evidence"
                )
            if turn.outcome is None:
                if state.current_turn_id != turn_id:
                    raise SessionLifecycleError(
                        "child turn is not the current open turn"
                    )
                terminal = SessionTurnTerminal(
                    schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                    event_id=new_id("session_turn_terminal"),
                    session_id=state.session_id,
                    lifecycle_id=state.lifecycle_id,
                    turn_id=turn_id,
                    outcome="abandoned",
                    reason=terminal_reason,
                    superseded_by_turn_id=None,
                    observed_at=effective_observed_at,
                )
                self._apply_terminal(state, terminal)
                terminal_created = True
            elif terminal is None or terminal.reason != terminal_reason:
                raise SessionLifecycleError(
                    "subagent_stop conflicts with existing terminal evidence"
                )

            if state.finalized:
                return self._terminal_receipt(
                    terminal,
                    state,
                    deduplicated=True,
                )

            records_hook = "subagent_stop" in state.supported_hooks
            if not records_hook:
                if terminal_created:
                    self.ledger.append(terminal.to_row())
                return self._terminal_receipt(
                    terminal,
                    state,
                    deduplicated=not terminal_created,
                )

            callback_context = SessionContext(
                session_id=state.session_id,
                lifecycle_id=state.lifecycle_id,
                source_id=child_session_id,
                turn_id=turn_id,
                source_kind="system",
                observed_at=effective_observed_at,
                fresh=False,
                supported_hooks=state.supported_hooks,
            )
            callback = _Callback(
                context=callback_context,
                hook="subagent_stop",
                settled=False,
                event_id=new_id("session_hook"),
                terminal_reason=terminal_reason,
            )
            self._apply(state, callback, allow_duplicate=False)
            if terminal_created:
                self.ledger.append(terminal.to_row())
            self.ledger.append(callback.to_row())
            return self._receipt(callback, state, deduplicated=False)

    def record_host_shutdown(
        self,
        context: SessionContext,
    ) -> SessionTurnTerminalReceipt | None:
        """Close only an in-flight turn while preserving the reusable session."""

        self._validate_inputs(context, "on_session_finalize", False)
        with file_lock(self.mutation_lock):
            state = self._replay_unlocked().get(context.lifecycle_id)
            if state is None or state.current_turn_id is None:
                return None
            turn_id = state.current_turn_id
            terminal = SessionTurnTerminal(
                schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                event_id=new_id("session_turn_terminal"),
                session_id=state.session_id,
                lifecycle_id=state.lifecycle_id,
                turn_id=turn_id,
                outcome="abandoned",
                reason="host_shutdown",
                superseded_by_turn_id=None,
                observed_at=context.observed_at,
            )
            self._apply_terminal(state, terminal)
            self.ledger.append(terminal.to_row())
            return self._terminal_receipt(terminal, state, deduplicated=False)

    def record_host_finalize(self, context: SessionContext) -> SessionHookReceipt:
        """Finalize a session using already-validated host termination evidence.

        The host adapter, rather than the portable lifecycle core, decides when
        a finalization is authoritative.  Under the normal mutation lock we
        apply the optional abandonment and finalize callback to the in-memory
        state before appending either row.  If the second append fails, replay
        of the first row leaves the finalize callback as the only operation to
        complete on retry.
        """

        self._validate_inputs(context, "on_session_finalize", False)
        callback = _Callback(
            context=context,
            hook="on_session_finalize",
            settled=False,
            event_id=new_id("session_hook"),
        )
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(context.lifecycle_id)
            if state is None:
                state = self._new_state(context)
                states[context.lifecycle_id] = state

            existing = state.callbacks.get(callback.key)
            if existing is not None:
                self._apply(state, callback, allow_duplicate=True)
                callback = existing
                deduplicated = True
            else:
                terminal: SessionTurnTerminal | None = None
                if state.current_turn_id is not None:
                    terminal = SessionTurnTerminal(
                        schema_version=SESSION_TURN_TERMINAL_SCHEMA,
                        event_id=new_id("session_turn_terminal"),
                        session_id=state.session_id,
                        lifecycle_id=state.lifecycle_id,
                        turn_id=state.current_turn_id,
                        outcome="abandoned",
                        reason="host_session_finalized",
                        superseded_by_turn_id=None,
                        observed_at=context.observed_at,
                    )
                    self._apply_terminal(state, terminal)

                # Apply every semantic check before making either durable
                # append.  A rejected finalize therefore cannot leave a
                # terminal row behind.
                self._apply(state, callback, allow_duplicate=True)
                if terminal is not None:
                    self.ledger.append(terminal.to_row())
                self.ledger.append(callback.to_row())
                deduplicated = False

            return self._receipt(
                callback,
                state,
                deduplicated=deduplicated,
            )

    def snapshots(self) -> tuple[SessionLifecycleSnapshot, ...]:
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            return tuple(
                _snapshot(states[lifecycle_id]) for lifecycle_id in sorted(states)
            )

    def replay(self) -> tuple[SessionLifecycleSnapshot, ...]:
        """Validate and return all durable lifecycle snapshots."""

        return self.snapshots()
