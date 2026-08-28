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
)

SESSION_LIFECYCLE_SCHEMA = "moon.session.lifecycle.v1"
SESSION_SCHEMA = SESSION_LIFECYCLE_SCHEMA
LIFECYCLE_SCHEMA = SESSION_LIFECYCLE_SCHEMA
SCHEMA_VERSION = SESSION_LIFECYCLE_SCHEMA
SESSION_LIFECYCLE_KIND = "hook"

HOOK_ORDER = (
    "pre_gateway_dispatch",
    "on_session_start",
    "pre_llm_call",
    "post_llm_call",
    "on_session_finalize",
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
class _Callback:
    context: SessionContext
    hook: str
    settled: bool
    event_id: str

    @property
    def key(self) -> tuple[str, str, str | None]:
        return (self.hook, self.context.source_id, self.context.turn_id)

    def to_row(self) -> dict[str, Any]:
        return {
            "schema_version": SESSION_LIFECYCLE_SCHEMA,
            "kind": SESSION_LIFECYCLE_KIND,
            "event_id": self.event_id,
            **self.context.to_dict(),
            "hook": self.hook,
            "settled": self.settled,
        }


@dataclass(slots=True)
class _TurnState:
    post_seen: bool = False
    settled: bool = False


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
        if hook == "on_session_finalize" and context.source_kind != "system":
            raise SessionLifecycleError(
                "on_session_finalize requires source_kind=system"
            )
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
            if state.current_turn_id != turn_id:
                raise SessionLifecycleError(
                    "post_llm_call must match the current pre_llm_call turn"
                )
            turn = state.turns[turn_id]
            if turn.settled:
                raise SessionLifecycleError("turn is already settled")
            turn.post_seen = True
            if callback.settled:
                turn.settled = True
                state.settled_turn_ids.append(turn_id)
                state.current_turn_id = None

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
            not turn.settled for turn in state.turns.values()
        ):
            raise SessionLifecycleError(
                "on_session_finalize requires all turns to be settled"
            )
        if "post_llm_call" in supported and not any(
            turn.post_seen for turn in state.turns.values()
        ):
            raise SessionLifecycleError("on_session_finalize requires post_llm_call")

    @staticmethod
    def _callback_from_row(row: Mapping[str, Any]) -> _Callback:
        if set(row) != _ROW_FIELDS:
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
        )

    def _replay_unlocked(self) -> dict[str, _LifecycleState]:
        states: dict[str, _LifecycleState] = {}
        event_ids: set[str] = set()
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            try:
                callback = self._callback_from_row(row)
                if callback.event_id in event_ids:
                    raise SessionLifecycleError("duplicate session lifecycle event_id")
                event_ids.add(callback.event_id)
                state = states.get(callback.context.lifecycle_id)
                if state is None:
                    state = self._new_state(callback.context)
                    states[callback.context.lifecycle_id] = state
                self._apply(state, callback, allow_duplicate=False)
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
            deduplicated = self._apply(state, callback, allow_duplicate=True)
            if deduplicated:
                existing = state.callbacks[callback.key]
                callback = existing
            else:
                # JsonlLedger propagates all write errors. State is rebuilt from
                # disk on the next retry, so a failed append cannot leak a
                # partially accepted callback into this store instance.
                self.ledger.append(callback.to_row())
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

    def snapshots(self) -> tuple[SessionLifecycleSnapshot, ...]:
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            return tuple(
                _snapshot(states[lifecycle_id]) for lifecycle_id in sorted(states)
            )

    def replay(self) -> tuple[SessionLifecycleSnapshot, ...]:
        """Validate and return all durable lifecycle snapshots."""

        return self.snapshots()
