"""Conversation bridge facade assembled from focused internal responsibilities."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path

from ..effects import EffectLedger
from ..runtime_core import JsonlLedger, file_lock, new_id, utc_now
from ..session import SessionHookReceipt, SessionLifecycleStore
from .checkpoints import _CheckpointMixin
from .contracts import (
    CONVERSATION_BRIDGE_SCHEMA,
    ConversationBridgeError,
    ConversationReceipt,
    ConversationSnapshot,
    _BridgeEvent,
    _ConversationState,
    _aware,
    _reference,
    _validate_session_receipt,
)
from .observer import _ObserverMixin
from .replay import _ReplayMixin


class ConversationBridge:
    """Replayable dirty/settled/checkpoint bridge.

    SessionLifecycleStore and EffectLedger are dependencies rather than
    alternate writers.  The bridge owns only conversation_bridge.jsonl and
    one mutation lock.  Every public read replays that ledger while holding
    the lock.
    """

    def __init__(
        self,
        root: Path,
        session_store: SessionLifecycleStore | None = None,
        effect_ledger: EffectLedger | None = None,
        *,
        sessions: SessionLifecycleStore | None = None,
        effects: EffectLedger | None = None,
        quiet_window: timedelta = timedelta(minutes=5),
        overdue_window: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if (
            session_store is not None
            and sessions is not None
            and session_store is not sessions
        ):
            raise TypeError("session_store and sessions name different owners")
        if (
            effect_ledger is not None
            and effects is not None
            and effect_ledger is not effects
        ):
            raise TypeError("effect_ledger and effects name different owners")
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "conversation_bridge.jsonl")
        self.path = self.ledger.path
        self.mutation_lock = self.root / "conversation_bridge.mutation.lock"
        self.mutation_lock_path = self.mutation_lock
        self.session_store = session_store if session_store is not None else sessions
        self.effect_ledger = effect_ledger if effect_ledger is not None else effects
        if self.session_store is None:
            self.session_store = SessionLifecycleStore(self.root)
        if self.effect_ledger is None:
            self.effect_ledger = EffectLedger(self.root, clock=clock)
        self.quiet_window = self._window(quiet_window, "quiet_window")
        self.overdue_window = self._window(overdue_window, "overdue_window")
        self.clock = clock

    @staticmethod
    def _window(value: timedelta, label: str) -> timedelta:
        if not isinstance(value, timedelta) or value.total_seconds() < 0:
            raise ValueError(f"{label} must be a non-negative timedelta")
        return value

    def observe(self, receipt: SessionHookReceipt) -> ConversationReceipt:
        """Consume one typed session receipt without storing its body."""

        counts_private, settles_turn = _validate_session_receipt(receipt)
        operation = (
            "mark_dirty"
            if counts_private
            else "mark_settled"
            if settles_turn
            else "ignored"
        )
        effective_now = _aware(self.clock(), "clock")
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(receipt.lifecycle_id)
            if state is None:
                state = _ConversationState(
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=receipt.context.session_id,
                )
                states[receipt.lifecycle_id] = state
            existing = state.events.get(receipt.event_id)
            deduplicated = existing is not None
            if existing is not None:
                if not self._receipt_event_matches(existing, receipt, operation):
                    raise ConversationBridgeError(
                        "session receipt conflicts with bridge evidence"
                    )
            elif operation == "ignored":
                context = receipt.context
                current = state.current_cycle
                cycle_id = (
                    current.cycle_id
                    if current is not None
                    else "ignored:"
                    + hashlib.sha256(receipt.event_id.encode("utf-8")).hexdigest()
                )
                event = _BridgeEvent(
                    operation=operation,
                    event_id=receipt.event_id,
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=context.session_id,
                    cycle_id=cycle_id,
                    source_event_id=receipt.event_id,
                    source_id=context.source_id,
                    turn_id=context.turn_id,
                    source_kind=context.source_kind,
                    fresh=context.fresh,
                    observed_at=context.observed_at,
                    effect_id=None,
                    idempotency_key=None,
                    epoch_id=None,
                    content_sha256=None,
                    content_length=None,
                    expires_at=None,
                    effect_state=None,
                    receipt=None,
                )
                self._apply_event(state, event)
                self.ledger.append(event.to_row())
            elif operation != "ignored":
                context = receipt.context
                current = state.current_cycle
                if current is None or current.checkpoint is not None:
                    cycle_id = new_id("conversation_cycle")
                else:
                    cycle_id = current.cycle_id
                event = _BridgeEvent(
                    operation=operation,
                    event_id=receipt.event_id,
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=context.session_id,
                    cycle_id=cycle_id,
                    source_event_id=receipt.event_id,
                    source_id=context.source_id,
                    turn_id=context.turn_id,
                    source_kind=context.source_kind,
                    fresh=context.fresh,
                    observed_at=context.observed_at,
                    effect_id=None,
                    idempotency_key=None,
                    epoch_id=None,
                    content_sha256=None,
                    content_length=None,
                    expires_at=None,
                    effect_state=None,
                    receipt=None,
                )
                self._apply_event(state, event)
                # Do not expose a state transition until the append succeeds.
                self.ledger.append(event.to_row())
            snapshot = self._build_snapshot(state, now=effective_now)
            return ConversationReceipt(
                schema_version=CONVERSATION_BRIDGE_SCHEMA,
                event_id=receipt.event_id,
                lifecycle_id=receipt.lifecycle_id,
                operation=operation,
                deduplicated=deduplicated,
                snapshot=snapshot,
            )

    def _snapshot_unlocked(
        self,
        lifecycle_id: str,
        *,
        now: datetime,
        states: Mapping[str, _ConversationState] | None = None,
    ) -> ConversationSnapshot | None:
        _reference(lifecycle_id, "lifecycle_id")
        actual_states = self._replay_unlocked() if states is None else states
        state = actual_states.get(lifecycle_id)
        if state is None:
            return None
        return self._build_snapshot(state, now=now)

    def snapshot(
        self,
        lifecycle_id: str,
        *,
        now: datetime | None = None,
    ) -> ConversationSnapshot | None:
        effective_now = self.clock() if now is None else now
        with file_lock(self.mutation_lock):
            return self._snapshot_unlocked(
                lifecycle_id,
                now=effective_now,
            )

    def evaluate(
        self,
        lifecycle_id: str,
        *,
        now: datetime | None = None,
    ) -> ConversationSnapshot | None:
        """Read-only gate evaluation for heartbeat/autonomy callers."""

        return self.snapshot(lifecycle_id, now=now)

    def snapshots(
        self,
        *,
        now: datetime | None = None,
    ) -> tuple[ConversationSnapshot, ...]:
        effective_now = self.clock() if now is None else now
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            return tuple(
                self._build_snapshot(states[lifecycle_id], now=effective_now)
                for lifecycle_id in sorted(states)
            )

    def replay(self) -> tuple[ConversationSnapshot, ...]:
        """Validate and return all durable bridge snapshots."""

        return self.snapshots()


_RESPONSIBILITY_METHODS = (
    (
        _ReplayMixin,
        (
            "_new_state",
            "_same_checkpoint_identity",
            "_checkpoint_is_active",
            "_checkpoint_for_reconcile",
            "_new_cycle",
            "_current_cycle_for_dirty",
            "_apply_event",
            "_replay_unlocked",
            "_receipt_event_matches",
        ),
    ),
    (
        _CheckpointMixin,
        (
            "_session_snapshot",
            "_effect_receipt",
            "_effect_record",
            "_effect_evidence",
            "_checkpoint_status",
            "_build_snapshot",
            "_validate_checkpoint_arguments",
            "_effect_identity_matches_request",
            "request_checkpoint",
            "reconcile",
            "reconcile_all",
        ),
    ),
    (
        _ObserverMixin,
        (
            "_observer_apply_event",
            "_observer_replay",
            "_observer_checkpoint_fact",
            "observer_status",
        ),
    ),
)
for _responsibility, _method_names in _RESPONSIBILITY_METHODS:
    for _method_name in _method_names:
        setattr(
            ConversationBridge,
            _method_name,
            _responsibility.__dict__[_method_name],
        )

del _method_name, _method_names, _responsibility
