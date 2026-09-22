"""Strict conversation bridge replay against the durable ledger."""

from __future__ import annotations

from ..effects import EFFECT_STATES
from ..runtime_core import StateError
from ..session import SessionHookReceipt
from .contracts import (
    CHECKPOINT_PENDING_STATES,
    ConversationBridgeError,
    _BridgeEvent,
    _CheckpointState,
    _ConversationCycle,
    _ConversationState,
    _parse_event,
    _same_receipt,
)


class _ReplayMixin:
    """Strict state-machine replay using the bridge owner's existing ledger."""

    @staticmethod
    def _new_state(event: _BridgeEvent) -> _ConversationState:
        return _ConversationState(
            lifecycle_id=event.lifecycle_id,
            session_id=event.session_id,
        )

    @staticmethod
    def _same_checkpoint_identity(
        checkpoint: _CheckpointState,
        event: _BridgeEvent,
    ) -> bool:
        return (
            checkpoint.cycle_id == event.cycle_id
            and checkpoint.effect_id == event.effect_id
            and checkpoint.source_event_id == event.source_event_id
            and checkpoint.idempotency_key == event.idempotency_key
            and checkpoint.epoch_id == event.epoch_id
            and checkpoint.content_sha256 == event.content_sha256
            and checkpoint.content_length == event.content_length
            and checkpoint.expires_at == event.expires_at
        )

    def _checkpoint_is_active(self, checkpoint: _CheckpointState) -> bool:
        effect = self._effect_record(checkpoint)
        return effect.state in CHECKPOINT_PENDING_STATES

    def _checkpoint_for_reconcile(
        self,
        state: _ConversationState,
    ) -> tuple[_ConversationCycle, _CheckpointState] | None:
        for cycle in reversed(state.cycles):
            checkpoint = cycle.checkpoint
            if checkpoint is not None and self._checkpoint_is_active(checkpoint):
                return cycle, checkpoint
        for cycle in reversed(state.cycles):
            if cycle.checkpoint is not None:
                return cycle, cycle.checkpoint
        return None

    @staticmethod
    def _new_cycle(state: _ConversationState, cycle_id: str) -> _ConversationCycle:
        if state.cycle(cycle_id) is not None:
            raise StateError("conversation cycle_id already exists")
        cycle = _ConversationCycle(cycle_id=cycle_id)
        state.cycles.append(cycle)
        return cycle

    def _current_cycle_for_dirty(
        self,
        state: _ConversationState,
        cycle_id: str,
    ) -> _ConversationCycle:
        current = state.current_cycle
        existing = state.cycle(cycle_id)
        if current is None:
            if existing is not None:
                raise StateError("conversation cycle replay is out of order")
            return self._new_cycle(state, cycle_id)
        if existing is current:
            if current.checkpoint is not None:
                raise StateError("new dirty event must start a new conversation cycle")
            return current
        if existing is not None:
            raise StateError("conversation cycle replay is out of order")
        if current.checkpoint is None:
            raise StateError("conversation cycle_id changed without checkpoint")
        return self._new_cycle(state, cycle_id)

    def _apply_event(self, state: _ConversationState, event: _BridgeEvent) -> None:
        if event.lifecycle_id != state.lifecycle_id:
            raise StateError("conversation bridge lifecycle_id changed")
        if event.session_id != state.session_id:
            raise StateError("conversation bridge session_id changed")
        if event.event_id in state.events:
            raise StateError("duplicate conversation bridge event_id")

        if event.operation == "ignored":
            # Preserve only the receipt identity.  Ignored lifecycle sources
            # must not create or mutate a conversation cycle.
            pass

        elif event.operation == "mark_dirty":
            if event.source_kind != "private_inbound" or event.fresh is not True:
                raise StateError("only fresh private inbound receipts may mark dirty")
            cycle = self._current_cycle_for_dirty(state, event.cycle_id)
            if (
                cycle.last_private_at is not None
                and event.observed_at < cycle.last_private_at
            ):
                raise StateError("conversation dirty events are out of order")
            cycle.dirty_event_ids.append(event.event_id)
            cycle.last_private_at = event.observed_at

        elif event.operation == "mark_settled":
            if event.source_kind != "assistant_response" or event.turn_id is None:
                raise StateError(
                    "settlement must come from post_llm assistant evidence"
                )
            cycle = state.cycle(event.cycle_id)
            if cycle is None:
                current = state.current_cycle
                if current is not None and current.checkpoint is None:
                    raise StateError("settlement references an unknown cycle")
                cycle = self._new_cycle(state, event.cycle_id)
            elif cycle is not state.current_cycle:
                raise StateError("settlement for an old conversation cycle")
            already_settled = any(
                state.events[event_id].turn_id == event.turn_id
                for event_id in cycle.settled_event_ids
            )
            if (
                cycle.checkpoint is not None
                or already_settled
                or (
                    cycle.last_settled_at is not None
                    and event.observed_at < cycle.last_settled_at
                )
            ):
                raise StateError("conversation settlement is out of order")
            cycle.settled_event_ids.append(event.event_id)
            cycle.last_settled_at = event.observed_at

        elif event.operation == "checkpoint_requested":
            cycle = state.cycle(event.cycle_id)
            if cycle is None or cycle is not state.current_cycle:
                raise StateError("checkpoint request references an unknown cycle")
            if cycle.checkpoint is not None:
                raise StateError("conversation has more than one checkpoint request")
            if any(
                candidate.checkpoint is not None
                and self._checkpoint_is_active(candidate.checkpoint)
                for candidate in state.cycles
            ):
                raise StateError("another checkpoint is still active")
            if not cycle.dirty_event_ids:
                raise StateError(
                    "checkpoint request has no dirty conversation evidence"
                )
            if cycle.last_settled_at is None or (
                cycle.last_private_at is not None
                and cycle.last_private_at > cycle.last_settled_at
            ):
                raise StateError("checkpoint request has an unsettled conversation")
            if event.effect_state not in EFFECT_STATES:
                raise StateError("checkpoint request has an invalid effect state")
            assert event.effect_id is not None
            assert event.idempotency_key is not None
            assert event.epoch_id is not None
            assert event.content_sha256 is not None
            assert event.content_length is not None
            assert event.expires_at is not None
            if event.expires_at <= event.observed_at:
                raise StateError("checkpoint expiry must be in the future")
            cycle.checkpoint = _CheckpointState(
                lifecycle_id=event.lifecycle_id,
                cycle_id=event.cycle_id,
                effect_id=event.effect_id,
                source_event_id=event.source_event_id,
                idempotency_key=event.idempotency_key,
                epoch_id=event.epoch_id,
                content_sha256=event.content_sha256,
                content_length=event.content_length,
                expires_at=event.expires_at,
                request_event_id=event.event_id,
                effect_state=event.effect_state,
            )

        elif event.operation == "reconcile":
            cycle = state.cycle(event.cycle_id)
            checkpoint = None if cycle is None else cycle.checkpoint
            if checkpoint is None or event.effect_id != checkpoint.effect_id:
                raise StateError("reconciliation has no matching checkpoint request")
            if not self._same_checkpoint_identity(checkpoint, event):
                raise StateError("reconciliation changes checkpoint identity")
            if event.effect_state not in EFFECT_STATES:
                raise StateError("reconciliation state is invalid")
            signature = (event.effect_state, event.receipt)
            previous = checkpoint.last_reconciliation
            if previous is not None:
                previous_state, previous_receipt = previous
                if previous_state == event.effect_state and _same_receipt(
                    previous_receipt, event.receipt
                ):
                    raise StateError("duplicate checkpoint reconciliation")
                if previous_state == "verified":
                    raise StateError("verified checkpoint cannot be reconciled again")
                reconciliation_order = {
                    "intent": 0,
                    "requeued": 0,
                    "pending": 1,
                    "executed_unverified": 2,
                }
                if (
                    previous_state in reconciliation_order
                    and event.effect_state in reconciliation_order
                    and reconciliation_order[event.effect_state]
                    < reconciliation_order[previous_state]
                ):
                    raise StateError("checkpoint reconciliation moved backwards")
            checkpoint.effect_state = event.effect_state
            checkpoint.last_reconciliation = signature

        state.events[event.event_id] = event

    def _replay_unlocked(self) -> dict[str, _ConversationState]:
        states: dict[str, _ConversationState] = {}
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            try:
                event = _parse_event(row)
                state = states.get(event.lifecycle_id)
                if state is None:
                    state = self._new_state(event)
                    states[event.lifecycle_id] = state
                self._apply_event(state, event)
            except (ConversationBridgeError, StateError, TypeError, ValueError) as exc:
                raise StateError(
                    f"conversation_bridge.jsonl row {row_number} is invalid"
                ) from exc
        return states

    @staticmethod
    def _receipt_event_matches(
        event: _BridgeEvent,
        receipt: SessionHookReceipt,
        operation: str,
    ) -> bool:
        context = receipt.context
        return (
            event.operation == operation
            and event.source_event_id == receipt.event_id
            and event.source_id == context.source_id
            and event.turn_id == context.turn_id
            and event.source_kind == context.source_kind
            and event.fresh == context.fresh
            and event.observed_at == context.observed_at
        )
