"""Conversation checkpoint state and effect coordination."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from ..effects import EFFECT_STATES, EffectReceipt
from ..runtime_core import StateError, file_lock, new_id
from ..session import SessionLifecycleSnapshot
from .contracts import (
    CHECKPOINT_FAILED_STATES,
    CHECKPOINT_PENDING_STATES,
    CONVERSATION_BRIDGE_SCHEMA,
    CheckpointRequest,
    ConversationBridgeError,
    ConversationGateError,
    ConversationSnapshot,
    _BridgeEvent,
    _CheckpointState,
    _ConversationCycle,
    _ConversationState,
    _aware,
    _hash,
    _positive_int,
    _reference,
    _scoped_idempotency_key,
)

_SESSION_SNAPSHOT_MISSING = object()


class _CheckpointMixin:
    """Checkpoint projection, request, and reconciliation for one owner."""

    def _session_snapshot(
        self,
        state: _ConversationState,
    ) -> SessionLifecycleSnapshot | Any | None:
        try:
            snapshot = self.session_store.snapshot(state.lifecycle_id)
        except (ConversationBridgeError, StateError):
            raise
        except Exception as exc:
            raise StateError("session lifecycle snapshot is unreadable") from exc
        if snapshot is None:
            return None

        snapshot_session_id = getattr(snapshot, "session_id", _SESSION_SNAPSHOT_MISSING)
        snapshot_lifecycle_id = getattr(
            snapshot, "lifecycle_id", _SESSION_SNAPSHOT_MISSING
        )
        open_turn_id = getattr(snapshot, "open_turn_id", _SESSION_SNAPSHOT_MISSING)
        settled_turn_ids = getattr(
            snapshot, "settled_turn_ids", _SESSION_SNAPSHOT_MISSING
        )
        if any(
            value is _SESSION_SNAPSHOT_MISSING
            for value in (
                snapshot_session_id,
                snapshot_lifecycle_id,
                open_turn_id,
                settled_turn_ids,
            )
        ):
            return None
        try:
            _reference(snapshot_session_id, "session_id")
            _reference(snapshot_lifecycle_id, "lifecycle_id")
        except (TypeError, ValueError) as exc:
            raise StateError("session lifecycle snapshot identity is invalid") from exc
        if (
            snapshot_session_id != state.session_id
            or snapshot_lifecycle_id != state.lifecycle_id
        ):
            raise StateError("session lifecycle snapshot identity conflicts")
        try:
            if open_turn_id is not None:
                _reference(open_turn_id, "open_turn_id")
            if not isinstance(settled_turn_ids, (tuple, list)):
                raise ValueError("settled_turn_ids must be a tuple or list")
            for turn_id in settled_turn_ids:
                _reference(turn_id, "turn_id")
        except (TypeError, ValueError) as exc:
            raise StateError("session lifecycle snapshot evidence is invalid") from exc
        return snapshot

    @staticmethod
    def _effect_receipt(effect: Any) -> EffectReceipt | None:
        receipt = getattr(effect, "receipt", None)
        if receipt is not None and not isinstance(receipt, EffectReceipt):
            raise StateError("checkpoint effect receipt is invalid")
        return receipt

    def _effect_record(self, checkpoint: _CheckpointState) -> Any:
        try:
            effect = self.effect_ledger.get(checkpoint.effect_id)
        except (ConversationBridgeError, StateError):
            raise
        except Exception as exc:
            raise StateError("checkpoint effect state is unreadable") from exc
        if effect is None:
            raise StateError("checkpoint effect reference is missing")
        if (
            getattr(effect, "kind", None) != "checkpoint"
            or getattr(effect, "source_event_id", None) != checkpoint.source_event_id
            or getattr(effect, "idempotency_key", None)
            not in {
                _scoped_idempotency_key(
                    checkpoint.lifecycle_id,
                    checkpoint.cycle_id,
                    checkpoint.idempotency_key,
                ),
                # Bridge rows written before scoped ledger identities remain
                # readable; all new requests use the scoped identity above.
                checkpoint.idempotency_key,
            }
            or getattr(effect, "epoch_id", None) != checkpoint.epoch_id
            or getattr(effect, "content_sha256", None) != checkpoint.content_sha256
            or getattr(effect, "content_length", None) != checkpoint.content_length
        ):
            raise StateError("checkpoint effect identity conflicts")
        effect_state = getattr(effect, "state", None)
        if effect_state not in EFFECT_STATES:
            raise StateError("checkpoint effect state is invalid")
        receipt = self._effect_receipt(effect)
        if effect_state == "verified":
            if receipt is None:
                raise StateError("verified checkpoint has no receipt evidence")
            if (
                receipt.event_id != checkpoint.source_event_id
                or receipt.content_sha256 != checkpoint.content_sha256
                or receipt.content_length != checkpoint.content_length
                or receipt.epoch_id != checkpoint.epoch_id
            ):
                raise StateError("checkpoint receipt does not match intent")
        elif receipt is not None:
            raise StateError("unverified checkpoint carries receipt evidence")
        return effect

    @staticmethod
    def _effect_evidence(effect: Any) -> Mapping[str, Any] | None:
        receipt = getattr(effect, "receipt", None)
        if receipt is not None:
            if not isinstance(receipt, EffectReceipt):
                raise StateError("checkpoint effect receipt is invalid")
            return receipt.to_dict()
        evidence = getattr(effect, "evidence", None)
        if isinstance(evidence, Mapping):
            return dict(evidence)
        return None

    def _checkpoint_status(
        self,
        state: _ConversationState,
        cycle: _ConversationCycle | None,
        effect: Any | None,
        *,
        session_evidence: bool,
    ) -> tuple[str, Mapping[str, Any] | None]:
        checkpoint = None if cycle is None else cycle.checkpoint
        if checkpoint is not None:
            if effect is None:
                raise StateError("checkpoint effect reference is missing")
            effect_state = getattr(effect, "state", None)
            if effect_state in CHECKPOINT_PENDING_STATES:
                return "checkpoint_pending", self._effect_evidence(effect)
            if effect_state == "verified":
                # _effect_record already checked all receipt fields.
                if not session_evidence:
                    return "checkpoint_unverified", self._effect_evidence(effect)
                return "checkpoint_complete", self._effect_evidence(effect)
            if effect_state in CHECKPOINT_FAILED_STATES:
                return "checkpoint_failed", self._effect_evidence(effect)
            raise StateError("checkpoint effect state cannot be reconciled")

        # A newer dirty cycle may coexist with an older pending checkpoint.
        # It remains blocked until the older effect reaches a terminal state.
        for candidate in state.cycles:
            old_checkpoint = candidate.checkpoint
            if old_checkpoint is None:
                continue
            old_effect = self._effect_record(old_checkpoint)
            if old_effect.state in CHECKPOINT_PENDING_STATES:
                return "checkpoint_pending", self._effect_evidence(old_effect)
        return "idle", None

    def _build_snapshot(
        self,
        state: _ConversationState,
        *,
        now: datetime,
        effect_override: Any | None = None,
    ) -> ConversationSnapshot:
        effective_now = _aware(now, "now")
        cycle = state.current_cycle
        session_snapshot = self._session_snapshot(state)
        if session_snapshot is None:
            open_turn_id = None
            settled_turn_ids: tuple[str, ...] = ()
            terminal_turn_ids: tuple[str, ...] = ()
            abandoned_turn_ids: tuple[str, ...] = ()
            session_evidence = False
        else:
            raw_open_turn = session_snapshot.open_turn_id
            open_turn_id = (
                None
                if raw_open_turn is None
                else _reference(raw_open_turn, "open_turn_id")
            )
            raw_settled = session_snapshot.settled_turn_ids
            if not isinstance(raw_settled, (tuple, list)):
                raise StateError("session settled_turn_ids are invalid")
            settled_turn_ids = tuple(
                _reference(item, "turn_id") for item in raw_settled
            )
            terminal_field_present = hasattr(session_snapshot, "terminal_turn_ids")
            abandoned_field_present = hasattr(session_snapshot, "abandoned_turn_ids")
            raw_terminal = getattr(session_snapshot, "terminal_turn_ids", ())
            raw_abandoned = getattr(session_snapshot, "abandoned_turn_ids", ())
            if not isinstance(raw_terminal, (tuple, list)):
                raise StateError("session terminal_turn_ids are invalid")
            if not isinstance(raw_abandoned, (tuple, list)):
                raise StateError("session abandoned_turn_ids are invalid")
            try:
                terminal_turn_ids = tuple(
                    _reference(item, "turn_id") for item in raw_terminal
                )
                abandoned_turn_ids = tuple(
                    _reference(item, "turn_id") for item in raw_abandoned
                )
            except (TypeError, ValueError) as exc:
                raise StateError("session terminal turn evidence is invalid") from exc
            if len(terminal_turn_ids) != len(set(terminal_turn_ids)):
                raise StateError("session terminal_turn_ids are duplicated")
            if len(abandoned_turn_ids) != len(set(abandoned_turn_ids)):
                raise StateError("session abandoned_turn_ids are duplicated")
            terminal_set = set(terminal_turn_ids)
            if terminal_field_present and not set(settled_turn_ids).issubset(
                terminal_set
            ):
                raise StateError("session settled turns are missing terminal evidence")
            if abandoned_field_present and not set(abandoned_turn_ids).issubset(
                terminal_set
            ):
                raise StateError(
                    "session abandoned turns are missing terminal evidence"
                )
            if (
                terminal_field_present
                and abandoned_field_present
                and (set(settled_turn_ids) & set(abandoned_turn_ids))
            ):
                raise StateError("session turn has conflicting terminal outcomes")
            if open_turn_id is not None and open_turn_id in terminal_set:
                raise StateError("session open turn has terminal evidence")
            session_evidence = True

        dirty = cycle is not None and bool(cycle.dirty_event_ids)
        if not dirty:
            unsettled = False
        elif not session_evidence:
            # Missing source evidence is fail-closed for effect gates.
            unsettled = True
        else:
            unsettled = open_turn_id is not None or (
                cycle is None
                or cycle.last_settled_at is None
                or (
                    cycle.last_private_at is not None
                    and cycle.last_private_at > cycle.last_settled_at
                )
            )
        settled = (
            cycle is not None and cycle.last_settled_at is not None and not unsettled
        )
        latest_terminal_turn_id = terminal_turn_ids[-1] if terminal_turn_ids else None
        latest_terminal_is_abandoned = (
            latest_terminal_turn_id is not None
            and latest_terminal_turn_id in abandoned_turn_ids
        )
        active_chat = open_turn_id is not None or (
            unsettled and not latest_terminal_is_abandoned
        )
        quiet_until = (
            None
            if cycle is None or cycle.last_settled_at is None
            else cycle.last_settled_at + self.quiet_window
        )
        overdue_at = (
            None
            if cycle is None or cycle.last_private_at is None
            else cycle.last_private_at + self.overdue_window
        )
        quiet = quiet_until is not None and effective_now >= quiet_until
        overdue = overdue_at is not None and effective_now >= overdue_at

        effect = effect_override
        if cycle is not None and cycle.checkpoint is not None and effect is None:
            effect = self._effect_record(cycle.checkpoint)
        checkpoint_state, evidence = self._checkpoint_status(
            state,
            cycle,
            effect,
            session_evidence=session_evidence,
        )
        if checkpoint_state != "idle":
            high_level_state = checkpoint_state
        elif not dirty:
            high_level_state = "clean"
        elif unsettled:
            high_level_state = "dirty"
        else:
            high_level_state = "settled"

        if not dirty:
            blocked_reason = "clean"
        elif not session_evidence:
            blocked_reason = "session_evidence_missing"
        elif active_chat:
            blocked_reason = "active_chat"
        elif checkpoint_state != "idle":
            blocked_reason = checkpoint_state
        elif not settled:
            blocked_reason = "unsettled"
        elif not (quiet or overdue):
            blocked_reason = "quiet_window"
        else:
            blocked_reason = None
        has_active_checkpoint = any(
            candidate.checkpoint is not None
            and self._effect_record(candidate.checkpoint).state
            in CHECKPOINT_PENDING_STATES
            for candidate in state.cycles
        )
        can_checkpoint = (
            dirty
            and session_evidence
            and settled
            and not active_chat
            and (quiet or overdue)
            and checkpoint_state == "idle"
            and not has_active_checkpoint
        )
        current_checkpoint = None if cycle is None else cycle.checkpoint
        checkpoint_effect_id = (
            None if current_checkpoint is None else current_checkpoint.effect_id
        )
        if checkpoint_effect_id is None:
            checkpoint_effect_id = next(
                (
                    candidate.checkpoint.effect_id
                    for candidate in state.cycles
                    if candidate.checkpoint is not None
                    and self._effect_record(candidate.checkpoint).state
                    in CHECKPOINT_PENDING_STATES
                ),
                None,
            )
        return ConversationSnapshot(
            schema_version=CONVERSATION_BRIDGE_SCHEMA,
            session_id=state.session_id,
            lifecycle_id=state.lifecycle_id,
            state=high_level_state,
            dirty=dirty,
            settled=settled,
            unsettled=unsettled,
            active_chat=active_chat,
            open_turn_id=open_turn_id,
            settled_turn_ids=settled_turn_ids,
            last_private_at=None if cycle is None else cycle.last_private_at,
            last_settled_at=None if cycle is None else cycle.last_settled_at,
            quiet_until=quiet_until,
            overdue_at=overdue_at,
            quiet=quiet,
            overdue=overdue,
            checkpoint_requested=(
                current_checkpoint is not None or has_active_checkpoint
            ),
            checkpoint_state=checkpoint_state,
            checkpoint_effect_id=checkpoint_effect_id,
            checkpoint_evidence=evidence,
            can_checkpoint=can_checkpoint,
            blocked_reason=blocked_reason,
        )

    @staticmethod
    def _validate_checkpoint_arguments(
        *,
        lifecycle_id: str,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content_sha256: str,
        content_length: int,
        expires_at: datetime,
    ) -> tuple[str, str, str, str, str, int, datetime]:
        return (
            _reference(lifecycle_id, "lifecycle_id"),
            _reference(source_event_id, "source_event_id"),
            _reference(idempotency_key, "idempotency_key"),
            _reference(epoch_id, "epoch_id"),
            _hash(content_sha256),
            _positive_int(content_length, "content_length"),
            _aware(expires_at, "expires_at"),
        )

    @staticmethod
    def _effect_identity_matches_request(
        effect: Any,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content_sha256: str,
        content_length: int,
    ) -> bool:
        return (
            getattr(effect, "kind", None) == "checkpoint"
            and getattr(effect, "source_event_id", None) == source_event_id
            and getattr(effect, "idempotency_key", None) == idempotency_key
            and getattr(effect, "epoch_id", None) == epoch_id
            and getattr(effect, "content_sha256", None) == content_sha256
            and getattr(effect, "content_length", None) == content_length
        )

    def request_checkpoint(
        self,
        lifecycle_id: str,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content_sha256: str,
        content_length: int,
        expires_at: datetime,
        now: datetime | None = None,
    ) -> CheckpointRequest:
        """Append/reuse exactly one checkpoint effect intent.

        The caller supplies only a digest and length.  The bridge intentionally
        has no body parameter and writes no body field.
        """

        (
            lifecycle_id,
            source_event_id,
            idempotency_key,
            epoch_id,
            content_sha256,
            content_length,
            expires_at,
        ) = self._validate_checkpoint_arguments(
            lifecycle_id=lifecycle_id,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            content_sha256=content_sha256,
            content_length=content_length,
            expires_at=expires_at,
        )
        effective_now = _aware(self.clock() if now is None else now, "now")
        if expires_at <= effective_now:
            raise ValueError("checkpoint expires_at must be later than now")
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(lifecycle_id)
            if state is None:
                raise ConversationGateError("session_evidence_missing")
            cycle = state.current_cycle
            if cycle is None:
                raise ConversationGateError("session_evidence_missing")

            if cycle.checkpoint is not None:
                checkpoint = cycle.checkpoint
                if (
                    checkpoint.source_event_id != source_event_id
                    or checkpoint.idempotency_key != idempotency_key
                    or checkpoint.epoch_id != epoch_id
                    or checkpoint.content_sha256 != content_sha256
                    or checkpoint.content_length != content_length
                    or checkpoint.expires_at != expires_at
                ):
                    raise ConversationBridgeError(
                        "checkpoint request identity conflicts"
                    )
                effect = self._effect_record(checkpoint)
                snapshot = self._build_snapshot(
                    state,
                    now=effective_now,
                    effect_override=effect,
                )
                return CheckpointRequest(
                    effect=effect,
                    snapshot=snapshot,
                    request_idempotency_key=idempotency_key,
                )

            snapshot = self._build_snapshot(state, now=effective_now)
            if not snapshot.can_checkpoint:
                raise ConversationGateError(
                    snapshot.blocked_reason or "checkpoint_not_allowed",
                    snapshot=snapshot,
                )

            ledger_idempotency_key = _scoped_idempotency_key(
                lifecycle_id, cycle.cycle_id, idempotency_key
            )
            effect_id = new_id("checkpoint")
            effect = self.effect_ledger.begin_intent(
                effect_id,
                kind="checkpoint",
                source_event_id=source_event_id,
                idempotency_key=ledger_idempotency_key,
                epoch_id=epoch_id,
                content_sha256=content_sha256,
                content_length=content_length,
                expires_at=expires_at,
                created_at=effective_now,
            )
            if not self._effect_identity_matches_request(
                effect,
                source_event_id=source_event_id,
                idempotency_key=ledger_idempotency_key,
                epoch_id=epoch_id,
                content_sha256=content_sha256,
                content_length=content_length,
            ):
                raise StateError("checkpoint effect identity conflicts with request")
            if any(
                candidate.checkpoint is not None
                and candidate.checkpoint.effect_id == effect.effect_id
                for candidate in state.cycles
            ):
                raise ConversationBridgeError(
                    "checkpoint idempotency key belongs to an earlier cycle"
                )
            if (
                effect.state not in {"requeued", "expired"}
                and effect.expires_at != expires_at
            ):
                raise ConversationBridgeError(
                    "checkpoint effect expiry conflicts with request"
                )
            event = _BridgeEvent(
                operation="checkpoint_requested",
                event_id=new_id("conversation_checkpoint"),
                lifecycle_id=lifecycle_id,
                session_id=state.session_id,
                cycle_id=cycle.cycle_id,
                source_event_id=source_event_id,
                source_id=None,
                turn_id=None,
                source_kind=None,
                fresh=None,
                observed_at=effective_now,
                effect_id=effect.effect_id,
                idempotency_key=idempotency_key,
                epoch_id=epoch_id,
                content_sha256=content_sha256,
                content_length=content_length,
                expires_at=expires_at,
                effect_state=effect.state,
                receipt=None,
            )
            self._apply_event(state, event)
            # This append follows the effect append intentionally.  If it
            # fails, retrying the same idempotency key reuses the existing
            # effect and can safely record its reference.
            self.ledger.append(event.to_row())
            snapshot = self._build_snapshot(
                state,
                now=effective_now,
                effect_override=effect,
            )
            return CheckpointRequest(
                effect=effect,
                snapshot=snapshot,
                request_idempotency_key=idempotency_key,
            )

    def reconcile(
        self,
        lifecycle_id: str,
        *,
        now: datetime | None = None,
    ) -> ConversationSnapshot | None:
        """Persist the latest adapter state/evidence and return a snapshot."""

        lifecycle_id = _reference(lifecycle_id, "lifecycle_id")
        effective_now = _aware(self.clock() if now is None else now, "now")
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(lifecycle_id)
            if state is None:
                return None
            selected = self._checkpoint_for_reconcile(state)
            if selected is None:
                return self._build_snapshot(state, now=effective_now)
            cycle, checkpoint = selected
            effect = self._effect_record(checkpoint)
            effect_state = effect.state
            receipt = self._effect_receipt(effect)
            signature = (effect_state, receipt)
            if checkpoint.last_reconciliation == signature:
                return self._build_snapshot(
                    state,
                    now=effective_now,
                    effect_override=effect,
                )
            event = _BridgeEvent(
                operation="reconcile",
                event_id=new_id("conversation_reconcile"),
                lifecycle_id=lifecycle_id,
                session_id=state.session_id,
                cycle_id=cycle.cycle_id,
                source_event_id=checkpoint.source_event_id,
                source_id=None,
                turn_id=None,
                source_kind=None,
                fresh=None,
                observed_at=effective_now,
                effect_id=checkpoint.effect_id,
                idempotency_key=checkpoint.idempotency_key,
                epoch_id=checkpoint.epoch_id,
                content_sha256=checkpoint.content_sha256,
                content_length=checkpoint.content_length,
                expires_at=checkpoint.expires_at,
                effect_state=effect_state,
                receipt=receipt,
            )
            self._apply_event(state, event)
            self.ledger.append(event.to_row())
            return self._build_snapshot(
                state,
                now=effective_now,
                effect_override=effect,
            )

    def reconcile_all(
        self,
        *,
        now: datetime | None = None,
    ) -> tuple[ConversationSnapshot, ...]:
        effective_now = self.clock() if now is None else now
        lifecycle_ids = [
            item.lifecycle_id for item in self.snapshots(now=effective_now)
        ]
        return tuple(
            snapshot
            for lifecycle_id in lifecycle_ids
            if (snapshot := self.reconcile(lifecycle_id, now=effective_now)) is not None
        )
