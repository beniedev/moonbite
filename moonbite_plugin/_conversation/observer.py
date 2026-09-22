"""Lock-free, content-free conversation observer replay."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..effects import EFFECT_STATES
from ..observer import ObservationFact, RecoveryEvidence
from ..runtime_core import StateError
from .contracts import (
    CHECKPOINT_FAILED_STATES,
    CHECKPOINT_PENDING_STATES,
    CONVERSATION_BRIDGE_SCHEMA,
    _BridgeEvent,
    _parse_event,
    _same_receipt,
)


class _ConversationObserverSchemaError(StateError):
    """The bridge observer encountered an unsupported ledger schema."""


def _conversation_integrity_code(exc: Exception) -> str:
    """Return a stable, content-free integrity code."""

    if isinstance(exc, _ConversationObserverSchemaError):
        return "conversation_schema_error"
    return f"conversation_integrity_error:{type(exc).__name__}"


def _conversation_integrity_fact(
    *, target_date: date, now: datetime, code: str, rows: int | None = None
) -> ObservationFact:
    return ObservationFact(
        key="conversation:integrity",
        code=code,
        state="current" if code != "conversation_ledger_valid" else "neutral",
        target_date=target_date,
        event_time=now,
        refs=("conversation_bridge",),
        counts={
            "errors": 0 if code == "conversation_ledger_valid" else 1,
            **({} if rows is None else {"rows": rows}),
        },
    )


def _lock_free_bridge_rows(path: Path) -> list[dict[str, Any]]:
    """Read bridge JSONL without creating or acquiring its lock."""

    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise StateError(
                    f"conversation observer row {line_number} is not an object"
                )
            rows.append(value)
    return rows


@dataclass
class _ObserverCheckpoint:
    """Content-free checkpoint history used by the lock-free observer."""

    effect_id: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    content_sha256: str
    content_length: int
    expires_at: datetime
    request_event_id: str
    latest_state: str
    latest_event: _BridgeEvent
    history: list[tuple[str, _BridgeEvent]] = field(default_factory=list)


@dataclass
class _ObserverCycle:
    cycle_id: str
    dirty_event_ids: list[str] = field(default_factory=list)
    settled_event_ids: list[str] = field(default_factory=list)
    settled_turn_ids: set[str] = field(default_factory=set)
    last_private_at: datetime | None = None
    last_settled_at: datetime | None = None
    checkpoint: _ObserverCheckpoint | None = None


@dataclass
class _ObserverLifecycle:
    lifecycle_id: str
    session_id: str
    cycles: list[_ObserverCycle] = field(default_factory=list)

    @property
    def current_cycle(self) -> _ObserverCycle | None:
        return None if not self.cycles else self.cycles[-1]

    def cycle(self, cycle_id: str) -> _ObserverCycle | None:
        return next(
            (cycle for cycle in self.cycles if cycle.cycle_id == cycle_id),
            None,
        )


class _ObserverMixin:
    """Read bridge evidence without consulting owners, locks, or writers."""

    @staticmethod
    def _observer_apply_event(state: _ObserverLifecycle, event: _BridgeEvent) -> None:
        """Replay bridge evidence without consulting session/effect owners."""

        if event.lifecycle_id != state.lifecycle_id:
            raise StateError("conversation observer lifecycle identity changed")
        if event.session_id != state.session_id:
            raise StateError("conversation observer session identity changed")

        if event.operation == "ignored":
            return

        if event.operation == "mark_dirty":
            if event.source_kind != "private_inbound" or event.fresh is not True:
                raise StateError("conversation observer dirty evidence is invalid")
            current = state.current_cycle
            existing = state.cycle(event.cycle_id)
            if current is None:
                if existing is not None:
                    raise StateError("conversation observer cycle is out of order")
                current = _ObserverCycle(cycle_id=event.cycle_id)
                state.cycles.append(current)
            elif existing is current:
                if current.checkpoint is not None:
                    raise StateError(
                        "conversation observer dirty event follows checkpoint"
                    )
            elif existing is not None or current.checkpoint is None:
                raise StateError("conversation observer cycle is out of order")
            else:
                current = _ObserverCycle(cycle_id=event.cycle_id)
                state.cycles.append(current)
            if (
                current.last_private_at is not None
                and event.observed_at < current.last_private_at
            ):
                raise StateError("conversation observer dirty events are out of order")
            current.dirty_event_ids.append(event.event_id)
            current.last_private_at = event.observed_at
            return

        if event.operation == "mark_settled":
            if event.source_kind != "assistant_response" or event.turn_id is None:
                raise StateError("conversation observer settlement evidence is invalid")
            current = state.current_cycle
            cycle = state.cycle(event.cycle_id)
            if cycle is None:
                if current is not None and current.checkpoint is None:
                    raise StateError(
                        "conversation observer settlement cycle is unknown"
                    )
                cycle = _ObserverCycle(cycle_id=event.cycle_id)
                state.cycles.append(cycle)
            if cycle is not state.current_cycle or cycle.checkpoint is not None:
                raise StateError("conversation observer settlement is out of order")
            if (
                event.event_id in cycle.settled_event_ids
                or event.turn_id in cycle.settled_turn_ids
            ):
                raise StateError("conversation observer settlement is duplicated")
            if (
                cycle.last_settled_at is not None
                and event.observed_at < cycle.last_settled_at
            ):
                raise StateError("conversation observer settlements are out of order")
            cycle.settled_event_ids.append(event.event_id)
            cycle.settled_turn_ids.add(event.turn_id)
            cycle.last_settled_at = event.observed_at
            return

        if event.operation == "checkpoint_requested":
            cycle = state.cycle(event.cycle_id)
            if cycle is None or cycle is not state.current_cycle:
                raise StateError("conversation observer checkpoint cycle is unknown")
            if cycle.checkpoint is not None:
                raise StateError(
                    "conversation observer has duplicate checkpoint intent"
                )
            if any(
                candidate.checkpoint is not None
                and candidate.checkpoint.latest_state in CHECKPOINT_PENDING_STATES
                for candidate in state.cycles
            ):
                raise StateError("conversation observer has an active checkpoint")
            if not cycle.dirty_event_ids:
                raise StateError(
                    "conversation observer checkpoint has no dirty evidence"
                )
            if cycle.last_settled_at is None or (
                cycle.last_private_at is not None
                and cycle.last_private_at > cycle.last_settled_at
            ):
                raise StateError("conversation observer checkpoint is unsettled")
            if event.effect_state not in EFFECT_STATES:
                raise StateError("conversation observer checkpoint state is invalid")
            if event.effect_state == "verified" or event.receipt is not None:
                raise StateError("conversation observer intent has invalid evidence")
            assert event.effect_id is not None
            assert event.idempotency_key is not None
            assert event.epoch_id is not None
            assert event.content_sha256 is not None
            assert event.content_length is not None
            assert event.expires_at is not None
            if event.expires_at <= event.observed_at:
                raise StateError("conversation observer checkpoint expiry is invalid")
            checkpoint = _ObserverCheckpoint(
                effect_id=event.effect_id,
                source_event_id=event.source_event_id,
                idempotency_key=event.idempotency_key,
                epoch_id=event.epoch_id,
                content_sha256=event.content_sha256,
                content_length=event.content_length,
                expires_at=event.expires_at,
                request_event_id=event.event_id,
                latest_state=event.effect_state,
                latest_event=event,
                history=[(event.effect_state, event)],
            )
            cycle.checkpoint = checkpoint
            return

        if event.operation == "reconcile":
            cycle = state.cycle(event.cycle_id)
            checkpoint = None if cycle is None else cycle.checkpoint
            if checkpoint is None or event.effect_id != checkpoint.effect_id:
                raise StateError("conversation observer reconciliation has no intent")
            if (
                event.source_event_id != checkpoint.source_event_id
                or event.idempotency_key != checkpoint.idempotency_key
                or event.epoch_id != checkpoint.epoch_id
                or event.content_sha256 != checkpoint.content_sha256
                or event.content_length != checkpoint.content_length
                or event.expires_at != checkpoint.expires_at
            ):
                raise StateError(
                    "conversation observer reconciliation identity changed"
                )
            if event.effect_state not in EFFECT_STATES:
                raise StateError(
                    "conversation observer reconciliation state is invalid"
                )
            if event.effect_state == "verified":
                receipt = event.receipt
                if receipt is None or (
                    receipt.event_id != checkpoint.source_event_id
                    or receipt.content_sha256 != checkpoint.content_sha256
                    or receipt.content_length != checkpoint.content_length
                    or receipt.epoch_id != checkpoint.epoch_id
                ):
                    raise StateError(
                        "conversation observer verified evidence is invalid"
                    )
            elif event.receipt is not None:
                raise StateError("conversation observer unverified evidence is invalid")
            previous = checkpoint.history[-1] if checkpoint.history else None
            if previous is not None:
                previous_state, previous_event = previous
                if previous_state == event.effect_state and _same_receipt(
                    previous_event.receipt, event.receipt
                ):
                    raise StateError(
                        "conversation observer reconciliation is duplicated"
                    )
                if previous_state == "verified":
                    raise StateError("conversation observer verified state changed")
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
                    raise StateError(
                        "conversation observer reconciliation is out of order"
                    )
            checkpoint.latest_state = event.effect_state
            checkpoint.latest_event = event
            checkpoint.history.append((event.effect_state, event))
            return

        raise StateError("conversation observer operation is unsupported")

    def _observer_replay(self) -> tuple[dict[str, _ObserverLifecycle], int]:
        """Validate and replay only the bridge ledger, without owner locks."""

        rows = _lock_free_bridge_rows(self.path)
        states: dict[str, _ObserverLifecycle] = {}
        seen_event_ids: set[str] = set()
        for row in rows:
            if row.get("schema_version") != CONVERSATION_BRIDGE_SCHEMA:
                raise _ConversationObserverSchemaError(
                    "conversation bridge schema is unsupported"
                )
            event = _parse_event(row)
            if event.event_id in seen_event_ids:
                raise StateError("conversation observer event_id is duplicated")
            seen_event_ids.add(event.event_id)
            state = states.get(event.lifecycle_id)
            if state is None:
                state = _ObserverLifecycle(
                    lifecycle_id=event.lifecycle_id,
                    session_id=event.session_id,
                )
                states[event.lifecycle_id] = state
            self._observer_apply_event(state, event)
        return states, len(rows)

    @staticmethod
    def _observer_checkpoint_fact(
        state: _ObserverLifecycle,
        checkpoint: _ObserverCheckpoint,
        *,
        target_date: date,
    ) -> ObservationFact:
        latest_state = checkpoint.latest_state
        if latest_state in CHECKPOINT_PENDING_STATES:
            fact_state = "current"
            code = (
                "checkpoint_intent"
                if latest_state == "intent"
                else "checkpoint_pending"
            )
            recovery = None
            counts = {latest_state: 1}
        elif latest_state in CHECKPOINT_FAILED_STATES:
            fact_state = "current"
            code = f"checkpoint_{latest_state}"
            recovery = None
            counts = {latest_state: 1}
        elif latest_state == "verified":
            had_prior_incident = any(
                previous_state in CHECKPOINT_PENDING_STATES
                or previous_state in CHECKPOINT_FAILED_STATES
                for previous_state, _event in checkpoint.history[:-1]
            )
            latest_event = checkpoint.latest_event
            receipt = latest_event.receipt
            if had_prior_incident:
                fact_state = "recovered_history"
                code = "checkpoint_recovered"
                recovery = RecoveryEvidence(
                    ref=receipt.receipt_id
                    if receipt is not None
                    else latest_event.event_id,
                    code="checkpoint_verified",
                    recovered_at=latest_event.observed_at,
                )
                counts = {"recovered": 1}
            else:
                fact_state = "neutral"
                code = "checkpoint_verified"
                recovery = None
                counts = {"verified": 1}
        else:  # Defensive; _parse_event already constrains effect states.
            raise StateError("conversation observer checkpoint state is invalid")

        latest_event = checkpoint.latest_event
        refs = [
            checkpoint.effect_id,
            checkpoint.source_event_id,
            checkpoint.request_event_id,
            latest_event.event_id,
        ]
        if latest_event.receipt is not None:
            refs.append(latest_event.receipt.receipt_id)
        return ObservationFact(
            key=f"conversation:{state.lifecycle_id}:checkpoint:{checkpoint.effect_id}",
            code=code,
            state=fact_state,
            target_date=target_date,
            event_time=latest_event.observed_at,
            refs=tuple(refs),
            counts=counts,
            recovery=recovery,
        )

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return content-free bridge health evidence with no lock or writes."""

        if not self.path.exists():
            return ()
        try:
            states, row_count = self._observer_replay()
        except Exception as exc:
            return (
                _conversation_integrity_fact(
                    target_date=target_date,
                    now=now,
                    code=_conversation_integrity_code(exc),
                ),
            )

        facts: list[ObservationFact] = [
            _conversation_integrity_fact(
                target_date=target_date,
                now=now,
                code="conversation_ledger_valid",
                rows=row_count,
            )
        ]
        for lifecycle_id, state in sorted(states.items()):
            current = state.current_cycle
            if current is None:
                continue
            dirty = bool(current.dirty_event_ids) and (
                current.last_settled_at is None
                or current.last_private_at is None
                or current.last_private_at > current.last_settled_at
            )
            dirty_time = current.last_private_at or now
            dirty_refs = tuple(current.dirty_event_ids) or (lifecycle_id,)
            facts.append(
                ObservationFact(
                    key=f"conversation:{lifecycle_id}:dirty",
                    code="conversation_dirty" if dirty else "conversation_clean",
                    state="current" if dirty else "neutral",
                    target_date=target_date,
                    event_time=dirty_time,
                    refs=dirty_refs,
                    counts={
                        "dirty": int(dirty),
                        "events": len(current.dirty_event_ids),
                    },
                )
            )
            if current.last_settled_at is not None and not dirty:
                facts.append(
                    ObservationFact(
                        key=f"conversation:{lifecycle_id}:settled",
                        code="conversation_settled",
                        state="neutral",
                        target_date=target_date,
                        event_time=current.last_settled_at,
                        refs=tuple(current.settled_event_ids),
                        counts={"settled": len(current.settled_event_ids)},
                    )
                )

            # SessionLifecycleStore.snapshot() acquires/creates its lock and
            # is deliberately not called here.  Without that source evidence,
            # active/quiet/overdue remain explicit unknown-neutral facts.
            evidence_ref = ("session_evidence_missing",)
            for status in ("active", "quiet", "overdue"):
                facts.append(
                    ObservationFact(
                        key=f"conversation:{lifecycle_id}:{status}",
                        code=f"conversation_{status}_unknown",
                        state="neutral",
                        target_date=target_date,
                        event_time=now,
                        refs=evidence_ref,
                        counts={"unknown": 1},
                    )
                )

            for cycle in state.cycles:
                if cycle.checkpoint is None:
                    continue
                facts.append(
                    self._observer_checkpoint_fact(
                        state,
                        cycle.checkpoint,
                        target_date=target_date,
                    )
                )
        return tuple(sorted(facts, key=lambda fact: (fact.key, fact.code)))
