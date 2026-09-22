"""Receipt-backed memory writer coordination and read-only projections."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from .contracts import _text, _time, content_descriptor
from .observation import (
    observer_integrity_fact as _observer_integrity_fact,
    observer_jsonl_rows as _observer_jsonl_rows,
    observer_merge_facts as _observer_merge_facts,
    observer_validate_context as _observer_validate_context,
)
from ..effects import EffectReceipt, EffectRecord, _valid_transition
from ..observer import ObservationFact, RecoveryEvidence
from ..runtime_core import StateError, file_lock, utc_now

WRITER_OPERATIONS = frozenset(
    {"turn_persistence", "flush", "diary", "consolidation", "maintenance"}
)

_MAX_REASON_BYTES = 4 * 1024
_WRITER_CURRENT_STATES = frozenset(
    {"pending", "executed_unverified", "expired", "failed", "requeued"}
)


@dataclass(frozen=True, slots=True)
class WriterRequest:
    """Typed, transient handoff envelope; only its descriptor is durable."""

    effect_id: str
    operation: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    content_sha256: str
    content_length: int
    attempt: int
    content: Any


@dataclass(frozen=True, slots=True)
class WriterHandoff:
    operation: str
    effect_id: str
    record: Any
    error_type: str | None = None
    request: WriterRequest | None = None

    @property
    def queued(self) -> bool:
        return self.record.state == "executed_unverified"

    @property
    def verified(self) -> bool:
        return self.record.state == "verified"

    @property
    def failed(self) -> bool:
        return self.record.state == "failed"


class WriterCoordinator:
    """Create receipt-backed writer intents before handing work to an adapter."""

    def __init__(
        self, effect_ledger: Any, *, clock: Callable[[], datetime] = utc_now
    ) -> None:
        required = (
            "get",
            "begin_intent",
            "mark_pending",
            "mark_queue_accepted",
            "verify",
            "fail",
        )
        if any(not callable(getattr(effect_ledger, name, None)) for name in required):
            raise TypeError("effect_ledger does not provide the required port")
        self.effect_ledger = effect_ledger
        self.clock = clock
        self._handoff_thread_lock = Lock()
        lock_path = getattr(effect_ledger, "mutation_lock_path", None)
        if lock_path is None:
            lock_path = getattr(effect_ledger, "mutation_lock", None)
        self._handoff_lock_path = (
            None
            if lock_path is None
            else Path(lock_path).with_name(f"{Path(lock_path).name}.writer")
        )

    @contextmanager
    def _claim_lock(self):
        """Serialize the intent-to-pending claim across threads/processes."""

        with self._handoff_thread_lock:
            if self._handoff_lock_path is None:
                yield
            else:
                with file_lock(self._handoff_lock_path):
                    yield

    def create_intent(
        self,
        operation: str,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> EffectRecord:
        if operation not in WRITER_OPERATIONS:
            raise ValueError(f"unsupported memory writer operation: {operation}")
        digest, length = content_descriptor(content)
        created_at = self.clock()
        deadline = (
            _time(expires_at, "expires_at")
            if expires_at is not None
            else created_at + ttl
        )
        return self.effect_ledger.begin_intent(
            effect_id,
            kind=operation,
            source_event_id=_text(source_event_id, "source_event_id"),
            idempotency_key=_text(idempotency_key, "idempotency_key"),
            epoch_id=_text(epoch_id, "epoch_id"),
            content_sha256=digest,
            content_length=length,
            expires_at=deadline,
            created_at=created_at,
        )

    def handoff(
        self,
        effect_id: str,
        writer: Any,
        *,
        content: Any = None,
        operation: str | None = None,
    ) -> WriterHandoff:
        with self._claim_lock():
            record = self.effect_ledger.get(effect_id)
            if record is None:
                raise ValueError(f"writer effect does not exist: {effect_id}")
            actual_operation = record.kind if operation is None else operation
            if record.state not in {"intent", "requeued"}:
                return WriterHandoff(actual_operation, effect_id, record)
            if content is None:
                raise ValueError("first writer handoff requires transient content")
            digest, length = content_descriptor(content)
            if digest != record.content_sha256 or length != record.content_length:
                raise ValueError("writer content does not match effect descriptor")
            record = self.effect_ledger.mark_pending(effect_id)
            if record.state != "pending":
                raise StateError("effect ledger did not claim writer intent")
            request = WriterRequest(
                effect_id=record.effect_id,
                operation=record.kind,
                source_event_id=record.source_event_id,
                idempotency_key=record.idempotency_key,
                epoch_id=record.epoch_id,
                content_sha256=record.content_sha256,
                content_length=record.content_length,
                attempt=record.attempt,
                content=content,
            )
        try:
            method = writer if callable(writer) else getattr(writer, "write", None)
            if method is None and not callable(writer):
                method = getattr(writer, "persist", None)
            if not callable(method):
                raise TypeError("writer must be callable or provide write/persist")
            result = method(request)
            if isinstance(result, EffectReceipt):
                try:
                    record = self.effect_ledger.verify(effect_id, result)
                except Exception as exc:  # noqa: BLE001 - receipt mismatch is visible
                    record = self.effect_ledger.fail(
                        effect_id,
                        f"writer receipt mismatch: {type(exc).__name__}",
                        retryable=True,
                    )
                    return WriterHandoff(
                        actual_operation, effect_id, record, type(exc).__name__, request
                    )
            else:
                record = self.effect_ledger.mark_queue_accepted(effect_id)
        except Exception as exc:  # noqa: BLE001 - failure must become visible ledger state
            record = self.effect_ledger.fail(
                effect_id,
                f"writer handoff failed: {type(exc).__name__}",
                retryable=True,
            )
            return WriterHandoff(
                actual_operation, effect_id, record, type(exc).__name__, request
            )
        return WriterHandoff(actual_operation, effect_id, record, request=request)

    def verify(self, effect_id: str, receipt: EffectReceipt) -> WriterHandoff:
        record = self.effect_ledger.verify(effect_id, receipt)
        return WriterHandoff(record.kind, effect_id, record)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project effect state without reconciliation, expiry, or writer calls."""

        effective_now = _observer_validate_context(target_date, now)
        del effective_now
        return _observer_writer_facts(
            self.effect_ledger,
            target_date=target_date,
            now=now,
        )

    def submit(
        self,
        operation: str,
        writer: Any,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> WriterHandoff:
        intent = self.create_intent(
            operation,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            content=content,
            expires_at=expires_at,
            ttl=ttl,
            effect_id=effect_id,
        )
        return self.handoff(
            intent.effect_id, writer, content=content, operation=operation
        )


def _observer_writer_facts(
    effect_ledger: Any,
    *,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Fallback effect projection for a path-backed injected effect port."""

    owner = getattr(effect_ledger, "ledger", None)
    path = getattr(owner, "path", None)
    if path is None:
        return (
            ObservationFact(
                key="memory.writer.adapter",
                code="writer_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("writer",),
            ),
        )
    try:
        raw_rows = _observer_jsonl_rows(path)
        records: list[tuple[str, EffectRecord]] = []
        by_effect: dict[str, list[EffectRecord]] = {}
        idempotency: dict[str, str] = {}
        for row_number, row in enumerate(raw_rows, start=1):
            operation = row.get("operation")
            if operation not in {
                "begin_intent",
                "mark_pending",
                "mark_queue_accepted",
                "verify",
                "fail",
                "expire",
                "requeue",
            }:
                raise StateError(f"writer row {row_number} has an invalid operation")
            record = EffectRecord.from_dict(row)
            records.append((operation, record))
            previous = by_effect.get(record.effect_id, [])
            if not previous:
                if operation != "begin_intent" or record.state != "intent":
                    raise StateError("writer ledger starts after intent")
                if record.attempt != 1:
                    raise StateError("writer ledger has an invalid initial attempt")
            else:
                prior = previous[-1]
                if (
                    prior.effect_id != record.effect_id
                    or prior.kind != record.kind
                    or prior.source_event_id != record.source_event_id
                    or prior.idempotency_key != record.idempotency_key
                    or prior.epoch_id != record.epoch_id
                    or prior.created_at != record.created_at
                    or prior.content_sha256 != record.content_sha256
                    or prior.content_length != record.content_length
                ):
                    raise StateError("writer ledger changes immutable identity")
                if not _valid_transition(prior, record, operation):
                    raise StateError("writer ledger contains an out-of-order event")
            previous_effect = idempotency.get(record.idempotency_key)
            if previous_effect is not None and previous_effect != record.effect_id:
                raise StateError("writer ledger reuses an idempotency key")
            idempotency[record.idempotency_key] = record.effect_id
            by_effect.setdefault(record.effect_id, []).append(record)
    except Exception as exc:  # noqa: BLE001 - content-free fail-closed status
        del exc
        return (_observer_integrity_fact("writer", target_date=target_date),)
    if not records:
        return ()

    facts: list[ObservationFact] = []
    for history in by_effect.values():
        current = history[-1]
        projected_state = current.state
        if (
            current.state in {"pending", "executed_unverified"}
            and current.expires_at < now
        ):
            # Projection only: never call EffectLedger.expire().
            projected_state = "expired"
        refs = [
            f"effect:{current.effect_id}",
            f"source:{current.source_event_id}",
            f"sha256:{current.content_sha256}",
        ]
        counts = {
            "effects": 1,
            "attempt": current.attempt,
            "content_length": current.content_length,
            f"state_{projected_state}": 1,
        }
        if current.receipt is not None:
            refs.append(f"receipt:{current.receipt.receipt_id}")
        if current.state == "verified":
            prior_states = {record.state for record in history[:-1]}
            recovery = None
            fact_state = "neutral"
            if prior_states & _WRITER_CURRENT_STATES:
                receipt = current.receipt
                if receipt is not None:
                    recovery = RecoveryEvidence(
                        f"receipt:{receipt.receipt_id}",
                        "effect_verified",
                        receipt.observed_at,
                    )
                    fact_state = "recovered_history"
            facts.append(
                ObservationFact(
                    key=f"memory.writer.effect:{current.effect_id}",
                    code="effect_verified",
                    state=fact_state,
                    target_date=target_date,
                    event_time=(current.observed_at or current.created_at),
                    refs=tuple(refs),
                    counts=counts,
                    recovery=recovery,
                )
            )
            continue
        if projected_state in _WRITER_CURRENT_STATES:
            fact_state = "current"
        else:
            fact_state = "neutral"
        facts.append(
            ObservationFact(
                key=f"memory.writer.effect:{current.effect_id}",
                code=f"effect_{projected_state}",
                state=fact_state,
                target_date=target_date,
                event_time=(
                    current.expires_at
                    if projected_state == "expired"
                    else current.created_at
                ),
                refs=tuple(refs),
                counts=counts,
            )
        )
    return _observer_merge_facts(facts)
