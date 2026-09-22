"""Host-neutral memory orchestration primitives for MB-50.

This module is deliberately a port layer.  A retriever can return opaque
references and bounded metadata, while an opener is called only after a
reference has been selected.  The durable part of the module is an
append-only exposure ledger: it records the reference and evidence
descriptor, never source material.  Memory records and writer effects are
delegated to their injected stores.
"""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from ._memory_orchestration.contracts import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureRecord,
    ExposedSource,
    MissingEvidenceError,
    ORCHESTRATION_SCHEMA,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRetriever,
    _MAX_SOURCE_BYTES,
    _text,
    _time,
    content_descriptor,
)
from ._memory_orchestration.exposure import (
    EXPOSURE_EVENTS,
    EXPOSURE_STATES,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    _observer_exposure_facts,
)
from ._memory_orchestration.maintenance import (
    MaintenanceApprovalAdapter,
    MemoryMaintenanceFacade,
)
from ._memory_orchestration.observation import (
    observer_integrity_fact as _observer_integrity_fact,
    observer_jsonl_rows as _observer_jsonl_rows,
    observer_merge_facts as _observer_merge_facts,
    observer_validate_context as _observer_validate_context,
)
from ._memory_orchestration.sources import SourceRegistry
from .effects import EffectLedger, EffectReceipt, EffectRecord, _valid_transition
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import StateError, file_lock, utc_now


WRITER_OPERATIONS = frozenset(
    {"turn_persistence", "flush", "diary", "consolidation", "maintenance"}
)

_MAX_REASON_BYTES = 4 * 1024
_WRITER_CURRENT_STATES = frozenset(
    {"pending", "executed_unverified", "expired", "failed", "requeued"}
)

for _public_contract in (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    ExposureRecord,
    ExposedSource,
    MaintenanceApprovalAdapter,
    MemoryMaintenanceFacade,
    MissingEvidenceError,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRegistry,
    SourceRetriever,
    content_descriptor,
):
    _public_contract.__module__ = __name__
del _public_contract


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


class MemoryOrchestrator:
    """Facade joining source ports, exposure ledger, injected stores, and effects."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        memory_store: Any = None,
        session_store: Any = None,
        effect_ledger: EffectLedger | None = None,
        retriever: Any = None,
        opener: Any = None,
        source_registry: SourceRegistry | None = None,
        exposure_ledger: ExposureLedger | None = None,
        policy: ExposurePolicy | None = None,
        continuity_policy: Callable[[str, str], bool] | None = None,
        approval_adapter: Any = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.clock = clock
        self.memory_store = memory_store
        self.session_store = session_store
        self.effect_ledger = effect_ledger
        if root is None and memory_store is not None:
            cards = getattr(memory_store, "cards", None)
            path = getattr(cards, "path", None)
            if path is not None:
                root = Path(path).parent
        if exposure_ledger is not None:
            self.exposures = exposure_ledger
        else:
            if root is None:
                raise ValueError("root or exposure_ledger is required")
            self.exposures = ExposureLedger(root, clock=clock)
        self.policy = policy or ExposurePolicy()
        self.continuity_policy = continuity_policy
        self.sources = source_registry or SourceRegistry(retriever, opener)
        self.writer = (
            None
            if effect_ledger is None
            else WriterCoordinator(effect_ledger, clock=clock)
        )
        self.maintenance = (
            None
            if memory_store is None
            else MemoryMaintenanceFacade(
                memory_store,
                approval_adapter=approval_adapter,
                root=root,
                clock=clock,
            )
        )

    @staticmethod
    def _context(
        value: Any, *, observed_at: datetime | None = None, turn_index: int = 0
    ) -> ExposureContext:
        if isinstance(value, ExposureContext):
            return value
        return ExposureContext.from_session(
            value, observed_at=observed_at, turn_index=turn_index
        )

    def retrieve(
        self,
        query: str,
        *,
        context: ExposureContext | Any,
        limit: int | None = None,
    ) -> tuple[SourceCandidate, ...]:
        self._context(context)
        budget = (
            self.policy.result_budget
            if limit is None
            else min(limit, self.policy.result_budget)
        )
        return self.sources.retrieve(query, limit=budget)

    def plan(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        now: datetime | None = None,
    ) -> ExposurePlan:
        actual_context = self._context(context)
        return self.policy.choose(
            candidates,
            context=actual_context,
            ledger=self.exposures,
            now=self.clock() if now is None else now,
            continuity_policy=self.continuity_policy,
            first_turn=first_turn,
        )

    def expose_candidates(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        now: datetime | None = None,
    ) -> tuple[ExposedSource, ...]:
        actual_context = self._context(context)
        plan = self.plan(
            candidates, context=actual_context, first_turn=first_turn, now=now
        )
        results: list[ExposedSource] = []
        for candidate in plan.candidates:
            selected = self.exposures.record_selected(
                candidate, context=actual_context, now=now
            )
            material = self.open_selected(
                selected.exposure_id, context=actual_context, now=now
            )
            exposed = self.exposures.record_exposed(
                selected.exposure_id,
                exposure_cap=self.policy.max_per_session,
                now=now,
            )
            results.append(ExposedSource(exposed, material))
        return tuple(results)

    def expose_query(
        self,
        query: str,
        *,
        context: ExposureContext | Any,
        first_turn: bool | None = None,
        limit: int | None = None,
        now: datetime | None = None,
    ) -> tuple[ExposedSource, ...]:
        candidates = self.retrieve(query, context=context, limit=limit)
        return self.expose_candidates(
            candidates, context=context, first_turn=first_turn, now=now
        )

    def open_selected(
        self,
        exposure_id: str,
        *,
        candidate: SourceCandidate | None = None,
        opener: Any = None,
        max_bytes: int = _MAX_SOURCE_BYTES,
        context: ExposureContext | Any | None = None,
        now: datetime | None = None,
    ) -> SourceMaterial:
        state = self.exposures.get(exposure_id)
        if state is None:
            raise MissingEvidenceError(f"exposure does not exist: {exposure_id}")
        if state.state not in {"selected", "opened", "exposed", "used", "consumed"}:
            raise ValueError(
                "exact open requires a selected or previously opened exposure"
            )
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_context = (
            self._context(context)
            if context is not None
            else ExposureContext(
                state.session_id,
                state.lifecycle_id,
                state.turn_id,
                state.context_source_kind,
                effective_now,
            )
        )
        if (
            actual_context.session_id != state.session_id
            or actual_context.lifecycle_id != state.lifecycle_id
            or actual_context.turn_id != state.turn_id
            or actual_context.source_kind != state.context_source_kind
        ):
            raise ExposureConflictError("exact-open context does not match selection")
        source_candidate = candidate or SourceCandidate(
            source_ref=state.source_ref,
            source_class=state.source_class,
            source_event_time=state.source_event_time,
            created_at=state.source_created_at,
            expires_at=state.source_expires_at,
            content_sha256=state.content_sha256,
            content_length=state.content_length,
        )
        if ExposureLedger._candidate_identity(source_candidate) != (
            state.source_ref,
            state.source_class,
            state.source_event_time,
            state.source_created_at,
            state.source_expires_at,
        ):
            raise ExposureConflictError("exact-open candidate does not match exposure")
        if state.content_sha256 is not None and (
            source_candidate.content_sha256 != state.content_sha256
            or source_candidate.content_length != state.content_length
        ):
            raise ExposureConflictError(
                "exact-open content descriptor does not match exposure"
            )
        try:
            if (
                source_candidate.expires_at is not None
                and effective_now >= source_candidate.expires_at
            ):
                raise ExpiredEvidenceError(
                    f"source evidence has expired: {state.source_ref}"
                )
            registry = self.sources if opener is None else SourceRegistry(opener=opener)
            material = registry.exact_open(source_candidate, max_bytes=max_bytes)
            if material.expires_at is not None and effective_now >= material.expires_at:
                raise ExpiredEvidenceError(
                    f"opened source evidence has expired: {state.source_ref}"
                )
            historical = (
                material.source_event_time.date() < actual_context.observed_at.date()
            )
            material = replace(
                material,
                framing="historical" if historical else "current",
                framing_date=material.source_event_time.date(),
            )
            self.exposures.record_opened(exposure_id, material, now=effective_now)
            return material
        except (MissingEvidenceError, ExpiredEvidenceError, ValueError) as exc:
            if state.state == "selected":
                self.exposures.record_open_failed(
                    exposure_id,
                    type(exc).__name__.lower(),
                    now=effective_now,
                )
            raise

    def mark_used(self, *args: Any, **kwargs: Any) -> ExposureRecord:
        return self.exposures.record_used(*args, **kwargs)

    def mark_consumed(self, *args: Any, **kwargs: Any) -> ExposureRecord:
        return self.exposures.record_consumed(*args, **kwargs)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Aggregate owner facts exactly once, with no orchestration side effects."""

        effective_now = _observer_validate_context(target_date, now)
        facts: list[ObservationFact] = []
        if isinstance(self.exposures, ExposureLedger):
            facts.extend(
                _observer_exposure_facts(
                    self.exposures,
                    target_date=target_date,
                    now=effective_now,
                    policy=self.policy,
                )
            )
        else:
            exposure_adapter = getattr(self.exposures, "observer_status", None)
            if callable(exposure_adapter):
                result = exposure_adapter(target_date=target_date, now=now)
                if isinstance(result, (str, bytes, bytearray, Mapping)):
                    raise TypeError("exposure observer result must be an iterable")
                values = tuple(result)
                if any(not isinstance(item, ObservationFact) for item in values):
                    raise TypeError(
                        "exposure observer result contains a malformed fact"
                    )
                facts.extend(values)
            else:
                facts.extend(
                    _observer_exposure_facts(
                        self.exposures,
                        target_date=target_date,
                        now=effective_now,
                        policy=self.policy,
                    )
                )
        if self.writer is not None:
            facts.extend(self.writer.observer_status(target_date=target_date, now=now))
        if self.maintenance is not None:
            facts.extend(
                self.maintenance.observer_status(
                    target_date=target_date,
                    now=now,
                )
            )
        if self.memory_store is not None:
            adapter = getattr(self.memory_store, "observer_status", None)
            if callable(adapter):
                result = adapter(target_date=target_date, now=now)
                if isinstance(result, (str, bytes, bytearray, Mapping)):
                    raise TypeError("memory observer result must be an iterable")
                values = tuple(result)
                if any(not isinstance(item, ObservationFact) for item in values):
                    raise TypeError("memory observer result contains a malformed fact")
                facts.extend(values)
            else:
                facts.append(
                    ObservationFact(
                        key="memory.store.adapter",
                        code="memory_adapter_unavailable",
                        state="neutral",
                        target_date=target_date,
                        refs=("memory_store",),
                    )
                )
        return _observer_merge_facts(facts)


__all__ = [
    "EXPOSURE_EVENTS",
    "EXPOSURE_STATES",
    "ORCHESTRATION_SCHEMA",
    "WRITER_OPERATIONS",
    "ExpiredEvidenceError",
    "ExposureConflictError",
    "ExposureContext",
    "ExposureLedger",
    "ExposurePlan",
    "ExposurePolicy",
    "ExposureRecord",
    "ExposedSource",
    "MemoryMaintenanceFacade",
    "MemoryOrchestrator",
    "MaintenanceApprovalAdapter",
    "MissingEvidenceError",
    "OrchestrationError",
    "PolicyDeniedError",
    "ReplyUseEvidence",
    "SourceCandidate",
    "SourceMaterial",
    "SourceOpener",
    "SourceRegistry",
    "SourceRetriever",
    "WriterCoordinator",
    "WriterHandoff",
    "WriterRequest",
    "content_descriptor",
]
