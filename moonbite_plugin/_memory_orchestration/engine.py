"""Host-neutral memory orchestration engine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .contracts import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureRecord,
    ExposedSource,
    MissingEvidenceError,
    SourceCandidate,
    SourceMaterial,
    _MAX_SOURCE_BYTES,
    _time,
)
from .exposure import (
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    _observer_exposure_facts,
)
from .maintenance import MemoryMaintenanceFacade
from .observation import (
    observer_merge_facts as _observer_merge_facts,
    observer_validate_context as _observer_validate_context,
)
from .sources import SourceRegistry
from .writer import WriterCoordinator
from ..effects import EffectLedger
from ..observer import ObservationFact
from ..runtime_core import utc_now


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
