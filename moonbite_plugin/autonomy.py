"""Single-choice, evidence-gated autonomy runtime.

Autonomy is intentionally conservative.  A provider invocation is recorded as
an effect intent before the adapter is called.  An ordinary return value means
only that the adapter returned; it is never proof that an external effect was
seen.  Only a strict :class:`~moonbite_plugin.effects.EffectReceipt` matching
the intent can produce a completed result.
"""

from __future__ import annotations

import hashlib as hashlib
import math
import random
import threading
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Protocol
from urllib.parse import quote, unquote

from ._autonomy.admission import (
    audit_history as _admission_audit_history,
    eligible as _admission_eligible,
    eligible_reason as _admission_eligible_reason,
    eligible_with_reasons as _admission_eligible_with_reasons,
    provider_history as _admission_provider_history,
    validate_provider_settings as _admission_validate_provider_settings,
    weighted_selection as _admission_weighted_selection,
)
from ._autonomy.execution import run_once_locked as _execution_run_once_locked
from ._autonomy.observation import (
    _autonomy_fact,
    _autonomy_integrity,
    _autonomy_observer_reason,
    _autonomy_text,
    engine_observer_status as _engine_observer_status,
    _read_audit_rows_lock_free as _read_autonomy_audit_rows_lock_free,
)
from ._autonomy.outcomes import (
    bound_control_id as _outcomes_bound_control_id,
    consume_verified as _outcomes_consume_verified,
    fail as _outcomes_fail,
    finish as _outcomes_finish,
    reconcile as _outcomes_reconcile,
    settle_expired_unverified as _outcomes_settle_expired_unverified,
)
from ._autonomy.recovery import (
    audit_identity_for_record as _recovery_audit_identity_for_record,
    canonical_terminal as _recovery_canonical_terminal,
    effect_identity as _recovery_effect_identity,
    existing_result as _recovery_existing_result,
    existing_terminal_result as _recovery_existing_terminal_result,
    find_by_idempotency as _recovery_find_by_idempotency,
    find_by_occurrence as _recovery_find_by_occurrence,
    find_implicit_occurrence as _recovery_find_implicit_occurrence,
    identity_overrides as _recovery_identity_overrides,
    public_epoch_from_record as _recovery_public_epoch_from_record,
    reason_code as _recovery_reason_code,
    receipt_from_output as _recovery_receipt_from_output,
    record_evidence as _recovery_record_evidence,
    record_provider as _recovery_record_provider,
    record_state as _recovery_record_state,
    record_value as _recovery_record_value,
    terminal_identity as _recovery_terminal_identity,
    validate_judge_decision as _recovery_validate_judge_decision,
)
from .control import ControlStore, GateResult, evaluate_gate
from .effects import (
    EffectLedger,
    EffectReceipt,
    EffectRecord,
    _read_effect_history_lock_free,
)
from .observer import ObservationFact, RecoveryEvidence as RecoveryEvidence
from .runtime_core import (
    EventBus,
    FileRuntimeLocks,
    RuntimeLocks,
    ensure_bounded_text,
    new_id,
    parse_time as parse_time,
    utc_now,
)

AUTONOMY_EFFECT_KIND = "autonomy_completion"
_HASH_LENGTH = 64
_STATUSES = frozenset(
    {"completed", "executed_unverified", "failed", "skipped", "awaiting_reconciliation"}
)
_COST_UNITS = {"low": 1, "medium": 2, "high": 3}
_DEFAULT_EFFECT_TTL = timedelta(hours=1)
_DEFAULT_EVIDENCE_CONTRACT = "effect_receipt"
_AUTONOMY_EFFECT_ID_PREFIX = "autonomy:"


def _bounded(value: Any, label: str, *, max_bytes: int = 512) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    ensure_bounded_text(value, label, max_bytes=max_bytes)
    return value


def _optional_gate_set(value: Any, label: str) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        values = (value,)
    else:
        try:
            values = tuple(value)
        except TypeError as exc:
            raise ValueError(f"{label} must be a bounded string collection") from exc
    if len(values) > 32:
        raise ValueError(f"{label} has too many entries")
    normalized: list[str] = []
    for item in values:
        normalized.append(_bounded(item, label, max_bytes=128))
    return frozenset(normalized)


def _positive_limit(value: Any, label: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0 or value > 100_000:
        raise ValueError(f"{label} must be a positive bounded integer")
    return value


def _nonnegative_number(value: Any, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a non-negative number")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0 or numeric > 31 * 24 * 60 * 60:
        raise ValueError(f"{label} must be bounded")
    return numeric


def _hash_text(value: Any) -> str:
    if type(value) is not str or len(value) != _HASH_LENGTH:
        raise ValueError("content_sha256 must be a 64-character lowercase hex digest")
    if any(character not in "0123456789abcdef" for character in value):
        raise ValueError("content_sha256 must be a 64-character lowercase hex digest")
    return value


def _new_autonomy_effect_id(provider: str) -> str:
    """Keep the canonical provider recoverable from the durable intent."""

    encoded = quote(_bounded(provider, "provider", max_bytes=128), safe="")
    return f"{_AUTONOMY_EFFECT_ID_PREFIX}{encoded}:{new_id('effect')}"


def _provider_from_effect_id(value: Any) -> str | None:
    if not isinstance(value, str) or not value.startswith(_AUTONOMY_EFFECT_ID_PREFIX):
        return None
    try:
        encoded, token = value[len(_AUTONOMY_EFFECT_ID_PREFIX) :].split(":", 1)
    except ValueError:
        return None
    if not encoded or not token.startswith("effect_"):
        return None
    provider = unquote(encoded)
    if quote(provider, safe="") != encoded:
        return None
    try:
        return _bounded(provider, "provider", max_bytes=128)
    except (TypeError, ValueError):
        return None


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


@dataclass(frozen=True)
class AutonomyContext:
    now: datetime
    facts: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AutonomyExecutionRequest:
    """The complete, content-free identity passed to an adapter runner."""

    provider: str
    effect_id: str
    idempotency_key: str
    source_event_id: str
    epoch_id: str
    content_sha256: str
    content_length: int
    attempt: int
    context: AutonomyContext

    def __post_init__(self) -> None:
        _bounded(self.provider, "provider")
        _bounded(self.effect_id, "effect_id")
        _bounded(self.idempotency_key, "idempotency_key")
        _bounded(self.source_event_id, "source_event_id")
        _bounded(self.epoch_id, "epoch_id")
        _hash_text(self.content_sha256)
        _positive_int(self.content_length, "content_length")
        _positive_int(self.attempt, "attempt")
        if not isinstance(self.context, AutonomyContext):
            raise TypeError("context must be an AutonomyContext")

    # Compatibility properties let old providers read the context while the
    # adapter-facing callable still receives the canonical request object.
    @property
    def now(self) -> datetime:
        return self.context.now

    @property
    def facts(self) -> Mapping[str, Any]:
        return self.context.facts


@dataclass(frozen=True)
class ActivityResult:
    status: str
    provider: str | None
    reason: str
    output: Any = None
    run_id: str | None = None
    effect_id: str | None = None
    evidence: Mapping[str, Any] | None = None
    source_event_id: str | None = None
    idempotency_key: str | None = None
    effect_record: EffectRecord | None = None
    canonical_event_id: str | None = None
    audit_status: str = "recorded"
    audit_error: str | None = None
    epoch_id: str | None = None

    def __post_init__(self) -> None:
        if self.status not in _STATUSES:
            raise ValueError(f"invalid activity terminal: {self.status}")
        if self.epoch_id is not None:
            _bounded(self.epoch_id, "epoch_id")

    @property
    def verified(self) -> bool:
        return self.status == "completed"

    @property
    def degraded(self) -> bool:
        """Whether the state truth is available but audit projection degraded."""

        return self.audit_status != "recorded"


class Eligibility(Protocol):
    def __call__(self, context: AutonomyContext) -> bool: ...


class Runner(Protocol):
    def __call__(self, request: AutonomyExecutionRequest) -> Any: ...


def _always_eligible(_context: AutonomyContext) -> bool:
    return True


@dataclass(frozen=True)
class ActivityProvider:
    """A bounded provider descriptor with an injectable adapter runner.

    The first three fields preserve the original minimal registration API.
    Descriptor limits are generic and contain no transport, endpoint, or
    credential data.
    """

    name: str
    run: Runner
    eligible: Eligibility = _always_eligible
    # Capabilities are descriptive metadata only; eligibility remains owned by
    # the explicit provider settings and ``eligible`` callback below.
    capabilities: frozenset[str] = frozenset()
    cost_class: str = "low"
    cost_budget: int | None = None
    allowed_sources: frozenset[str] = frozenset()
    allowed_channels: frozenset[str] = frozenset()
    cooldown: timedelta | int | float | None = None
    daily_limit: int | None = None
    repeat_limit: int | None = None
    evidence_contract: str = _DEFAULT_EVIDENCE_CONTRACT

    def __post_init__(self) -> None:
        _bounded(self.name, "provider name", max_bytes=128)
        if not callable(self.run) or not callable(self.eligible):
            raise TypeError("provider run and eligible must be callable")
        object.__setattr__(
            self, "capabilities", _optional_gate_set(self.capabilities, "capabilities")
        )
        if type(self.cost_class) is not str or not self.cost_class.strip():
            raise ValueError("cost_class must be bounded text")
        ensure_bounded_text(self.cost_class, "cost_class", max_bytes=64)
        if self.cost_class not in _COST_UNITS:
            raise ValueError("cost_class must be low, medium, or high")
        if self.cost_budget is not None:
            _positive_limit(self.cost_budget, "cost_budget")
        object.__setattr__(
            self,
            "allowed_sources",
            _optional_gate_set(self.allowed_sources, "allowed_sources"),
        )
        object.__setattr__(
            self,
            "allowed_channels",
            _optional_gate_set(self.allowed_channels, "allowed_channels"),
        )
        if isinstance(self.cooldown, timedelta):
            if self.cooldown.total_seconds() < 0 or self.cooldown > timedelta(days=31):
                raise ValueError("cooldown must be bounded and non-negative")
        else:
            _nonnegative_number(self.cooldown, "cooldown")
        _positive_limit(self.daily_limit, "daily_limit")
        _positive_limit(self.repeat_limit, "repeat_limit")
        _bounded(self.evidence_contract, "evidence_contract", max_bytes=64)
        if self.evidence_contract != _DEFAULT_EVIDENCE_CONTRACT:
            raise ValueError(
                "evidence_contract must be the public effect_receipt contract"
            )


@dataclass(frozen=True)
class AutonomyDecision:
    allowed: bool
    reason: str
    provider_weights: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.allowed) is not bool:
            raise TypeError("judge allowed must be a bool")
        _bounded(self.reason, "judge reason", max_bytes=128)
        if not isinstance(self.provider_weights, Mapping):
            raise TypeError("judge provider_weights must be a mapping")
        if len(self.provider_weights) > 64:
            raise ValueError("judge provider_weights is too large")
        normalized: dict[str, int] = {}
        for name, weight in self.provider_weights.items():
            provider = _bounded(name, "provider name", max_bytes=128)
            if type(weight) is not int or not 0 <= weight <= 100:
                raise ValueError("judge provider weight must be between 0 and 100")
            normalized[provider] = weight
        object.__setattr__(self, "provider_weights", MappingProxyType(normalized))


class AutonomyJudge(Protocol):
    def decide(self, context: AutonomyContext) -> AutonomyDecision: ...


class ProviderEligibilityError(RuntimeError):
    def __init__(self, provider: str, cause: Exception):
        self.provider = provider
        self.cause = cause
        super().__init__(f"{provider}: {type(cause).__name__}")


class _ProviderSettingsError(ValueError):
    def __init__(self, provider: str | None, field: str):
        self.provider = provider
        self.field = field
        super().__init__(f"invalid provider setting: {field}")


class AllowAutonomyJudge:
    def decide(self, context: AutonomyContext) -> AutonomyDecision:
        return AutonomyDecision(True, "rule_allow")


class DenyAutonomyJudge:
    def __init__(self, reason: str = "judge_adapter_not_configured"):
        self.reason = reason

    def decide(self, context: AutonomyContext) -> AutonomyDecision:
        return AutonomyDecision(False, self.reason)


class ProviderRegistry:
    def __init__(self):
        self._providers: dict[str, ActivityProvider] = {}

    def register(self, provider: ActivityProvider) -> None:
        if not isinstance(provider, ActivityProvider):
            raise TypeError("autonomy provider must be an ActivityProvider")
        if provider.name in self._providers:
            raise ValueError(f"duplicate or empty autonomy provider: {provider.name!r}")
        self._providers[provider.name] = provider

    def get(self, name: str) -> ActivityProvider | None:
        return self._providers.get(name)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._providers))


class _ThreadRuntimeLocks:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    @contextmanager
    def try_exclusive(self, name: str):
        del name
        acquired = self._lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                self._lock.release()

    @contextmanager
    def exclusive(self, name: str):
        del name
        with self._lock:
            yield


def _state_root(controls: Any):
    try:
        ledger = controls.ledger
        path = getattr(ledger, "path", None)
    except Exception:
        return None
    return getattr(path, "parent", None)


def _callable_port(value: Any, name: str) -> None:
    if not callable(getattr(value, name, None)):
        raise TypeError(f"effect ledger port missing callable {name}")


def _read_audit_rows_lock_free(
    path: Path,
) -> tuple[tuple[dict[str, Any], ...], str | None]:
    """Read only bounded autonomy audit telemetry without retaining payloads."""

    return _read_autonomy_audit_rows_lock_free(path, statuses=_STATUSES)


class AutonomyEngine:
    def __init__(
        self,
        *,
        bus: EventBus,
        controls: ControlStore,
        registry: ProviderRegistry,
        judge: AutonomyJudge,
        rng: random.Random | None = None,
        clock: Callable[[], datetime] = utc_now,
        locks: RuntimeLocks | None = None,
        effect_ledger: Any | None = None,
    ):
        self.bus = bus
        self.controls = controls
        self.registry = registry
        self.judge = judge
        self.rng = rng or random.Random()
        self.clock = clock
        root = _state_root(controls)
        if effect_ledger is None:
            if root is None:
                raise TypeError("effect_ledger_required_for_pathless_controls")
            self.effect_ledger = EffectLedger(root, clock=clock)
        else:
            self.effect_ledger = effect_ledger
        for required in (
            "begin_intent",
            "mark_pending",
            "mark_queue_accepted",
            "verify",
            "fail",
            "get",
            "find_by_idempotency",
            "records",
        ):
            _callable_port(self.effect_ledger, required)
        if locks is None:
            if root is None:
                self.locks = _ThreadRuntimeLocks()
                self.execution_lock_path = None
            else:
                self.locks = FileRuntimeLocks(root)
                self.execution_lock_path = root / "autonomy_execution.lock"
        else:
            self.locks = locks
            self.execution_lock_path = None

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return provider/effect telemetry without invoking autonomy actors.

        The observer consumes only the effect ledger and the already-written
        audit stream.  It never calls the Judge, a provider runner, a sink, or
        reconciliation, and it never exposes ``ActivityResult.output``,
        context facts, or raw exception/reason text.
        """

        return _engine_observer_status(
            target_date=target_date,
            now=now,
            effect_ledger=self.effect_ledger,
            bus=self.bus,
            controls=self.controls,
            state_root=_state_root,
            record_provider=lambda record: self._record_provider(record),
            effect_kind=AUTONOMY_EFFECT_KIND,
            read_effect_history_lock_free=_read_effect_history_lock_free,
            read_audit_rows_lock_free=_read_audit_rows_lock_free,
            observer_reason=_autonomy_observer_reason,
            text=_autonomy_text,
            fact=_autonomy_fact,
            integrity_fact=_autonomy_integrity,
        )

    @classmethod
    def _public_epoch_from_record(cls, record: Any) -> Any:
        """Hide the date-derived ledger epoch from the public identity."""

        return _recovery_public_epoch_from_record(
            record,
            record_value=lambda selected, key, default=None: cls._record_value(
                selected, key, default
            ),
        )

    @classmethod
    def _terminal_identity(cls, result: ActivityResult) -> tuple[str, str | None]:
        """Resolve the public terminal identity without losing effect evidence."""

        return _recovery_terminal_identity(
            result,
            record_value=lambda record, key, default=None: cls._record_value(
                record, key, default
            ),
            public_epoch_from_record=lambda record: cls._public_epoch_from_record(
                record
            ),
            new_id_fn=new_id,
        )

    def _finish(
        self,
        result: ActivityResult,
        gate: GateResult,
        *,
        record_terminal: bool = True,
    ) -> ActivityResult:
        return _outcomes_finish(
            result,
            gate,
            record_terminal=record_terminal,
            terminal_identity=lambda selected: self._terminal_identity(selected),
            canonical_terminal=lambda selected: self._canonical_terminal(selected),
            reason_code=lambda reason: self._reason_code(reason),
            bus=self.bus,
        )

    @staticmethod
    def _canonical_terminal(result: ActivityResult) -> str | None:
        return _recovery_canonical_terminal(
            result,
            reason_code=AutonomyEngine._reason_code,
        )

    def _existing_terminal_result(
        self,
        occurrence_id: str,
        *,
        epoch_id: str | None = None,
        ledger_epoch_id: str,
    ) -> ActivityResult | None:
        return _recovery_existing_terminal_result(
            occurrence_id,
            epoch_id=epoch_id,
            ledger_epoch_id=ledger_epoch_id,
            bus=self.bus,
            effect_ledger=self.effect_ledger,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_state=lambda record: self._record_state(record),
            reason_code=lambda reason: self._reason_code(reason),
            result_type=ActivityResult,
            effect_kind=AUTONOMY_EFFECT_KIND,
        )

    @staticmethod
    def _reason_code(reason: Any) -> str:
        return _recovery_reason_code(reason)

    @staticmethod
    def _record_value(record: Any, key: str, default: Any = None) -> Any:
        return _recovery_record_value(record, key, default)

    @staticmethod
    def _identity_overrides(
        facts: Mapping[str, Any],
    ) -> tuple[str | None, str | None, str | None]:
        return _recovery_identity_overrides(facts, bounded=_bounded)

    @staticmethod
    def _validate_judge_decision(value: Any) -> AutonomyDecision | None:
        return _recovery_validate_judge_decision(
            value,
            decision_type=AutonomyDecision,
            bounded=_bounded,
        )

    def _audit_history(self) -> list[dict[str, Any]]:
        """Return EventBus telemetry with durable effect fallback.

        EventBus remains the primary telemetry owner.  Effect records fill
        only the gap where an audit projection was unavailable, so limits
        cannot be bypassed by a failed audit write.
        """

        return _admission_audit_history(
            bus=self.bus,
            effect_ledger=self.effect_ledger,
            effect_kind=AUTONOMY_EFFECT_KIND,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_provider=lambda record: self._record_provider(record),
            record_state=lambda record: self._record_state(record),
        )

    @staticmethod
    def _record_provider(record: Any) -> str | None:
        return _recovery_record_provider(
            record,
            record_value=AutonomyEngine._record_value,
            provider_from_effect_id=_provider_from_effect_id,
        )

    def _provider_history(
        self, name: str, *, exclude_effect_id: str | None = None
    ) -> list[Any]:
        return _admission_provider_history(
            name,
            exclude_effect_id=exclude_effect_id,
            audit_history=lambda: self._audit_history(),
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_provider=lambda record: self._record_provider(record),
        )

    def _bound_control_id(self, effect_id: str) -> str | None:
        return _outcomes_bound_control_id(
            effect_id,
            audit_history=lambda: self._audit_history(),
        )

    @staticmethod
    def _record_time(record: Any) -> datetime | None:
        value = AutonomyEngine._record_value(record, "created_at")
        if isinstance(value, datetime):
            return value
        return None

    @staticmethod
    def _cooldown_seconds(value: Any) -> float:
        if isinstance(value, timedelta):
            return value.total_seconds()
        if value is None:
            return 0.0
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _setting(
        provider: ActivityProvider, settings: Mapping[str, Any], name: str, default: Any
    ):
        value = settings.get(name, default)
        return default if value is None else value

    @staticmethod
    def _validate_provider_settings(
        settings: Mapping[str, Mapping[str, Any]],
    ) -> None:
        return _admission_validate_provider_settings(
            settings,
            bounded=_bounded,
            optional_gate_set=_optional_gate_set,
            nonnegative_number=_nonnegative_number,
            positive_limit=_positive_limit,
            settings_error=_ProviderSettingsError,
            cost_units=_COST_UNITS,
        )

    def _eligible_reason(
        self,
        provider: ActivityProvider,
        provider_settings: Mapping[str, Any],
        context: AutonomyContext,
        *,
        exclude_effect_id: str | None = None,
    ) -> str | None:
        return _admission_eligible_reason(
            provider,
            provider_settings,
            context,
            exclude_effect_id=exclude_effect_id,
            optional_gate_set=_optional_gate_set,
            setting=lambda selected, values, name, default: self._setting(
                selected, values, name, default
            ),
            provider_history=lambda name, exclude_effect_id=None: (
                self._provider_history(name, exclude_effect_id=exclude_effect_id)
            ),
            eligibility_error=ProviderEligibilityError,
            cooldown_seconds=lambda value: self._cooldown_seconds(value),
            record_time=lambda record: self._record_time(record),
            positive_limit=_positive_limit,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            cost_units=_COST_UNITS,
        )

    def _eligible(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        context: AutonomyContext,
    ) -> list[tuple[str, int]]:
        return _admission_eligible(
            settings,
            context,
            registry_get=lambda name: self.registry.get(name),
            eligible_reason=lambda provider, provider_settings, selected_context: (
                self._eligible_reason(provider, provider_settings, selected_context)
            ),
        )

    def _eligible_with_reasons(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        context: AutonomyContext,
    ) -> tuple[list[tuple[str, int]], dict[str, str]]:
        return _admission_eligible_with_reasons(
            settings,
            context,
            registry_get=lambda name: self.registry.get(name),
            eligible_reason=lambda provider, provider_settings, selected_context: (
                self._eligible_reason(provider, provider_settings, selected_context)
            ),
        )

    @staticmethod
    def _record_state(record: Any) -> str:
        return _recovery_record_state(
            record,
            record_value=AutonomyEngine._record_value,
        )

    @staticmethod
    def _record_evidence(record: Any) -> dict[str, Any]:
        return _recovery_record_evidence(
            record,
            record_state=AutonomyEngine._record_state,
            record_value=AutonomyEngine._record_value,
        )

    def _find_by_idempotency(self, key: str) -> Any | None:
        return _recovery_find_by_idempotency(
            key,
            effect_ledger=self.effect_ledger,
        )

    def _find_by_occurrence(self, source_event_id: str, epoch_id: str) -> Any | None:
        return _recovery_find_by_occurrence(
            source_event_id,
            epoch_id,
            effect_ledger=self.effect_ledger,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            effect_kind=AUTONOMY_EFFECT_KIND,
        )

    def _audit_identity_for_record(self, source_event_id: str, record: Any) -> str:
        """Classify scoped audit proof for one durable autonomy effect."""

        return _recovery_audit_identity_for_record(
            source_event_id,
            record,
            record_value=lambda selected, key, default=None: self._record_value(
                selected, key, default
            ),
            public_epoch_from_record=lambda selected: self._public_epoch_from_record(
                selected
            ),
            bus=self.bus,
        )

    def _find_implicit_occurrence(
        self, source_event_id: str, *, requested_epoch_id: str
    ) -> Any | None:
        """Find one legacy occurrence from a scoped audit identity proof.

        A caller that omits the public epoch cannot infer identity from a
        date-shaped internal epoch or from the generated idempotency key.  An
        ``audit.autonomy`` row emitted by this module, with the same
        occurrence/source and effect, is the proof.  Rows from another audit
        kind/source, rows with contradictory identity fields, and effects with
        no proof are ignored or fail closed.  A non-null public epoch marks an
        exact effect as explicit, so it cannot be replayed by a legacy retry.
        """

        return _recovery_find_implicit_occurrence(
            source_event_id,
            requested_epoch_id=requested_epoch_id,
            effect_ledger=self.effect_ledger,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            audit_identity_for_record=lambda occurrence, record: (
                self._audit_identity_for_record(occurrence, record)
            ),
            effect_kind=AUTONOMY_EFFECT_KIND,
        )

    def _consume_verified(
        self,
        gate: GateResult,
        *,
        effect_id: str | None = None,
        allow_current: bool = False,
        original_control_id: str | None = None,
    ) -> bool:
        return _outcomes_consume_verified(
            gate,
            effect_id=effect_id,
            allow_current=allow_current,
            original_control_id=original_control_id,
            bound_control_id=lambda selected: self._bound_control_id(selected),
            consume_control=lambda control_id: self.controls.consume(control_id),
        )

    def _existing_result(
        self,
        record: Any,
        *,
        provider: str,
        gate: GateResult,
        run_id: str,
        public_epoch_id: str | None = None,
    ) -> ActivityResult | None:
        return _recovery_existing_result(
            record,
            provider=provider,
            gate=gate,
            run_id=run_id,
            public_epoch_id=public_epoch_id,
            record_state=lambda selected: self._record_state(selected),
            record_value=lambda selected, key, default=None: self._record_value(
                selected, key, default
            ),
            record_evidence=lambda selected: self._record_evidence(selected),
            public_epoch_from_record=lambda selected: self._public_epoch_from_record(
                selected
            ),
            consume_verified=lambda selected_gate, **kwargs: self._consume_verified(
                selected_gate, **kwargs
            ),
            finish=lambda result, selected_gate, **kwargs: self._finish(
                result, selected_gate, **kwargs
            ),
            result_type=ActivityResult,
            effect_record_type=EffectRecord,
        )

    @staticmethod
    def _effect_identity(
        provider: str,
        source_event_id: str,
        epoch_id: str,
    ) -> tuple[str, str, int]:
        return _recovery_effect_identity(provider, source_event_id, epoch_id)

    @staticmethod
    def _weighted_selection(
        candidates: list[tuple[str, int]], occurrence_identity: str
    ) -> str:
        """Choose reproducibly from bounded weights for one occurrence."""

        return _admission_weighted_selection(candidates, occurrence_identity)

    @staticmethod
    def _receipt_from_output(output: Any) -> tuple[EffectReceipt | None, str | None]:
        return _recovery_receipt_from_output(
            output,
            receipt_type=EffectReceipt,
        )

    def reconcile(
        self,
        effect_id: str,
        receipt: EffectReceipt,
        *,
        control_id: str | None = None,
    ) -> ActivityResult:
        """Write explicit host evidence for an existing autonomy effect.

        Reconciliation never invokes a provider.  A mismatched receipt is
        fail-closed and cannot be used to consume a play-next control.
        """

        return _outcomes_reconcile(
            effect_id,
            receipt,
            control_id=control_id,
            receipt_type=EffectReceipt,
            effect_record_type=EffectRecord,
            effect_kind=AUTONOMY_EFFECT_KIND,
            effect_ledger=self.effect_ledger,
            resolve_control=lambda name: self.controls.resolve(name),
            evaluate_gate_fn=evaluate_gate,
            clock=self.clock,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_provider=lambda record: self._record_provider(record),
            record_state=lambda record: self._record_state(record),
            record_evidence=lambda record: self._record_evidence(record),
            consume_verified=lambda gate, **kwargs: self._consume_verified(
                gate, **kwargs
            ),
            finish=lambda result, gate, **kwargs: self._finish(result, gate, **kwargs),
            result_type=ActivityResult,
        )

    def fail(self, effect_id: str, reason: str) -> ActivityResult:
        """Settle an asynchronous provider with an explicit host failure."""

        return _outcomes_fail(
            effect_id,
            reason,
            bounded=_bounded,
            effect_record_type=EffectRecord,
            effect_kind=AUTONOMY_EFFECT_KIND,
            effect_ledger=self.effect_ledger,
            resolve_control=lambda name: self.controls.resolve(name),
            evaluate_gate_fn=evaluate_gate,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_provider=lambda record: self._record_provider(record),
            record_state=lambda record: self._record_state(record),
            record_evidence=lambda record: self._record_evidence(record),
            existing_result=lambda record, **kwargs: self._existing_result(
                record, **kwargs
            ),
            finish=lambda result, gate, **kwargs: self._finish(result, gate, **kwargs),
            result_type=ActivityResult,
        )

    def _settle_expired_unverified(
        self,
        *,
        now: datetime,
        gate: GateResult,
    ) -> None:
        """Fail old unverified autonomy effects without replaying providers."""

        return _outcomes_settle_expired_unverified(
            now=now,
            gate=gate,
            effect_record_type=EffectRecord,
            effect_kind=AUTONOMY_EFFECT_KIND,
            effect_ledger=self.effect_ledger,
            record_value=lambda record, key, default=None: self._record_value(
                record, key, default
            ),
            record_provider=lambda record: self._record_provider(record),
            record_state=lambda record: self._record_state(record),
            record_evidence=lambda record: self._record_evidence(record),
            finish=lambda result, selected_gate, **kwargs: self._finish(
                result, selected_gate, **kwargs
            ),
            result_type=ActivityResult,
        )

    def run_once(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        *,
        facts: Mapping[str, Any] | None = None,
    ) -> ActivityResult:
        with self.locks.try_exclusive("autonomy_execution") as acquired:
            if not acquired:
                gate = GateResult(
                    False, "execution_lock", "execution_in_progress", None
                )
                run_id = new_id("autonomy_run")
                source_event_id: str | None = None
                epoch_id: str | None = None
                if isinstance(facts, Mapping):
                    try:
                        source_event_id, epoch_id, _ = self._identity_overrides(facts)
                    except ValueError as exc:
                        # A lock race must not silently discard malformed
                        # occurrence identity.  Report the same fail-closed
                        # validation error as the owner path and use this
                        # invocation's id only as a safe audit fallback.
                        return self._finish(
                            ActivityResult(
                                "failed",
                                None,
                                str(exc),
                                run_id=run_id,
                                source_event_id=run_id,
                                canonical_event_id=run_id,
                            ),
                            gate,
                        )
                source_event_id = source_event_id or run_id
                return self._finish(
                    ActivityResult(
                        "skipped",
                        None,
                        "execution_in_progress",
                        run_id=run_id,
                        source_event_id=source_event_id,
                        canonical_event_id=source_event_id,
                        epoch_id=epoch_id,
                    ),
                    gate,
                )
            return self._run_once_locked(settings, facts=facts)

    def _run_once_locked(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        *,
        facts: Mapping[str, Any] | None = None,
    ) -> ActivityResult:
        return _execution_run_once_locked(
            self,
            settings,
            facts=facts,
            result_type=ActivityResult,
            context_type=AutonomyContext,
            request_type=AutonomyExecutionRequest,
            effect_record_type=EffectRecord,
            effect_kind=AUTONOMY_EFFECT_KIND,
            provider_eligibility_error_type=ProviderEligibilityError,
            provider_settings_error_type=_ProviderSettingsError,
            default_effect_ttl=_DEFAULT_EFFECT_TTL,
            new_effect_id=_new_autonomy_effect_id,
            evaluate_gate_fn=evaluate_gate,
            new_id_fn=new_id,
        )


__all__ = [
    "AUTONOMY_EFFECT_KIND",
    "ActivityProvider",
    "ActivityResult",
    "AllowAutonomyJudge",
    "AutonomyContext",
    "AutonomyDecision",
    "AutonomyEngine",
    "AutonomyExecutionRequest",
    "AutonomyJudge",
    "DenyAutonomyJudge",
    "ProviderEligibilityError",
    "ProviderRegistry",
]
