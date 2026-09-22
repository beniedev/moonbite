"""Single-choice, evidence-gated autonomy runtime.

Autonomy is intentionally conservative.  A provider invocation is recorded as
an effect intent before the adapter is called.  An ordinary return value means
only that the adapter returned; it is never proof that an external effect was
seen.  Only a strict :class:`~moonbite_plugin.effects.EffectReceipt` matching
the intent can produce a completed result.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import threading
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Protocol
from urllib.parse import quote, unquote

from ._autonomy.observation import (
    _autonomy_fact,
    _autonomy_integrity,
    _autonomy_observer_reason,
    _autonomy_text,
    engine_observer_status as _engine_observer_status,
    _read_audit_rows_lock_free as _read_autonomy_audit_rows_lock_free,
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

        epoch_id = cls._record_value(record, "epoch_id")
        if epoch_id is None:
            return None
        created_at = cls._record_value(record, "created_at")
        if (
            type(epoch_id) is str
            and isinstance(created_at, datetime)
            and epoch_id == f"autonomy:{created_at.date().isoformat()}"
        ):
            return None
        return epoch_id

    @classmethod
    def _terminal_identity(cls, result: ActivityResult) -> tuple[str, str | None]:
        """Resolve the public terminal identity without losing effect evidence."""

        source_values: list[str] = []
        for value in (
            result.source_event_id,
            result.canonical_event_id,
            cls._record_value(result.effect_record, "source_event_id"),
        ):
            if value is None:
                continue
            if type(value) is not str or not value.strip():
                raise ValueError("invalid_source_event_id")
            source_values.append(value)
        if len(set(source_values)) > 1:
            raise ValueError("conflicting_source_event_id")
        source_event_id = source_values[0] if source_values else None
        if source_event_id is None and result.run_id is not None:
            if type(result.run_id) is not str or not result.run_id.strip():
                raise ValueError("invalid_source_event_id")
            source_event_id = result.run_id
        if source_event_id is None:
            source_event_id = new_id("autonomy_terminal")

        evidence = result.evidence
        evidence_epoch = (
            evidence.get("epoch_id") if isinstance(evidence, Mapping) else None
        )
        record_epoch_raw = cls._record_value(result.effect_record, "epoch_id")
        record_epoch = cls._public_epoch_from_record(result.effect_record)
        if record_epoch is None and evidence_epoch == record_epoch_raw:
            # ``_record_evidence`` exposes the immutable ledger epoch.  A
            # legacy effect's date-derived value is not a public epoch.
            evidence_epoch = None
        epoch_id: str | None = None
        for value in (result.epoch_id, evidence_epoch, record_epoch):
            if value is None:
                continue
            if type(value) is not str or not value.strip():
                raise ValueError("invalid_epoch_id")
            if epoch_id is not None and epoch_id != value:
                raise ValueError("conflicting_epoch_id")
            epoch_id = value
        return source_event_id, epoch_id

    def _finish(
        self,
        result: ActivityResult,
        gate: GateResult,
        *,
        record_terminal: bool = True,
    ) -> ActivityResult:
        occurrence_id, epoch_id = self._terminal_identity(result)
        result = replace(
            result,
            source_event_id=result.source_event_id or occurrence_id,
            canonical_event_id=result.canonical_event_id or occurrence_id,
            epoch_id=epoch_id,
        )
        terminal = self._canonical_terminal(result) if record_terminal else None
        evidence = None
        if result.evidence:
            evidence = {
                key: result.evidence[key]
                for key in (
                    "state",
                    "receipt_id",
                    "event_id",
                    "epoch_id",
                    "content_sha256",
                    "content_length",
                )
                if key in result.evidence
            }
        details = {
            "provider": result.provider,
            "reason": self._reason_code(result.reason),
            "run_id": result.run_id,
            "effect_id": result.effect_id,
            "source_event_id": result.source_event_id,
            "epoch_id": epoch_id,
            "idempotency_key": result.idempotency_key,
            "control_id": gate.control_id,
            "evidence": evidence,
            "gate": {
                "allowed": gate.allowed,
                "mode": gate.mode,
                "reason": self._reason_code(gate.reason),
                "control_id": gate.control_id,
            },
        }
        if occurrence_id is not None:
            details["occurrence_id"] = occurrence_id
        try:
            if terminal is not None and occurrence_id is not None:
                self.bus.record_audit_terminal(
                    "autonomy",
                    occurrence_id=occurrence_id,
                    epoch_id=epoch_id,
                    terminal=terminal,
                    status=result.status,
                    source="autonomy",
                    details=details,
                )
            else:
                self.bus.record_audit(
                    "autonomy",
                    status=result.status,
                    source="autonomy",
                    details=details,
                )
        except Exception as exc:
            if isinstance(exc, RuntimeError) and "conflict" in str(exc).lower():
                return replace(
                    result,
                    status="failed",
                    reason="terminal_conflict",
                    audit_status="degraded",
                    audit_error=f"audit_terminal_conflict:{type(exc).__name__}",
                )
            # The EffectLedger is the state owner.  A failed audit projection
            # must not erase a verified effect or invite a second execution.
            return replace(
                result,
                audit_status="degraded",
                audit_error=f"audit_error:{type(exc).__name__}",
            )
        return result

    @staticmethod
    def _canonical_terminal(result: ActivityResult) -> str | None:
        if result.status == "skipped":
            if result.reason == "execution_in_progress":
                # A lock race is only telemetry.  It must not claim the
                # occurrence before the owner can settle its real outcome.
                return None
            return AutonomyEngine._reason_code(result.reason)
        if result.status == "completed":
            return "verified"
        if result.status == "failed":
            if result.effect_record is not None and getattr(
                result.effect_record, "state", None
            ) not in {"failed", "verified"}:
                return None
            if result.reason == "effect_expired_unverified":
                return "expired"
            return "failed"
        return None

    def _existing_terminal_result(
        self,
        occurrence_id: str,
        *,
        epoch_id: str | None = None,
        ledger_epoch_id: str,
    ) -> ActivityResult | None:
        finder = getattr(self.bus, "find_audit_terminal", None)
        if not callable(finder):
            return None
        event = finder("autonomy", occurrence_id, epoch_id=epoch_id)
        if event is None:
            return None
        payload = event.payload
        terminal = payload.get("terminal")
        if type(terminal) is not str or not terminal.strip():
            raise RuntimeError("autonomy terminal audit is invalid")
        # EffectLedger remains truth for this exact public occurrence and its
        # durable internal epoch.  The public epoch is optional for legacy
        # callers, so checking only ``epoch_id`` would let a no-epoch audit
        # mask a date-derived effect or mix two identities.
        matching = [
            record
            for record in self.effect_ledger.records()
            if self._record_value(record, "kind") == AUTONOMY_EFFECT_KIND
            and self._record_value(record, "source_event_id") == occurrence_id
            and self._record_value(record, "epoch_id") == ledger_epoch_id
        ]
        if len(matching) > 1:
            raise RuntimeError("autonomy occurrence conflict")
        if matching:
            record = matching[0]
            state = self._record_state(record)
            effect_terminal = (
                "verified"
                if state == "verified"
                else "failed"
                if state == "failed"
                else None
            )
            if (
                effect_terminal == "failed"
                and self._record_value(record, "reason") == "effect_expired_unverified"
            ):
                effect_terminal = "expired"
            if effect_terminal is not None:
                if effect_terminal != terminal:
                    raise RuntimeError("audit effect conflict")
                # Let _existing_result serialize the durable effect, including
                # its receipt and failure reason, instead of trusting audit.
                return None
            # A terminal skip cannot mask an intent that is still awaiting a
            # durable provider outcome.  Replaying it would turn a temporary
            # authority decision into a false terminal state.
            raise RuntimeError("audit effect conflict")
        elif payload.get("effect_id") is not None:
            # An effect-bearing terminal without its exact ledger record is
            # untrusted.  Replaying it would hide an incomplete or conflicting
            # effect, especially on the legacy no-public-epoch path.
            raise RuntimeError("autonomy terminal effect unavailable")
        status = payload.get("status")
        if status == "completed":
            # Autonomy completion is receipt-backed.  A canonical success with
            # no matching effect cannot prove delivery and must not be replayed.
            # Let the caller's existing-terminal conflict path fail closed
            # without appending another terminal for the same identity.
            raise RuntimeError("autonomy terminal effect unavailable")
        if status not in {"skipped", "failed"}:
            raise RuntimeError("autonomy terminal status invalid")
        result_status = status
        provider = payload.get("provider")
        if not isinstance(provider, str) or not provider.strip():
            provider = None
        effect_id = payload.get("effect_id")
        if not isinstance(effect_id, str) or not effect_id.strip():
            effect_id = None
        idempotency_key = payload.get("idempotency_key")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            idempotency_key = None
        return ActivityResult(
            result_status,
            provider,
            self._reason_code(payload.get("reason") or terminal),
            effect_id=effect_id,
            source_event_id=occurrence_id,
            idempotency_key=idempotency_key,
            canonical_event_id=occurrence_id,
            epoch_id=payload.get("epoch_id"),
        )

    @staticmethod
    def _reason_code(reason: Any) -> str:
        if not isinstance(reason, str) or not reason:
            return "unspecified"
        normalized = "".join(
            character
            if character.isascii() and (character.isalnum() or character in "_:-")
            else "_"
            for character in reason
        )
        return normalized[:128]

    @staticmethod
    def _record_value(record: Any, key: str, default: Any = None) -> Any:
        if isinstance(record, Mapping):
            return record.get(key, default)
        return getattr(record, key, default)

    @staticmethod
    def _identity_overrides(
        facts: Mapping[str, Any],
    ) -> tuple[str | None, str | None, str | None]:
        def read(*keys: str) -> str | None:
            values: list[str] = []
            for key in keys:
                if key not in facts:
                    continue
                try:
                    values.append(_bounded(facts[key], key))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"invalid_{key}") from exc
            if len(set(values)) > 1:
                raise ValueError("conflicting_identity")
            return values[0] if values else None

        return (
            read("source_event_id", "occurrence_id", "event_id"),
            read("epoch_id", "epoch"),
            read("idempotency_key"),
        )

    @staticmethod
    def _validate_judge_decision(value: Any) -> AutonomyDecision | None:
        if not isinstance(value, AutonomyDecision):
            return None
        if type(value.allowed) is not bool:
            return None
        try:
            _bounded(value.reason, "judge reason", max_bytes=128)
            provider_weights = value.provider_weights
            if not isinstance(provider_weights, Mapping) or len(provider_weights) > 64:
                return None
            for name, weight in provider_weights.items():
                _bounded(name, "provider name", max_bytes=128)
                if type(weight) is not int or not 0 <= weight <= 100:
                    return None
        except (AttributeError, TypeError, ValueError):
            return None
        return value

    def _audit_history(self) -> list[dict[str, Any]]:
        """Return EventBus telemetry with durable effect fallback.

        EventBus remains the primary telemetry owner.  Effect records fill
        only the gap where an audit projection was unavailable, so limits
        cannot be bypassed by a failed audit write.
        """

        rows: list[dict[str, Any]] = []
        seen_effect_ids: set[str] = set()
        try:
            events = self.bus.read_audit()
        except Exception:
            events = ()
        for event in events:
            if getattr(event, "kind", None) != "audit.autonomy":
                continue
            payload = getattr(event, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            if payload.get("status") not in {
                "completed",
                "executed_unverified",
                "failed",
            }:
                continue
            provider = payload.get("provider")
            if not isinstance(provider, str) or not provider:
                continue
            row = dict(payload)
            row["created_at"] = getattr(event, "created_at", None)
            rows.append(row)

            effect_id = payload.get("effect_id")
            if isinstance(effect_id, str) and effect_id:
                seen_effect_ids.add(effect_id)

        try:
            records = self.effect_ledger.records()
        except Exception:
            if rows:
                return rows
            raise
        record_iter = records.values() if isinstance(records, Mapping) else records
        for record in record_iter:
            if self._record_value(record, "kind") != AUTONOMY_EFFECT_KIND:
                continue
            effect_id = self._record_value(record, "effect_id")
            if not isinstance(effect_id, str) or not effect_id:
                continue
            if effect_id in seen_effect_ids:
                continue
            provider = self._record_provider(record)
            if not provider:
                continue
            state = self._record_state(record)
            rows.append(
                {
                    "provider": provider,
                    "status": "completed" if state == "verified" else state,
                    "effect_id": effect_id,
                    "source_event_id": self._record_value(record, "source_event_id"),
                    "idempotency_key": self._record_value(record, "idempotency_key"),
                    "created_at": self._record_value(record, "created_at"),
                }
            )
        return rows

    @staticmethod
    def _record_provider(record: Any) -> str | None:
        direct = AutonomyEngine._record_value(record, "provider")
        if isinstance(direct, str) and direct:
            return direct
        payload = AutonomyEngine._record_value(record, "payload")
        if isinstance(payload, Mapping):
            value = payload.get("provider")
            if isinstance(value, str) and value:
                return value
        value = _provider_from_effect_id(
            AutonomyEngine._record_value(record, "effect_id")
        )
        if value is not None:
            return value
        key = AutonomyEngine._record_value(record, "idempotency_key", "")
        if isinstance(key, str) and key.startswith("autonomy:"):
            parts = key.split(":", 2)
            if len(parts) == 3 and parts[1]:
                return parts[1]
        return None

    def _provider_history(
        self, name: str, *, exclude_effect_id: str | None = None
    ) -> list[Any]:
        def is_excluded(record: Any) -> bool:
            return (
                exclude_effect_id is not None
                and self._record_value(record, "effect_id") == exclude_effect_id
            )

        rows = [
            record
            for record in self._audit_history()
            if self._record_provider(record) == name and not is_excluded(record)
        ]
        latest_by_effect: dict[str, Any] = {}
        without_effect: list[Any] = []
        for record in rows:
            effect_id = self._record_value(record, "effect_id")
            if is_excluded(record):
                continue
            if isinstance(effect_id, str) and effect_id:
                latest_by_effect[effect_id] = record
            else:
                without_effect.append(record)
        return without_effect + list(latest_by_effect.values())

    def _bound_control_id(self, effect_id: str) -> str | None:
        for row in self._audit_history():
            if row.get("effect_id") != effect_id:
                continue
            control_id = row.get("control_id")
            if isinstance(control_id, str) and control_id:
                return control_id
        return None

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
        if not isinstance(settings, Mapping):
            raise _ProviderSettingsError(None, "settings")
        for name, provider_settings in settings.items():
            try:
                provider_name = _bounded(name, "provider name", max_bytes=128)
            except (TypeError, ValueError) as exc:
                raise _ProviderSettingsError(None, "provider_name") from exc
            if not isinstance(provider_settings, Mapping):
                raise _ProviderSettingsError(provider_name, "settings")

            known_fields = {
                "enabled",
                "weight",
                "allowed_sources",
                "allowed_channels",
                "cooldown",
                "effect_ttl",
                "daily_limit",
                "repeat_limit",
                "cost_budget",
                "cost",
                "cost_class",
            }
            if any(field not in known_fields for field in provider_settings):
                raise _ProviderSettingsError(provider_name, "unknown")

            if (
                "enabled" in provider_settings
                and type(provider_settings["enabled"]) is not bool
            ):
                raise _ProviderSettingsError(provider_name, "enabled")
            if "weight" in provider_settings:
                weight = provider_settings["weight"]
                if type(weight) is not int or not 1 <= weight <= 100:
                    raise _ProviderSettingsError(provider_name, "weight")
            for field_name in ("allowed_sources", "allowed_channels"):
                if field_name in provider_settings:
                    try:
                        _optional_gate_set(provider_settings[field_name], field_name)
                    except (TypeError, ValueError) as exc:
                        raise _ProviderSettingsError(provider_name, field_name) from exc

            for field_name in ("cooldown", "effect_ttl"):
                if field_name not in provider_settings:
                    continue
                value = provider_settings[field_name]
                if value is None:
                    continue
                try:
                    seconds = (
                        value.total_seconds()
                        if isinstance(value, timedelta)
                        else _nonnegative_number(value, field_name)
                    )
                except (TypeError, ValueError) as exc:
                    raise _ProviderSettingsError(provider_name, field_name) from exc
                if (
                    seconds is None
                    or seconds < 0
                    or seconds > 31 * 24 * 60 * 60
                    or field_name == "effect_ttl"
                    and seconds <= 0
                ):
                    raise _ProviderSettingsError(provider_name, field_name)

            for field_name in (
                "daily_limit",
                "repeat_limit",
                "cost_budget",
                "cost",
            ):
                if field_name not in provider_settings:
                    continue
                try:
                    _positive_limit(provider_settings[field_name], field_name)
                except (TypeError, ValueError) as exc:
                    raise _ProviderSettingsError(provider_name, field_name) from exc

            if "cost_class" in provider_settings:
                cost_class = provider_settings["cost_class"]
                if type(cost_class) is not str or cost_class not in _COST_UNITS:
                    raise _ProviderSettingsError(provider_name, "cost_class")

    def _eligible_reason(
        self,
        provider: ActivityProvider,
        provider_settings: Mapping[str, Any],
        context: AutonomyContext,
        *,
        exclude_effect_id: str | None = None,
    ) -> str | None:
        facts = context.facts
        source = facts.get("source", facts.get("source_kind"))
        channel = facts.get("channel")
        allowed_sources = _optional_gate_set(
            self._setting(
                provider, provider_settings, "allowed_sources", provider.allowed_sources
            ),
            "allowed_sources",
        )
        allowed_channels = _optional_gate_set(
            self._setting(
                provider,
                provider_settings,
                "allowed_channels",
                provider.allowed_channels,
            ),
            "allowed_channels",
        )
        if allowed_sources and source not in allowed_sources:
            return "source_not_allowed"
        if allowed_channels and channel not in allowed_channels:
            return "channel_not_allowed"

        try:
            history = self._provider_history(
                provider.name, exclude_effect_id=exclude_effect_id
            )
        except Exception as exc:
            raise ProviderEligibilityError(provider.name, exc) from exc
        now = context.now
        cooldown = self._setting(
            provider, provider_settings, "cooldown", provider.cooldown
        )
        if cooldown is not None:
            seconds = self._cooldown_seconds(cooldown)
            recent = [self._record_time(record) for record in history]
            recent = [item for item in recent if item is not None]
            if recent and (now - max(recent)).total_seconds() < seconds:
                return "cooldown"

        daily_limit = self._setting(
            provider, provider_settings, "daily_limit", provider.daily_limit
        )
        if daily_limit is not None:
            daily_limit = _positive_limit(daily_limit, "daily_limit")
            count = sum(
                1
                for record in history
                if self._record_time(record) is not None
                and self._record_time(record).date() == now.date()
            )
            if count >= daily_limit:
                return "daily_limit"

        repeat_limit = self._setting(
            provider, provider_settings, "repeat_limit", provider.repeat_limit
        )
        if repeat_limit is not None:
            repeat_limit = _positive_limit(repeat_limit, "repeat_limit")
            repeat_key = facts.get("repeat_key", facts.get("source_event_id"))
            if repeat_key is not None:
                repeated = sum(
                    1
                    for record in history
                    if self._record_value(record, "source_event_id") == repeat_key
                )
                if repeated >= repeat_limit:
                    return "repeat_limit"

        cost_class = self._setting(
            provider, provider_settings, "cost_class", provider.cost_class
        )
        units = _COST_UNITS.get(str(cost_class).casefold(), 1)
        cost = self._setting(provider, provider_settings, "cost", units)
        try:
            units = max(1, int(cost))
        except (TypeError, ValueError):
            units = 1
        remaining = facts.get("cost_budget_remaining", facts.get("cost_budget"))
        if remaining is not None:
            try:
                if float(remaining) < units:
                    return "cost_budget"
            except (TypeError, ValueError):
                return "cost_budget"
        budget = self._setting(
            provider, provider_settings, "cost_budget", provider.cost_budget
        )
        if budget is not None:
            budget = _positive_limit(budget, "cost_budget")
            spent = 0
            for record in history:
                when = self._record_time(record)
                if when is not None and when.date() == now.date():
                    spent += units
            if spent + units > budget:
                return "cost_budget"

        try:
            if not provider.eligible(context):
                return "provider_ineligible"
        except Exception as exc:
            raise ProviderEligibilityError(provider.name, exc) from exc
        return None

    def _eligible(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        context: AutonomyContext,
    ) -> list[tuple[str, int]]:
        result: list[tuple[str, int]] = []
        for name, provider_settings in sorted(settings.items()):
            if provider_settings.get("enabled") is not True:
                continue
            provider = self.registry.get(name)
            if provider is None:
                continue
            reason = self._eligible_reason(provider, provider_settings, context)
            if reason is not None:
                continue
            try:
                weight = int(provider_settings.get("weight", 1))
            except (TypeError, ValueError):
                weight = 1
            result.append((name, max(1, min(weight, 100))))
        return result

    def _eligible_with_reasons(
        self,
        settings: Mapping[str, Mapping[str, Any]],
        context: AutonomyContext,
    ) -> tuple[list[tuple[str, int]], dict[str, str]]:
        candidates: list[tuple[str, int]] = []
        reasons: dict[str, str] = {}
        for name, provider_settings in sorted(settings.items()):
            if provider_settings.get("enabled") is not True:
                reasons[name] = "disabled"
                continue
            provider = self.registry.get(name)
            if provider is None:
                reasons[name] = "provider_not_registered"
                continue
            reason = self._eligible_reason(provider, provider_settings, context)
            if reason is not None:
                reasons[name] = reason
                continue
            try:
                weight = int(provider_settings.get("weight", 1))
            except (TypeError, ValueError):
                weight = 1
            candidates.append((name, max(1, min(weight, 100))))
        return candidates, reasons

    @staticmethod
    def _record_state(record: Any) -> str:
        return str(AutonomyEngine._record_value(record, "state", ""))

    @staticmethod
    def _record_evidence(record: Any) -> dict[str, Any]:
        state = AutonomyEngine._record_state(record)
        evidence: dict[str, Any] = {"state": state}
        for key in (
            "receipt_id",
            "event_id",
            "epoch_id",
            "content_sha256",
            "content_length",
        ):
            value = AutonomyEngine._record_value(record, key)
            if value is None and key in {"receipt_id", "event_id"}:
                receipt = AutonomyEngine._record_value(record, "receipt")
                if receipt is not None:
                    value = AutonomyEngine._record_value(receipt, key)
            if value is not None:
                evidence[key] = value
        return evidence

    def _find_by_idempotency(self, key: str) -> Any | None:
        try:
            return self.effect_ledger.find_by_idempotency(key)
        except (KeyError, ValueError):
            return None

    def _find_by_occurrence(self, source_event_id: str, epoch_id: str) -> Any | None:
        matches = [
            record
            for record in self.effect_ledger.records()
            if self._record_value(record, "kind") == AUTONOMY_EFFECT_KIND
            and self._record_value(record, "source_event_id") == source_event_id
            and self._record_value(record, "epoch_id") == epoch_id
        ]
        if len(matches) > 1:
            raise ValueError("occurrence_conflict")
        return matches[0] if matches else None

    def _audit_identity_for_record(self, source_event_id: str, record: Any) -> str:
        """Classify scoped audit proof for one durable autonomy effect."""

        effect_id = self._record_value(record, "effect_id")
        record_key = self._record_value(record, "idempotency_key")
        record_epoch = self._record_value(record, "epoch_id")
        if (
            not isinstance(effect_id, str)
            or not effect_id.strip()
            or not isinstance(record_epoch, str)
            or not record_epoch.strip()
        ):
            return "unknown"
        record_public_epoch = self._public_epoch_from_record(record)
        implicit = False
        explicit = False
        matched = False
        invalid = False
        try:
            events = self.bus.read_audit()
        except Exception:
            return "unknown"
        for event in events:
            if (
                getattr(event, "kind", None) != "audit.autonomy"
                or getattr(event, "source", None) != "autonomy"
            ):
                continue
            payload = getattr(event, "payload", None)
            if not isinstance(payload, Mapping):
                continue
            if payload.get("effect_id") != effect_id:
                continue
            occurrence_id = payload.get("occurrence_id")
            payload_source = payload.get("source_event_id")
            if (
                occurrence_id is not None
                and payload_source is not None
                and occurrence_id != payload_source
            ):
                if (
                    occurrence_id == source_event_id
                    or payload_source == source_event_id
                ):
                    invalid = True
                continue
            if occurrence_id != source_event_id and payload_source != source_event_id:
                continue
            matched = True
            row_key = payload.get("idempotency_key")
            if row_key is not None and row_key != record_key:
                invalid = True
                continue
            row_epoch = payload.get("epoch_id")
            if row_epoch is None:
                if record_public_epoch is None:
                    implicit = True
                else:
                    invalid = True
                continue
            if type(row_epoch) is not str or not row_epoch.strip():
                invalid = True
                continue
            if row_epoch != record_epoch:
                invalid = True
                continue
            explicit = True
        if invalid or not matched or (implicit and explicit):
            return "unknown"
        if explicit:
            return "explicit"
        if implicit:
            return "implicit"
        return "unknown"

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

        matches: list[Any] = []
        unproven: list[Any] = []
        for record in self.effect_ledger.records():
            if self._record_value(record, "kind") != AUTONOMY_EFFECT_KIND:
                continue
            if self._record_value(record, "source_event_id") != source_event_id:
                continue
            effect_id = self._record_value(record, "effect_id")
            if not isinstance(effect_id, str) or not effect_id.strip():
                continue
            identity = self._audit_identity_for_record(source_event_id, record)
            if identity == "unknown":
                unproven.append(record)
            elif identity == "implicit":
                matches.append(record)
            else:
                # An explicit epoch that equals the internal epoch requested
                # by this legacy retry is indistinguishable from it in the
                # ledger schema.  Keep the retry pending instead of allowing
                # a fresh begin_intent to collide or replay it.
                record_epoch = self._record_value(record, "epoch_id")
                if record_epoch == requested_epoch_id:
                    unproven.append(record)
        if unproven:
            raise ValueError("implicit_identity_unavailable")
        if len(matches) > 1:
            raise ValueError("occurrence_conflict")
        return matches[0] if matches else None

    def _consume_verified(
        self,
        gate: GateResult,
        *,
        effect_id: str | None = None,
        allow_current: bool = False,
        original_control_id: str | None = None,
    ) -> bool:
        if gate.mode != "play_next" or not gate.control_id:
            return False
        if effect_id is not None and not allow_current:
            durable_bound = self._bound_control_id(effect_id)
            if (
                durable_bound is not None
                and original_control_id is not None
                and original_control_id != durable_bound
            ):
                return False
            bound = durable_bound or original_control_id
            if bound is None or bound != gate.control_id:
                return False
        self.controls.consume(gate.control_id)
        return True

    def _existing_result(
        self,
        record: Any,
        *,
        provider: str,
        gate: GateResult,
        run_id: str,
        public_epoch_id: str | None = None,
    ) -> ActivityResult | None:
        state = self._record_state(record)
        effect_id = self._record_value(record, "effect_id")
        source_event_id = self._record_value(record, "source_event_id")
        idempotency_key = self._record_value(record, "idempotency_key")
        evidence = self._record_evidence(record)
        result_epoch_id = (
            public_epoch_id
            if public_epoch_id is not None
            else self._public_epoch_from_record(record)
        )
        if state == "verified":
            try:
                self._consume_verified(gate, effect_id=effect_id)
            except Exception:
                return self._finish(
                    ActivityResult(
                        "failed",
                        provider,
                        "control_consume_error",
                        run_id=run_id,
                        effect_id=effect_id,
                        evidence=evidence,
                        source_event_id=source_event_id,
                        idempotency_key=idempotency_key,
                        effect_record=record
                        if isinstance(record, EffectRecord)
                        else None,
                        canonical_event_id=source_event_id,
                        epoch_id=result_epoch_id,
                    ),
                    gate,
                )
            return self._finish(
                ActivityResult(
                    "completed",
                    provider,
                    "already_verified",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=evidence,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                    epoch_id=result_epoch_id,
                ),
                gate,
            )
        if state in {"pending", "executed_unverified"}:
            return self._finish(
                ActivityResult(
                    "awaiting_reconciliation",
                    provider,
                    "awaiting_reconciliation",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=evidence,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                    epoch_id=result_epoch_id,
                ),
                gate,
            )
        if state == "expired":
            return self._finish(
                ActivityResult(
                    "awaiting_reconciliation",
                    provider,
                    "expired_requeue_required",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=evidence,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                    epoch_id=result_epoch_id,
                ),
                gate,
            )
        if state == "failed":
            return self._finish(
                ActivityResult(
                    "failed",
                    provider,
                    self._record_value(record, "reason") or "already_failed",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=evidence,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                    epoch_id=result_epoch_id,
                ),
                gate,
            )
        return None

    @staticmethod
    def _effect_identity(
        provider: str,
        source_event_id: str,
        epoch_id: str,
    ) -> tuple[str, str, int]:
        identity = json.dumps(
            {
                "provider": provider,
                "source_event_id": source_event_id,
                "epoch_id": epoch_id,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return (
            hashlib.sha256(identity).hexdigest(),
            f"autonomy:{provider}:{source_event_id}:{epoch_id}",
            len(identity),
        )

    @staticmethod
    def _weighted_selection(
        candidates: list[tuple[str, int]], occurrence_identity: str
    ) -> str:
        """Choose reproducibly from bounded weights for one occurrence."""

        ordered = sorted(candidates)
        identity = json.dumps(
            {
                "candidates": ordered,
                "occurrence": occurrence_identity,
                "selection_contract": "moon.autonomy.weighted.v1",
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        slot = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big") % sum(
            weight for _name, weight in ordered
        )
        for name, weight in ordered:
            if slot < weight:
                return name
            slot -= weight
        raise AssertionError("bounded weighted selection exhausted")

    @staticmethod
    def _receipt_from_output(output: Any) -> tuple[EffectReceipt | None, str | None]:
        candidate = output if isinstance(output, EffectReceipt) else None
        if candidate is None and isinstance(output, Mapping):
            for key in ("receipt", "effect_receipt", "evidence"):
                value = output.get(key)
                if isinstance(value, EffectReceipt):
                    candidate = value
                    break
                if isinstance(value, Mapping):
                    try:
                        candidate = EffectReceipt.from_dict(value)
                    except Exception:
                        return None, "evidence_invalid"
                    break
            if (
                candidate is None
                and "schema_version" in output
                and "receipt_id" in output
            ):
                try:
                    candidate = EffectReceipt.from_dict(output)
                except Exception:
                    return None, "evidence_invalid"
        if candidate is None:
            value = getattr(output, "receipt", None)
            if isinstance(value, EffectReceipt):
                candidate = value
        return candidate, None

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

        if not isinstance(receipt, EffectReceipt):
            raise TypeError("receipt must be an EffectReceipt")
        record = self.effect_ledger.get(effect_id)
        if record is None:
            raise ValueError("autonomy effect does not exist")
        if self._record_value(record, "kind") != AUTONOMY_EFFECT_KIND:
            raise ValueError("effect is not an autonomy completion")
        provider = self._record_provider(record) or "unknown"
        gate = evaluate_gate(self.controls.resolve("autonomy"))
        state = self._record_state(record)
        expires_at = self._record_value(record, "expires_at")
        if state == "expired" or (
            state not in {"verified", "failed"}
            and isinstance(expires_at, datetime)
            and self.clock() >= expires_at
        ):
            return self._finish(
                ActivityResult(
                    "awaiting_reconciliation",
                    provider,
                    "expired_requeue_required",
                    effect_id=effect_id,
                    evidence=self._record_evidence(record),
                    source_event_id=self._record_value(record, "source_event_id"),
                    idempotency_key=self._record_value(record, "idempotency_key"),
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=self._record_value(record, "source_event_id"),
                ),
                gate,
            )
        try:
            verified = self.effect_ledger.verify(effect_id, receipt)
        except Exception:
            try:
                failed = self.effect_ledger.fail(effect_id, "receipt_mismatch", False)
            except Exception:
                failed = record
            return self._finish(
                ActivityResult(
                    "failed",
                    provider,
                    "receipt_mismatch",
                    effect_id=effect_id,
                    evidence=self._record_evidence(failed),
                    source_event_id=self._record_value(record, "source_event_id"),
                    idempotency_key=self._record_value(record, "idempotency_key"),
                    effect_record=failed if isinstance(failed, EffectRecord) else None,
                    canonical_event_id=self._record_value(record, "source_event_id"),
                ),
                gate,
            )
        self._consume_verified(
            gate, effect_id=effect_id, original_control_id=control_id
        )
        return self._finish(
            ActivityResult(
                "completed",
                provider,
                "verified_reconciliation",
                effect_id=effect_id,
                evidence=self._record_evidence(verified),
                source_event_id=self._record_value(verified, "source_event_id"),
                idempotency_key=self._record_value(verified, "idempotency_key"),
                effect_record=verified if isinstance(verified, EffectRecord) else None,
                canonical_event_id=self._record_value(verified, "source_event_id"),
            ),
            gate,
        )

    def fail(self, effect_id: str, reason: str) -> ActivityResult:
        """Settle an asynchronous provider with an explicit host failure."""

        failure_reason = _bounded(reason, "failure reason", max_bytes=128)
        record = self.effect_ledger.get(effect_id)
        if record is None:
            raise ValueError("autonomy effect does not exist")
        if self._record_value(record, "kind") != AUTONOMY_EFFECT_KIND:
            raise ValueError("effect is not an autonomy completion")
        provider = self._record_provider(record) or "unknown"
        gate = evaluate_gate(self.controls.resolve("autonomy"))
        if self._record_state(record) in {"verified", "failed"}:
            existing = self._existing_result(
                record,
                provider=provider,
                gate=gate,
                run_id=self._record_value(record, "effect_id"),
            )
            if existing is not None:
                return existing
        failed = self.effect_ledger.fail(effect_id, failure_reason, False)
        return self._finish(
            ActivityResult(
                "failed",
                provider,
                failure_reason,
                effect_id=effect_id,
                evidence=self._record_evidence(failed),
                source_event_id=self._record_value(failed, "source_event_id"),
                idempotency_key=self._record_value(failed, "idempotency_key"),
                effect_record=failed if isinstance(failed, EffectRecord) else None,
                canonical_event_id=self._record_value(failed, "source_event_id"),
            ),
            gate,
        )

    def _settle_expired_unverified(
        self,
        *,
        now: datetime,
        gate: GateResult,
    ) -> None:
        """Fail old unverified autonomy effects without replaying providers."""

        for record in self.effect_ledger.records():
            if self._record_value(
                record, "kind"
            ) != AUTONOMY_EFFECT_KIND or self._record_state(record) not in {
                "pending",
                "executed_unverified",
            }:
                continue
            expires_at = self._record_value(record, "expires_at")
            if not isinstance(expires_at, datetime) or not expires_at < now:
                continue
            effect_id = self._record_value(record, "effect_id")
            provider = self._record_provider(record) or "unknown"
            try:
                failed = self.effect_ledger.fail(
                    effect_id,
                    "effect_expired_unverified",
                    False,
                )
            except Exception:
                current = self.effect_ledger.get(effect_id)
                if self._record_state(current) in {"verified", "failed"}:
                    continue
                raise
            self._finish(
                ActivityResult(
                    "failed",
                    provider,
                    "effect_expired_unverified",
                    effect_id=effect_id,
                    evidence=self._record_evidence(failed),
                    source_event_id=self._record_value(failed, "source_event_id"),
                    idempotency_key=self._record_value(failed, "idempotency_key"),
                    effect_record=(
                        failed if isinstance(failed, EffectRecord) else None
                    ),
                    canonical_event_id=self._record_value(failed, "source_event_id"),
                ),
                gate,
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
        resolution = self.controls.resolve("autonomy")
        gate = evaluate_gate(resolution)
        now = self.clock()
        context = AutonomyContext(now, {} if facts is None else dict(facts))
        run_id = new_id("autonomy_run")
        try:
            source_override, epoch_override, idempotency_override = (
                self._identity_overrides(context.facts)
            )
        except ValueError as exc:
            return self._finish(
                ActivityResult("failed", None, str(exc), run_id=run_id), gate
            )
        source_event_id = source_override or run_id
        # ``epoch_id`` is optional at the public terminal boundary.  The
        # effect ledger still needs a durable epoch for its immutable schema;
        # legacy callers use the date-derived internal value while retaining
        # their original no-epoch audit/key identity.
        epoch_id = epoch_override
        effect_epoch_id = epoch_override or (
            f"autonomy:{context.now.date().isoformat()}"
        )

        def finish(
            result: ActivityResult,
            _gate: GateResult | None = None,
            *,
            record_terminal: bool = True,
        ) -> ActivityResult:
            if (
                result.source_event_id is None
                or result.canonical_event_id is None
                or result.epoch_id is None
                and epoch_id is not None
            ):
                result = replace(
                    result,
                    run_id=result.run_id or run_id,
                    source_event_id=result.source_event_id or source_event_id,
                    canonical_event_id=result.canonical_event_id or source_event_id,
                    epoch_id=result.epoch_id or epoch_id,
                )
            return self._finish(result, gate, record_terminal=record_terminal)

        requested: str | None = None
        payload = resolution.intent.payload if resolution.intent is not None else {}
        if gate.mode == "play_next" and isinstance(payload, Mapping):
            value = payload.get("provider")
            if isinstance(value, str) and value.strip():
                requested = value
        existing_selection: Any | None = None
        try:
            if idempotency_override is not None:
                existing_selection = self._find_by_idempotency(idempotency_override)
            if (
                existing_selection is None
                and source_override is not None
                and epoch_override is not None
            ):
                existing_selection = self._find_by_occurrence(
                    source_event_id, effect_epoch_id
                )
            if (
                existing_selection is None
                and source_override is not None
                and epoch_override is None
            ):
                existing_selection = self._find_implicit_occurrence(
                    source_event_id,
                    requested_epoch_id=effect_epoch_id,
                )
            if existing_selection is not None:
                if (
                    self._record_value(existing_selection, "kind")
                    != AUTONOMY_EFFECT_KIND
                ):
                    raise ValueError("occurrence_conflict")
                recorded_source = self._record_value(
                    existing_selection, "source_event_id"
                )
                recorded_epoch = self._record_value(existing_selection, "epoch_id")
                if source_override is not None and source_override != recorded_source:
                    raise ValueError("occurrence_conflict")
                if epoch_override is not None and epoch_override != recorded_epoch:
                    raise ValueError("occurrence_conflict")
                source_event_id = recorded_source
                effect_epoch_id = recorded_epoch
                if (
                    idempotency_override is not None
                    and epoch_override is None
                    and self._audit_identity_for_record(
                        source_event_id, existing_selection
                    )
                    != "implicit"
                ):
                    # An idempotency key identifies a durable effect, but it
                    # does not supply the missing public epoch.  Refuse an
                    # explicit effect under a legacy request instead of
                    # replaying it across public identity boundaries.
                    raise ValueError("implicit_identity_unavailable")
                if (
                    epoch_override is not None
                    and epoch_override == recorded_epoch
                    and self._public_epoch_from_record(existing_selection) is None
                    and self._audit_identity_for_record(
                        source_event_id, existing_selection
                    )
                    != "explicit"
                ):
                    # A date-shaped internal epoch can belong either to a
                    # legacy occurrence or to an explicit caller value.  A
                    # source-plus-epoch retry cannot resolve that collision
                    # from the ledger alone, so leave the record untouched.
                    raise ValueError("implicit_identity_unavailable")
        except ValueError as exc:
            if str(exc) == "implicit_identity_unavailable":
                # The ledger candidate remains untouched, but its internal
                # epoch is not enough to prove a legacy public identity.  Do
                # not turn that uncertainty into a permanent terminal that
                # would poison a later retry once valid evidence exists.
                return finish(
                    ActivityResult(
                        "awaiting_reconciliation",
                        None,
                        "implicit_identity_unavailable",
                        run_id=run_id,
                        source_event_id=source_event_id,
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                    record_terminal=False,
                )
            reason = "occurrence_conflict"
            return finish(ActivityResult("failed", None, reason), gate)
        except Exception as exc:
            return finish(
                ActivityResult(
                    "failed", None, f"effect_lookup_error:{type(exc).__name__}"
                ),
                gate,
            )

        # Resolve an existing canonical terminal before current admission
        # gates.  A prior no-effect skip is still the durable fact for this
        # occurrence, while an effect-bearing terminal is checked against the
        # exact ledger epoch before its state is replayed.
        try:
            existing_terminal = self._existing_terminal_result(
                source_event_id,
                epoch_id=epoch_id,
                ledger_epoch_id=effect_epoch_id,
            )
        except Exception as exc:
            return finish(
                ActivityResult(
                    "failed",
                    None,
                    f"terminal_integrity_error:{type(exc).__name__}",
                ),
                gate,
            )
        if existing_terminal is not None:
            return existing_terminal

        unexecuted_intent = (
            existing_selection is not None
            and self._record_state(existing_selection) == "intent"
        )
        intent_provider = (
            self._record_provider(existing_selection) if unexecuted_intent else None
        )
        intent_effect_id = (
            self._record_value(existing_selection, "effect_id")
            if unexecuted_intent
            else None
        )

        def intent_skip(reason: str) -> ActivityResult:
            return ActivityResult(
                "skipped",
                intent_provider,
                reason,
                run_id=run_id,
                effect_id=intent_effect_id,
                evidence=self._record_evidence(existing_selection),
                source_event_id=source_event_id,
                idempotency_key=self._record_value(
                    existing_selection, "idempotency_key"
                ),
                effect_record=(
                    existing_selection
                    if isinstance(existing_selection, EffectRecord)
                    else None
                ),
                canonical_event_id=source_event_id,
                epoch_id=epoch_id,
            )

        self._settle_expired_unverified(now=now, gate=gate)

        # Resolve the durable effect before evaluating current gates.  A
        # pending or executed effect belongs to the host's reconciliation
        # path; replaying it through a current chat/control gate would append
        # a misleading terminal skip for work that already started.
        if existing_selection is not None:
            refreshed = self.effect_ledger.get(
                self._record_value(existing_selection, "effect_id")
            )
            if refreshed is not None:
                existing_selection = refreshed
            if self._record_state(existing_selection) != "intent":
                selected_existing = self._record_provider(existing_selection)
                reconciled = self._existing_result(
                    existing_selection,
                    provider=selected_existing or "unknown",
                    gate=gate,
                    run_id=self._record_value(existing_selection, "effect_id"),
                    public_epoch_id=epoch_id,
                )
                if reconciled is not None:
                    return reconciled
                if selected_existing is None:
                    return finish(
                        ActivityResult(
                            "awaiting_reconciliation",
                            None,
                            "selection_provider_unavailable",
                            run_id=run_id,
                            effect_id=self._record_value(
                                existing_selection, "effect_id"
                            ),
                            evidence=self._record_evidence(existing_selection),
                            source_event_id=source_event_id,
                            idempotency_key=self._record_value(
                                existing_selection, "idempotency_key"
                            ),
                            effect_record=(
                                existing_selection
                                if isinstance(existing_selection, EffectRecord)
                                else None
                            ),
                            canonical_event_id=source_event_id,
                        ),
                        gate,
                    )

        if not gate.allowed:
            if unexecuted_intent:
                return finish(
                    intent_skip(gate.reason),
                    gate,
                    record_terminal=False,
                )
            return finish(ActivityResult("skipped", None, gate.reason))

        # Active-chat is a hard gate for new effects and unexecuted intents.
        # Existing pending/executed effects were replayed above so that the
        # host can reconcile an already-started operation.
        for chat_key in ("active_chat", "chat_active"):
            if chat_key in context.facts and type(context.facts[chat_key]) is not bool:
                return finish(
                    ActivityResult("failed", None, f"{chat_key}_invalid"), gate
                )
            if context.facts.get(chat_key) is True:
                if unexecuted_intent:
                    return finish(
                        intent_skip("active_chat"),
                        gate,
                        record_terminal=False,
                    )
                return finish(ActivityResult("skipped", None, "active_chat"))

        selected: str | None = None
        selection_reason = "selected"
        if existing_selection is not None:
            selected = self._record_provider(existing_selection)
            selection_state = self._record_state(existing_selection)
            if selection_state == "intent":
                effect_id = self._record_value(existing_selection, "effect_id")
                try:
                    self._validate_provider_settings(settings)
                except _ProviderSettingsError as exc:
                    return finish(
                        ActivityResult(
                            "failed",
                            selected,
                            f"invalid_provider_settings:{exc.field}",
                            run_id=run_id,
                            effect_id=effect_id,
                            evidence=self._record_evidence(existing_selection),
                            source_event_id=source_event_id,
                            idempotency_key=self._record_value(
                                existing_selection, "idempotency_key"
                            ),
                            effect_record=(
                                existing_selection
                                if isinstance(existing_selection, EffectRecord)
                                else None
                            ),
                            canonical_event_id=source_event_id,
                        ),
                        gate,
                    )
                provider_settings = (
                    settings.get(selected) if selected is not None else None
                )
                provider = self.registry.get(selected) if selected is not None else None
                if (
                    provider is None
                    or not isinstance(provider_settings, Mapping)
                    or provider_settings.get("enabled") is not True
                ):
                    return finish(
                        intent_skip("no_eligible_provider"),
                        gate,
                        record_terminal=False,
                    )
                try:
                    eligibility_reason = self._eligible_reason(
                        provider,
                        provider_settings,
                        context,
                        exclude_effect_id=effect_id,
                    )
                except ProviderEligibilityError as exc:
                    return finish(
                        ActivityResult(
                            "failed",
                            selected,
                            f"eligibility_error:{type(exc.cause).__name__}",
                            run_id=run_id,
                            effect_id=effect_id,
                            evidence=self._record_evidence(existing_selection),
                            source_event_id=source_event_id,
                            idempotency_key=self._record_value(
                                existing_selection, "idempotency_key"
                            ),
                            effect_record=(
                                existing_selection
                                if isinstance(existing_selection, EffectRecord)
                                else None
                            ),
                            canonical_event_id=source_event_id,
                        ),
                        gate,
                    )
                except (TypeError, ValueError):
                    return finish(
                        ActivityResult(
                            "failed",
                            selected,
                            "invalid_provider_settings",
                            run_id=run_id,
                            effect_id=effect_id,
                            evidence=self._record_evidence(existing_selection),
                            source_event_id=source_event_id,
                            idempotency_key=self._record_value(
                                existing_selection, "idempotency_key"
                            ),
                            effect_record=(
                                existing_selection
                                if isinstance(existing_selection, EffectRecord)
                                else None
                            ),
                            canonical_event_id=source_event_id,
                        ),
                        gate,
                    )
                if eligibility_reason is not None:
                    return finish(
                        intent_skip("no_eligible_provider"),
                        gate,
                        record_terminal=False,
                    )
            selection_reason = "resumed_intent"
        else:
            try:
                self._validate_provider_settings(settings)
            except _ProviderSettingsError as exc:
                return finish(
                    ActivityResult(
                        "failed",
                        exc.provider,
                        f"invalid_provider_settings:{exc.field}",
                    ),
                    gate,
                )
            if requested is None and not any(
                provider_settings.get("enabled") is True
                and self.registry.get(name) is not None
                for name, provider_settings in settings.items()
            ):
                return finish(
                    ActivityResult("skipped", None, "no_eligible_provider"), gate
                )
            try:
                decision = self._validate_judge_decision(self.judge.decide(context))
            except Exception as exc:
                return finish(
                    ActivityResult("failed", None, f"judge_error:{type(exc).__name__}"),
                    gate,
                )
            if decision is None:
                return finish(
                    ActivityResult("failed", None, "judge_invalid_result"), gate
                )
            if not decision.allowed:
                return finish(ActivityResult("skipped", None, decision.reason), gate)
            decision_weights = dict(decision.provider_weights)
            if any(
                name not in settings or self.registry.get(name) is None
                for name in decision_weights
            ):
                return finish(
                    ActivityResult("failed", None, "judge_unknown_provider"), gate
                )
            try:
                candidates, reasons = self._eligible_with_reasons(settings, context)
            except ProviderEligibilityError as exc:
                return finish(
                    ActivityResult(
                        "failed",
                        exc.provider,
                        f"eligibility_error:{type(exc.cause).__name__}",
                    ),
                    gate,
                )
            except (TypeError, ValueError):
                return finish(
                    ActivityResult("failed", None, "invalid_provider_settings"), gate
                )
            candidates = [
                (name, decision_weights.get(name, weight))
                for name, weight in candidates
                if decision_weights.get(name, weight) > 0
            ]

            if idempotency_override is not None:
                occurrence_identity = idempotency_override
            else:
                occurrence_identity = json.dumps(
                    {
                        "epoch_id": effect_epoch_id,
                        "source_event_id": source_event_id,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            if requested is not None:
                if any(name == requested for name, _weight in candidates):
                    selected = requested
                else:
                    fallback = (
                        payload.get("fallback_provider")
                        if isinstance(payload, Mapping)
                        else None
                    )
                    allow_fallback = isinstance(payload, Mapping) and (
                        payload.get("allow_fallback") is True
                        or payload.get("fallback") in {"first", "deterministic"}
                    )
                    if isinstance(fallback, str) and any(
                        name == fallback for name, _ in candidates
                    ):
                        selected = fallback
                        selection_reason = "play_next_fallback"
                    elif allow_fallback and candidates:
                        selected = candidates[0][0]
                        selection_reason = "play_next_fallback"
                    else:
                        return finish(
                            ActivityResult(
                                "skipped",
                                requested,
                                "play_next_unavailable:"
                                f"{reasons.get(requested, 'ineligible')}",
                            ),
                            gate,
                        )
            elif candidates:
                # The stable occurrence identity preserves weighted diversity
                # without process randomness. The runner is never re-rolled.
                selected = self._weighted_selection(candidates, occurrence_identity)
                selection_reason = "weighted_replayable"
        if selected is None:
            return finish(ActivityResult("skipped", None, "no_eligible_provider"), gate)

        provider = self.registry.get(selected)
        if provider is None:
            if existing_selection is not None:
                return finish(
                    ActivityResult(
                        "awaiting_reconciliation",
                        selected,
                        "selected_provider_unavailable",
                        run_id=run_id,
                        effect_id=self._record_value(existing_selection, "effect_id"),
                        evidence=self._record_evidence(existing_selection),
                        source_event_id=source_event_id,
                        idempotency_key=self._record_value(
                            existing_selection, "idempotency_key"
                        ),
                        effect_record=(
                            existing_selection
                            if isinstance(existing_selection, EffectRecord)
                            else None
                        ),
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                )
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    "provider_not_registered",
                    run_id=run_id,
                ),
                gate,
            )

        if existing_selection is not None:
            record = existing_selection
            digest = self._record_value(record, "content_sha256")
            content_length = self._record_value(record, "content_length")
            idempotency_key = self._record_value(record, "idempotency_key")
        else:
            digest, generated_key, content_length = self._effect_identity(
                selected, source_event_id, effect_epoch_id
            )
            idempotency_key = idempotency_override or generated_key
            provider_settings = settings.get(selected, {})
            ttl_value = provider_settings.get("effect_ttl", _DEFAULT_EFFECT_TTL)
            if isinstance(ttl_value, timedelta):
                ttl = ttl_value
            else:
                try:
                    ttl = timedelta(seconds=float(ttl_value))
                except (TypeError, ValueError):
                    ttl = _DEFAULT_EFFECT_TTL
            if ttl <= timedelta(0) or ttl > timedelta(days=31):
                ttl = _DEFAULT_EFFECT_TTL
            try:
                record = self.effect_ledger.begin_intent(
                    effect_id=_new_autonomy_effect_id(selected),
                    kind=AUTONOMY_EFFECT_KIND,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    epoch_id=effect_epoch_id,
                    content_sha256=digest,
                    content_length=content_length,
                    expires_at=context.now + ttl,
                    created_at=context.now,
                )
            except Exception as exc:
                return finish(
                    ActivityResult(
                        "failed",
                        selected,
                        f"effect_intent_error:{type(exc).__name__}",
                        run_id=run_id,
                    ),
                    gate,
                )
            reconciled = self._existing_result(
                record,
                provider=selected,
                gate=gate,
                run_id=run_id,
                public_epoch_id=epoch_id,
            )
            if reconciled is not None:
                return reconciled
        effect_id = self._record_value(record, "effect_id")
        try:
            self.bus.emit(
                "autonomy.started",
                source="autonomy",
                payload={
                    "provider": selected,
                    "selection": selection_reason,
                    "effect_id": effect_id,
                    "occurrence_id": source_event_id,
                    "epoch_id": effect_epoch_id,
                    "idempotency_key": idempotency_key,
                },
            )
        except Exception as exc:
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    f"started_event_error:{type(exc).__name__}",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(record),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record if isinstance(record, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        try:
            pending = self.effect_ledger.mark_pending(effect_id)
        except Exception as exc:
            try:
                self.effect_ledger.fail(effect_id, "effect_pending_error", False)
            except Exception:
                pass
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    f"effect_pending_error:{type(exc).__name__}",
                    run_id=run_id,
                    effect_id=effect_id,
                ),
                gate,
            )

        request = AutonomyExecutionRequest(
            provider=selected,
            effect_id=effect_id,
            idempotency_key=idempotency_key,
            source_event_id=source_event_id,
            epoch_id=effect_epoch_id,
            content_sha256=digest,
            content_length=content_length,
            attempt=int(self._record_value(pending, "attempt", 1)),
            context=context,
        )
        try:
            output = provider.run(request)
        except Exception as exc:
            try:
                failed = self.effect_ledger.fail(
                    effect_id, f"provider_error:{type(exc).__name__}", True
                )
            except Exception:
                failed = pending
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    f"provider_error:{type(exc).__name__}",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(failed),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=failed if isinstance(failed, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )

        receipt, evidence_error = self._receipt_from_output(output)
        if evidence_error is not None:
            try:
                failed = self.effect_ledger.fail(effect_id, evidence_error, False)
            except Exception:
                failed = pending
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    evidence_error,
                    output=output,
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(failed),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=failed if isinstance(failed, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        if receipt is None:
            try:
                unverified = self.effect_ledger.mark_queue_accepted(effect_id)
            except Exception as exc:
                return finish(
                    ActivityResult(
                        "failed",
                        selected,
                        f"effect_queue_error:{type(exc).__name__}",
                        output=output,
                        run_id=run_id,
                        effect_id=effect_id,
                        source_event_id=source_event_id,
                        idempotency_key=idempotency_key,
                    ),
                    gate,
                )
            return finish(
                ActivityResult(
                    "executed_unverified",
                    selected,
                    "awaiting_receipt",
                    output=output,
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(unverified),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=unverified
                    if isinstance(unverified, EffectRecord)
                    else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        try:
            verified = self.effect_ledger.verify(effect_id, receipt)
        except Exception as exc:
            try:
                failed = self.effect_ledger.fail(effect_id, "receipt_mismatch", False)
            except Exception:
                failed = pending
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    f"receipt_mismatch:{type(exc).__name__}",
                    output=output,
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(failed),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=failed if isinstance(failed, EffectRecord) else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        try:
            self._consume_verified(gate, effect_id=effect_id, allow_current=True)
        except Exception:
            return finish(
                ActivityResult(
                    "failed",
                    selected,
                    "control_consume_error",
                    output=output,
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=self._record_evidence(verified),
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=verified
                    if isinstance(verified, EffectRecord)
                    else None,
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        return finish(
            ActivityResult(
                "completed",
                selected,
                "verified",
                output=output,
                run_id=run_id,
                effect_id=effect_id,
                evidence=self._record_evidence(verified),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=verified if isinstance(verified, EffectRecord) else None,
                canonical_event_id=source_event_id,
            ),
            gate,
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
