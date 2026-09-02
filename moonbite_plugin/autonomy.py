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

from .control import ControlStore, GateResult, evaluate_gate
from .effects import (
    EffectLedger,
    EffectReceipt,
    EffectRecord,
    _read_effect_history_lock_free,
)
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    EventBus,
    FileRuntimeLocks,
    RuntimeLocks,
    ensure_bounded_text,
    new_id,
    parse_time,
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

    def __post_init__(self) -> None:
        if self.status not in _STATUSES:
            raise ValueError(f"invalid activity terminal: {self.status}")

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
    """Read only bounded autonomy audit telemetry without retaining payloads.

    Unrelated audit events are envelope-checked and discarded immediately
    after their payload is confirmed to be a mapping.  Their payload is never
    copied or traversed.  For ``audit.autonomy`` we retain only the scalar
    fields needed by the observer and one receipt reference.
    """

    if not path.exists():
        return (), None
    rows: list[dict[str, Any]] = []
    integrity: str | None = None
    seen_event_ids: set[str] = set()
    try:
        handle = path.open("r", encoding="utf-8")
    except Exception as exc:
        return (), f"read_{type(exc).__name__}"
    try:
        with handle:
            for raw in handle:
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw)
                    if not isinstance(value, Mapping):
                        raise ValueError("row is not an object")
                    if set(value) != {
                        "schema_version",
                        "event_id",
                        "created_at",
                        "kind",
                        "source",
                        "payload",
                    }:
                        raise ValueError("event fields are invalid")
                    if value.get("schema_version") != "moon.event.v1":
                        raise ValueError("event schema is invalid")
                    event_id = value.get("event_id")
                    kind = value.get("kind")
                    source = value.get("source")
                    if (
                        type(event_id) is not str
                        or not event_id.strip()
                        or type(kind) is not str
                        or not kind.strip()
                        or type(source) is not str
                        or not source.strip()
                    ):
                        raise ValueError("event identity is invalid")
                    if event_id in seen_event_ids:
                        integrity = integrity or "duplicate_event_id"
                        continue
                    seen_event_ids.add(event_id)
                    payload = value.get("payload")
                    if not isinstance(payload, Mapping):
                        raise ValueError("event payload is invalid")
                    if kind != "audit.autonomy":
                        continue
                    created_at = parse_time(value.get("created_at"))

                    def optional_scalar(
                        name: str, *, selected_payload: Mapping[str, Any] = payload
                    ) -> str | None:
                        selected = selected_payload.get(name)
                        if selected is None:
                            return None
                        if type(selected) is not str or not selected.strip():
                            raise ValueError(f"autonomy audit field {name} is invalid")
                        ensure_bounded_text(selected, name, max_bytes=512)
                        return selected

                    status = optional_scalar("status")
                    if status not in _STATUSES:
                        raise ValueError("autonomy audit status is invalid")
                    provider = optional_scalar("provider")
                    if provider is None and status not in {"failed", "skipped"}:
                        raise ValueError("autonomy audit fields are invalid")
                    evidence = payload.get("evidence")
                    if evidence is not None and not isinstance(evidence, Mapping):
                        raise ValueError("autonomy audit evidence is invalid")

                    def evidence_scalar(
                        name: str, *, selected_evidence: Any = evidence
                    ) -> str | None:
                        if not isinstance(selected_evidence, Mapping):
                            return None
                        selected = selected_evidence.get(name)
                        if selected is None:
                            return None
                        if type(selected) is not str or not selected.strip():
                            raise ValueError(
                                f"autonomy audit evidence {name} is invalid"
                            )
                        ensure_bounded_text(selected, name, max_bytes=512)
                        return selected

                    receipt_id = evidence_scalar("receipt_id")
                    receipt_event_id = evidence_scalar("event_id")
                    receipt_epoch_id = evidence_scalar("epoch_id")
                    receipt_content_sha256 = evidence_scalar("content_sha256")
                    receipt_content_length = None
                    if isinstance(evidence, Mapping):
                        selected_length = evidence.get("content_length")
                        if selected_length is not None:
                            if type(selected_length) is not int or selected_length <= 0:
                                raise ValueError(
                                    "autonomy audit evidence content_length is invalid"
                                )
                            receipt_content_length = selected_length
                    effect_id = optional_scalar("effect_id")
                    source_event_id = optional_scalar("source_event_id")
                    idempotency_key = optional_scalar("idempotency_key")
                except Exception as exc:
                    integrity = integrity or f"row_{type(exc).__name__}"
                    continue
                rows.append(
                    {
                        "event_id": event_id,
                        "kind": kind,
                        "created_at": created_at,
                        "status": status,
                        "provider": provider,
                        "effect_id": effect_id,
                        "source_event_id": source_event_id,
                        "idempotency_key": idempotency_key,
                        "receipt_id": receipt_id,
                        "receipt_event_id": receipt_event_id,
                        "receipt_epoch_id": receipt_epoch_id,
                        "receipt_content_sha256": receipt_content_sha256,
                        "receipt_content_length": receipt_content_length,
                    }
                )
    except Exception as exc:
        integrity = integrity or f"read_{type(exc).__name__}"
    return tuple(rows), integrity


def _autonomy_observer_reason(reason: Any) -> str:
    """Convert private reason text into a fixed public code."""

    if not isinstance(reason, str) or not reason:
        return "unspecified"
    prefix = reason.split(":", 1)[0].strip().lower()
    known = {
        "adapter_error",
        "adapter_malformed_return",
        "adapter_rejected",
        "adapter_unavailable",
        "effect_intent_error",
        "effect_pending_error",
        "effect_queue_error",
        "evidence_invalid",
        "missing_receipt",
        "provider_error",
        "receipt_mismatch",
    }
    return prefix if prefix in known else "failure"


def _autonomy_text(value: Any) -> str | None:
    if type(value) is str and value.strip():
        return value
    return None


def _autonomy_fact(
    *,
    key: str,
    code: str,
    state: str,
    target_date: date,
    event_time: datetime | None,
    refs: tuple[str, ...],
    counts: Mapping[str, int],
    recovery: RecoveryEvidence | None = None,
) -> ObservationFact:
    return ObservationFact(
        key=key,
        code=code,
        state=state,
        target_date=target_date,
        event_time=event_time,
        refs=refs,
        counts=dict(counts),
        recovery=recovery,
    )


def _autonomy_integrity(
    *, source: str, code: str, target_date: date
) -> ObservationFact:
    return _autonomy_fact(
        key=f"autonomy:integrity:{source}",
        code=f"autonomy_integrity_error:{code}",
        state="current",
        target_date=target_date,
        event_time=None,
        refs=(source,),
        counts={"integrity_errors": 1},
    )


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

        if type(target_date) is not date:
            raise TypeError("target_date must be a date")
        if (
            not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            raise ValueError("now must be timezone-aware")

        facts: list[ObservationFact] = []
        effect_path: Path | None = None
        ledger = self.effect_ledger
        ledger_file = getattr(getattr(ledger, "ledger", None), "path", None)
        if isinstance(ledger_file, Path):
            effect_path = ledger_file
        if effect_path is None:
            root = _state_root(self.controls)
            if isinstance(root, Path):
                effect_path = root / "effects.jsonl"

        effect_records: tuple[EffectRecord, ...] = ()
        effect_integrity: str | None = None
        if effect_path is not None:
            effect_records, effect_integrity = _read_effect_history_lock_free(
                effect_path
            )

        audit_path: Path | None = None
        audit_file = getattr(getattr(self.bus, "audit", None), "path", None)
        if isinstance(audit_file, Path):
            audit_path = audit_file
        audit_rows: tuple[dict[str, Any], ...] = ()
        audit_integrity: str | None = None
        if audit_path is not None:
            audit_rows, audit_integrity = _read_audit_rows_lock_free(audit_path)

        # A timeline entry contains only stable identifiers and bounded status
        # metadata.  The source payload itself is never retained in an entry.
        timelines: dict[tuple[str, str], list[dict[str, Any]]] = {}
        latest_effect: dict[tuple[str, str], dict[str, Any]] = {}
        effect_group_by_id: dict[str, tuple[str, str]] = {}
        effect_group_by_idempotency: dict[str, tuple[str, str]] = {}
        effect_groups_by_source: dict[str, set[tuple[str, str]]] = {}
        effect_group_by_receipt: dict[str, tuple[str, str]] = {}

        for record in effect_records:
            if record.kind != AUTONOMY_EFFECT_KIND:
                continue
            provider = self._record_provider(record)
            group = (record.kind, record.idempotency_key)
            effect_group_by_id[record.effect_id] = group
            effect_group_by_idempotency[record.idempotency_key] = group
            effect_groups_by_source.setdefault(record.source_event_id, set()).add(group)
            event_time = (
                record.receipt.observed_at
                if record.receipt is not None
                else record.created_at
            )
            entry = {
                "provider": provider,
                "effect_id": record.effect_id,
                "idempotency_key": record.idempotency_key,
                "source_event_id": record.source_event_id,
                "status": record.state,
                "event_time": event_time,
                "record": record,
                "receipt_id": (
                    None if record.receipt is None else record.receipt.receipt_id
                ),
            }
            timelines.setdefault(group, []).append(entry)
            latest_effect[group] = entry
            if record.receipt is not None:
                effect_group_by_receipt[record.receipt.receipt_id] = group
                effect_group_by_receipt[record.receipt.event_id] = group

        audit_only: dict[tuple[str, str], list[dict[str, Any]]] = {}
        audit_conflict = False
        canonical_provider_labels: dict[tuple[str, str], set[str]] = {}
        canonical_provider_conflicts: set[tuple[str, str]] = set()

        def audit_group(row: Mapping[str, Any]) -> tuple[str, str]:
            effect_id = _autonomy_text(row.get("effect_id"))
            idempotency_key = _autonomy_text(row.get("idempotency_key"))
            event_id = _autonomy_text(row.get("event_id")) or "unknown"
            if effect_id is not None:
                return ("effect", effect_id)
            if idempotency_key is not None:
                return ("idempotency", idempotency_key)
            return ("audit", event_id)

        def linked_group(
            row: Mapping[str, Any],
        ) -> tuple[tuple[str, str] | None, bool]:
            effect_id = _autonomy_text(row.get("effect_id"))
            idempotency_key = _autonomy_text(row.get("idempotency_key"))
            effect_group = (
                None if effect_id is None else effect_group_by_id.get(effect_id)
            )
            idempotency_group = (
                None
                if idempotency_key is None
                else effect_group_by_idempotency.get(idempotency_key)
            )
            source_event_id = _autonomy_text(row.get("source_event_id"))
            source_groups = (
                set()
                if source_event_id is None
                else effect_groups_by_source.get(source_event_id, set())
            )
            source_group = (
                next(iter(source_groups)) if len(source_groups) == 1 else None
            )
            receipt_groups = {
                effect_group_by_receipt[receipt]
                for receipt in (
                    _autonomy_text(row.get("receipt_id")),
                    _autonomy_text(row.get("receipt_event_id")),
                )
                if receipt is not None and receipt in effect_group_by_receipt
            }
            receipt_group = (
                next(iter(receipt_groups)) if len(receipt_groups) == 1 else None
            )
            candidates = {
                candidate
                for candidate in (
                    effect_group,
                    idempotency_group,
                    source_group,
                    receipt_group,
                )
                if candidate is not None
            }
            if len(candidates) > 1:
                return next(iter(candidates)), True
            if not candidates:
                return None, False
            candidate = next(iter(candidates))
            anchor_conflict = (
                effect_group is None
                and effect_id is not None
                and idempotency_group is not None
            ) or (
                idempotency_group is None
                and idempotency_key is not None
                and effect_group is not None
            )
            return candidate, anchor_conflict

        def audit_conflicts_with_record(
            row: Mapping[str, Any],
            group: tuple[str, str],
        ) -> bool:
            record_entry = latest_effect[group]
            record = record_entry["record"]
            if not isinstance(record, EffectRecord):
                return True
            checks = (
                ("effect_id", record.effect_id),
                ("idempotency_key", record.idempotency_key),
                ("source_event_id", record.source_event_id),
                ("provider", record_entry["provider"]),
            )
            for field_name, expected in checks:
                actual = _autonomy_text(row.get(field_name))
                if field_name == "provider" and expected is None:
                    continue
                if actual is not None and actual != expected:
                    return True

            receipt = record.receipt
            receipt_checks = (
                ("receipt_id", None if receipt is None else receipt.receipt_id),
                (
                    "receipt_event_id",
                    None if receipt is None else receipt.event_id,
                ),
                (
                    "receipt_epoch_id",
                    record.epoch_id,
                ),
                (
                    "receipt_content_sha256",
                    record.content_sha256,
                ),
                (
                    "receipt_content_length",
                    record.content_length,
                ),
            )
            for field_name, expected in receipt_checks:
                actual = row.get(field_name)
                if actual is not None and actual != expected:
                    return True

            status = _autonomy_text(row.get("status"))
            history_states = {
                item["record"].state
                for item in timelines[group]
                if isinstance(item.get("record"), EffectRecord)
            }
            if status == "completed":
                return record.state != "verified" or not isinstance(
                    record.receipt, EffectReceipt
                )
            if status == "failed":
                return record.state != "failed"
            if status == "executed_unverified":
                return "executed_unverified" not in history_states
            if status == "awaiting_reconciliation":
                return not history_states.intersection(
                    {"intent", "pending", "executed_unverified", "expired", "requeued"}
                )
            return status == "skipped"

        def exact_linked_provider(
            row: Mapping[str, Any], group: tuple[str, str]
        ) -> str | None:
            provider = _autonomy_text(row.get("provider"))
            record = latest_effect[group]["record"]
            if (
                provider is None
                or not isinstance(record, EffectRecord)
                or record.state != "verified"
                or not isinstance(record.receipt, EffectReceipt)
                or _autonomy_text(row.get("status")) != "completed"
            ):
                return None
            if any(
                _autonomy_text(row.get(field_name)) != expected
                for field_name, expected in (
                    ("effect_id", record.effect_id),
                    ("source_event_id", record.source_event_id),
                    ("idempotency_key", record.idempotency_key),
                    ("receipt_id", record.receipt.receipt_id),
                )
            ):
                return None
            return provider

        for row in audit_rows:
            if row.get("kind") != "audit.autonomy":
                continue
            status = _autonomy_text(row.get("status"))
            if status == "skipped":
                continue
            group, link_conflict = linked_group(row)
            if group is not None:
                row_conflict = link_conflict or audit_conflicts_with_record(row, group)
                if row_conflict:
                    audit_conflict = True
                else:
                    provider = exact_linked_provider(row, group)
                    if provider is not None:
                        labels = canonical_provider_labels.setdefault(group, set())
                        labels.add(provider)
                        if len(labels) > 1:
                            canonical_provider_conflicts.add(group)
                            audit_conflict = True
                continue
            audit_only.setdefault(audit_group(row), []).append(row)

        canonical_bad_states = {
            "intent",
            "pending",
            "executed_unverified",
            "expired",
            "requeued",
            "failed",
        }
        for group, timeline in timelines.items():
            selected = latest_effect[group]
            record = selected["record"]
            provider = _autonomy_text(selected.get("provider"))
            if not isinstance(record, EffectRecord):
                continue
            provider_labels = canonical_provider_labels.get(group, set())
            if (
                provider is None
                and len(provider_labels) == 1
                and group not in canonical_provider_conflicts
            ):
                provider = next(iter(provider_labels))
            effect_id = _autonomy_text(record.effect_id)
            source_event_id = _autonomy_text(record.source_event_id)
            idempotency_key = _autonomy_text(record.idempotency_key)
            event_time = selected.get("event_time")
            if not isinstance(event_time, datetime):
                event_time = now

            status = record.state
            if status in {"pending", "executed_unverified"} and record.expires_at < now:
                status = "expired"
                event_time = record.expires_at
            if status == "verified":
                if not isinstance(record.receipt, EffectReceipt):
                    facts.append(
                        _autonomy_integrity(
                            source=effect_path.name
                            if effect_path is not None
                            else "effects.jsonl",
                            code="verified_effect_missing_receipt",
                            target_date=target_date,
                        )
                    )
                    continue
                public_status = "completed"
            else:
                if provider is None:
                    facts.append(
                        _autonomy_integrity(
                            source=effect_path.name
                            if effect_path is not None
                            else "effects.jsonl",
                            code="provider_unavailable",
                            target_date=target_date,
                        )
                    )
                    continue
                public_status = status

            recovered = public_status == "completed" and any(
                isinstance(item.get("record"), EffectRecord)
                and item["record"].state in canonical_bad_states
                for item in timeline
                if item is not selected
            )
            if public_status == "completed":
                code = "autonomy_verified"
                fact_state = "recovered_history" if recovered else "neutral"
            elif public_status == "failed":
                reason_code = _autonomy_observer_reason(record.reason)
                code = f"autonomy_provider_failure:{reason_code}"
                fact_state = "current"
            elif public_status == "executed_unverified":
                code = "autonomy_executed_unverified"
                fact_state = "current"
            elif public_status in {
                "intent",
                "pending",
                "awaiting_reconciliation",
                "expired",
                "requeued",
            }:
                code = "autonomy_awaiting_reconciliation"
                fact_state = "current"
            else:
                continue

            refs: list[str] = []
            if provider is not None:
                refs.append(f"provider:{provider}")
            if effect_id is not None:
                refs.append(f"effect:{effect_id}")
            if source_event_id is not None:
                refs.append(f"source:{source_event_id}")
            if idempotency_key is not None:
                refs.append(f"idempotency:{idempotency_key}")
            refs.append(f"sha256:{record.content_sha256}")
            receipt_id = None if record.receipt is None else record.receipt.receipt_id
            if receipt_id is not None:
                refs.append(f"receipt:{receipt_id}")

            recovery: RecoveryEvidence | None = None
            if fact_state == "recovered_history":
                recovery = RecoveryEvidence(
                    ref=receipt_id,
                    code="autonomy_verified",
                    recovered_at=event_time,
                )
            counts = {
                "telemetry": 1,
                "attempt": record.attempt,
                "content_length": record.content_length,
            }
            facts.append(
                _autonomy_fact(
                    key=(
                        f"autonomy:{provider or 'unknown'}:effect:"
                        f"{effect_id or idempotency_key or 'unknown'}"
                    ),
                    code=code,
                    state=fact_state,
                    target_date=target_date,
                    event_time=event_time,
                    refs=tuple(refs),
                    counts=counts,
                    recovery=recovery,
                )
            )

        for timeline in audit_only.values():
            selected = max(
                enumerate(timeline),
                key=lambda item: (item[1].get("event_time") or now, item[0]),
            )[1]
            status = _autonomy_text(selected.get("status"))
            if status == "skipped":
                continue
            provider = _autonomy_text(selected.get("provider"))
            effect_id = _autonomy_text(selected.get("effect_id"))
            source_event_id = _autonomy_text(selected.get("source_event_id"))
            idempotency_key = _autonomy_text(selected.get("idempotency_key"))
            event_id = _autonomy_text(selected.get("event_id"))
            event_time = selected.get("event_time")
            if not isinstance(event_time, datetime):
                event_time = now
            if status == "completed":
                code = "autonomy_completion_unverified"
            elif status == "failed":
                code = "autonomy_provider_failure:failure"
            elif status == "executed_unverified":
                code = "autonomy_executed_unverified"
            elif status == "awaiting_reconciliation":
                code = "autonomy_awaiting_reconciliation"
            else:
                continue
            refs: list[str] = []
            if provider is not None:
                refs.append(f"provider:{provider}")
            if effect_id is not None:
                refs.append(f"effect:{effect_id}")
            if source_event_id is not None:
                refs.append(f"source:{source_event_id}")
            if idempotency_key is not None:
                refs.append(f"idempotency:{idempotency_key}")
            suffix = effect_id or idempotency_key or event_id or "global"
            scope = provider or "global"
            facts.append(
                _autonomy_fact(
                    key=f"autonomy:{scope}:effect:{suffix}",
                    code=code,
                    state="current",
                    target_date=target_date,
                    event_time=event_time,
                    refs=tuple(refs),
                    counts={"telemetry": 1},
                )
            )

        if audit_conflict:
            facts.append(
                _autonomy_integrity(
                    source=(
                        f"{audit_path.name}:projection"
                        if audit_path is not None
                        else "audit.jsonl:projection"
                    ),
                    code="audit_effect_conflict",
                    target_date=target_date,
                )
            )

        if effect_integrity is not None:
            facts.append(
                _autonomy_integrity(
                    source=effect_path.name
                    if effect_path is not None
                    else "effects.jsonl",
                    code=effect_integrity,
                    target_date=target_date,
                )
            )
        if audit_integrity is not None:
            facts.append(
                _autonomy_integrity(
                    source=audit_path.name if audit_path is not None else "audit.jsonl",
                    code=audit_integrity,
                    target_date=target_date,
                )
            )

        selected_facts: dict[str, ObservationFact] = {}
        rank = {"neutral": 0, "recovered_history": 1, "current": 2}
        for fact in facts:
            previous = selected_facts.get(fact.key)
            if previous is None or rank[fact.state] > rank[previous.state]:
                selected_facts[fact.key] = fact
        return tuple(sorted(selected_facts.values(), key=lambda fact: fact.key))

    def _finish(
        self,
        result: ActivityResult,
        gate: GateResult,
    ) -> ActivityResult:
        terminal = self._canonical_terminal(result)
        occurrence_id = (
            result.source_event_id or result.canonical_event_id or result.run_id
        )
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
    ) -> ActivityResult | None:
        finder = getattr(self.bus, "find_audit_terminal", None)
        if not callable(finder):
            return None
        event = finder("autonomy", occurrence_id)
        if event is None:
            return None
        payload = event.payload
        terminal = payload.get("terminal")
        if type(terminal) is not str or not terminal.strip():
            raise RuntimeError("autonomy terminal audit is invalid")
        # EffectLedger remains truth if an effect exists for this exact
        # occurrence.  An audit projection cannot mask a conflicting effect.
        if epoch_id is not None:
            matching = [
                record
                for record in self.effect_ledger.records()
                if self._record_value(record, "kind") == AUTONOMY_EFFECT_KIND
                and self._record_value(record, "source_event_id") == occurrence_id
                and self._record_value(record, "epoch_id") == epoch_id
            ]
            if len(matching) > 1:
                raise RuntimeError("autonomy occurrence conflict")
            if matching:
                state = self._record_state(matching[0])
                effect_terminal = (
                    "verified"
                    if state == "verified"
                    else "failed"
                    if state == "failed"
                    else None
                )
                if (
                    effect_terminal == "failed"
                    and self._record_value(matching[0], "reason")
                    == "effect_expired_unverified"
                ):
                    effect_terminal = "expired"
                if effect_terminal is not None and effect_terminal != terminal:
                    raise RuntimeError("audit effect conflict")
                if effect_terminal is not None:
                    return None
                raise RuntimeError("audit effect conflict")
        status = payload.get("status")
        result_status = (
            "completed"
            if status == "completed"
            else "failed"
            if status == "failed"
            else "skipped"
        )
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

    def _provider_history(self, name: str) -> list[Any]:
        rows = [
            record
            for record in self._audit_history()
            if self._record_provider(record) == name
        ]
        latest_by_effect: dict[str, Any] = {}
        without_effect: list[Any] = []
        for record in rows:
            effect_id = self._record_value(record, "effect_id")
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
            history = self._provider_history(provider.name)
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
    ) -> ActivityResult | None:
        state = self._record_state(record)
        effect_id = self._record_value(record, "effect_id")
        source_event_id = self._record_value(record, "source_event_id")
        idempotency_key = self._record_value(record, "idempotency_key")
        evidence = self._record_evidence(record)
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
                return self._finish(
                    ActivityResult("skipped", None, "execution_in_progress"), gate
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
        try:
            source_override, epoch_override, idempotency_override = (
                self._identity_overrides(context.facts)
            )
        except ValueError as exc:
            return self._finish(ActivityResult("failed", None, str(exc)), gate)
        run_id = new_id("autonomy_run")
        source_event_id = source_override or run_id
        epoch_id = epoch_override or f"autonomy:{context.now.date().isoformat()}"
        existing_terminal = self._existing_terminal_result(
            source_event_id, epoch_id=epoch_id
        )
        if existing_terminal is not None:
            return existing_terminal

        def finish(
            result: ActivityResult, _gate: GateResult | None = None
        ) -> ActivityResult:
            if result.source_event_id is None:
                result = replace(
                    result,
                    run_id=result.run_id or run_id,
                    source_event_id=source_event_id,
                    canonical_event_id=source_event_id,
                )
            return self._finish(result, gate)

        self._settle_expired_unverified(now=now, gate=gate)
        if not gate.allowed:
            return finish(ActivityResult("skipped", None, gate.reason))

        # Active-chat is a hard gate.  It is checked before Judge and before
        # any autonomy event/effect intent is emitted.  A present non-bool is
        # malformed control input, not an invitation to guess.
        for chat_key in ("active_chat", "chat_active"):
            if chat_key in context.facts and type(context.facts[chat_key]) is not bool:
                return finish(
                    ActivityResult("failed", None, f"{chat_key}_invalid"), gate
                )
            if context.facts.get(chat_key) is True:
                return finish(ActivityResult("skipped", None, "active_chat"))

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
            if existing_selection is None and source_override is not None:
                existing_selection = self._find_by_occurrence(source_event_id, epoch_id)
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
                epoch_id = recorded_epoch
        except ValueError:
            return finish(ActivityResult("failed", None, "occurrence_conflict"), gate)
        except Exception as exc:
            return finish(
                ActivityResult(
                    "failed", None, f"effect_lookup_error:{type(exc).__name__}"
                ),
                gate,
            )

        selected: str | None = None
        selection_reason = "selected"
        if existing_selection is not None:
            selected = self._record_provider(existing_selection)
            reconciled = self._existing_result(
                existing_selection,
                provider=selected or "unknown",
                gate=gate,
                run_id=self._record_value(existing_selection, "effect_id"),
            )
            if reconciled is not None:
                return reconciled
            if selected is None:
                return finish(
                    ActivityResult(
                        "awaiting_reconciliation",
                        None,
                        "selection_provider_unavailable",
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

            occurrence_identity = idempotency_override or json.dumps(
                {"epoch_id": epoch_id, "source_event_id": source_event_id},
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
                selected, source_event_id, epoch_id
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
                    epoch_id=epoch_id,
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
                record, provider=selected, gate=gate, run_id=run_id
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
                    "epoch_id": epoch_id,
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
            epoch_id=epoch_id,
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
