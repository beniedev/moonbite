"""Receipt-backed, fail-closed Heartbeat policy and cadence state."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    StateError,
    atomic_json_write,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)
from .session import SessionHookReceipt

CADENCE_SCHEMA_V1 = "moon.heartbeat.cadence.v1"
CADENCE_SCHEMA_V2 = "moon.heartbeat.cadence.v2"
CADENCE_SCHEMA_V3 = "moon.heartbeat.cadence.v3"
HEARTBEAT_CADENCE_SCHEMA = CADENCE_SCHEMA_V3
CADENCE_SCHEMA = HEARTBEAT_CADENCE_SCHEMA
_PRIVATE_CONTACT_MAX = 128
_VISIBLE_CONTACT_MAX = 128
_EFFECT_TERMINAL_MAX = 256
_EFFECT_REF_MAX = 256
_SILENCE_BACKOFF_RECEIPT_MAX = 256
_SILENCE_BACKOFF_RECEIPT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
DEFAULT_JUDGE_INTERVAL = timedelta(hours=1)
DEFAULT_AUTOMATIC_COOLDOWN = timedelta(hours=1)
DEFAULT_MANUAL_COOLDOWN = timedelta(hours=1)
DEFAULT_RECENT_CONTACT_WINDOW = timedelta(minutes=30)
DEFAULT_EFFECT_TTL = timedelta(hours=1)
DEFAULT_ANCHOR_HOUR = 6
HEARTBEAT_BYPASSES = frozenset(
    {"automatic_cooldown", "manual_snooze", "recent_contact", "active_chat"}
)
HEARTBEAT_PROFILES = frozenset({"routine", "daily_anchor", "urgent", "maintenance"})
HEARTBEAT_JUDGE_TERMINALS = frozenset(
    {"approved", "denied", "failed", "maintenance", "unknown"}
)
HEARTBEAT_WAKE_TERMINALS = frozenset(
    {"verified", "unverified", "failed", "maintenance", "not_requested", "unknown"}
)
HEARTBEAT_DELIVERY_TERMINALS = frozenset(
    {"verified", "unverified", "failed", "not_requested", "unknown"}
)
_HEARTBEAT_KIND_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


class HeartbeatReasonCode(StrEnum):
    NO_EVENT = "no_event"
    NOT_DUE = "not_due"
    COOLDOWN = "cooldown"
    RECENT_CONTACT = "recent_contact"
    ACTIVE_CHAT = "active_chat"
    ALLOWED = "allowed"
    DENIED = "denied"
    EXECUTION_LOCK = "execution_lock"
    CONTROL = "control"
    CANDIDATE_INVALID = "candidate_invalid"
    JUDGE_ERROR = "judge_error"
    JUDGE_MALFORMED = "judge_malformed"
    CADENCE_ERROR = "cadence_error"
    EFFECT_ERROR = "effect_error"
    EFFECT_REPLAY_ERROR = "effect_replay_error"
    EFFECT_PENDING = "effect_pending"
    EFFECT_EXPIRED = "effect_expired"
    ADAPTER_UNAVAILABLE = "adapter_unavailable"
    ADAPTER_REJECTED = "adapter_rejected"
    ADAPTER_ERROR = "adapter_error"
    ADAPTER_MALFORMED = "adapter_malformed"


@dataclass(frozen=True, slots=True)
class HeartbeatKindPolicy:
    """The normalized, host-owned policy for one heartbeat kind."""

    enabled: bool
    profile: str
    judge: str
    host_only: bool
    bypass: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("heartbeat kind enabled must be boolean")
        if type(self.profile) is not str or not self.profile.strip():
            raise ValueError("heartbeat kind profile must be non-empty")
        if self.profile not in {
            "routine",
            "daily_anchor",
            "urgent",
            "maintenance",
        }:
            raise ValueError("heartbeat kind profile is unsupported")
        if self.judge not in {"required", "skip"}:
            raise ValueError("heartbeat kind judge must be required or skip")
        if type(self.host_only) is not bool:
            raise ValueError("heartbeat kind host_only must be boolean")
        if type(self.bypass) is not frozenset or not self.bypass <= HEARTBEAT_BYPASSES:
            raise ValueError("heartbeat kind bypass contains an unsupported value")
        if self.profile == "routine" and (self.judge != "required" or self.bypass):
            raise ValueError("routine heartbeat policy is fixed")
        if self.profile == "daily_anchor" and (
            not self.host_only
            or self.judge != "required"
            or not self.bypass <= {"automatic_cooldown"}
        ):
            raise ValueError("daily_anchor heartbeat policy is fixed")
        if self.profile == "urgent" and (
            not self.host_only or self.judge != "required"
        ):
            raise ValueError("urgent heartbeat policy is fixed")
        if self.profile == "maintenance" and (not self.host_only or self.bypass):
            raise ValueError("maintenance heartbeat policy is fixed")

    @property
    def maintenance_skip(self) -> bool:
        return self.profile == "maintenance" and self.judge == "skip"


@dataclass(frozen=True, slots=True)
class HeartbeatSilenceReceipt:
    """Content-free settlement evidence for automatic silence backoff."""

    receipt_id: str
    completed_at: datetime
    profile: str
    settled: bool
    intentional_silence: bool
    judge_terminal: str
    wake_terminal: str
    delivery_terminal: str
    manual_override: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.receipt_id) is not str
            or _SILENCE_BACKOFF_RECEIPT_ID.fullmatch(self.receipt_id) is None
        ):
            raise ValueError("silence receipt_id is invalid")
        _aware(self.completed_at, "silence completed_at")
        if self.profile not in HEARTBEAT_PROFILES:
            raise ValueError("silence profile is unsupported")
        for label, value in (
            ("settled", self.settled),
            ("intentional_silence", self.intentional_silence),
            ("manual_override", self.manual_override),
        ):
            if type(value) is not bool:
                raise ValueError(f"silence {label} must be boolean")
        if self.judge_terminal not in HEARTBEAT_JUDGE_TERMINALS:
            raise ValueError("silence judge terminal is unsupported")
        if self.wake_terminal not in HEARTBEAT_WAKE_TERMINALS:
            raise ValueError("silence wake terminal is unsupported")
        if self.delivery_terminal not in HEARTBEAT_DELIVERY_TERMINALS:
            raise ValueError("silence delivery terminal is unsupported")

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "completed_at": isoformat(self.completed_at),
            "profile": self.profile,
            "settled": self.settled,
            "intentional_silence": self.intentional_silence,
            "judge_terminal": self.judge_terminal,
            "wake_terminal": self.wake_terminal,
            "delivery_terminal": self.delivery_terminal,
            "manual_override": self.manual_override,
        }


class _CandidateInvalidError(ValueError):
    """A configured candidate cannot safely be evaluated."""


def _aware(value: datetime, label: str = "timestamp") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    if value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value


def _optional_time(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _aware(value, label)
    if type(value) is not str:
        raise ValueError(f"{label} must be an ISO timestamp or null")
    try:
        return parse_time(value)
    except (StateError, ValueError) as exc:
        raise ValueError(f"{label} must be an ISO timestamp") from exc


def _strict_iso_date(value: Any, label: str) -> date:
    """Decode only the canonical ``YYYY-MM-DD`` representation."""

    if type(value) is not str or len(value) != 10:
        raise ValueError(f"{label} must be a strict ISO date")
    try:
        decoded = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a strict ISO date") from exc
    if decoded.isoformat() != value:
        raise ValueError(f"{label} must be a strict ISO date")
    return decoded


def _json_time(value: datetime | None) -> str | None:
    return None if value is None else isoformat(value)


def _decode_time(value: Any, label: str) -> str | None:
    return _json_time(_optional_time(value, label))


_CADENCE_OBSERVER_FIELDS = frozenset(
    {
        "schema_version",
        "auto_until",
        "manual_until",
        "automatic_cooldown_until",
        "manual_cooldown_until",
        "last_judge_at",
        "next_judge_at",
        "last_effect_at",
        "daily_anchor_epoch",
        "daily_anchor_completed",
        "private_contacts",
        "verified_visible_contacts",
        "private_contact_overflow_until",
        "verified_visible_overflow_until",
        "effect_terminals",
        "effect_refs",
        "private_contact_bloom",
        "verified_visible_bloom",
        "last_private_contact_at",
        "last_verified_visible_contact_at",
        "silence_backoff_processed_receipts",
        "silence_backoff_streak",
        "silence_backoff_last_completed_at",
    }
)
_OBSERVER_STATE_RANK = {"neutral": 0, "recovered_history": 1, "current": 2}


def _read_cadence_state_lock_free(
    path: Path,
) -> tuple[Mapping[str, Any] | None, str | None]:
    """Read an existing cadence file without lock creation or migration."""

    if not path.exists():
        return None, None
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except Exception as exc:
        return None, f"read_{type(exc).__name__}"
    if not isinstance(raw, Mapping):
        return None, "state_type"
    if raw.get("schema_version") not in {
        None,
        CADENCE_SCHEMA_V1,
        CADENCE_SCHEMA_V2,
        CADENCE_SCHEMA_V3,
    }:
        return None, "unsupported_schema"
    if set(raw) - _CADENCE_OBSERVER_FIELDS:
        return None, "unsupported_fields"
    return raw, None


def _observer_time(raw: Any, label: str) -> datetime | None:
    if raw is None:
        return None
    return _optional_time(raw, label)


def _observer_fact(
    *,
    key: str,
    code: str,
    state: str,
    target_date: date,
    event_time: datetime | None = None,
    refs: tuple[str, ...] = (),
    counts: Mapping[str, int] | None = None,
    recovery: RecoveryEvidence | None = None,
) -> ObservationFact:
    return ObservationFact(
        key=key,
        code=code,
        state=state,
        target_date=target_date,
        event_time=event_time,
        refs=refs,
        counts={} if counts is None else dict(counts),
        recovery=recovery,
    )


def _observer_integrity(
    *, source: str, code: str, target_date: date
) -> ObservationFact:
    return _observer_fact(
        key=f"heartbeat:integrity:{source}",
        code=f"heartbeat_integrity_error:{code}",
        state="current",
        target_date=target_date,
        refs=(source,),
        counts={"integrity_errors": 1},
    )


def _dedupe_observer_facts(facts: list[ObservationFact]) -> tuple[ObservationFact, ...]:
    selected: dict[str, ObservationFact] = {}
    for fact in facts:
        previous = selected.get(fact.key)
        if previous is None:
            selected[fact.key] = fact
            continue
        rank = _OBSERVER_STATE_RANK[fact.state]
        previous_rank = _OBSERVER_STATE_RANK[previous.state]
        if rank > previous_rank or (
            rank == previous_rank and fact.code < previous.code
        ):
            selected[fact.key] = fact
    return tuple(sorted(selected.values(), key=lambda item: item.key))


def _heartbeat_effect_fact(
    record: EffectRecord,
    *,
    history: tuple[EffectRecord, ...],
    target_date: date,
    now: datetime,
) -> ObservationFact:
    state = record.state
    event_time = record.created_at
    if state == "verified" and record.receipt is not None:
        event_time = record.receipt.observed_at
    elif state == "expired":
        event_time = record.expires_at
    elif state in {"pending", "executed_unverified"} and record.expires_at < now:
        state = "expired"
        event_time = record.expires_at

    state_codes = {
        "intent": "intent",
        "pending": "pending",
        "executed_unverified": "executed_unverified",
        "expired": "expired",
        "failed": "failed",
        "requeued": "requeued",
        "verified": "verified",
    }
    safe_state = state_codes.get(state, "integrity")
    fact_state = "neutral" if state == "verified" else "current"
    recovery: RecoveryEvidence | None = None
    if state == "verified":
        previous_states = {item.state for item in history[:-1]}
        if (
            previous_states
            & {
                "pending",
                "executed_unverified",
                "expired",
                "failed",
                "requeued",
            }
            and record.receipt is not None
        ):
            fact_state = "recovered_history"
            recovery = RecoveryEvidence(
                ref=record.receipt.receipt_id,
                code="heartbeat_delivery_verified",
                recovered_at=record.receipt.observed_at,
            )

    refs = [
        f"effect:{record.effect_id}",
        f"kind:{record.kind}",
        f"source:{record.source_event_id}",
        f"idempotency:{record.idempotency_key}",
        f"sha256:{record.content_sha256}",
    ]
    if record.receipt is not None:
        refs.append(f"receipt:{record.receipt.receipt_id}")
    return _observer_fact(
        key=f"heartbeat:effect:{record.kind.removeprefix('heartbeat_')}:{record.effect_id}",
        code=f"heartbeat_effect_{safe_state}",
        state=fact_state,
        target_date=target_date,
        event_time=event_time,
        refs=tuple(refs),
        counts={
            "effects": 1,
            "attempt": record.attempt,
            "content_length": record.content_length,
            f"state_{safe_state}": 1,
        },
        recovery=recovery,
    )


@dataclass(frozen=True)
class HeartbeatCandidate:
    kind: str
    context: Mapping[str, Any] = field(default_factory=dict)
    candidate_id: str = ""
    session_receipt: SessionHookReceipt | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not str or not self.kind.strip():
            raise ValueError("heartbeat candidate kind must be non-empty")
        if not isinstance(self.context, Mapping):
            raise TypeError("heartbeat candidate context must be a mapping")
        if type(self.candidate_id) is not str:
            raise TypeError("heartbeat candidate_id must be a string")
        if self.session_receipt is not None and not isinstance(
            self.session_receipt, SessionHookReceipt
        ):
            raise TypeError("session_receipt must be a SessionHookReceipt")


@dataclass(frozen=True)
class JudgeDecision:
    wake_main: bool
    dm_user: bool
    reason: str
    message: str = ""
    allow_autonomy: bool | None = None
    maintenance: bool | None = None
    next_judge_at: datetime | str | None = None
    cadence_minutes: int | None = None
    delivery_mode: str = "direct"

    def __post_init__(self) -> None:
        if type(self.delivery_mode) is not str or self.delivery_mode not in {
            "direct",
            "delegated",
        }:
            raise ValueError("delivery_mode must be direct or delegated")
        if self.dm_user and (type(self.message) is not str or not self.message.strip()):
            raise ValueError("direct or delegated delivery requires text")
        if self.delivery_mode == "delegated" and not self.wake_main:
            raise ValueError("delegated delivery requires wake_main")

    @property
    def wake(self) -> bool:
        return self.wake_main

    @property
    def direct_message(self) -> bool:
        return self.dm_user

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "wake_main": self.wake_main,
            "dm_user": self.dm_user,
            "reason": self.reason,
            "message": self.message,
            "delivery_mode": self.delivery_mode,
        }
        if self.allow_autonomy is not None:
            value["allow_autonomy"] = self.allow_autonomy
        if self.maintenance is not None:
            value["maintenance"] = self.maintenance
        if self.next_judge_at is not None:
            value["next_judge_at"] = (
                isoformat(self.next_judge_at)
                if isinstance(self.next_judge_at, datetime)
                else self.next_judge_at
            )
        if self.cadence_minutes is not None:
            value["cadence_minutes"] = self.cadence_minutes
        return value


@dataclass(frozen=True)
class EffectResult:
    ok: bool
    status: str
    receipt: EffectReceipt | None = None
    verified: bool = False
    effect_id: str | None = None
    terminal: str | None = None
    reason_code: HeartbeatReasonCode | None = None
    degraded: bool = False
    projection_errors: tuple[str, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.ok

    @property
    def evidence(self) -> dict[str, Any]:
        if self.receipt is None:
            return {
                "verified": False,
                "kind": "delivery_receipt",
                "receipt_id": None,
                "event_id": None,
                "observed_at": None,
                "content_sha256": None,
                "content_length": None,
                "epoch_id": None,
            }
        return {"verified": True, **self.receipt.to_dict()}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "receipt": None if self.receipt is None else self.receipt.to_dict(),
            "verified": self.verified,
            "effect_id": self.effect_id,
            "terminal": self.terminal,
            "reason_code": None if self.reason_code is None else self.reason_code.value,
            "degraded": self.degraded,
            "projection_errors": list(self.projection_errors),
        }


@dataclass(frozen=True)
class HeartbeatResult:
    status: str
    reason: str
    candidate_id: str
    gate: GateResult
    decision: JudgeDecision | None = None
    delivery: EffectResult | None = None
    wake: EffectResult | None = None
    reason_code: HeartbeatReasonCode | None = None
    next_judge_at: datetime | None = None
    snapshot: Mapping[str, Any] | None = None
    degraded: bool = False
    projection_errors: tuple[str, ...] = ()

    @property
    def code(self) -> HeartbeatReasonCode | None:
        return self.reason_code

    @property
    def effects(self) -> tuple[EffectResult, ...]:
        return tuple(x for x in (self.delivery, self.wake) if x is not None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "reason_code": None if self.reason_code is None else self.reason_code.value,
            "candidate_id": self.candidate_id,
            "gate": self.gate.__dict__,
            "decision": None if self.decision is None else self.decision.to_dict(),
            "delivery": None if self.delivery is None else self.delivery.to_dict(),
            "wake": None if self.wake is None else self.wake.to_dict(),
            "next_judge_at": _json_time(self.next_judge_at),
            "snapshot": None if self.snapshot is None else dict(self.snapshot),
            "degraded": self.degraded,
            "projection_errors": list(self.projection_errors),
        }


class Judge(Protocol):
    def decide(self, candidate: HeartbeatCandidate) -> Any: ...


class WakeSink(Protocol):
    def deliver(
        self,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        intent: Any | None = None,
    ) -> Any: ...
    def wake(
        self,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        intent: Any | None = None,
    ) -> Any: ...


class SilentJudge:
    def decide(self, candidate: HeartbeatCandidate) -> JudgeDecision:
        return JudgeDecision(False, False, "judge_adapter_not_configured")


class NoopWakeSink:
    def deliver(
        self,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        intent: Any | None = None,
    ) -> EffectResult:
        return EffectResult(False, "direct_message_adapter_unavailable")

    def wake(
        self,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        intent: Any | None = None,
    ) -> EffectResult:
        return EffectResult(False, "targeted_wake_adapter_unavailable")


def _kind_policy_from_descriptor(
    kind: str, descriptor: Mapping[str, Any]
) -> HeartbeatKindPolicy:
    required = {"enabled", "profile", "judge", "host_only", "bypass"}
    if set(descriptor) != required:
        raise _CandidateInvalidError(
            f"heartbeat kind {kind} descriptor keys are invalid"
        )
    raw_bypass = descriptor["bypass"]
    if type(raw_bypass) is not list:
        raise _CandidateInvalidError(f"heartbeat kind {kind} bypass is malformed")
    bypass = tuple(raw_bypass)
    if any(type(value) is not str for value in bypass):
        raise _CandidateInvalidError(f"heartbeat kind {kind} bypass is malformed")
    if len(set(bypass)) != len(bypass):
        raise _CandidateInvalidError(f"heartbeat kind {kind} bypass is malformed")
    try:
        return HeartbeatKindPolicy(
            enabled=descriptor["enabled"],
            profile=descriptor["profile"],
            judge=descriptor["judge"],
            host_only=descriptor["host_only"],
            bypass=frozenset(bypass),
        )
    except (TypeError, ValueError) as exc:
        raise _CandidateInvalidError(
            f"heartbeat kind {kind} policy is malformed"
        ) from exc


def _compact_contacts(values: Mapping[str, str], limit: int) -> dict[str, str]:
    ordered = sorted(
        values.items(),
        key=lambda item: _optional_time(item[1], "contact timestamp") or datetime.min,
        reverse=True,
    )
    return dict(ordered[:limit])


def _compact_tail(values: Mapping[str, str], limit: int) -> dict[str, str]:
    return dict(list(values.items())[-limit:])


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": CADENCE_SCHEMA_V3,
        "last_judge_at": None,
        "next_judge_at": None,
        "manual_cooldown_until": None,
        "automatic_cooldown_until": None,
        "last_effect_at": None,
        "daily_anchor_epoch": None,
        "daily_anchor_completed": False,
        "private_contacts": {},
        "verified_visible_contacts": {},
        "private_contact_overflow_until": None,
        "verified_visible_overflow_until": None,
        "effect_terminals": {},
        "effect_refs": {},
        "last_private_contact_at": None,
        "last_verified_visible_contact_at": None,
        "silence_backoff_processed_receipts": {},
        "silence_backoff_streak": 0,
        "silence_backoff_last_completed_at": None,
    }


def _normalise_silence_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    required = {"enabled", "first_minutes", "repeat_minutes", "max_minutes"}
    if not isinstance(policy, Mapping) or set(policy) != required:
        raise ValueError("heartbeat silence_backoff policy is malformed")
    if type(policy["enabled"]) is not bool:
        raise ValueError("heartbeat silence_backoff.enabled must be boolean")
    values: dict[str, int] = {}
    for key in ("first_minutes", "repeat_minutes", "max_minutes"):
        value = policy[key]
        if type(value) is not int or not 1 <= value <= 1440:
            raise ValueError(f"heartbeat silence_backoff.{key} is out of bounds")
        values[key] = value
    if values["first_minutes"] > values["repeat_minutes"]:
        raise ValueError(
            "heartbeat silence_backoff.first_minutes must be <= repeat_minutes"
        )
    if values["repeat_minutes"] > values["max_minutes"]:
        raise ValueError(
            "heartbeat silence_backoff.repeat_minutes must be <= max_minutes"
        )
    return {"enabled": policy["enabled"], **values}


class HeartbeatCadence:
    """Durable cadence/contact state with non-writing constructor migration."""

    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], datetime] = utc_now,
        judge_interval: timedelta | int | float = DEFAULT_JUDGE_INTERVAL,
        automatic_cooldown: timedelta | int | float = DEFAULT_AUTOMATIC_COOLDOWN,
        manual_cooldown: timedelta | int | float = DEFAULT_MANUAL_COOLDOWN,
        recent_contact_window: timedelta | int | float = DEFAULT_RECENT_CONTACT_WINDOW,
        effect_ttl: timedelta | int | float = DEFAULT_EFFECT_TTL,
        anchor_hour: int = DEFAULT_ANCHOR_HOUR,
        timezone_name: str = "UTC",
        **kwargs: Any,
    ):
        minute_options = {
            "judge_interval_minutes": "judge_interval",
            "automatic_cooldown_minutes": "automatic_cooldown",
            "manual_cooldown_minutes": "manual_cooldown",
            "recent_contact_minutes": "recent_contact_window",
        }
        for option, field_name in minute_options.items():
            if option in kwargs:
                value = timedelta(minutes=kwargs.pop(option))
                if field_name == "judge_interval":
                    judge_interval = value
                elif field_name == "automatic_cooldown":
                    automatic_cooldown = value
                elif field_name == "manual_cooldown":
                    manual_cooldown = value
                else:
                    recent_contact_window = value
        if "anchor" in kwargs:
            anchor_hour = kwargs.pop("anchor")
        if kwargs:
            raise TypeError(
                f"unknown HeartbeatCadence option(s): {', '.join(sorted(kwargs))}"
            )
        self.path = Path(root) / "heartbeat_cadence.json"
        self.lock_path = Path(root) / "heartbeat_cadence.lock"
        self.clock = clock
        self.judge_interval = self._duration(judge_interval, "judge_interval")
        self.automatic_cooldown = self._duration(
            automatic_cooldown, "automatic_cooldown"
        )
        self.manual_cooldown = self._duration(manual_cooldown, "manual_cooldown")
        self.recent_contact_window = self._duration(
            recent_contact_window, "recent_contact_window"
        )
        self.effect_ttl = self._duration(effect_ttl, "effect_ttl")
        if type(anchor_hour) is not int or not 0 <= anchor_hour <= 23:
            raise ValueError("anchor_hour must be from 0 to 23")
        self.anchor_hour = anchor_hour
        try:
            self.timezone = ZoneInfo(timezone_name)
        except (TypeError, ZoneInfoNotFoundError) as exc:
            raise ValueError(
                "timezone_name must identify an installed timezone"
            ) from exc
        self.timezone_name = timezone_name

    @staticmethod
    def _duration(value: timedelta | int | float, label: str) -> timedelta:
        if isinstance(value, timedelta):
            result = value
        elif type(value) in {int, float}:
            result = timedelta(minutes=float(value))
        else:
            raise TypeError(f"{label} must be a timedelta or minutes")
        if result <= timedelta(0):
            raise ValueError(f"{label} must be positive")
        return result

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_state()
        import json as _json

        try:
            raw = _json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, _json.JSONDecodeError) as exc:
            raise StateError("heartbeat cadence state is unreadable") from exc
        if not isinstance(raw, Mapping):
            raise StateError("heartbeat cadence state must be an object")
        return self._normalise(raw)

    @staticmethod
    def _normalise(raw: Mapping[str, Any]) -> dict[str, Any]:
        if raw.get("schema_version") not in {
            None,
            CADENCE_SCHEMA_V1,
            CADENCE_SCHEMA_V2,
            CADENCE_SCHEMA_V3,
        }:
            raise StateError("heartbeat cadence state has an unsupported schema")
        allowed = {
            "schema_version",
            "auto_until",
            "manual_until",
            "automatic_cooldown_until",
            "manual_cooldown_until",
            "last_judge_at",
            "next_judge_at",
            "last_effect_at",
            "daily_anchor_epoch",
            "daily_anchor_completed",
            "private_contacts",
            "verified_visible_contacts",
            "private_contact_overflow_until",
            "verified_visible_overflow_until",
            "effect_terminals",
            "effect_refs",
            # These fields were written by an earlier v2 implementation.  They
            # are accepted for safe migration, but never loaded or persisted;
            # contact dedupe is exact and window-bounded below.
            "private_contact_bloom",
            "verified_visible_bloom",
            "last_private_contact_at",
            "last_verified_visible_contact_at",
            "silence_backoff_processed_receipts",
            "silence_backoff_streak",
            "silence_backoff_last_completed_at",
        }
        if set(raw) - allowed:
            raise StateError("heartbeat cadence state has unsupported fields")
        state = _empty_state()
        for key, aliases in {
            "last_judge_at": ("last_judge_at",),
            "next_judge_at": ("next_judge_at",),
            "last_effect_at": ("last_effect_at",),
            "automatic_cooldown_until": ("automatic_cooldown_until", "auto_until"),
            "manual_cooldown_until": ("manual_cooldown_until", "manual_until"),
            "private_contact_overflow_until": ("private_contact_overflow_until",),
            "verified_visible_overflow_until": ("verified_visible_overflow_until",),
            "last_private_contact_at": ("last_private_contact_at",),
            "last_verified_visible_contact_at": ("last_verified_visible_contact_at",),
            "silence_backoff_last_completed_at": ("silence_backoff_last_completed_at",),
        }.items():
            try:
                decoded = [
                    _decode_time(raw[alias], key) for alias in aliases if alias in raw
                ]
            except ValueError as exc:
                raise StateError(
                    f"heartbeat cadence {key} has invalid timestamp"
                ) from exc
            if len(set(decoded)) > 1:
                raise StateError(f"heartbeat cadence {key} has conflicting timestamps")
            state[key] = decoded[0] if decoded else None
        epoch = raw.get("daily_anchor_epoch")
        if epoch is not None:
            try:
                _strict_iso_date(epoch, "heartbeat daily_anchor_epoch")
            except ValueError as exc:
                raise StateError("heartbeat daily_anchor_epoch is invalid") from exc
        state["daily_anchor_epoch"] = epoch
        completed = raw.get("daily_anchor_completed", False)
        if type(completed) is not bool:
            raise StateError("heartbeat daily_anchor_completed is invalid")
        state["daily_anchor_completed"] = completed
        for key in (
            "private_contacts",
            "verified_visible_contacts",
            "effect_terminals",
        ):
            value = raw.get(key, {})
            if not isinstance(value, Mapping):
                raise StateError(f"heartbeat {key} must be an object")
            copied: dict[str, Any] = {}
            for item_key, item_value in value.items():
                if type(item_key) is not str or not item_key.strip():
                    raise StateError(f"heartbeat {key} has an invalid key")
                if key == "effect_terminals":
                    if type(item_value) is not str or not item_value.strip():
                        raise StateError(f"heartbeat {key} has an invalid state")
                    copied[item_key] = item_value
                else:
                    try:
                        decoded = _decode_time(item_value, f"{key}.{item_key}")
                        if decoded is None:
                            raise ValueError("contact timestamp must be non-null")
                        copied[item_key] = decoded
                    except ValueError as exc:
                        raise StateError(
                            f"heartbeat {key} has an invalid timestamp"
                        ) from exc
            state[key] = copied
        refs = raw.get("effect_refs", {})
        if not isinstance(refs, Mapping):
            raise StateError("heartbeat effect_refs must be an object")
        state["effect_refs"] = {}
        for item_key, item_value in refs.items():
            if (
                type(item_key) is not str
                or not item_key.strip()
                or type(item_value) is not str
                or not item_value.strip()
            ):
                raise StateError("heartbeat effect_refs has an invalid entry")
            state["effect_refs"][item_key] = item_value
        state["private_contacts"] = _compact_contacts(
            state["private_contacts"], _PRIVATE_CONTACT_MAX
        )
        state["verified_visible_contacts"] = _compact_contacts(
            state["verified_visible_contacts"], _VISIBLE_CONTACT_MAX
        )
        state["effect_terminals"] = _compact_tail(
            state["effect_terminals"], _EFFECT_TERMINAL_MAX
        )
        state["effect_refs"] = _compact_tail(state["effect_refs"], _EFFECT_REF_MAX)
        for key in (
            "last_private_contact_at",
            "last_verified_visible_contact_at",
        ):
            values = list(
                state[
                    "private_contacts"
                    if key == "last_private_contact_at"
                    else "verified_visible_contacts"
                ].values()
            )
            current = _optional_time(state.get(key), key)
            mapped = max(
                (_optional_time(value, key) for value in values),
                default=None,
            )
            if mapped is not None and (current is None or mapped > current):
                current = mapped
            state[key] = _json_time(current)
        processed = raw.get("silence_backoff_processed_receipts", {})
        if not isinstance(processed, Mapping):
            raise StateError(
                "heartbeat silence_backoff_processed_receipts must be an object"
            )
        selected_processed: dict[str, str] = {}
        for receipt_id, status in processed.items():
            if (
                type(receipt_id) is not str
                or _SILENCE_BACKOFF_RECEIPT_ID.fullmatch(receipt_id) is None
                or type(status) is not str
                or not status.strip()
                or len(status) > 64
            ):
                raise StateError(
                    "heartbeat silence_backoff_processed_receipts has an invalid entry"
                )
            selected_processed[receipt_id] = status
        state["silence_backoff_processed_receipts"] = _compact_tail(
            selected_processed, _SILENCE_BACKOFF_RECEIPT_MAX
        )
        streak = raw.get("silence_backoff_streak", 0)
        if type(streak) is not int or not 0 <= streak <= _SILENCE_BACKOFF_RECEIPT_MAX:
            raise StateError("heartbeat silence_backoff_streak is invalid")
        state["silence_backoff_streak"] = streak
        return state

    @staticmethod
    def _serialise(state: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": CADENCE_SCHEMA_V3,
            "last_judge_at": state.get("last_judge_at"),
            "next_judge_at": state.get("next_judge_at"),
            "manual_cooldown_until": state.get("manual_cooldown_until"),
            "automatic_cooldown_until": state.get("automatic_cooldown_until"),
            "manual_until": state.get("manual_cooldown_until"),
            "auto_until": state.get("automatic_cooldown_until"),
            "last_effect_at": state.get("last_effect_at"),
            "daily_anchor_epoch": state.get("daily_anchor_epoch"),
            "daily_anchor_completed": bool(state.get("daily_anchor_completed", False)),
            "private_contacts": dict(state.get("private_contacts", {})),
            "verified_visible_contacts": dict(
                state.get("verified_visible_contacts", {})
            ),
            "private_contact_overflow_until": state.get(
                "private_contact_overflow_until"
            ),
            "verified_visible_overflow_until": state.get(
                "verified_visible_overflow_until"
            ),
            "effect_terminals": _compact_tail(
                state.get("effect_terminals", {}), _EFFECT_TERMINAL_MAX
            ),
            "effect_refs": _compact_tail(state.get("effect_refs", {}), _EFFECT_REF_MAX),
            "last_private_contact_at": state.get("last_private_contact_at"),
            "last_verified_visible_contact_at": state.get(
                "last_verified_visible_contact_at"
            ),
            "silence_backoff_processed_receipts": _compact_tail(
                state.get("silence_backoff_processed_receipts", {}),
                _SILENCE_BACKOFF_RECEIPT_MAX,
            ),
            "silence_backoff_streak": int(state.get("silence_backoff_streak", 0)),
            "silence_backoff_last_completed_at": state.get(
                "silence_backoff_last_completed_at"
            ),
        }

    def _save(self, state: Mapping[str, Any]) -> None:
        atomic_json_write(self.path, self._serialise(state))

    def _now(self, value: datetime | None = None) -> datetime:
        return _aware(self.clock() if value is None else value)

    def _prune_contact_state(self, state: dict[str, Any], now: datetime) -> bool:
        """Keep only exact source ids that can still affect the recent gate.

        The maps are intentionally bounded, but they never evict an id that is
        still inside the contact window merely to make room for another id.  A
        single expiry marker represents the conservative overflow case until
        the window clears; this avoids lifetime probabilistic dedupe.
        """
        changed = False
        cutoff = now - self.recent_contact_window
        for contacts_key, overflow_key, limit in (
            (
                "private_contacts",
                "private_contact_overflow_until",
                _PRIVATE_CONTACT_MAX,
            ),
            (
                "verified_visible_contacts",
                "verified_visible_overflow_until",
                _VISIBLE_CONTACT_MAX,
            ),
        ):
            contacts = state[contacts_key]
            kept: dict[str, str] = {}
            for key, raw in contacts.items():
                observed = _optional_time(raw, f"{contacts_key}.{key}")
                if observed is not None and observed > cutoff:
                    kept[key] = raw
            if len(kept) > limit:
                kept = _compact_contacts(kept, limit)
            if kept != contacts:
                state[contacts_key] = kept
                changed = True
            overflow = _optional_time(state.get(overflow_key), overflow_key)
            if overflow is not None and overflow <= now:
                state[overflow_key] = None
                changed = True
        return changed

    def _recent_from_state(
        self, state: Mapping[str, Any], now: datetime
    ) -> tuple[str | None, datetime | None]:
        items: list[tuple[str, datetime]] = []
        for contacts_key, label in (
            ("private_contacts", "recent_private_inbound"),
            ("verified_visible_contacts", "recent_verified_visible_contact"),
        ):
            for raw in state[contacts_key].values():
                parsed = _optional_time(raw, contacts_key)
                if parsed is not None and parsed + self.recent_contact_window > now:
                    items.append((label, parsed))
        for overflow_key, label in (
            ("private_contact_overflow_until", "recent_private_inbound"),
            (
                "verified_visible_overflow_until",
                "recent_verified_visible_contact",
            ),
        ):
            expiry = _optional_time(state.get(overflow_key), overflow_key)
            if expiry is not None and expiry > now:
                items.append((label, expiry - self.recent_contact_window))
        if not items:
            return None, None
        return max(items, key=lambda item: item[1])

    def snooze(self, minutes: int, *, manual: bool) -> datetime:
        if type(minutes) is not int or not 1 <= minutes <= 1440:
            raise ValueError("snooze minutes must be from 1 to 1440")
        until = self._now() + timedelta(minutes=minutes)
        with file_lock(self.lock_path):
            state = self._load()
            state["manual_cooldown_until" if manual else "automatic_cooldown_until"] = (
                isoformat(until)
            )
            self._save(state)
        return until

    def resume(self) -> None:
        with file_lock(self.lock_path):
            state = self._load()
            state["manual_cooldown_until"] = state["manual_until"] = None
            state["automatic_cooldown_until"] = state["auto_until"] = None
            state["silence_backoff_streak"] = 0
            state["silence_backoff_last_completed_at"] = None
            self._save(state)

    @staticmethod
    def _clear_automatic_backoff(state: dict[str, Any]) -> None:
        state["automatic_cooldown_until"] = state["auto_until"] = None
        state["silence_backoff_streak"] = 0
        state["silence_backoff_last_completed_at"] = None

    def observe_private_reply(self, observed_at: datetime | None = None) -> None:
        observed = self._now(observed_at)
        with file_lock(self.lock_path):
            state = self._load()
            previous = _optional_time(
                state.get("last_private_contact_at"), "last_private_contact_at"
            )
            if previous is None or observed > previous:
                state["last_private_contact_at"] = isoformat(observed)
            self._clear_automatic_backoff(state)
            self._save(state)

    def apply_silence_backoff(
        self,
        receipt: HeartbeatSilenceReceipt,
        *,
        policy: Mapping[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically dedupe a settled silence and update cadence cooldown."""

        selected_policy = _normalise_silence_policy(policy)
        if not selected_policy["enabled"]:
            return {
                "status": "disabled",
                "applied": False,
                "processed": False,
                "streak": 0,
                "cooldown_until": None,
            }
        if not isinstance(receipt, HeartbeatSilenceReceipt):
            raise TypeError("silence backoff requires HeartbeatSilenceReceipt")
        effective_now = self._now(now)
        with file_lock(self.lock_path):
            state = self._load()
            changed = self._prune_contact_state(state, effective_now)
            processed = dict(state["silence_backoff_processed_receipts"])
            if receipt.receipt_id in processed:
                if changed:
                    self._save(state)
                return {
                    "status": "duplicate",
                    "applied": False,
                    "processed": True,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            if not receipt.settled:
                if changed:
                    self._save(state)
                return {
                    "status": "pending_settlement",
                    "applied": False,
                    "processed": False,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            if "unknown" in {
                receipt.judge_terminal,
                receipt.wake_terminal,
                receipt.delivery_terminal,
            }:
                if changed:
                    self._save(state)
                return {
                    "status": "pending_terminal",
                    "applied": False,
                    "processed": False,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            if receipt.completed_at > effective_now:
                if changed:
                    self._save(state)
                return {
                    "status": "future_receipt",
                    "applied": False,
                    "processed": False,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            contact_watermarks = tuple(
                value
                for value in (
                    _optional_time(
                        state.get("last_private_contact_at"),
                        "last_private_contact_at",
                    ),
                    _optional_time(
                        state.get("last_verified_visible_contact_at"),
                        "last_verified_visible_contact_at",
                    ),
                )
                if value is not None
            )
            if contact_watermarks and receipt.completed_at <= max(contact_watermarks):
                processed[receipt.receipt_id] = "contact_after_receipt"
                state["silence_backoff_processed_receipts"] = _compact_tail(
                    processed, _SILENCE_BACKOFF_RECEIPT_MAX
                )
                self._save(state)
                return {
                    "status": "contact_after_receipt",
                    "applied": False,
                    "processed": True,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            if not (
                receipt.profile == "routine"
                and receipt.intentional_silence
                and receipt.judge_terminal == "approved"
                and receipt.wake_terminal == "verified"
                and receipt.delivery_terminal == "not_requested"
                and not receipt.manual_override
            ):
                processed[receipt.receipt_id] = "ineligible"
                state["silence_backoff_processed_receipts"] = _compact_tail(
                    processed, _SILENCE_BACKOFF_RECEIPT_MAX
                )
                self._save(state)
                return {
                    "status": "ineligible",
                    "applied": False,
                    "processed": True,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            previous_completed = _optional_time(
                state.get("silence_backoff_last_completed_at"),
                "silence_backoff_last_completed_at",
            )
            if (
                previous_completed is not None
                and receipt.completed_at <= previous_completed
            ):
                processed[receipt.receipt_id] = "out_of_order"
                state["silence_backoff_processed_receipts"] = _compact_tail(
                    processed, _SILENCE_BACKOFF_RECEIPT_MAX
                )
                self._save(state)
                return {
                    "status": "out_of_order",
                    "applied": False,
                    "processed": True,
                    "streak": state["silence_backoff_streak"],
                    "cooldown_until": state["automatic_cooldown_until"],
                }
            streak = min(
                state["silence_backoff_streak"] + 1,
                _SILENCE_BACKOFF_RECEIPT_MAX,
            )
            duration = (
                selected_policy["first_minutes"]
                if streak == 1
                else selected_policy["repeat_minutes"]
            )
            duration = min(duration, selected_policy["max_minutes"])
            expiry = receipt.completed_at + timedelta(minutes=duration)
            current_until = _optional_time(
                state.get("automatic_cooldown_until"),
                "automatic_cooldown_until",
            )
            if current_until is None or expiry > current_until:
                state["automatic_cooldown_until"] = state["auto_until"] = (
                    isoformat(expiry) if expiry > effective_now else None
                )
            processed[receipt.receipt_id] = (
                "applied" if expiry > effective_now else "expired"
            )
            state["silence_backoff_processed_receipts"] = _compact_tail(
                processed, _SILENCE_BACKOFF_RECEIPT_MAX
            )
            state["silence_backoff_streak"] = streak
            state["silence_backoff_last_completed_at"] = isoformat(receipt.completed_at)
            self._save(state)
            return {
                "status": "applied" if expiry > effective_now else "expired",
                "applied": expiry > effective_now,
                "processed": True,
                "streak": streak,
                "cooldown_until": state["automatic_cooldown_until"],
                "duration_minutes": duration,
            }

    def mark_verified_dm(self) -> None:
        self.resume()

    def cooldown(
        self,
        kind: str,
        *,
        now: datetime | None = None,
        bypass: Iterable[str] | None = None,
    ) -> tuple[bool, str, datetime | None]:
        selected_bypass = frozenset() if bypass is None else frozenset(bypass)
        if not selected_bypass <= HEARTBEAT_BYPASSES:
            raise ValueError("heartbeat cooldown bypass is unsupported")
        effective_now = self._now(now)
        if not self.path.exists():
            return False, "open", None
        with file_lock(self.lock_path):
            state = self._load()
        for key, label, bypass_name in (
            ("manual_cooldown_until", "manual_snooze", "manual_snooze"),
            (
                "automatic_cooldown_until",
                "automatic_cadence",
                "automatic_cooldown",
            ),
        ):
            if bypass_name in selected_bypass:
                continue
            until = _optional_time(state.get(key), key)
            if until is not None and until > effective_now:
                return True, label, until
        return False, "open", None

    def blocked(
        self,
        kind: str,
        *,
        now: datetime | None = None,
        bypass: Iterable[str] | None = None,
    ) -> tuple[bool, str]:
        blocked, reason, _until = self.cooldown(kind, now=now, bypass=bypass)
        return blocked, reason

    def _anchor_epoch(self, now: datetime) -> str:
        local = _aware(now).astimezone(self.timezone)
        date = local.date()
        if local.hour < self.anchor_hour:
            date -= timedelta(days=1)
        return date.isoformat()

    def daily_anchor_epoch(self, now: datetime | None = None) -> str:
        return self._anchor_epoch(self._now(now))

    def daily_anchor_due(self, now: datetime | None = None) -> bool:
        epoch = self.daily_anchor_epoch(now)
        if not self.path.exists():
            return True
        with file_lock(self.lock_path):
            state = self._load()
        return not (
            state["daily_anchor_completed"] and state["daily_anchor_epoch"] == epoch
        )

    def mark_daily_anchor(
        self, epoch: str | None = None, *, now: datetime | None = None
    ) -> str:
        selected = epoch if epoch is not None else self.daily_anchor_epoch(now)
        _strict_iso_date(selected, "daily anchor epoch")
        with file_lock(self.lock_path):
            state = self._load()
            state["daily_anchor_epoch"] = selected
            state["daily_anchor_completed"] = True
            self._save(state)
        return selected

    def next_judge_at(self, now: datetime | None = None) -> datetime:
        effective_now = self._now(now)
        if not self.path.exists():
            return effective_now
        with file_lock(self.lock_path):
            state = self._load()
        value = _optional_time(state["next_judge_at"], "next_judge_at")
        if value is not None:
            return value
        last = _optional_time(state["last_judge_at"], "last_judge_at")
        return effective_now if last is None else last + self.judge_interval

    def mark_judge(
        self,
        *,
        now: datetime | None = None,
        next_judge_at: datetime | str | None = None,
        cadence_minutes: int | None = None,
        anchor_epoch: str | None = None,
    ) -> datetime:
        effective_now = self._now(now)
        if next_judge_at is not None:
            selected = _optional_time(next_judge_at, "next_judge_at")
            assert selected is not None
            if selected <= effective_now:
                raise ValueError("next_judge_at must be later than now")
        elif cadence_minutes is not None:
            if type(cadence_minutes) is not int or not 1 <= cadence_minutes <= 10080:
                raise ValueError("cadence_minutes is out of bounds")
            selected = effective_now + timedelta(minutes=cadence_minutes)
        elif anchor_epoch is not None:
            _strict_iso_date(anchor_epoch, "anchor_epoch")
            local = effective_now.astimezone(self.timezone)
            next_date = local.date() + timedelta(days=1)
            selected = datetime(
                next_date.year,
                next_date.month,
                next_date.day,
                self.anchor_hour,
                tzinfo=self.timezone,
            ).astimezone(effective_now.tzinfo)
        else:
            selected = effective_now + self.judge_interval
        with file_lock(self.lock_path):
            state = self._load()
            state["last_judge_at"] = isoformat(effective_now)
            state["next_judge_at"] = isoformat(selected)
            if anchor_epoch is not None:
                state["daily_anchor_epoch"] = anchor_epoch
                state["daily_anchor_completed"] = True
            self._save(state)
        return selected

    def record_private_contact(
        self,
        receipt: SessionHookReceipt | None = None,
        *,
        source_id: str | None = None,
        observed_at: datetime | None = None,
        fresh: bool = True,
        source_kind: str = "private_inbound",
    ) -> bool:
        if receipt is not None:
            if not isinstance(receipt, SessionHookReceipt):
                raise TypeError("receipt must be a SessionHookReceipt")
            context = receipt.context
            if not context.counts_as_private_contact:
                return False
            source_id, observed_at, fresh, source_kind = (
                context.source_id,
                context.observed_at,
                context.fresh,
                context.source_kind,
            )
        if (
            type(source_id) is not str
            or not source_id.strip()
            or type(fresh) is not bool
            or not fresh
            or source_kind != "private_inbound"
        ):
            return False
        effective_now = self._now()
        observed = _aware(effective_now if observed_at is None else observed_at)
        if observed > effective_now:
            return False
        with file_lock(self.lock_path):
            state = self._load()
            changed = self._prune_contact_state(state, effective_now)
            contacts = dict(state["private_contacts"])
            if source_id in contacts:
                if changed:
                    self._save(state)
                return False
            watermark = _optional_time(
                state.get("last_private_contact_at"), "last_private_contact_at"
            )
            if watermark is None or observed > watermark:
                state["last_private_contact_at"] = isoformat(observed)
            if observed + self.recent_contact_window <= effective_now:
                self._clear_automatic_backoff(state)
                self._save(state)
                return False
            if len(contacts) >= _PRIVATE_CONTACT_MAX:
                expiry = observed + self.recent_contact_window
                current_expiry = _optional_time(
                    state.get("private_contact_overflow_until"),
                    "private_contact_overflow_until",
                )
                if current_expiry is not None and current_expiry > expiry:
                    expiry = current_expiry
                state["private_contact_overflow_until"] = isoformat(expiry)
                self._clear_automatic_backoff(state)
                self._save(state)
                return True
            contacts[source_id] = isoformat(observed)
            state["private_contacts"] = _compact_contacts(
                contacts, _PRIVATE_CONTACT_MAX
            )
            self._clear_automatic_backoff(state)
            self._save(state)
        return True

    def record_verified_visible_contact(
        self,
        record: EffectRecord,
        receipt: EffectReceipt | None = None,
    ) -> bool:
        """Project only a verified heartbeat delivery into contact state."""

        if not isinstance(record, EffectRecord):
            raise TypeError("verified visible contact requires EffectRecord")
        if (
            record.kind != "heartbeat_delivery"
            or record.state != "verified"
            or not record.verified
            or not isinstance(record.receipt, EffectReceipt)
        ):
            return False
        selected = record.receipt if receipt is None else receipt
        if not isinstance(selected, EffectReceipt):
            raise TypeError("verified visible contact requires EffectReceipt")
        if selected != record.receipt or (
            selected.event_id != record.source_event_id
            or selected.content_sha256 != record.content_sha256
            or selected.content_length != record.content_length
            or selected.epoch_id != record.epoch_id
        ):
            return False
        key = record.effect_id
        observed = _aware(selected.observed_at, "receipt observed_at")
        effective_now = self._now()
        if observed > effective_now:
            return False
        with file_lock(self.lock_path):
            state = self._load()
            changed = self._prune_contact_state(state, effective_now)
            contacts = dict(state["verified_visible_contacts"])
            if key in contacts:
                if changed:
                    self._save(state)
                return False
            watermark = _optional_time(
                state.get("last_verified_visible_contact_at"),
                "last_verified_visible_contact_at",
            )
            watermark_changed = watermark is None or observed > watermark
            if watermark_changed:
                state["last_verified_visible_contact_at"] = isoformat(observed)
            if observed + self.recent_contact_window <= effective_now:
                self._clear_automatic_backoff(state)
                self._save(state)
                return False
            if len(contacts) >= _VISIBLE_CONTACT_MAX:
                expiry = observed + self.recent_contact_window
                current_expiry = _optional_time(
                    state.get("verified_visible_overflow_until"),
                    "verified_visible_overflow_until",
                )
                if current_expiry is not None and current_expiry > expiry:
                    expiry = current_expiry
                state["verified_visible_overflow_until"] = isoformat(expiry)
                self._clear_automatic_backoff(state)
                self._save(state)
                return True
            contacts[key] = isoformat(observed)
            state["verified_visible_contacts"] = _compact_contacts(
                contacts, _VISIBLE_CONTACT_MAX
            )
            self._clear_automatic_backoff(state)
            self._save(state)
        return True

    def record_effect_terminal(
        self, effect_id: str, terminal: str, *, observed_at: datetime | None = None
    ) -> None:
        if type(effect_id) is not str or not effect_id.strip():
            raise ValueError("effect_id must be non-empty")
        if type(terminal) is not str or not terminal.strip():
            raise ValueError("effect terminal must be non-empty")
        observed = self._now(observed_at)
        with file_lock(self.lock_path):
            state = self._load()
            terminals = dict(state["effect_terminals"])
            terminals.pop(effect_id, None)
            terminals[effect_id] = terminal
            state["effect_terminals"] = _compact_tail(terminals, _EFFECT_TERMINAL_MAX)
            state["last_effect_at"] = isoformat(observed)
            self._save(state)

    @staticmethod
    def _effect_ref_key(source_event_id: str, kind: str) -> str:
        if (
            type(source_event_id) is not str
            or not source_event_id.strip()
            or type(kind) is not str
            or not kind.strip()
        ):
            raise ValueError("effect reference identity is invalid")
        return f"{kind}:{source_event_id}"

    def remember_effect_ref(
        self, source_event_id: str, kind: str, effect_id: str
    ) -> None:
        key = self._effect_ref_key(source_event_id, kind)
        if type(effect_id) is not str or not effect_id.strip():
            raise ValueError("effect reference id is invalid")
        with file_lock(self.lock_path):
            state = self._load()
            refs = dict(state["effect_refs"])
            refs.pop(key, None)
            refs[key] = effect_id
            state["effect_refs"] = _compact_tail(refs, _EFFECT_REF_MAX)
            self._save(state)

    def effect_ref(self, source_event_id: str, kind: str) -> str | None:
        key = self._effect_ref_key(source_event_id, kind)
        if not self.path.exists():
            return None
        with file_lock(self.lock_path):
            return self._load()["effect_refs"].get(key)

    def recent_contact(
        self, *, now: datetime | None = None
    ) -> tuple[str | None, datetime | None]:
        effective_now = self._now(now)
        if not self.path.exists():
            return None, None
        with file_lock(self.lock_path):
            state = self._load()
            self._prune_contact_state(state, effective_now)
            return self._recent_from_state(state, effective_now)

    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        effective_now = self._now(now)
        if self.path.exists():
            with file_lock(self.lock_path):
                state = self._load()
                self._prune_contact_state(state, effective_now)
        else:
            state = _empty_state()
        private, visible = state["private_contacts"], state["verified_visible_contacts"]
        next_at = _optional_time(state["next_judge_at"], "next_judge_at")
        if next_at is None:
            last = _optional_time(state["last_judge_at"], "last_judge_at")
            next_at = effective_now if last is None else last + self.judge_interval
        recent_kind, recent_at = self._recent_from_state(state, effective_now)
        return {
            "schema_version": CADENCE_SCHEMA_V3,
            "last_judge_at": state["last_judge_at"],
            "next_judge_at": isoformat(next_at),
            "manual_cooldown_until": state["manual_cooldown_until"],
            "automatic_cooldown_until": state["automatic_cooldown_until"],
            "last_effect_at": state["last_effect_at"],
            "daily_anchor_epoch": state["daily_anchor_epoch"],
            "daily_anchor_completed": state["daily_anchor_completed"],
            "daily_anchor_hour": self.anchor_hour,
            "timezone": self.timezone_name,
            "last_private_contact_at": state["last_private_contact_at"],
            "private_contact_sources": sorted(private),
            "private_contact_overflow_until": state["private_contact_overflow_until"],
            "last_verified_visible_contact_at": state[
                "last_verified_visible_contact_at"
            ],
            "verified_visible_effects": sorted(visible),
            "verified_visible_overflow_until": state["verified_visible_overflow_until"],
            "recent_contact_kind": recent_kind,
            "recent_contact_at": None if recent_at is None else isoformat(recent_at),
            "effect_terminals": dict(state["effect_terminals"]),
            "effect_reference_count": len(state["effect_refs"]),
            "silence_backoff": {
                "streak": state["silence_backoff_streak"],
                "processed_receipts": len(state["silence_backoff_processed_receipts"]),
                "last_completed_at": state["silence_backoff_last_completed_at"],
                "active": (
                    _optional_time(
                        state["automatic_cooldown_until"],
                        "automatic_cooldown_until",
                    )
                    or effective_now
                )
                > effective_now,
            },
        }

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project cadence state without normalising, pruning, or locking."""

        if type(target_date) is not date:
            raise TypeError("target_date must be a date")
        _aware(now, "now")
        raw, integrity = _read_cadence_state_lock_free(self.path)
        if raw is None:
            if integrity is None:
                return ()
            return (
                _observer_integrity(
                    source=self.path.name, code=integrity, target_date=target_date
                ),
            )

        try:

            def timestamp(name: str, *aliases: str) -> datetime | None:
                values = [
                    _observer_time(raw[key], name)
                    for key in (name, *aliases)
                    if key in raw and raw[key] is not None
                ]
                if len({value.isoformat() for value in values}) > 1:
                    raise ValueError(f"conflicting {name}")
                return values[0] if values else None

            last_judge = timestamp("last_judge_at")
            next_judge = timestamp("next_judge_at")
            last_effect = timestamp("last_effect_at")
            timestamp("automatic_cooldown_until", "auto_until")
            timestamp("manual_cooldown_until", "manual_until")
            timestamp("private_contact_overflow_until")
            timestamp("verified_visible_overflow_until")
            timestamp("last_private_contact_at")
            timestamp("last_verified_visible_contact_at")
            silence_last_completed = timestamp("silence_backoff_last_completed_at")
            silence_processed = raw.get("silence_backoff_processed_receipts", {})
            if not isinstance(silence_processed, Mapping):
                raise ValueError("silence_backoff_processed_receipts is invalid")
            for receipt_id, status in silence_processed.items():
                if (
                    type(receipt_id) is not str
                    or _SILENCE_BACKOFF_RECEIPT_ID.fullmatch(receipt_id) is None
                    or type(status) is not str
                    or not status.strip()
                    or len(status) > 64
                ):
                    raise ValueError("silence backoff processed receipt is invalid")
            silence_streak = raw.get("silence_backoff_streak", 0)
            if (
                type(silence_streak) is not int
                or not 0 <= silence_streak <= _SILENCE_BACKOFF_RECEIPT_MAX
            ):
                raise ValueError("silence backoff streak is invalid")

            completed_present = "daily_anchor_completed" in raw
            completed = raw.get("daily_anchor_completed", False)
            if type(completed) is not bool:
                raise ValueError("daily anchor completion is invalid")
            epoch = raw.get("daily_anchor_epoch")
            if epoch is not None:
                _strict_iso_date(epoch, "daily anchor epoch")

            contact_maps: dict[str, dict[str, datetime]] = {}
            for name in ("private_contacts", "verified_visible_contacts"):
                value = raw.get(name, {})
                if not isinstance(value, Mapping):
                    raise ValueError(f"{name} is invalid")
                selected: dict[str, datetime] = {}
                for source, observed in value.items():
                    if type(source) is not str or not source.strip():
                        raise ValueError(f"{name} source is invalid")
                    parsed = _observer_time(observed, f"{name}.{source}")
                    if parsed is None:
                        raise ValueError(f"{name} timestamp is invalid")
                    selected[source] = parsed
                contact_maps[name] = selected

            effect_terminals = raw.get("effect_terminals", {})
            if not isinstance(effect_terminals, Mapping):
                raise ValueError("effect_terminals is invalid")
            terminals: dict[str, str] = {}
            for effect_id, terminal in effect_terminals.items():
                if (
                    type(effect_id) is not str
                    or not effect_id.strip()
                    or type(terminal) is not str
                    or not terminal.strip()
                ):
                    raise ValueError("effect terminal is invalid")
                terminals[effect_id] = terminal

            effect_refs = raw.get("effect_refs", {})
            if not isinstance(effect_refs, Mapping):
                raise ValueError("effect_refs is invalid")
            kind_by_effect: dict[str, str] = {}
            for ref_key, effect_id in effect_refs.items():
                if (
                    type(ref_key) is not str
                    or not ref_key.strip()
                    or type(effect_id) is not str
                    or not effect_id.strip()
                ):
                    raise ValueError("effect reference is invalid")
                kind, separator, _source = ref_key.partition(":")
                if separator and kind in {"heartbeat_delivery", "heartbeat_wake"}:
                    previous = kind_by_effect.get(effect_id)
                    if previous is not None and previous != kind:
                        raise ValueError("conflicting heartbeat effect reference kinds")
                    kind_by_effect[effect_id] = kind

            state_evidence_fields = (
                "auto_until",
                "manual_until",
                "automatic_cooldown_until",
                "manual_cooldown_until",
                "last_judge_at",
                "next_judge_at",
                "last_effect_at",
                "daily_anchor_epoch",
                "daily_anchor_completed",
                "private_contacts",
                "verified_visible_contacts",
                "effect_terminals",
                "effect_refs",
                "private_contact_bloom",
                "verified_visible_bloom",
                "last_private_contact_at",
                "last_verified_visible_contact_at",
                "silence_backoff_processed_receipts",
                "silence_backoff_streak",
                "silence_backoff_last_completed_at",
            )
            has_state_evidence = any(
                raw.get(key) not in (None, {}, False) for key in state_evidence_fields
            )
            facts: list[ObservationFact] = [
                _observer_fact(
                    key="heartbeat:cadence:state",
                    code=(
                        "heartbeat_cadence_observed"
                        if has_state_evidence
                        else "heartbeat_cadence_uninitialized"
                    ),
                    state="neutral",
                    target_date=target_date,
                    refs=(self.path.name,),
                    counts={
                        "cadence_state": 1,
                        "initialized": int(has_state_evidence),
                    },
                )
            ]
            if (
                silence_processed
                or silence_streak
                or silence_last_completed is not None
            ):
                active_until = timestamp("automatic_cooldown_until", "auto_until")
                is_active = active_until is not None and active_until > now
                facts.append(
                    _observer_fact(
                        key="heartbeat:silence_backoff",
                        code=(
                            "heartbeat_silence_backoff_active"
                            if is_active
                            else "heartbeat_silence_backoff_observed"
                        ),
                        state="current" if is_active else "neutral",
                        target_date=target_date,
                        event_time=silence_last_completed,
                        refs=(self.path.name,),
                        counts={
                            "processed_receipts": len(silence_processed),
                            "streak": silence_streak,
                            "active": int(is_active),
                        },
                    )
                )
            if last_judge is not None:
                facts.append(
                    _observer_fact(
                        key="heartbeat:judge:last",
                        code="heartbeat_last_judge",
                        state="neutral",
                        target_date=target_date,
                        event_time=last_judge,
                        refs=(self.path.name,),
                        counts={"judge_events": 1},
                    )
                )
            if next_judge is not None:
                facts.append(
                    _observer_fact(
                        key="heartbeat:judge:next",
                        code="heartbeat_next_judge",
                        state="neutral",
                        target_date=target_date,
                        event_time=next_judge,
                        refs=(self.path.name,),
                        counts={"judge_schedule_entries": 1},
                    )
                )
            if epoch is not None:
                target_epoch = target_date.isoformat()
                current_epoch = self._anchor_epoch(now)
                if not completed_present:
                    anchor_code = "heartbeat_anchor_observed"
                    anchor_state = "neutral"
                    anchor_refs = (f"anchor:{epoch}", "completion:uninitialized")
                    anchor_counts = {"anchor_entries": 1, "uninitialized": 1}
                elif epoch != target_epoch:
                    anchor_code = "heartbeat_anchor_outside_target"
                    anchor_state = "neutral"
                    anchor_refs = (
                        f"anchor:{epoch}",
                        f"target:{target_epoch}",
                        f"current:{current_epoch}",
                    )
                    anchor_counts = {"anchor_entries": 1, "outside_target": 1}
                elif epoch != current_epoch:
                    anchor_code = "heartbeat_anchor_stale_observed"
                    anchor_state = "neutral"
                    anchor_refs = (
                        f"anchor:{epoch}",
                        f"target:{target_epoch}",
                        f"current:{current_epoch}",
                    )
                    anchor_counts = {"anchor_entries": 1, "stale": 1}
                else:
                    anchor_code = (
                        "heartbeat_anchor_completed"
                        if completed
                        else "heartbeat_anchor_pending"
                    )
                    anchor_state = "neutral" if completed else "current"
                    anchor_refs = (f"anchor:{epoch}",)
                    anchor_counts = {"anchor_entries": 1}
                facts.append(
                    _observer_fact(
                        key="heartbeat:anchor",
                        code=anchor_code,
                        state=anchor_state,
                        target_date=target_date,
                        refs=anchor_refs,
                        counts=anchor_counts,
                    )
                )

            for name, code, key in (
                (
                    "private_contacts",
                    "heartbeat_contact_private",
                    "heartbeat:contact:private",
                ),
                (
                    "verified_visible_contacts",
                    "heartbeat_contact_verified_visible",
                    "heartbeat:contact:verified_visible",
                ),
            ):
                contacts = contact_maps[name]
                if not contacts:
                    continue
                latest_source, latest_at = max(
                    contacts.items(), key=lambda item: item[1]
                )
                source_refs = tuple(sorted(contacts))[:32]
                facts.append(
                    _observer_fact(
                        key=key,
                        code=code,
                        state="neutral",
                        target_date=target_date,
                        event_time=latest_at,
                        refs=(f"source:{latest_source}", *source_refs),
                        counts={
                            "contacts": len(contacts),
                            "source_refs": len(source_refs),
                        },
                    )
                )

            terminal_code = {
                "pending": "pending",
                "executed_unverified": "executed_unverified",
                "expired": "expired",
                "failed": "failed",
                "requeued": "requeued",
                "verified": "verified",
            }
            for effect_id, terminal in terminals.items():
                kind = kind_by_effect.get(effect_id)
                if kind is None:
                    continue
                safe_terminal = terminal_code.get(terminal, "unknown")
                is_current = safe_terminal in {
                    "pending",
                    "executed_unverified",
                    "expired",
                    "failed",
                    "requeued",
                    "unknown",
                }
                facts.append(
                    _observer_fact(
                        key=f"heartbeat:terminal:{kind}:{effect_id}",
                        code=f"heartbeat_{kind}_{safe_terminal}",
                        state="current" if is_current else "neutral",
                        target_date=target_date,
                        event_time=last_effect,
                        refs=(f"effect:{effect_id}", f"kind:{kind}"),
                        counts={"terminals": 1},
                    )
                )
            return _dedupe_observer_facts(facts)
        except Exception as exc:
            return (
                _observer_integrity(
                    source=self.path.name,
                    code=f"state_{type(exc).__name__}",
                    target_date=target_date,
                ),
            )

    status = snapshot


_ACCEPTED = frozenset(
    {
        "accepted",
        "queued",
        "queued_unverified",
        "pending",
        "sent",
        "ok",
        "executed_unverified",
    }
)
_DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX = ":delegated"
_DELEGATED_FAILURE_REASON = "delegated_delivery_failed"


def _is_delegated_delivery(record: EffectRecord) -> bool:
    return record.kind == "heartbeat_delivery" and record.idempotency_key.endswith(
        _DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX
    )


def _effect_body(
    kind: str, candidate: HeartbeatCandidate, decision: JudgeDecision
) -> bytes:
    if kind == "delivery" and decision.delivery_mode == "delegated":
        instruction = decision.message.encode("utf-8")
        return json.dumps(
            {
                "schema": "moon.heartbeat.delivery_obligation.v1",
                "mode": "delegated",
                "candidate_id": candidate.candidate_id,
                "kind": candidate.kind,
                "instruction_sha256": hashlib.sha256(instruction).hexdigest(),
                "instruction_length": len(instruction),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    if kind == "delivery":
        return decision.message.encode("utf-8")
    return json.dumps(
        {
            "schema_version": "moon.wake_packet.v1",
            "event_type": "moonbite_heartbeat_wake",
            "candidate_id": candidate.candidate_id,
            "kind": candidate.kind,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class HeartbeatEngine:
    def __init__(
        self,
        *,
        bus: EventBus,
        controls: ControlStore,
        cadence: HeartbeatCadence,
        judge: Judge,
        sink: WakeSink | None = None,
        locks: RuntimeLocks | None = None,
        effect_ledger: EffectLedger | None = None,
        session_receipt: SessionHookReceipt | None = None,
        session_hook_receipt: SessionHookReceipt | None = None,
        effect_ttl: timedelta | int | float | None = None,
        kind_policies: Mapping[str, Mapping[str, Any]] | None = None,
    ):
        if kind_policies is not None and not isinstance(kind_policies, Mapping):
            raise TypeError("kind_policies must be a mapping or None")
        self.bus, self.controls, self.cadence = bus, controls, cadence
        self.judge, self.sink = judge, sink or NoopWakeSink()
        self.kind_policies = kind_policies
        self.effect_ledger = effect_ledger
        self.session_receipt = session_receipt or session_hook_receipt
        if effect_ttl is None:
            effect_ttl = getattr(cadence, "effect_ttl", DEFAULT_EFFECT_TTL)
        self.effect_ttl = HeartbeatCadence._duration(effect_ttl, "effect_ttl")
        if locks is None:
            root = self._cadence_root()
            if root is None:
                raise ValueError("locks are required for a pathless cadence")
            self.locks = FileRuntimeLocks(root)
            self.execution_lock_path = root / "heartbeat_execution.lock"
        else:
            self.locks, self.execution_lock_path = locks, None
        if self.effect_ledger is None:
            root = self._cadence_root()
            if root is not None and locks is None:
                self.effect_ledger = EffectLedger(root, clock=self._clock)

    def _cadence_root(self) -> Path | None:
        try:
            value = getattr(self.cadence, "path")
        except (AttributeError, AssertionError, OSError):
            return None
        return value.parent if isinstance(value, Path) else None

    def _clock(self) -> datetime:
        clock = getattr(self.cadence, "clock", utc_now)
        return _aware(clock() if callable(clock) else utc_now())

    def kind_policy(self, kind: str) -> HeartbeatKindPolicy | None:
        if type(kind) is not str or _HEARTBEAT_KIND_PATTERN.fullmatch(kind) is None:
            raise ValueError("heartbeat kind has invalid syntax")
        if self.kind_policies is None:
            return HeartbeatKindPolicy(
                enabled=True,
                profile="routine",
                judge="required",
                host_only=False,
            )
        try:
            descriptor = self.kind_policies[kind]
        except KeyError:
            return None
        if not isinstance(descriptor, Mapping):
            raise _CandidateInvalidError(f"heartbeat kind {kind} policy is malformed")
        return _kind_policy_from_descriptor(kind, descriptor)

    def _candidate_policy(
        self, kind: str
    ) -> tuple[HeartbeatKindPolicy | None, str | None]:
        try:
            policy = self.kind_policy(kind)
        except ValueError as exc:
            return None, str(exc)
        if policy is None:
            return None, f"heartbeat kind {kind} is unconfigured"
        if not policy.enabled:
            return policy, f"heartbeat kind {kind} is disabled"
        return policy, None

    def _effect_owner_exists(self) -> bool:
        if self.effect_ledger is None:
            return False
        try:
            owner = getattr(self.effect_ledger, "ledger", None)
            path = getattr(owner, "path", None)
        except Exception:
            return True
        return True if not isinstance(path, Path) else path.exists()

    def _pristine_neutral_probe(
        self,
        candidate: HeartbeatCandidate,
        *,
        session_receipt: SessionHookReceipt | None,
        policy: HeartbeatKindPolicy,
    ) -> bool:
        """Skip the execution lock for a genuinely pristine no-event probe."""

        try:
            # A configured daily anchor is a durable cadence operation even
            # when the host supplies no ordinary event payload.  Its profile,
            # not an optional caller-controlled context flag, owns that
            # semantic and therefore must reach the locked state path.
            if policy.profile == "daily_anchor":
                return False
            if self._events_present(candidate):
                return False
            receipt = self._context_receipt(
                candidate, session_receipt, self.session_receipt
            )
            if receipt is not None and (
                receipt.event_id.strip() or receipt.source_id.strip()
            ):
                return False
            expires_at = candidate.context.get("expires_at")
            if expires_at is not None:
                expires = _optional_time(expires_at, "expires_at")
                if expires is not None and expires <= self._clock():
                    return False
            cadence_path = getattr(self.cadence, "path", None)
            if isinstance(cadence_path, Path) and cadence_path.exists():
                return False
            if self._effect_owner_exists():
                return False
            controls_path = getattr(
                getattr(self.controls, "ledger", None), "path", None
            )
            if isinstance(controls_path, Path) and controls_path.exists():
                return False
            return True
        except (AssertionError, AttributeError, TypeError, ValueError, StateError):
            return False

    def _snapshot(self) -> dict[str, Any]:
        try:
            cadence = (
                self.cadence.snapshot()
                if callable(getattr(self.cadence, "snapshot", None))
                else {}
            )
        except Exception as exc:
            cadence = {"degraded": True, "error": type(exc).__name__}
        pending: list[dict[str, Any]] = []
        if self._effect_owner_exists():
            try:
                pending = [
                    {
                        "effect_id": record.effect_id,
                        "state": record.state,
                        "expires_at": isoformat(record.expires_at),
                        "attempt": record.attempt,
                    }
                    for record in self.effect_ledger.pending_for_reconciliation(
                        now=self._clock()
                    )
                    if record.kind in {"heartbeat_delivery", "heartbeat_wake"}
                ]
            except Exception as exc:
                pending = [{"degraded": True, "error": type(exc).__name__}]
        snapshot = {"cadence": cadence, "pending_effects": pending}
        if isinstance(cadence, Mapping):
            for key in (
                "last_judge_at",
                "next_judge_at",
                "recent_contact_kind",
                "recent_contact_at",
                "effect_terminals",
            ):
                if key in cadence:
                    snapshot[key] = cadence[key]
        return snapshot

    def status(self) -> dict[str, Any]:
        return self._snapshot()

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Expose cadence and delivery evidence without executing heartbeat.

        The engine deliberately reads the effect JSONL directly.  Calling
        ``pending_for_reconciliation`` or ``snapshot`` here would acquire
        owner locks and, for expired effects, invite the mutating reconcile
        path.  A wake is retained as its own effect kind and is never
        projected as a visible contact.
        """

        if type(target_date) is not date:
            raise TypeError("target_date must be a date")
        _aware(now, "now")
        facts: list[ObservationFact] = []
        cadence_port = getattr(self.cadence, "observer_status", None)
        if callable(cadence_port):
            try:
                candidate_facts = cadence_port(target_date=target_date, now=now)
                if isinstance(candidate_facts, (str, bytes, bytearray, Mapping)):
                    raise TypeError(
                        "cadence observer result must be an iterable of facts"
                    )
                if not isinstance(candidate_facts, Iterable):
                    raise TypeError(
                        "cadence observer result must be an iterable of facts"
                    )
                candidate_facts = tuple(candidate_facts)
                if any(
                    not isinstance(fact, ObservationFact) for fact in candidate_facts
                ):
                    raise TypeError("cadence observer result contains a malformed fact")
                facts.extend(candidate_facts)
            except Exception as exc:
                try:
                    path = getattr(self.cadence, "path", None)
                except Exception:
                    path = None
                source = path.name if isinstance(path, Path) else "cadence"
                facts.append(
                    _observer_integrity(
                        source=source,
                        code=f"port_{type(exc).__name__}",
                        target_date=target_date,
                    )
                )

        else:
            facts.append(
                _observer_fact(
                    key="heartbeat:cadence:observer",
                    code="heartbeat_cadence_observer_unavailable",
                    state="neutral",
                    target_date=target_date,
                    refs=("cadence",),
                    counts={"observer_unavailable": 1},
                )
            )
        ledger_path: Path | None = None
        ledger = self.effect_ledger
        ledger_file = getattr(getattr(ledger, "ledger", None), "path", None)
        if isinstance(ledger_file, Path):
            ledger_path = ledger_file
        if ledger_path is None:
            cadence_path = getattr(self.cadence, "path", None)
            if isinstance(cadence_path, Path):
                ledger_path = cadence_path.parent / "effects.jsonl"
        if ledger_path is None:
            return _dedupe_observer_facts(facts)

        records, integrity = _read_effect_history_lock_free(ledger_path)
        if integrity is not None:
            facts.append(
                _observer_integrity(
                    source=ledger_path.name,
                    code=integrity,
                    target_date=target_date,
                )
            )
            return _dedupe_observer_facts(facts)
        by_effect: dict[str, list[EffectRecord]] = {}
        for record in records:
            if record.kind in {"heartbeat_delivery", "heartbeat_wake"}:
                by_effect.setdefault(record.effect_id, []).append(record)
        visible_candidates: list[ObservationFact] = []
        for history in by_effect.values():
            current = history[-1]
            same_key = [
                item
                for item in records
                if item.kind == current.kind
                and item.idempotency_key == current.idempotency_key
            ]
            facts.append(
                _heartbeat_effect_fact(
                    current,
                    history=same_key,
                    target_date=target_date,
                    now=now,
                )
            )
            if (
                current.kind == "heartbeat_delivery"
                and current.state == "verified"
                and current.receipt is not None
            ):
                visible_candidates.append(
                    _observer_fact(
                        key="heartbeat:contact:verified_visible",
                        code="heartbeat_contact_verified_visible",
                        state="neutral",
                        target_date=target_date,
                        event_time=current.receipt.observed_at,
                        refs=(
                            f"effect:{current.effect_id}",
                            f"receipt:{current.receipt.receipt_id}",
                            "contact:verified_visible",
                        ),
                        counts={"verified_visible_contacts": 1},
                    )
                )
        facts.extend(visible_candidates)
        return _dedupe_observer_facts(facts)

    def _finish(self, result: HeartbeatResult) -> HeartbeatResult:
        projection_errors = list(result.projection_errors)
        try:
            self.bus.record_audit(
                "heartbeat",
                status=result.status,
                source="heartbeat",
                details=self._audit_details(result),
            )
        except Exception as exc:
            projection_errors.append(f"audit_write:{type(exc).__name__}")
        if projection_errors:
            status = result.status
            reason = result.reason
            if status == "completed":
                status = "partial"
                reason = f"{reason}_degraded"
            return replace(
                result,
                status=status,
                reason=reason,
                degraded=True,
                projection_errors=tuple(dict.fromkeys(projection_errors)),
            )
        return result

    @staticmethod
    def _audit_details(result: HeartbeatResult) -> dict[str, Any]:
        details = result.to_dict()
        reason = details.pop("reason", "")
        if type(reason) is str:
            encoded = reason.encode("utf-8")
            details["reason_sha256"] = hashlib.sha256(encoded).hexdigest()
            details["reason_length"] = len(encoded)
        decision = details.get("decision")
        if isinstance(decision, dict):
            for field_name in ("reason", "message"):
                value = decision.pop(field_name, "")
                if type(value) is str:
                    encoded = value.encode("utf-8")
                    decision[f"{field_name}_sha256"] = hashlib.sha256(
                        encoded
                    ).hexdigest()
                    decision[f"{field_name}_length"] = len(encoded)
        return details

    def _result(
        self,
        status: str,
        reason: str,
        candidate_id: str,
        gate: GateResult,
        *,
        code: HeartbeatReasonCode,
        decision: JudgeDecision | None = None,
        delivery: EffectResult | None = None,
        wake: EffectResult | None = None,
        next_judge_at: datetime | None = None,
        audit: bool = True,
        include_snapshot: bool = True,
        projection_errors: tuple[str, ...] = (),
    ) -> HeartbeatResult:
        effect_projection_errors = tuple(
            error
            for effect in (delivery, wake)
            if effect is not None
            for error in effect.projection_errors
        )
        all_projection_errors = tuple(
            dict.fromkeys((*projection_errors, *effect_projection_errors))
        )
        result = HeartbeatResult(
            status,
            reason,
            candidate_id,
            gate,
            decision,
            delivery,
            wake,
            code,
            next_judge_at,
            self._snapshot() if include_snapshot else None,
            bool(all_projection_errors),
            all_projection_errors,
        )
        if not audit:
            return result
        return self._finish(result)

    @staticmethod
    def _candidate_id(candidate: HeartbeatCandidate) -> str:
        if candidate.candidate_id.strip():
            return candidate.candidate_id
        for key in ("event_id", "source_event_id", "candidate_id"):
            value = candidate.context.get(key)
            if type(value) is str and value.strip():
                return value
        events = candidate.context.get("events")
        if events:
            try:
                encoded = json.dumps(
                    events, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                return f"events:{hashlib.sha256(encoded).hexdigest()}"
            except (TypeError, ValueError):
                pass
        return new_id("heartbeat_attempt")

    @staticmethod
    def _context_receipt(
        candidate: HeartbeatCandidate,
        override: SessionHookReceipt | None,
        default: SessionHookReceipt | None,
    ) -> SessionHookReceipt | None:
        value = next(
            (
                item
                for item in (
                    override,
                    candidate.session_receipt,
                    default,
                    candidate.context.get("session_receipt"),
                    candidate.context.get("session_hook_receipt"),
                )
                if item is not None
            ),
            None,
        )
        if value is not None and not isinstance(value, SessionHookReceipt):
            raise ValueError("session receipt must be a SessionHookReceipt")
        return value

    def _control(self) -> GateResult:
        try:
            try:
                resolution = self.controls.resolve("heartbeat", now=self._clock())
            except TypeError:
                resolution = self.controls.resolve("heartbeat")
            return evaluate_gate(resolution)
        except Exception:
            return GateResult(False, "error", "control_state_error", None)

    @staticmethod
    def _events_present(candidate: HeartbeatCandidate) -> bool:
        context = candidate.context
        if "events" in context:
            events = context["events"]
            if isinstance(events, (str, bytes)) and bool(events):
                return True
            if events is not None and events is not False:
                try:
                    if len(events) > 0:
                        return True
                except TypeError:
                    if bool(events):
                        return True
        for key in ("event_id", "source_event_id", "source_id"):
            value = context.get(key)
            if value is not None:
                if type(value) is not str:
                    raise ValueError(f"{key} must be a non-empty string")
                if value.strip():
                    return True
        for receipt in (
            candidate.session_receipt,
            context.get("session_receipt"),
            context.get("session_hook_receipt"),
        ):
            if isinstance(receipt, SessionHookReceipt):
                return bool(receipt.event_id.strip() or receipt.source_id.strip())
        return False

    def _next_due(self, candidate: HeartbeatCandidate, now: datetime) -> datetime:
        context = candidate.context
        if context.get("next_judge_at") is not None:
            value = _optional_time(context["next_judge_at"], "next_judge_at")
            assert value is not None
            return value
        if context.get("last_judge_at") is not None:
            value = _optional_time(context["last_judge_at"], "last_judge_at")
            assert value is not None
            return value + getattr(
                self.cadence, "judge_interval", DEFAULT_JUDGE_INTERVAL
            )
        try:
            return self.cadence.next_judge_at(now=now)
        except (AttributeError, TypeError):
            return now

    def _due(
        self,
        candidate: HeartbeatCandidate,
        now: datetime,
        policy: HeartbeatKindPolicy,
    ) -> tuple[bool, datetime | None]:
        context = candidate.context
        is_anchor = policy.profile == "daily_anchor"
        explicit_anchor = context.get("daily_anchor")
        if explicit_anchor is not None:
            if type(explicit_anchor) is not bool:
                raise ValueError("daily_anchor must be a boolean")
            if explicit_anchor is not is_anchor:
                raise ValueError("daily_anchor conflicts with configured kind profile")
        completed = context.get("anchor_completed_for_epoch")
        if completed is not None and type(completed) is not bool:
            raise ValueError("anchor_completed_for_epoch must be a boolean")
        if not is_anchor and completed is not None:
            raise ValueError(
                "anchor_completed_for_epoch requires a daily_anchor kind profile"
            )
        if is_anchor:
            try:
                due = self.cadence.daily_anchor_due(now)
            except AttributeError:
                # Compatibility fallback for a minimal injected cadence port.
                # A durable cadence implementation remains the authority when
                # it exposes daily_anchor_due().
                due = completed is not True
            return due, None if due else self._next_due(candidate, now)
        explicit = context.get("due")
        if explicit is not None and type(explicit) is not bool:
            raise ValueError("due must be a boolean")
        if explicit is True:
            return True, None
        next_at = self._next_due(candidate, now)
        return (next_at <= now if explicit is None else False), next_at

    def _cooldown(
        self,
        candidate: HeartbeatCandidate,
        now: datetime,
        policy: HeartbeatKindPolicy | None = None,
    ) -> tuple[bool, str, datetime | None]:
        effective_policy = policy or HeartbeatKindPolicy(
            enabled=True,
            profile="routine",
            judge="required",
            host_only=False,
        )
        bypass = effective_policy.bypass
        explicit = candidate.context.get("cooldown")
        if explicit is not None and type(explicit) is not bool:
            raise ValueError("cooldown must be a boolean")
        if explicit is True:
            raw = candidate.context.get("last_effect_at")
            last = _optional_time(raw, "last_effect_at")
            duration = getattr(
                self.cadence, "automatic_cooldown", DEFAULT_AUTOMATIC_COOLDOWN
            )
            return True, "effect_cooldown", None if last is None else last + duration

        cooldown = getattr(self.cadence, "cooldown", None)
        if callable(cooldown):
            try:
                return cooldown(candidate.kind, now=now, bypass=bypass)
            except TypeError as exc:
                if bypass:
                    raise _CandidateInvalidError(
                        "cadence adapter does not support heartbeat bypass"
                    ) from exc
                try:
                    return cooldown(candidate.kind, now=now)
                except TypeError:
                    return cooldown(candidate.kind)

        blocked = getattr(self.cadence, "blocked", None)
        if not callable(blocked):
            raise _CandidateInvalidError("cadence adapter has no cooldown port")
        try:
            blocked_value = blocked(candidate.kind, now=now, bypass=bypass)
        except TypeError as exc:
            if bypass:
                raise _CandidateInvalidError(
                    "cadence adapter does not support heartbeat bypass"
                ) from exc
            try:
                blocked_value = blocked(candidate.kind, now=now)
            except TypeError:
                blocked_value = blocked(candidate.kind)
        blocked_result, reason = blocked_value
        return blocked_result, reason, None

    def _contact(
        self, candidate: HeartbeatCandidate, now: datetime
    ) -> tuple[str | None, datetime | None]:
        window = getattr(
            self.cadence, "recent_contact_window", DEFAULT_RECENT_CONTACT_WINDOW
        )
        items: list[tuple[str, datetime]] = []
        for key, label in (
            ("recent_private_inbound_at", "recent_private_inbound"),
            ("recent_verified_visible_at", "recent_verified_visible_contact"),
        ):
            if candidate.context.get(key) is not None:
                value = _optional_time(candidate.context[key], key)
                assert value is not None
                items.append((label, value))
        try:
            kind, value = self.cadence.recent_contact(now=now)
            if kind is not None and value is not None:
                items.append((kind, value))
        except AttributeError:
            pass
        if not items:
            return None, None
        kind, value = max(items, key=lambda item: item[1])
        return (kind, value) if value + window > now else (None, None)

    @staticmethod
    def _active_chat(candidate: HeartbeatCandidate, value: bool | None) -> bool:
        actual = candidate.context.get("active_chat") if value is None else value
        if actual is None:
            return False
        if type(actual) is not bool:
            raise ValueError("active_chat must be a boolean")
        return actual

    @staticmethod
    def _validate_judge(value: Any) -> JudgeDecision:
        if isinstance(value, JudgeDecision):
            decision = value
        elif isinstance(value, Mapping):
            allowed = {
                "wake_main",
                "wake",
                "dm_user",
                "direct_message",
                "reason",
                "message",
                "allow_autonomy",
                "maintenance",
                "next_judge_at",
                "cadence_minutes",
                "delivery_mode",
            }
            if set(value) - allowed or not {
                "reason",
                "message",
            } <= set(value):
                raise ValueError("Judge decision fields are invalid")
            wake_fields = [value[key] for key in ("wake_main", "wake") if key in value]
            dm_fields = [
                value[key] for key in ("dm_user", "direct_message") if key in value
            ]
            if not wake_fields or not dm_fields:
                raise ValueError("Judge decision fields are invalid")
            if len(set(wake_fields)) > 1 or len(set(dm_fields)) > 1:
                raise ValueError("Judge decision aliases conflict")
            decision = JudgeDecision(
                wake_fields[0],
                dm_fields[0],
                value["reason"],
                value["message"],
                value.get("allow_autonomy"),
                value.get("maintenance"),
                value.get("next_judge_at"),
                value.get("cadence_minutes"),
                value.get("delivery_mode", "direct"),
            )
        else:
            raise ValueError("Judge returned a non-structured decision")
        if type(decision.wake_main) is not bool or type(decision.dm_user) is not bool:
            raise ValueError("Judge wake/dm fields must be booleans")
        if type(decision.reason) is not str or not decision.reason.strip():
            raise ValueError("Judge reason must be non-empty")
        if type(decision.message) is not str:
            raise ValueError("Judge message must be text")
        if decision.dm_user and not decision.message.strip():
            raise ValueError("delivery requires text")
        if decision.delivery_mode not in {"direct", "delegated"}:
            raise ValueError("delivery_mode must be direct or delegated")
        if decision.delivery_mode == "delegated" and not decision.wake_main:
            raise ValueError("delegated delivery requires wake_main")
        if (
            decision.allow_autonomy is not None
            and type(decision.allow_autonomy) is not bool
        ):
            raise ValueError("allow_autonomy must be boolean or null")
        if decision.maintenance is not None and type(decision.maintenance) is not bool:
            raise ValueError("maintenance must be boolean or null")
        if decision.next_judge_at is not None:
            parsed = _optional_time(decision.next_judge_at, "next_judge_at")
            decision = JudgeDecision(
                decision.wake_main,
                decision.dm_user,
                decision.reason.strip(),
                decision.message,
                decision.allow_autonomy,
                decision.maintenance,
                parsed,
                decision.cadence_minutes,
                decision.delivery_mode,
            )
        if decision.cadence_minutes is not None and (
            type(decision.cadence_minutes) is not int
            or not 1 <= decision.cadence_minutes <= 10080
        ):
            raise ValueError("cadence_minutes is out of bounds")
        return decision

    def _reconcile_pending(self, now: datetime) -> tuple[str, int] | None:
        if not self._effect_owner_exists():
            return None
        try:
            records = [
                record
                for record in self.effect_ledger.pending_for_reconciliation(now=now)
                if record.kind in {"heartbeat_delivery", "heartbeat_wake"}
            ]
        except Exception as exc:
            raise StateError("effect ledger replay failed") from exc
        if not records:
            return None
        waiting = 0
        requeued = 0
        for record in records:
            if record.expires_at < now:
                try:
                    self.effect_ledger.expire(record.effect_id, now=now)
                    current = self.effect_ledger.requeue(
                        record.effect_id, expires_at=now + self.effect_ttl
                    )
                except Exception as exc:
                    raise StateError("expired effect reconciliation failed") from exc
                try:
                    self.cadence.record_effect_terminal(
                        record.effect_id, current.state, observed_at=now
                    )
                except Exception as exc:
                    raise StateError(
                        "cadence effect terminal projection failed"
                    ) from exc
                requeued += 1
            else:
                try:
                    self.cadence.record_effect_terminal(
                        record.effect_id, record.state, observed_at=now
                    )
                except Exception as exc:
                    raise StateError(
                        "cadence effect terminal projection failed"
                    ) from exc
                waiting += 1
        return (
            ("awaiting_receipt", requeued) if waiting else ("expired_effect", requeued)
        )

    def _effect_result(
        self, record: EffectRecord, status: str, code: HeartbeatReasonCode | None = None
    ) -> EffectResult:
        projection_errors: list[str] = []
        if record.state == "verified" and record.receipt is not None:
            if record.kind == "heartbeat_delivery":
                try:
                    self.cadence.record_verified_visible_contact(record, record.receipt)
                except Exception as exc:
                    projection_errors.append(
                        f"visible_contact_write:{type(exc).__name__}"
                    )
            result = EffectResult(
                True,
                status or "verified",
                record.receipt,
                True,
                record.effect_id,
                "verified",
                code,
                bool(projection_errors),
                tuple(projection_errors),
            )
        elif record.state == "failed":
            result = EffectResult(
                False,
                status or "failed",
                effect_id=record.effect_id,
                terminal="failed",
                reason_code=code or HeartbeatReasonCode.EFFECT_ERROR,
            )
        elif record.state == "requeued":
            result = EffectResult(
                True,
                status or "requeued",
                effect_id=record.effect_id,
                terminal="requeued",
                reason_code=code or HeartbeatReasonCode.EFFECT_EXPIRED,
            )
        else:
            result = EffectResult(
                True,
                status or "queued_unverified",
                effect_id=record.effect_id,
                terminal=record.state,
                reason_code=code,
            )
        try:
            self.cadence.record_effect_terminal(
                record.effect_id, record.state, observed_at=self._clock()
            )
        except Exception as exc:
            projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
        if projection_errors:
            result = replace(
                result,
                degraded=True,
                projection_errors=tuple(dict.fromkeys(projection_errors)),
            )
        return result

    def _fail_effect(
        self,
        record: EffectRecord,
        status: str,
        reason: str,
        code: HeartbeatReasonCode,
        retryable: bool,
    ) -> EffectResult:
        projection_errors: list[str] = []
        effective_code = code
        failed = record
        try:
            failed = self.effect_ledger.fail(record.effect_id, reason, retryable)
        except Exception as exc:
            projection_errors.append(f"effect_failure_write:{type(exc).__name__}")
            effective_code = HeartbeatReasonCode.EFFECT_REPLAY_ERROR
        try:
            self.cadence.record_effect_terminal(
                record.effect_id, "failed", observed_at=self._clock()
            )
        except Exception as exc:
            projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
        return EffectResult(
            False,
            status,
            effect_id=record.effect_id,
            terminal=getattr(failed, "state", "failed"),
            reason_code=effective_code,
            degraded=bool(projection_errors),
            projection_errors=tuple(dict.fromkeys(projection_errors)),
        )

    def _delegated_completion_result(
        self,
        record: EffectRecord,
        *,
        status: str,
        terminal: str,
        reason_code: HeartbeatReasonCode,
    ) -> EffectResult:
        """Project a delegated host terminal without claiming visible delivery."""

        projection_errors: list[str] = []
        try:
            self.cadence.record_effect_terminal(
                record.effect_id, terminal, observed_at=self._clock()
            )
        except Exception as exc:
            projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
        return EffectResult(
            False,
            status,
            effect_id=record.effect_id,
            terminal=terminal,
            reason_code=reason_code,
            degraded=bool(projection_errors),
            projection_errors=tuple(projection_errors),
        )

    def reconcile_heartbeat_delivery(
        self,
        effect_id: str,
        status: str | None = None,
        receipt: EffectReceipt | None = None,
        *,
        terminal: str | None = None,
    ) -> EffectResult:
        """Reconcile one delegated delivery after host-side settlement.

        The generic effect ledger remains the durable owner.  This narrow seam
        accepts only delegated delivery intents and never treats a queue
        acknowledgement as visible contact.
        """

        selected_status = status if status is not None else terminal
        if type(selected_status) is not str or selected_status not in {
            "verified",
            "intentional_silence",
            "unknown",
            "failed",
        }:
            raise ValueError("delegated delivery status is unsupported")
        if status is not None and terminal is not None and status != terminal:
            raise ValueError("delegated delivery status aliases conflict")
        if self.effect_ledger is None:
            raise RuntimeError("effect ledger is unavailable")
        record = self.effect_ledger.get(effect_id)
        if record is None:
            raise ValueError("heartbeat delivery effect is unknown")
        if record.kind != "heartbeat_delivery":
            raise ValueError("effect is not a heartbeat delivery")
        if not _is_delegated_delivery(record):
            raise ValueError("effect is not a delegated heartbeat delivery")
        if selected_status == "verified":
            if not isinstance(receipt, EffectReceipt):
                raise TypeError("verified delegated delivery requires EffectReceipt")
            if record.state == "verified":
                if record.receipt != receipt:
                    raise ValueError("conflicting delegated delivery receipt")
                return self._effect_result(record, "verified")
            if record.state not in {"pending", "executed_unverified"}:
                raise ValueError("delegated delivery is not awaiting settlement")
            try:
                verified = self.effect_ledger.verify(effect_id, receipt)
            except Exception as exc:
                raise ValueError("delegated delivery receipt mismatch") from exc
            return self._effect_result(verified, "verified")
        if receipt is not None:
            raise ValueError("non-verified delegated delivery cannot carry receipt")
        if selected_status == "unknown":
            if record.state not in {"pending", "executed_unverified"}:
                raise ValueError("unknown delegated delivery is not pending")
            result = self._effect_result(
                record, "unknown", HeartbeatReasonCode.EFFECT_PENDING
            )
            return replace(result, terminal=record.state)
        if selected_status == "intentional_silence":
            if record.state == "failed":
                if (
                    record.reason != "intentional_silence"
                    or record.retryable is not False
                ):
                    raise ValueError("conflicting delegated delivery completion")
                return self._delegated_completion_result(
                    record,
                    status="intentional_silence",
                    terminal="intentional_silence",
                    reason_code=HeartbeatReasonCode.EFFECT_ERROR,
                )
            if record.state not in {"pending", "executed_unverified"}:
                raise ValueError("delegated delivery is not awaiting settlement")
            try:
                failed = self.effect_ledger.fail(
                    effect_id, "intentional_silence", retryable=False
                )
            except Exception as exc:
                raise ValueError("delegated silence completion failed") from exc
            return self._delegated_completion_result(
                failed,
                status="intentional_silence",
                terminal="intentional_silence",
                reason_code=HeartbeatReasonCode.EFFECT_ERROR,
            )
        if record.state == "failed":
            if (
                record.reason != _DELEGATED_FAILURE_REASON
                or record.retryable is not False
            ):
                raise ValueError("conflicting delegated delivery completion")
            return self._delegated_completion_result(
                record,
                status="failed",
                terminal="failed",
                reason_code=HeartbeatReasonCode.EFFECT_ERROR,
            )
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("delegated delivery is not awaiting settlement")
        try:
            failed = self.effect_ledger.fail(
                effect_id, _DELEGATED_FAILURE_REASON, retryable=False
            )
        except Exception as exc:
            raise ValueError("delegated delivery failure completion failed") from exc
        return self._delegated_completion_result(
            failed,
            status="failed",
            terminal="failed",
            reason_code=HeartbeatReasonCode.EFFECT_ERROR,
        )

    def reconcile_heartbeat_wake(
        self,
        effect_id: str,
        receipt: EffectReceipt,
    ) -> EffectResult:
        """Reconcile one host-acknowledged heartbeat wake.

        A wake is a control effect, not visible contact.  The durable effect
        ledger remains the sole owner of verification; this seam only accepts
        the exact receipt for a ``heartbeat_wake`` intent and projects its
        terminal state into cadence.
        """

        if self.effect_ledger is None:
            raise RuntimeError("effect ledger is unavailable")
        record = self.effect_ledger.get(effect_id)
        if record is None:
            raise ValueError("heartbeat wake effect is unknown")
        if record.kind != "heartbeat_wake":
            raise ValueError("effect is not a heartbeat wake")
        if not isinstance(receipt, EffectReceipt):
            raise TypeError("verified heartbeat wake requires EffectReceipt")
        if not record.created_at <= receipt.observed_at < record.expires_at:
            raise ValueError("heartbeat wake receipt is outside the effect lifetime")
        if record.state == "verified":
            if record.receipt != receipt:
                raise ValueError("conflicting heartbeat wake receipt")
            return self._effect_result(record, "verified")
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("heartbeat wake is not awaiting settlement")
        try:
            verified = self.effect_ledger.verify(effect_id, receipt)
        except Exception as exc:
            raise ValueError("heartbeat wake receipt mismatch") from exc
        return self._effect_result(verified, "verified")

    def _existing_effect(
        self, record: EffectRecord, now: datetime
    ) -> EffectResult | None:
        if record.state == "verified":
            return self._effect_result(record, "verified")
        if record.state in {"pending", "executed_unverified"}:
            if record.expires_at >= now:
                return self._effect_result(
                    record, "queued_unverified", HeartbeatReasonCode.EFFECT_PENDING
                )
            try:
                self.effect_ledger.expire(record.effect_id, now=now)
                record = self.effect_ledger.requeue(
                    record.effect_id, expires_at=now + self.effect_ttl
                )
            except Exception:
                return self._fail_effect(
                    record,
                    "effect_reconciliation_error",
                    "effect_reconciliation_error",
                    HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                    True,
                )
            return self._effect_result(
                record, "requeued", HeartbeatReasonCode.EFFECT_EXPIRED
            )
        if record.state == "expired":
            try:
                record = self.effect_ledger.requeue(
                    record.effect_id, expires_at=now + self.effect_ttl
                )
            except Exception:
                return self._fail_effect(
                    record,
                    "effect_reconciliation_error",
                    "effect_reconciliation_error",
                    HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                    True,
                )
            return self._effect_result(
                record, "requeued", HeartbeatReasonCode.EFFECT_EXPIRED
            )
        if record.state == "requeued":
            return self._effect_result(
                record, "requeued", HeartbeatReasonCode.EFFECT_EXPIRED
            )
        if record.state == "failed":
            return self._effect_result(
                record, record.reason or "failed", HeartbeatReasonCode.EFFECT_ERROR
            )
        return None

    @staticmethod
    def _invoke(
        method: Callable[..., Any],
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        intent: Any,
    ) -> Any:
        try:
            signature = inspect.signature(method)
            params = list(signature.parameters.values())
            positional = [
                p
                for p in params
                if p.kind
                in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }
            ]
            if len(positional) >= 3 or any(
                p.kind is inspect.Parameter.VAR_POSITIONAL for p in params
            ):
                return method(candidate, decision, intent)
            for name in ("intent", "effect", "effect_intent"):
                parameter = signature.parameters.get(name)
                if (
                    parameter is not None
                    and parameter.kind is inspect.Parameter.KEYWORD_ONLY
                ):
                    return method(candidate, decision, **{name: intent})
        except (TypeError, ValueError):
            pass
        return method(candidate, decision)

    def _run_effect(
        self,
        kind: str,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        now: datetime,
    ) -> EffectResult:
        if self.effect_ledger is None:
            return EffectResult(
                False,
                "effect_ledger_unavailable",
                reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
            )
        body = _effect_body(kind, candidate, decision)
        source = (
            candidate.context.get("source_event_id")
            or candidate.context.get("event_id")
            or candidate.candidate_id
        )
        epoch = candidate.context.get("epoch_id", "heartbeat")
        if (
            type(source) is not str
            or not source.strip()
            or type(epoch) is not str
            or not epoch.strip()
        ):
            return EffectResult(
                False,
                "effect_identity_invalid",
                reason_code=HeartbeatReasonCode.EFFECT_ERROR,
            )
        try:
            idempotency_key = f"heartbeat:{source}:{kind}"
            if kind == "delivery" and decision.delivery_mode == "delegated":
                idempotency_key += _DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX
            record = self.effect_ledger.begin_intent(
                kind=f"heartbeat_{kind}",
                source_event_id=source,
                idempotency_key=idempotency_key,
                epoch_id=epoch,
                content_sha256=hashlib.sha256(body).hexdigest(),
                content_length=len(body),
                expires_at=now + self.effect_ttl,
                created_at=now,
            )
        except Exception:
            return EffectResult(
                False,
                "effect_intent_error",
                reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
            )
        remember = getattr(self.cadence, "remember_effect_ref", None)
        if callable(remember):
            try:
                remember(source, f"heartbeat_{kind}", record.effect_id)
            except Exception as exc:
                return EffectResult(
                    False,
                    "effect_reference_projection_error",
                    effect_id=record.effect_id,
                    reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                    degraded=True,
                    projection_errors=(f"effect_reference_write:{type(exc).__name__}",),
                )
        existing = self._existing_effect(record, now)
        if existing is not None:
            return existing
        try:
            record = self.effect_ledger.mark_pending(record.effect_id)
        except Exception:
            return EffectResult(
                False,
                "effect_pending_error",
                effect_id=record.effect_id,
                reason_code=HeartbeatReasonCode.EFFECT_ERROR,
            )
        method = getattr(self.sink, "deliver" if kind == "delivery" else "wake", None)
        if not callable(method):
            return self._fail_effect(
                record,
                "adapter_unavailable",
                "adapter_unavailable",
                HeartbeatReasonCode.ADAPTER_UNAVAILABLE,
                True,
            )
        try:
            raw = self._invoke(method, candidate, decision, record.to_intent())
        except Exception as exc:
            return self._fail_effect(
                record,
                f"{kind}_error:{type(exc).__name__}",
                f"adapter_error:{type(exc).__name__}",
                HeartbeatReasonCode.ADAPTER_ERROR,
                True,
            )
        if isinstance(raw, EffectReceipt):
            adapter = EffectResult(True, "verified", raw, True)
        elif isinstance(raw, EffectResult):
            adapter = raw
        else:
            return self._fail_effect(
                record,
                "adapter_malformed_return",
                "adapter_malformed_return",
                HeartbeatReasonCode.ADAPTER_MALFORMED,
                False,
            )
        if (
            type(adapter.ok) is not bool
            or type(adapter.status) is not str
            or not adapter.status.strip()
        ):
            return self._fail_effect(
                record,
                "adapter_malformed_result",
                "adapter_malformed_result",
                HeartbeatReasonCode.ADAPTER_MALFORMED,
                False,
            )
        if adapter.receipt is not None and not isinstance(
            adapter.receipt, EffectReceipt
        ):
            return self._fail_effect(
                record,
                "adapter_malformed_receipt",
                "adapter_malformed_receipt",
                HeartbeatReasonCode.ADAPTER_MALFORMED,
                False,
            )
        status = adapter.status.strip().lower()
        if adapter.receipt is not None:
            if kind == "delivery" and decision.delivery_mode == "delegated":
                return self._fail_effect(
                    record,
                    "delegated_receipt_not_allowed",
                    "delegated_receipt_not_allowed",
                    HeartbeatReasonCode.ADAPTER_MALFORMED,
                    False,
                )
            if not adapter.ok:
                return self._fail_effect(
                    record,
                    "adapter_rejected",
                    "adapter_rejected",
                    HeartbeatReasonCode.ADAPTER_REJECTED,
                    False,
                )
            try:
                verified = self.effect_ledger.verify(record.effect_id, adapter.receipt)
            except Exception:
                return self._fail_effect(
                    record,
                    "receipt_mismatch",
                    "receipt_mismatch",
                    HeartbeatReasonCode.EFFECT_ERROR,
                    False,
                )
            return self._effect_result(verified, "verified")
        if not adapter.ok:
            unavailable = any(
                word in status for word in ("unavailable", "not_configured", "disabled")
            )
            return self._fail_effect(
                record,
                "adapter_unavailable" if unavailable else "adapter_rejected",
                "adapter_unavailable" if unavailable else "adapter_rejected",
                HeartbeatReasonCode.ADAPTER_UNAVAILABLE
                if unavailable
                else HeartbeatReasonCode.ADAPTER_REJECTED,
                unavailable,
            )
        if status == "verified":
            return self._fail_effect(
                record,
                "missing_receipt",
                "missing_receipt",
                HeartbeatReasonCode.EFFECT_ERROR,
                False,
            )
        if status not in _ACCEPTED:
            return self._fail_effect(
                record,
                "adapter_malformed_status",
                "adapter_malformed_status",
                HeartbeatReasonCode.ADAPTER_MALFORMED,
                False,
            )
        try:
            accepted = self.effect_ledger.mark_queue_accepted(record.effect_id)
        except Exception:
            return self._fail_effect(
                record,
                "effect_queue_accept_error",
                "effect_queue_accept_error",
                HeartbeatReasonCode.EFFECT_ERROR,
                True,
            )
        return self._effect_result(
            accepted, "queued_unverified", HeartbeatReasonCode.EFFECT_PENDING
        )

    def _candidate_existing_effects(
        self, candidate: HeartbeatCandidate, now: datetime
    ) -> tuple[EffectResult | None, EffectResult | None] | None:
        """Reuse pending/terminal effects for a duplicate candidate.

        Cadence stores bounded source/kind references.  Effect reconciliation
        uses only public ledger ports, so a duplicate never invokes private
        replay access or another adapter call.
        """
        if not self._effect_owner_exists():
            return None
        source = (
            candidate.context.get("source_event_id")
            or candidate.context.get("event_id")
            or candidate.candidate_id
        )
        selected: dict[str, EffectRecord] = {}
        precomputed: dict[str, EffectResult] = {}
        terminals: Mapping[str, Any] = {}
        cadence_snapshot = getattr(self.cadence, "snapshot", None)
        if callable(cadence_snapshot):
            try:
                snapshot = cadence_snapshot()
            except AttributeError:
                snapshot = None
            if isinstance(snapshot, Mapping):
                raw_terminals = snapshot.get("effect_terminals", {})
                if isinstance(raw_terminals, Mapping):
                    terminals = raw_terminals
        effect_ref = getattr(self.cadence, "effect_ref", None)
        ledger_get = getattr(self.effect_ledger, "get", None)
        if callable(effect_ref) and callable(ledger_get):
            for kind in ("delivery", "wake"):
                try:
                    effect_id = effect_ref(source, f"heartbeat_{kind}")
                except (AttributeError, TypeError, ValueError):
                    effect_id = None
                if effect_id is None:
                    continue
                terminal = terminals.get(effect_id)
                if terminal in {"pending", "executed_unverified"}:
                    expire = getattr(self.effect_ledger, "expire", None)
                    requeue = getattr(self.effect_ledger, "requeue", None)
                    if callable(expire) and callable(requeue):
                        try:
                            expire(effect_id, now=now)
                        except ValueError as exc:
                            if str(exc).strip().lower() != "effect has not expired":
                                raise StateError(
                                    "effect reconciliation failed"
                                ) from exc
                            precomputed[kind] = EffectResult(
                                True,
                                "queued_unverified",
                                effect_id=effect_id,
                                terminal=terminal,
                                reason_code=HeartbeatReasonCode.EFFECT_PENDING,
                            )
                            continue
                        except Exception as exc:
                            raise StateError("effect reconciliation failed") from exc
                        try:
                            requeued = requeue(
                                effect_id, expires_at=now + self.effect_ttl
                            )
                        except Exception as exc:
                            raise StateError("effect requeue failed") from exc
                        precomputed[kind] = self._effect_result(
                            requeued,
                            "requeued",
                            HeartbeatReasonCode.EFFECT_EXPIRED,
                        )
                        continue
                if terminal == "requeued":
                    precomputed[kind] = EffectResult(
                        True,
                        "requeued",
                        effect_id=effect_id,
                        terminal=terminal,
                        reason_code=HeartbeatReasonCode.EFFECT_EXPIRED,
                    )
                    continue
                if terminal == "failed":
                    precomputed[kind] = EffectResult(
                        False,
                        "failed",
                        effect_id=effect_id,
                        terminal=terminal,
                        reason_code=HeartbeatReasonCode.EFFECT_ERROR,
                    )
                    continue
                try:
                    record = ledger_get(effect_id)
                except Exception as exc:
                    raise StateError("effect ledger replay failed") from exc
                if record is not None:
                    selected[kind] = record
        if not precomputed and callable(
            getattr(self.effect_ledger, "pending_for_reconciliation", None)
        ):
            try:
                pending = self.effect_ledger.pending_for_reconciliation(now=now)
            except Exception as exc:
                raise StateError("effect ledger replay failed") from exc
            for record in pending:
                if record.source_event_id != source:
                    continue
                if record.kind == "heartbeat_delivery":
                    selected.setdefault("delivery", record)
                elif record.kind == "heartbeat_wake":
                    selected.setdefault("wake", record)
        if not selected and not precomputed:
            return None
        results = dict(precomputed)
        results.update(
            {
                kind: self._existing_effect(record, now)
                for kind, record in selected.items()
            }
        )
        if any(result is None for result in results.values()):
            return None
        return results.get("delivery"), results.get("wake")

    def run(
        self,
        candidate: HeartbeatCandidate,
        *,
        session_receipt: SessionHookReceipt | None = None,
        active_chat: bool | None = None,
    ) -> HeartbeatResult:
        if not isinstance(candidate, HeartbeatCandidate):
            raise TypeError("candidate must be a HeartbeatCandidate")
        candidate_id = self._candidate_id(candidate)
        policy, policy_error = self._candidate_policy(candidate.kind)
        if policy_error is not None:
            reason = (
                "kind_disabled"
                if policy is not None and not policy.enabled
                else (
                    "kind_unconfigured"
                    if policy_error.endswith("is unconfigured")
                    else "kind_invalid"
                )
            )
            return self._result(
                "skipped",
                reason,
                candidate_id,
                GateResult(False, "candidate", reason, None),
                code=HeartbeatReasonCode.CANDIDATE_INVALID,
            )
        effective = HeartbeatCandidate(
            candidate.kind,
            dict(candidate.context),
            candidate_id,
            candidate.session_receipt,
        )
        if self._pristine_neutral_probe(
            effective,
            session_receipt=session_receipt,
            policy=policy,
        ):
            return self._run_locked(
                effective,
                session_receipt=session_receipt,
                active_chat=active_chat,
                policy=policy,
            )
        with self.locks.try_exclusive("heartbeat_execution") as acquired:
            if not acquired:
                return self._result(
                    "skipped",
                    "execution_in_progress",
                    candidate_id,
                    GateResult(False, "execution_lock", "execution_in_progress", None),
                    code=HeartbeatReasonCode.EXECUTION_LOCK,
                )
            return self._run_locked(
                effective,
                session_receipt=session_receipt,
                active_chat=active_chat,
                policy=policy,
            )

    def _run_locked(
        self,
        candidate: HeartbeatCandidate,
        *,
        session_receipt: SessionHookReceipt | None,
        active_chat: bool | None,
        policy: HeartbeatKindPolicy,
    ) -> HeartbeatResult:
        candidate_id = candidate.candidate_id
        gate = self._control()
        if not gate.allowed:
            return self._result(
                "skipped",
                gate.reason,
                candidate_id,
                gate,
                code=HeartbeatReasonCode.CONTROL,
            )
        now = self._clock()
        try:
            receipt = self._context_receipt(
                candidate, session_receipt, self.session_receipt
            )
            if candidate.context.get("expires_at") is not None:
                expires = _optional_time(candidate.context["expires_at"], "expires_at")
                assert expires is not None
                if expires <= now:
                    return self._result(
                        "skipped",
                        "candidate_expired",
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.CANDIDATE_INVALID,
                        next_judge_at=self._next_due(candidate, now),
                    )
            events = self._events_present(candidate)
            if receipt is not None and (
                receipt.event_id.strip() or receipt.source_id.strip()
            ):
                events = True
            if not events:
                reconciled = self._reconcile_pending(now)
                if reconciled is not None:
                    reason, _count = reconciled
                    return self._result(
                        "requeued" if reason == "expired_effect" else "pending",
                        reason,
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.EFFECT_EXPIRED
                        if reason == "expired_effect"
                        else HeartbeatReasonCode.EFFECT_PENDING,
                        next_judge_at=now
                        + getattr(
                            self.cadence,
                            "recent_contact_window",
                            DEFAULT_RECENT_CONTACT_WINDOW,
                        ),
                    )
            due, next_due = self._due(candidate, now, policy)
            if not events and not (policy.profile == "daily_anchor" and due):
                return self._result(
                    "neutral",
                    "no_event",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.NO_EVENT,
                    next_judge_at=now
                    + getattr(self.cadence, "judge_interval", DEFAULT_JUDGE_INTERVAL),
                    audit=False,
                    include_snapshot=False,
                )
            if receipt is not None:
                try:
                    self.cadence.record_private_contact(receipt)
                except AttributeError:
                    pass
            if not due:
                return self._result(
                    "skipped",
                    "cadence_not_due",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.NOT_DUE,
                    next_judge_at=next_due,
                )
            blocked, blocked_reason, until = self._cooldown(candidate, now, policy)
            if blocked:
                return self._result(
                    "skipped",
                    "manual_snooze"
                    if blocked_reason == "manual_snooze"
                    else "effect_cooldown",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.COOLDOWN,
                    next_judge_at=until or next_due,
                )
            if "recent_contact" not in policy.bypass:
                contact, _at = self._contact(candidate, now)
                if contact is not None:
                    return self._result(
                        "skipped",
                        contact,
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.RECENT_CONTACT,
                        next_judge_at=now
                        + getattr(
                            self.cadence,
                            "recent_contact_window",
                            DEFAULT_RECENT_CONTACT_WINDOW,
                        ),
                    )
            if "active_chat" not in policy.bypass and self._active_chat(
                candidate, active_chat
            ):
                return self._result(
                    "skipped",
                    "active_chat",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.ACTIVE_CHAT,
                    next_judge_at=now
                    + getattr(
                        self.cadence,
                        "recent_contact_window",
                        DEFAULT_RECENT_CONTACT_WINDOW,
                    ),
                )
            if policy.maintenance_skip:
                decision = JudgeDecision(
                    False,
                    False,
                    "maintenance",
                    maintenance=True,
                )
                try:
                    next_judge = self.cadence.mark_judge(now=now)
                except AttributeError:
                    next_judge = None
                except Exception:
                    return self._result(
                        "failed",
                        "cadence_state_error",
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.CADENCE_ERROR,
                        decision=decision,
                    )
                return self._result(
                    "allowed",
                    "maintenance",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.ALLOWED,
                    decision=decision,
                    next_judge_at=next_judge,
                )
            existing = self._candidate_existing_effects(candidate, now)
            if existing is not None:
                delivery, wake = existing
                effects = [x for x in existing if x is not None]
                if any(not x.ok for x in effects):
                    failure_code = next(
                        (
                            x.reason_code
                            for x in effects
                            if not x.ok and x.reason_code is not None
                        ),
                        HeartbeatReasonCode.EFFECT_ERROR,
                    )
                    return self._result(
                        "failed",
                        "effect_failed",
                        candidate_id,
                        gate,
                        code=failure_code,
                        delivery=delivery,
                        wake=wake,
                        next_judge_at=next_due,
                    )
                if all(x.verified for x in effects):
                    return self._result(
                        "completed",
                        "effects_verified",
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.ALLOWED,
                        delivery=delivery,
                        wake=wake,
                        next_judge_at=next_due,
                    )
                reason = (
                    "expired_effect"
                    if any(x.terminal == "requeued" for x in effects)
                    else "awaiting_receipt"
                )
                return self._result(
                    "requeued" if reason == "expired_effect" else "pending",
                    reason,
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.EFFECT_EXPIRED
                    if reason == "expired_effect"
                    else HeartbeatReasonCode.EFFECT_PENDING,
                    delivery=delivery,
                    wake=wake,
                    next_judge_at=next_due,
                )
            reconciled = self._reconcile_pending(now)
            if reconciled is not None:
                reason, _count = reconciled
                return self._result(
                    "requeued" if reason == "expired_effect" else "pending",
                    reason,
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.EFFECT_EXPIRED
                    if reason == "expired_effect"
                    else HeartbeatReasonCode.EFFECT_PENDING,
                    next_judge_at=now
                    + getattr(
                        self.cadence,
                        "recent_contact_window",
                        DEFAULT_RECENT_CONTACT_WINDOW,
                    ),
                )
        except _CandidateInvalidError:
            return self._result(
                "skipped",
                "candidate_invalid",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.CANDIDATE_INVALID,
            )
        except (StateError, ValueError, TypeError) as exc:
            message = str(exc).lower()
            if "effect" in message or "ledger" in message:
                code, reason = (
                    HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                    "effect_replay_error",
                )
            elif "cadence" in message or "timestamp" in message:
                code, reason = HeartbeatReasonCode.CADENCE_ERROR, "cadence_state_error"
            else:
                code, reason = (
                    HeartbeatReasonCode.CANDIDATE_INVALID,
                    "heartbeat_input_error",
                )
            return self._result(
                "failed",
                reason,
                candidate_id,
                gate,
                code=code,
                projection_errors=(f"pre_effect_gate:{type(exc).__name__}",),
            )
        try:
            raw = self.judge.decide(candidate)
        except Exception as exc:
            return self._result(
                "failed",
                f"judge_error:{type(exc).__name__}",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.JUDGE_ERROR,
            )
        try:
            decision = self._validate_judge(raw)
        except Exception:
            return self._result(
                "failed",
                "judge_malformed",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.JUDGE_MALFORMED,
            )
        anchor_epoch = None
        if policy.profile == "daily_anchor":
            try:
                anchor_epoch = self.cadence.daily_anchor_epoch(now)
            except AttributeError:
                pass
        try:
            next_judge = self.cadence.mark_judge(
                now=now,
                next_judge_at=decision.next_judge_at,
                cadence_minutes=decision.cadence_minutes,
                anchor_epoch=anchor_epoch,
            )
        except AttributeError:
            next_judge = (
                _optional_time(decision.next_judge_at, "next_judge_at")
                if decision.next_judge_at is not None
                else None
            )
        except Exception:
            return self._result(
                "failed",
                "cadence_state_error",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.CADENCE_ERROR,
                decision=decision,
            )
        if not decision.wake_main and not decision.dm_user:
            if decision.allow_autonomy is True or decision.maintenance is True:
                return self._result(
                    "allowed",
                    "allowed",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.ALLOWED,
                    decision=decision,
                    next_judge_at=next_judge,
                )
            return self._result(
                "skipped",
                decision.reason,
                candidate_id,
                gate,
                code=HeartbeatReasonCode.DENIED,
                decision=decision,
                next_judge_at=next_judge,
            )
        delivery = (
            self._run_effect("delivery", candidate, decision, now)
            if decision.dm_user
            else None
        )
        wake = (
            self._run_effect("wake", candidate, decision, now)
            if decision.wake_main
            else None
        )
        effects = [x for x in (delivery, wake) if x is not None]
        if any(not x.ok for x in effects):
            failure_code = next(
                (
                    x.reason_code
                    for x in effects
                    if not x.ok and x.reason_code is not None
                ),
                HeartbeatReasonCode.EFFECT_ERROR,
            )
            return self._result(
                "failed",
                "effect_failed",
                candidate_id,
                gate,
                code=failure_code,
                decision=decision,
                delivery=delivery,
                wake=wake,
                next_judge_at=next_judge,
            )
        if all(x.verified for x in effects):
            return self._result(
                "completed",
                "effects_verified",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.ALLOWED,
                decision=decision,
                delivery=delivery,
                wake=wake,
                next_judge_at=next_judge,
            )
        return self._result(
            "pending",
            "effects_accepted_unverified",
            candidate_id,
            gate,
            code=HeartbeatReasonCode.EFFECT_PENDING,
            decision=decision,
            delivery=delivery,
            wake=wake,
            next_judge_at=next_judge,
        )


__all__ = [
    "CADENCE_SCHEMA_V1",
    "CADENCE_SCHEMA_V2",
    "CADENCE_SCHEMA",
    "HEARTBEAT_CADENCE_SCHEMA",
    "HEARTBEAT_BYPASSES",
    "HeartbeatReasonCode",
    "HeartbeatKindPolicy",
    "HeartbeatCandidate",
    "JudgeDecision",
    "EffectResult",
    "HeartbeatResult",
    "Judge",
    "WakeSink",
    "SilentJudge",
    "NoopWakeSink",
    "HeartbeatCadence",
    "HeartbeatEngine",
]
