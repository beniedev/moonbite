"""Receipt-backed, fail-closed Heartbeat policy and cadence state."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ._heartbeat.cadence import (
    anchor_epoch as _cadence_anchor_epoch,
    apply_silence_backoff as _cadence_apply_silence_backoff,
    clear_automatic_backoff as _cadence_clear_automatic_backoff,
    cooldown as _cadence_cooldown,
    daily_anchor_due as _cadence_daily_anchor_due,
    effect_ref as _cadence_effect_ref,
    effect_ref_key as _cadence_effect_ref_key,
    mark_daily_anchor as _cadence_mark_daily_anchor,
    mark_judge as _cadence_mark_judge,
    next_judge_at as _cadence_next_judge_at,
    observe_private_reply as _cadence_observe_private_reply,
    prune_contact_state as _cadence_prune_contact_state,
    recent_contact as _cadence_recent_contact,
    recent_from_state as _cadence_recent_from_state,
    recent_private_inbound as _cadence_recent_private_inbound,
    record_effect_terminal as _cadence_record_effect_terminal,
    record_private_contact as _cadence_record_private_contact,
    record_verified_visible_contact as _cadence_record_verified_visible_contact,
    remember_effect_ref as _cadence_remember_effect_ref,
    resume as _cadence_resume,
    snapshot as _cadence_snapshot,
    snooze as _cadence_snooze,
)
from ._heartbeat.cadence_codec import (
    CADENCE_SCHEMA as CADENCE_SCHEMA,
    CADENCE_SCHEMA_V1 as CADENCE_SCHEMA_V1,
    CADENCE_SCHEMA_V2 as CADENCE_SCHEMA_V2,
    CADENCE_SCHEMA_V3 as CADENCE_SCHEMA_V3,
    CADENCE_SCHEMA_V4 as CADENCE_SCHEMA_V4,
    HEARTBEAT_CADENCE_SCHEMA as HEARTBEAT_CADENCE_SCHEMA,
    HEARTBEAT_KIND_PATTERN as _HEARTBEAT_KIND_PATTERN,
    SILENCE_BACKOFF_RECEIPT_ID as _SILENCE_BACKOFF_RECEIPT_ID,
    SILENCE_BACKOFF_RECEIPT_MAX as _SILENCE_BACKOFF_RECEIPT_MAX,
    aware as _aware,
    empty_state as _empty_state,
    json_time as _json_time,
    normalise_cadence_state as _normalise_cadence_state,
    normalise_daily_anchor_state as _normalise_daily_anchor_state,
    optional_time as _optional_time,
    serialise_cadence_state as _serialise_cadence_state,
)
from ._heartbeat.observation import (
    cadence_observer_status as _cadence_observer_status,
    engine_observer_status as _engine_observer_status,
)
from ._heartbeat.plans import (
    DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX as _DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX,
    HEARTBEAT_EFFECT_PLAN_SCHEMA as HEARTBEAT_EFFECT_PLAN_SCHEMA,
    effect_plan as _plans_effect_plan,
    effect_plan_for_candidate as _plans_effect_plan_for_candidate,
    effect_plan_for_occurrence as _plans_effect_plan_for_occurrence,
    effect_plan_incomplete as _plans_effect_plan_incomplete,
    effect_plan_rows as _plans_effect_plan_rows,
    ensure_effect_plan as _plans_ensure_effect_plan,
    infer_legacy_plan_public_epoch as _plans_infer_legacy_plan_public_epoch,
    plan_effect as _plans_plan_effect,
    plan_effect_identity as _plans_plan_effect_identity,
    plan_effect_key_matches as _plans_plan_effect_key_matches,
    plan_matches as _plans_plan_matches,
    plan_public_epoch_candidates as _plans_plan_public_epoch_candidates,
    validate_effect_plan as _plans_validate_effect_plan,
    validate_plan_record as _plans_validate_plan_record,
)
from ._heartbeat.recovery import (
    canonical_terminal as _recovery_canonical_terminal,
    candidate_existing_effects as _recovery_candidate_existing_effects,
    effect_failure_audit_statuses as _recovery_effect_failure_audit_statuses,
    existing_effect as _recovery_existing_effect,
    existing_terminal_result as _recovery_existing_terminal_result,
    has_explicit_occurrence as _recovery_has_explicit_occurrence,
    public_epoch_from_effect as _recovery_public_epoch_from_effect,
    replay_effects as _recovery_replay_effects,
    result_public_epoch as _recovery_result_public_epoch,
)
from .control import ControlStore, GateResult, evaluate_gate
from .effects import (
    EffectLedger,
    EffectReceipt,
    EffectRecord,
)
from .observer import ObservationFact, RecoveryEvidence as RecoveryEvidence
from .runtime_core import (
    EventBus,
    FileRuntimeLocks,
    JsonlLedger,
    RuntimeLocks,
    StateError,
    atomic_json_write,
    isoformat,
    new_id,
    utc_now,
)
from .session import SessionHookReceipt

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


class HeartbeatReasonCode(StrEnum):
    NO_EVENT = "no_event"
    NOT_DUE = "not_due"
    COOLDOWN = "cooldown"
    RECENT_CONTACT = "recent_contact"
    ACTIVE_CHAT = "active_chat"
    ACTIVITY_BUSY = "activity_busy"
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
            or not self.bypass <= {"automatic_cooldown", "manual_snooze"}
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


def _accepts_keyword(method: Callable[..., Any], keyword: str) -> bool:
    """Return whether a callable explicitly supports one keyword."""

    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get(keyword)
    return (
        parameter is not None
        and parameter.kind
        in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }
    ) or any(item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values())


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
        "daily_anchor_epochs",
        "daily_anchor_legacy_epoch",
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
        CADENCE_SCHEMA_V4,
    }:
        return None, "unsupported_schema"
    if set(raw) - _CADENCE_OBSERVER_FIELDS:
        return None, "unsupported_fields"
    return raw, None


def _observer_time(raw: Any, label: str) -> datetime | None:
    if raw is None:
        return None
    return _optional_time(raw, label)


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
    epoch_id: str | None = None

    def __post_init__(self) -> None:
        if self.epoch_id is not None and (
            type(self.epoch_id) is not str or not self.epoch_id.strip()
        ):
            raise ValueError("heartbeat epoch_id must be non-empty when provided")

    @property
    def code(self) -> HeartbeatReasonCode | None:
        return self.reason_code

    @property
    def canonical_event_id(self) -> str:
        """The candidate occurrence identity used by the public audit."""

        return self.candidate_id

    @property
    def effects(self) -> tuple[EffectResult, ...]:
        return tuple(x for x in (self.delivery, self.wake) if x is not None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "reason_code": None if self.reason_code is None else self.reason_code.value,
            "candidate_id": self.candidate_id,
            "epoch_id": self.epoch_id,
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
        return _normalise_cadence_state(raw)

    @staticmethod
    def _serialise(state: Mapping[str, Any]) -> dict[str, Any]:
        return _serialise_cadence_state(state)

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
        return _cadence_prune_contact_state(self, state, now)

    def _recent_from_state(
        self,
        state: Mapping[str, Any],
        now: datetime,
        *,
        include_verified_visible: bool = True,
    ) -> tuple[str | None, datetime | None]:
        return _cadence_recent_from_state(
            self,
            state,
            now,
            include_verified_visible=include_verified_visible,
        )

    def snooze(self, minutes: int, *, manual: bool) -> datetime:
        return _cadence_snooze(self, minutes, manual=manual)

    def resume(self) -> None:
        return _cadence_resume(self)

    @staticmethod
    def _clear_automatic_backoff(state: dict[str, Any]) -> None:
        return _cadence_clear_automatic_backoff(state)

    def observe_private_reply(self, observed_at: datetime | None = None) -> None:
        return _cadence_observe_private_reply(self, observed_at)

    def apply_silence_backoff(
        self,
        receipt: HeartbeatSilenceReceipt,
        *,
        policy: Mapping[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically dedupe a settled silence and update cadence cooldown."""
        return _cadence_apply_silence_backoff(
            self,
            receipt,
            receipt_type=HeartbeatSilenceReceipt,
            normalise_policy=_normalise_silence_policy,
            policy=policy,
            now=now,
        )

    def mark_verified_dm(self) -> None:
        self.resume()

    def cooldown(
        self,
        kind: str,
        *,
        now: datetime | None = None,
        bypass: Iterable[str] | None = None,
    ) -> tuple[bool, str, datetime | None]:
        return _cadence_cooldown(
            self,
            kind,
            supported_bypasses=HEARTBEAT_BYPASSES,
            now=now,
            bypass=bypass,
        )

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
        return _cadence_anchor_epoch(self, now)

    def daily_anchor_epoch(self, now: datetime | None = None) -> str:
        return self._anchor_epoch(self._now(now))

    def daily_anchor_due(
        self, now: datetime | None = None, *, kind: str = "daily_anchor"
    ) -> bool:
        return _cadence_daily_anchor_due(self, now, kind=kind)

    def mark_daily_anchor(
        self,
        epoch: str | None = None,
        *,
        kind: str = "daily_anchor",
        now: datetime | None = None,
    ) -> str:
        return _cadence_mark_daily_anchor(self, epoch, kind=kind, now=now)

    def next_judge_at(self, now: datetime | None = None) -> datetime:
        return _cadence_next_judge_at(self, now)

    def mark_judge(
        self,
        *,
        now: datetime | None = None,
        next_judge_at: datetime | str | None = None,
        cadence_minutes: int | None = None,
        anchor_epoch: str | None = None,
        anchor_kind: str | None = None,
    ) -> datetime:
        return _cadence_mark_judge(
            self,
            now=now,
            next_judge_at=next_judge_at,
            cadence_minutes=cadence_minutes,
            anchor_epoch=anchor_epoch,
            anchor_kind=anchor_kind,
        )

    def record_private_contact(
        self,
        receipt: SessionHookReceipt | None = None,
        *,
        source_id: str | None = None,
        observed_at: datetime | None = None,
        fresh: bool = True,
        source_kind: str = "private_inbound",
    ) -> bool:
        return _cadence_record_private_contact(
            self,
            receipt,
            receipt_type=SessionHookReceipt,
            source_id=source_id,
            observed_at=observed_at,
            fresh=fresh,
            source_kind=source_kind,
        )

    def record_verified_visible_contact(
        self,
        record: EffectRecord,
        receipt: EffectReceipt | None = None,
    ) -> bool:
        """Project only a verified heartbeat delivery into contact state."""
        return _cadence_record_verified_visible_contact(
            self,
            record,
            receipt,
            record_type=EffectRecord,
            receipt_type=EffectReceipt,
        )

    def record_effect_terminal(
        self, effect_id: str, terminal: str, *, observed_at: datetime | None = None
    ) -> None:
        return _cadence_record_effect_terminal(
            self,
            effect_id,
            terminal,
            observed_at=observed_at,
        )

    @staticmethod
    def _effect_ref_key(
        source_event_id: str, kind: str, epoch_id: str | None = None
    ) -> str:
        return _cadence_effect_ref_key(source_event_id, kind, epoch_id)

    def remember_effect_ref(
        self,
        source_event_id: str,
        kind: str,
        effect_id: str,
        *,
        epoch_id: str | None = None,
    ) -> None:
        return _cadence_remember_effect_ref(
            self,
            source_event_id,
            kind,
            effect_id,
            epoch_id=epoch_id,
        )

    def effect_ref(
        self,
        source_event_id: str,
        kind: str,
        *,
        epoch_id: str | None = None,
    ) -> str | None:
        return _cadence_effect_ref(
            self,
            source_event_id,
            kind,
            epoch_id=epoch_id,
        )

    def recent_contact(
        self, *, now: datetime | None = None
    ) -> tuple[str | None, datetime | None]:
        return _cadence_recent_contact(self, now=now)

    def recent_private_inbound(
        self, *, now: datetime | None = None
    ) -> tuple[str | None, datetime | None]:
        """Return only recent user-originated private contact.

        Verified outbound delivery remains part of ``recent_contact`` so a
        heartbeat can avoid repeated messages, but it is not user-presence
        evidence for autonomy admission.
        """
        return _cadence_recent_private_inbound(self, now=now)

    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        return _cadence_snapshot(self, now=now)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project cadence state without normalising, pruning, or locking."""

        return _cadence_observer_status(
            path=self.path,
            anchor_epoch=self._anchor_epoch,
            ensure_aware=_aware,
            read_state=_read_cadence_state_lock_free,
            parse_observer_time=_observer_time,
            normalise_daily_anchor_state=_normalise_daily_anchor_state,
            silence_receipt_match=_SILENCE_BACKOFF_RECEIPT_ID.fullmatch,
            silence_receipt_max=_SILENCE_BACKOFF_RECEIPT_MAX,
            target_date=target_date,
            now=now,
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
        cadence_root = self._cadence_root()
        self._effect_plans = (
            JsonlLedger(cadence_root / "heartbeat_effect_plans.jsonl")
            if cadence_root is not None
            else None
        )
        if locks is None:
            if cadence_root is None:
                raise ValueError("locks are required for a pathless cadence")
            self.locks = FileRuntimeLocks(cadence_root)
            self.execution_lock_path = cadence_root / "heartbeat_execution.lock"
        else:
            self.locks, self.execution_lock_path = locks, None
        if self.effect_ledger is None:
            if cadence_root is not None and locks is None:
                self.effect_ledger = EffectLedger(cadence_root, clock=self._clock)

    def _cadence_root(self) -> Path | None:
        try:
            value = getattr(self.cadence, "path")
        except (AttributeError, AssertionError, OSError):
            return None
        return value.parent if isinstance(value, Path) else None

    def _clock(self) -> datetime:
        clock = getattr(self.cadence, "clock", utc_now)
        return _aware(clock() if callable(clock) else utc_now())

    @staticmethod
    def _plan_effect_identity(
        value: Mapping[str, Any], *, label: str = "heartbeat effect plan"
    ) -> dict[str, Any]:
        return _plans_plan_effect_identity(value, label=label)

    @classmethod
    def _validate_effect_plan(cls, value: Mapping[str, Any]) -> dict[str, Any]:
        return _plans_validate_effect_plan(
            value,
            validate_effect=cls._plan_effect_identity,
            infer_public_epoch=cls._infer_legacy_plan_public_epoch,
            effect_key_matches=cls._plan_effect_key_matches,
        )

    def _effect_plan_rows(self) -> tuple[dict[str, Any], ...]:
        return _plans_effect_plan_rows(
            self._effect_plans,
            validate_plan=self._validate_effect_plan,
        )

    @staticmethod
    def _plan_public_epoch_candidates(
        value: Mapping[str, Any],
    ) -> frozenset[str | None]:
        return _plans_plan_public_epoch_candidates(value)

    @classmethod
    def _infer_legacy_plan_public_epoch(
        cls, effects: Iterable[Mapping[str, Any]]
    ) -> str | None:
        return _plans_infer_legacy_plan_public_epoch(
            effects,
            public_epoch_candidates=cls._plan_public_epoch_candidates,
        )

    @staticmethod
    def _plan_effect_key_matches(
        effect: Mapping[str, Any], public_epoch_id: str | None
    ) -> bool:
        return _plans_plan_effect_key_matches(effect, public_epoch_id)

    def _effect_plan(
        self, source_event_id: str, public_epoch_id: str | None
    ) -> dict[str, Any] | None:
        return _plans_effect_plan(
            self._effect_plan_rows(), source_event_id, public_epoch_id
        )

    def _effect_plan_for_candidate(
        self, candidate: HeartbeatCandidate
    ) -> dict[str, Any] | None:
        return _plans_effect_plan_for_candidate(
            candidate,
            candidate_epoch=self._candidate_epoch,
            find_effect_plan=self._effect_plan,
        )

    def _effect_plan_for_occurrence(
        self, occurrence_id: str, epoch_id: str | None
    ) -> dict[str, Any] | None:
        return _plans_effect_plan_for_occurrence(
            self._effect_plan_rows(), occurrence_id, epoch_id
        )

    @staticmethod
    def _plan_effect(
        plan: Mapping[str, Any] | None, kind: str
    ) -> Mapping[str, Any] | None:
        return _plans_plan_effect(plan, kind)

    @staticmethod
    def _plan_matches(
        existing: Mapping[str, Any], candidate: Mapping[str, Any]
    ) -> bool:
        return _plans_plan_matches(existing, candidate)

    def _ensure_effect_plan(
        self,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        now: datetime,
    ) -> dict[str, Any] | None:
        return _plans_ensure_effect_plan(
            self._effect_plans,
            candidate,
            decision,
            now,
            candidate_epoch=self._candidate_epoch,
            effect_body=_effect_body,
            new_effect_id=lambda: new_id("effect"),
            validate_plan=self._validate_effect_plan,
            plans_match=self._plan_matches,
        )

    def _effect_plan_incomplete(self, candidate: HeartbeatCandidate) -> bool:
        return _plans_effect_plan_incomplete(
            candidate,
            find_effect_plan=self._effect_plan_for_candidate,
            effect_ledger=self.effect_ledger,
            cadence=self.cadence,
            validate_record=self._validate_plan_record,
        )

    @staticmethod
    def _validate_plan_record(
        record: EffectRecord, expected: Mapping[str, Any]
    ) -> None:
        _plans_validate_plan_record(record, expected)

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
        return _engine_observer_status(
            cadence=self.cadence,
            effect_ledger=self.effect_ledger,
            target_date=target_date,
            now=now,
        )

    @staticmethod
    def _canonical_terminal(result: HeartbeatResult) -> str | None:
        """Return a terminal only when durable effect truth is settled."""
        return _recovery_canonical_terminal(
            result,
            execution_lock_code=HeartbeatReasonCode.EXECUTION_LOCK,
        )

    @staticmethod
    def _public_epoch_from_effect(record: Any) -> Any:
        """Return the explicit epoch while hiding the legacy ledger default."""
        return _recovery_public_epoch_from_effect(record)

    def _result_public_epoch(self, result: HeartbeatResult) -> str | None:
        """Recover explicit terminal epoch without promoting legacy defaults."""
        return _recovery_result_public_epoch(result)

    def _finish(self, result: HeartbeatResult) -> HeartbeatResult:
        result = replace(result, epoch_id=self._result_public_epoch(result))
        projection_errors = list(result.projection_errors)
        terminal = self._canonical_terminal(result)
        try:
            details = self._audit_details(result)
            details["occurrence_id"] = result.candidate_id
            if terminal is not None and result.effects:
                self._add_effect_audit_identity(result, details)
            if terminal is None:
                self.bus.record_audit(
                    "heartbeat",
                    status=result.status,
                    source="heartbeat",
                    details=details,
                )
            else:
                record_terminal = self.bus.record_audit_terminal
                terminal_kwargs = {
                    "occurrence_id": result.candidate_id,
                    "terminal": terminal,
                    "status": result.status,
                    "source": "heartbeat",
                    "details": details,
                }
                if _accepts_keyword(record_terminal, "epoch_id"):
                    terminal_kwargs["epoch_id"] = result.epoch_id
                elif result.epoch_id is not None:
                    raise StateError("heartbeat audit epoch is unsupported")
                record_terminal("heartbeat", **terminal_kwargs)
        except StateError as exc:
            # A conflicting terminal is an integrity failure, not a benign
            # audit projection error.  Never report the new result as truth.
            projection_errors.append(f"audit_terminal_conflict:{type(exc).__name__}")
            return replace(
                result,
                status="failed",
                reason="terminal_conflict",
                degraded=True,
                projection_errors=tuple(dict.fromkeys(projection_errors)),
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

    def _add_effect_audit_identity(
        self, result: HeartbeatResult, details: dict[str, Any]
    ) -> None:
        effect_ids = sorted(
            effect.effect_id
            for effect in result.effects
            if type(effect.effect_id) is str and effect.effect_id.strip()
        )
        if not effect_ids:
            return
        source_event_id: str | None = None
        public_epoch = result.epoch_id
        effect_epoch = public_epoch or "heartbeat"
        for effect in result.effects:
            if effect.effect_id is None or self.effect_ledger is None:
                continue
            record = self.effect_ledger.get(effect.effect_id)
            if record is None:
                continue
            if source_event_id is None:
                source_event_id = record.source_event_id
            elif source_event_id != record.source_event_id:
                raise StateError("heartbeat audit source identity conflict")
            expected_epoch = record.epoch_id
            if expected_epoch != effect_epoch:
                if result.epoch_id is None and expected_epoch == "heartbeat":
                    continue
                raise StateError("heartbeat audit epoch identity conflict")
        plan = None
        if source_event_id is not None:
            plan = self._effect_plan(source_event_id, public_epoch)
        if plan is not None:
            effect_ids = sorted(effect["effect_id"] for effect in plan["effects"])
            source_event_id = plan["source_event_id"]
        details["effect_ids"] = effect_ids
        if source_event_id is not None:
            details["source_event_id"] = source_event_id
        if len(effect_ids) == 1:
            details["effect_id"] = effect_ids[0]

    @staticmethod
    def _effect_failure_audit_statuses(record: EffectRecord) -> frozenset[str]:
        """Return the bounded failure statuses emitted for one effect."""
        return _recovery_effect_failure_audit_statuses(record)

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
        epoch_id: str | None = None,
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
            epoch_id,
        )
        if not audit:
            return result
        return self._finish(result)

    def _existing_terminal_result(
        self, candidate_id: str, *, epoch_id: str | None = None
    ) -> HeartbeatResult | None:
        ledger = self.effect_ledger
        return _recovery_existing_terminal_result(
            candidate_id,
            epoch_id=epoch_id,
            finder=getattr(self.bus, "find_audit_terminal", None),
            accepts_keyword=_accepts_keyword,
            effect_owner_exists=lambda: (
                self._effect_owner_exists() and ledger is not None
            ),
            effect_records=(
                lambda: tuple(ledger.records()) if ledger is not None else ()
            ),
            find_plan=self._effect_plan_for_occurrence,
            validate_plan_record=self._validate_plan_record,
            public_epoch=self._public_epoch_from_effect,
            failure_audit_statuses=self._effect_failure_audit_statuses,
            canonical_result_terminal=self._canonical_terminal,
            result_type=HeartbeatResult,
            reason_code_type=HeartbeatReasonCode,
        )

    @staticmethod
    def _candidate_id(candidate: HeartbeatCandidate) -> str:
        if candidate.candidate_id.strip():
            return candidate.candidate_id
        for key in ("event_id", "source_event_id", "candidate_id"):
            value = candidate.context.get(key)
            if type(value) is str and value.strip():
                return value
        # An event payload without an explicit source/candidate id is not a
        # durable occurrence identity.  Treat each such invocation as a new
        # attempt; schedulers that need replay must provide candidate_id or
        # source_event_id explicitly.
        return new_id("heartbeat_attempt")

    @staticmethod
    def _candidate_epoch(candidate: HeartbeatCandidate) -> str | None:
        value = candidate.context.get("epoch_id")
        if value is None:
            return None
        if type(value) is not str or not value.strip():
            raise ValueError("epoch_id must be a non-empty string")
        return value

    @staticmethod
    def _has_explicit_occurrence(candidate: HeartbeatCandidate) -> bool:
        return _recovery_has_explicit_occurrence(candidate)

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
            daily_anchor_due = getattr(self.cadence, "daily_anchor_due", None)
            if not callable(daily_anchor_due):
                # Compatibility fallback for a minimal injected cadence port.
                # A durable cadence implementation remains the authority when
                # it exposes daily_anchor_due().
                due = completed is not True
            else:
                try:
                    due = (
                        daily_anchor_due(now, kind=candidate.kind)
                        if _accepts_keyword(daily_anchor_due, "kind")
                        else daily_anchor_due(now)
                    )
                except Exception as exc:
                    raise StateError(
                        "heartbeat cadence daily_anchor_due failed"
                    ) from exc
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
        self,
        record: EffectRecord,
        status: str,
        code: HeartbeatReasonCode | None = None,
        *,
        audit_terminal: bool = False,
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
            terminal = (
                "intentional_silence"
                if record.reason == "intentional_silence"
                else "failed"
            )
            result = EffectResult(
                False,
                status or "failed",
                effect_id=record.effect_id,
                terminal=terminal,
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
        if audit_terminal and record.state in {"verified", "failed"}:
            try:
                self._record_settled_occurrence_terminal(record)
            except StateError:
                raise
            except Exception as exc:
                projection_errors.append(f"audit_write:{type(exc).__name__}")
        if projection_errors:
            result = replace(
                result,
                degraded=True,
                projection_errors=tuple(dict.fromkeys(projection_errors)),
            )
        return result

    def _record_settled_occurrence_terminal(self, record: EffectRecord) -> None:
        """Project a terminal only after every sibling effect is settled."""

        if self.effect_ledger is None:
            raise StateError("effect ledger is unavailable")
        plan = self._effect_plan(
            record.source_event_id, self._public_epoch_from_effect(record)
        )
        if plan is not None:
            siblings_list: list[EffectRecord] = []
            for expected in plan["effects"]:
                candidate = self.effect_ledger.get(expected["effect_id"])
                if candidate is None:
                    return
                self._validate_plan_record(candidate, expected)
                siblings_list.append(candidate)
            siblings = tuple(siblings_list)
            if record.effect_id not in {candidate.effect_id for candidate in siblings}:
                raise StateError("heartbeat occurrence effect is outside its plan")
            occurrence_id = plan["candidate_id"]
        else:
            # A pre-plan history cannot prove that the current effect is the
            # complete occurrence.  An already-written canonical audit may be
            # replayed by the validator, but reconciliation must not create a
            # new aggregate from a singleton or an arbitrary sibling set.
            existing = self._existing_terminal_result(
                record.source_event_id,
                epoch_id=self._public_epoch_from_effect(record),
            )
            if existing is None:
                return
            return
        if any(candidate.state not in {"verified", "failed"} for candidate in siblings):
            return
        # A real failed sibling must remain visible even when another sibling
        # intentionally silenced delivery.  Silence is only the aggregate
        # terminal when no sibling failed.
        real_failure = any(
            candidate.state == "failed" and candidate.reason != "intentional_silence"
            for candidate in siblings
        )
        intentional_silence = any(
            candidate.state == "failed" and candidate.reason == "intentional_silence"
            for candidate in siblings
        )
        terminal = (
            "failed"
            if real_failure
            else "intentional_silence"
            if intentional_silence
            else "verified"
        )
        effect_ids = sorted(candidate.effect_id for candidate in siblings)
        details: dict[str, Any] = {
            "effect_ids": effect_ids,
            "source_event_id": record.source_event_id,
            "reason_code": (
                HeartbeatReasonCode.ALLOWED.value
                if terminal == "verified"
                else HeartbeatReasonCode.DENIED.value
                if terminal == "intentional_silence"
                else HeartbeatReasonCode.EFFECT_ERROR.value
            ),
        }
        if len(effect_ids) == 1:
            details["effect_id"] = effect_ids[0]
        record_terminal = self.bus.record_audit_terminal
        terminal_kwargs = {
            "occurrence_id": occurrence_id,
            "terminal": terminal,
            "status": (
                "intentional_silence"
                if terminal == "intentional_silence"
                else "failed"
                if terminal == "failed"
                else "completed"
            ),
            "source": "heartbeat",
            "details": details,
        }
        public_epoch = self._public_epoch_from_effect(record)
        if _accepts_keyword(record_terminal, "epoch_id"):
            terminal_kwargs["epoch_id"] = public_epoch
        elif public_epoch is not None:
            raise StateError("heartbeat audit epoch is unsupported")
        record_terminal("heartbeat", **terminal_kwargs)

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
        try:
            self._record_settled_occurrence_terminal(record)
        except StateError:
            raise
        except Exception as exc:
            projection_errors.append(f"audit_write:{type(exc).__name__}")
        return EffectResult(
            False,
            status,
            effect_id=record.effect_id,
            terminal=terminal,
            reason_code=reason_code,
            degraded=bool(projection_errors),
            projection_errors=tuple(projection_errors),
        )

    @staticmethod
    def _validate_receipt_time(record: EffectRecord, receipt: EffectReceipt) -> None:
        if not record.created_at <= receipt.observed_at < record.expires_at:
            raise ValueError("heartbeat receipt is outside the effect lifetime")

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
            self._validate_receipt_time(record, receipt)
            if record.state == "verified":
                if record.receipt != receipt:
                    raise ValueError("conflicting delegated delivery receipt")
                return self._effect_result(record, "verified", audit_terminal=True)
            if record.state not in {"pending", "executed_unverified"}:
                raise ValueError("delegated delivery is not awaiting settlement")
            try:
                verified = self.effect_ledger.verify(effect_id, receipt)
            except Exception as exc:
                raise ValueError("delegated delivery receipt mismatch") from exc
            return self._effect_result(verified, "verified", audit_terminal=True)
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
        self._validate_receipt_time(record, receipt)
        if record.state == "verified":
            if record.receipt != receipt:
                raise ValueError("conflicting heartbeat wake receipt")
            return self._effect_result(record, "verified", audit_terminal=True)
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("heartbeat wake is not awaiting settlement")
        try:
            verified = self.effect_ledger.verify(effect_id, receipt)
        except Exception as exc:
            raise ValueError("heartbeat wake receipt mismatch") from exc
        return self._effect_result(verified, "verified", audit_terminal=True)

    def _existing_effect(
        self, record: EffectRecord, now: datetime
    ) -> EffectResult | None:
        return _recovery_existing_effect(
            record,
            now,
            effect_ledger=self.effect_ledger,
            effect_ttl=self.effect_ttl,
            effect_result=self._effect_result,
            fail_effect=self._fail_effect,
            pending_code=HeartbeatReasonCode.EFFECT_PENDING,
            expired_code=HeartbeatReasonCode.EFFECT_EXPIRED,
            replay_error_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
            effect_error_code=HeartbeatReasonCode.EFFECT_ERROR,
        )

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
        *,
        planned: Mapping[str, Any] | None = None,
        prepared: EffectRecord | None = None,
    ) -> EffectResult:
        if self.effect_ledger is None:
            return EffectResult(
                False,
                "effect_ledger_unavailable",
                reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
            )
        try:
            record = prepared or self._prepare_effect_intent(
                kind, candidate, decision, now, planned=planned
            )
        except Exception:
            return EffectResult(
                False,
                "effect_intent_error",
                reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
            )
        if planned is not None:
            try:
                self._validate_plan_record(record, planned)
            except Exception:
                return EffectResult(
                    False,
                    "effect_intent_error",
                    effect_id=record.effect_id,
                    reason_code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
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
                self._validate_receipt_time(record, adapter.receipt)
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
            try:
                current = self.effect_ledger.get(record.effect_id)
            except Exception:
                current = None
            if current is not None and current.state == "verified":
                return self._effect_result(current, "verified")
            if current is not None and current.state == "failed":
                return self._effect_result(
                    current,
                    current.reason or "failed",
                    HeartbeatReasonCode.EFFECT_ERROR,
                )
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

    def _prepare_effect_intent(
        self,
        kind: str,
        candidate: HeartbeatCandidate,
        decision: JudgeDecision,
        now: datetime,
        *,
        planned: Mapping[str, Any] | None = None,
    ) -> EffectRecord:
        """Persist one effect identity before any adapter invocation."""

        if self.effect_ledger is None:
            raise StateError("effect ledger is unavailable")
        body = _effect_body(kind, candidate, decision)
        source = (
            candidate.context.get("source_event_id")
            or candidate.context.get("event_id")
            or candidate.candidate_id
        )
        public_epoch = self._candidate_epoch(candidate)
        epoch = public_epoch or "heartbeat"
        if (
            type(source) is not str
            or not source.strip()
            or type(epoch) is not str
            or not epoch.strip()
        ):
            raise StateError("heartbeat effect identity is invalid")
        idempotency_key = f"heartbeat:{source}:{kind}"
        if public_epoch is not None:
            idempotency_key += f":{public_epoch}"
        if kind == "delivery" and decision.delivery_mode == "delegated":
            idempotency_key += _DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX
        content_sha256 = hashlib.sha256(body).hexdigest()
        content_length = len(body)
        intent_kwargs: dict[str, Any] = {
            "kind": f"heartbeat_{kind}",
            "source_event_id": source,
            "idempotency_key": idempotency_key,
            "epoch_id": epoch,
            "content_sha256": content_sha256,
            "content_length": content_length,
            "expires_at": now + self.effect_ttl,
            "created_at": now,
        }
        if planned is not None:
            if (
                planned["kind"] != f"heartbeat_{kind}"
                or planned["source_event_id"] != source
                or planned["epoch_id"] != epoch
                or planned["idempotency_key"] != idempotency_key
                or planned["content_sha256"] != content_sha256
                or planned["content_length"] != content_length
            ):
                raise StateError("heartbeat effect plan identity conflict")
            intent_kwargs["effect_id"] = planned["effect_id"]
        record = self.effect_ledger.begin_intent(**intent_kwargs)
        if planned is not None:
            self._validate_plan_record(record, planned)
        remember = getattr(self.cadence, "remember_effect_ref", None)
        if callable(remember):
            ref_kwargs = (
                {"epoch_id": public_epoch}
                if _accepts_keyword(remember, "epoch_id")
                else {}
            )
            remember(source, f"heartbeat_{kind}", record.effect_id, **ref_kwargs)
        return record

    def _candidate_existing_effects(
        self, candidate: HeartbeatCandidate, now: datetime
    ) -> tuple[EffectResult | None, EffectResult | None] | None:
        """Reuse pending/terminal effects for a duplicate candidate.

        Cadence stores bounded source/kind references.  Effect reconciliation
        uses only public ledger ports, so a duplicate never invokes private
        replay access or another adapter call.
        """
        return _recovery_candidate_existing_effects(
            candidate,
            now,
            effect_owner_exists=self._effect_owner_exists,
            candidate_epoch=self._candidate_epoch,
            find_plan=self._effect_plan,
            cadence=self.cadence,
            effect_ledger=self.effect_ledger,
            effect_ttl=self.effect_ttl,
            validate_plan_record=self._validate_plan_record,
            resolve_existing_effect=self._existing_effect,
            effect_result=self._effect_result,
            find_existing_terminal=self._existing_terminal_result,
            public_epoch=self._public_epoch_from_effect,
            accepts_keyword=_accepts_keyword,
            result_type=EffectResult,
            pending_code=HeartbeatReasonCode.EFFECT_PENDING,
            expired_code=HeartbeatReasonCode.EFFECT_EXPIRED,
            effect_error_code=HeartbeatReasonCode.EFFECT_ERROR,
        )

    def run(
        self,
        candidate: HeartbeatCandidate,
        *,
        session_receipt: SessionHookReceipt | None = None,
        active_chat: bool | None = None,
        activity_busy: bool | None = None,
    ) -> HeartbeatResult:
        if not isinstance(candidate, HeartbeatCandidate):
            raise TypeError("candidate must be a HeartbeatCandidate")
        candidate_id = self._candidate_id(candidate)
        try:
            candidate_epoch = self._candidate_epoch(candidate)
        except ValueError:
            return self._result(
                "failed",
                "candidate_invalid",
                candidate_id,
                GateResult(False, "candidate", "candidate_invalid", None),
                code=HeartbeatReasonCode.CANDIDATE_INVALID,
            )
        existing_terminal = (
            self._existing_terminal_result(candidate_id, epoch_id=candidate_epoch)
            if self._has_explicit_occurrence(candidate)
            else None
        )
        if existing_terminal is not None:
            return existing_terminal
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
                epoch_id=candidate_epoch,
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
                activity_busy=activity_busy,
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
                    epoch_id=candidate_epoch,
                )
            return self._run_locked(
                effective,
                session_receipt=session_receipt,
                active_chat=active_chat,
                activity_busy=activity_busy,
                policy=policy,
            )

    def _run_locked(
        self,
        candidate: HeartbeatCandidate,
        *,
        session_receipt: SessionHookReceipt | None,
        active_chat: bool | None,
        activity_busy: bool | None,
        policy: HeartbeatKindPolicy,
    ) -> HeartbeatResult:
        candidate_id = candidate.candidate_id
        try:
            public_epoch = self._candidate_epoch(candidate)
        except ValueError:
            return self._result(
                "failed",
                "candidate_invalid",
                candidate_id,
                self._control(),
                code=HeartbeatReasonCode.CANDIDATE_INVALID,
            )

        now = self._clock()
        try:
            existing_effects = self._candidate_existing_effects(candidate, now)
            incomplete_plan = self._effect_plan_incomplete(candidate)
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
                self._control(),
                code=code,
                projection_errors=(f"pre_effect_gate:{type(exc).__name__}",),
                epoch_id=public_epoch,
            )
        gate = self._control()

        def make_result(*args: Any, **kwargs: Any) -> HeartbeatResult:
            kwargs.setdefault("epoch_id", public_epoch)
            return self._result(*args, **kwargs)

        def replay_effects(
            existing: tuple[EffectResult | None, EffectResult | None],
        ) -> HeartbeatResult:
            return _recovery_replay_effects(
                existing,
                make_result=make_result,
                candidate_id=candidate_id,
                gate=gate,
                now=now,
                effect_error_code=HeartbeatReasonCode.EFFECT_ERROR,
                denied_code=HeartbeatReasonCode.DENIED,
                allowed_code=HeartbeatReasonCode.ALLOWED,
                expired_code=HeartbeatReasonCode.EFFECT_EXPIRED,
                pending_code=HeartbeatReasonCode.EFFECT_PENDING,
            )

        if existing_effects is not None:
            return replay_effects(existing_effects)
        if incomplete_plan:
            return make_result(
                "pending",
                "awaiting_effect_intent",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.EFFECT_PENDING,
                next_judge_at=now
                + getattr(
                    self.cadence,
                    "recent_contact_window",
                    DEFAULT_RECENT_CONTACT_WINDOW,
                ),
            )
        if not gate.allowed:
            return make_result(
                "skipped",
                gate.reason,
                candidate_id,
                gate,
                code=HeartbeatReasonCode.CONTROL,
            )
        try:
            reconciled = self._reconcile_pending(now)
            if reconciled is not None:
                reason, _count = reconciled
                return make_result(
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
            receipt = self._context_receipt(
                candidate, session_receipt, self.session_receipt
            )
            if candidate.context.get("expires_at") is not None:
                expires = _optional_time(candidate.context["expires_at"], "expires_at")
                assert expires is not None
                if expires <= now:
                    return make_result(
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
            due, next_due = self._due(candidate, now, policy)
            if not events and not (policy.profile == "daily_anchor" and due):
                return make_result(
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
                return make_result(
                    "skipped",
                    "cadence_not_due",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.NOT_DUE,
                    next_judge_at=next_due,
                )
            blocked, blocked_reason, until = self._cooldown(candidate, now, policy)
            if blocked:
                return make_result(
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
                    return make_result(
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
                return make_result(
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
            busy = (
                candidate.context.get("activity_busy")
                if activity_busy is None
                else activity_busy
            )
            if busy is not None and type(busy) is not bool:
                raise _CandidateInvalidError("activity_busy must be a boolean")
            if busy is True:
                return make_result(
                    "skipped",
                    "activity_busy",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.ACTIVITY_BUSY,
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
                    return make_result(
                        "failed",
                        "cadence_state_error",
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.CADENCE_ERROR,
                        decision=decision,
                    )
                return make_result(
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
                return replay_effects(existing)
        except _CandidateInvalidError:
            return make_result(
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
            return make_result(
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
            return make_result(
                "failed",
                f"judge_error:{type(exc).__name__}",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.JUDGE_ERROR,
            )
        try:
            decision = self._validate_judge(raw)
        except Exception:
            return make_result(
                "failed",
                "judge_malformed",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.JUDGE_MALFORMED,
            )
        anchor_epoch = None
        if policy.profile == "daily_anchor":
            daily_anchor_epoch = getattr(self.cadence, "daily_anchor_epoch", None)
            if callable(daily_anchor_epoch):
                try:
                    anchor_epoch = daily_anchor_epoch(now)
                except Exception:
                    return make_result(
                        "failed",
                        "cadence_state_error",
                        candidate_id,
                        gate,
                        code=HeartbeatReasonCode.CADENCE_ERROR,
                        decision=decision,
                    )
        # Keep the approved effect set durable before consuming cadence. A
        # crash after mark_judge must replay as pending rather than silently
        # skipping a daily anchor whose effect intents were never created.
        try:
            effect_plan = self._ensure_effect_plan(candidate, decision, now)
        except (StateError, TypeError, ValueError) as exc:
            return make_result(
                "failed",
                "effect_replay_error",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                decision=decision,
                projection_errors=(f"effect_plan:{type(exc).__name__}",),
            )
        mark_judge = getattr(self.cadence, "mark_judge", None)
        if callable(mark_judge):
            mark_kwargs = {
                "now": now,
                "next_judge_at": decision.next_judge_at,
                "cadence_minutes": decision.cadence_minutes,
                "anchor_epoch": anchor_epoch,
            }
            if anchor_epoch is not None and _accepts_keyword(mark_judge, "anchor_kind"):
                mark_kwargs["anchor_kind"] = candidate.kind
        if not callable(mark_judge):
            next_judge = (
                _optional_time(decision.next_judge_at, "next_judge_at")
                if decision.next_judge_at is not None
                else None
            )
        else:
            try:
                next_judge = mark_judge(**mark_kwargs)
            except Exception:
                return make_result(
                    "failed",
                    "cadence_state_error",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.CADENCE_ERROR,
                    decision=decision,
                )
        if not decision.wake_main and not decision.dm_user:
            if decision.allow_autonomy is True or decision.maintenance is True:
                return make_result(
                    "allowed",
                    "allowed",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.ALLOWED,
                    decision=decision,
                    next_judge_at=next_judge,
                )
            return make_result(
                "skipped",
                decision.reason,
                candidate_id,
                gate,
                code=HeartbeatReasonCode.DENIED,
                decision=decision,
                next_judge_at=next_judge,
            )
        prepared: dict[str, EffectRecord] = {}
        try:
            for kind, enabled in (
                ("delivery", decision.dm_user),
                ("wake", decision.wake_main),
            ):
                if enabled:
                    prepared[kind] = self._prepare_effect_intent(
                        kind,
                        candidate,
                        decision,
                        now,
                        planned=self._plan_effect(effect_plan, kind),
                    )
        except (StateError, TypeError, ValueError) as exc:
            if effect_plan is not None:
                return make_result(
                    "pending",
                    "awaiting_effect_intent",
                    candidate_id,
                    gate,
                    code=HeartbeatReasonCode.EFFECT_PENDING,
                    decision=decision,
                    next_judge_at=now
                    + getattr(
                        self.cadence,
                        "recent_contact_window",
                        DEFAULT_RECENT_CONTACT_WINDOW,
                    ),
                    projection_errors=(f"effect_intent:{type(exc).__name__}",),
                )
            return make_result(
                "failed",
                "effect_intent_error",
                candidate_id,
                gate,
                code=HeartbeatReasonCode.EFFECT_REPLAY_ERROR,
                decision=decision,
                next_judge_at=next_judge,
                projection_errors=(f"effect_intent:{type(exc).__name__}",),
            )
        delivery = (
            self._run_effect(
                "delivery",
                candidate,
                decision,
                now,
                planned=self._plan_effect(effect_plan, "delivery"),
                prepared=prepared.get("delivery"),
            )
            if decision.dm_user
            else None
        )
        wake = (
            self._run_effect(
                "wake",
                candidate,
                decision,
                now,
                planned=self._plan_effect(effect_plan, "wake"),
                prepared=prepared.get("wake"),
            )
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
            return make_result(
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
            return make_result(
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
        return make_result(
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
    "CADENCE_SCHEMA_V3",
    "CADENCE_SCHEMA_V4",
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
