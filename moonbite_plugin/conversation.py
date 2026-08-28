"""Append-only conversation continuity bridge.

The bridge is deliberately a small sidecar.  It consumes typed session
receipts, records only lifecycle/evidence metadata, and delegates all
checkpoint delivery state to :mod:`moonbite_plugin.effects`.  In particular,
the bridge never accepts or persists a message body.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

from .effects import EFFECT_STATES, EffectLedger, EffectReceipt
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    JsonlLedger,
    StateError,
    as_utc,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)
from .session import (
    SESSION_LIFECYCLE_SCHEMA,
    SessionHookReceipt,
    SessionLifecycleSnapshot,
    SessionLifecycleStore,
)

CONVERSATION_BRIDGE_SCHEMA = "moon.conversation_bridge.v1"
CONVERSATION_SCHEMA = CONVERSATION_BRIDGE_SCHEMA
SCHEMA_VERSION = CONVERSATION_BRIDGE_SCHEMA
CONVERSATION_BRIDGE_KIND = "conversation"

CONVERSATION_OPERATIONS = frozenset(
    {"ignored", "mark_dirty", "mark_settled", "checkpoint_requested", "reconcile"}
)
CHECKPOINT_PENDING_STATES = frozenset(
    {"intent", "pending", "executed_unverified", "requeued"}
)
CHECKPOINT_FAILED_STATES = frozenset({"failed", "expired"})
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REFERENCE_BYTES = 512
_SESSION_SNAPSHOT_MISSING = object()
_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "operation",
        "event_id",
        "lifecycle_id",
        "session_id",
        "cycle_id",
        "source_event_id",
        "source_id",
        "turn_id",
        "source_kind",
        "fresh",
        "observed_at",
        "effect_id",
        "idempotency_key",
        "epoch_id",
        "content_sha256",
        "content_length",
        "expires_at",
        "effect_state",
        "receipt",
    }
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


class ConversationBridgeError(StateError):
    """Raised when conversation bridge state cannot be used safely."""


class ConversationGateError(ConversationBridgeError):
    """Raised when a checkpoint is requested while a gate is closed."""

    def __init__(self, reason: str, snapshot: ConversationSnapshot | None = None):
        self.reason = reason
        self.snapshot = snapshot
        super().__init__(f"checkpoint blocked by {reason}")


def _reference(value: Any, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    ensure_bounded_text(value, label, max_bytes=_MAX_REFERENCE_BYTES)
    return value


def _optional_reference(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _reference(value, label)


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    try:
        return as_utc(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be timezone-aware") from exc


def _parse_timestamp(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise StateError(f"{label} must be an ISO timestamp")
    try:
        return parse_time(value)
    except (StateError, ValueError) as exc:
        raise StateError(f"{label} is invalid") from exc


def _optional_timestamp(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _parse_timestamp(value, label)


def _hash(value: Any, label: str = "content_sha256") -> str:
    if type(value) is not str or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be exactly 64 lowercase hex characters")
    return value


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _scoped_idempotency_key(lifecycle_id: str, cycle_id: str, request_key: str) -> str:
    """Namespace ledger identity without changing the public request key."""

    identity = "\x00".join((lifecycle_id, cycle_id, request_key)).encode("utf-8")
    return f"conversation:{hashlib.sha256(identity).hexdigest()}"


def _optional_bool(value: Any, label: str) -> bool | None:
    if value is not None and type(value) is not bool:
        raise StateError(f"{label} must be a boolean or null")
    return value


def _same_receipt(left: EffectReceipt | None, right: EffectReceipt | None) -> bool:
    if left is None or right is None:
        return left is right
    return left == right


@dataclass(frozen=True, slots=True)
class ConversationSnapshot:
    """Read-only derived conversation and checkpoint state.

    The snapshot has no content field by design.  ``checkpoint_evidence`` is
    only adapter receipt metadata and is empty/null while an effect is not
    verified.
    """

    schema_version: str
    session_id: str
    lifecycle_id: str
    state: str
    dirty: bool
    settled: bool
    unsettled: bool
    active_chat: bool
    open_turn_id: str | None
    settled_turn_ids: tuple[str, ...]
    last_private_at: datetime | None
    last_settled_at: datetime | None
    quiet_until: datetime | None
    overdue_at: datetime | None
    quiet: bool
    overdue: bool
    checkpoint_requested: bool
    checkpoint_state: str
    checkpoint_effect_id: str | None
    checkpoint_evidence: Mapping[str, Any] | None
    can_checkpoint: bool
    blocked_reason: str | None

    schema: ClassVar[str] = CONVERSATION_BRIDGE_SCHEMA

    @property
    def status(self) -> str:
        return self.state

    @property
    def has_open_turn(self) -> bool:
        return self.open_turn_id is not None

    @property
    def checkpoint_pending(self) -> bool:
        return self.checkpoint_state == "checkpoint_pending"

    @property
    def checkpoint_complete(self) -> bool:
        return self.checkpoint_state == "checkpoint_complete"

    @property
    def checkpoint_failed(self) -> bool:
        return self.checkpoint_state == "checkpoint_failed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "lifecycle_id": self.lifecycle_id,
            "state": self.state,
            "dirty": self.dirty,
            "settled": self.settled,
            "unsettled": self.unsettled,
            "active_chat": self.active_chat,
            "open_turn_id": self.open_turn_id,
            "settled_turn_ids": list(self.settled_turn_ids),
            "last_private_at": (
                None
                if self.last_private_at is None
                else isoformat(self.last_private_at)
            ),
            "last_settled_at": (
                None
                if self.last_settled_at is None
                else isoformat(self.last_settled_at)
            ),
            "quiet_until": None
            if self.quiet_until is None
            else isoformat(self.quiet_until),
            "overdue_at": None
            if self.overdue_at is None
            else isoformat(self.overdue_at),
            "quiet": self.quiet,
            "overdue": self.overdue,
            "checkpoint_requested": self.checkpoint_requested,
            "checkpoint_state": self.checkpoint_state,
            "checkpoint_effect_id": self.checkpoint_effect_id,
            "checkpoint_evidence": (
                None
                if self.checkpoint_evidence is None
                else dict(self.checkpoint_evidence)
            ),
            "can_checkpoint": self.can_checkpoint,
            "blocked_reason": self.blocked_reason,
        }


@dataclass(frozen=True, slots=True)
class ConversationReceipt:
    """Receipt returned after a session receipt is accepted or replayed."""

    schema_version: str
    event_id: str
    lifecycle_id: str
    operation: str
    deduplicated: bool
    snapshot: ConversationSnapshot

    @property
    def state(self) -> ConversationSnapshot:
        return self.snapshot

    @property
    def duplicate(self) -> bool:
        return self.deduplicated

    @property
    def is_duplicate(self) -> bool:
        return self.deduplicated


@dataclass(frozen=True, slots=True)
class CheckpointRequest:
    """Result of a checkpoint intent append.

    ``effect`` is the injected :class:`EffectRecord` returned by
    ``EffectLedger.begin_intent``.  Delegating its identity fields keeps the
    result convenient for adapters while retaining the derived bridge view.
    """

    effect: Any
    snapshot: ConversationSnapshot
    request_idempotency_key: str | None = None

    @property
    def effect_id(self) -> str:
        return self.effect.effect_id

    @property
    def state(self) -> str:
        return self.effect.state

    @property
    def kind(self) -> str:
        return self.effect.kind

    @property
    def idempotency_key(self) -> str:
        return self.request_idempotency_key or self.effect.idempotency_key

    @property
    def source_event_id(self) -> str:
        return self.effect.source_event_id

    @property
    def epoch_id(self) -> str:
        return self.effect.epoch_id


@dataclass(frozen=True, slots=True)
class _BridgeEvent:
    operation: str
    event_id: str
    lifecycle_id: str
    session_id: str
    cycle_id: str
    source_event_id: str
    source_id: str | None
    turn_id: str | None
    source_kind: str | None
    fresh: bool | None
    observed_at: datetime
    effect_id: str | None
    idempotency_key: str | None
    epoch_id: str | None
    content_sha256: str | None
    content_length: int | None
    expires_at: datetime | None
    effect_state: str | None
    receipt: EffectReceipt | None

    def to_row(self) -> dict[str, Any]:
        return {
            "schema_version": CONVERSATION_BRIDGE_SCHEMA,
            "kind": CONVERSATION_BRIDGE_KIND,
            "operation": self.operation,
            "event_id": self.event_id,
            "lifecycle_id": self.lifecycle_id,
            "session_id": self.session_id,
            "cycle_id": self.cycle_id,
            "source_event_id": self.source_event_id,
            "source_id": self.source_id,
            "turn_id": self.turn_id,
            "source_kind": self.source_kind,
            "fresh": self.fresh,
            "observed_at": isoformat(self.observed_at),
            "effect_id": self.effect_id,
            "idempotency_key": self.idempotency_key,
            "epoch_id": self.epoch_id,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "expires_at": None
            if self.expires_at is None
            else isoformat(self.expires_at),
            "effect_state": self.effect_state,
            "receipt": None if self.receipt is None else self.receipt.to_dict(),
        }


@dataclass
class _CheckpointState:
    lifecycle_id: str
    cycle_id: str
    effect_id: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    content_sha256: str
    content_length: int
    expires_at: datetime
    request_event_id: str
    effect_state: str
    last_reconciliation: tuple[str, EffectReceipt | None] | None = None


@dataclass
class _ConversationCycle:
    cycle_id: str
    dirty_event_ids: list[str] = field(default_factory=list)
    settled_event_ids: list[str] = field(default_factory=list)
    last_private_at: datetime | None = None
    last_settled_at: datetime | None = None
    checkpoint: _CheckpointState | None = None


@dataclass
class _ConversationState:
    lifecycle_id: str
    session_id: str
    events: dict[str, _BridgeEvent] = field(default_factory=dict)
    cycles: list[_ConversationCycle] = field(default_factory=list)

    @property
    def current_cycle(self) -> _ConversationCycle | None:
        return None if not self.cycles else self.cycles[-1]

    def cycle(self, cycle_id: str) -> _ConversationCycle | None:
        return next(
            (cycle for cycle in self.cycles if cycle.cycle_id == cycle_id),
            None,
        )


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


def _parse_event(row: Mapping[str, Any]) -> _BridgeEvent:
    """Parse one strict bridge row without accepting content fields."""

    if set(row) != _ROW_FIELDS:
        raise StateError("conversation bridge row has invalid fields")
    if row["schema_version"] != CONVERSATION_BRIDGE_SCHEMA:
        raise StateError("conversation bridge row has an unsupported schema")
    if row["kind"] != CONVERSATION_BRIDGE_KIND:
        raise StateError("conversation bridge row has an unsupported kind")

    operation = row["operation"]
    if type(operation) is not str or operation not in CONVERSATION_OPERATIONS:
        raise StateError("conversation bridge row operation is invalid")
    event_id = _reference(row["event_id"], "event_id")
    lifecycle_id = _reference(row["lifecycle_id"], "lifecycle_id")
    session_id = _reference(row["session_id"], "session_id")
    cycle_id = _reference(row["cycle_id"], "cycle_id")
    source_event_id = _reference(row["source_event_id"], "source_event_id")
    source_id = _optional_reference(row["source_id"], "source_id")
    turn_id = _optional_reference(row["turn_id"], "turn_id")
    source_kind = row["source_kind"]
    if source_kind is not None:
        source_kind = _reference(source_kind, "source_kind")
    fresh = _optional_bool(row["fresh"], "fresh")
    observed_at = _parse_timestamp(row["observed_at"], "observed_at")
    effect_id = _optional_reference(row["effect_id"], "effect_id")
    idempotency_key = _optional_reference(row["idempotency_key"], "idempotency_key")
    epoch_id = _optional_reference(row["epoch_id"], "epoch_id")
    raw_hash = row["content_sha256"]
    content_sha256 = None if raw_hash is None else _hash(raw_hash)
    raw_length = row["content_length"]
    content_length = (
        None if raw_length is None else _positive_int(raw_length, "content_length")
    )
    expires_at = _optional_timestamp(row["expires_at"], "expires_at")
    effect_state = row["effect_state"]
    if effect_state is not None and effect_state not in EFFECT_STATES:
        raise StateError("conversation bridge effect state is invalid")

    raw_receipt = row["receipt"]
    receipt: EffectReceipt | None
    if raw_receipt is None:
        receipt = None
    elif isinstance(raw_receipt, Mapping):
        try:
            receipt = EffectReceipt.from_dict(dict(raw_receipt))
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise StateError("conversation bridge receipt is invalid") from exc
    else:
        raise StateError("conversation bridge receipt must be an object or null")

    event = _BridgeEvent(
        operation=operation,
        event_id=event_id,
        lifecycle_id=lifecycle_id,
        session_id=session_id,
        cycle_id=cycle_id,
        source_event_id=source_event_id,
        source_id=source_id,
        turn_id=turn_id,
        source_kind=source_kind,
        fresh=fresh,
        observed_at=observed_at,
        effect_id=effect_id,
        idempotency_key=idempotency_key,
        epoch_id=epoch_id,
        content_sha256=content_sha256,
        content_length=content_length,
        expires_at=expires_at,
        effect_state=effect_state,
        receipt=receipt,
    )
    if operation in {"mark_dirty", "mark_settled"}:
        if (
            source_id is None
            or (turn_id is None and operation == "mark_settled")
            or source_kind is None
            or fresh is None
            or any(
                value is not None
                for value in (
                    effect_id,
                    idempotency_key,
                    epoch_id,
                    content_sha256,
                    content_length,
                    expires_at,
                    effect_state,
                    receipt,
                )
            )
        ):
            raise StateError("conversation observation row has invalid effect fields")
    elif operation == "ignored":
        if (
            source_id is None
            or source_kind is None
            or fresh is None
            or any(
                value is not None
                for value in (
                    effect_id,
                    idempotency_key,
                    epoch_id,
                    content_sha256,
                    content_length,
                    expires_at,
                    effect_state,
                    receipt,
                )
            )
        ):
            raise StateError("ignored session row has invalid effect fields")
    else:
        if (
            source_id is not None
            or turn_id is not None
            or source_kind is not None
            or fresh is not None
            or effect_id is None
            or idempotency_key is None
            or epoch_id is None
            or content_sha256 is None
            or content_length is None
            or expires_at is None
            or effect_state is None
        ):
            raise StateError("conversation checkpoint row has invalid fields")
        if operation == "checkpoint_requested" and receipt is not None:
            raise StateError("checkpoint request cannot carry receipt evidence")
        if operation == "reconcile" and effect_state == "verified" and receipt is None:
            raise StateError("verified reconciliation requires receipt evidence")
        if (
            operation == "reconcile"
            and effect_state != "verified"
            and receipt is not None
        ):
            raise StateError("unverified reconciliation cannot carry receipt evidence")
    return event


def _validate_session_receipt(
    receipt: SessionHookReceipt,
) -> tuple[bool, bool]:
    """Return (counts_as_private_contact, is_settled_turn).

    The second value is intentionally stricter than receipt.settled: only a
    post-LLM receipt whose immutable snapshot contains the turn in its settled
    set is accepted as settlement evidence.
    """

    if not isinstance(receipt, SessionHookReceipt):
        raise TypeError("receipt must be a SessionHookReceipt")
    if receipt.schema_version != SESSION_LIFECYCLE_SCHEMA:
        raise ConversationBridgeError("session receipt has an unsupported schema")
    if receipt.lifecycle_id != receipt.context.lifecycle_id:
        raise ConversationBridgeError("session receipt lifecycle identity conflicts")
    if receipt.source_id != receipt.context.source_id:
        raise ConversationBridgeError("session receipt source identity conflicts")
    _reference(receipt.event_id, "event_id")
    if receipt.context.source_kind == "private_inbound" and receipt.hook not in {
        "pre_gateway_dispatch",
        "pre_llm_call",
    }:
        raise ConversationBridgeError(
            "private inbound contact requires a dispatch or pre_llm receipt"
        )
    if (
        receipt.context.source_kind == "assistant_response"
        and receipt.hook != "post_llm_call"
    ):
        raise ConversationBridgeError(
            "assistant response evidence requires post_llm_call"
        )
    if (
        receipt.hook == "post_llm_call"
        and receipt.context.source_kind != "assistant_response"
    ):
        raise ConversationBridgeError(
            "post_llm_call evidence requires assistant_response"
        )
    snapshot = receipt.snapshot
    if not isinstance(snapshot, SessionLifecycleSnapshot):
        raise ConversationBridgeError("session receipt snapshot is invalid")
    if (
        snapshot.lifecycle_id != receipt.lifecycle_id
        or snapshot.session_id != receipt.context.session_id
        or snapshot.supported_hooks != receipt.context.supported_hooks
    ):
        raise ConversationBridgeError("session receipt snapshot identity conflicts")
    if receipt.hook == "post_llm_call" and receipt.settled:
        turn_id = receipt.turn_id
        if (
            turn_id is None
            or receipt.context.source_kind != "assistant_response"
            or snapshot.open_turn_id is not None
            or turn_id not in snapshot.settled_turn_ids
        ):
            raise ConversationBridgeError(
                "settled evidence requires a settled post_llm_call snapshot"
            )
        return receipt.context.counts_as_private_contact, True
    if receipt.settled:
        raise ConversationBridgeError(
            "only a settled post_llm_call receipt may settle a turn"
        )
    return receipt.context.counts_as_private_contact, False


class ConversationBridge:
    """Replayable dirty/settled/checkpoint bridge.

    SessionLifecycleStore and EffectLedger are dependencies rather than
    alternate writers.  The bridge owns only conversation_bridge.jsonl and
    one mutation lock.  Every public read replays that ledger while holding
    the lock.
    """

    def __init__(
        self,
        root: Path,
        session_store: SessionLifecycleStore | None = None,
        effect_ledger: EffectLedger | None = None,
        *,
        sessions: SessionLifecycleStore | None = None,
        effects: EffectLedger | None = None,
        quiet_window: timedelta = timedelta(minutes=5),
        overdue_window: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if (
            session_store is not None
            and sessions is not None
            and session_store is not sessions
        ):
            raise TypeError("session_store and sessions name different owners")
        if (
            effect_ledger is not None
            and effects is not None
            and effect_ledger is not effects
        ):
            raise TypeError("effect_ledger and effects name different owners")
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "conversation_bridge.jsonl")
        self.path = self.ledger.path
        self.mutation_lock = self.root / "conversation_bridge.mutation.lock"
        self.mutation_lock_path = self.mutation_lock
        self.session_store = session_store if session_store is not None else sessions
        self.effect_ledger = effect_ledger if effect_ledger is not None else effects
        if self.session_store is None:
            self.session_store = SessionLifecycleStore(self.root)
        if self.effect_ledger is None:
            self.effect_ledger = EffectLedger(self.root, clock=clock)
        self.quiet_window = self._window(quiet_window, "quiet_window")
        self.overdue_window = self._window(overdue_window, "overdue_window")
        self.clock = clock

    @staticmethod
    def _window(value: timedelta, label: str) -> timedelta:
        if not isinstance(value, timedelta) or value.total_seconds() < 0:
            raise ValueError(f"{label} must be a non-negative timedelta")
        return value

    @staticmethod
    def _new_state(event: _BridgeEvent) -> _ConversationState:
        return _ConversationState(
            lifecycle_id=event.lifecycle_id,
            session_id=event.session_id,
        )

    @staticmethod
    def _same_checkpoint_identity(
        checkpoint: _CheckpointState,
        event: _BridgeEvent,
    ) -> bool:
        return (
            checkpoint.cycle_id == event.cycle_id
            and checkpoint.effect_id == event.effect_id
            and checkpoint.source_event_id == event.source_event_id
            and checkpoint.idempotency_key == event.idempotency_key
            and checkpoint.epoch_id == event.epoch_id
            and checkpoint.content_sha256 == event.content_sha256
            and checkpoint.content_length == event.content_length
            and checkpoint.expires_at == event.expires_at
        )

    def _checkpoint_is_active(self, checkpoint: _CheckpointState) -> bool:
        effect = self._effect_record(checkpoint)
        return effect.state in CHECKPOINT_PENDING_STATES

    def _checkpoint_for_reconcile(
        self,
        state: _ConversationState,
    ) -> tuple[_ConversationCycle, _CheckpointState] | None:
        for cycle in reversed(state.cycles):
            checkpoint = cycle.checkpoint
            if checkpoint is not None and self._checkpoint_is_active(checkpoint):
                return cycle, checkpoint
        for cycle in reversed(state.cycles):
            if cycle.checkpoint is not None:
                return cycle, cycle.checkpoint
        return None

    @staticmethod
    def _new_cycle(state: _ConversationState, cycle_id: str) -> _ConversationCycle:
        if state.cycle(cycle_id) is not None:
            raise StateError("conversation cycle_id already exists")
        cycle = _ConversationCycle(cycle_id=cycle_id)
        state.cycles.append(cycle)
        return cycle

    def _current_cycle_for_dirty(
        self,
        state: _ConversationState,
        cycle_id: str,
    ) -> _ConversationCycle:
        current = state.current_cycle
        existing = state.cycle(cycle_id)
        if current is None:
            if existing is not None:
                raise StateError("conversation cycle replay is out of order")
            return self._new_cycle(state, cycle_id)
        if existing is current:
            if current.checkpoint is not None:
                raise StateError("new dirty event must start a new conversation cycle")
            return current
        if existing is not None:
            raise StateError("conversation cycle replay is out of order")
        if current.checkpoint is None:
            raise StateError("conversation cycle_id changed without checkpoint")
        return self._new_cycle(state, cycle_id)

    def _apply_event(self, state: _ConversationState, event: _BridgeEvent) -> None:
        if event.lifecycle_id != state.lifecycle_id:
            raise StateError("conversation bridge lifecycle_id changed")
        if event.session_id != state.session_id:
            raise StateError("conversation bridge session_id changed")
        if event.event_id in state.events:
            raise StateError("duplicate conversation bridge event_id")

        if event.operation == "ignored":
            # Preserve only the receipt identity.  Ignored lifecycle sources
            # must not create or mutate a conversation cycle.
            pass

        elif event.operation == "mark_dirty":
            if event.source_kind != "private_inbound" or event.fresh is not True:
                raise StateError("only fresh private inbound receipts may mark dirty")
            cycle = self._current_cycle_for_dirty(state, event.cycle_id)
            if (
                cycle.last_private_at is not None
                and event.observed_at < cycle.last_private_at
            ):
                raise StateError("conversation dirty events are out of order")
            cycle.dirty_event_ids.append(event.event_id)
            cycle.last_private_at = event.observed_at

        elif event.operation == "mark_settled":
            if event.source_kind != "assistant_response" or event.turn_id is None:
                raise StateError(
                    "settlement must come from post_llm assistant evidence"
                )
            cycle = state.cycle(event.cycle_id)
            if cycle is None:
                current = state.current_cycle
                if current is not None and current.checkpoint is None:
                    raise StateError("settlement references an unknown cycle")
                cycle = self._new_cycle(state, event.cycle_id)
            elif cycle is not state.current_cycle:
                raise StateError("settlement for an old conversation cycle")
            already_settled = any(
                state.events[event_id].turn_id == event.turn_id
                for event_id in cycle.settled_event_ids
            )
            if (
                cycle.checkpoint is not None
                or already_settled
                or (
                    cycle.last_settled_at is not None
                    and event.observed_at < cycle.last_settled_at
                )
            ):
                raise StateError("conversation settlement is out of order")
            cycle.settled_event_ids.append(event.event_id)
            cycle.last_settled_at = event.observed_at

        elif event.operation == "checkpoint_requested":
            cycle = state.cycle(event.cycle_id)
            if cycle is None or cycle is not state.current_cycle:
                raise StateError("checkpoint request references an unknown cycle")
            if cycle.checkpoint is not None:
                raise StateError("conversation has more than one checkpoint request")
            if any(
                candidate.checkpoint is not None
                and self._checkpoint_is_active(candidate.checkpoint)
                for candidate in state.cycles
            ):
                raise StateError("another checkpoint is still active")
            if not cycle.dirty_event_ids:
                raise StateError(
                    "checkpoint request has no dirty conversation evidence"
                )
            if cycle.last_settled_at is None or (
                cycle.last_private_at is not None
                and cycle.last_private_at > cycle.last_settled_at
            ):
                raise StateError("checkpoint request has an unsettled conversation")
            if event.effect_state not in EFFECT_STATES:
                raise StateError("checkpoint request has an invalid effect state")
            assert event.effect_id is not None
            assert event.idempotency_key is not None
            assert event.epoch_id is not None
            assert event.content_sha256 is not None
            assert event.content_length is not None
            assert event.expires_at is not None
            if event.expires_at <= event.observed_at:
                raise StateError("checkpoint expiry must be in the future")
            cycle.checkpoint = _CheckpointState(
                lifecycle_id=event.lifecycle_id,
                cycle_id=event.cycle_id,
                effect_id=event.effect_id,
                source_event_id=event.source_event_id,
                idempotency_key=event.idempotency_key,
                epoch_id=event.epoch_id,
                content_sha256=event.content_sha256,
                content_length=event.content_length,
                expires_at=event.expires_at,
                request_event_id=event.event_id,
                effect_state=event.effect_state,
            )

        elif event.operation == "reconcile":
            cycle = state.cycle(event.cycle_id)
            checkpoint = None if cycle is None else cycle.checkpoint
            if checkpoint is None or event.effect_id != checkpoint.effect_id:
                raise StateError("reconciliation has no matching checkpoint request")
            if not self._same_checkpoint_identity(checkpoint, event):
                raise StateError("reconciliation changes checkpoint identity")
            if event.effect_state not in EFFECT_STATES:
                raise StateError("reconciliation state is invalid")
            signature = (event.effect_state, event.receipt)
            previous = checkpoint.last_reconciliation
            if previous is not None:
                previous_state, previous_receipt = previous
                if previous_state == event.effect_state and _same_receipt(
                    previous_receipt, event.receipt
                ):
                    raise StateError("duplicate checkpoint reconciliation")
                if previous_state == "verified":
                    raise StateError("verified checkpoint cannot be reconciled again")
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
                    raise StateError("checkpoint reconciliation moved backwards")
            checkpoint.effect_state = event.effect_state
            checkpoint.last_reconciliation = signature

        state.events[event.event_id] = event

    def _replay_unlocked(self) -> dict[str, _ConversationState]:
        states: dict[str, _ConversationState] = {}
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            try:
                event = _parse_event(row)
                state = states.get(event.lifecycle_id)
                if state is None:
                    state = self._new_state(event)
                    states[event.lifecycle_id] = state
                self._apply_event(state, event)
            except (ConversationBridgeError, StateError, TypeError, ValueError) as exc:
                raise StateError(
                    f"conversation_bridge.jsonl row {row_number} is invalid"
                ) from exc
        return states

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
        active_chat = open_turn_id is not None or unsettled
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
    def _receipt_event_matches(
        event: _BridgeEvent,
        receipt: SessionHookReceipt,
        operation: str,
    ) -> bool:
        context = receipt.context
        return (
            event.operation == operation
            and event.source_event_id == receipt.event_id
            and event.source_id == context.source_id
            and event.turn_id == context.turn_id
            and event.source_kind == context.source_kind
            and event.fresh == context.fresh
            and event.observed_at == context.observed_at
        )

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

    def observe(self, receipt: SessionHookReceipt) -> ConversationReceipt:
        """Consume one typed session receipt without storing its body."""

        counts_private, settles_turn = _validate_session_receipt(receipt)
        operation = (
            "mark_dirty"
            if counts_private
            else "mark_settled"
            if settles_turn
            else "ignored"
        )
        effective_now = _aware(self.clock(), "clock")
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            state = states.get(receipt.lifecycle_id)
            if state is None:
                state = _ConversationState(
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=receipt.context.session_id,
                )
                states[receipt.lifecycle_id] = state
            existing = state.events.get(receipt.event_id)
            deduplicated = existing is not None
            if existing is not None:
                if not self._receipt_event_matches(existing, receipt, operation):
                    raise ConversationBridgeError(
                        "session receipt conflicts with bridge evidence"
                    )
            elif operation == "ignored":
                context = receipt.context
                current = state.current_cycle
                cycle_id = (
                    current.cycle_id
                    if current is not None
                    else "ignored:"
                    + hashlib.sha256(receipt.event_id.encode("utf-8")).hexdigest()
                )
                event = _BridgeEvent(
                    operation=operation,
                    event_id=receipt.event_id,
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=context.session_id,
                    cycle_id=cycle_id,
                    source_event_id=receipt.event_id,
                    source_id=context.source_id,
                    turn_id=context.turn_id,
                    source_kind=context.source_kind,
                    fresh=context.fresh,
                    observed_at=context.observed_at,
                    effect_id=None,
                    idempotency_key=None,
                    epoch_id=None,
                    content_sha256=None,
                    content_length=None,
                    expires_at=None,
                    effect_state=None,
                    receipt=None,
                )
                self._apply_event(state, event)
                self.ledger.append(event.to_row())
            elif operation != "ignored":
                context = receipt.context
                current = state.current_cycle
                if current is None or current.checkpoint is not None:
                    cycle_id = new_id("conversation_cycle")
                else:
                    cycle_id = current.cycle_id
                event = _BridgeEvent(
                    operation=operation,
                    event_id=receipt.event_id,
                    lifecycle_id=receipt.lifecycle_id,
                    session_id=context.session_id,
                    cycle_id=cycle_id,
                    source_event_id=receipt.event_id,
                    source_id=context.source_id,
                    turn_id=context.turn_id,
                    source_kind=context.source_kind,
                    fresh=context.fresh,
                    observed_at=context.observed_at,
                    effect_id=None,
                    idempotency_key=None,
                    epoch_id=None,
                    content_sha256=None,
                    content_length=None,
                    expires_at=None,
                    effect_state=None,
                    receipt=None,
                )
                self._apply_event(state, event)
                # Do not expose a state transition until the append succeeds.
                self.ledger.append(event.to_row())
            snapshot = self._build_snapshot(state, now=effective_now)
            return ConversationReceipt(
                schema_version=CONVERSATION_BRIDGE_SCHEMA,
                event_id=receipt.event_id,
                lifecycle_id=receipt.lifecycle_id,
                operation=operation,
                deduplicated=deduplicated,
                snapshot=snapshot,
            )

    def _snapshot_unlocked(
        self,
        lifecycle_id: str,
        *,
        now: datetime,
        states: Mapping[str, _ConversationState] | None = None,
    ) -> ConversationSnapshot | None:
        _reference(lifecycle_id, "lifecycle_id")
        actual_states = self._replay_unlocked() if states is None else states
        state = actual_states.get(lifecycle_id)
        if state is None:
            return None
        return self._build_snapshot(state, now=now)

    def snapshot(
        self,
        lifecycle_id: str,
        *,
        now: datetime | None = None,
    ) -> ConversationSnapshot | None:
        effective_now = self.clock() if now is None else now
        with file_lock(self.mutation_lock):
            return self._snapshot_unlocked(
                lifecycle_id,
                now=effective_now,
            )

    def evaluate(
        self,
        lifecycle_id: str,
        *,
        now: datetime | None = None,
    ) -> ConversationSnapshot | None:
        """Read-only gate evaluation for heartbeat/autonomy callers."""

        return self.snapshot(lifecycle_id, now=now)

    def snapshots(
        self,
        *,
        now: datetime | None = None,
    ) -> tuple[ConversationSnapshot, ...]:
        effective_now = self.clock() if now is None else now
        with file_lock(self.mutation_lock):
            states = self._replay_unlocked()
            return tuple(
                self._build_snapshot(states[lifecycle_id], now=effective_now)
                for lifecycle_id in sorted(states)
            )

    def replay(self) -> tuple[ConversationSnapshot, ...]:
        """Validate and return all durable bridge snapshots."""

        return self.snapshots()

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


__all__ = [
    "CHECKPOINT_FAILED_STATES",
    "CHECKPOINT_PENDING_STATES",
    "CONVERSATION_BRIDGE_KIND",
    "CONVERSATION_BRIDGE_SCHEMA",
    "CONVERSATION_OPERATIONS",
    "CONVERSATION_SCHEMA",
    "SCHEMA_VERSION",
    "CheckpointRequest",
    "ConversationBridge",
    "ConversationBridgeError",
    "ConversationGateError",
    "ConversationReceipt",
    "ConversationSnapshot",
]
