"""Conversation bridge event and state contracts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from ..effects import EFFECT_STATES, EffectReceipt
from ..runtime_core import (
    StateError,
    as_utc,
    ensure_bounded_text,
    isoformat,
    parse_time,
)
from ..session import (
    SESSION_LIFECYCLE_SCHEMA,
    SessionHookReceipt,
    SessionLifecycleSnapshot,
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
