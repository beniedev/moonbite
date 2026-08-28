"""Append-only, receipt-backed effect intent ledger.

The ledger records intent and observable state, but deliberately never stores
effect content.  A queue acknowledgement is evidence that a transport
accepted work, not evidence that a user saw it; only a matching delivery
receipt can move an effect to ``verified``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, ClassVar

from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    JsonlLedger,
    StateError,
    ensure_bounded_text,
    file_lock,
    isoformat,
    new_id,
    parse_time,
    utc_now,
)

EFFECT_SCHEMA = "moon.effect.v1"
EFFECT_RECEIPT_SCHEMA = "moon.effect.receipt.v1"
RECEIPT_SCHEMA = EFFECT_RECEIPT_SCHEMA
EFFECT_STATES = frozenset(
    {
        "intent",
        "pending",
        "executed_unverified",
        "verified",
        "failed",
        "expired",
        "requeued",
    }
)
EFFECT_KIND = "delivery_receipt"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_ID_MAX_BYTES = 512
_REASON_MAX_BYTES = 4 * 1024
_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "receipt_id",
        "event_id",
        "observed_at",
        "content_sha256",
        "content_length",
        "epoch_id",
    }
)
_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "operation",
        "effect_id",
        "kind",
        "source_event_id",
        "idempotency_key",
        "epoch_id",
        "attempt",
        "state",
        "created_at",
        "expires_at",
        "content_sha256",
        "content_length",
        "observed_at",
        "receipt",
        "reason",
        "retryable",
    }
)


def _nonempty_text(value: Any, label: str, *, max_bytes: int = _ID_MAX_BYTES) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    ensure_bounded_text(value, label, max_bytes=max_bytes)
    return value


def _aware_datetime(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be timezone-aware") from exc
    return value


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _content_hash(value: Any) -> str:
    if type(value) is not str or _HASH_RE.fullmatch(value) is None:
        raise ValueError("content_sha256 must be exactly 64 lowercase hex characters")
    return value


def _optional_bool(value: Any, label: str) -> bool | None:
    if value is not None and type(value) is not bool:
        raise ValueError(f"{label} must be a boolean or null")
    return value


def _empty_receipt() -> dict[str, Any]:
    """Return explicit null evidence for an unverified effect."""

    return {
        "schema_version": EFFECT_RECEIPT_SCHEMA,
        "kind": EFFECT_KIND,
        "receipt_id": None,
        "event_id": None,
        "observed_at": None,
        "content_sha256": None,
        "content_length": None,
        "epoch_id": None,
    }


def _parse_time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise ValueError(f"{label} must be an ISO timestamp")
    try:
        return parse_time(value)
    except (StateError, ValueError) as exc:
        raise ValueError(f"{label} must be an ISO timestamp") from exc


def _read_effect_history_lock_free(
    path: Path,
) -> tuple[tuple[EffectRecord, ...], str | None]:
    """Read an existing effect ledger without creating or acquiring a lock.

    ``JsonlLedger.rows`` is intentionally unsuitable for an observer port:
    its lock helper creates the parent directory and adjacent lock on first
    read.  Owner status is allowed to see a file that already exists, but it
    must not make durable state merely by asking for status.  Malformed or
    out-of-order rows are omitted from the usable history and reported to the
    caller as a content-free integrity code.  Once a row identifies an effect
    but fails replay validation, that effect's chain is quarantined for the
    rest of this read: later rows must not turn a broken ledger into a neutral
    ``verified`` fact.
    """

    if not path.exists():
        return (), None
    parsed: list[EffectRecord] = []
    records: dict[str, EffectRecord] = {}
    idempotency: dict[str, str] = {}
    broken_effects: set[str] = set()
    integrity: str | None = None
    operations = {
        "begin_intent",
        "mark_pending",
        "mark_queue_accepted",
        "verify",
        "fail",
        "expire",
        "requeue",
    }
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
                except Exception as exc:
                    integrity = integrity or f"json_{type(exc).__name__}"
                    continue
                if not isinstance(value, Mapping):
                    integrity = integrity or "row_type"
                    continue
                row_effect_id = value.get("effect_id")
                if type(row_effect_id) is not str or not row_effect_id:
                    row_effect_id = None
                try:
                    record = _record_from_row(value, row_number=None)
                    operation = value.get("operation")
                    if operation not in operations:
                        raise ValueError("operation is invalid")
                    row_effect_id = record.effect_id
                    if record.effect_id in broken_effects:
                        raise ValueError("effect history is broken")
                    previous = records.get(record.effect_id)
                    if previous is None:
                        if operation != "begin_intent" or record.state != "intent":
                            raise ValueError("effect history is out of order")
                        if record.attempt != 1:
                            raise ValueError("effect history has an invalid attempt")
                    else:
                        if not _same_identity(previous, record):
                            raise ValueError(
                                "effect history changes immutable identity"
                            )
                        if not _valid_transition(previous, record, operation):
                            raise ValueError("effect history is out of order")
                    prior_id = idempotency.get(record.idempotency_key)
                    if prior_id is not None and prior_id != record.effect_id:
                        raise ValueError("effect history reuses an idempotency key")
                except Exception as exc:
                    integrity = integrity or f"row_{type(exc).__name__}"
                    if row_effect_id is not None:
                        broken_effects.add(row_effect_id)
                    continue
                parsed.append(record)
                records[record.effect_id] = record
                idempotency[record.idempotency_key] = record.effect_id
    except Exception as exc:
        integrity = integrity or f"read_{type(exc).__name__}"
    if integrity is not None:
        # All consumers share this helper.  Do not let one consumer project
        # the rows that another consumer has already rejected as untrusted.
        return (), integrity
    return tuple(parsed), None


@dataclass(frozen=True)
class EffectReceipt:
    """A host-observed delivery receipt.

    ``EffectReceipt`` is intentionally strict: an unverified effect uses the
    null evidence mapping emitted by :class:`EffectRecord`, never an instance
    of this class with fabricated fields.
    """

    kind: str = EFFECT_KIND
    receipt_id: str = ""
    event_id: str = ""
    observed_at: datetime | None = None
    content_sha256: str | None = None
    content_length: int | None = None
    epoch_id: str = ""

    schema_version: ClassVar[str] = EFFECT_RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        if self.kind != EFFECT_KIND:
            raise ValueError("effect receipt kind must be delivery_receipt")
        _nonempty_text(self.receipt_id, "receipt_id")
        _nonempty_text(self.event_id, "event_id")
        _aware_datetime(self.observed_at, "observed_at")
        _content_hash(self.content_sha256)
        _positive_int(self.content_length, "content_length")
        _nonempty_text(self.epoch_id, "epoch_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "receipt_id": self.receipt_id,
            "event_id": self.event_id,
            "observed_at": isoformat(self.observed_at),
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "epoch_id": self.epoch_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EffectReceipt:
        if set(value) != _RECEIPT_FIELDS:
            raise StateError("effect receipt has unsupported fields")
        if value.get("schema_version") != EFFECT_RECEIPT_SCHEMA:
            raise StateError("effect receipt has an unsupported schema")
        try:
            return cls(
                kind=value["kind"],
                receipt_id=value["receipt_id"],
                event_id=value["event_id"],
                observed_at=_parse_time(value["observed_at"], "observed_at"),
                content_sha256=value["content_sha256"],
                content_length=value["content_length"],
                epoch_id=value["epoch_id"],
            )
        except (KeyError, TypeError, ValueError, StateError) as exc:
            raise StateError("effect receipt is invalid") from exc


@dataclass(frozen=True)
class EffectIntent:
    """The immutable effect identity recorded before adapter work."""

    effect_id: str
    kind: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    created_at: datetime
    expires_at: datetime
    content_sha256: str
    content_length: int
    attempt: int = 1

    schema_version: ClassVar[str] = EFFECT_SCHEMA

    def __post_init__(self) -> None:
        _validate_identity(
            effect_id=self.effect_id,
            kind=self.kind,
            source_event_id=self.source_event_id,
            idempotency_key=self.idempotency_key,
            epoch_id=self.epoch_id,
            created_at=self.created_at,
            expires_at=self.expires_at,
            content_sha256=self.content_sha256,
            content_length=self.content_length,
            attempt=self.attempt,
        )

    @property
    def state(self) -> str:
        return "intent"

    @property
    def receipt(self) -> None:
        return None

    @property
    def verified(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "effect_id": self.effect_id,
            "kind": self.kind,
            "source_event_id": self.source_event_id,
            "idempotency_key": self.idempotency_key,
            "epoch_id": self.epoch_id,
            "attempt": self.attempt,
            "state": self.state,
            "created_at": isoformat(self.created_at),
            "expires_at": isoformat(self.expires_at),
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "observed_at": None,
            "receipt": _empty_receipt(),
            "reason": None,
            "retryable": None,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EffectIntent:
        try:
            if value.get("schema_version") != EFFECT_SCHEMA:
                raise StateError("effect intent has an unsupported schema")
            if value.get("state") != "intent":
                raise StateError("effect intent must be in intent state")
            return cls(
                effect_id=value["effect_id"],
                kind=value["kind"],
                source_event_id=value["source_event_id"],
                idempotency_key=value["idempotency_key"],
                epoch_id=value["epoch_id"],
                created_at=_parse_time(value["created_at"], "created_at"),
                expires_at=_parse_time(value["expires_at"], "expires_at"),
                content_sha256=value["content_sha256"],
                content_length=value["content_length"],
                attempt=value["attempt"],
            )
        except (KeyError, TypeError, ValueError, StateError) as exc:
            if isinstance(exc, StateError):
                raise
            raise StateError("effect intent is invalid") from exc


@dataclass(frozen=True)
class EffectRecord:
    """Current replayed view of an effect intent and its evidence."""

    effect_id: str
    kind: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    created_at: datetime
    expires_at: datetime
    content_sha256: str
    content_length: int
    state: str
    attempt: int = 1
    receipt: EffectReceipt | None = None
    reason: str | None = None
    retryable: bool | None = None
    observed_at: datetime | None = None

    schema_version: ClassVar[str] = EFFECT_SCHEMA

    def __post_init__(self) -> None:
        _validate_identity(
            effect_id=self.effect_id,
            kind=self.kind,
            source_event_id=self.source_event_id,
            idempotency_key=self.idempotency_key,
            epoch_id=self.epoch_id,
            created_at=self.created_at,
            expires_at=self.expires_at,
            content_sha256=self.content_sha256,
            content_length=self.content_length,
            attempt=self.attempt,
        )
        if self.state not in EFFECT_STATES:
            raise ValueError(f"unknown effect state: {self.state}")
        if self.receipt is not None and not isinstance(self.receipt, EffectReceipt):
            raise TypeError("effect receipt must be an EffectReceipt or null")
        if self.state == "verified" and self.receipt is None:
            raise ValueError("verified effect requires a delivery receipt")
        if self.state != "verified" and self.receipt is not None:
            raise ValueError("unverified effect cannot carry a delivery receipt")
        if self.state == "failed":
            _nonempty_text(self.reason, "failure reason", max_bytes=_REASON_MAX_BYTES)
            if type(self.retryable) is not bool:
                raise ValueError("failed effect retryable must be a boolean")
        elif self.state == "expired":
            if self.reason != "expired" or self.retryable is not True:
                raise ValueError("expired effect must be retryable with reason expired")
        elif self.reason is not None or self.retryable is not None:
            raise ValueError(
                "reason and retryable only apply to failed or expired effects"
            )
        expected_observed = None if self.receipt is None else self.receipt.observed_at
        if self.observed_at is not None:
            _aware_datetime(self.observed_at, "observed_at")
        if self.observed_at is None and expected_observed is not None:
            object.__setattr__(self, "observed_at", expected_observed)
        elif self.observed_at != expected_observed:
            raise ValueError("observed_at must match the delivery receipt")

    @property
    def verified(self) -> bool:
        return self.state == "verified" and self.receipt is not None

    @property
    def queue_accepted(self) -> bool:
        return self.state in {"executed_unverified", "verified"}

    @property
    def receipt_id(self) -> str | None:
        return None if self.receipt is None else self.receipt.receipt_id

    @property
    def event_id(self) -> str | None:
        return None if self.receipt is None else self.receipt.event_id

    @property
    def evidence(self) -> dict[str, Any]:
        if self.receipt is None:
            return _empty_receipt()
        return self.receipt.to_dict()

    def to_intent(self) -> EffectIntent:
        return EffectIntent(
            effect_id=self.effect_id,
            kind=self.kind,
            source_event_id=self.source_event_id,
            idempotency_key=self.idempotency_key,
            epoch_id=self.epoch_id,
            created_at=self.created_at,
            expires_at=self.expires_at,
            content_sha256=self.content_sha256,
            content_length=self.content_length,
            attempt=self.attempt,
        )

    @property
    def intent(self) -> EffectIntent:
        return self.to_intent()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "effect_id": self.effect_id,
            "kind": self.kind,
            "source_event_id": self.source_event_id,
            "idempotency_key": self.idempotency_key,
            "epoch_id": self.epoch_id,
            "attempt": self.attempt,
            "state": self.state,
            "created_at": isoformat(self.created_at),
            "expires_at": isoformat(self.expires_at),
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "observed_at": (
                None if self.observed_at is None else isoformat(self.observed_at)
            ),
            "receipt": self.evidence,
            "reason": self.reason,
            "retryable": self.retryable,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EffectRecord:
        # ``to_dict`` is a record snapshot and intentionally omits the
        # append-only operation marker.  Replay rows include that marker.
        snapshot = dict(value)
        snapshot.setdefault("operation", "snapshot")
        return _record_from_row(snapshot, row_number=None)


def _validate_identity(
    *,
    effect_id: Any,
    kind: Any,
    source_event_id: Any,
    idempotency_key: Any,
    epoch_id: Any,
    created_at: Any,
    expires_at: Any,
    content_sha256: Any,
    content_length: Any,
    attempt: Any,
) -> None:
    _nonempty_text(effect_id, "effect_id")
    _nonempty_text(kind, "effect kind")
    _nonempty_text(source_event_id, "source_event_id")
    _nonempty_text(idempotency_key, "idempotency_key")
    _nonempty_text(epoch_id, "epoch_id")
    created_value = _aware_datetime(created_at, "created_at")
    expires_value = _aware_datetime(expires_at, "expires_at")
    if expires_value <= created_value:
        raise ValueError("expires_at must be later than created_at")
    _content_hash(content_sha256)
    _positive_int(content_length, "content_length")
    _positive_int(attempt, "attempt")


def _record_from_row(row: Mapping[str, Any], *, row_number: int | None) -> EffectRecord:
    prefix = "effect row" if row_number is None else f"effects.jsonl row {row_number}"
    try:
        if set(row) != _ROW_FIELDS:
            raise ValueError("unsupported fields")
        if row["schema_version"] != EFFECT_SCHEMA:
            raise ValueError("unsupported schema")
        if type(row["operation"]) is not str:
            raise ValueError("operation is invalid")
        state = row["state"]
        if state not in EFFECT_STATES:
            raise ValueError("state is invalid")
        receipt_value = row["receipt"]
        if not isinstance(receipt_value, Mapping):
            raise TypeError("receipt evidence must be an object")
        if set(receipt_value) != _RECEIPT_FIELDS:
            raise ValueError("receipt evidence has unsupported fields")
        if (
            receipt_value.get("schema_version") != EFFECT_RECEIPT_SCHEMA
            or receipt_value.get("kind") != EFFECT_KIND
        ):
            raise ValueError("receipt evidence schema is invalid")
        receipt_values = [
            receipt_value.get("receipt_id"),
            receipt_value.get("event_id"),
            receipt_value.get("observed_at"),
            receipt_value.get("content_sha256"),
            receipt_value.get("content_length"),
            receipt_value.get("epoch_id"),
        ]
        if all(value is None for value in receipt_values):
            receipt = None
        elif any(value is None for value in receipt_values):
            raise ValueError("receipt evidence must be fully null or fully populated")
        else:
            receipt = EffectReceipt.from_dict(receipt_value)
        observed_at_value = row["observed_at"]
        observed_at = (
            None
            if observed_at_value is None
            else _parse_time(observed_at_value, "observed_at")
        )
        record = EffectRecord(
            effect_id=row["effect_id"],
            kind=row["kind"],
            source_event_id=row["source_event_id"],
            idempotency_key=row["idempotency_key"],
            epoch_id=row["epoch_id"],
            created_at=_parse_time(row["created_at"], "created_at"),
            expires_at=_parse_time(row["expires_at"], "expires_at"),
            content_sha256=row["content_sha256"],
            content_length=row["content_length"],
            state=state,
            attempt=row["attempt"],
            receipt=receipt,
            reason=row["reason"],
            retryable=row["retryable"],
            observed_at=observed_at,
        )
        if record.state == "verified" and receipt is None:
            raise ValueError("verified row has no receipt")
        if record.state != "verified" and receipt is not None:
            raise ValueError("unverified row has a receipt")
        if receipt is not None and not _receipt_matches_record(record, receipt):
            raise ValueError("delivery receipt does not match effect intent")
        return record
    except (KeyError, TypeError, ValueError, StateError) as exc:
        raise StateError(f"{prefix} is invalid") from exc


def _observer_inputs(*, target_date: date, now: datetime) -> None:
    if type(target_date) is not date:
        raise TypeError("target_date must be a date")
    _aware_datetime(now, "now")


def _observer_reason_code(reason: str | None) -> str:
    """Map private failure text to a bounded, non-content reason code."""

    if not isinstance(reason, str) or not reason:
        return "unspecified"
    prefix = reason.split(":", 1)[0].strip().lower()
    known = {
        "adapter_error",
        "adapter_malformed_return",
        "adapter_malformed_result",
        "adapter_rejected",
        "adapter_unavailable",
        "effect_pending_error",
        "effect_queue_accept_error",
        "evidence_invalid",
        "missing_receipt",
        "provider_error",
        "receipt_mismatch",
    }
    return prefix if prefix in known else "failure"


def _observer_refs(record: EffectRecord) -> tuple[str, ...]:
    refs = [
        f"effect:{record.effect_id}",
        f"kind:{record.kind}",
        f"source:{record.source_event_id}",
        f"idempotency:{record.idempotency_key}",
        f"sha256:{record.content_sha256}",
    ]
    if record.receipt is not None:
        refs.extend(
            (
                f"receipt:{record.receipt.receipt_id}",
                f"receipt_event:{record.receipt.event_id}",
            )
        )
    return tuple(refs)


def _observer_fact(
    record: EffectRecord,
    *,
    target_date: date,
    now: datetime,
    history: tuple[EffectRecord, ...],
) -> ObservationFact:
    state = record.state
    event_time = record.created_at
    if state == "verified" and record.receipt is not None:
        event_time = record.receipt.observed_at
    elif state == "expired":
        event_time = record.expires_at
    elif (
        state in {"intent", "pending", "executed_unverified"}
        and record.expires_at < record.created_at
    ):
        # Invalid expiry cannot pass EffectRecord validation, but keeping this
        # branch defensive makes hostile record-like test doubles fail closed.
        state = "expired"
        event_time = record.expires_at
    elif state in {"pending", "executed_unverified"} and record.expires_at < now:
        # Projection only: never call expire(), append, or rewrite the ledger.
        state = "expired"
        event_time = record.expires_at

    counts: dict[str, int] = {
        "effects": 1,
        "attempt": record.attempt,
        "content_length": record.content_length,
        f"state_{state}": 1,
    }
    reason_code: str | None = None
    if record.state == "failed":
        reason_code = _observer_reason_code(record.reason)
        counts["reason_code_present"] = 1

    code_by_state = {
        "intent": "effect_intent",
        "pending": "effect_pending",
        "executed_unverified": "effect_executed_unverified",
        "expired": "effect_expired",
        "failed": "effect_failed",
        "requeued": "effect_requeued",
        "verified": "effect_verified",
    }
    code = code_by_state.get(state, "effect_integrity_error")
    if reason_code is not None:
        code = f"{code}:{reason_code}"

    recovery: RecoveryEvidence | None = None
    fact_state = "neutral" if state == "verified" else "current"
    if state == "verified":
        prior_states = {
            previous.state
            for previous in history[:-1]
            if previous.effect_id == record.effect_id
            and previous.kind == record.kind
            and previous.idempotency_key == record.idempotency_key
        }
        if prior_states & {
            "pending",
            "executed_unverified",
            "expired",
            "failed",
            "requeued",
        }:
            receipt = record.receipt
            if receipt is not None:
                recovery = RecoveryEvidence(
                    ref=receipt.receipt_id,
                    code="effect_verified",
                    recovered_at=receipt.observed_at,
                )
                fact_state = "recovered_history"

    return ObservationFact(
        key=f"effect:{record.kind}:{record.effect_id}",
        code=code,
        state=fact_state,
        target_date=target_date,
        event_time=event_time,
        refs=_observer_refs(record),
        counts=counts,
        recovery=recovery,
    )


def _observer_integrity_fact(
    *, target_date: date, source: str, code: str
) -> ObservationFact:
    return ObservationFact(
        key=f"effect:integrity:{source}",
        code=f"effect_integrity_error:{code}",
        state="current",
        target_date=target_date,
        refs=(source,),
        counts={"integrity_errors": 1},
    )


def _same_identity(left: EffectRecord, right: EffectRecord) -> bool:
    return (
        left.effect_id == right.effect_id
        and left.kind == right.kind
        and left.source_event_id == right.source_event_id
        and left.idempotency_key == right.idempotency_key
        and left.epoch_id == right.epoch_id
        and left.created_at == right.created_at
        and left.content_sha256 == right.content_sha256
        and left.content_length == right.content_length
    )


def _same_begin_identity(
    record: EffectRecord,
    *,
    kind: str,
    source_event_id: str,
    epoch_id: str,
    content_sha256: str,
    content_length: int,
) -> bool:
    return (
        record.kind == kind
        and record.source_event_id == source_event_id
        and record.epoch_id == epoch_id
        and record.content_sha256 == content_sha256
        and record.content_length == content_length
    )


def _receipt_matches_record(
    record: EffectRecord,
    receipt: EffectReceipt,
) -> bool:
    return (
        receipt.event_id == record.source_event_id
        and receipt.content_sha256 == record.content_sha256
        and receipt.content_length == record.content_length
        and receipt.epoch_id == record.epoch_id
    )


def _transition_error(effect_id: str, state: str, operation: str) -> ValueError:
    return ValueError(f"effect {effect_id} cannot {operation} from {state}")


class EffectLedger:
    """Concurrent-safe append-only effect state ledger."""

    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "effects.jsonl")
        self.mutation_lock = self.root / "effects.mutation.lock"
        self.mutation_lock_path = self.mutation_lock
        self.clock = clock

    def _append(self, operation: str, record: EffectRecord) -> None:
        row = record.to_dict()
        row["operation"] = operation
        # JsonlLedger errors intentionally propagate.  The candidate record is
        # returned by callers only after this append succeeds.
        self.ledger.append(row)

    def _replay(self) -> dict[str, EffectRecord]:
        records: dict[str, EffectRecord] = {}
        idempotency: dict[str, str] = {}
        for index, row in enumerate(self.ledger.rows(), start=1):
            record = _record_from_row(row, row_number=index)
            operation = row["operation"]
            previous = records.get(record.effect_id)
            if previous is None:
                if operation != "begin_intent" or record.state != "intent":
                    raise StateError(f"effects.jsonl row {index} is out of order")
                if record.attempt != 1:
                    raise StateError(
                        f"effects.jsonl row {index} has an invalid initial attempt"
                    )
            else:
                if not _same_identity(previous, record):
                    raise StateError(
                        f"effects.jsonl row {index} changes immutable effect identity"
                    )
                if not _valid_transition(previous, record, operation):
                    raise StateError(f"effects.jsonl row {index} is out of order")
            prior_id = idempotency.get(record.idempotency_key)
            if prior_id is not None and prior_id != record.effect_id:
                raise StateError(f"effects.jsonl row {index} reuses an idempotency key")
            idempotency[record.idempotency_key] = record.effect_id
            records[record.effect_id] = record
        return records

    def _snapshot(self) -> dict[str, EffectRecord]:
        if not self.ledger.path.exists():
            return {}
        with file_lock(self.mutation_lock):
            if not self.ledger.path.exists():
                return {}
            return self._replay()

    def begin_intent(
        self,
        effect_id: str | None = None,
        *,
        kind: str,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content_sha256: str,
        content_length: int,
        expires_at: datetime,
        created_at: datetime | None = None,
        attempt: int = 1,
    ) -> EffectRecord:
        if type(attempt) is not int or attempt != 1:
            raise ValueError("initial effect attempt must be 1")
        actual_effect_id = new_id("effect") if effect_id is None else effect_id
        actual_created_at = self.clock() if created_at is None else created_at
        candidate = EffectRecord(
            effect_id=actual_effect_id,
            kind=kind,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            created_at=actual_created_at,
            expires_at=expires_at,
            content_sha256=content_sha256,
            content_length=content_length,
            state="intent",
            attempt=attempt,
        )
        with file_lock(self.mutation_lock):
            records = self._replay()
            existing_by_key = next(
                (
                    record
                    for record in records.values()
                    if record.idempotency_key == idempotency_key
                ),
                None,
            )
            if existing_by_key is not None:
                if _same_begin_identity(
                    existing_by_key,
                    kind=kind,
                    source_event_id=source_event_id,
                    epoch_id=epoch_id,
                    content_sha256=content_sha256,
                    content_length=content_length,
                ):
                    return existing_by_key
                raise ValueError("idempotency key conflicts with existing effect")
            if actual_effect_id in records:
                raise ValueError("effect_id already exists")
            self._append("begin_intent", candidate)
            return candidate

    def mark_pending(self, effect_id: str) -> EffectRecord:
        with file_lock(self.mutation_lock):
            record = self._require(self._replay(), effect_id)
            if record.state == "pending":
                return record
            if record.state not in {"intent", "requeued"}:
                raise _transition_error(effect_id, record.state, "mark pending")
            candidate = replace(record, state="pending")
            self._append("mark_pending", candidate)
            return candidate

    def mark_queue_accepted(self, effect_id: str) -> EffectRecord:
        with file_lock(self.mutation_lock):
            record = self._require(self._replay(), effect_id)
            if record.state == "executed_unverified":
                return record
            if record.state != "pending":
                raise _transition_error(effect_id, record.state, "mark queue accepted")
            candidate = replace(record, state="executed_unverified")
            self._append("mark_queue_accepted", candidate)
            return candidate

    def verify(self, effect_id: str, receipt: EffectReceipt) -> EffectRecord:
        if not isinstance(receipt, EffectReceipt):
            raise TypeError("receipt must be an EffectReceipt")
        with file_lock(self.mutation_lock):
            record = self._require(self._replay(), effect_id)
            if record.state == "verified":
                if record.receipt == receipt:
                    return record
                raise ValueError("conflicting or stale delivery receipt")
            if record.state not in {"pending", "executed_unverified"}:
                raise _transition_error(effect_id, record.state, "verify")
            if (
                receipt.event_id != record.source_event_id
                or receipt.content_sha256 != record.content_sha256
                or receipt.content_length != record.content_length
                or receipt.epoch_id != record.epoch_id
            ):
                raise ValueError("delivery receipt does not match effect intent")
            candidate = replace(
                record,
                state="verified",
                receipt=receipt,
                observed_at=receipt.observed_at,
            )
            self._append("verify", candidate)
            return candidate

    def fail(
        self,
        effect_id: str,
        reason: str,
        retryable: bool,
    ) -> EffectRecord:
        _nonempty_text(reason, "failure reason", max_bytes=_REASON_MAX_BYTES)
        if type(retryable) is not bool:
            raise ValueError("retryable must be a boolean")
        with file_lock(self.mutation_lock):
            record = self._require(self._replay(), effect_id)
            if record.state == "failed":
                if record.reason == reason and record.retryable == retryable:
                    return record
                raise ValueError("conflicting failure replay")
            if record.state not in {
                "intent",
                "pending",
                "executed_unverified",
                "requeued",
            }:
                raise _transition_error(effect_id, record.state, "fail")
            candidate = replace(
                record,
                state="failed",
                reason=reason,
                retryable=retryable,
            )
            self._append("fail", candidate)
            return candidate

    def expire(
        self,
        effect_id: str,
        now: datetime | None = None,
    ) -> EffectRecord:
        effective_now = self.clock() if now is None else now
        _aware_datetime(effective_now, "now")
        with file_lock(self.mutation_lock):
            record = self._require(self._replay(), effect_id)
            if record.state not in {"pending", "executed_unverified"}:
                raise _transition_error(effect_id, record.state, "expire")
            if not record.expires_at < effective_now:
                raise ValueError("effect has not expired")
            candidate = replace(
                record,
                state="expired",
                reason="expired",
                retryable=True,
            )
            self._append("expire", candidate)
            return candidate

    def requeue(
        self,
        effect_id: str,
        *,
        expires_at: datetime,
        idempotency_key: str | None = None,
        source_event_id: str | None = None,
        epoch_id: str | None = None,
    ) -> EffectRecord:
        with file_lock(self.mutation_lock):
            effective_now = self.clock()
            _aware_datetime(effective_now, "now")
            new_expires_at = _aware_datetime(expires_at, "expires_at")
            if new_expires_at <= effective_now:
                raise ValueError("requeue expires_at must be later than current time")
            record = self._require(self._replay(), effect_id)
            if record.state != "expired":
                raise _transition_error(effect_id, record.state, "requeue")
            if (
                idempotency_key is not None
                and idempotency_key != record.idempotency_key
            ):
                raise ValueError("requeue idempotency key does not match effect")
            if (
                source_event_id is not None
                and source_event_id != record.source_event_id
            ):
                raise ValueError("requeue source event does not match effect")
            if epoch_id is not None and epoch_id != record.epoch_id:
                raise ValueError("requeue epoch does not match effect")
            candidate = replace(
                record,
                state="requeued",
                attempt=record.attempt + 1,
                expires_at=new_expires_at,
                reason=None,
                retryable=None,
                receipt=None,
                observed_at=None,
            )
            self._append("requeue", candidate)
            return candidate

    def get(self, effect_id: str) -> EffectRecord | None:
        _nonempty_text(effect_id, "effect_id")
        return self._snapshot().get(effect_id)

    def find_by_idempotency(self, key: str) -> EffectRecord | None:
        """Return the current effect record for an exact idempotency key.

        This is deliberately a read-only public port.  Replay happens while
        holding the same mutation lock used by all state transitions, so a
        caller cannot observe a partially appended effect history.
        """

        _nonempty_text(key, "idempotency_key")
        records = self._snapshot()
        return next(
            (record for record in records.values() if record.idempotency_key == key),
            None,
        )

    def pending_for_reconciliation(
        self,
        now: datetime | None = None,
    ) -> list[EffectRecord]:
        """Return all pending evidence without expiring or mutating it.

        ``now`` is validated as part of the reconciliation contract, while
        deciding whether an effect is expired remains the caller's job.
        """

        effective_now = self.clock() if now is None else now
        _aware_datetime(effective_now, "now")
        del effective_now  # The timestamp is validated for a stable API contract.
        records = self._snapshot()
        pending = [
            record
            for record in records.values()
            if record.state in {"pending", "executed_unverified"}
        ]
        return sorted(pending, key=lambda record: (record.created_at, record.effect_id))

    def records(self) -> tuple[EffectRecord, ...]:
        """Return the latest replayed record for every effect.

        An absent ledger is an empty read-only state.  Once the ledger exists,
        replay occurs under the mutation lock so corrupt or out-of-order state
        fails closed and readers cannot observe a partial transition.
        """

        records = self._snapshot()
        return tuple(
            sorted(
                records.values(),
                key=lambda record: (record.created_at, record.effect_id),
            )
        )

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return a content-free, read-only projection of effect history.

        This port intentionally does not use :meth:`records`, which acquires
        the mutation lock through ``JsonlLedger.rows``.  A status request may
        inspect an existing ledger, but it must not create a state directory
        or lock when the owner is pristine (or when an old ledger has no
        adjacent lock yet).
        """

        _observer_inputs(target_date=target_date, now=now)
        rows, integrity = _read_effect_history_lock_free(self.ledger.path)
        if not rows and integrity is None:
            return ()
        if integrity is not None:
            # A single replay invariant violation makes the whole ledger
            # untrusted.  Keep the valid rows in the reader's result for
            # diagnostics/recovery on a later clean read, but do not mix any
            # state projection with a current integrity fact.
            return (
                _observer_integrity_fact(
                    target_date=target_date,
                    source=self.ledger.path.name,
                    code=integrity,
                ),
            )

        by_effect: dict[str, list[EffectRecord]] = {}
        for record in rows:
            by_effect.setdefault(record.effect_id, []).append(record)
        facts: list[ObservationFact] = []
        for history in by_effect.values():
            current = history[-1]
            same_key_history = [
                record
                for record in rows
                if record.kind == current.kind
                and record.idempotency_key == current.idempotency_key
            ]
            facts.append(
                _observer_fact(
                    current,
                    target_date=target_date,
                    now=now,
                    history=same_key_history,
                )
            )
        return tuple(sorted(facts, key=lambda fact: fact.key))

    @staticmethod
    def _require(records: Mapping[str, EffectRecord], effect_id: str) -> EffectRecord:
        _nonempty_text(effect_id, "effect_id")
        record = records.get(effect_id)
        if record is None:
            raise ValueError(f"effect {effect_id} does not exist")
        return record


def _valid_transition(
    previous: EffectRecord,
    current: EffectRecord,
    operation: Any,
) -> bool:
    if not isinstance(operation, str):
        return False
    if previous.state == "expired":
        return (
            operation == "requeue"
            and current.state == "requeued"
            and current.attempt == previous.attempt + 1
            and current.expires_at > previous.expires_at
        )
    if current.expires_at != previous.expires_at:
        return False
    if current.attempt != previous.attempt:
        return False
    if previous.state == "intent":
        return (operation == "mark_pending" and current.state == "pending") or (
            operation == "fail" and current.state == "failed"
        )
    if previous.state == "requeued":
        return (operation == "mark_pending" and current.state == "pending") or (
            operation == "fail" and current.state == "failed"
        )
    if previous.state == "pending":
        if operation == "mark_queue_accepted":
            return current.state == "executed_unverified"
        if operation == "verify":
            return current.state == "verified"
        if operation == "fail":
            return current.state == "failed"
        if operation == "expire":
            return current.state == "expired"
        return False
    if previous.state == "executed_unverified":
        if operation == "verify":
            return current.state == "verified"
        if operation == "fail":
            return current.state == "failed"
        if operation == "expire":
            return current.state == "expired"
        return False
    return False


__all__ = [
    "EFFECT_RECEIPT_SCHEMA",
    "EFFECT_SCHEMA",
    "EFFECT_STATES",
    "RECEIPT_SCHEMA",
    "EffectIntent",
    "EffectLedger",
    "EffectReceipt",
    "EffectRecord",
]
