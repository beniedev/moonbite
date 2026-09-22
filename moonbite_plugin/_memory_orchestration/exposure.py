"""Durable exposure ledger, read-only projection, and admission policy."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .contracts import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureRecord,
    ORCHESTRATION_SCHEMA,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    _coerce_reply,
    _hash,
    _length,
    _MAX_CLASS_BYTES,
    _optional_time,
    _reply_tuple,
    _text,
    _time,
)
from .observation import (
    observer_integrity_fact as _observer_integrity_fact,
    observer_jsonl_rows as _observer_jsonl_rows,
    observer_merge_facts as _observer_merge_facts,
    observer_refs as _observer_refs,
    observer_validate_context as _observer_validate_context,
)
from ..observer import ObservationFact, RecoveryEvidence
from ..runtime_core import (
    JsonlLedger,
    StateError,
    file_lock,
    isoformat,
    new_id,
    utc_now,
)

EXPOSURE_EVENTS = ("selected", "opened", "exposed", "used", "consumed")
EXPOSURE_STATES = frozenset((*EXPOSURE_EVENTS, "failed_to_open"))
_EXPOSURE_TRANSITIONS = {
    "selected": frozenset({"opened", "failed_to_open"}),
    "opened": frozenset({"exposed"}),
    "exposed": frozenset({"used"}),
    "used": frozenset({"consumed"}),
    "consumed": frozenset(),
    "failed_to_open": frozenset(),
}
_MAX_EVENT_ID_BYTES = 512
_MAX_FAILURE_BYTES = 256
_PRIVATE_SOURCE_CLASSES = frozenset(
    {
        "private",
        "private_continuity",
        "self_brief",
        "identity",
        "relationship",
        "personal",
        "continuity",
    }
)
_SELF_BRIEF_CLASSES = frozenset(
    {"self_brief", "identity", "relationship", "private_continuity"}
)
_LEDGER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "event",
        "state",
        "exposure_id",
        "session_id",
        "lifecycle_id",
        "turn_id",
        "context_source_kind",
        "source_ref",
        "source_class",
        "source_event_time",
        "source_created_at",
        "source_expires_at",
        "content_sha256",
        "content_length",
        "created_at",
        "reply_use_id",
        "reply_content_sha256",
        "reply_content_length",
        "failure_code",
    }
)


def _parse_ledger_row(value: Mapping[str, Any], row_number: int) -> dict[str, Any]:
    if set(value) != _LEDGER_FIELDS:
        raise StateError(
            f"memory orchestration row {row_number} has unsupported fields"
        )
    try:
        if (
            value["schema_version"] != ORCHESTRATION_SCHEMA
            or value["kind"] != "exposure_event"
        ):
            raise ValueError("unsupported orchestration schema")
        event = value["event"]
        state = value["state"]
        if event not in EXPOSURE_STATES or state != event:
            raise ValueError("invalid exposure event/state")
        event_id = _text(
            value["event_id"], "exposure event_id", limit=_MAX_EVENT_ID_BYTES
        )
        exposure_id = _text(value["exposure_id"], "exposure_id")
        session_id = _text(value["session_id"], "session_id")
        lifecycle_id = _text(value["lifecycle_id"], "lifecycle_id")
        turn_id = _text(value["turn_id"], "turn_id")
        context_source_kind = _text(
            value["context_source_kind"], "context source_kind", limit=_MAX_CLASS_BYTES
        )
        source_ref = _text(value["source_ref"], "source_ref")
        source_class = _text(
            value["source_class"], "source_class", limit=_MAX_CLASS_BYTES
        )
        source_event_time = _time(value["source_event_time"], "source_event_time")
        source_created_at = _time(value["source_created_at"], "source created_at")
        source_expires_at = _optional_time(
            value["source_expires_at"], "source expires_at"
        )
        if source_expires_at is not None and source_expires_at <= source_created_at:
            raise ValueError("source expires_at is not later than source created_at")
        content_values = (value["content_sha256"], value["content_length"])
        if all(item is None for item in content_values):
            content_sha256 = content_length = None
        elif any(item is None for item in content_values):
            raise ValueError(
                "source content hash and length must be all null or populated"
            )
        else:
            content_sha256 = _hash(value["content_sha256"])
            content_length = _length(value["content_length"])
        created_at = _time(value["created_at"], "created_at")
        reply_values = (
            value["reply_use_id"],
            value["reply_content_sha256"],
            value["reply_content_length"],
        )
        if all(item is None for item in reply_values):
            reply = None
        elif any(item is None for item in reply_values):
            raise ValueError("reply evidence must be all null or all populated")
        else:
            reply = ReplyUseEvidence(*reply_values)
        failure_code = value["failure_code"]
        if failure_code is not None:
            failure_code = _text(failure_code, "failure_code", limit=_MAX_FAILURE_BYTES)
        if event == "selected" and failure_code is not None:
            raise ValueError("selected event cannot contain failure_code")
        if event == "failed_to_open" and failure_code is None:
            raise ValueError("failed_to_open event requires failure_code")
        if event != "failed_to_open" and failure_code is not None:
            raise ValueError("only failed_to_open event may contain failure_code")
        if event in {"opened", "exposed", "used", "consumed"}:
            if content_sha256 is None or content_length is None:
                raise ValueError(
                    "opened/exposed events require source content descriptor"
                )
        if (
            event in {"selected", "opened", "exposed", "failed_to_open"}
            and reply is not None
        ):
            raise ValueError(f"{event} event cannot contain reply-use evidence")
        if event == "consumed" and reply is None:
            raise ValueError("consumed event requires reply-use evidence")
        return {
            "event": event,
            "event_id": event_id,
            "exposure_id": exposure_id,
            "session_id": session_id,
            "lifecycle_id": lifecycle_id,
            "turn_id": turn_id,
            "context_source_kind": context_source_kind,
            "source_ref": source_ref,
            "source_class": source_class,
            "source_event_time": source_event_time,
            "source_created_at": source_created_at,
            "source_expires_at": source_expires_at,
            "content_sha256": content_sha256,
            "content_length": content_length,
            "created_at": created_at,
            "reply": reply,
            "failure_code": failure_code,
        }
    except (KeyError, TypeError, ValueError, StateError) as exc:
        raise StateError(f"memory orchestration row {row_number} is invalid") from exc


class ExposureLedger:
    """Concurrent-safe append-only exposed/selected/used/consumed ledger."""

    def __init__(self, root: Path, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.root = Path(root)
        self.ledger = JsonlLedger(self.root / "memory_orchestration.jsonl")
        self.mutation_lock = self.root / "memory_orchestration.mutation.lock"
        self.mutation_lock_path = self.mutation_lock
        self.clock = clock

    @staticmethod
    def _identity(state: ExposureRecord | Mapping[str, Any]) -> tuple[Any, ...]:
        if isinstance(state, ExposureRecord):
            return (
                state.exposure_id,
                state.session_id,
                state.lifecycle_id,
                state.turn_id,
                state.context_source_kind,
                state.source_ref,
                state.source_class,
                state.source_event_time,
                state.source_created_at,
                state.source_expires_at,
            )
        return (
            state["exposure_id"],
            state["session_id"],
            state["lifecycle_id"],
            state["turn_id"],
            state["context_source_kind"],
            state["source_ref"],
            state["source_class"],
            state["source_event_time"],
            state["source_created_at"],
            state["source_expires_at"],
        )

    @staticmethod
    def _record(
        parsed: Mapping[str, Any], *, events: tuple[str, ...], last_event_id: str
    ) -> ExposureRecord:
        reply = parsed["reply"]
        reply_id, reply_hash, reply_length = _reply_tuple(reply)
        return ExposureRecord(
            exposure_id=parsed["exposure_id"],
            state=parsed["event"],
            session_id=parsed["session_id"],
            lifecycle_id=parsed["lifecycle_id"],
            turn_id=parsed["turn_id"],
            context_source_kind=parsed["context_source_kind"],
            source_ref=parsed["source_ref"],
            source_class=parsed["source_class"],
            source_event_time=parsed["source_event_time"],
            source_created_at=parsed["source_created_at"],
            source_expires_at=parsed["source_expires_at"],
            content_sha256=parsed["content_sha256"],
            content_length=parsed["content_length"],
            created_at=parsed["created_at"],
            events=events,
            last_event_id=last_event_id,
            reply_use_id=reply_id,
            reply_content_sha256=reply_hash,
            reply_content_length=reply_length,
            failure_code=parsed.get("failure_code"),
        )

    def _replay_unlocked(
        self,
    ) -> tuple[dict[str, ExposureRecord], dict[str, Mapping[str, Any]]]:
        states: dict[str, ExposureRecord] = {}
        event_rows: dict[str, Mapping[str, Any]] = {}
        for row_number, row in enumerate(self.ledger.rows(), start=1):
            parsed = _parse_ledger_row(row, row_number)
            event_id = parsed["event_id"]
            if event_id in event_rows:
                raise StateError(f"duplicate memory orchestration event_id: {event_id}")
            event_rows[event_id] = dict(row)
            exposure_id = parsed["exposure_id"]
            previous = states.get(exposure_id)
            if previous is None:
                if parsed["event"] != "selected":
                    raise StateError(
                        f"memory orchestration row {row_number} starts after selected"
                    )
                current = self._record(
                    parsed, events=("selected",), last_event_id=event_id
                )
            else:
                if self._identity(previous) != self._identity(parsed):
                    raise StateError(
                        f"memory orchestration row {row_number} changes immutable identity"
                    )
                if parsed["event"] not in _EXPOSURE_TRANSITIONS[previous.state]:
                    raise StateError(
                        f"memory orchestration row {row_number} is out of order"
                    )
                if previous.content_sha256 is not None and (
                    parsed["content_sha256"] != previous.content_sha256
                    or parsed["content_length"] != previous.content_length
                ):
                    raise StateError(
                        f"memory orchestration row {row_number} changes content descriptor"
                    )
                if parsed["event"] == "consumed":
                    if parsed["reply"] is None:
                        raise StateError("consumed exposure has no reply-use evidence")
                    previous_reply = _reply_tuple(
                        None
                        if previous.reply_use_id is None
                        else ReplyUseEvidence(
                            previous.reply_use_id,
                            previous.reply_content_sha256,
                            previous.reply_content_length,
                        )
                    )
                    if previous_reply[0] is not None and previous_reply != _reply_tuple(
                        parsed["reply"]
                    ):
                        raise StateError(
                            "consumed exposure reply-use evidence conflicts"
                        )
                current = self._record(
                    parsed,
                    events=(*previous.events, parsed["event"]),
                    last_event_id=event_id,
                )
            states[exposure_id] = current
        return states, event_rows

    def _snapshot(self) -> dict[str, ExposureRecord]:
        with file_lock(self.mutation_lock):
            return self._replay_unlocked()[0]

    @staticmethod
    def _candidate_identity(candidate: SourceCandidate) -> tuple[Any, ...]:
        return (
            candidate.source_ref,
            candidate.source_class,
            candidate.source_event_time,
            candidate.created_at,
            candidate.expires_at,
        )

    @staticmethod
    def _candidate_descriptor(
        candidate: SourceCandidate,
    ) -> tuple[str | None, int | None]:
        return candidate.content_sha256, candidate.content_length

    def _new_row(
        self,
        *,
        event: str,
        event_id: str,
        exposure_id: str,
        context: ExposureContext,
        candidate: SourceCandidate,
        created_at: datetime,
        reply: ReplyUseEvidence | None = None,
        failure_code: str | None = None,
    ) -> dict[str, Any]:
        digest, length = self._candidate_descriptor(candidate)
        if event in {"opened", "exposed", "used", "consumed"} and (
            digest is None or length is None
        ):
            raise ValueError(f"{event} requires a source content descriptor")
        reply_id, reply_hash, reply_length = _reply_tuple(reply)
        return {
            "schema_version": ORCHESTRATION_SCHEMA,
            "kind": "exposure_event",
            "event_id": _text(event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES),
            "event": event,
            "state": event,
            "exposure_id": _text(exposure_id, "exposure_id"),
            "session_id": context.session_id,
            "lifecycle_id": context.lifecycle_id,
            "turn_id": context.turn_id,
            "context_source_kind": context.source_kind,
            "source_ref": candidate.source_ref,
            "source_class": candidate.source_class,
            "source_event_time": isoformat(candidate.source_event_time),
            "source_created_at": isoformat(candidate.created_at),
            "source_expires_at": None
            if candidate.expires_at is None
            else isoformat(candidate.expires_at),
            "content_sha256": digest,
            "content_length": length,
            "created_at": isoformat(created_at),
            "reply_use_id": reply_id,
            "reply_content_sha256": reply_hash,
            "reply_content_length": reply_length,
            "failure_code": failure_code,
        }

    @staticmethod
    def _validate_fresh(candidate: SourceCandidate, now: datetime) -> None:
        if candidate.expires_at is not None and now >= candidate.expires_at:
            raise ExpiredEvidenceError(
                f"source evidence has expired: {candidate.source_ref}"
            )

    def record_selected(
        self,
        candidate: SourceCandidate,
        *,
        context: ExposureContext,
        exposure_id: str | None = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        if not isinstance(candidate, SourceCandidate):
            raise TypeError("candidate must be a SourceCandidate")
        if not isinstance(context, ExposureContext):
            raise TypeError("context must be an ExposureContext")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_exposure_id = (
            exposure_id
            or hashlib.sha256(
                f"{context.session_id}|{context.lifecycle_id}|{context.turn_id}|{candidate.source_ref}".encode()
            ).hexdigest()[:32]
        )
        actual_event_id = event_id or new_id("memory_exposure")
        _text(actual_exposure_id, "exposure_id")
        _text(actual_event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES)
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            existing = states.get(actual_exposure_id)
            if existing is not None:
                if self._identity(existing) != (
                    actual_exposure_id,
                    context.session_id,
                    context.lifecycle_id,
                    context.turn_id,
                    context.source_kind,
                    *self._candidate_identity(candidate),
                ):
                    raise ExposureConflictError(
                        f"exposure identity conflicts: {actual_exposure_id}"
                    )
                if existing.content_sha256 is not None and self._candidate_descriptor(
                    candidate
                ) != (existing.content_sha256, existing.content_length):
                    raise ExposureConflictError(
                        f"exposure content descriptor conflicts: {actual_exposure_id}"
                    )
                if actual_event_id in event_rows:
                    row = event_rows[actual_event_id]
                    if (
                        row.get("exposure_id") != actual_exposure_id
                        or row.get("event") != "selected"
                    ):
                        raise ExposureConflictError("exposure event_id conflicts")
                elif event_id is not None:
                    raise ExposureConflictError(
                        "selection event_id does not match the replayed selection"
                    )
                return existing
            if actual_event_id in event_rows:
                raise ExposureConflictError(
                    f"exposure event_id already exists: {actual_event_id}"
                )
            row = self._new_row(
                event="selected",
                event_id=actual_event_id,
                exposure_id=actual_exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed, events=("selected",), last_event_id=actual_event_id
            )

    def record_opened(
        self,
        exposure_id: str,
        material: SourceMaterial,
        *,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        if not isinstance(material, SourceMaterial):
            raise TypeError("opened evidence must be SourceMaterial")
        exposure_id = _text(exposure_id, "exposure_id")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if (
                material.source_ref != current.source_ref
                or material.source_class != current.source_class
                or material.source_event_time != current.source_event_time
                or material.created_at != current.source_created_at
                or material.expires_at != current.source_expires_at
            ):
                raise ExposureConflictError(
                    "opened evidence identity conflicts with selected source"
                )
            if current.state in {"opened", "exposed", "used", "consumed"}:
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "opened"
                    ):
                        raise ExposureConflictError(
                            "open event_id does not match the replayed opening"
                        )
                if (
                    current.content_sha256 != material.content_sha256
                    or current.content_length != material.content_length
                ):
                    raise ExposureConflictError(
                        "opened evidence descriptor conflicts on replay"
                    )
                return current
            if current.state == "failed_to_open":
                raise ValueError(
                    "failed-to-open exposure cannot be opened without a new selection"
                )
            if current.state != "selected":
                raise ValueError(f"exposure cannot open from {current.state}")
            if current.content_sha256 is not None and (
                material.content_sha256 != current.content_sha256
                or material.content_length != current.content_length
            ):
                raise ExposureConflictError(
                    "opened evidence descriptor does not match selected candidate"
                )
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=material.content_sha256,
                content_length=material.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="opened",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "opened"),
                last_event_id=actual_event_id,
            )

    def record_open_failed(
        self,
        exposure_id: str,
        failure_code: str,
        *,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        failure_code = _text(failure_code, "failure_code", limit=_MAX_FAILURE_BYTES)
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state == "failed_to_open":
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "failed_to_open"
                    ):
                        raise ExposureConflictError(
                            "failure event_id does not match the replayed failure"
                        )
                if current.failure_code != failure_code:
                    raise ExposureConflictError("failed-to-open replay conflicts")
                return current
            if current.state != "selected":
                raise ValueError(f"exposure cannot fail open from {current.state}")
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="failed_to_open",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
                failure_code=failure_code,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "failed_to_open"),
                last_event_id=actual_event_id,
            )

    def record_exposed(
        self,
        exposure_id: str,
        *,
        event_id: str | None = None,
        exposure_cap: int | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        if exposure_cap is not None and (
            type(exposure_cap) is not int or exposure_cap <= 0
        ):
            raise ValueError("exposure_cap must be a positive integer")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state in {"exposed", "used", "consumed"}:
                if event_id is not None:
                    row = event_rows.get(actual_event_id)
                    if (
                        row is None
                        or row.get("exposure_id") != exposure_id
                        or row.get("event") != "exposed"
                    ):
                        raise ExposureConflictError(
                            "exposure event_id does not match the replayed exposure"
                        )
                return current
            if current.state != "opened":
                raise ValueError(f"exposure cannot become exposed from {current.state}")
            if exposure_cap is not None:
                exposed_count = sum(
                    item.session_id == current.session_id
                    and item.state in {"exposed", "used", "consumed"}
                    for item in states.values()
                )
                if exposed_count >= exposure_cap:
                    raise ValueError("session exposure cap reached")
            if actual_event_id in event_rows:
                raise ExposureConflictError("exposure event_id already exists")
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event="exposed",
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, "exposed"),
                last_event_id=actual_event_id,
            )

    def _transition(
        self,
        exposure_id: str,
        *,
        target: str,
        reply: ReplyUseEvidence | None = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        exposure_id = _text(exposure_id, "exposure_id")
        if target not in {"used", "consumed"}:
            raise ValueError("invalid exposure transition")
        if target == "consumed" and reply is None:
            raise ValueError("consumed requires matching reply-use evidence")
        effective_now = self.clock() if now is None else _time(now, "now")
        actual_event_id = event_id or new_id("memory_exposure")
        _text(actual_event_id, "exposure event_id", limit=_MAX_EVENT_ID_BYTES)
        with file_lock(self.mutation_lock):
            states, event_rows = self._replay_unlocked()
            current = states.get(exposure_id)
            if current is None:
                raise ValueError(f"exposure does not exist: {exposure_id}")
            if current.state == target or (
                target == "used" and current.state in {"consumed"}
            ):
                if reply is not None and current.reply_use_id is not None:
                    if _reply_tuple(reply) != (
                        current.reply_use_id,
                        current.reply_content_sha256,
                        current.reply_content_length,
                    ):
                        raise ExposureConflictError(
                            "reply-use evidence conflicts on replay"
                        )
                if actual_event_id in event_rows:
                    row = event_rows[actual_event_id]
                    if (
                        row.get("exposure_id") != exposure_id
                        or row.get("event") != target
                    ):
                        raise ExposureConflictError("exposure event_id conflicts")
                elif event_id is not None:
                    raise ExposureConflictError(
                        "transition event_id does not match the replayed transition"
                    )
                return current
            if target not in _EXPOSURE_TRANSITIONS.get(current.state, frozenset()):
                raise ValueError(
                    f"exposure cannot transition from {current.state} to {target}"
                )
            if (
                target == "consumed"
                and current.reply_use_id is not None
                and _reply_tuple(reply)
                != (
                    current.reply_use_id,
                    current.reply_content_sha256,
                    current.reply_content_length,
                )
            ):
                raise ExposureConflictError(
                    "consumed reply-use evidence does not match used evidence"
                )
            if actual_event_id in event_rows:
                raise ExposureConflictError(
                    f"exposure event_id already exists: {actual_event_id}"
                )
            candidate = SourceCandidate(
                source_ref=current.source_ref,
                source_class=current.source_class,
                source_event_time=current.source_event_time,
                created_at=current.source_created_at,
                expires_at=current.source_expires_at,
                content_sha256=current.content_sha256,
                content_length=current.content_length,
            )
            context = ExposureContext(
                session_id=current.session_id,
                lifecycle_id=current.lifecycle_id,
                turn_id=current.turn_id,
                source_kind=current.context_source_kind,
                observed_at=effective_now,
            )
            row = self._new_row(
                event=target,
                event_id=actual_event_id,
                exposure_id=exposure_id,
                context=context,
                candidate=candidate,
                created_at=effective_now,
                reply=reply,
            )
            self.ledger.append(row)
            parsed = _parse_ledger_row(row, 1)
            return self._record(
                parsed,
                events=(*current.events, target),
                last_event_id=actual_event_id,
            )

    def record_used(
        self,
        exposure_id: str,
        evidence: ReplyUseEvidence | None = None,
        *,
        reply_use_id: str | None = None,
        reply_content_sha256: str | None = None,
        reply_content_length: int | None = None,
        reply_content: Any = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        reply = _coerce_reply(
            evidence,
            reply_use_id=reply_use_id,
            reply_content_sha256=reply_content_sha256,
            reply_content_length=reply_content_length,
            reply_content=reply_content,
        )
        return self._transition(
            exposure_id, target="used", reply=reply, event_id=event_id, now=now
        )

    def record_consumed(
        self,
        exposure_id: str,
        evidence: ReplyUseEvidence | None = None,
        *,
        reply_use_id: str | None = None,
        reply_content_sha256: str | None = None,
        reply_content_length: int | None = None,
        reply_content: Any = None,
        event_id: str | None = None,
        now: datetime | None = None,
    ) -> ExposureRecord:
        reply = _coerce_reply(
            evidence,
            reply_use_id=reply_use_id,
            reply_content_sha256=reply_content_sha256,
            reply_content_length=reply_content_length,
            reply_content=reply_content,
        )
        return self._transition(
            exposure_id, target="consumed", reply=reply, event_id=event_id, now=now
        )

    def get(self, exposure_id: str) -> ExposureRecord | None:
        exposure_id = _text(exposure_id, "exposure_id")
        return self._snapshot().get(exposure_id)

    def records_for_session(self, session_id: str) -> tuple[ExposureRecord, ...]:
        session_id = _text(session_id, "session_id")
        return tuple(
            sorted(
                (
                    item
                    for item in self._snapshot().values()
                    if item.session_id == session_id
                ),
                key=lambda item: item.exposure_id,
            )
        )

    def session_count(self, session_id: str) -> int:
        return sum(
            item.state in {"exposed", "used", "consumed"}
            for item in self.records_for_session(session_id)
        )

    def last_exposure_at(self, session_id: str, source_ref: str) -> datetime | None:
        session_id = _text(session_id, "session_id")
        source_ref = _text(source_ref, "source_ref")
        with file_lock(self.mutation_lock):
            self._replay_unlocked()
            values: list[datetime] = []
            for row in self.ledger.rows():
                parsed = _parse_ledger_row(row, 0)
                if (
                    parsed["event"] == "exposed"
                    and parsed["session_id"] == session_id
                    and parsed["source_ref"] == source_ref
                ):
                    values.append(parsed["created_at"])
            return max(values) if values else None

    def events(self) -> tuple[dict[str, Any], ...]:
        with file_lock(self.mutation_lock):
            self._replay_unlocked()
            return tuple(dict(row) for row in self.ledger.rows())

    def replay(self) -> tuple[ExposureRecord, ...]:
        return tuple(
            sorted(self._snapshot().values(), key=lambda item: item.exposure_id)
        )

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Return content-free exposure facts without taking the mutation lock."""

        effective_now = _observer_validate_context(target_date, now)
        return _observer_exposure_facts(
            self,
            target_date=target_date,
            now=effective_now,
        )


def _observer_exposure_replay(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[ExposureRecord, ...]:
    """Replay exposure envelopes without ``JsonlLedger.rows`` or a lock."""

    states: dict[str, ExposureRecord] = {}
    event_rows: dict[str, Mapping[str, Any]] = {}
    for row_number, row in enumerate(rows, start=1):
        parsed = _parse_ledger_row(row, row_number)
        event_id = parsed["event_id"]
        if event_id in event_rows:
            raise StateError("duplicate memory orchestration event id")
        event_rows[event_id] = row
        exposure_id = parsed["exposure_id"]
        previous = states.get(exposure_id)
        if previous is None:
            if parsed["event"] != "selected":
                raise StateError("exposure ledger starts after selected")
            current = ExposureLedger._record(
                parsed, events=("selected",), last_event_id=event_id
            )
        else:
            identity = (
                parsed["exposure_id"],
                parsed["session_id"],
                parsed["lifecycle_id"],
                parsed["turn_id"],
                parsed["context_source_kind"],
                parsed["source_ref"],
                parsed["source_class"],
                parsed["source_event_time"],
                parsed["source_created_at"],
                parsed["source_expires_at"],
            )
            if ExposureLedger._identity(previous) != identity:
                raise StateError("exposure ledger changes immutable identity")
            if parsed["event"] not in _EXPOSURE_TRANSITIONS[previous.state]:
                raise StateError("exposure ledger contains an out-of-order event")
            if previous.content_sha256 is not None and (
                parsed["content_sha256"] != previous.content_sha256
                or parsed["content_length"] != previous.content_length
            ):
                raise StateError("exposure ledger changes content descriptor")
            if parsed["event"] == "consumed":
                reply = parsed["reply"]
                if reply is None:
                    raise StateError("consumed exposure has no reply evidence")
                if previous.reply_use_id is not None and _reply_tuple(reply) != (
                    previous.reply_use_id,
                    previous.reply_content_sha256,
                    previous.reply_content_length,
                ):
                    raise StateError("consumed exposure reply evidence conflicts")
            current = ExposureLedger._record(
                parsed,
                events=(*previous.events, parsed["event"]),
                last_event_id=event_id,
            )
        states[exposure_id] = current
    return tuple(sorted(states.values(), key=lambda item: item.exposure_id))


def _observer_exposure_facts(
    ledger: Any,
    *,
    target_date: date,
    now: datetime,
    policy: Any = None,
) -> tuple[ObservationFact, ...]:
    """Project exposure state and policy limits using only safe envelopes."""

    ledger_owner = getattr(ledger, "ledger", None)
    path = getattr(ledger_owner, "path", None)
    if path is None:
        return (
            ObservationFact(
                key="memory.exposure.adapter",
                code="exposure_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("exposure",),
            ),
        )
    try:
        rows = _observer_jsonl_rows(path)
        records = _observer_exposure_replay(rows)
    except Exception as exc:  # noqa: BLE001 - observer must fail closed
        del exc
        return (_observer_integrity_fact("exposure", target_date=target_date),)
    if not records:
        return ()

    exposure_positions = {
        row.get("exposure_id"): position
        for position, row in enumerate(rows)
        if type(row.get("exposure_id")) is str
    }
    facts: list[ObservationFact] = []
    success_states = {"exposed", "used", "consumed"}
    failed = [record for record in records if record.state == "failed_to_open"]
    for record in records:
        refs = _observer_refs(
            record.exposure_id,
            record.last_event_id,
            record.source_ref,
        )
        for event in record.events:
            if event == "failed_to_open":
                continue
            facts.append(
                ObservationFact(
                    key=f"memory.exposure.event:{record.exposure_id}:{event}",
                    code=f"exposure_{event}",
                    state="neutral",
                    target_date=target_date,
                    event_time=record.created_at,
                    refs=refs,
                    counts={f"exposure_{event}": 1},
                )
            )

    for record in failed:
        later = max(
            (
                candidate
                for candidate in records
                if candidate.source_ref == record.source_ref
                and candidate.state in success_states
                and exposure_positions.get(candidate.exposure_id, -1)
                > exposure_positions.get(record.exposure_id, -1)
            ),
            key=lambda candidate: candidate.created_at,
            default=None,
        )
        recovery = None
        state = "current"
        refs = _observer_refs(
            record.exposure_id,
            record.last_event_id,
            record.source_ref,
        )
        if later is not None:
            state = "recovered_history"
            recovery_ref = later.last_event_id or later.exposure_id
            recovery = RecoveryEvidence(
                recovery_ref,
                f"exposure_{later.state}",
                later.created_at,
            )
            refs = _observer_refs(
                *refs,
                later.exposure_id,
                later.last_event_id,
            )
        facts.append(
            ObservationFact(
                key=f"memory.exposure.open_failed:{record.source_ref}",
                code="exposure_open_failed",
                state=state,
                target_date=target_date,
                event_time=record.created_at,
                refs=refs,
                counts={"exposure_open_failed": 1},
                recovery=recovery,
            )
        )

    # A cap and source cooldown are policy observations, not ledger mutations.
    if policy is not None:
        cap = getattr(policy, "max_per_session", None)
        if type(cap) is int and cap > 0:
            by_session: dict[str, list[ExposureRecord]] = {}
            for record in records:
                if record.state in success_states:
                    by_session.setdefault(record.session_id, []).append(record)
            for session_id, exposed in sorted(by_session.items()):
                if len(exposed) >= cap:
                    facts.append(
                        ObservationFact(
                            key=f"memory.exposure.cap:{session_id}",
                            code="exposure_cap_reached",
                            state="neutral",
                            target_date=target_date,
                            event_time=max(item.created_at for item in exposed),
                            refs=_observer_refs(session_id),
                            counts={
                                "exposure_count": len(exposed),
                                "exposure_cap": cap,
                            },
                        )
                    )
        cooldown = getattr(policy, "source_cooldown", None)
        if isinstance(cooldown, timedelta) and cooldown > timedelta(0):
            latest: dict[tuple[str, str], ExposureRecord] = {}
            for record in records:
                if record.state not in success_states:
                    continue
                identity = (record.session_id, record.source_ref)
                previous = latest.get(identity)
                if previous is None or record.created_at > previous.created_at:
                    latest[identity] = record
            for (session_id, source_ref), record in sorted(latest.items()):
                if now - record.created_at < cooldown:
                    facts.append(
                        ObservationFact(
                            key=f"memory.exposure.cooldown:{session_id}:{source_ref}",
                            code="exposure_cooldown",
                            state="neutral",
                            target_date=target_date,
                            event_time=record.created_at,
                            refs=_observer_refs(
                                session_id,
                                source_ref,
                                record.last_event_id,
                            ),
                            counts={"cooldown_seconds": int(cooldown.total_seconds())},
                        )
                    )
    return _observer_merge_facts(facts)


@dataclass(frozen=True, slots=True)
class ExposurePolicy:
    """Deterministic selection limits and first/continuation reservoir rules."""

    max_per_session: int = 8
    source_cooldown: timedelta = timedelta(minutes=30)
    result_budget: int = 8
    exposure_cap: int | None = None

    def __post_init__(self) -> None:
        cap = self.max_per_session if self.exposure_cap is None else self.exposure_cap
        if type(cap) is not int or cap <= 0:
            raise ValueError("exposure cap must be a positive integer")
        if type(self.max_per_session) is not int or self.max_per_session <= 0:
            raise ValueError("max_per_session must be a positive integer")
        if type(self.result_budget) is not int or self.result_budget <= 0:
            raise ValueError("result_budget must be a positive integer")
        if not isinstance(
            self.source_cooldown, timedelta
        ) or self.source_cooldown < timedelta(0):
            raise ValueError("source_cooldown must be non-negative")
        object.__setattr__(self, "max_per_session", cap)
        object.__setattr__(self, "exposure_cap", cap)

    def choose(
        self,
        candidates: Iterable[SourceCandidate],
        *,
        context: ExposureContext,
        ledger: ExposureLedger,
        now: datetime,
        continuity_policy: Callable[[str, str], bool] | None = None,
        first_turn: bool | None = None,
    ) -> "ExposurePlan":
        effective_now = _time(now, "now")
        incoming = list(candidates)
        if any(not isinstance(item, SourceCandidate) for item in incoming):
            raise TypeError("selection candidates must be SourceCandidate instances")
        is_first = context.turn_index == 0 if first_turn is None else first_turn
        prior = tuple(
            item
            for item in ledger.records_for_session(context.session_id)
            if item.state in {"exposed", "used", "consumed"}
        )
        remaining = self.max_per_session - len(prior)
        if remaining <= 0:
            mode = "self_brief" if is_first else "wider_reservoir"
            return ExposurePlan(mode, (), 0, self.result_budget)
        if is_first:
            mode = "self_brief"
            brief = [
                item
                for item in incoming
                if item.source_class.lower() in _SELF_BRIEF_CLASSES
            ]
            pool = brief or incoming
        else:
            previous_time = max(
                (item.source_event_time for item in prior), default=None
            )
            delta = [
                item
                for item in incoming
                if previous_time is None or item.source_event_time > previous_time
            ]
            mode = "continuation_delta" if delta else "wider_reservoir"
            pool = delta or incoming
        selected: list[SourceCandidate] = []
        seen: set[str] = set()
        for candidate in sorted(
            pool,
            key=lambda item: (
                -float(item.relevance),
                -item.source_event_time.timestamp(),
                item.source_ref,
            ),
        ):
            if candidate.source_ref in seen:
                continue
            seen.add(candidate.source_ref)
            if _is_private_class(candidate.source_class):
                if continuity_policy is None:
                    if context.source_kind != "private_inbound":
                        raise PolicyDeniedError(
                            f"private continuity is not authorized for {context.source_kind}"
                        )
                elif not continuity_policy(candidate.source_class, context.source_kind):
                    raise PolicyDeniedError(
                        f"source class is not authorized: {candidate.source_class}"
                    )
            last = ledger.last_exposure_at(context.session_id, candidate.source_ref)
            if last is not None and effective_now - last < self.source_cooldown:
                continue
            selected.append(candidate)
            if len(selected) >= min(remaining, self.result_budget):
                break
        return ExposurePlan(mode, tuple(selected), remaining, self.result_budget)


@dataclass(frozen=True, slots=True)
class ExposurePlan:
    mode: str
    candidates: tuple[SourceCandidate, ...]
    remaining_cap: int
    result_budget: int

    @property
    def selected(self) -> tuple[SourceCandidate, ...]:
        return self.candidates


def _is_private_class(source_class: str) -> bool:
    normalized = source_class.strip().lower()
    return normalized in _PRIVATE_SOURCE_CLASSES or normalized.startswith("private_")


__all__ = ()
