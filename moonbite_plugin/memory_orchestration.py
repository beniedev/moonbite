"""Host-neutral memory orchestration primitives for MB-50.

This module is deliberately a port layer.  A retriever can return opaque
references and bounded metadata, while an opener is called only after a
reference has been selected.  The durable part of the module is an
append-only exposure ledger: it records the reference and evidence
descriptor, never source material.  Memory records and writer effects are
delegated to their injected stores.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Protocol

from ._memory_orchestration.contracts import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureRecord,
    ExposedSource,
    MissingEvidenceError,
    ORCHESTRATION_SCHEMA,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRetriever,
    _hash,
    _HASH_RE,
    _length,
    _MAX_SOURCE_BYTES,
    _text,
    _time,
    content_descriptor,
)
from ._memory_orchestration.exposure import (
    EXPOSURE_EVENTS,
    EXPOSURE_STATES,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    _observer_exposure_facts,
)
from ._memory_orchestration.observation import (
    observer_integrity_fact as _observer_integrity_fact,
    observer_jsonl_rows as _observer_jsonl_rows,
    observer_merge_facts as _observer_merge_facts,
    observer_refs as _observer_refs,
    observer_validate_context as _observer_validate_context,
)
from ._memory_orchestration.sources import SourceRegistry
from .effects import EffectLedger, EffectReceipt, EffectRecord, _valid_transition
from .observer import ObservationFact, RecoveryEvidence
from .runtime_core import (
    JsonlLedger,
    StateError,
    file_lock,
    isoformat,
    utc_now,
)


WRITER_OPERATIONS = frozenset(
    {"turn_persistence", "flush", "diary", "consolidation", "maintenance"}
)

_MAX_REASON_BYTES = 4 * 1024
_WRITER_CURRENT_STATES = frozenset(
    {"pending", "executed_unverified", "expired", "failed", "requeued"}
)

for _public_contract in (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    ExposureRecord,
    ExposedSource,
    MissingEvidenceError,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRegistry,
    SourceRetriever,
    content_descriptor,
):
    _public_contract.__module__ = __name__
del _public_contract


class MaintenanceApprovalAdapter(Protocol):
    """Adapter-declared approval classification; core never infers it."""

    def approval_required(self, proposal: Mapping[str, Any]) -> bool: ...


_APPROVAL_SCHEMA = "moon.memory.approval.v1"
_APPROVAL_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event",
        "proposal_id",
        "required",
        "approved",
        "created_at",
        "evidence_sha256",
        "evidence_length",
    }
)
_MAINTENANCE_OBSERVER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "proposal_id",
        "request_id",
        "created_at",
        "operation",
        "evidence_refs",
        "evidence_sha256",
        "reason",
        "proposed_value",
        "status",
        "applied",
    }
)
_HISTORY_OBSERVER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "event_id",
        "proposal_id",
        "request_id",
        "created_at",
        "operation",
        "activity",
        "permission",
        "evidence_sha256",
        "created_card",
        "archived_card_ids",
        "status",
    }
)


def _approval_descriptor(value: Any) -> tuple[str, int]:
    if isinstance(value, bool) or value is None:
        raise ValueError("approval evidence must be explicit non-boolean evidence")
    return content_descriptor(value)


def _approval_classification(adapter: Any, proposal: Mapping[str, Any]) -> bool:
    """Read only an adapter-declared approval classification."""

    if adapter is None:
        return False
    method = getattr(adapter, "approval_required", None)
    if method is None:
        method = getattr(adapter, "requires_approval", None)
    if callable(method):
        result = method(proposal)
    elif type(method) is bool:
        result = method
    elif callable(adapter):
        result = adapter(proposal)
    else:
        raise TypeError("approval adapter must provide an approval_required flag")
    if type(result) is not bool:
        raise ValueError("approval adapter must return a boolean")
    return result


class MemoryMaintenanceFacade:
    """Reference-only facade for proposal, apply, and archive semantics."""

    def __init__(
        self,
        memory_store: Any,
        *,
        approval_adapter: Any = None,
        root: Path | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.memory_store = memory_store
        self.approval_adapter = approval_adapter
        self.clock = clock
        maintenance_owner = getattr(memory_store, "maintenance", None)
        history_owner = getattr(memory_store, "history", None)
        self._maintenance_path = getattr(maintenance_owner, "path", None)
        self._history_path = getattr(history_owner, "path", None)
        if root is None:
            path = self._maintenance_path
            if path is not None:
                root = Path(path).parent
        self._root = None if root is None else Path(root)
        self._approval_ledger = (
            None
            if root is None
            else JsonlLedger(Path(root) / "memory_orchestration_approvals.jsonl")
        )
        self._approval_lock = (
            None
            if root is None
            else Path(root) / "memory_orchestration_approvals.mutation.lock"
        )
        self._approval_memory: dict[str, tuple[bool, bool, str | None, int | None]] = {}
        self._proposals: dict[str, Mapping[str, Any]] = {}

    def _approval_replay(self) -> dict[str, tuple[bool, bool, str | None, int | None]]:
        if self._approval_ledger is None:
            return dict(self._approval_memory)
        states: dict[str, tuple[bool, bool, str | None, int | None]] = {}
        for index, row in enumerate(self._approval_ledger.rows(), start=1):
            if set(row) != _APPROVAL_FIELDS:
                raise StateError(f"approval row {index} has unsupported fields")
            if row["schema_version"] != _APPROVAL_SCHEMA or row["kind"] != "approval":
                raise StateError(f"approval row {index} has an unsupported schema")
            proposal_id = _text(row["proposal_id"], "proposal_id")
            if type(row["required"]) is not bool or type(row["approved"]) is not bool:
                raise StateError(f"approval row {index} has invalid booleans")
            _time(row["created_at"], "approval created_at")
            digest = row["evidence_sha256"]
            length = row["evidence_length"]
            if digest is None or length is None:
                if digest is not None or length is not None:
                    raise StateError(f"approval row {index} has partial evidence")
            else:
                _hash(digest, "approval evidence hash")
                _length(length, "approval evidence length")
            if row["event"] == "pending":
                if row["approved"] or digest is not None:
                    raise StateError(f"approval row {index} has invalid pending state")
                if proposal_id in states:
                    raise StateError(f"duplicate approval pending event: {proposal_id}")
            elif row["event"] == "approved":
                if not row["required"] or not row["approved"] or digest is None:
                    raise StateError(f"approval row {index} has invalid approved state")
                previous = states.get(proposal_id)
                if previous is None or not previous[0] or previous[1]:
                    raise StateError(f"approval row {index} is out of order")
            else:
                raise StateError(f"approval row {index} has invalid event")
            states[proposal_id] = (row["required"], row["approved"], digest, length)
        return states

    def _approval_state(
        self,
        proposal_id: str,
    ) -> tuple[bool, bool, str | None, int | None] | None:
        with (
            file_lock(self._approval_lock)
            if self._approval_lock is not None
            else nullcontext()
        ):
            return self._approval_replay().get(proposal_id)

    def _append_approval(
        self,
        proposal_id: str,
        *,
        required: bool,
        approved: bool,
        digest: str | None = None,
        length: int | None = None,
    ) -> None:
        event = "approved" if approved else "pending"
        row = {
            "schema_version": _APPROVAL_SCHEMA,
            "kind": "approval",
            "event": event,
            "proposal_id": _text(proposal_id, "proposal_id"),
            "required": required,
            "approved": approved,
            "created_at": isoformat(self.clock()),
            "evidence_sha256": digest,
            "evidence_length": length,
        }
        if self._approval_ledger is None:
            states = self._approval_replay()
            if proposal_id in states and (not approved or states[proposal_id][1]):
                return
            self._approval_memory[proposal_id] = (
                required,
                approved,
                digest,
                length,
            )
            return
        self._approval_ledger.append(row)

    def _classify_approval(
        self,
        proposal: Mapping[str, Any],
        explicit: bool | None,
    ) -> bool:
        adapter_required = _approval_classification(self.approval_adapter, proposal)
        if explicit is not None:
            if type(explicit) is not bool:
                raise ValueError("approval_required must be a boolean")
            return adapter_required or explicit
        return adapter_required

    def propose(
        self,
        *,
        request_id: str,
        operation: str,
        evidence_refs: Iterable[str],
        reason: str,
        proposed_value: Any = None,
        approval_required: bool | None = None,
        sensitive: bool | None = None,
    ) -> Mapping[str, Any]:
        method = getattr(self.memory_store, "propose_maintenance", None)
        if not callable(method):
            raise TypeError("injected memory store lacks maintenance proposal support")
        if sensitive is not None:
            if type(sensitive) is not bool:
                raise ValueError("sensitive must be a boolean")
            if approval_required is not None and approval_required != sensitive:
                raise ValueError("sensitivity and approval classification conflict")
            approval_required = sensitive
        proposal = method(
            request_id=request_id,
            operation=operation,
            evidence_refs=evidence_refs,
            reason=reason,
            proposed_value=proposed_value,
        )
        proposal_id = _text(
            proposal.get("proposal_id") or f"proposal:{request_id}", "proposal_id"
        )
        required = self._classify_approval(proposal, approval_required)
        self._proposals[proposal_id] = dict(proposal)
        with (
            file_lock(self._approval_lock)
            if self._approval_lock is not None
            else nullcontext()
        ):
            existing = self._approval_replay().get(proposal_id)
            if existing is None:
                self._append_approval(proposal_id, required=required, approved=False)
            elif existing[0] != required:
                raise StateError("approval classification conflicts on replay")
        result = dict(proposal)
        result["approval_required"] = required
        result["approval_state"] = "pending" if required else "not_required"
        return result

    def apply(
        self,
        proposal_id: str,
        *,
        activity: str,
        permission: str,
        approval_evidence: Any = None,
        approval: bool | None = None,
    ) -> Mapping[str, Any]:
        proposal_id = _text(proposal_id, "proposal_id")
        state = self._approval_state(proposal_id)
        if state is None:
            raise ValueError(f"maintenance proposal is not registered: {proposal_id}")
        if state is not None and state[0] and not state[1]:
            if approval is not None and type(approval) is not bool:
                raise ValueError("approval must be a boolean")
            if approval is False:
                approval_evidence = None
            if approval_evidence is None:
                return {
                    "proposal_id": proposal_id,
                    "status": "pending",
                    "reason": "approval_required",
                    "approval_requested": True,
                    "write_performed": False,
                }
            digest, length = _approval_descriptor(approval_evidence)
            with (
                file_lock(self._approval_lock)
                if self._approval_lock is not None
                else nullcontext()
            ):
                state = self._approval_replay().get(proposal_id)
                if state is None or not state[0]:
                    raise StateError("approval state disappeared")
                if not state[1]:
                    self._append_approval(
                        proposal_id,
                        required=True,
                        approved=True,
                        digest=digest,
                        length=length,
                    )
        method = getattr(self.memory_store, "apply_maintenance", None)
        if not callable(method):
            raise TypeError("injected memory store lacks maintenance apply support")
        return method(proposal_id, activity=activity, permission=permission)

    def archive(
        self,
        *,
        request_id: str,
        evidence_refs: Iterable[str],
        reason: str,
    ) -> Mapping[str, Any]:
        return self.propose(
            request_id=request_id,
            operation="retire",
            evidence_refs=evidence_refs,
            reason=reason,
        )

    def approval_required(self, proposal: Mapping[str, Any]) -> bool:
        declared = False
        if isinstance(proposal, Mapping):
            raw_declared = proposal.get("approval_required")
            if raw_declared is not None and type(raw_declared) is not bool:
                raise ValueError("proposal approval_required must be a boolean")
            declared = raw_declared is True
        adapter_required = _approval_classification(self.approval_adapter, proposal)
        return adapter_required or declared

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Read proposal/approval envelopes without opening memory content."""

        _observer_validate_context(target_date, now)
        return _observer_maintenance_facts(
            self,
            target_date=target_date,
            now=now,
        )


def _observer_maintenance_proposal(
    row: Mapping[str, Any], row_number: int
) -> dict[str, Any]:
    if set(row) != _MAINTENANCE_OBSERVER_FIELDS:
        raise StateError(
            f"maintenance observer row {row_number} has unsupported fields"
        )
    if row["schema_version"] != "moon.memory.maintenance.v1":
        raise StateError("maintenance observer row has an unsupported schema")
    if row["kind"] != "maintenance_proposal":
        raise StateError("maintenance observer row has an unsupported kind")
    proposal_id = _text(row["proposal_id"], "proposal_id")
    request_id = _text(row["request_id"], "request_id")
    created_at = _time(row["created_at"], "created_at")
    operation = row["operation"]
    if operation not in {"merge", "retire", "distill"}:
        raise StateError("maintenance observer row has an invalid operation")
    evidence_refs = row["evidence_refs"]
    if not isinstance(evidence_refs, list) or any(
        type(ref) is not str or not ref.strip() for ref in evidence_refs
    ):
        raise StateError("maintenance observer row has invalid evidence refs")
    digest = row["evidence_sha256"]
    if type(digest) is not str or _HASH_RE.fullmatch(digest) is None:
        raise StateError("maintenance observer row has an invalid evidence hash")
    if row["status"] != "proposed" or row["applied"] is not False:
        raise StateError("maintenance observer row has an invalid state")
    # Deliberately do not inspect ``reason`` or ``proposed_value``.  They may
    # contain memory/source material and are not observer evidence.
    return {
        "proposal_id": proposal_id,
        "request_id": request_id,
        "created_at": created_at,
        "operation": operation,
        "evidence_refs": tuple(evidence_refs),
        "evidence_sha256": digest,
    }


def _observer_maintenance_history(
    row: Mapping[str, Any], row_number: int
) -> dict[str, Any]:
    if set(row) != _HISTORY_OBSERVER_FIELDS:
        raise StateError(f"maintenance history row {row_number} has unsupported fields")
    if row["schema_version"] != "moon.memory.history.v1":
        raise StateError("maintenance history has an unsupported schema")
    if row["kind"] != "maintenance_applied" or row["status"] != "applied":
        raise StateError("maintenance history has an invalid state")
    operation = row["operation"]
    if operation not in {"merge", "retire", "distill"}:
        raise StateError("maintenance history has an invalid operation")
    request_id = _text(row["request_id"], "history request_id")
    _text(row["activity"], "history activity")
    _text(row["permission"], "history permission")
    evidence_sha256 = _hash(row["evidence_sha256"], "history evidence hash")
    archived_card_ids = row["archived_card_ids"]
    if not isinstance(archived_card_ids, list) or any(
        type(value) is not str or not value.strip() for value in archived_card_ids
    ):
        raise StateError("maintenance history has invalid archive refs")
    return {
        "event_id": _text(row["event_id"], "history event_id"),
        "proposal_id": _text(row["proposal_id"], "history proposal_id"),
        "request_id": request_id,
        "created_at": _time(row["created_at"], "history created_at"),
        "operation": operation,
        "evidence_sha256": evidence_sha256,
    }


def _observer_approval_row(row: Mapping[str, Any], row_number: int) -> dict[str, Any]:
    if set(row) != _APPROVAL_FIELDS:
        raise StateError(f"approval observer row {row_number} has unsupported fields")
    if row["schema_version"] != _APPROVAL_SCHEMA or row["kind"] != "approval":
        raise StateError("approval observer row has an unsupported schema")
    proposal_id = _text(row["proposal_id"], "approval proposal_id")
    required = row["required"]
    approved = row["approved"]
    if type(required) is not bool or type(approved) is not bool:
        raise StateError("approval observer row has invalid booleans")
    created_at = _time(row["created_at"], "approval created_at")
    digest = row["evidence_sha256"]
    length = row["evidence_length"]
    if digest is None or length is None:
        if digest is not None or length is not None:
            raise StateError("approval observer row has partial evidence")
    else:
        _hash(digest, "approval evidence hash")
        _length(length, "approval evidence length")
    event = row["event"]
    if event == "pending":
        if approved or digest is not None or length is not None:
            raise StateError("approval pending row has evidence")
    elif event == "approved":
        if not required or not approved or digest is None or length is None:
            raise StateError("approval approved row is invalid")
    else:
        raise StateError("approval observer row has an invalid event")
    return {
        "proposal_id": proposal_id,
        "required": required,
        "approved": approved,
        "created_at": created_at,
        "digest": digest,
        "length": length,
        "event": event,
    }


def _observer_maintenance_facts(
    facade: MemoryMaintenanceFacade,
    *,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Project approval/proposal receipts without reading card/diary content."""

    maintenance_rows: tuple[Mapping[str, Any], ...] = ()
    history_rows: tuple[Mapping[str, Any], ...] = ()
    approval_rows: tuple[Mapping[str, Any], ...] = ()
    try:
        if facade._maintenance_path is not None:
            maintenance_rows = _observer_jsonl_rows(facade._maintenance_path)
        if facade._history_path is not None:
            history_rows = _observer_jsonl_rows(facade._history_path)
        if facade._approval_ledger is not None:
            approval_rows = _observer_jsonl_rows(facade._approval_ledger.path)
        proposals: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(maintenance_rows, start=1):
            proposal = _observer_maintenance_proposal(row, row_number)
            proposal_id = proposal["proposal_id"]
            if proposal_id in proposals:
                raise StateError("duplicate maintenance proposal")
            proposals[proposal_id] = proposal

        # An in-memory proposal is a useful owner only when no durable
        # maintenance ledger exists.  Read its envelope fields exclusively;
        # never touch proposed_value/reason or recursively inspect the map.
        if not maintenance_rows:
            for value in facade._proposals.values():
                if not isinstance(value, Mapping):
                    continue
                proposal_id = value.get("proposal_id")
                if type(proposal_id) is not str or not proposal_id.strip():
                    continue
                operation = value.get("operation")
                if operation not in {"merge", "retire", "distill"}:
                    continue
                request_id = value.get("request_id", proposal_id)
                if type(request_id) is not str or not request_id.strip():
                    request_id = proposal_id
                else:
                    request_id = request_id.strip()
                created_at_value = value.get("created_at")
                created_at = (
                    None
                    if created_at_value is None
                    else _time(created_at_value, "in-memory proposal created_at")
                )
                proposals.setdefault(
                    proposal_id,
                    {
                        "proposal_id": proposal_id,
                        "request_id": request_id,
                        # An in-memory proposal has no durable timestamp or
                        # apply receipt to project into observer evidence.
                        "created_at": created_at,
                        "operation": operation,
                        "evidence_refs": (),
                        "evidence_sha256": None,
                    },
                )

        applied: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(history_rows, start=1):
            event = _observer_maintenance_history(row, row_number)
            proposal = proposals.get(event["proposal_id"])
            if proposal is None:
                raise StateError("maintenance history references an unknown proposal")
            if (
                event["proposal_id"] != proposal["proposal_id"]
                or event["operation"] != proposal["operation"]
                or event["request_id"] != proposal["request_id"]
                or event["evidence_sha256"] != proposal["evidence_sha256"]
            ):
                raise StateError("maintenance history does not match proposal")
            if event["proposal_id"] in applied:
                raise StateError("duplicate maintenance history event")
            applied[event["proposal_id"]] = event

        approvals: dict[str, dict[str, Any]] = {}
        for row_number, row in enumerate(approval_rows, start=1):
            approval = _observer_approval_row(row, row_number)
            proposal_id = approval["proposal_id"]
            if proposal_id not in proposals:
                raise StateError("approval references an unknown proposal")
            previous = approvals.get(proposal_id)
            if previous is None and approval["event"] == "approved":
                raise StateError("approval approval event starts without pending")
            if previous is not None:
                if approval["event"] != "approved" or previous["approved"]:
                    raise StateError("approval events are out of order")
                if not previous["required"]:
                    raise StateError("approval classification changed")
            approvals[proposal_id] = approval
        if not approvals and facade._approval_memory:
            for proposal_id, state in facade._approval_memory.items():
                if proposal_id not in proposals:
                    raise StateError("approval references an unknown proposal")
                required, approved, digest, length = state
                approvals[proposal_id] = {
                    "proposal_id": proposal_id,
                    "required": required,
                    "approved": approved,
                    # The in-memory approval state has no durable timestamp.
                    "created_at": None,
                    "digest": digest,
                    "length": length,
                    "event": "approved" if approved else "pending",
                }
    except Exception as exc:  # noqa: BLE001 - observer fails closed
        del exc
        return (_observer_integrity_fact("maintenance", target_date=target_date),)

    facts: list[ObservationFact] = []
    for proposal_id, proposal in proposals.items():
        applied_event = applied.get(proposal_id)
        if applied_event is not None:
            code = (
                "maintenance_archive_applied"
                if proposal["operation"] == "retire"
                else "maintenance_applied"
            )
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.proposal:{proposal_id}",
                    code=code,
                    state="neutral",
                    target_date=target_date,
                    event_time=applied_event["created_at"],
                    refs=_observer_refs(proposal_id, applied_event["event_id"]),
                    counts={"maintenance_applied": 1},
                )
            )
            continue
        facts.append(
            ObservationFact(
                key=f"memory.maintenance.proposal:{proposal_id}",
                code="maintenance_proposal_pending",
                state="current",
                target_date=target_date,
                event_time=proposal["created_at"],
                refs=_observer_refs(proposal_id, proposal["request_id"]),
                counts={"maintenance_proposals": 1},
            )
        )
        if proposal["operation"] == "retire":
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.archive:{proposal_id}",
                    code="maintenance_archive_proposal",
                    state="neutral",
                    target_date=target_date,
                    event_time=proposal["created_at"],
                    refs=_observer_refs(proposal_id),
                    counts={"maintenance_archives": 1},
                )
            )

    for proposal_id, approval in approvals.items():
        refs = _observer_refs(proposal_id)
        if approval["digest"] is not None:
            refs = _observer_refs(*refs, approval["digest"])
        if approval["required"] and not approval["approved"]:
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.approval:{proposal_id}",
                    code="maintenance_approval_pending",
                    state="current",
                    target_date=target_date,
                    event_time=approval["created_at"],
                    refs=refs,
                    counts={"approval_pending": 1},
                )
            )
        elif approval["approved"]:
            facts.append(
                ObservationFact(
                    key=f"memory.maintenance.approval:{proposal_id}",
                    code="maintenance_approval_verified",
                    state="neutral",
                    target_date=target_date,
                    event_time=approval["created_at"],
                    refs=refs,
                    counts={
                        "approval_verified": 1,
                        "approval_evidence_length": approval["length"] or 0,
                    },
                )
            )
    return _observer_merge_facts(facts)


@dataclass(frozen=True, slots=True)
class WriterRequest:
    """Typed, transient handoff envelope; only its descriptor is durable."""

    effect_id: str
    operation: str
    source_event_id: str
    idempotency_key: str
    epoch_id: str
    content_sha256: str
    content_length: int
    attempt: int
    content: Any


@dataclass(frozen=True, slots=True)
class WriterHandoff:
    operation: str
    effect_id: str
    record: Any
    error_type: str | None = None
    request: WriterRequest | None = None

    @property
    def queued(self) -> bool:
        return self.record.state == "executed_unverified"

    @property
    def verified(self) -> bool:
        return self.record.state == "verified"

    @property
    def failed(self) -> bool:
        return self.record.state == "failed"


class WriterCoordinator:
    """Create receipt-backed writer intents before handing work to an adapter."""

    def __init__(
        self, effect_ledger: Any, *, clock: Callable[[], datetime] = utc_now
    ) -> None:
        required = (
            "get",
            "begin_intent",
            "mark_pending",
            "mark_queue_accepted",
            "verify",
            "fail",
        )
        if any(not callable(getattr(effect_ledger, name, None)) for name in required):
            raise TypeError("effect_ledger does not provide the required port")
        self.effect_ledger = effect_ledger
        self.clock = clock
        self._handoff_thread_lock = Lock()
        lock_path = getattr(effect_ledger, "mutation_lock_path", None)
        if lock_path is None:
            lock_path = getattr(effect_ledger, "mutation_lock", None)
        self._handoff_lock_path = (
            None
            if lock_path is None
            else Path(lock_path).with_name(f"{Path(lock_path).name}.writer")
        )

    @contextmanager
    def _claim_lock(self):
        """Serialize the intent-to-pending claim across threads/processes."""

        with self._handoff_thread_lock:
            if self._handoff_lock_path is None:
                yield
            else:
                with file_lock(self._handoff_lock_path):
                    yield

    def create_intent(
        self,
        operation: str,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> EffectRecord:
        if operation not in WRITER_OPERATIONS:
            raise ValueError(f"unsupported memory writer operation: {operation}")
        digest, length = content_descriptor(content)
        created_at = self.clock()
        deadline = (
            _time(expires_at, "expires_at")
            if expires_at is not None
            else created_at + ttl
        )
        return self.effect_ledger.begin_intent(
            effect_id,
            kind=operation,
            source_event_id=_text(source_event_id, "source_event_id"),
            idempotency_key=_text(idempotency_key, "idempotency_key"),
            epoch_id=_text(epoch_id, "epoch_id"),
            content_sha256=digest,
            content_length=length,
            expires_at=deadline,
            created_at=created_at,
        )

    def handoff(
        self,
        effect_id: str,
        writer: Any,
        *,
        content: Any = None,
        operation: str | None = None,
    ) -> WriterHandoff:
        with self._claim_lock():
            record = self.effect_ledger.get(effect_id)
            if record is None:
                raise ValueError(f"writer effect does not exist: {effect_id}")
            actual_operation = record.kind if operation is None else operation
            if record.state not in {"intent", "requeued"}:
                return WriterHandoff(actual_operation, effect_id, record)
            if content is None:
                raise ValueError("first writer handoff requires transient content")
            digest, length = content_descriptor(content)
            if digest != record.content_sha256 or length != record.content_length:
                raise ValueError("writer content does not match effect descriptor")
            record = self.effect_ledger.mark_pending(effect_id)
            if record.state != "pending":
                raise StateError("effect ledger did not claim writer intent")
            request = WriterRequest(
                effect_id=record.effect_id,
                operation=record.kind,
                source_event_id=record.source_event_id,
                idempotency_key=record.idempotency_key,
                epoch_id=record.epoch_id,
                content_sha256=record.content_sha256,
                content_length=record.content_length,
                attempt=record.attempt,
                content=content,
            )
        try:
            method = writer if callable(writer) else getattr(writer, "write", None)
            if method is None and not callable(writer):
                method = getattr(writer, "persist", None)
            if not callable(method):
                raise TypeError("writer must be callable or provide write/persist")
            result = method(request)
            if isinstance(result, EffectReceipt):
                try:
                    record = self.effect_ledger.verify(effect_id, result)
                except Exception as exc:  # noqa: BLE001 - receipt mismatch is visible
                    record = self.effect_ledger.fail(
                        effect_id,
                        f"writer receipt mismatch: {type(exc).__name__}",
                        retryable=True,
                    )
                    return WriterHandoff(
                        actual_operation, effect_id, record, type(exc).__name__, request
                    )
            else:
                record = self.effect_ledger.mark_queue_accepted(effect_id)
        except Exception as exc:  # noqa: BLE001 - failure must become visible ledger state
            record = self.effect_ledger.fail(
                effect_id,
                f"writer handoff failed: {type(exc).__name__}",
                retryable=True,
            )
            return WriterHandoff(
                actual_operation, effect_id, record, type(exc).__name__, request
            )
        return WriterHandoff(actual_operation, effect_id, record, request=request)

    def verify(self, effect_id: str, receipt: EffectReceipt) -> WriterHandoff:
        record = self.effect_ledger.verify(effect_id, receipt)
        return WriterHandoff(record.kind, effect_id, record)

    def observer_status(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project effect state without reconciliation, expiry, or writer calls."""

        effective_now = _observer_validate_context(target_date, now)
        del effective_now
        return _observer_writer_facts(
            self.effect_ledger,
            target_date=target_date,
            now=now,
        )

    def submit(
        self,
        operation: str,
        writer: Any,
        *,
        source_event_id: str,
        idempotency_key: str,
        epoch_id: str,
        content: Any,
        expires_at: datetime | None = None,
        ttl: timedelta = timedelta(minutes=5),
        effect_id: str | None = None,
    ) -> WriterHandoff:
        intent = self.create_intent(
            operation,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            content=content,
            expires_at=expires_at,
            ttl=ttl,
            effect_id=effect_id,
        )
        return self.handoff(
            intent.effect_id, writer, content=content, operation=operation
        )


def _observer_writer_facts(
    effect_ledger: Any,
    *,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Fallback effect projection for a path-backed injected effect port."""

    owner = getattr(effect_ledger, "ledger", None)
    path = getattr(owner, "path", None)
    if path is None:
        return (
            ObservationFact(
                key="memory.writer.adapter",
                code="writer_adapter_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("writer",),
            ),
        )
    try:
        raw_rows = _observer_jsonl_rows(path)
        records: list[tuple[str, EffectRecord]] = []
        by_effect: dict[str, list[EffectRecord]] = {}
        idempotency: dict[str, str] = {}
        for row_number, row in enumerate(raw_rows, start=1):
            operation = row.get("operation")
            if operation not in {
                "begin_intent",
                "mark_pending",
                "mark_queue_accepted",
                "verify",
                "fail",
                "expire",
                "requeue",
            }:
                raise StateError(f"writer row {row_number} has an invalid operation")
            record = EffectRecord.from_dict(row)
            records.append((operation, record))
            previous = by_effect.get(record.effect_id, [])
            if not previous:
                if operation != "begin_intent" or record.state != "intent":
                    raise StateError("writer ledger starts after intent")
                if record.attempt != 1:
                    raise StateError("writer ledger has an invalid initial attempt")
            else:
                prior = previous[-1]
                if (
                    prior.effect_id != record.effect_id
                    or prior.kind != record.kind
                    or prior.source_event_id != record.source_event_id
                    or prior.idempotency_key != record.idempotency_key
                    or prior.epoch_id != record.epoch_id
                    or prior.created_at != record.created_at
                    or prior.content_sha256 != record.content_sha256
                    or prior.content_length != record.content_length
                ):
                    raise StateError("writer ledger changes immutable identity")
                if not _valid_transition(prior, record, operation):
                    raise StateError("writer ledger contains an out-of-order event")
            previous_effect = idempotency.get(record.idempotency_key)
            if previous_effect is not None and previous_effect != record.effect_id:
                raise StateError("writer ledger reuses an idempotency key")
            idempotency[record.idempotency_key] = record.effect_id
            by_effect.setdefault(record.effect_id, []).append(record)
    except Exception as exc:  # noqa: BLE001 - content-free fail-closed status
        del exc
        return (_observer_integrity_fact("writer", target_date=target_date),)
    if not records:
        return ()

    facts: list[ObservationFact] = []
    for history in by_effect.values():
        current = history[-1]
        projected_state = current.state
        if (
            current.state in {"pending", "executed_unverified"}
            and current.expires_at < now
        ):
            # Projection only: never call EffectLedger.expire().
            projected_state = "expired"
        refs = [
            f"effect:{current.effect_id}",
            f"source:{current.source_event_id}",
            f"sha256:{current.content_sha256}",
        ]
        counts = {
            "effects": 1,
            "attempt": current.attempt,
            "content_length": current.content_length,
            f"state_{projected_state}": 1,
        }
        if current.receipt is not None:
            refs.append(f"receipt:{current.receipt.receipt_id}")
        if current.state == "verified":
            prior_states = {record.state for record in history[:-1]}
            recovery = None
            fact_state = "neutral"
            if prior_states & _WRITER_CURRENT_STATES:
                receipt = current.receipt
                if receipt is not None:
                    recovery = RecoveryEvidence(
                        f"receipt:{receipt.receipt_id}",
                        "effect_verified",
                        receipt.observed_at,
                    )
                    fact_state = "recovered_history"
            facts.append(
                ObservationFact(
                    key=f"memory.writer.effect:{current.effect_id}",
                    code="effect_verified",
                    state=fact_state,
                    target_date=target_date,
                    event_time=(current.observed_at or current.created_at),
                    refs=tuple(refs),
                    counts=counts,
                    recovery=recovery,
                )
            )
            continue
        if projected_state in _WRITER_CURRENT_STATES:
            fact_state = "current"
        else:
            fact_state = "neutral"
        facts.append(
            ObservationFact(
                key=f"memory.writer.effect:{current.effect_id}",
                code=f"effect_{projected_state}",
                state=fact_state,
                target_date=target_date,
                event_time=(
                    current.expires_at
                    if projected_state == "expired"
                    else current.created_at
                ),
                refs=tuple(refs),
                counts=counts,
            )
        )
    return _observer_merge_facts(facts)


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


__all__ = [
    "EXPOSURE_EVENTS",
    "EXPOSURE_STATES",
    "ORCHESTRATION_SCHEMA",
    "WRITER_OPERATIONS",
    "ExpiredEvidenceError",
    "ExposureConflictError",
    "ExposureContext",
    "ExposureLedger",
    "ExposurePlan",
    "ExposurePolicy",
    "ExposureRecord",
    "ExposedSource",
    "MemoryMaintenanceFacade",
    "MemoryOrchestrator",
    "MaintenanceApprovalAdapter",
    "MissingEvidenceError",
    "OrchestrationError",
    "PolicyDeniedError",
    "ReplyUseEvidence",
    "SourceCandidate",
    "SourceMaterial",
    "SourceOpener",
    "SourceRegistry",
    "SourceRetriever",
    "WriterCoordinator",
    "WriterHandoff",
    "WriterRequest",
    "content_descriptor",
]
