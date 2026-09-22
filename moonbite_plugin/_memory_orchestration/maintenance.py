"""Maintenance proposals, approvals, and read-only projections."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

from .contracts import (
    _hash,
    _HASH_RE,
    _length,
    _text,
    _time,
    content_descriptor,
)
from .observation import (
    observer_integrity_fact as _observer_integrity_fact,
    observer_jsonl_rows as _observer_jsonl_rows,
    observer_merge_facts as _observer_merge_facts,
    observer_refs as _observer_refs,
    observer_validate_context as _observer_validate_context,
)
from ..observer import ObservationFact
from ..runtime_core import JsonlLedger, StateError, file_lock, isoformat, utc_now


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
