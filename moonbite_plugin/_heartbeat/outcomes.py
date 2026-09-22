"""Heartbeat effect projections, aggregate terminals, and reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from typing import Any

from ..effects import EffectReceipt, EffectRecord
from ..runtime_core import StateError


def effect_result(
    record: EffectRecord,
    status: str,
    code: Any = None,
    *,
    audit_terminal: bool,
    cadence: Any,
    clock: Callable[[], datetime],
    record_settled_occurrence_terminal: Callable[[EffectRecord], None],
    result_type: type,
    effect_error_code: Any,
    effect_expired_code: Any,
) -> Any:
    projection_errors: list[str] = []
    if record.state == "verified" and record.receipt is not None:
        if record.kind == "heartbeat_delivery":
            try:
                cadence.record_verified_visible_contact(record, record.receipt)
            except Exception as exc:
                projection_errors.append(f"visible_contact_write:{type(exc).__name__}")
        result = result_type(
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
        result = result_type(
            False,
            status or "failed",
            effect_id=record.effect_id,
            terminal=terminal,
            reason_code=code or effect_error_code,
        )
    elif record.state == "requeued":
        result = result_type(
            True,
            status or "requeued",
            effect_id=record.effect_id,
            terminal="requeued",
            reason_code=code or effect_expired_code,
        )
    else:
        result = result_type(
            True,
            status or "queued_unverified",
            effect_id=record.effect_id,
            terminal=record.state,
            reason_code=code,
        )
    try:
        cadence.record_effect_terminal(
            record.effect_id, record.state, observed_at=clock()
        )
    except Exception as exc:
        projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
    if audit_terminal and record.state in {"verified", "failed"}:
        try:
            record_settled_occurrence_terminal(record)
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


def record_settled_occurrence_terminal(
    record: EffectRecord,
    *,
    effect_ledger: Any,
    find_plan: Callable[[str, str | None], Mapping[str, Any] | None],
    public_epoch: Callable[[EffectRecord], str | None],
    validate_plan_record: Callable[[EffectRecord, Mapping[str, Any]], None],
    find_existing_terminal: Callable[..., Any],
    bus: Any,
    accepts_keyword: Callable[[Callable[..., Any], str], bool],
    allowed_code: Any,
    denied_code: Any,
    effect_error_code: Any,
) -> None:
    """Project a terminal only after every sibling effect is settled."""

    if effect_ledger is None:
        raise StateError("effect ledger is unavailable")
    plan = find_plan(record.source_event_id, public_epoch(record))
    if plan is not None:
        siblings_list: list[EffectRecord] = []
        for expected in plan["effects"]:
            candidate = effect_ledger.get(expected["effect_id"])
            if candidate is None:
                return
            validate_plan_record(candidate, expected)
            siblings_list.append(candidate)
        siblings = tuple(siblings_list)
        if record.effect_id not in {candidate.effect_id for candidate in siblings}:
            raise StateError("heartbeat occurrence effect is outside its plan")
        occurrence_id = plan["candidate_id"]
    else:
        existing = find_existing_terminal(
            record.source_event_id,
            epoch_id=public_epoch(record),
        )
        if existing is None:
            return
        return
    if any(candidate.state not in {"verified", "failed"} for candidate in siblings):
        return
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
            allowed_code.value
            if terminal == "verified"
            else denied_code.value
            if terminal == "intentional_silence"
            else effect_error_code.value
        ),
    }
    if len(effect_ids) == 1:
        details["effect_id"] = effect_ids[0]
    record_terminal = bus.record_audit_terminal
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
    selected_epoch = public_epoch(record)
    if accepts_keyword(record_terminal, "epoch_id"):
        terminal_kwargs["epoch_id"] = selected_epoch
    elif selected_epoch is not None:
        raise StateError("heartbeat audit epoch is unsupported")
    record_terminal("heartbeat", **terminal_kwargs)


def fail_effect(
    record: EffectRecord,
    status: str,
    reason: str,
    code: Any,
    retryable: bool,
    *,
    effect_ledger: Any,
    cadence: Any,
    clock: Callable[[], datetime],
    result_type: type,
    replay_error_code: Any,
) -> Any:
    projection_errors: list[str] = []
    effective_code = code
    failed = record
    try:
        failed = effect_ledger.fail(record.effect_id, reason, retryable)
    except Exception as exc:
        projection_errors.append(f"effect_failure_write:{type(exc).__name__}")
        effective_code = replay_error_code
    try:
        cadence.record_effect_terminal(record.effect_id, "failed", observed_at=clock())
    except Exception as exc:
        projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
    return result_type(
        False,
        status,
        effect_id=record.effect_id,
        terminal=getattr(failed, "state", "failed"),
        reason_code=effective_code,
        degraded=bool(projection_errors),
        projection_errors=tuple(dict.fromkeys(projection_errors)),
    )


def delegated_completion_result(
    record: EffectRecord,
    *,
    status: str,
    terminal: str,
    reason_code: Any,
    cadence: Any,
    clock: Callable[[], datetime],
    record_settled_occurrence_terminal: Callable[[EffectRecord], None],
    result_type: type,
) -> Any:
    """Project a delegated host terminal without claiming visible delivery."""

    projection_errors: list[str] = []
    try:
        cadence.record_effect_terminal(record.effect_id, terminal, observed_at=clock())
    except Exception as exc:
        projection_errors.append(f"effect_terminal_write:{type(exc).__name__}")
    try:
        record_settled_occurrence_terminal(record)
    except StateError:
        raise
    except Exception as exc:
        projection_errors.append(f"audit_write:{type(exc).__name__}")
    return result_type(
        False,
        status,
        effect_id=record.effect_id,
        terminal=terminal,
        reason_code=reason_code,
        degraded=bool(projection_errors),
        projection_errors=tuple(projection_errors),
    )


def validate_receipt_time(record: EffectRecord, receipt: EffectReceipt) -> None:
    if not record.created_at <= receipt.observed_at < record.expires_at:
        raise ValueError("heartbeat receipt is outside the effect lifetime")


def reconcile_delivery(
    effect_id: str,
    status: str | None,
    receipt: EffectReceipt | None,
    *,
    terminal: str | None,
    effect_ledger: Any,
    is_delegated_delivery: Callable[[EffectRecord], bool],
    validate_receipt: Callable[[EffectRecord, EffectReceipt], None],
    project_effect_result: Callable[..., Any],
    project_completion: Callable[..., Any],
    pending_code: Any,
    effect_error_code: Any,
    delegated_failure_reason: str,
) -> Any:
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
    if effect_ledger is None:
        raise RuntimeError("effect ledger is unavailable")
    record = effect_ledger.get(effect_id)
    if record is None:
        raise ValueError("heartbeat delivery effect is unknown")
    if record.kind != "heartbeat_delivery":
        raise ValueError("effect is not a heartbeat delivery")
    if not is_delegated_delivery(record):
        raise ValueError("effect is not a delegated heartbeat delivery")
    if selected_status == "verified":
        if not isinstance(receipt, EffectReceipt):
            raise TypeError("verified delegated delivery requires EffectReceipt")
        validate_receipt(record, receipt)
        if record.state == "verified":
            if record.receipt != receipt:
                raise ValueError("conflicting delegated delivery receipt")
            return project_effect_result(record, "verified", audit_terminal=True)
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("delegated delivery is not awaiting settlement")
        try:
            verified = effect_ledger.verify(effect_id, receipt)
        except Exception as exc:
            raise ValueError("delegated delivery receipt mismatch") from exc
        return project_effect_result(verified, "verified", audit_terminal=True)
    if receipt is not None:
        raise ValueError("non-verified delegated delivery cannot carry receipt")
    if selected_status == "unknown":
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("unknown delegated delivery is not pending")
        result = project_effect_result(record, "unknown", pending_code)
        return replace(result, terminal=record.state)
    if selected_status == "intentional_silence":
        if record.state == "failed":
            if record.reason != "intentional_silence" or record.retryable is not False:
                raise ValueError("conflicting delegated delivery completion")
            return project_completion(
                record,
                status="intentional_silence",
                terminal="intentional_silence",
                reason_code=effect_error_code,
            )
        if record.state not in {"pending", "executed_unverified"}:
            raise ValueError("delegated delivery is not awaiting settlement")
        try:
            failed = effect_ledger.fail(
                effect_id, "intentional_silence", retryable=False
            )
        except Exception as exc:
            raise ValueError("delegated silence completion failed") from exc
        return project_completion(
            failed,
            status="intentional_silence",
            terminal="intentional_silence",
            reason_code=effect_error_code,
        )
    if record.state == "failed":
        if record.reason != delegated_failure_reason or record.retryable is not False:
            raise ValueError("conflicting delegated delivery completion")
        return project_completion(
            record,
            status="failed",
            terminal="failed",
            reason_code=effect_error_code,
        )
    if record.state not in {"pending", "executed_unverified"}:
        raise ValueError("delegated delivery is not awaiting settlement")
    try:
        failed = effect_ledger.fail(
            effect_id, delegated_failure_reason, retryable=False
        )
    except Exception as exc:
        raise ValueError("delegated delivery failure completion failed") from exc
    return project_completion(
        failed,
        status="failed",
        terminal="failed",
        reason_code=effect_error_code,
    )


def reconcile_wake(
    effect_id: str,
    receipt: EffectReceipt,
    *,
    effect_ledger: Any,
    validate_receipt: Callable[[EffectRecord, EffectReceipt], None],
    project_effect_result: Callable[..., Any],
) -> Any:
    if effect_ledger is None:
        raise RuntimeError("effect ledger is unavailable")
    record = effect_ledger.get(effect_id)
    if record is None:
        raise ValueError("heartbeat wake effect is unknown")
    if record.kind != "heartbeat_wake":
        raise ValueError("effect is not a heartbeat wake")
    if not isinstance(receipt, EffectReceipt):
        raise TypeError("verified heartbeat wake requires EffectReceipt")
    validate_receipt(record, receipt)
    if record.state == "verified":
        if record.receipt != receipt:
            raise ValueError("conflicting heartbeat wake receipt")
        return project_effect_result(record, "verified", audit_terminal=True)
    if record.state not in {"pending", "executed_unverified"}:
        raise ValueError("heartbeat wake is not awaiting settlement")
    try:
        verified = effect_ledger.verify(effect_id, receipt)
    except Exception as exc:
        raise ValueError("heartbeat wake receipt mismatch") from exc
    return project_effect_result(verified, "verified", audit_terminal=True)
