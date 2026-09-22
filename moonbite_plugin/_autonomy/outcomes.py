"""Autonomy terminal projection and explicit effect settlement."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any, Callable


def finish(
    result: Any,
    gate: Any,
    *,
    record_terminal: bool = True,
    terminal_identity: Callable[[Any], tuple[str, str | None]],
    canonical_terminal: Callable[[Any], str | None],
    reason_code: Callable[[Any], str],
    bus: Any,
) -> Any:
    occurrence_id, epoch_id = terminal_identity(result)
    result = replace(
        result,
        source_event_id=result.source_event_id or occurrence_id,
        canonical_event_id=result.canonical_event_id or occurrence_id,
        epoch_id=epoch_id,
    )
    terminal = canonical_terminal(result) if record_terminal else None
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
        "reason": reason_code(result.reason),
        "run_id": result.run_id,
        "effect_id": result.effect_id,
        "source_event_id": result.source_event_id,
        "epoch_id": epoch_id,
        "idempotency_key": result.idempotency_key,
        "control_id": gate.control_id,
        "evidence": evidence,
        "gate": {
            "allowed": gate.allowed,
            "mode": gate.mode,
            "reason": reason_code(gate.reason),
            "control_id": gate.control_id,
        },
    }
    if occurrence_id is not None:
        details["occurrence_id"] = occurrence_id
    try:
        if terminal is not None and occurrence_id is not None:
            bus.record_audit_terminal(
                "autonomy",
                occurrence_id=occurrence_id,
                epoch_id=epoch_id,
                terminal=terminal,
                status=result.status,
                source="autonomy",
                details=details,
            )
        else:
            bus.record_audit(
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


def bound_control_id(
    effect_id: str,
    *,
    audit_history: Callable[[], list[dict[str, Any]]],
) -> str | None:
    for row in audit_history():
        if row.get("effect_id") != effect_id:
            continue
        control_id = row.get("control_id")
        if isinstance(control_id, str) and control_id:
            return control_id
    return None


def consume_verified(
    gate: Any,
    *,
    effect_id: str | None = None,
    allow_current: bool = False,
    original_control_id: str | None = None,
    bound_control_id: Callable[[str], str | None],
    consume_control: Callable[[str], Any],
) -> bool:
    if gate.mode != "play_next" or not gate.control_id:
        return False
    if effect_id is not None and not allow_current:
        durable_bound = bound_control_id(effect_id)
        if (
            durable_bound is not None
            and original_control_id is not None
            and original_control_id != durable_bound
        ):
            return False
        bound = durable_bound or original_control_id
        if bound is None or bound != gate.control_id:
            return False
    consume_control(gate.control_id)
    return True


def reconcile(
    effect_id: str,
    receipt: Any,
    *,
    control_id: str | None = None,
    receipt_type: type,
    effect_record_type: type,
    effect_kind: str,
    effect_ledger: Any,
    resolve_control: Callable[[str], Any],
    evaluate_gate_fn: Callable[[Any], Any],
    clock: Callable[[], datetime],
    record_value: Callable[..., Any],
    record_provider: Callable[[Any], str | None],
    record_state: Callable[[Any], str],
    record_evidence: Callable[[Any], dict[str, Any]],
    consume_verified: Callable[..., bool],
    finish: Callable[..., Any],
    result_type: Callable[..., Any],
) -> Any:
    """Write explicit host evidence for an existing autonomy effect.

    Reconciliation never invokes a provider.  A mismatched receipt is
    fail-closed and cannot be used to consume a play-next control.
    """

    if not isinstance(receipt, receipt_type):
        raise TypeError("receipt must be an EffectReceipt")
    record = effect_ledger.get(effect_id)
    if record is None:
        raise ValueError("autonomy effect does not exist")
    if record_value(record, "kind") != effect_kind:
        raise ValueError("effect is not an autonomy completion")
    provider = record_provider(record) or "unknown"
    gate = evaluate_gate_fn(resolve_control("autonomy"))
    state = record_state(record)
    expires_at = record_value(record, "expires_at")
    if state == "expired" or (
        state not in {"verified", "failed"}
        and isinstance(expires_at, datetime)
        and clock() >= expires_at
    ):
        return finish(
            result_type(
                "awaiting_reconciliation",
                provider,
                "expired_requeue_required",
                effect_id=effect_id,
                evidence=record_evidence(record),
                source_event_id=record_value(record, "source_event_id"),
                idempotency_key=record_value(record, "idempotency_key"),
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=record_value(record, "source_event_id"),
            ),
            gate,
        )
    try:
        verified = effect_ledger.verify(effect_id, receipt)
    except Exception:
        try:
            failed = effect_ledger.fail(effect_id, "receipt_mismatch", False)
        except Exception:
            failed = record
        return finish(
            result_type(
                "failed",
                provider,
                "receipt_mismatch",
                effect_id=effect_id,
                evidence=record_evidence(failed),
                source_event_id=record_value(record, "source_event_id"),
                idempotency_key=record_value(record, "idempotency_key"),
                effect_record=failed
                if isinstance(failed, effect_record_type)
                else None,
                canonical_event_id=record_value(record, "source_event_id"),
            ),
            gate,
        )
    consume_verified(gate, effect_id=effect_id, original_control_id=control_id)
    return finish(
        result_type(
            "completed",
            provider,
            "verified_reconciliation",
            effect_id=effect_id,
            evidence=record_evidence(verified),
            source_event_id=record_value(verified, "source_event_id"),
            idempotency_key=record_value(verified, "idempotency_key"),
            effect_record=verified
            if isinstance(verified, effect_record_type)
            else None,
            canonical_event_id=record_value(verified, "source_event_id"),
        ),
        gate,
    )


def fail(
    effect_id: str,
    reason: str,
    *,
    bounded: Callable[..., str],
    effect_record_type: type,
    effect_kind: str,
    effect_ledger: Any,
    resolve_control: Callable[[str], Any],
    evaluate_gate_fn: Callable[[Any], Any],
    record_value: Callable[..., Any],
    record_provider: Callable[[Any], str | None],
    record_state: Callable[[Any], str],
    record_evidence: Callable[[Any], dict[str, Any]],
    existing_result: Callable[..., Any],
    finish: Callable[..., Any],
    result_type: Callable[..., Any],
) -> Any:
    """Settle an asynchronous provider with an explicit host failure."""

    failure_reason = bounded(reason, "failure reason", max_bytes=128)
    record = effect_ledger.get(effect_id)
    if record is None:
        raise ValueError("autonomy effect does not exist")
    if record_value(record, "kind") != effect_kind:
        raise ValueError("effect is not an autonomy completion")
    provider = record_provider(record) or "unknown"
    gate = evaluate_gate_fn(resolve_control("autonomy"))
    if record_state(record) in {"verified", "failed"}:
        existing = existing_result(
            record,
            provider=provider,
            gate=gate,
            run_id=record_value(record, "effect_id"),
        )
        if existing is not None:
            return existing
    failed = effect_ledger.fail(effect_id, failure_reason, False)
    return finish(
        result_type(
            "failed",
            provider,
            failure_reason,
            effect_id=effect_id,
            evidence=record_evidence(failed),
            source_event_id=record_value(failed, "source_event_id"),
            idempotency_key=record_value(failed, "idempotency_key"),
            effect_record=failed if isinstance(failed, effect_record_type) else None,
            canonical_event_id=record_value(failed, "source_event_id"),
        ),
        gate,
    )


def settle_expired_unverified(
    *,
    now: datetime,
    gate: Any,
    effect_record_type: type,
    effect_kind: str,
    effect_ledger: Any,
    record_value: Callable[..., Any],
    record_provider: Callable[[Any], str | None],
    record_state: Callable[[Any], str],
    record_evidence: Callable[[Any], dict[str, Any]],
    finish: Callable[..., Any],
    result_type: Callable[..., Any],
) -> None:
    """Fail old unverified autonomy effects without replaying providers."""

    for record in effect_ledger.records():
        if record_value(record, "kind") != effect_kind or record_state(record) not in {
            "pending",
            "executed_unverified",
        }:
            continue
        expires_at = record_value(record, "expires_at")
        if not isinstance(expires_at, datetime) or not expires_at < now:
            continue
        effect_id = record_value(record, "effect_id")
        provider = record_provider(record) or "unknown"
        try:
            failed = effect_ledger.fail(
                effect_id,
                "effect_expired_unverified",
                False,
            )
        except Exception:
            current = effect_ledger.get(effect_id)
            if record_state(current) in {"verified", "failed"}:
                continue
            raise
        finish(
            result_type(
                "failed",
                provider,
                "effect_expired_unverified",
                effect_id=effect_id,
                evidence=record_evidence(failed),
                source_event_id=record_value(failed, "source_event_id"),
                idempotency_key=record_value(failed, "idempotency_key"),
                effect_record=(
                    failed if isinstance(failed, effect_record_type) else None
                ),
                canonical_event_id=record_value(failed, "source_event_id"),
            ),
            gate,
        )


__all__ = ()
