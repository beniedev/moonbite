"""Heartbeat effect intent creation and adapter execution.

The public engine remains the coordinator and supplies its existing ports so
tests and hosts can keep overriding the narrow facade methods.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable, Collection, Mapping
from datetime import datetime, timedelta
from typing import Any

from ..effects import EffectReceipt, EffectRecord
from ..runtime_core import StateError


def invoke(
    method: Callable[..., Any],
    candidate: Any,
    decision: Any,
    intent: Any,
) -> Any:
    """Call an adapter while preserving the legacy optional-intent surface."""

    try:
        signature = inspect.signature(method)
        params = list(signature.parameters.values())
        positional = [
            parameter
            for parameter in params
            if parameter.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        ]
        if len(positional) >= 3 or any(
            parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in params
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


def prepare_effect_intent(
    kind: str,
    candidate: Any,
    decision: Any,
    now: datetime,
    *,
    planned: Mapping[str, Any] | None,
    effect_ledger: Any,
    effect_ttl: timedelta,
    effect_body: Callable[[str, Any, Any], bytes],
    candidate_epoch: Callable[[Any], str | None],
    validate_plan_record: Callable[[EffectRecord, Mapping[str, Any]], None],
    cadence: Any,
    accepts_keyword: Callable[[Callable[..., Any], str], bool],
    delegated_suffix: str,
) -> EffectRecord:
    """Persist one effect identity before any adapter invocation."""

    if effect_ledger is None:
        raise StateError("effect ledger is unavailable")
    body = effect_body(kind, candidate, decision)
    source = (
        candidate.context.get("source_event_id")
        or candidate.context.get("event_id")
        or candidate.candidate_id
    )
    public_epoch = candidate_epoch(candidate)
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
        idempotency_key += delegated_suffix
    content_sha256 = hashlib.sha256(body).hexdigest()
    content_length = len(body)
    intent_kwargs: dict[str, Any] = {
        "kind": f"heartbeat_{kind}",
        "source_event_id": source,
        "idempotency_key": idempotency_key,
        "epoch_id": epoch,
        "content_sha256": content_sha256,
        "content_length": content_length,
        "expires_at": now + effect_ttl,
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
    record = effect_ledger.begin_intent(**intent_kwargs)
    if planned is not None:
        validate_plan_record(record, planned)
    remember = getattr(cadence, "remember_effect_ref", None)
    if callable(remember):
        ref_kwargs = (
            {"epoch_id": public_epoch} if accepts_keyword(remember, "epoch_id") else {}
        )
        remember(source, f"heartbeat_{kind}", record.effect_id, **ref_kwargs)
    return record


def run_effect(
    kind: str,
    candidate: Any,
    decision: Any,
    now: datetime,
    *,
    planned: Mapping[str, Any] | None,
    prepared: EffectRecord | None,
    effect_ledger: Any,
    prepare_intent: Callable[..., EffectRecord],
    validate_plan_record: Callable[[EffectRecord, Mapping[str, Any]], None],
    existing_effect: Callable[[EffectRecord, datetime], Any | None],
    sink: Any,
    invoke_adapter: Callable[[Callable[..., Any], Any, Any, Any], Any],
    validate_receipt_time: Callable[[EffectRecord, EffectReceipt], None],
    effect_result: Callable[..., Any],
    fail_effect: Callable[..., Any],
    result_type: type,
    accepted_statuses: Collection[str],
    replay_error_code: Any,
    effect_error_code: Any,
    adapter_unavailable_code: Any,
    adapter_error_code: Any,
    adapter_malformed_code: Any,
    adapter_rejected_code: Any,
    pending_code: Any,
) -> Any:
    """Run one planned delivery or wake through the existing ledger ports."""

    if effect_ledger is None:
        return result_type(
            False,
            "effect_ledger_unavailable",
            reason_code=replay_error_code,
        )
    try:
        record = prepared or prepare_intent(
            kind, candidate, decision, now, planned=planned
        )
    except Exception:
        return result_type(False, "effect_intent_error", reason_code=replay_error_code)
    if planned is not None:
        try:
            validate_plan_record(record, planned)
        except Exception:
            return result_type(
                False,
                "effect_intent_error",
                effect_id=record.effect_id,
                reason_code=replay_error_code,
            )
    existing = existing_effect(record, now)
    if existing is not None:
        return existing
    try:
        record = effect_ledger.mark_pending(record.effect_id)
    except Exception:
        return result_type(
            False,
            "effect_pending_error",
            effect_id=record.effect_id,
            reason_code=effect_error_code,
        )
    method = getattr(sink, "deliver" if kind == "delivery" else "wake", None)
    if not callable(method):
        return fail_effect(
            record,
            "adapter_unavailable",
            "adapter_unavailable",
            adapter_unavailable_code,
            True,
        )
    try:
        raw = invoke_adapter(method, candidate, decision, record.to_intent())
    except Exception as exc:
        return fail_effect(
            record,
            f"{kind}_error:{type(exc).__name__}",
            f"adapter_error:{type(exc).__name__}",
            adapter_error_code,
            True,
        )
    if isinstance(raw, EffectReceipt):
        adapter = result_type(True, "verified", raw, True)
    elif isinstance(raw, result_type):
        adapter = raw
    else:
        return fail_effect(
            record,
            "adapter_malformed_return",
            "adapter_malformed_return",
            adapter_malformed_code,
            False,
        )
    if (
        type(adapter.ok) is not bool
        or type(adapter.status) is not str
        or not adapter.status.strip()
    ):
        return fail_effect(
            record,
            "adapter_malformed_result",
            "adapter_malformed_result",
            adapter_malformed_code,
            False,
        )
    if adapter.receipt is not None and not isinstance(adapter.receipt, EffectReceipt):
        return fail_effect(
            record,
            "adapter_malformed_receipt",
            "adapter_malformed_receipt",
            adapter_malformed_code,
            False,
        )
    status = adapter.status.strip().lower()
    if adapter.receipt is not None:
        if kind == "delivery" and decision.delivery_mode == "delegated":
            return fail_effect(
                record,
                "delegated_receipt_not_allowed",
                "delegated_receipt_not_allowed",
                adapter_malformed_code,
                False,
            )
        if not adapter.ok:
            return fail_effect(
                record,
                "adapter_rejected",
                "adapter_rejected",
                adapter_rejected_code,
                False,
            )
        try:
            validate_receipt_time(record, adapter.receipt)
            verified = effect_ledger.verify(record.effect_id, adapter.receipt)
        except Exception:
            return fail_effect(
                record,
                "receipt_mismatch",
                "receipt_mismatch",
                effect_error_code,
                False,
            )
        return effect_result(verified, "verified")
    if not adapter.ok:
        unavailable = any(
            word in status for word in ("unavailable", "not_configured", "disabled")
        )
        return fail_effect(
            record,
            "adapter_unavailable" if unavailable else "adapter_rejected",
            "adapter_unavailable" if unavailable else "adapter_rejected",
            adapter_unavailable_code if unavailable else adapter_rejected_code,
            unavailable,
        )
    if status == "verified":
        return fail_effect(
            record,
            "missing_receipt",
            "missing_receipt",
            effect_error_code,
            False,
        )
    if status not in accepted_statuses:
        return fail_effect(
            record,
            "adapter_malformed_status",
            "adapter_malformed_status",
            adapter_malformed_code,
            False,
        )
    try:
        accepted = effect_ledger.mark_queue_accepted(record.effect_id)
    except Exception:
        try:
            current = effect_ledger.get(record.effect_id)
        except Exception:
            current = None
        if current is not None and current.state == "verified":
            return effect_result(current, "verified")
        if current is not None and current.state == "failed":
            return effect_result(
                current,
                current.reason or "failed",
                effect_error_code,
            )
        return fail_effect(
            record,
            "effect_queue_accept_error",
            "effect_queue_accept_error",
            effect_error_code,
            True,
        )
    return effect_result(accepted, "queued_unverified", pending_code)
