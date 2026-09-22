"""Autonomy identity, durable occurrence, and replay helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Callable


def public_epoch_from_record(
    record: Any,
    *,
    record_value: Callable[..., Any],
) -> Any:
    """Hide the date-derived ledger epoch from the public identity."""

    epoch_id = record_value(record, "epoch_id")
    if epoch_id is None:
        return None
    created_at = record_value(record, "created_at")
    if (
        type(epoch_id) is str
        and isinstance(created_at, datetime)
        and epoch_id == f"autonomy:{created_at.date().isoformat()}"
    ):
        return None
    return epoch_id


def terminal_identity(
    result: Any,
    *,
    record_value: Callable[..., Any],
    public_epoch_from_record: Callable[[Any], Any],
    new_id_fn: Callable[[str], str],
) -> tuple[str, str | None]:
    """Resolve the public terminal identity without losing effect evidence."""

    source_values: list[str] = []
    for value in (
        result.source_event_id,
        result.canonical_event_id,
        record_value(result.effect_record, "source_event_id"),
    ):
        if value is None:
            continue
        if type(value) is not str or not value.strip():
            raise ValueError("invalid_source_event_id")
        source_values.append(value)
    if len(set(source_values)) > 1:
        raise ValueError("conflicting_source_event_id")
    source_event_id = source_values[0] if source_values else None
    if source_event_id is None and result.run_id is not None:
        if type(result.run_id) is not str or not result.run_id.strip():
            raise ValueError("invalid_source_event_id")
        source_event_id = result.run_id
    if source_event_id is None:
        source_event_id = new_id_fn("autonomy_terminal")

    evidence = result.evidence
    evidence_epoch = evidence.get("epoch_id") if isinstance(evidence, Mapping) else None
    record_epoch_raw = record_value(result.effect_record, "epoch_id")
    record_epoch = public_epoch_from_record(result.effect_record)
    if record_epoch is None and evidence_epoch == record_epoch_raw:
        # ``_record_evidence`` exposes the immutable ledger epoch.  A
        # legacy effect's date-derived value is not a public epoch.
        evidence_epoch = None
    epoch_id: str | None = None
    for value in (result.epoch_id, evidence_epoch, record_epoch):
        if value is None:
            continue
        if type(value) is not str or not value.strip():
            raise ValueError("invalid_epoch_id")
        if epoch_id is not None and epoch_id != value:
            raise ValueError("conflicting_epoch_id")
        epoch_id = value
    return source_event_id, epoch_id


def canonical_terminal(
    result: Any,
    *,
    reason_code: Callable[[Any], str],
) -> str | None:
    if result.status == "skipped":
        if result.reason == "execution_in_progress":
            # A lock race is only telemetry.  It must not claim the
            # occurrence before the owner can settle its real outcome.
            return None
        return reason_code(result.reason)
    if result.status == "completed":
        return "verified"
    if result.status == "failed":
        if result.effect_record is not None and getattr(
            result.effect_record, "state", None
        ) not in {"failed", "verified"}:
            return None
        if result.reason == "effect_expired_unverified":
            return "expired"
        return "failed"
    return None


def existing_terminal_result(
    occurrence_id: str,
    *,
    epoch_id: str | None = None,
    ledger_epoch_id: str,
    bus: Any,
    effect_ledger: Any,
    record_value: Callable[..., Any],
    record_state: Callable[[Any], str],
    reason_code: Callable[[Any], str],
    result_type: Callable[..., Any],
    effect_kind: str,
) -> Any | None:
    finder = getattr(bus, "find_audit_terminal", None)
    if not callable(finder):
        return None
    event = finder("autonomy", occurrence_id, epoch_id=epoch_id)
    if event is None:
        return None
    payload = event.payload
    terminal = payload.get("terminal")
    if type(terminal) is not str or not terminal.strip():
        raise RuntimeError("autonomy terminal audit is invalid")
    # EffectLedger remains truth for this exact public occurrence and its
    # durable internal epoch.  The public epoch is optional for legacy
    # callers, so checking only ``epoch_id`` would let a no-epoch audit
    # mask a date-derived effect or mix two identities.
    matching = [
        record
        for record in effect_ledger.records()
        if record_value(record, "kind") == effect_kind
        and record_value(record, "source_event_id") == occurrence_id
        and record_value(record, "epoch_id") == ledger_epoch_id
    ]
    if len(matching) > 1:
        raise RuntimeError("autonomy occurrence conflict")
    if matching:
        record = matching[0]
        state = record_state(record)
        effect_terminal = (
            "verified"
            if state == "verified"
            else "failed"
            if state == "failed"
            else None
        )
        if (
            effect_terminal == "failed"
            and record_value(record, "reason") == "effect_expired_unverified"
        ):
            effect_terminal = "expired"
        if effect_terminal is not None:
            if effect_terminal != terminal:
                raise RuntimeError("audit effect conflict")
            # Let _existing_result serialize the durable effect, including
            # its receipt and failure reason, instead of trusting audit.
            return None
        # A terminal skip cannot mask an intent that is still awaiting a
        # durable provider outcome.  Replaying it would turn a temporary
        # authority decision into a false terminal state.
        raise RuntimeError("audit effect conflict")
    elif payload.get("effect_id") is not None:
        # An effect-bearing terminal without its exact ledger record is
        # untrusted.  Replaying it would hide an incomplete or conflicting
        # effect, especially on the legacy no-public-epoch path.
        raise RuntimeError("autonomy terminal effect unavailable")
    status = payload.get("status")
    if status == "completed":
        # Autonomy completion is receipt-backed.  A canonical success with
        # no matching effect cannot prove delivery and must not be replayed.
        # Let the caller's existing-terminal conflict path fail closed
        # without appending another terminal for the same identity.
        raise RuntimeError("autonomy terminal effect unavailable")
    if status not in {"skipped", "failed"}:
        raise RuntimeError("autonomy terminal status invalid")
    result_status = status
    provider = payload.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        provider = None
    effect_id = payload.get("effect_id")
    if not isinstance(effect_id, str) or not effect_id.strip():
        effect_id = None
    idempotency_key = payload.get("idempotency_key")
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        idempotency_key = None
    return result_type(
        result_status,
        provider,
        reason_code(payload.get("reason") or terminal),
        effect_id=effect_id,
        source_event_id=occurrence_id,
        idempotency_key=idempotency_key,
        canonical_event_id=occurrence_id,
        epoch_id=payload.get("epoch_id"),
    )


def reason_code(reason: Any) -> str:
    if not isinstance(reason, str) or not reason:
        return "unspecified"
    normalized = "".join(
        character
        if character.isascii() and (character.isalnum() or character in "_:-")
        else "_"
        for character in reason
    )
    return normalized[:128]


def record_value(record: Any, key: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        return record.get(key, default)
    return getattr(record, key, default)


def identity_overrides(
    facts: Mapping[str, Any],
    *,
    bounded: Callable[..., str],
) -> tuple[str | None, str | None, str | None]:
    def read(*keys: str) -> str | None:
        values: list[str] = []
        for key in keys:
            if key not in facts:
                continue
            try:
                values.append(bounded(facts[key], key))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid_{key}") from exc
        if len(set(values)) > 1:
            raise ValueError("conflicting_identity")
        return values[0] if values else None

    return (
        read("source_event_id", "occurrence_id", "event_id"),
        read("epoch_id", "epoch"),
        read("idempotency_key"),
    )


def validate_judge_decision(
    value: Any,
    *,
    decision_type: type,
    bounded: Callable[..., str],
) -> Any | None:
    if not isinstance(value, decision_type):
        return None
    if type(value.allowed) is not bool:
        return None
    try:
        bounded(value.reason, "judge reason", max_bytes=128)
        provider_weights = value.provider_weights
        if not isinstance(provider_weights, Mapping) or len(provider_weights) > 64:
            return None
        for name, weight in provider_weights.items():
            bounded(name, "provider name", max_bytes=128)
            if type(weight) is not int or not 0 <= weight <= 100:
                return None
    except (AttributeError, TypeError, ValueError):
        return None
    return value


def record_provider(
    record: Any,
    *,
    record_value: Callable[..., Any],
    provider_from_effect_id: Callable[[Any], str | None],
) -> str | None:
    direct = record_value(record, "provider")
    if isinstance(direct, str) and direct:
        return direct
    payload = record_value(record, "payload")
    if isinstance(payload, Mapping):
        value = payload.get("provider")
        if isinstance(value, str) and value:
            return value
    value = provider_from_effect_id(record_value(record, "effect_id"))
    if value is not None:
        return value
    key = record_value(record, "idempotency_key", "")
    if isinstance(key, str) and key.startswith("autonomy:"):
        parts = key.split(":", 2)
        if len(parts) == 3 and parts[1]:
            return parts[1]
    return None


def record_state(
    record: Any,
    *,
    record_value: Callable[..., Any],
) -> str:
    return str(record_value(record, "state", ""))


def record_evidence(
    record: Any,
    *,
    record_state: Callable[[Any], str],
    record_value: Callable[..., Any],
) -> dict[str, Any]:
    state = record_state(record)
    evidence: dict[str, Any] = {"state": state}
    for key in (
        "receipt_id",
        "event_id",
        "epoch_id",
        "content_sha256",
        "content_length",
    ):
        value = record_value(record, key)
        if value is None and key in {"receipt_id", "event_id"}:
            receipt = record_value(record, "receipt")
            if receipt is not None:
                value = record_value(receipt, key)
        if value is not None:
            evidence[key] = value
    return evidence


def find_by_idempotency(
    key: str,
    *,
    effect_ledger: Any,
) -> Any | None:
    try:
        return effect_ledger.find_by_idempotency(key)
    except (KeyError, ValueError):
        return None


def find_by_occurrence(
    source_event_id: str,
    epoch_id: str,
    *,
    effect_ledger: Any,
    record_value: Callable[..., Any],
    effect_kind: str,
) -> Any | None:
    matches = [
        record
        for record in effect_ledger.records()
        if record_value(record, "kind") == effect_kind
        and record_value(record, "source_event_id") == source_event_id
        and record_value(record, "epoch_id") == epoch_id
    ]
    if len(matches) > 1:
        raise ValueError("occurrence_conflict")
    return matches[0] if matches else None


def audit_identity_for_record(
    source_event_id: str,
    record: Any,
    *,
    record_value: Callable[..., Any],
    public_epoch_from_record: Callable[[Any], Any],
    bus: Any,
) -> str:
    """Classify scoped audit proof for one durable autonomy effect."""

    effect_id = record_value(record, "effect_id")
    record_key = record_value(record, "idempotency_key")
    record_epoch = record_value(record, "epoch_id")
    if (
        not isinstance(effect_id, str)
        or not effect_id.strip()
        or not isinstance(record_epoch, str)
        or not record_epoch.strip()
    ):
        return "unknown"
    record_public_epoch = public_epoch_from_record(record)
    implicit = False
    explicit = False
    matched = False
    invalid = False
    try:
        events = bus.read_audit()
    except Exception:
        return "unknown"
    for event in events:
        if (
            getattr(event, "kind", None) != "audit.autonomy"
            or getattr(event, "source", None) != "autonomy"
        ):
            continue
        payload = getattr(event, "payload", None)
        if not isinstance(payload, Mapping):
            continue
        if payload.get("effect_id") != effect_id:
            continue
        occurrence_id = payload.get("occurrence_id")
        payload_source = payload.get("source_event_id")
        if (
            occurrence_id is not None
            and payload_source is not None
            and occurrence_id != payload_source
        ):
            if occurrence_id == source_event_id or payload_source == source_event_id:
                invalid = True
            continue
        if occurrence_id != source_event_id and payload_source != source_event_id:
            continue
        matched = True
        row_key = payload.get("idempotency_key")
        if row_key is not None and row_key != record_key:
            invalid = True
            continue
        row_epoch = payload.get("epoch_id")
        if row_epoch is None:
            if record_public_epoch is None:
                implicit = True
            else:
                invalid = True
            continue
        if type(row_epoch) is not str or not row_epoch.strip():
            invalid = True
            continue
        if row_epoch != record_epoch:
            invalid = True
            continue
        explicit = True
    if invalid or not matched or (implicit and explicit):
        return "unknown"
    if explicit:
        return "explicit"
    if implicit:
        return "implicit"
    return "unknown"


def find_implicit_occurrence(
    source_event_id: str,
    *,
    requested_epoch_id: str,
    effect_ledger: Any,
    record_value: Callable[..., Any],
    audit_identity_for_record: Callable[[str, Any], str],
    effect_kind: str,
) -> Any | None:
    """Find one legacy occurrence from a scoped audit identity proof.

    A caller that omits the public epoch cannot infer identity from a
    date-shaped internal epoch or from the generated idempotency key.  An
    ``audit.autonomy`` row emitted by this module, with the same
    occurrence/source and effect, is the proof.  Rows from another audit
    kind/source, rows with contradictory identity fields, and effects with
    no proof are ignored or fail closed.  A non-null public epoch marks an
    exact effect as explicit, so it cannot be replayed by a legacy retry.
    """

    matches: list[Any] = []
    unproven: list[Any] = []
    for record in effect_ledger.records():
        if record_value(record, "kind") != effect_kind:
            continue
        if record_value(record, "source_event_id") != source_event_id:
            continue
        effect_id = record_value(record, "effect_id")
        if not isinstance(effect_id, str) or not effect_id.strip():
            continue
        identity = audit_identity_for_record(source_event_id, record)
        if identity == "unknown":
            unproven.append(record)
        elif identity == "implicit":
            matches.append(record)
        else:
            # An explicit epoch that equals the internal epoch requested
            # by this legacy retry is indistinguishable from it in the
            # ledger schema.  Keep the retry pending instead of allowing
            # a fresh begin_intent to collide or replay it.
            record_epoch = record_value(record, "epoch_id")
            if record_epoch == requested_epoch_id:
                unproven.append(record)
    if unproven:
        raise ValueError("implicit_identity_unavailable")
    if len(matches) > 1:
        raise ValueError("occurrence_conflict")
    return matches[0] if matches else None


def existing_result(
    record: Any,
    *,
    provider: str,
    gate: Any,
    run_id: str,
    public_epoch_id: str | None = None,
    record_state: Callable[[Any], str],
    record_value: Callable[..., Any],
    record_evidence: Callable[[Any], dict[str, Any]],
    public_epoch_from_record: Callable[[Any], Any],
    consume_verified: Callable[..., bool],
    finish: Callable[..., Any],
    result_type: Callable[..., Any],
    effect_record_type: type,
) -> Any | None:
    state = record_state(record)
    effect_id = record_value(record, "effect_id")
    source_event_id = record_value(record, "source_event_id")
    idempotency_key = record_value(record, "idempotency_key")
    evidence = record_evidence(record)
    result_epoch_id = (
        public_epoch_id
        if public_epoch_id is not None
        else public_epoch_from_record(record)
    )
    if state == "verified":
        try:
            consume_verified(gate, effect_id=effect_id)
        except Exception:
            return finish(
                result_type(
                    "failed",
                    provider,
                    "control_consume_error",
                    run_id=run_id,
                    effect_id=effect_id,
                    evidence=evidence,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                    effect_record=record
                    if isinstance(record, effect_record_type)
                    else None,
                    canonical_event_id=source_event_id,
                    epoch_id=result_epoch_id,
                ),
                gate,
            )
        return finish(
            result_type(
                "completed",
                provider,
                "already_verified",
                run_id=run_id,
                effect_id=effect_id,
                evidence=evidence,
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
                epoch_id=result_epoch_id,
            ),
            gate,
        )
    if state in {"pending", "executed_unverified"}:
        return finish(
            result_type(
                "awaiting_reconciliation",
                provider,
                "awaiting_reconciliation",
                run_id=run_id,
                effect_id=effect_id,
                evidence=evidence,
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
                epoch_id=result_epoch_id,
            ),
            gate,
        )
    if state == "expired":
        return finish(
            result_type(
                "awaiting_reconciliation",
                provider,
                "expired_requeue_required",
                run_id=run_id,
                effect_id=effect_id,
                evidence=evidence,
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
                epoch_id=result_epoch_id,
            ),
            gate,
        )
    if state == "failed":
        return finish(
            result_type(
                "failed",
                provider,
                record_value(record, "reason") or "already_failed",
                run_id=run_id,
                effect_id=effect_id,
                evidence=evidence,
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
                epoch_id=result_epoch_id,
            ),
            gate,
        )
    return None


def effect_identity(
    provider: str,
    source_event_id: str,
    epoch_id: str,
) -> tuple[str, str, int]:
    identity = json.dumps(
        {
            "provider": provider,
            "source_event_id": source_event_id,
            "epoch_id": epoch_id,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return (
        hashlib.sha256(identity).hexdigest(),
        f"autonomy:{provider}:{source_event_id}:{epoch_id}",
        len(identity),
    )


def receipt_from_output(
    output: Any,
    *,
    receipt_type: type,
) -> tuple[Any | None, str | None]:
    candidate = output if isinstance(output, receipt_type) else None
    if candidate is None and isinstance(output, Mapping):
        for key in ("receipt", "effect_receipt", "evidence"):
            value = output.get(key)
            if isinstance(value, receipt_type):
                candidate = value
                break
            if isinstance(value, Mapping):
                try:
                    candidate = receipt_type.from_dict(value)
                except Exception:
                    return None, "evidence_invalid"
                break
        if candidate is None and "schema_version" in output and "receipt_id" in output:
            try:
                candidate = receipt_type.from_dict(output)
            except Exception:
                return None, "evidence_invalid"
    if candidate is None:
        value = getattr(output, "receipt", None)
        if isinstance(value, receipt_type):
            candidate = value
    return candidate, None


__all__ = ()
