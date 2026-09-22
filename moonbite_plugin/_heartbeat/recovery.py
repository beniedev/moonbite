"""Heartbeat occurrence identity and settled-terminal recovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, Protocol

from ..control import GateResult
from ..effects import EffectRecord
from ..runtime_core import StateError
from .plans import DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX


class _Candidate(Protocol):
    candidate_id: str
    context: Mapping[str, Any]


def canonical_terminal(result: Any, *, execution_lock_code: Any) -> str | None:
    """Return a terminal only when durable effect truth is settled."""

    if result.status == "skipped":
        if result.reason_code is execution_lock_code:
            return None
        return result.reason_code.value if result.reason_code is not None else "skipped"
    if result.status == "allowed":
        return "allowed"
    if result.status == "completed":
        if result.effects and not all(effect.verified for effect in result.effects):
            return None
        return "verified" if result.effects else "completed"
    if result.status == "intentional_silence":
        if not result.effects or any(
            effect.terminal not in {"intentional_silence", "verified"}
            for effect in result.effects
        ):
            return None
        if not any(
            effect.terminal == "intentional_silence" for effect in result.effects
        ):
            return None
        return "intentional_silence"
    if result.status == "failed":
        if result.effects and any(
            effect.terminal not in {"failed", "intentional_silence", "verified"}
            for effect in result.effects
        ):
            return None
        return "failed"
    return None


def public_epoch_from_effect(record: Any) -> Any:
    """Return the explicit epoch while hiding the legacy ledger default."""

    epoch_id = getattr(record, "epoch_id", None)
    if epoch_id is None:
        return None
    if epoch_id != "heartbeat":
        return epoch_id
    source = getattr(record, "source_event_id", None)
    kind = getattr(record, "kind", None)
    key = getattr(record, "idempotency_key", None)
    if (
        type(source) is str
        and type(kind) is str
        and type(key) is str
        and kind.startswith("heartbeat_")
    ):
        legacy = f"heartbeat:{source}:{kind.removeprefix('heartbeat_')}"
        if key in {
            legacy,
            legacy + DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX,
        }:
            return None
    return epoch_id


def result_public_epoch(result: Any) -> str | None:
    """Recover explicit terminal epoch without promoting legacy defaults."""

    epoch_id = result.epoch_id
    for effect in result.effects:
        candidate_epoch: Any = None
        if effect.receipt is not None:
            candidate_epoch = effect.receipt.epoch_id
            if candidate_epoch == "heartbeat":
                candidate_epoch = None
        if candidate_epoch is None:
            continue
        if type(candidate_epoch) is not str or not candidate_epoch.strip():
            raise ValueError("heartbeat epoch_id must be non-empty when provided")
        if epoch_id is not None and epoch_id != candidate_epoch:
            raise StateError("heartbeat epoch identity conflict")
        epoch_id = candidate_epoch
    return epoch_id


def effect_failure_audit_statuses(record: EffectRecord) -> frozenset[str]:
    """Return the bounded failure statuses emitted for one effect."""

    reason = record.reason
    if not isinstance(reason, str) or not reason.strip():
        return frozenset({"failed"})
    if reason == "intentional_silence":
        return frozenset({"intentional_silence"})

    statuses = {"failed", reason}
    if reason.startswith("adapter_error:"):
        error_name = reason.removeprefix("adapter_error:")
        if record.kind == "heartbeat_delivery":
            statuses.add(f"delivery_error:{error_name}")
        elif record.kind == "heartbeat_wake":
            statuses.add(f"wake_error:{error_name}")
    return frozenset(statuses)


def has_explicit_occurrence(candidate: _Candidate) -> bool:
    if candidate.candidate_id.strip():
        return True
    return any(
        type(candidate.context.get(key)) is str
        and bool(candidate.context.get(key).strip())
        for key in ("event_id", "source_event_id", "candidate_id")
    )


def existing_terminal_result(
    candidate_id: str,
    *,
    epoch_id: str | None = None,
    finder: Callable[..., Any] | None,
    accepts_keyword: Callable[[Callable[..., Any], str], bool],
    effect_owner_exists: Callable[[], bool],
    effect_records: Callable[[], tuple[EffectRecord, ...]],
    find_plan: Callable[[str, str | None], Mapping[str, Any] | None],
    validate_plan_record: Callable[[EffectRecord, Mapping[str, Any]], None],
    public_epoch: Callable[[Any], Any],
    failure_audit_statuses: Callable[[EffectRecord], frozenset[str]],
    canonical_result_terminal: Callable[[Any], str | None],
    result_type: Callable[..., Any],
    reason_code_type: Callable[[Any], Any],
) -> Any | None:
    if not callable(finder):
        return None
    if accepts_keyword(finder, "epoch_id"):
        event = finder("heartbeat", candidate_id, epoch_id=epoch_id)
    elif epoch_id is None:
        event = finder("heartbeat", candidate_id)
    else:
        raise StateError("heartbeat audit epoch is unsupported")
    if event is None:
        return None
    payload = event.payload
    status = payload.get("status")
    if not isinstance(status, str) or not status.strip():
        raise StateError("heartbeat terminal audit has invalid status")
    allowed_statuses = {
        "skipped",
        "allowed",
        "completed",
        "failed",
        "intentional_silence",
    }
    if status not in allowed_statuses:
        raise StateError("heartbeat terminal audit has invalid status")
    terminal = payload.get("terminal")
    if type(terminal) is not str or not terminal.strip():
        raise StateError("heartbeat terminal audit has invalid terminal")

    def validate_status(expected_terminal: str) -> None:
        expected_statuses = {
            "verified": {"completed", "allowed"},
            "failed": {"failed"},
            "intentional_silence": {"completed", "intentional_silence"},
        }
        if (
            expected_terminal in expected_statuses
            and status not in expected_statuses[expected_terminal]
        ):
            raise StateError("heartbeat audit status conflicts with effect terminal")

    effect_owner = effect_owner_exists()
    if terminal == "verified" and not effect_owner:
        raise StateError("heartbeat verified audit effect ledger is unavailable")
    matching: list[EffectRecord] = []
    plan = find_plan(candidate_id, epoch_id)
    if plan is not None and not effect_owner:
        raise StateError("heartbeat audit effect evidence is missing")
    if effect_owner:
        all_records = effect_records()
        raw_source = payload.get("source_event_id")
        if raw_source is not None and (
            type(raw_source) is not str or not raw_source.strip()
        ):
            raise StateError("heartbeat terminal audit has invalid source")
        expected_source = (
            plan["source_event_id"] if plan is not None else raw_source or candidate_id
        )
        if (
            plan is not None
            and raw_source is not None
            and raw_source != expected_source
        ):
            raise StateError("heartbeat audit source identity conflict")
        matching = [
            record
            for record in all_records
            if record.kind in {"heartbeat_delivery", "heartbeat_wake"}
            and record.source_event_id == expected_source
            and public_epoch(record) == epoch_id
        ]
        if raw_source is not None and not matching:
            raise StateError("heartbeat audit source identity conflict")
        expected_records: list[EffectRecord] | None = None
        if plan is not None:
            expected_records = []
            for expected in plan["effects"]:
                record = next(
                    (
                        item
                        for item in all_records
                        if item.effect_id == expected["effect_id"]
                    ),
                    None,
                )
                if record is None:
                    raise StateError("heartbeat audit effect is missing")
                validate_plan_record(record, expected)
                expected_records.append(record)
            matching = expected_records
        raw_effect_ids = payload.get("effect_ids")
        audit_effect_ids: list[str] | None = None
        if raw_effect_ids is not None:
            if (
                not isinstance(raw_effect_ids, list)
                or not raw_effect_ids
                or any(
                    type(effect_id) is not str or not effect_id.strip()
                    for effect_id in raw_effect_ids
                )
                or len(set(raw_effect_ids)) != len(raw_effect_ids)
            ):
                raise StateError("heartbeat terminal audit has invalid effects")
            audit_effect_ids = list(raw_effect_ids)
            if plan is not None and set(audit_effect_ids) != {
                effect["effect_id"] for effect in plan["effects"]
            }:
                raise StateError("heartbeat audit effect conflict")
            if plan is None and set(audit_effect_ids) != {
                record.effect_id for record in matching
            }:
                raise StateError("heartbeat audit effect set is unproven")
            effect_records_by_id = [
                record
                for record in all_records
                if record.effect_id in set(audit_effect_ids)
            ]
            if len(effect_records_by_id) != len(audit_effect_ids):
                raise StateError("heartbeat audit effect conflict")
            if any(
                record.kind not in {"heartbeat_delivery", "heartbeat_wake"}
                for record in effect_records_by_id
            ):
                raise StateError("heartbeat audit effect conflict")
            if any(
                record.source_event_id != expected_source
                or public_epoch(record) != epoch_id
                for record in effect_records_by_id
            ):
                raise StateError("heartbeat audit effect conflict")
            matching = effect_records_by_id
        elif plan is None and (
            len(matching) > 1
            or (terminal == "verified" and payload.get("effect_id") is None)
        ):
            legacy_effect_ids: list[str] = []
            for field_name in ("delivery", "wake"):
                nested = payload.get(field_name)
                if not isinstance(nested, Mapping):
                    continue
                effect_id = nested.get("effect_id")
                if effect_id is None:
                    continue
                if type(effect_id) is not str or not effect_id.strip():
                    raise StateError("heartbeat audit effect conflict")
                legacy_effect_ids.append(effect_id)
            if (
                not isinstance(payload.get("decision"), Mapping)
                or len(set(legacy_effect_ids)) != len(legacy_effect_ids)
                or set(legacy_effect_ids) != {record.effect_id for record in matching}
            ):
                raise StateError("heartbeat audit effect set is unproven")
            audit_effect_ids = legacy_effect_ids
        audit_effect_id = payload.get("effect_id")
        if audit_effect_id is not None:
            if type(audit_effect_id) is not str or not audit_effect_id.strip():
                raise StateError("heartbeat terminal audit has invalid effect")
            if audit_effect_ids is not None and audit_effect_ids != [audit_effect_id]:
                raise StateError("heartbeat audit effect conflict")
            if plan is not None and len(plan["effects"]) != 1:
                raise StateError("heartbeat audit effect conflict")
            effect_records_by_id = [
                record for record in all_records if record.effect_id == audit_effect_id
            ]
            if len(effect_records_by_id) != 1:
                raise StateError("heartbeat audit effect conflict")
            effect = effect_records_by_id[0]
            if (
                effect.kind not in {"heartbeat_delivery", "heartbeat_wake"}
                or effect.source_event_id != expected_source
                or public_epoch(effect) != epoch_id
            ):
                raise StateError("heartbeat audit effect conflict")
            if plan is None and len(matching) > 1 and audit_effect_ids is None:
                raise StateError("heartbeat audit effect set is unproven")
            if effect.state == "verified":
                expected = "verified"
            elif effect.state == "failed":
                expected = (
                    "intentional_silence"
                    if effect.reason == "intentional_silence"
                    else "failed"
                )
            else:
                raise StateError("heartbeat audit effect conflict")
            if terminal != expected:
                raise StateError("heartbeat audit effect conflict")
            validate_status(expected)

        if terminal == "verified" and not matching:
            raise StateError("heartbeat verified audit effect is missing")
        elif matching:
            if plan is None and len(matching) > 1 and audit_effect_ids is None:
                raise StateError("heartbeat audit effect set is unproven")
            if any(record.state not in {"verified", "failed"} for record in matching):
                raise StateError("heartbeat audit effect conflict")
            real_failure = any(
                record.state == "failed" and record.reason != "intentional_silence"
                for record in matching
            )
            intentional_silence = any(
                record.state == "failed" and record.reason == "intentional_silence"
                for record in matching
            )
            expected = (
                "failed"
                if real_failure
                else "intentional_silence"
                if intentional_silence
                else "verified"
            )
            if terminal != expected:
                raise StateError("heartbeat audit effect conflict")
            validate_status(expected)
    records_by_id = {record.effect_id: record for record in matching}
    for field_name, expected_kind in (
        ("delivery", "heartbeat_delivery"),
        ("wake", "heartbeat_wake"),
    ):
        if field_name not in payload or payload.get(field_name) is None:
            continue
        nested = payload.get(field_name)
        if not isinstance(nested, Mapping):
            raise StateError("heartbeat audit effect conflict")
        effect_id = nested.get("effect_id")
        if effect_id is None:
            if any(
                key in nested
                for key in ("ok", "status", "terminal", "verified", "receipt")
            ):
                raise StateError("heartbeat audit effect conflict")
            continue
        if type(effect_id) is not str or not effect_id.strip():
            raise StateError("heartbeat audit effect conflict")
        record = records_by_id.get(effect_id)
        if record is None or record.kind != expected_kind:
            raise StateError("heartbeat audit effect conflict")
        if record.state == "verified" and record.receipt is not None:
            if (
                nested.get("ok") is not True
                or nested.get("status") != "verified"
                or nested.get("terminal") != "verified"
                or nested.get("verified") is not True
                or nested.get("receipt") != record.receipt.to_dict()
            ):
                raise StateError("heartbeat audit receipt conflict")
        elif record.state == "failed":
            expected_nested_statuses = failure_audit_statuses(record)
            expected_nested_terminal = (
                "intentional_silence"
                if record.reason == "intentional_silence"
                else "failed"
            )
            if (
                nested.get("ok") is not False
                or nested.get("status") not in expected_nested_statuses
                or nested.get("terminal") != expected_nested_terminal
                or nested.get("verified") is not False
                or nested.get("receipt") is not None
            ):
                raise StateError("heartbeat audit receipt conflict")
        else:
            raise StateError("heartbeat audit effect conflict")
    has_declared_effect = (
        payload.get("effect_ids") is not None or payload.get("effect_id") is not None
    )
    if not matching and has_declared_effect:
        raise StateError("heartbeat audit effect evidence is missing")
    if not matching and not has_declared_effect:
        try:
            effectless_code = reason_code_type(payload.get("reason_code"))
        except (TypeError, ValueError):
            effectless_code = None
        expected = canonical_result_terminal(
            result_type(
                status=status,
                reason=terminal,
                candidate_id=candidate_id,
                gate=GateResult(True, "replay", "already_settled", None),
                reason_code=effectless_code,
            )
        )
        if expected is None or terminal != expected:
            raise StateError("heartbeat audit status conflicts with effect terminal")
    raw_gate = payload.get("gate")
    if isinstance(raw_gate, Mapping):
        gate = GateResult(
            bool(raw_gate.get("allowed", False)),
            str(raw_gate.get("mode", "replay")),
            str(raw_gate.get("reason", terminal)),
            raw_gate.get("control_id")
            if isinstance(raw_gate.get("control_id"), str)
            else None,
        )
    else:
        gate = GateResult(True, "replay", "already_settled", None)
    raw_code = payload.get("reason_code")
    try:
        reason_code = reason_code_type(raw_code)
    except (TypeError, ValueError):
        reason_code = None
    replay_status = "completed" if status == "intentional_silence" else status
    return result_type(
        status=replay_status,
        reason=terminal,
        candidate_id=candidate_id,
        gate=gate,
        reason_code=reason_code,
        degraded=payload.get("degraded") is True,
        projection_errors=(),
        epoch_id=epoch_id,
    )


def existing_effect(
    record: EffectRecord,
    now: datetime,
    *,
    effect_ledger: Any,
    effect_ttl: timedelta,
    effect_result: Callable[..., Any],
    fail_effect: Callable[..., Any],
    pending_code: Any,
    expired_code: Any,
    replay_error_code: Any,
    effect_error_code: Any,
) -> Any | None:
    if record.state == "verified":
        return effect_result(record, "verified")
    if record.state in {"pending", "executed_unverified"}:
        if record.expires_at >= now:
            return effect_result(record, "queued_unverified", pending_code)
        try:
            effect_ledger.expire(record.effect_id, now=now)
            record = effect_ledger.requeue(
                record.effect_id, expires_at=now + effect_ttl
            )
        except Exception:
            return fail_effect(
                record,
                "effect_reconciliation_error",
                "effect_reconciliation_error",
                replay_error_code,
                True,
            )
        return effect_result(record, "requeued", expired_code)
    if record.state == "expired":
        try:
            record = effect_ledger.requeue(
                record.effect_id, expires_at=now + effect_ttl
            )
        except Exception:
            return fail_effect(
                record,
                "effect_reconciliation_error",
                "effect_reconciliation_error",
                replay_error_code,
                True,
            )
        return effect_result(record, "requeued", expired_code)
    if record.state == "requeued":
        return effect_result(record, "requeued", expired_code)
    if record.state == "failed":
        return effect_result(record, record.reason or "failed", effect_error_code)
    return None


def candidate_existing_effects(
    candidate: _Candidate,
    now: datetime,
    *,
    effect_owner_exists: Callable[[], bool],
    candidate_epoch: Callable[[_Candidate], str | None],
    find_plan: Callable[[str, str | None], Mapping[str, Any] | None],
    cadence: Any,
    effect_ledger: Any,
    effect_ttl: timedelta,
    validate_plan_record: Callable[[EffectRecord, Mapping[str, Any]], None],
    resolve_existing_effect: Callable[[EffectRecord, datetime], Any | None],
    effect_result: Callable[..., Any],
    find_existing_terminal: Callable[..., Any],
    public_epoch: Callable[[Any], Any],
    accepts_keyword: Callable[[Callable[..., Any], str], bool],
    result_type: Callable[..., Any],
    pending_code: Any,
    expired_code: Any,
    effect_error_code: Any,
) -> tuple[Any | None, Any | None] | None:
    if not effect_owner_exists():
        return None
    source = (
        candidate.context.get("source_event_id")
        or candidate.context.get("event_id")
        or candidate.candidate_id
    )
    selected_epoch = candidate_epoch(candidate)
    plan = find_plan(source, selected_epoch)
    if plan is not None:
        terminals: Mapping[str, Any] = {}
        cadence_snapshot = getattr(cadence, "snapshot", None)
        if callable(cadence_snapshot):
            try:
                snapshot = cadence_snapshot()
            except AttributeError:
                snapshot = None
            if isinstance(snapshot, Mapping):
                raw_terminals = snapshot.get("effect_terminals", {})
                if isinstance(raw_terminals, Mapping):
                    terminals = raw_terminals
        ledger_get = getattr(effect_ledger, "get", None)
        if not callable(ledger_get):
            raise StateError("effect ledger replay port is unavailable")
        results: dict[str, Any] = {}
        for expected in plan["effects"]:
            effect_id = expected["effect_id"]
            terminal = terminals.get(effect_id)
            if terminal in {"pending", "executed_unverified"}:
                expire = getattr(effect_ledger, "expire", None)
                requeue = getattr(effect_ledger, "requeue", None)
                if callable(expire) and callable(requeue):
                    try:
                        expire(effect_id, now=now)
                    except ValueError as exc:
                        message = str(exc).strip().lower()
                        if message == "effect has not expired":
                            result = result_type(
                                True,
                                "queued_unverified",
                                effect_id=effect_id,
                                terminal=terminal,
                                reason_code=pending_code,
                            )
                        elif message.endswith(" from failed"):
                            # The marker is stale; the durable failure
                            # (including intentional_silence) is the
                            # projection truth.
                            try:
                                stale = ledger_get(effect_id)
                            except Exception as read_exc:
                                raise StateError(
                                    "effect ledger replay failed"
                                ) from read_exc
                            if stale is None or stale.state != "failed":
                                raise StateError(
                                    "effect reconciliation failed"
                                ) from exc
                            result = effect_result(
                                stale,
                                stale.reason or "failed",
                                effect_error_code,
                            )
                        else:
                            raise StateError("effect reconciliation failed") from exc
                    except Exception as exc:
                        raise StateError("effect reconciliation failed") from exc
                    else:
                        try:
                            requeued = requeue(effect_id, expires_at=now + effect_ttl)
                        except Exception as exc:
                            raise StateError("effect requeue failed") from exc
                        result = effect_result(requeued, "requeued", expired_code)
                else:
                    result = result_type(
                        True,
                        "queued_unverified",
                        effect_id=effect_id,
                        terminal=terminal,
                        reason_code=pending_code,
                    )
                results[expected["kind"].removeprefix("heartbeat_")] = result
                continue
            if terminal == "failed":
                try:
                    record = ledger_get(effect_id)
                except Exception as exc:
                    raise StateError("effect ledger replay failed") from exc
                # A durable failed record keeps its reason-derived terminal
                # (intentional_silence is not an effect_error).
                results[expected["kind"].removeprefix("heartbeat_")] = (
                    effect_result(
                        record,
                        record.reason or "failed",
                        effect_error_code,
                    )
                    if record is not None and record.state == "failed"
                    else result_type(
                        False,
                        "failed",
                        effect_id=effect_id,
                        terminal=terminal,
                        reason_code=effect_error_code,
                    )
                )
                continue
            if terminal == "requeued":
                results[expected["kind"].removeprefix("heartbeat_")] = result_type(
                    True,
                    "requeued",
                    effect_id=effect_id,
                    terminal=terminal,
                    reason_code=expired_code,
                )
                continue
            record = ledger_get(effect_id)
            if record is None:
                return None
            validate_plan_record(record, expected)
            kind = expected["kind"].removeprefix("heartbeat_")
            result = resolve_existing_effect(record, now)
            if result is None:
                return None
            results[kind] = result
        return results.get("delivery"), results.get("wake")
    legacy_siblings = tuple(
        record
        for record in effect_ledger.records()
        if record.kind in {"heartbeat_delivery", "heartbeat_wake"}
        and record.source_event_id == source
        and public_epoch(record) == selected_epoch
    )
    if len(legacy_siblings) > 1:
        raise StateError("heartbeat effect plan is unavailable for multiple effects")
    if len(legacy_siblings) == 1:
        canonical = find_existing_terminal(
            candidate.candidate_id,
            epoch_id=selected_epoch,
        )
        if canonical is None:
            record = legacy_siblings[0]
            unresolved = result_type(
                True,
                "awaiting_effect_plan",
                effect_id=record.effect_id,
                terminal=record.state,
                reason_code=pending_code,
            )
            return (
                unresolved if record.kind == "heartbeat_delivery" else None,
                unresolved if record.kind == "heartbeat_wake" else None,
            )
    selected: dict[str, EffectRecord] = {}
    precomputed: dict[str, Any] = {}
    terminals: Mapping[str, Any] = {}
    cadence_snapshot = getattr(cadence, "snapshot", None)
    if callable(cadence_snapshot):
        try:
            snapshot = cadence_snapshot()
        except AttributeError:
            snapshot = None
        if isinstance(snapshot, Mapping):
            raw_terminals = snapshot.get("effect_terminals", {})
            if isinstance(raw_terminals, Mapping):
                terminals = raw_terminals
    effect_ref = getattr(cadence, "effect_ref", None)
    ledger_get = getattr(effect_ledger, "get", None)
    if callable(effect_ref) and callable(ledger_get):
        for kind in ("delivery", "wake"):
            try:
                ref_kwargs = (
                    {"epoch_id": selected_epoch}
                    if accepts_keyword(effect_ref, "epoch_id")
                    else {}
                )
                effect_id = effect_ref(
                    source,
                    f"heartbeat_{kind}",
                    **ref_kwargs,
                )
            except (AttributeError, TypeError, ValueError):
                effect_id = None
            if effect_id is None:
                continue
            terminal = terminals.get(effect_id)
            if terminal in {"pending", "executed_unverified"}:
                expire = getattr(effect_ledger, "expire", None)
                requeue = getattr(effect_ledger, "requeue", None)
                if callable(expire) and callable(requeue):
                    try:
                        expire(effect_id, now=now)
                    except ValueError as exc:
                        message = str(exc).strip().lower()
                        if message == "effect has not expired":
                            precomputed[kind] = result_type(
                                True,
                                "queued_unverified",
                                effect_id=effect_id,
                                terminal=terminal,
                                reason_code=pending_code,
                            )
                            continue
                        if message.endswith(" from failed"):
                            # The marker is stale; the durable failure
                            # (including intentional_silence) is the
                            # projection truth.
                            try:
                                stale = ledger_get(effect_id)
                            except Exception as read_exc:
                                raise StateError(
                                    "effect ledger replay failed"
                                ) from read_exc
                            if stale is None or stale.state != "failed":
                                raise StateError(
                                    "effect reconciliation failed"
                                ) from exc
                            precomputed[kind] = effect_result(
                                stale,
                                stale.reason or "failed",
                                effect_error_code,
                            )
                            continue
                        raise StateError("effect reconciliation failed") from exc
                    except Exception as exc:
                        raise StateError("effect reconciliation failed") from exc
                    try:
                        requeued = requeue(effect_id, expires_at=now + effect_ttl)
                    except Exception as exc:
                        raise StateError("effect requeue failed") from exc
                    precomputed[kind] = effect_result(
                        requeued,
                        "requeued",
                        expired_code,
                    )
                    continue
            if terminal == "requeued":
                precomputed[kind] = result_type(
                    True,
                    "requeued",
                    effect_id=effect_id,
                    terminal=terminal,
                    reason_code=expired_code,
                )
                continue
            if terminal == "failed":
                try:
                    record = ledger_get(effect_id)
                except Exception as exc:
                    raise StateError("effect ledger replay failed") from exc
                # A durable failed record keeps its reason-derived terminal
                # (intentional_silence is not an effect_error).
                precomputed[kind] = (
                    effect_result(
                        record,
                        record.reason or "failed",
                        effect_error_code,
                    )
                    if record is not None and record.state == "failed"
                    else result_type(
                        False,
                        "failed",
                        effect_id=effect_id,
                        terminal=terminal,
                        reason_code=effect_error_code,
                    )
                )
                continue
            try:
                record = ledger_get(effect_id)
            except Exception as exc:
                raise StateError("effect ledger replay failed") from exc
            if record is not None and (
                record.source_event_id != source
                or public_epoch(record) != selected_epoch
            ):
                continue
            if record is not None:
                selected[kind] = record
    if not precomputed and callable(
        getattr(effect_ledger, "pending_for_reconciliation", None)
    ):
        try:
            pending = effect_ledger.pending_for_reconciliation(now=now)
        except Exception as exc:
            raise StateError("effect ledger replay failed") from exc
        for record in pending:
            if (
                record.source_event_id != source
                or public_epoch(record) != selected_epoch
            ):
                continue
            if record.kind == "heartbeat_delivery":
                selected.setdefault("delivery", record)
            elif record.kind == "heartbeat_wake":
                selected.setdefault("wake", record)
    if not selected and not precomputed:
        return None
    results = dict(precomputed)
    results.update(
        {
            kind: resolve_existing_effect(record, now)
            for kind, record in selected.items()
        }
    )
    if any(result is None for result in results.values()):
        return None
    return results.get("delivery"), results.get("wake")


def replay_effects(
    existing: tuple[Any | None, Any | None],
    *,
    make_result: Callable[..., Any],
    candidate_id: str,
    gate: GateResult,
    now: datetime,
    effect_error_code: Any,
    denied_code: Any,
    allowed_code: Any,
    expired_code: Any,
    pending_code: Any,
) -> Any:
    delivery, wake = existing
    effects = [effect for effect in existing if effect is not None]
    real_failures = [
        effect
        for effect in effects
        if not effect.ok and effect.terminal != "intentional_silence"
    ]
    if real_failures:
        failure_code = next(
            (
                effect.reason_code
                for effect in real_failures
                if effect.reason_code is not None
            ),
            effect_error_code,
        )
        return make_result(
            "failed",
            "effect_failed",
            candidate_id,
            gate,
            code=failure_code,
            delivery=delivery,
            wake=wake,
            next_judge_at=now,
        )
    if any(effect.terminal == "intentional_silence" for effect in effects) and all(
        effect.verified or effect.terminal == "intentional_silence"
        for effect in effects
    ):
        return make_result(
            "intentional_silence",
            "effects_settled_silent",
            candidate_id,
            gate,
            code=denied_code,
            delivery=delivery,
            wake=wake,
            next_judge_at=now,
        )
    if all(effect.verified for effect in effects):
        return make_result(
            "completed",
            "effects_verified",
            candidate_id,
            gate,
            code=allowed_code,
            delivery=delivery,
            wake=wake,
            next_judge_at=now,
        )
    reason = (
        "awaiting_effect_plan"
        if any(effect.status == "awaiting_effect_plan" for effect in effects)
        else "expired_effect"
        if any(effect.terminal == "requeued" for effect in effects)
        else "awaiting_receipt"
    )
    return make_result(
        "requeued" if reason == "expired_effect" else "pending",
        reason,
        candidate_id,
        gate,
        code=expired_code if reason == "expired_effect" else pending_code,
        delivery=delivery,
        wake=wake,
        next_judge_at=now,
    )
