"""Heartbeat occurrence identity and settled-terminal recovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
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
