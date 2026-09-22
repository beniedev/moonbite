"""Read-only Autonomy observation helpers.

This module owns no runtime state and never acquires owner locks.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

from ..effects import EffectReceipt, EffectRecord
from ..observer import ObservationFact, RecoveryEvidence
from ..runtime_core import ensure_bounded_text, parse_time


def _read_audit_rows_lock_free(
    path: Path,
    *,
    statuses: frozenset[str],
) -> tuple[tuple[dict[str, Any], ...], str | None]:
    """Read only bounded autonomy audit telemetry without retaining payloads.

    Unrelated audit events are envelope-checked and discarded immediately
    after their payload is confirmed to be a mapping.  Their payload is never
    copied or traversed.  For ``audit.autonomy`` we retain only the scalar
    fields needed by the observer and one receipt reference.
    """

    if not path.exists():
        return (), None
    rows: list[dict[str, Any]] = []
    integrity: str | None = None
    seen_event_ids: set[str] = set()
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
                    if not isinstance(value, Mapping):
                        raise ValueError("row is not an object")
                    if set(value) != {
                        "schema_version",
                        "event_id",
                        "created_at",
                        "kind",
                        "source",
                        "payload",
                    }:
                        raise ValueError("event fields are invalid")
                    if value.get("schema_version") != "moon.event.v1":
                        raise ValueError("event schema is invalid")
                    event_id = value.get("event_id")
                    kind = value.get("kind")
                    source = value.get("source")
                    if (
                        type(event_id) is not str
                        or not event_id.strip()
                        or type(kind) is not str
                        or not kind.strip()
                        or type(source) is not str
                        or not source.strip()
                    ):
                        raise ValueError("event identity is invalid")
                    if event_id in seen_event_ids:
                        integrity = integrity or "duplicate_event_id"
                        continue
                    seen_event_ids.add(event_id)
                    payload = value.get("payload")
                    if not isinstance(payload, Mapping):
                        raise ValueError("event payload is invalid")
                    if kind != "audit.autonomy":
                        continue
                    created_at = parse_time(value.get("created_at"))

                    def optional_scalar(
                        name: str, *, selected_payload: Mapping[str, Any] = payload
                    ) -> str | None:
                        selected = selected_payload.get(name)
                        if selected is None:
                            return None
                        if type(selected) is not str or not selected.strip():
                            raise ValueError(f"autonomy audit field {name} is invalid")
                        ensure_bounded_text(selected, name, max_bytes=512)
                        return selected

                    status = optional_scalar("status")
                    if status not in statuses:
                        raise ValueError("autonomy audit status is invalid")
                    provider = optional_scalar("provider")
                    if provider is None and status not in {"failed", "skipped"}:
                        raise ValueError("autonomy audit fields are invalid")
                    evidence = payload.get("evidence")
                    if evidence is not None and not isinstance(evidence, Mapping):
                        raise ValueError("autonomy audit evidence is invalid")

                    def evidence_scalar(
                        name: str, *, selected_evidence: Any = evidence
                    ) -> str | None:
                        if not isinstance(selected_evidence, Mapping):
                            return None
                        selected = selected_evidence.get(name)
                        if selected is None:
                            return None
                        if type(selected) is not str or not selected.strip():
                            raise ValueError(
                                f"autonomy audit evidence {name} is invalid"
                            )
                        ensure_bounded_text(selected, name, max_bytes=512)
                        return selected

                    receipt_id = evidence_scalar("receipt_id")
                    receipt_event_id = evidence_scalar("event_id")
                    receipt_epoch_id = evidence_scalar("epoch_id")
                    receipt_content_sha256 = evidence_scalar("content_sha256")
                    receipt_content_length = None
                    if isinstance(evidence, Mapping):
                        selected_length = evidence.get("content_length")
                        if selected_length is not None:
                            if type(selected_length) is not int or selected_length <= 0:
                                raise ValueError(
                                    "autonomy audit evidence content_length is invalid"
                                )
                            receipt_content_length = selected_length
                    effect_id = optional_scalar("effect_id")
                    source_event_id = optional_scalar("source_event_id")
                    idempotency_key = optional_scalar("idempotency_key")
                except Exception as exc:
                    integrity = integrity or f"row_{type(exc).__name__}"
                    continue
                rows.append(
                    {
                        "event_id": event_id,
                        "kind": kind,
                        "created_at": created_at,
                        "status": status,
                        "provider": provider,
                        "effect_id": effect_id,
                        "source_event_id": source_event_id,
                        "idempotency_key": idempotency_key,
                        "receipt_id": receipt_id,
                        "receipt_event_id": receipt_event_id,
                        "receipt_epoch_id": receipt_epoch_id,
                        "receipt_content_sha256": receipt_content_sha256,
                        "receipt_content_length": receipt_content_length,
                    }
                )
    except Exception as exc:
        integrity = integrity or f"read_{type(exc).__name__}"
    return tuple(rows), integrity


def _autonomy_observer_reason(reason: Any) -> str:
    """Convert private reason text into a fixed public code."""

    if not isinstance(reason, str) or not reason:
        return "unspecified"
    prefix = reason.split(":", 1)[0].strip().lower()
    known = {
        "adapter_error",
        "adapter_malformed_return",
        "adapter_rejected",
        "adapter_unavailable",
        "effect_intent_error",
        "effect_pending_error",
        "effect_queue_error",
        "evidence_invalid",
        "missing_receipt",
        "provider_error",
        "receipt_mismatch",
    }
    return prefix if prefix in known else "failure"


def _autonomy_text(value: Any) -> str | None:
    if type(value) is str and value.strip():
        return value
    return None


def _autonomy_fact(
    *,
    key: str,
    code: str,
    state: str,
    target_date: date,
    event_time: datetime | None,
    refs: tuple[str, ...],
    counts: Mapping[str, int],
    recovery: RecoveryEvidence | None = None,
) -> ObservationFact:
    return ObservationFact(
        key=key,
        code=code,
        state=state,
        target_date=target_date,
        event_time=event_time,
        refs=refs,
        counts=dict(counts),
        recovery=recovery,
    )


def _autonomy_integrity(
    *, source: str, code: str, target_date: date
) -> ObservationFact:
    return _autonomy_fact(
        key=f"autonomy:integrity:{source}",
        code=f"autonomy_integrity_error:{code}",
        state="current",
        target_date=target_date,
        event_time=None,
        refs=(source,),
        counts={"integrity_errors": 1},
    )


def engine_observer_status(
    *,
    target_date: date,
    now: datetime,
    effect_ledger: Any,
    bus: Any,
    controls: Any,
    state_root: Callable[[Any], Any],
    record_provider: Callable[[Any], str | None],
    effect_kind: str,
    read_effect_history_lock_free: Callable[..., Any],
    read_audit_rows_lock_free: Callable[..., Any],
    observer_reason: Callable[[Any], str],
    text: Callable[[Any], str | None],
    fact: Callable[..., ObservationFact],
    integrity_fact: Callable[..., ObservationFact],
) -> tuple[ObservationFact, ...]:
    """Return provider/effect telemetry without invoking autonomy actors.

    The observer consumes only the effect ledger and the already-written
    audit stream.  It never calls the Judge, a provider runner, a sink, or
    reconciliation, and it never exposes ``ActivityResult.output``,
    context facts, or raw exception/reason text.
    """

    if type(target_date) is not date:
        raise TypeError("target_date must be a date")
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    facts: list[ObservationFact] = []
    effect_path: Path | None = None
    ledger = effect_ledger
    ledger_file = getattr(getattr(ledger, "ledger", None), "path", None)
    if isinstance(ledger_file, Path):
        effect_path = ledger_file
    if effect_path is None:
        root = state_root(controls)
        if isinstance(root, Path):
            effect_path = root / "effects.jsonl"

    effect_records: tuple[EffectRecord, ...] = ()
    effect_integrity: str | None = None
    if effect_path is not None:
        effect_records, effect_integrity = read_effect_history_lock_free(effect_path)

    audit_path: Path | None = None
    audit_file = getattr(getattr(bus, "audit", None), "path", None)
    if isinstance(audit_file, Path):
        audit_path = audit_file
    audit_rows: tuple[dict[str, Any], ...] = ()
    audit_integrity: str | None = None
    if audit_path is not None:
        audit_rows, audit_integrity = read_audit_rows_lock_free(audit_path)

    # A timeline entry contains only stable identifiers and bounded status
    # metadata.  The source payload itself is never retained in an entry.
    timelines: dict[tuple[str, str], list[dict[str, Any]]] = {}
    latest_effect: dict[tuple[str, str], dict[str, Any]] = {}
    effect_group_by_id: dict[str, tuple[str, str]] = {}
    effect_group_by_idempotency: dict[str, tuple[str, str]] = {}
    effect_groups_by_source: dict[str, set[tuple[str, str]]] = {}
    effect_group_by_receipt: dict[str, tuple[str, str]] = {}

    for record in effect_records:
        if record.kind != effect_kind:
            continue
        provider = record_provider(record)
        group = (record.kind, record.idempotency_key)
        effect_group_by_id[record.effect_id] = group
        effect_group_by_idempotency[record.idempotency_key] = group
        effect_groups_by_source.setdefault(record.source_event_id, set()).add(group)
        event_time = (
            record.receipt.observed_at
            if record.receipt is not None
            else record.created_at
        )
        entry = {
            "provider": provider,
            "effect_id": record.effect_id,
            "idempotency_key": record.idempotency_key,
            "source_event_id": record.source_event_id,
            "status": record.state,
            "event_time": event_time,
            "record": record,
            "receipt_id": (
                None if record.receipt is None else record.receipt.receipt_id
            ),
        }
        timelines.setdefault(group, []).append(entry)
        latest_effect[group] = entry
        if record.receipt is not None:
            effect_group_by_receipt[record.receipt.receipt_id] = group
            effect_group_by_receipt[record.receipt.event_id] = group

    audit_only: dict[tuple[str, str], list[dict[str, Any]]] = {}
    audit_conflict = False
    canonical_provider_labels: dict[tuple[str, str], set[str]] = {}
    canonical_provider_conflicts: set[tuple[str, str]] = set()

    def audit_group(row: Mapping[str, Any]) -> tuple[str, str]:
        effect_id = text(row.get("effect_id"))
        idempotency_key = text(row.get("idempotency_key"))
        event_id = text(row.get("event_id")) or "unknown"
        if effect_id is not None:
            return ("effect", effect_id)
        if idempotency_key is not None:
            return ("idempotency", idempotency_key)
        return ("audit", event_id)

    def linked_group(
        row: Mapping[str, Any],
    ) -> tuple[tuple[str, str] | None, bool]:
        effect_id = text(row.get("effect_id"))
        idempotency_key = text(row.get("idempotency_key"))
        effect_group = None if effect_id is None else effect_group_by_id.get(effect_id)
        idempotency_group = (
            None
            if idempotency_key is None
            else effect_group_by_idempotency.get(idempotency_key)
        )
        source_event_id = text(row.get("source_event_id"))
        source_groups = (
            set()
            if source_event_id is None
            else effect_groups_by_source.get(source_event_id, set())
        )
        source_group = next(iter(source_groups)) if len(source_groups) == 1 else None
        receipt_groups = {
            effect_group_by_receipt[receipt]
            for receipt in (
                text(row.get("receipt_id")),
                text(row.get("receipt_event_id")),
            )
            if receipt is not None and receipt in effect_group_by_receipt
        }
        receipt_group = next(iter(receipt_groups)) if len(receipt_groups) == 1 else None
        candidates = {
            candidate
            for candidate in (
                effect_group,
                idempotency_group,
                source_group,
                receipt_group,
            )
            if candidate is not None
        }
        if len(candidates) > 1:
            return next(iter(candidates)), True
        if not candidates:
            return None, False
        candidate = next(iter(candidates))
        anchor_conflict = (
            effect_group is None
            and effect_id is not None
            and idempotency_group is not None
        ) or (
            idempotency_group is None
            and idempotency_key is not None
            and effect_group is not None
        )
        return candidate, anchor_conflict

    def audit_conflicts_with_record(
        row: Mapping[str, Any],
        group: tuple[str, str],
    ) -> bool:
        record_entry = latest_effect[group]
        record = record_entry["record"]
        if not isinstance(record, EffectRecord):
            return True
        checks = (
            ("effect_id", record.effect_id),
            ("idempotency_key", record.idempotency_key),
            ("source_event_id", record.source_event_id),
            ("provider", record_entry["provider"]),
        )
        for field_name, expected in checks:
            actual = text(row.get(field_name))
            if field_name == "provider" and expected is None:
                continue
            if actual is not None and actual != expected:
                return True

        receipt = record.receipt
        receipt_checks = (
            ("receipt_id", None if receipt is None else receipt.receipt_id),
            (
                "receipt_event_id",
                None if receipt is None else receipt.event_id,
            ),
            (
                "receipt_epoch_id",
                record.epoch_id,
            ),
            (
                "receipt_content_sha256",
                record.content_sha256,
            ),
            (
                "receipt_content_length",
                record.content_length,
            ),
        )
        for field_name, expected in receipt_checks:
            actual = row.get(field_name)
            if actual is not None and actual != expected:
                return True

        status = text(row.get("status"))
        history_states = {
            item["record"].state
            for item in timelines[group]
            if isinstance(item.get("record"), EffectRecord)
        }
        if status == "completed":
            return record.state != "verified" or not isinstance(
                record.receipt, EffectReceipt
            )
        if status == "failed":
            return record.state != "failed"
        if status == "executed_unverified":
            return "executed_unverified" not in history_states
        if status == "awaiting_reconciliation":
            return not history_states.intersection(
                {"intent", "pending", "executed_unverified", "expired", "requeued"}
            )
        return status == "skipped"

    def exact_linked_provider(
        row: Mapping[str, Any], group: tuple[str, str]
    ) -> str | None:
        provider = text(row.get("provider"))
        record = latest_effect[group]["record"]
        if (
            provider is None
            or not isinstance(record, EffectRecord)
            or record.state != "verified"
            or not isinstance(record.receipt, EffectReceipt)
            or text(row.get("status")) != "completed"
        ):
            return None
        if any(
            text(row.get(field_name)) != expected
            for field_name, expected in (
                ("effect_id", record.effect_id),
                ("source_event_id", record.source_event_id),
                ("idempotency_key", record.idempotency_key),
                ("receipt_id", record.receipt.receipt_id),
            )
        ):
            return None
        return provider

    for row in audit_rows:
        if row.get("kind") != "audit.autonomy":
            continue
        status = text(row.get("status"))
        if status == "skipped":
            continue
        group, link_conflict = linked_group(row)
        if group is not None:
            row_conflict = link_conflict or audit_conflicts_with_record(row, group)
            if row_conflict:
                audit_conflict = True
            else:
                provider = exact_linked_provider(row, group)
                if provider is not None:
                    labels = canonical_provider_labels.setdefault(group, set())
                    labels.add(provider)
                    if len(labels) > 1:
                        canonical_provider_conflicts.add(group)
                        audit_conflict = True
            continue
        audit_only.setdefault(audit_group(row), []).append(row)

    canonical_bad_states = {
        "intent",
        "pending",
        "executed_unverified",
        "expired",
        "requeued",
        "failed",
    }
    for group, timeline in timelines.items():
        selected = latest_effect[group]
        record = selected["record"]
        provider = text(selected.get("provider"))
        if not isinstance(record, EffectRecord):
            continue
        provider_labels = canonical_provider_labels.get(group, set())
        if (
            provider is None
            and len(provider_labels) == 1
            and group not in canonical_provider_conflicts
        ):
            provider = next(iter(provider_labels))
        effect_id = text(record.effect_id)
        source_event_id = text(record.source_event_id)
        idempotency_key = text(record.idempotency_key)
        event_time = selected.get("event_time")
        if not isinstance(event_time, datetime):
            event_time = now

        status = record.state
        if status in {"pending", "executed_unverified"} and record.expires_at < now:
            status = "expired"
            event_time = record.expires_at
        if status == "verified":
            if not isinstance(record.receipt, EffectReceipt):
                facts.append(
                    integrity_fact(
                        source=effect_path.name
                        if effect_path is not None
                        else "effects.jsonl",
                        code="verified_effect_missing_receipt",
                        target_date=target_date,
                    )
                )
                continue
            public_status = "completed"
        else:
            if provider is None:
                facts.append(
                    integrity_fact(
                        source=effect_path.name
                        if effect_path is not None
                        else "effects.jsonl",
                        code="provider_unavailable",
                        target_date=target_date,
                    )
                )
                continue
            public_status = status

        recovered = public_status == "completed" and any(
            isinstance(item.get("record"), EffectRecord)
            and item["record"].state in canonical_bad_states
            for item in timeline
            if item is not selected
        )
        if public_status == "completed":
            code = "autonomy_verified"
            fact_state = "recovered_history" if recovered else "neutral"
        elif public_status == "failed":
            reason_code = observer_reason(record.reason)
            code = f"autonomy_provider_failure:{reason_code}"
            fact_state = "current"
        elif public_status == "executed_unverified":
            code = "autonomy_executed_unverified"
            fact_state = "current"
        elif public_status in {
            "intent",
            "pending",
            "awaiting_reconciliation",
            "expired",
            "requeued",
        }:
            code = "autonomy_awaiting_reconciliation"
            fact_state = "current"
        else:
            continue

        refs: list[str] = []
        if provider is not None:
            refs.append(f"provider:{provider}")
        if effect_id is not None:
            refs.append(f"effect:{effect_id}")
        if source_event_id is not None:
            refs.append(f"source:{source_event_id}")
        if idempotency_key is not None:
            refs.append(f"idempotency:{idempotency_key}")
        refs.append(f"sha256:{record.content_sha256}")
        receipt_id = None if record.receipt is None else record.receipt.receipt_id
        if receipt_id is not None:
            refs.append(f"receipt:{receipt_id}")

        recovery: RecoveryEvidence | None = None
        if fact_state == "recovered_history":
            recovery = RecoveryEvidence(
                ref=receipt_id,
                code="autonomy_verified",
                recovered_at=event_time,
            )
        counts = {
            "telemetry": 1,
            "attempt": record.attempt,
            "content_length": record.content_length,
        }
        facts.append(
            fact(
                key=(
                    f"autonomy:{provider or 'unknown'}:effect:"
                    f"{effect_id or idempotency_key or 'unknown'}"
                ),
                code=code,
                state=fact_state,
                target_date=target_date,
                event_time=event_time,
                refs=tuple(refs),
                counts=counts,
                recovery=recovery,
            )
        )

    for timeline in audit_only.values():
        selected = max(
            enumerate(timeline),
            key=lambda item: (item[1].get("event_time") or now, item[0]),
        )[1]
        status = text(selected.get("status"))
        if status == "skipped":
            continue
        provider = text(selected.get("provider"))
        effect_id = text(selected.get("effect_id"))
        source_event_id = text(selected.get("source_event_id"))
        idempotency_key = text(selected.get("idempotency_key"))
        event_id = text(selected.get("event_id"))
        event_time = selected.get("event_time")
        if not isinstance(event_time, datetime):
            event_time = now
        if status == "completed":
            code = "autonomy_completion_unverified"
        elif status == "failed":
            code = "autonomy_provider_failure:failure"
        elif status == "executed_unverified":
            code = "autonomy_executed_unverified"
        elif status == "awaiting_reconciliation":
            code = "autonomy_awaiting_reconciliation"
        else:
            continue
        refs: list[str] = []
        if provider is not None:
            refs.append(f"provider:{provider}")
        if effect_id is not None:
            refs.append(f"effect:{effect_id}")
        if source_event_id is not None:
            refs.append(f"source:{source_event_id}")
        if idempotency_key is not None:
            refs.append(f"idempotency:{idempotency_key}")
        suffix = effect_id or idempotency_key or event_id or "global"
        scope = provider or "global"
        facts.append(
            fact(
                key=f"autonomy:{scope}:effect:{suffix}",
                code=code,
                state="current",
                target_date=target_date,
                event_time=event_time,
                refs=tuple(refs),
                counts={"telemetry": 1},
            )
        )

    if audit_conflict:
        facts.append(
            integrity_fact(
                source=(
                    f"{audit_path.name}:projection"
                    if audit_path is not None
                    else "audit.jsonl:projection"
                ),
                code="audit_effect_conflict",
                target_date=target_date,
            )
        )

    if effect_integrity is not None:
        facts.append(
            integrity_fact(
                source=effect_path.name if effect_path is not None else "effects.jsonl",
                code=effect_integrity,
                target_date=target_date,
            )
        )
    if audit_integrity is not None:
        facts.append(
            integrity_fact(
                source=audit_path.name if audit_path is not None else "audit.jsonl",
                code=audit_integrity,
                target_date=target_date,
            )
        )

    selected_facts: dict[str, ObservationFact] = {}
    rank = {"neutral": 0, "recovered_history": 1, "current": 2}
    for fact in facts:
        previous = selected_facts.get(fact.key)
        if previous is None or rank[fact.state] > rank[previous.state]:
            selected_facts[fact.key] = fact
    return tuple(sorted(selected_facts.values(), key=lambda fact: fact.key))


__all__ = ()
