"""Read-only Autonomy observation helpers.

This module owns no runtime state and never acquires owner locks.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

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


__all__ = ()
