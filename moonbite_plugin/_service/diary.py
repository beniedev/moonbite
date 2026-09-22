"""Receipt-backed diary synthesis use case."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from typing import Any

from ..effects import EffectReceipt, EffectRecord
from ..memory_orchestration import content_descriptor
from ..runtime_core import StateError


class DiaryUseCaseMethods:
    def synthesize_diary(
        self,
        *,
        day: date,
        evidence_refs: list[str],
        title_hint: str = "",
    ) -> dict[str, Any]:
        if not self.config["modules"]["memory"]:
            raise RuntimeError("memory module is disabled")
        if self.diary_writer is None:
            raise RuntimeError("hippocampus model route is not configured")
        refs = sorted({ref.strip() for ref in evidence_refs if ref.strip()})
        if not refs or len(refs) > 20:
            raise ValueError("diary synthesis requires 1 to 20 evidence refs")
        normalized_title_hint = title_hint.strip()
        canonical = json.dumps(
            {
                "day": day.isoformat(),
                "evidence_refs": refs,
                "title_hint": normalized_title_hint,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        identity_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        source_event_id = f"diary-source:{identity_hash}"
        idempotency_key = f"diary-idempotency:{identity_hash}"
        epoch_id = f"diary-epoch:{identity_hash}"
        entry_id = f"diary_{identity_hash}"
        source_ref = "evidence:" + ",".join(refs)
        refs_hash = hashlib.sha256(
            json.dumps(
                refs,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

        def audit(
            status: str,
            reason_code: str,
            effect_record: EffectRecord | None = None,
        ) -> None:
            details: dict[str, Any] = {
                "entry_id": entry_id,
                "evidence_refs_sha256": refs_hash,
                "evidence_count": len(refs),
                "reason_code": reason_code,
            }
            if effect_record is not None:
                details["effect_id"] = effect_record.effect_id
            self._record_memory_audit(
                "diary_synthesis",
                status=status,
                details=details,
            )

        def state_audit_status(state: str) -> str:
            if state == "failed":
                return "failed"
            if state in {"executed_unverified", "expired"}:
                return "awaiting_reconciliation"
            return "pending"

        def open_verified_entry(
            effect_record: EffectRecord,
            *,
            audit_code: str,
        ) -> dict[str, Any]:
            if not isinstance(effect_record.receipt, EffectReceipt) or (
                effect_record.receipt.event_id != source_event_id
                or effect_record.receipt.epoch_id != epoch_id
                or effect_record.receipt.content_sha256 != effect_record.content_sha256
                or effect_record.receipt.content_length != effect_record.content_length
            ):
                audit("failed", "receipt_mismatch", effect_record)
                raise StateError("verified diary effect receipt does not match")
            open_ref = f"diary:{entry_id}"
            try:
                opened = self.memory.open(open_ref)
            except StateError:
                audit("failed", "verified_record_open_error", effect_record)
                raise
            except Exception as exc:
                audit("failed", "verified_record_open_error", effect_record)
                raise StateError(
                    "verified diary effect record could not be opened"
                ) from exc
            if not isinstance(opened, Mapping):
                audit("failed", "verified_record_missing", effect_record)
                raise StateError("verified diary effect has no deterministic record")
            if (
                opened.get("entry_id") != entry_id
                or opened.get("day") != day.isoformat()
                or opened.get("source_ref") != source_ref
            ):
                audit("failed", "verified_record_mismatch", effect_record)
                raise StateError("verified diary effect record does not match")
            try:
                opened_digest, opened_length = content_descriptor(
                    {
                        "body": opened["body"],
                        "day": opened["day"],
                        "entry_id": opened["entry_id"],
                        "source_ref": opened["source_ref"],
                        "title": opened["title"],
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                audit("failed", "verified_record_invalid", effect_record)
                raise StateError("verified diary effect record is invalid") from exc
            if (
                opened_digest != effect_record.content_sha256
                or opened_length != effect_record.content_length
            ):
                audit("failed", "verified_record_mismatch", effect_record)
                raise StateError("verified diary effect content does not match")
            audit("completed", audit_code, effect_record)
            return dict(opened)

        find_by_idempotency = getattr(self.effects, "find_by_idempotency", None)
        if not callable(find_by_idempotency):
            raise StateError("diary writer effect lookup is unavailable")
        existing = find_by_idempotency(idempotency_key)
        if existing is not None:
            if (
                existing.kind != "diary"
                or existing.source_event_id != source_event_id
                or existing.epoch_id != epoch_id
            ):
                audit("failed", "effect_identity_mismatch", existing)
                raise StateError("diary writer effect identity mismatch")
            if existing.state != "verified":
                audit(
                    state_audit_status(existing.state),
                    f"effect_state:{existing.state}",
                    existing,
                )
                raise StateError(
                    f"diary writer effect is not verified: {existing.state}"
                )
            return open_verified_entry(existing, audit_code="verified_replay")

        evidence: list[Mapping[str, Any]] = []
        for open_ref in refs:
            opened = self.memory.open(open_ref)
            if opened is None:
                raise ValueError(f"memory evidence ref not found: {open_ref}")
            evidence.append({"open_ref": open_ref, "record": opened})
        try:
            draft = self.diary_writer.synthesize(
                day=day,
                evidence=evidence,
                title_hint=normalized_title_hint,
            )
        except Exception as exc:
            audit("failed", f"synthesis_error:{type(exc).__name__}")
            raise

        content = {
            "body": draft.body,
            "day": day.isoformat(),
            "entry_id": entry_id,
            "source_ref": source_ref,
            "title": draft.title,
        }

        def local_writer(request: Any) -> EffectReceipt:
            if (
                request.operation != "diary"
                or request.source_event_id != source_event_id
                or request.idempotency_key != idempotency_key
                or request.epoch_id != epoch_id
            ):
                raise StateError("diary writer request identity mismatch")
            request_content = request.content
            if not isinstance(request_content, Mapping):
                raise StateError("diary writer request content is invalid")
            if dict(request_content) != content:
                raise StateError("diary writer request content mismatch")
            entry = self.memory.append_diary(
                day=date.fromisoformat(request_content["day"]),
                title=request_content["title"],
                body=request_content["body"],
                source_ref=request_content["source_ref"],
                entry_id=request_content["entry_id"],
            )
            return EffectReceipt(
                receipt_id=f"receipt:{entry_id}",
                event_id=request.source_event_id,
                observed_at=entry.created_at,
                content_sha256=request.content_sha256,
                content_length=request.content_length,
                epoch_id=request.epoch_id,
            )

        handoff = self.submit_memory_write(
            "diary",
            local_writer,
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            epoch_id=epoch_id,
            content=content,
        )
        effect_record = getattr(handoff, "record", None)
        if not isinstance(effect_record, EffectRecord):
            audit("failed", "writer_effect_invalid")
            raise StateError("diary writer returned an invalid effect record")
        if not effect_record.verified:
            reason_code = (
                f"append_error:{handoff.error_type}"
                if handoff.error_type
                else f"effect_state:{effect_record.state}"
            )
            audit(state_audit_status(effect_record.state), reason_code, effect_record)
            raise StateError(
                f"diary writer effect is not verified: {effect_record.state}"
            )
        return open_verified_entry(effect_record, audit_code="synthesized")
