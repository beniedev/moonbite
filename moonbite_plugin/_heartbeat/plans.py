"""Durable Heartbeat effect-plan validation and storage helpers."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from ..effects import EffectRecord
from ..runtime_core import StateError

HEARTBEAT_EFFECT_PLAN_SCHEMA = "moon.heartbeat.effect_plan.v1"
DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX = ":delegated"

_EFFECT_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "candidate_id",
        "source_event_id",
        "epoch_id",
        "public_epoch_id",
        "closed",
        "effects",
    }
)
_LEGACY_EFFECT_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "candidate_id",
        "source_event_id",
        "epoch_id",
        "closed",
        "effects",
    }
)
_EFFECT_PLAN_EFFECT_FIELDS = frozenset(
    {
        "effect_id",
        "kind",
        "source_event_id",
        "epoch_id",
        "idempotency_key",
        "content_sha256",
        "content_length",
    }
)


class _EffectPlanLedger(Protocol):
    path: Path

    def rows(self) -> list[dict[str, Any]]: ...

    def get_or_append(
        self,
        row: Mapping[str, Any],
        *,
        matcher: Callable[[Mapping[str, Any]], bool],
    ) -> tuple[dict[str, Any], bool]: ...


class _Candidate(Protocol):
    candidate_id: str
    kind: str
    context: Mapping[str, Any]


class _Decision(Protocol):
    dm_user: bool
    wake_main: bool
    delivery_mode: str


class _EffectLedger(Protocol):
    def get(self, effect_id: str) -> EffectRecord | None: ...


class _Cadence(Protocol):
    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]: ...


def plan_effect_identity(
    value: Mapping[str, Any], *, label: str = "heartbeat effect plan"
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _EFFECT_PLAN_EFFECT_FIELDS:
        raise StateError(f"{label} effect has invalid fields")
    for field_name in (
        "effect_id",
        "kind",
        "source_event_id",
        "epoch_id",
        "idempotency_key",
    ):
        field_value = value[field_name]
        if type(field_value) is not str or not field_value.strip():
            raise StateError(f"{label} effect has invalid {field_name}")
    if value["kind"] not in {"heartbeat_delivery", "heartbeat_wake"}:
        raise StateError(f"{label} effect has invalid kind")
    content_sha256 = value["content_sha256"]
    if (
        type(content_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None
    ):
        raise StateError(f"{label} effect has invalid content hash")
    content_length = value["content_length"]
    if type(content_length) is not int or content_length <= 0:
        raise StateError(f"{label} effect has invalid content length")
    return dict(value)


def validate_effect_plan(
    value: Mapping[str, Any],
    *,
    validate_effect: Callable[..., dict[str, Any]] = plan_effect_identity,
    infer_public_epoch: Callable[[Iterable[Mapping[str, Any]]], str | None]
    | None = None,
    effect_key_matches: Callable[[Mapping[str, Any], str | None], bool] | None = None,
) -> dict[str, Any]:
    if infer_public_epoch is None:
        infer_public_epoch = infer_legacy_plan_public_epoch
    if effect_key_matches is None:
        effect_key_matches = plan_effect_key_matches
    if not isinstance(value, Mapping) or set(value) not in {
        _EFFECT_PLAN_FIELDS,
        _LEGACY_EFFECT_PLAN_FIELDS,
    }:
        raise StateError("heartbeat effect plan has invalid fields")
    if value["schema_version"] != HEARTBEAT_EFFECT_PLAN_SCHEMA:
        raise StateError("heartbeat effect plan has unsupported schema")
    for field_name in ("candidate_id", "source_event_id", "epoch_id"):
        field_value = value[field_name]
        if type(field_value) is not str or not field_value.strip():
            raise StateError(f"heartbeat effect plan has invalid {field_name}")
    if value["closed"] is not True:
        raise StateError("heartbeat effect plan must be closed")
    effects = value["effects"]
    if not isinstance(effects, list) or not 1 <= len(effects) <= 2:
        raise StateError("heartbeat effect plan has invalid effect set")
    selected = [validate_effect(effect) for effect in effects]
    kinds = [effect["kind"] for effect in selected]
    ids = [effect["effect_id"] for effect in selected]
    idempotency = [effect["idempotency_key"] for effect in selected]
    if len(set(kinds)) != len(kinds):
        raise StateError("heartbeat effect plan repeats an effect kind")
    if len(set(ids)) != len(ids) or len(set(idempotency)) != len(idempotency):
        raise StateError("heartbeat effect plan repeats an effect identity")
    if "public_epoch_id" in value:
        public_epoch = value["public_epoch_id"]
        if public_epoch is not None and (
            type(public_epoch) is not str or not public_epoch.strip()
        ):
            raise StateError("heartbeat effect plan has invalid public epoch")
    else:
        public_epoch = infer_public_epoch(selected)
    if value["epoch_id"] != (public_epoch or "heartbeat"):
        raise StateError("heartbeat effect plan public epoch conflicts")
    for effect in selected:
        if (
            effect["source_event_id"] != value["source_event_id"]
            or effect["epoch_id"] != value["epoch_id"]
        ):
            raise StateError("heartbeat effect plan identity conflicts")
        if not effect_key_matches(effect, public_epoch):
            raise StateError("heartbeat effect plan public epoch conflicts")
    return {
        **dict(value),
        "public_epoch_id": public_epoch,
        "effects": selected,
    }


def effect_plan_rows(
    ledger: _EffectPlanLedger | None,
    *,
    validate_plan: Callable[[Mapping[str, Any]], dict[str, Any]] = validate_effect_plan,
) -> tuple[dict[str, Any], ...]:
    if ledger is None or not ledger.path.exists():
        return ()
    try:
        rows = ledger.rows()
    except Exception as exc:
        raise StateError("heartbeat effect plan is unreadable") from exc
    return tuple(validate_plan(row) for row in rows)


def plan_public_epoch_candidates(
    value: Mapping[str, Any],
    *,
    effect_key_matches: Callable[[Mapping[str, Any], str | None], bool] | None = None,
) -> frozenset[str | None]:
    if effect_key_matches is None:
        effect_key_matches = plan_effect_key_matches
    internal_epoch = value["epoch_id"]
    candidates = {
        public_epoch
        for public_epoch in (None, internal_epoch)
        if internal_epoch == (public_epoch or "heartbeat")
        and effect_key_matches(value, public_epoch)
    }
    return frozenset(candidates)


def infer_legacy_plan_public_epoch(
    effects: Iterable[Mapping[str, Any]],
    *,
    public_epoch_candidates: Callable[
        [Mapping[str, Any]], frozenset[str | None]
    ] = plan_public_epoch_candidates,
) -> str | None:
    candidate_sets = tuple(public_epoch_candidates(effect) for effect in effects)
    if not candidate_sets or any(not candidates for candidates in candidate_sets):
        raise StateError("heartbeat effect plan public epoch is unproven")
    common = set(candidate_sets[0]).intersection(*candidate_sets[1:])
    if len(common) != 1:
        raise StateError("heartbeat effect plan public epoch is ambiguous")
    return next(iter(common))


def plan_effect_key_matches(
    effect: Mapping[str, Any], public_epoch_id: str | None
) -> bool:
    base = (
        f"heartbeat:{effect['source_event_id']}:"
        f"{effect['kind'].removeprefix('heartbeat_')}"
    )
    prefix = base if public_epoch_id is None else f"{base}:{public_epoch_id}"
    expected = {prefix}
    if effect["kind"] == "heartbeat_delivery":
        expected.add(prefix + DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX)
    return effect["idempotency_key"] in expected


def effect_plan(
    rows: Iterable[Mapping[str, Any]],
    source_event_id: str,
    public_epoch_id: str | None,
) -> dict[str, Any] | None:
    matches = tuple(
        row
        for row in rows
        if row["source_event_id"] == source_event_id
        and row["public_epoch_id"] == public_epoch_id
    )
    if len(matches) > 1:
        first = matches[0]
        if any(row != first for row in matches[1:]):
            raise StateError("heartbeat effect plan identity conflict")
    return matches[0] if matches else None


def effect_plan_for_candidate(
    candidate: _Candidate,
    *,
    candidate_epoch: Callable[[_Candidate], str | None],
    find_effect_plan: Callable[[str, str | None], dict[str, Any] | None],
) -> dict[str, Any] | None:
    source = (
        candidate.context.get("source_event_id")
        or candidate.context.get("event_id")
        or candidate.candidate_id
    )
    public_epoch = candidate_epoch(candidate)
    if type(source) is not str or not source.strip():
        return None
    return find_effect_plan(source, public_epoch)


def effect_plan_for_occurrence(
    rows: Iterable[Mapping[str, Any]],
    occurrence_id: str,
    epoch_id: str | None,
) -> dict[str, Any] | None:
    matches = tuple(
        row
        for row in rows
        if row["public_epoch_id"] == epoch_id
        and (
            row["candidate_id"] == occurrence_id
            or row["source_event_id"] == occurrence_id
        )
    )
    if len(matches) > 1:
        first = matches[0]
        if any(row != first for row in matches[1:]):
            raise StateError("heartbeat effect plan occurrence conflict")
    return matches[0] if matches else None


def plan_effect(plan: Mapping[str, Any] | None, kind: str) -> Mapping[str, Any] | None:
    if plan is None:
        return None
    return next(
        (effect for effect in plan["effects"] if effect["kind"] == f"heartbeat_{kind}"),
        None,
    )


def plan_matches(existing: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    if any(
        existing[field_name] != candidate[field_name]
        for field_name in (
            "candidate_id",
            "source_event_id",
            "epoch_id",
            "public_epoch_id",
            "closed",
        )
    ):
        return False
    by_kind = {effect["kind"]: effect for effect in existing["effects"]}
    for effect in candidate["effects"]:
        current = by_kind.get(effect["kind"])
        if current is None:
            return False
        if any(
            current[field_name] != effect[field_name]
            for field_name in (
                "kind",
                "source_event_id",
                "epoch_id",
                "idempotency_key",
                "content_sha256",
                "content_length",
            )
        ):
            return False
    return len(by_kind) == len(candidate["effects"])


def ensure_effect_plan(
    ledger: _EffectPlanLedger | None,
    candidate: _Candidate,
    decision: _Decision,
    now: datetime,
    *,
    candidate_epoch: Callable[[_Candidate], str | None],
    effect_body: Callable[[str, _Candidate, _Decision], bytes],
    new_effect_id: Callable[[], str],
    validate_plan: Callable[[Mapping[str, Any]], dict[str, Any]],
    plans_match: Callable[[Mapping[str, Any], Mapping[str, Any]], bool],
) -> dict[str, Any] | None:
    if not decision.dm_user and not decision.wake_main:
        return None
    if ledger is None:
        raise StateError("heartbeat closed effect plan requires a durable cadence root")
    source = (
        candidate.context.get("source_event_id")
        or candidate.context.get("event_id")
        or candidate.candidate_id
    )
    public_epoch = candidate_epoch(candidate)
    epoch = public_epoch or "heartbeat"
    if type(source) is not str or not source.strip():
        raise StateError("heartbeat effect plan source is invalid")
    effects: list[dict[str, Any]] = []
    for kind, enabled in (
        ("delivery", decision.dm_user),
        ("wake", decision.wake_main),
    ):
        if not enabled:
            continue
        body = effect_body(kind, candidate, decision)
        idempotency_key = f"heartbeat:{source}:{kind}"
        if public_epoch is not None:
            idempotency_key += f":{public_epoch}"
        if kind == "delivery" and decision.delivery_mode == "delegated":
            idempotency_key += DELEGATED_DELIVERY_IDEMPOTENCY_SUFFIX
        effects.append(
            {
                "effect_id": new_effect_id(),
                "kind": f"heartbeat_{kind}",
                "source_event_id": source,
                "epoch_id": epoch,
                "idempotency_key": idempotency_key,
                "content_sha256": hashlib.sha256(body).hexdigest(),
                "content_length": len(body),
            }
        )
    if not effects:
        return None
    plan = validate_plan(
        {
            "schema_version": HEARTBEAT_EFFECT_PLAN_SCHEMA,
            "candidate_id": candidate.candidate_id,
            "source_event_id": source,
            "epoch_id": epoch,
            "public_epoch_id": public_epoch,
            "closed": True,
            "effects": effects,
        }
    )
    try:
        existing, _created = ledger.get_or_append(
            plan,
            matcher=lambda row: (
                isinstance(row, Mapping)
                and row.get("schema_version") == HEARTBEAT_EFFECT_PLAN_SCHEMA
                and row.get("source_event_id") == source
                and row.get("epoch_id") == epoch
                and validate_plan(row)["public_epoch_id"] == public_epoch
            ),
        )
    except Exception as exc:
        raise StateError("heartbeat effect plan write failed") from exc
    existing_plan = validate_plan(existing)
    if not plans_match(existing_plan, plan):
        raise StateError("heartbeat effect plan decision conflict")
    return existing_plan


def effect_plan_incomplete(
    candidate: _Candidate,
    *,
    find_effect_plan: Callable[[_Candidate], dict[str, Any] | None],
    effect_ledger: _EffectLedger | None,
    cadence: _Cadence,
    validate_record: Callable[[EffectRecord, Mapping[str, Any]], None],
) -> bool:
    selected = find_effect_plan(candidate)
    if selected is None or effect_ledger is None:
        return False
    ledger_get = getattr(effect_ledger, "get", None)
    if not callable(ledger_get):
        raise StateError("effect ledger replay port is unavailable")
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
    for expected in selected["effects"]:
        if terminals.get(expected["effect_id"]) in {
            "pending",
            "executed_unverified",
        }:
            continue
        record = ledger_get(expected["effect_id"])
        if record is None or record.state == "intent":
            return True
        validate_record(record, expected)
    return False


def validate_plan_record(record: EffectRecord, expected: Mapping[str, Any]) -> None:
    if not isinstance(record, EffectRecord):
        raise StateError("heartbeat effect plan record is invalid")
    for field_name in (
        "effect_id",
        "kind",
        "source_event_id",
        "epoch_id",
        "idempotency_key",
        "content_sha256",
        "content_length",
    ):
        if getattr(record, field_name) != expected[field_name]:
            raise StateError("heartbeat effect plan record identity conflict")
