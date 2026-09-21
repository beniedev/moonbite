"""Read-only Heartbeat evidence projection.

This module does not own cadence or effect state.  It reads existing evidence
without acquiring owner locks, reconciling effects, or creating state files.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..effects import EffectRecord, _read_effect_history_lock_free
from ..observer import ObservationFact, RecoveryEvidence

_OBSERVER_STATE_RANK = {"neutral": 0, "recovered_history": 1, "current": 2}


def _observer_fact(
    *,
    key: str,
    code: str,
    state: str,
    target_date: date,
    event_time: datetime | None = None,
    refs: tuple[str, ...] = (),
    counts: Mapping[str, int] | None = None,
    recovery: RecoveryEvidence | None = None,
) -> ObservationFact:
    return ObservationFact(
        key=key,
        code=code,
        state=state,
        target_date=target_date,
        event_time=event_time,
        refs=refs,
        counts={} if counts is None else dict(counts),
        recovery=recovery,
    )


def _observer_integrity(
    *, source: str, code: str, target_date: date
) -> ObservationFact:
    return _observer_fact(
        key=f"heartbeat:integrity:{source}",
        code=f"heartbeat_integrity_error:{code}",
        state="current",
        target_date=target_date,
        refs=(source,),
        counts={"integrity_errors": 1},
    )


def _dedupe_observer_facts(facts: list[ObservationFact]) -> tuple[ObservationFact, ...]:
    selected: dict[str, ObservationFact] = {}
    for fact in facts:
        previous = selected.get(fact.key)
        if previous is None:
            selected[fact.key] = fact
            continue
        rank = _OBSERVER_STATE_RANK[fact.state]
        previous_rank = _OBSERVER_STATE_RANK[previous.state]
        if rank > previous_rank or (
            rank == previous_rank and fact.code < previous.code
        ):
            selected[fact.key] = fact
    return tuple(sorted(selected.values(), key=lambda item: item.key))


def _heartbeat_effect_fact(
    record: EffectRecord,
    *,
    history: tuple[EffectRecord, ...],
    target_date: date,
    now: datetime,
) -> ObservationFact:
    state = record.state
    event_time = record.created_at
    if state == "verified" and record.receipt is not None:
        event_time = record.receipt.observed_at
    elif state == "expired":
        event_time = record.expires_at
    elif state in {"pending", "executed_unverified"} and record.expires_at < now:
        state = "expired"
        event_time = record.expires_at

    state_codes = {
        "intent": "intent",
        "pending": "pending",
        "executed_unverified": "executed_unverified",
        "expired": "expired",
        "failed": "failed",
        "requeued": "requeued",
        "verified": "verified",
    }
    safe_state = state_codes.get(state, "integrity")
    fact_state = "neutral" if state == "verified" else "current"
    recovery: RecoveryEvidence | None = None
    if state == "verified":
        previous_states = {item.state for item in history[:-1]}
        if (
            previous_states
            & {
                "pending",
                "executed_unverified",
                "expired",
                "failed",
                "requeued",
            }
            and record.receipt is not None
        ):
            fact_state = "recovered_history"
            recovery = RecoveryEvidence(
                ref=record.receipt.receipt_id,
                code="heartbeat_delivery_verified",
                recovered_at=record.receipt.observed_at,
            )

    refs = [
        f"effect:{record.effect_id}",
        f"kind:{record.kind}",
        f"source:{record.source_event_id}",
        f"idempotency:{record.idempotency_key}",
        f"sha256:{record.content_sha256}",
    ]
    if record.receipt is not None:
        refs.append(f"receipt:{record.receipt.receipt_id}")
    return _observer_fact(
        key=f"heartbeat:effect:{record.kind.removeprefix('heartbeat_')}:{record.effect_id}",
        code=f"heartbeat_effect_{safe_state}",
        state=fact_state,
        target_date=target_date,
        event_time=event_time,
        refs=tuple(refs),
        counts={
            "effects": 1,
            "attempt": record.attempt,
            "content_length": record.content_length,
            f"state_{safe_state}": 1,
        },
        recovery=recovery,
    )


def cadence_observer_status(
    *,
    path: Path,
    anchor_epoch: Callable[[datetime], str],
    ensure_aware: Callable[[datetime, str], datetime],
    read_state: Callable[[Path], tuple[Mapping[str, Any] | None, str | None]],
    parse_observer_time: Callable[[Any, str], datetime | None],
    normalise_daily_anchor_state: Callable[
        [Mapping[str, Any]], tuple[dict[str, str], str | None, bool]
    ],
    silence_receipt_match: Callable[[str], object | None],
    silence_receipt_max: int,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Project cadence state without normalising, pruning, or locking."""

    if type(target_date) is not date:
        raise TypeError("target_date must be a date")
    ensure_aware(now, "now")
    raw, integrity = read_state(path)
    if raw is None:
        if integrity is None:
            return ()
        return (
            _observer_integrity(
                source=path.name, code=integrity, target_date=target_date
            ),
        )

    try:

        def timestamp(name: str, *aliases: str) -> datetime | None:
            values = [
                parse_observer_time(raw[key], name)
                for key in (name, *aliases)
                if key in raw and raw[key] is not None
            ]
            if len({value.isoformat() for value in values}) > 1:
                raise ValueError(f"conflicting {name}")
            return values[0] if values else None

        last_judge = timestamp("last_judge_at")
        next_judge = timestamp("next_judge_at")
        last_effect = timestamp("last_effect_at")
        timestamp("automatic_cooldown_until", "auto_until")
        timestamp("manual_cooldown_until", "manual_until")
        timestamp("private_contact_overflow_until")
        timestamp("verified_visible_overflow_until")
        timestamp("last_private_contact_at")
        timestamp("last_verified_visible_contact_at")
        silence_last_completed = timestamp("silence_backoff_last_completed_at")
        silence_processed = raw.get("silence_backoff_processed_receipts", {})
        if not isinstance(silence_processed, Mapping):
            raise ValueError("silence_backoff_processed_receipts is invalid")
        for receipt_id, status in silence_processed.items():
            if (
                type(receipt_id) is not str
                or silence_receipt_match(receipt_id) is None
                or type(status) is not str
                or not status.strip()
                or len(status) > 64
            ):
                raise ValueError("silence backoff processed receipt is invalid")
        silence_streak = raw.get("silence_backoff_streak", 0)
        if (
            type(silence_streak) is not int
            or not 0 <= silence_streak <= silence_receipt_max
        ):
            raise ValueError("silence backoff streak is invalid")

        anchor_epochs, legacy_epoch, legacy_state = normalise_daily_anchor_state(raw)
        completed_present = "daily_anchor_completed" in raw
        completed = raw.get("daily_anchor_completed", False)
        epoch = raw.get("daily_anchor_epoch")

        contact_maps: dict[str, dict[str, datetime]] = {}
        for name in ("private_contacts", "verified_visible_contacts"):
            value = raw.get(name, {})
            if not isinstance(value, Mapping):
                raise ValueError(f"{name} is invalid")
            selected: dict[str, datetime] = {}
            for source, observed in value.items():
                if type(source) is not str or not source.strip():
                    raise ValueError(f"{name} source is invalid")
                parsed = parse_observer_time(observed, f"{name}.{source}")
                if parsed is None:
                    raise ValueError(f"{name} timestamp is invalid")
                selected[source] = parsed
            contact_maps[name] = selected

        effect_terminals = raw.get("effect_terminals", {})
        if not isinstance(effect_terminals, Mapping):
            raise ValueError("effect_terminals is invalid")
        terminals: dict[str, str] = {}
        for effect_id, terminal in effect_terminals.items():
            if (
                type(effect_id) is not str
                or not effect_id.strip()
                or type(terminal) is not str
                or not terminal.strip()
            ):
                raise ValueError("effect terminal is invalid")
            terminals[effect_id] = terminal

        effect_refs = raw.get("effect_refs", {})
        if not isinstance(effect_refs, Mapping):
            raise ValueError("effect_refs is invalid")
        kind_by_effect: dict[str, str] = {}
        for ref_key, effect_id in effect_refs.items():
            if (
                type(ref_key) is not str
                or not ref_key.strip()
                or type(effect_id) is not str
                or not effect_id.strip()
            ):
                raise ValueError("effect reference is invalid")
            kind, separator, _source = ref_key.partition(":")
            if separator and kind in {"heartbeat_delivery", "heartbeat_wake"}:
                previous = kind_by_effect.get(effect_id)
                if previous is not None and previous != kind:
                    raise ValueError("conflicting heartbeat effect reference kinds")
                kind_by_effect[effect_id] = kind

        state_evidence_fields = (
            "auto_until",
            "manual_until",
            "automatic_cooldown_until",
            "manual_cooldown_until",
            "last_judge_at",
            "next_judge_at",
            "last_effect_at",
            "daily_anchor_epoch",
            "daily_anchor_completed",
            "daily_anchor_epochs",
            "daily_anchor_legacy_epoch",
            "private_contacts",
            "verified_visible_contacts",
            "effect_terminals",
            "effect_refs",
            "private_contact_bloom",
            "verified_visible_bloom",
            "last_private_contact_at",
            "last_verified_visible_contact_at",
            "silence_backoff_processed_receipts",
            "silence_backoff_streak",
            "silence_backoff_last_completed_at",
        )
        has_state_evidence = any(
            raw.get(key) not in (None, {}, False) for key in state_evidence_fields
        )
        facts: list[ObservationFact] = [
            _observer_fact(
                key="heartbeat:cadence:state",
                code=(
                    "heartbeat_cadence_observed"
                    if has_state_evidence
                    else "heartbeat_cadence_uninitialized"
                ),
                state="neutral",
                target_date=target_date,
                refs=(path.name,),
                counts={
                    "cadence_state": 1,
                    "initialized": int(has_state_evidence),
                },
            )
        ]
        if silence_processed or silence_streak or silence_last_completed is not None:
            active_until = timestamp("automatic_cooldown_until", "auto_until")
            is_active = active_until is not None and active_until > now
            facts.append(
                _observer_fact(
                    key="heartbeat:silence_backoff",
                    code=(
                        "heartbeat_silence_backoff_active"
                        if is_active
                        else "heartbeat_silence_backoff_observed"
                    ),
                    state="current" if is_active else "neutral",
                    target_date=target_date,
                    event_time=silence_last_completed,
                    refs=(path.name,),
                    counts={
                        "processed_receipts": len(silence_processed),
                        "streak": silence_streak,
                        "active": int(is_active),
                    },
                )
            )
        if last_judge is not None:
            facts.append(
                _observer_fact(
                    key="heartbeat:judge:last",
                    code="heartbeat_last_judge",
                    state="neutral",
                    target_date=target_date,
                    event_time=last_judge,
                    refs=(path.name,),
                    counts={"judge_events": 1},
                )
            )
        if next_judge is not None:
            facts.append(
                _observer_fact(
                    key="heartbeat:judge:next",
                    code="heartbeat_next_judge",
                    state="neutral",
                    target_date=target_date,
                    event_time=next_judge,
                    refs=(path.name,),
                    counts={"judge_schedule_entries": 1},
                )
            )
        if legacy_state and epoch is not None:
            target_epoch = target_date.isoformat()
            current_epoch = anchor_epoch(now)
            if not completed_present:
                anchor_code = "heartbeat_anchor_observed"
                anchor_state = "neutral"
                anchor_refs = (f"anchor:{epoch}", "completion:uninitialized")
                anchor_counts = {"anchor_entries": 1, "uninitialized": 1}
            elif epoch != target_epoch:
                anchor_code = "heartbeat_anchor_outside_target"
                anchor_state = "neutral"
                anchor_refs = (
                    f"anchor:{epoch}",
                    f"target:{target_epoch}",
                    f"current:{current_epoch}",
                )
                anchor_counts = {"anchor_entries": 1, "outside_target": 1}
            elif epoch != current_epoch:
                anchor_code = "heartbeat_anchor_stale_observed"
                anchor_state = "neutral"
                anchor_refs = (
                    f"anchor:{epoch}",
                    f"target:{target_epoch}",
                    f"current:{current_epoch}",
                )
                anchor_counts = {"anchor_entries": 1, "stale": 1}
            else:
                anchor_code = (
                    "heartbeat_anchor_completed"
                    if completed
                    else "heartbeat_anchor_pending"
                )
                anchor_state = "neutral" if completed else "current"
                anchor_refs = (f"anchor:{epoch}",)
                anchor_counts = {"anchor_entries": 1}
            anchor_refs = (*anchor_refs, "migration:legacy")
            anchor_counts = {**anchor_counts, "legacy": 1}
            facts.append(
                _observer_fact(
                    key="heartbeat:anchor",
                    code=anchor_code,
                    state=anchor_state,
                    target_date=target_date,
                    refs=anchor_refs,
                    counts=anchor_counts,
                )
            )
        elif anchor_epochs or legacy_epoch is not None:
            target_epoch = target_date.isoformat()
            current_epoch = anchor_epoch(now)

            def append_anchor_fact(
                kind: str, anchor_epoch: str, *, legacy: bool = False
            ) -> None:
                if anchor_epoch != target_epoch:
                    anchor_code = "heartbeat_anchor_outside_target"
                    anchor_state = "neutral"
                    anchor_refs = (
                        f"anchor:{anchor_epoch}",
                        f"target:{target_epoch}",
                        f"current:{current_epoch}",
                    )
                    anchor_counts = {"anchor_entries": 1, "outside_target": 1}
                elif anchor_epoch != current_epoch:
                    anchor_code = "heartbeat_anchor_stale_observed"
                    anchor_state = "neutral"
                    anchor_refs = (
                        f"anchor:{anchor_epoch}",
                        f"target:{target_epoch}",
                        f"current:{current_epoch}",
                    )
                    anchor_counts = {"anchor_entries": 1, "stale": 1}
                else:
                    anchor_code = (
                        "heartbeat_anchor_legacy_completed"
                        if legacy
                        else "heartbeat_anchor_completed"
                    )
                    anchor_state = "neutral"
                    anchor_refs = (
                        f"anchor:{anchor_epoch}",
                        f"kind:{kind}",
                        "completion:legacy" if legacy else "completion:exact",
                    )
                    anchor_counts = {
                        "anchor_entries": 1,
                        "legacy": int(legacy),
                    }
                facts.append(
                    _observer_fact(
                        key=f"heartbeat:anchor:{kind}",
                        code=anchor_code,
                        state=anchor_state,
                        target_date=target_date,
                        refs=anchor_refs,
                        counts=anchor_counts,
                    )
                )

            if legacy_epoch is not None:
                append_anchor_fact("legacy", legacy_epoch, legacy=True)
            for kind, anchor_epoch in sorted(anchor_epochs.items()):
                append_anchor_fact(kind, anchor_epoch)

        for name, code, key in (
            (
                "private_contacts",
                "heartbeat_contact_private",
                "heartbeat:contact:private",
            ),
            (
                "verified_visible_contacts",
                "heartbeat_contact_verified_visible",
                "heartbeat:contact:verified_visible",
            ),
        ):
            contacts = contact_maps[name]
            if not contacts:
                continue
            latest_source, latest_at = max(contacts.items(), key=lambda item: item[1])
            source_refs = tuple(sorted(contacts))[:32]
            facts.append(
                _observer_fact(
                    key=key,
                    code=code,
                    state="neutral",
                    target_date=target_date,
                    event_time=latest_at,
                    refs=(f"source:{latest_source}", *source_refs),
                    counts={
                        "contacts": len(contacts),
                        "source_refs": len(source_refs),
                    },
                )
            )

        terminal_code = {
            "pending": "pending",
            "executed_unverified": "executed_unverified",
            "expired": "expired",
            "failed": "failed",
            "requeued": "requeued",
            "verified": "verified",
        }
        for effect_id, terminal in terminals.items():
            kind = kind_by_effect.get(effect_id)
            if kind is None:
                continue
            safe_terminal = terminal_code.get(terminal, "unknown")
            is_current = safe_terminal in {
                "pending",
                "executed_unverified",
                "expired",
                "failed",
                "requeued",
                "unknown",
            }
            facts.append(
                _observer_fact(
                    key=f"heartbeat:terminal:{kind}:{effect_id}",
                    code=f"heartbeat_{kind}_{safe_terminal}",
                    state="current" if is_current else "neutral",
                    target_date=target_date,
                    event_time=last_effect,
                    refs=(f"effect:{effect_id}", f"kind:{kind}"),
                    counts={"terminals": 1},
                )
            )
        return _dedupe_observer_facts(facts)
    except Exception as exc:
        return (
            _observer_integrity(
                source=path.name,
                code=f"state_{type(exc).__name__}",
                target_date=target_date,
            ),
        )


def engine_observer_status(
    *,
    cadence: object,
    effect_ledger: object,
    target_date: date,
    now: datetime,
) -> tuple[ObservationFact, ...]:
    """Project cadence and effect evidence without executing Heartbeat."""

    facts: list[ObservationFact] = []
    cadence_port = getattr(cadence, "observer_status", None)
    if callable(cadence_port):
        try:
            candidate_facts = cadence_port(target_date=target_date, now=now)
            if isinstance(candidate_facts, (str, bytes, bytearray, Mapping)):
                raise TypeError("cadence observer result must be an iterable of facts")
            if not isinstance(candidate_facts, Iterable):
                raise TypeError("cadence observer result must be an iterable of facts")
            candidate_facts = tuple(candidate_facts)
            if any(not isinstance(fact, ObservationFact) for fact in candidate_facts):
                raise TypeError("cadence observer result contains a malformed fact")
            facts.extend(candidate_facts)
        except Exception as exc:
            try:
                path = getattr(cadence, "path", None)
            except Exception:
                path = None
            source = path.name if isinstance(path, Path) else "cadence"
            facts.append(
                _observer_integrity(
                    source=source,
                    code=f"port_{type(exc).__name__}",
                    target_date=target_date,
                )
            )
    else:
        facts.append(
            _observer_fact(
                key="heartbeat:cadence:observer",
                code="heartbeat_cadence_observer_unavailable",
                state="neutral",
                target_date=target_date,
                refs=("cadence",),
                counts={"observer_unavailable": 1},
            )
        )

    ledger_path: Path | None = None
    ledger_file = getattr(getattr(effect_ledger, "ledger", None), "path", None)
    if isinstance(ledger_file, Path):
        ledger_path = ledger_file
    if ledger_path is None:
        cadence_path = getattr(cadence, "path", None)
        if isinstance(cadence_path, Path):
            ledger_path = cadence_path.parent / "effects.jsonl"
    if ledger_path is None:
        return _dedupe_observer_facts(facts)

    records, integrity = _read_effect_history_lock_free(ledger_path)
    if integrity is not None:
        facts.append(
            _observer_integrity(
                source=ledger_path.name,
                code=integrity,
                target_date=target_date,
            )
        )
        return _dedupe_observer_facts(facts)

    by_effect: dict[str, list[EffectRecord]] = {}
    for record in records:
        if record.kind in {"heartbeat_delivery", "heartbeat_wake"}:
            by_effect.setdefault(record.effect_id, []).append(record)
    visible_candidates: list[ObservationFact] = []
    for history in by_effect.values():
        current = history[-1]
        same_key = [
            item
            for item in records
            if item.kind == current.kind
            and item.idempotency_key == current.idempotency_key
        ]
        facts.append(
            _heartbeat_effect_fact(
                current,
                history=tuple(same_key),
                target_date=target_date,
                now=now,
            )
        )
        if (
            current.kind == "heartbeat_delivery"
            and current.state == "verified"
            and current.receipt is not None
        ):
            visible_candidates.append(
                _observer_fact(
                    key="heartbeat:contact:verified_visible",
                    code="heartbeat_contact_verified_visible",
                    state="neutral",
                    target_date=target_date,
                    event_time=current.receipt.observed_at,
                    refs=(
                        f"effect:{current.effect_id}",
                        f"receipt:{current.receipt.receipt_id}",
                        "contact:verified_visible",
                    ),
                    counts={"verified_visible_contacts": 1},
                )
            )
    facts.extend(visible_candidates)
    return _dedupe_observer_facts(facts)
