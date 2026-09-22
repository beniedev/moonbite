"""Read-only helpers shared by memory orchestration observers.

This module reads existing evidence without creating owner state or locks.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..observer import ObservationFact
from ..runtime_core import StateError, as_utc


_OBSERVATION_STATE_RANK = {"neutral": 0, "recovered_history": 1, "current": 2}


def observer_validate_context(target_date: date, now: datetime) -> datetime:
    """Validate observer inputs without consulting any durable owner."""

    if type(target_date) is not date:
        raise ValueError("target_date must be a date")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    try:
        effective = as_utc(now)
    except ValueError as exc:
        raise ValueError("now must be timezone-aware") from exc
    return effective


def observer_jsonl_rows(path: Any) -> tuple[Mapping[str, Any], ...]:
    """Read an owner ledger directly, without creating or taking a lock.

    Observer probes intentionally avoid ``JsonlLedger.rows``.  That method
    acquires (and, on a pristine path, creates) a sibling lock file.  This
    helper only reads existing JSONL bytes and validates the outer object
    envelope; callers decide which content-free fields are safe to inspect.
    """

    if path is None:
        return ()
    path = Path(path)
    if not path.exists():
        return ()
    rows: list[Mapping[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for row_number, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise StateError(
                        f"observer ledger row {row_number} is not valid JSON"
                    ) from exc
                if not isinstance(value, Mapping):
                    raise StateError(
                        f"observer ledger row {row_number} is not an object"
                    )
                rows.append(value)
    except OSError as exc:
        raise StateError("observer ledger is unreadable") from exc
    return tuple(rows)


def observer_integrity_fact(
    owner: str, *, target_date: date, code: str | None = None
) -> ObservationFact:
    """Return a redacted current integrity fact for malformed owner state."""

    return ObservationFact(
        key=f"memory.{owner}.integrity",
        code=code or f"{owner}_ledger_corrupt",
        state="current",
        target_date=target_date,
        refs=(owner,),
        counts={"integrity_errors": 1},
    )


def observer_refs(*values: Any) -> tuple[str, ...]:
    """Keep only already validated, content-free reference strings."""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if type(value) is not str or not value.strip() or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def observer_merge_facts(
    facts: Iterable[ObservationFact],
) -> tuple[ObservationFact, ...]:
    """Dedupe an owner aggregate before the shared Observer sees it."""

    selected: dict[str, ObservationFact] = {}
    for fact in facts:
        previous = selected.get(fact.key)
        if previous is None:
            selected[fact.key] = fact
            continue
        candidate_rank = _OBSERVATION_STATE_RANK[fact.state]
        previous_rank = _OBSERVATION_STATE_RANK[previous.state]
        if candidate_rank > previous_rank or (
            candidate_rank == previous_rank
            and (fact.code, fact.refs, tuple(fact.counts.items()))
            < (previous.code, previous.refs, tuple(previous.counts.items()))
        ):
            selected[fact.key] = fact
    return tuple(
        sorted(
            selected.values(),
            key=lambda fact: (
                fact.key,
                -_OBSERVATION_STATE_RANK[fact.state],
                fact.code,
            ),
        )
    )


__all__ = ()
