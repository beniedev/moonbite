"""Read-only health observation primitives for Moonbite.

The observer deliberately only consumes caller-owned source and schedule
ports.  It never opens files, imports host adapters, or mutates durable state.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

OBSERVER_SCHEMA = "moon.observer.v1"
_STATES = frozenset({"current", "recovered_history", "neutral"})
_OUTCOMES = frozenset({"observed", "missed"})
_FORBIDDEN_KEYS = frozenset(
    {"body", "message", "content", "summary", "payload", "value", "output"}
)
_MAX_TEXT_BYTES = 1024
_STATE_RANK = {"neutral": 0, "recovered_history": 1, "current": 2}


def _reject_forbidden(value: Any) -> None:
    """Reject private/source-material-shaped mapping keys recursively."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in _FORBIDDEN_KEYS:
                raise ValueError(f"forbidden mapping key: {key}")
            _reject_forbidden(key)
            _reject_forbidden(child)
        return
    if isinstance(value, (str, bytes, bytearray)):
        return
    if isinstance(value, Iterable):
        for child in value:
            _reject_forbidden(child)


def _text(value: Any, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    if len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
        raise ValueError(f"{label} exceeds {_MAX_TEXT_BYTES} bytes")
    return value


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception as exc:  # pragma: no cover - hostile tzinfo implementation
        raise ValueError(f"{label} must be timezone-aware") from exc
    if offset is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value


def _target_date(value: Any, label: str = "target_date") -> date:
    if type(value) is not date:
        raise ValueError(f"{label} must be a date")
    return value


def _refs(value: Any, label: str = "refs") -> tuple[str, ...]:
    if type(value) not in (tuple, list):
        raise ValueError(f"{label} must be a built-in tuple or list")
    items = value
    checked = {_text(item, f"{label} item") for item in items}
    return tuple(sorted(checked))


def _counts(value: Any, label: str = "counts") -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    result: dict[str, int] = {}
    for key, count in value.items():
        _text(key, f"{label} key")
        if key in _FORBIDDEN_KEYS:
            raise ValueError(f"forbidden mapping key: {key}")
        if type(count) is not int or count < 0:
            raise ValueError(f"{label} values must be non-negative integers")
        result[key] = count
    return dict(sorted(result.items()))


def _optional_aware(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return _aware(value, label)


def _iso(value: datetime | date | None) -> str | None:
    return None if value is None else value.isoformat()


@dataclass(frozen=True, slots=True)
class RecoveryEvidence:
    ref: str
    code: str
    recovered_at: datetime

    def __post_init__(self) -> None:
        _text(self.ref, "recovery ref")
        _text(self.code, "recovery code")
        _aware(self.recovered_at, "recovered_at")

    def to_dict(self) -> dict[str, Any]:
        result = {
            "ref": self.ref,
            "code": self.code,
            "recovered_at": _iso(self.recovered_at),
        }
        _reject_forbidden(result)
        return result


@dataclass(frozen=True, slots=True)
class ObservationFact:
    key: str
    code: str
    state: str
    target_date: date | None = None
    event_time: datetime | None = None
    refs: tuple[str, ...] = ()
    counts: dict[str, int] = field(default_factory=dict)
    recovery: RecoveryEvidence | None = None

    def __post_init__(self) -> None:
        _text(self.key, "fact key")
        _text(self.code, "fact code")
        if self.state not in _STATES:
            raise ValueError(
                "fact state must be current, recovered_history, or neutral"
            )
        if self.target_date is not None:
            _target_date(self.target_date)
        _optional_aware(self.event_time, "event_time")
        object.__setattr__(self, "refs", _refs(self.refs))
        object.__setattr__(self, "counts", _counts(self.counts))
        if self.recovery is not None and not isinstance(
            self.recovery, RecoveryEvidence
        ):
            raise TypeError("recovery must be RecoveryEvidence or None")
        if self.state == "recovered_history" and self.recovery is None:
            raise ValueError("recovered_history facts require recovery evidence")
        if self.state != "recovered_history" and self.recovery is not None:
            raise ValueError("only recovered_history facts may carry recovery evidence")

    def to_dict(self) -> dict[str, Any]:
        result = {
            "key": self.key,
            "code": self.code,
            "state": self.state,
            "target_date": _iso(self.target_date),
            "event_time": _iso(self.event_time),
            "refs": list(self.refs),
            "counts": dict(self.counts),
            "recovery": None if self.recovery is None else self.recovery.to_dict(),
        }
        _reject_forbidden(result)
        return result


@dataclass(frozen=True, slots=True)
class ScheduleOccurrence:
    ref: str
    expected_at: datetime
    outcome: str
    event_ref: str | None = None
    event_time: datetime | None = None

    def __post_init__(self) -> None:
        _text(self.ref, "occurrence ref")
        _aware(self.expected_at, "expected_at")
        if self.outcome not in _OUTCOMES:
            raise ValueError("outcome must be observed or missed")
        if self.outcome == "observed":
            if self.event_ref is None or self.event_time is None:
                raise ValueError(
                    "observed occurrences require event_ref and event_time"
                )
        elif self.event_ref is not None or self.event_time is not None:
            raise ValueError("missed occurrences forbid event_ref and event_time")
        if self.event_ref is not None:
            _text(self.event_ref, "event_ref")
        _optional_aware(self.event_time, "event_time")

    def to_dict(self) -> dict[str, Any]:
        result = {
            "ref": self.ref,
            "expected_at": _iso(self.expected_at),
            "outcome": self.outcome,
            "event_ref": self.event_ref,
            "event_time": _iso(self.event_time),
        }
        _reject_forbidden(result)
        return result


@dataclass(frozen=True, slots=True)
class ScheduleProof:
    ref: str
    target_date: date
    occurrences: tuple[ScheduleOccurrence, ...] = ()

    def __post_init__(self) -> None:
        _text(self.ref, "schedule proof ref")
        _target_date(self.target_date)
        try:
            occurrences = tuple(self.occurrences)
        except TypeError as exc:
            raise ValueError("occurrences must be an iterable") from exc
        if any(not isinstance(item, ScheduleOccurrence) for item in occurrences):
            raise TypeError("occurrences must contain ScheduleOccurrence values")
        occurrence_refs = [item.ref for item in occurrences]
        if len(occurrence_refs) != len(set(occurrence_refs)):
            raise ValueError("schedule proof occurrence refs must be unique")
        object.__setattr__(self, "occurrences", occurrences)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "ref": self.ref,
            "target_date": self.target_date.isoformat(),
            "occurrences": [item.to_dict() for item in self.occurrences],
        }
        _reject_forbidden(result)
        return result


@dataclass(frozen=True, slots=True, kw_only=True)
class HealthSnapshot:
    schema_version: str = OBSERVER_SCHEMA
    observed_at: datetime
    target_date: date
    state: str
    schedule_known: bool
    facts: tuple[ObservationFact, ...]
    counts: dict[str, int]
    codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != OBSERVER_SCHEMA:
            raise ValueError(f"schema_version must be {OBSERVER_SCHEMA!r}")
        _aware(self.observed_at, "observed_at")
        _target_date(self.target_date)
        if self.state not in _STATES:
            raise ValueError(
                "snapshot state must be current, recovered_history, or neutral"
            )
        if type(self.schedule_known) is not bool:
            raise TypeError("schedule_known must be a bool")
        try:
            facts = tuple(self.facts)
        except TypeError as exc:
            raise ValueError("facts must be an iterable") from exc
        if any(not isinstance(item, ObservationFact) for item in facts):
            raise TypeError("facts must contain ObservationFact values")
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "counts", _counts(self.counts, "snapshot counts"))
        object.__setattr__(self, "codes", _refs(self.codes, "codes"))

    def to_dict(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            "observed_at": self.observed_at.isoformat(),
            "target_date": self.target_date.isoformat(),
            "state": self.state,
            "schedule_known": self.schedule_known,
            "facts": [item.to_dict() for item in self.facts],
            "counts": dict(self.counts),
            "codes": list(self.codes),
        }
        _reject_forbidden(result)
        return result


def _fact_sort_key(fact: ObservationFact) -> tuple[Any, ...]:
    return (
        fact.key,
        -_STATE_RANK[fact.state],
        fact.code,
        "" if fact.target_date is None else fact.target_date.isoformat(),
        "" if fact.event_time is None else fact.event_time.isoformat(),
        fact.refs,
        tuple(fact.counts.items()),
        "" if fact.recovery is None else fact.recovery.ref,
        "" if fact.recovery is None else fact.recovery.code,
    )


def _dedupe_facts(facts: Iterable[ObservationFact]) -> tuple[ObservationFact, ...]:
    selected: dict[str, ObservationFact] = {}
    for fact in facts:
        previous = selected.get(fact.key)
        if previous is None:
            selected[fact.key] = fact
            continue
        candidate_rank = _STATE_RANK[fact.state]
        previous_rank = _STATE_RANK[previous.state]
        if candidate_rank > previous_rank or (
            candidate_rank == previous_rank
            and _fact_sort_key(fact) < _fact_sort_key(previous)
        ):
            selected[fact.key] = fact
    return tuple(sorted(selected.values(), key=_fact_sort_key))


def _snapshot_counts(facts: Iterable[ObservationFact]) -> dict[str, int]:
    facts = tuple(facts)
    result: dict[str, int] = {
        "facts_total": len(facts),
        "current": sum(fact.state == "current" for fact in facts),
        "recovered_history": sum(fact.state == "recovered_history" for fact in facts),
        "neutral": sum(fact.state == "neutral" for fact in facts),
    }
    for fact in facts:
        for key, count in fact.counts.items():
            result[key] = result.get(key, 0) + count
    return dict(sorted(result.items()))


def _source_error(
    source_name: str, exc: Exception, target_date: date
) -> ObservationFact:
    exception_type = type(exc).__name__
    code = f"source_integrity_error:{exception_type}"
    _text(code, "source error code")
    return ObservationFact(
        key=f"source:{source_name}",
        code=code,
        state="current",
        target_date=target_date,
        refs=(source_name,),
    )


def _schedule_error(target_date: date) -> ObservationFact:
    return ObservationFact(
        key="schedule:proof",
        code="schedule_proof_invalid",
        state="current",
        target_date=target_date,
    )


def _schedule_facts(
    proof: Any, target_date: date
) -> tuple[bool, tuple[ObservationFact, ...]]:
    if proof is None:
        return False, ()
    try:
        if not isinstance(proof, ScheduleProof) or proof.target_date != target_date:
            raise ValueError("schedule proof does not match target date")
        facts = []
        for occurrence in proof.occurrences:
            if occurrence.outcome == "observed":
                fact = ObservationFact(
                    key=f"schedule:occurrence:{occurrence.ref}",
                    code="schedule_observed",
                    state="neutral",
                    target_date=target_date,
                    event_time=occurrence.event_time,
                    refs=(proof.ref, occurrence.ref, occurrence.event_ref),
                )
            elif occurrence.outcome == "missed":
                fact = ObservationFact(
                    key=f"schedule:occurrence:{occurrence.ref}",
                    code="schedule_missed",
                    state="current",
                    target_date=target_date,
                    event_time=occurrence.expected_at,
                    refs=(proof.ref, occurrence.ref),
                )
            else:  # defensive if a hostile object bypassed dataclass validation
                raise ValueError("unsupported schedule outcome")
            facts.append(fact)
        return True, tuple(facts)
    except Exception:  # noqa: BLE001 - malformed proof must fail closed
        return False, (_schedule_error(target_date),)


class Observer:
    """Collect source facts and optional schedule proof without side effects."""

    def __init__(self, sources: Mapping[str, Any] | None = None) -> None:
        if sources is None:
            sources = {}
        if not isinstance(sources, Mapping):
            raise TypeError("sources must be a mapping")
        checked: dict[str, Any] = {}
        for name, source in sources.items():
            checked_name = _text(name, "source name")
            if checked_name in _FORBIDDEN_KEYS:
                raise ValueError(f"forbidden mapping key: {checked_name}")
            checked[checked_name] = source
        self.sources = dict(sorted(checked.items()))

    def _source_facts(
        self, source_name: str, source: Any, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        try:
            callback = (
                source if callable(source) else getattr(source, "observer_status", None)
            )
            if not callable(callback):
                raise TypeError("source is not callable")
            result = callback(target_date=target_date, now=now)
            if isinstance(result, (str, bytes, bytearray, Mapping)):
                raise TypeError("source result must be an iterable of facts")
            if not isinstance(result, Iterable):
                raise TypeError("source result must be an iterable of facts")
            facts = tuple(result)
            if any(not isinstance(item, ObservationFact) for item in facts):
                raise TypeError("source result contains a malformed fact")
            return facts
        except Exception as exc:  # noqa: BLE001 - source errors become facts
            return (_source_error(source_name, exc, target_date),)

    def snapshot(
        self,
        target_date: date,
        now: datetime,
        schedule_proof: ScheduleProof | None = None,
    ) -> HealthSnapshot:
        target_date = _target_date(target_date)
        now = _aware(now, "now")
        facts: list[ObservationFact] = []
        for source_name, source in self.sources.items():
            facts.extend(
                self._source_facts(
                    source_name, source, target_date=target_date, now=now
                )
            )
        schedule_known, schedule_facts = _schedule_facts(schedule_proof, target_date)
        facts.extend(schedule_facts)
        selected = _dedupe_facts(facts)
        state = max(
            (fact.state for fact in selected),
            key=_STATE_RANK.__getitem__,
            default="neutral",
        )
        counts = _snapshot_counts(selected)
        codes = tuple(sorted({fact.code for fact in selected}))
        return HealthSnapshot(
            observed_at=now,
            target_date=target_date,
            state=state,
            schedule_known=schedule_known,
            facts=selected,
            counts=counts,
            codes=codes,
        )


__all__ = [
    "OBSERVER_SCHEMA",
    "HealthSnapshot",
    "ObservationFact",
    "Observer",
    "RecoveryEvidence",
    "ScheduleOccurrence",
    "ScheduleProof",
]
