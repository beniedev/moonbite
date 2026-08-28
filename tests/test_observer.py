from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta

import pytest

from moonbite_plugin.observer import (
    OBSERVER_SCHEMA,
    HealthSnapshot,
    ObservationFact,
    Observer,
    RecoveryEvidence,
    ScheduleOccurrence,
    ScheduleProof,
)

DAY = date(2026, 8, 24)
NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def _fact(key: str, code: str, state: str = "neutral", **kwargs) -> ObservationFact:
    return ObservationFact(key=key, code=code, state=state, **kwargs)


def test_dataclasses_require_aware_times_and_bounded_nonempty_refs():
    with pytest.raises(ValueError):
        RecoveryEvidence("", "recovered", NOW)
    with pytest.raises(ValueError):
        RecoveryEvidence("receipt", "recovered", NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        ScheduleOccurrence("run", NOW, "observed")
    with pytest.raises(ValueError):
        ObservationFact("key", "code", "neutral", event_time=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        ObservationFact("x" * 1025, "code", "neutral")


def test_counts_are_copied_and_nonnegative_integers():
    counts = {"runs": 2}
    fact = _fact("a", "ok", counts=counts)
    counts["runs"] = 99
    assert fact.counts == {"runs": 2}
    with pytest.raises(ValueError):
        _fact("a", "ok", counts={"runs": -1})
    with pytest.raises(ValueError):
        _fact("a", "ok", counts={"runs": True})


def test_refs_accept_only_builtin_sequences_and_normalize_them():
    class HostileMapping(Mapping[str, object]):
        def __init__(self):
            self.touches = []

        def __getitem__(self, key):
            self.touches.append("getitem")
            raise AssertionError("mapping ref was read")

        def __iter__(self):
            self.touches.append("iter")
            raise AssertionError("mapping refs were iterated")

        def __len__(self):
            self.touches.append("len")
            raise AssertionError("mapping ref length was inspected")

    class HostileIterable:
        def __iter__(self):
            raise AssertionError("custom refs were iterated")

    hostile_mapping = HostileMapping()
    for refs in (hostile_mapping, HostileIterable(), iter(("generator",))):
        with pytest.raises(ValueError):
            _fact("refs", "ok", refs=refs)
    assert hostile_mapping.touches == []

    assert _fact("refs", "ok", refs=["z", "a", "z"]).refs == ("a", "z")
    with pytest.raises(ValueError):
        _fact("refs", "ok", refs={"mapping-key": 1})


def test_cyclic_counts_fail_closed_without_recursive_forbidden_scan():
    counts = {}
    counts["cycle"] = counts
    with pytest.raises(ValueError):
        _fact("counts", "ok", counts=counts)


def test_recovered_history_requires_evidence_and_current_cannot_fake_it():
    evidence = RecoveryEvidence("receipt-1", "verified", NOW)
    recovered = _fact("incident", "down", "recovered_history", recovery=evidence)
    assert recovered.to_dict()["recovery"] == {
        "ref": "receipt-1",
        "code": "verified",
        "recovered_at": NOW.isoformat(),
    }
    with pytest.raises(ValueError):
        _fact("incident", "down", "recovered_history")
    with pytest.raises(ValueError):
        _fact("incident", "down", "current", recovery=evidence)
    with pytest.raises(FrozenInstanceError):
        evidence.ref = "other"  # type: ignore[misc]


def test_empty_observer_is_neutral_and_schedule_is_unknown():
    snapshot = Observer().snapshot(DAY, NOW)
    assert snapshot.state == "neutral"
    assert snapshot.schedule_known is False
    assert snapshot.facts == ()
    assert snapshot.counts == {
        "current": 0,
        "facts_total": 0,
        "neutral": 0,
        "recovered_history": 0,
    }
    assert snapshot.codes == ()
    assert OBSERVER_SCHEMA == "moon.observer.v1"
    assert snapshot.to_dict()["schema_version"] == OBSERVER_SCHEMA


def test_health_snapshot_rejects_unknown_schema_version():
    with pytest.raises(ValueError):
        HealthSnapshot(
            schema_version="moon.observer.v0",
            observed_at=NOW,
            target_date=DAY,
            state="neutral",
            schedule_known=False,
            facts=(),
            counts={},
            codes=(),
        )


def test_callable_and_object_sources_receive_requested_context():
    seen = []

    def callable_source(*, target_date, now):
        seen.append((target_date, now))
        return [_fact("callable", "ready", counts={"checks": 2})]

    class ObjectSource:
        def observer_status(self, *, target_date, now):
            seen.append((target_date, now))
            return [_fact("object", "ready")]

    snapshot = Observer(
        sources={"call": callable_source, "object": ObjectSource()}
    ).snapshot(DAY, NOW)
    assert seen == [(DAY, NOW), (DAY, NOW)]
    assert [fact.key for fact in snapshot.facts] == ["callable", "object"]
    assert snapshot.counts == {
        "checks": 2,
        "current": 0,
        "facts_total": 2,
        "neutral": 2,
        "recovered_history": 0,
    }


def test_mapping_source_owner_is_opaque_except_for_observer_status_port():
    class HostileOwner(Mapping[str, object]):
        def __init__(self):
            self.touches = []
            self.observer_status_calls = 0

        def __getitem__(self, key):
            self.touches.append("getitem")
            raise AssertionError("source owner value was read")

        def __iter__(self):
            self.touches.append("iter")
            raise AssertionError("source owner value was iterated")

        def __len__(self):
            self.touches.append("len")
            raise AssertionError("source owner length was inspected")

        def items(self):
            self.touches.append("items")
            raise AssertionError("source owner items were inspected")

        def get(self, key, default=None):
            self.touches.append("get")
            raise AssertionError("source owner value was looked up")

        def observer_status(self, *, target_date, now):
            self.observer_status_calls += 1
            return [_fact("hostile-owner", "ready")]

    owner = HostileOwner()
    observer = Observer(sources={"owner": owner})
    assert owner.touches == []

    snapshot = observer.snapshot(DAY, NOW)

    assert owner.touches == []
    assert owner.observer_status_calls == 1
    assert [fact.key for fact in snapshot.facts] == ["hostile-owner"]


def test_source_exception_is_current_and_redacts_exception_text():
    def broken(*, target_date, now):
        raise RuntimeError("private body should not escape")

    snapshot = Observer(sources={"ledger": broken}).snapshot(DAY, NOW)
    fact = snapshot.facts[0]
    assert snapshot.state == "current"
    assert fact.code == "source_integrity_error:RuntimeError"
    assert fact.refs == ("ledger",)
    assert "private body" not in repr(snapshot.to_dict())


def test_malformed_source_becomes_type_specific_current_fact():
    snapshot = Observer(sources={"bad": lambda **_: {"not": "facts"}}).snapshot(
        DAY, NOW
    )
    assert snapshot.state == "current"
    assert snapshot.facts[0].code == "source_integrity_error:TypeError"
    assert snapshot.facts[0].refs == ("bad",)


def test_valid_schedule_proof_converts_observed_and_missed_occurrences():
    observed = ScheduleOccurrence(
        "run-observed", NOW + timedelta(minutes=1), "observed", "event-1", NOW
    )
    missed = ScheduleOccurrence("run-missed", NOW + timedelta(minutes=2), "missed")
    proof = ScheduleProof("schedule-1", DAY, (missed, observed))
    snapshot = Observer().snapshot(DAY, NOW, schedule_proof=proof)
    by_code = {fact.code: fact for fact in snapshot.facts}
    assert snapshot.schedule_known is True
    assert snapshot.state == "current"
    assert by_code["schedule_observed"].state == "neutral"
    assert by_code["schedule_observed"].event_time == NOW
    assert by_code["schedule_missed"].state == "current"
    assert by_code["schedule_missed"].event_time == missed.expected_at
    assert snapshot.counts == {
        "current": 1,
        "facts_total": 2,
        "neutral": 1,
        "recovered_history": 0,
    }


def test_schedule_proof_rejects_duplicate_occurrence_refs():
    first = ScheduleOccurrence("duplicate-ref", NOW + timedelta(minutes=1), "missed")
    second = ScheduleOccurrence("duplicate-ref", NOW + timedelta(minutes=2), "missed")
    with pytest.raises(ValueError):
        ScheduleProof("schedule-1", DAY, (first, second))


def test_schedule_occurrence_key_is_namespaced_from_owner_fact_key():
    occurrence = ScheduleOccurrence("shared", NOW + timedelta(minutes=1), "missed")
    proof = ScheduleProof("schedule-1", DAY, (occurrence,))
    owner_fact = _fact("shared", "owner_fact", "current")

    snapshot = Observer(sources={"owner": lambda **_: [owner_fact]}).snapshot(
        DAY, NOW, schedule_proof=proof
    )

    by_key = {fact.key: fact for fact in snapshot.facts}
    assert set(by_key) == {"shared", "schedule:occurrence:shared"}
    assert by_key["shared"] == owner_fact
    assert by_key["schedule:occurrence:shared"].refs == ("schedule-1", "shared")
    assert snapshot.counts["facts_total"] == 2


def test_schedule_occurrence_rules_reject_missing_or_forbidden_event_evidence():
    with pytest.raises(ValueError):
        ScheduleOccurrence("run", NOW, "observed", event_ref="event")
    with pytest.raises(ValueError):
        ScheduleOccurrence("run", NOW, "missed", event_ref="event")
    with pytest.raises(ValueError):
        ScheduleOccurrence("run", NOW, "missed", event_time=NOW)


def test_invalid_or_mismatched_schedule_proof_is_current():
    mismatched = ScheduleProof("schedule-1", DAY - timedelta(days=1), ())
    snapshot = Observer().snapshot(DAY, NOW, schedule_proof=mismatched)
    assert snapshot.schedule_known is False
    assert snapshot.state == "current"
    assert snapshot.codes == ("schedule_proof_invalid",)

    malformed = Observer().snapshot(DAY, NOW, schedule_proof=object())
    assert malformed.schedule_known is False
    assert malformed.codes == ("schedule_proof_invalid",)


def test_same_key_current_wins_over_recovered_history_regardless_of_source_order():
    evidence = RecoveryEvidence("receipt", "recovered", NOW)
    recovered = _fact("shared", "old", "recovered_history", recovery=evidence)
    current = _fact("shared", "new", "current")
    first = Observer(sources={"a": lambda **_: [recovered], "b": lambda **_: [current]})
    second = Observer(
        sources={"a": lambda **_: [current], "b": lambda **_: [recovered]}
    )
    one = first.snapshot(DAY, NOW)
    two = second.snapshot(DAY, NOW)
    assert one == two
    assert one.state == "current"
    assert one.facts == (current,)


def test_deterministic_sort_and_dedupe_is_independent_of_source_order():
    left = _fact("z", "same")
    right = _fact("a", "same")
    first = Observer(sources={"z": lambda **_: [left], "a": lambda **_: [right]})
    second = Observer(sources={"a": lambda **_: [right], "z": lambda **_: [left]})
    assert first.snapshot(DAY, NOW).to_dict() == second.snapshot(DAY, NOW).to_dict()
    assert [fact.key for fact in first.snapshot(DAY, NOW).facts] == ["a", "z"]


def test_forbidden_mapping_keys_are_rejected_recursively():
    with pytest.raises(ValueError):
        _fact("x", "ok", counts={"nested": {"payload": 1}})
    with pytest.raises(ValueError):
        Observer(sources={"body": lambda **_: ()})
    with pytest.raises(ValueError):
        HealthSnapshot(
            observed_at=NOW,
            target_date=DAY,
            state="neutral",
            schedule_known=False,
            facts=(),
            counts={"output": 0},
            codes=(),
        )


def test_observer_does_not_touch_tmp_path(tmp_path):
    before = sorted(
        path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")
    )
    Observer().snapshot(DAY, NOW)
    after = sorted(
        path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")
    )
    assert before == after
