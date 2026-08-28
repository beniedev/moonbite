from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from moonbite_plugin.observations import (
    MAX_CURSOR_EVENT_IDS,
    OBSERVATION_SCHEMA,
    ObservationCursor,
    ObservationEvidence,
    decide_observation,
)


NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
SOURCE = "fixture.source"
CAPABILITY = "activity.observation"
FIXTURE = Path(__file__).parent / "fixtures" / "observation_contract.json"


def evidence(
    event_id: str = "fixture-event-001",
    *,
    observed_at: datetime = NOW - timedelta(minutes=1),
    received_at: datetime = NOW,
    expires_at: datetime | None = NOW + timedelta(minutes=10),
    availability: str = "available",
    freshness: str = "fresh",
    confidence: float = 1.0,
    source_sequence: int | None = 1,
    reason_code: str = "observed",
) -> ObservationEvidence:
    return ObservationEvidence(
        event_id=event_id,
        source=SOURCE,
        capability=CAPABILITY,
        observed_at=observed_at,
        received_at=received_at,
        expires_at=expires_at,
        availability=availability,
        freshness=freshness,
        confidence=confidence,
        source_sequence=source_sequence,
        reason_code=reason_code,
    )


def test_fresh_evidence_is_the_only_projectable_candidate():
    item = evidence()

    decision = decide_observation(item, now=NOW)

    assert decision.status == "candidate"
    assert decision.projectable is True
    assert decision.projection == item.to_event_payload()
    assert decision.next_cursor is not None
    assert decision.next_cursor.last_event_id == item.event_id
    assert decision.next_cursor.last_source_sequence == item.source_sequence


@pytest.mark.parametrize(
    ("availability", "freshness", "reason"),
    [
        ("disabled", "unknown", "disabled"),
        ("not_configured", "unknown", "not_configured"),
    ],
)
def test_disabled_or_unconfigured_evidence_is_neutral(
    availability: str, freshness: str, reason: str
):
    decision = decide_observation(
        evidence(
            availability=availability,
            freshness=freshness,
            source_sequence=None,
        ),
        now=NOW,
    )

    assert decision.status == "neutral"
    assert decision.reason_code == reason
    assert decision.projectable is False
    assert decision.projection is None
    assert decision.health_projectable is False
    assert decision.health_projection is None
    assert decision.next_cursor is None


@pytest.mark.parametrize(
    ("availability", "freshness", "reason"),
    [
        ("offline", "unknown", "offline"),
        ("error", "unknown", "error"),
        ("unknown", "unknown", "unknown"),
        ("available", "delayed", "delayed"),
        ("available", "stale", "stale"),
        ("available", "unknown", "unknown"),
    ],
)
def test_degraded_evidence_advances_cursor_without_user_state_candidate(
    availability: str, freshness: str, reason: str
):
    item = evidence(
        availability=availability,
        freshness=freshness,
        source_sequence=None,
    )
    decision = decide_observation(item, now=NOW)

    assert decision.status == "degraded"
    assert decision.reason_code == reason
    assert decision.projectable is False
    assert decision.projection is None
    assert decision.health_projectable is True
    assert decision.health_projection == item.to_projection_payload()
    assert decision.next_cursor is not None
    assert decision.next_cursor.last_event_id == item.event_id


def test_degraded_replay_is_duplicate_and_recovery_can_follow():
    degraded = evidence(
        event_id="fixture-degraded",
        availability="offline",
        freshness="unknown",
        source_sequence=1,
    )
    degraded_decision = decide_observation(degraded, now=NOW)
    assert degraded_decision.status == "degraded"
    assert degraded_decision.next_cursor is not None

    replay = decide_observation(
        degraded,
        cursor=degraded_decision.next_cursor,
        now=NOW + timedelta(minutes=1),
    )
    assert replay.status == "duplicate"
    assert replay.next_cursor is None

    recovered = evidence(
        event_id="fixture-recovered",
        source_sequence=2,
        observed_at=NOW + timedelta(minutes=1),
        received_at=NOW + timedelta(minutes=1),
    )
    recovered_decision = decide_observation(
        recovered,
        cursor=degraded_decision.next_cursor,
        now=NOW + timedelta(minutes=1),
    )
    assert recovered_decision.status == "candidate"
    assert recovered_decision.next_cursor is not None


def test_degraded_cursor_timestamps_never_roll_back():
    first = evidence(
        event_id="fixture-degraded-first",
        availability="offline",
        freshness="unknown",
        source_sequence=1,
        observed_at=NOW,
        received_at=NOW,
    )
    first_decision = decide_observation(first, now=NOW)
    assert first_decision.next_cursor is not None

    second = evidence(
        event_id="fixture-degraded-second",
        availability="error",
        freshness="unknown",
        source_sequence=2,
        observed_at=NOW + timedelta(minutes=1),
        received_at=NOW,
    )
    second_decision = decide_observation(
        second,
        cursor=first_decision.next_cursor,
        now=NOW + timedelta(minutes=1),
    )
    assert second_decision.status == "degraded"
    assert second_decision.next_cursor is not None
    assert second_decision.next_cursor.last_observed_at == second.observed_at
    assert second_decision.next_cursor.last_received_at == first.received_at


def test_cursor_replay_window_is_bounded_and_rejects_oversize_state():
    existing = tuple(f"fixture-seen-{index}" for index in range(MAX_CURSOR_EVENT_IDS))
    cursor = ObservationCursor(
        source=SOURCE,
        capability=CAPABILITY,
        seen_event_ids=existing,
    )
    advanced = cursor.advance(
        evidence(event_id="fixture-seen-new", source_sequence=None)
    )

    assert len(advanced.seen_event_ids) == MAX_CURSOR_EVENT_IDS
    assert advanced.seen_event_ids == existing[1:] + ("fixture-seen-new",)

    with pytest.raises(ValueError):
        ObservationCursor(
            source=SOURCE,
            capability=CAPABILITY,
            seen_event_ids=existing + ("fixture-seen-overflow",),
        )


def test_same_fact_replay_is_duplicate_even_after_expiry():
    item = evidence()
    first = decide_observation(item, now=NOW)
    assert first.next_cursor is not None

    replay = decide_observation(
        item,
        cursor=first.next_cursor,
        now=NOW + timedelta(hours=2),
    )

    assert replay.status == "duplicate"
    assert replay.reason_code == "duplicate_event"
    assert replay.projection is None
    assert replay.next_cursor is None


def test_durable_cursor_rejects_source_rewind_without_in_process_memory():
    first = evidence(event_id="fixture-event-005", source_sequence=5)
    first_decision = decide_observation(first, now=NOW)
    assert first_decision.next_cursor is not None
    durable_cursor = ObservationCursor(
        source=SOURCE,
        capability=CAPABILITY,
        last_event_id=first_decision.next_cursor.last_event_id,
        last_source_sequence=first_decision.next_cursor.last_source_sequence,
        last_observed_at=first_decision.next_cursor.last_observed_at,
        last_received_at=first_decision.next_cursor.last_received_at,
        seen_event_ids=first_decision.next_cursor.seen_event_ids,
    )

    rewind = decide_observation(
        evidence(event_id="fixture-event-004", source_sequence=4),
        cursor=durable_cursor,
        now=NOW,
    )

    assert rewind.status == "out_of_order"
    assert rewind.reason_code == "source_sequence_rewind"
    assert rewind.projection is None


def test_timestamp_rewind_without_sequence_is_out_of_order():
    first = evidence(event_id="fixture-event-010", source_sequence=None)
    first_decision = decide_observation(first, now=NOW)
    assert first_decision.next_cursor is not None

    rewind = decide_observation(
        evidence(
            event_id="fixture-event-009",
            source_sequence=None,
            observed_at=NOW - timedelta(minutes=2),
        ),
        cursor=first_decision.next_cursor,
        now=NOW,
    )

    assert rewind.status == "out_of_order"
    assert rewind.reason_code == "observed_time_rewind"
    assert rewind.projection is None


@pytest.mark.parametrize(
    "item",
    [
        evidence(
            event_id="fixture-clock-observed",
            observed_at=NOW + timedelta(minutes=10),
            received_at=NOW,
            expires_at=None,
            source_sequence=None,
        ),
        evidence(
            event_id="fixture-clock-received",
            observed_at=NOW - timedelta(minutes=1),
            received_at=NOW + timedelta(minutes=10),
            source_sequence=None,
        ),
        evidence(
            event_id="fixture-clock-sender",
            observed_at=NOW,
            received_at=NOW - timedelta(minutes=10),
            source_sequence=None,
        ),
    ],
)
def test_clock_skew_never_projects(item: ObservationEvidence):
    decision = decide_observation(item, now=NOW)

    assert decision.status == "clock_skew"
    assert decision.projectable is False
    assert decision.next_cursor is None


def test_normal_long_interval_recovery_is_not_a_sender_clock_jump():
    previous = evidence(
        event_id="fixture-event-old",
        source_sequence=1,
        observed_at=NOW - timedelta(hours=25),
        received_at=NOW - timedelta(hours=25),
        expires_at=None,
    )
    cursor = decide_observation(previous, now=NOW - timedelta(hours=25)).next_cursor
    assert cursor is not None

    recovered = decide_observation(
        evidence(
            event_id="fixture-event-recovered",
            source_sequence=2,
            observed_at=NOW,
            received_at=NOW,
            expires_at=None,
        ),
        cursor=cursor,
        now=NOW,
    )

    assert recovered.status == "candidate"
    assert recovered.next_cursor is not None


def test_sender_elapsed_offset_jump_is_rejected():
    previous = evidence(
        event_id="fixture-event-old-offset",
        source_sequence=1,
        observed_at=NOW,
        received_at=NOW,
        expires_at=None,
    )
    cursor = decide_observation(previous, now=NOW).next_cursor
    assert cursor is not None

    jumped = decide_observation(
        evidence(
            event_id="fixture-event-offset-jump",
            source_sequence=2,
            observed_at=NOW + timedelta(hours=26),
            received_at=NOW + timedelta(hours=1),
            expires_at=None,
        ),
        cursor=cursor,
        now=NOW + timedelta(hours=1),
        max_clock_skew=timedelta(days=2),
        max_sender_clock_jump=timedelta(hours=24),
    )

    assert jumped.status == "sender_clock_jump"
    assert jumped.reason_code == "sender_clock_jump"
    assert jumped.projection is None
    assert jumped.next_cursor is None


def test_expired_evidence_is_not_a_candidate():
    decision = decide_observation(
        evidence(
            event_id="fixture-expired",
            observed_at=NOW - timedelta(minutes=10),
            received_at=NOW,
            expires_at=NOW - timedelta(minutes=1),
            source_sequence=None,
        ),
        now=NOW,
    )

    assert decision.status == "expired"
    assert decision.reason_code == "expired"
    assert decision.projection is None


def test_projection_payload_is_content_and_topology_free():
    payload = evidence().to_projection_payload()

    assert set(payload) == {
        "schema_version",
        "event_id",
        "source",
        "capability",
        "source_sequence",
        "observed_at",
        "received_at",
        "expires_at",
        "availability",
        "freshness",
        "confidence",
        "reason_code",
    }
    encoded = json.dumps(payload, sort_keys=True).casefold()
    for forbidden in ("transport", "os", "endpoint", "artifact", "raw", "value"):
        assert forbidden not in encoded
    assert evidence().to_dict()["schema_version"] == OBSERVATION_SCHEMA


def test_contract_bounds_and_incompatible_fresh_state_fail_closed():
    with pytest.raises(ValueError):
        evidence(availability="offline", freshness="fresh")
    with pytest.raises(ValueError):
        evidence(confidence=1.1)
    with pytest.raises(ValueError):
        evidence(reason_code="not a code")
    with pytest.raises(ValueError):
        evidence().__class__(
            "fixture-event",
            SOURCE,
            CAPABILITY,
            NOW.replace(tzinfo=None),
            NOW,
            None,
            "available",
            "fresh",
            1.0,
        )


def test_backward_sender_clock_jump_is_not_a_fresh_candidate():
    previous = evidence(event_id="fixture-event-new", source_sequence=10)
    cursor = decide_observation(previous, now=NOW).next_cursor
    assert cursor is not None

    jumped_back = decide_observation(
        evidence(
            event_id="fixture-event-clock-old",
            source_sequence=11,
            observed_at=NOW - timedelta(hours=25),
            received_at=NOW,
            expires_at=None,
        ),
        cursor=cursor,
        now=NOW,
        max_sender_clock_jump=timedelta(hours=24),
    )

    assert jumped_back.status == "sender_clock_jump"
    assert jumped_back.projectable is False


def test_synthetic_contract_fixture_is_complete_and_versioned():
    contract = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert contract["schema_version"] == "moonbite.parity.observation_contract.v1"
    assert set(contract) == {
        "schema_version",
        "purpose",
        "owner_boundary",
        "portable_invariants",
        "scenarios",
    }
    assert OBSERVATION_SCHEMA == "moon.observation.v1"
    assert {scenario["id"] for scenario in contract["scenarios"]} == {
        "fresh_candidate",
        "disabled_neutral",
        "offline_degraded",
        "stale_degraded",
        "duplicate_replay",
        "out_of_order",
        "clock_skew",
        "expired",
        "long_interval_recovery",
        "sender_clock_jump",
    }
