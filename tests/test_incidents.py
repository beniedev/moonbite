from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import pytest

from moonbite_plugin.incidents import (
    INCIDENT_PROJECTION_SCHEMA,
    MAX_INCIDENT_SEEN_EVENT_IDS,
    IncidentCursor,
    IncidentEvidence,
    IncidentProjection,
    aggregate_incidents,
    decide_incident,
    fingerprint_codes,
)


NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)
SOURCE = "fixture.source"
CATEGORY = "runtime.health"
REASON = "provider_timeout"
FINGERPRINT = "a" * 64


def evidence(
    event_id: str = "incident-001",
    *,
    fingerprint: str = FINGERPRINT,
    reason: str = REASON,
    observed_at: datetime = NOW - timedelta(minutes=1),
    received_at: datetime | None = None,
    severity: str = "warning",
    state: str = "active",
    source_sequence: int | None = 1,
    recovery_ref: str | None = None,
) -> IncidentEvidence:
    return IncidentEvidence(
        event_id=event_id,
        source=SOURCE,
        category=CATEGORY,
        reason=reason,
        observed_at=observed_at,
        received_at=received_at or observed_at + timedelta(seconds=1),
        fingerprint=fingerprint,
        severity=severity,
        state=state,
        source_sequence=source_sequence,
        recovery_ref=(fingerprint if state == "recovered" else recovery_ref),
    )


def test_fingerprint_helper_is_canonical_and_rejects_raw_text():
    assert fingerprint_codes(["runtime", "health"]) == fingerprint_codes(
        ("runtime", "health")
    )
    assert fingerprint_codes(["runtime", "health"]) != fingerprint_codes(
        ["health", "runtime"]
    )
    assert len(fingerprint_codes(["runtime.health", "timeout"])) == 64
    with pytest.raises(ValueError):
        fingerprint_codes([])
    with pytest.raises(ValueError):
        fingerprint_codes(["runtime health"])
    with pytest.raises(ValueError):
        fingerprint_codes(["runtime"] * 9)


def test_evidence_is_strict_and_projection_has_no_transport_or_raw_fields():
    item = evidence()
    assert set(item.to_dict()) == {
        "schema_version",
        "event_id",
        "source",
        "category",
        "reason",
        "observed_at",
        "received_at",
        "source_sequence",
        "fingerprint",
        "severity",
        "state",
        "recovery_ref",
    }
    with pytest.raises(ValueError):
        evidence(reason="free text")
    with pytest.raises(ValueError):
        evidence(fingerprint="A" * 64)
    with pytest.raises(ValueError):
        evidence(severity="fatal")
    with pytest.raises(ValueError):
        IncidentEvidence(
            event_id="incident-recovery",
            source=SOURCE,
            category=CATEGORY,
            reason=REASON,
            observed_at=NOW,
            received_at=NOW,
            fingerprint=FINGERPRINT,
            severity="warning",
            state="recovered",
        )
    with pytest.raises(ValueError):
        evidence(observed_at=NOW.replace(tzinfo=None))

    text = json.dumps(item.to_dict(), sort_keys=True).lower()
    for forbidden in (
        "message",
        "path",
        "endpoint",
        "device",
        "transport",
        "payload",
        "raw",
    ):
        assert forbidden not in text


def test_first_active_is_new_then_continuous_active_is_current():
    first = decide_incident(evidence(), now=NOW)
    assert first.status == "new"
    assert first.projection is not None
    assert first.projection["lifecycle"] == "new"
    assert first.next_cursor is not None
    assert first.next_cursor.active_seen is True

    current = decide_incident(
        evidence(
            event_id="incident-002",
            observed_at=NOW,
            source_sequence=2,
        ),
        now=NOW + timedelta(minutes=1),
        cursor=first.next_cursor,
    )
    assert current.status == "current"
    assert current.projection is not None
    assert current.projection["state"] == "active"


def test_recovery_requires_active_temporal_evidence_and_recurrence_is_new():
    first = decide_incident(evidence(), now=NOW)
    assert first.next_cursor is not None
    recovered = decide_incident(
        evidence(
            event_id="incident-recovered",
            observed_at=NOW + timedelta(minutes=1),
            source_sequence=2,
            state="recovered",
        ),
        now=NOW + timedelta(minutes=2),
        cursor=first.next_cursor,
    )
    assert recovered.status == "recovered_history"
    assert recovered.projection is not None
    assert recovered.projection["lifecycle"] == "recovered_history"
    assert recovered.projection["state"] == "recovered"
    assert recovered.next_cursor is not None
    assert recovered.next_cursor.recovered_at == NOW + timedelta(minutes=1)

    stale = decide_incident(
        evidence(
            event_id="incident-recovered-again",
            observed_at=NOW + timedelta(minutes=2),
            source_sequence=3,
            state="recovered",
        ),
        now=NOW + timedelta(minutes=3),
        cursor=recovered.next_cursor,
    )
    assert stale.status == "neutral"
    assert stale.reason_code == "stale_recovery"

    recurrence = decide_incident(
        evidence(
            event_id="incident-recurred",
            observed_at=NOW + timedelta(minutes=3),
            source_sequence=4,
            severity="critical",
        ),
        now=NOW + timedelta(minutes=4),
        cursor=stale.next_cursor,
    )
    assert recurrence.status == "new"
    assert recurrence.next_cursor is not None
    assert recurrence.next_cursor.recovered_at is None


def test_orphan_recovery_is_neutral_and_does_not_lighten_a_future_active_event():
    orphan = decide_incident(
        evidence(
            event_id="orphan-recovery",
            state="recovered",
        ),
        now=NOW,
    )
    assert orphan.status == "neutral"
    assert orphan.reason_code == "orphan_recovery"
    assert orphan.projection is not None
    assert orphan.projection["state"] == "neutral"
    assert orphan.active is False
    assert orphan.next_cursor is not None

    active = decide_incident(
        evidence(event_id="after-orphan", observed_at=NOW, source_sequence=2),
        now=NOW + timedelta(minutes=1),
        cursor=orphan.next_cursor,
    )
    assert active.status == "new"


def test_duplicate_order_and_clock_fail_closed():
    first = decide_incident(evidence(), now=NOW)
    assert first.next_cursor is not None

    duplicate = decide_incident(
        evidence(),
        now=NOW + timedelta(hours=1),
        cursor=first.next_cursor,
    )
    assert duplicate.status == "duplicate"
    assert duplicate.next_cursor is None

    out_of_order = decide_incident(
        evidence(
            event_id="older",
            observed_at=NOW - timedelta(minutes=2),
            source_sequence=2,
        ),
        now=NOW,
        cursor=first.next_cursor,
    )
    assert out_of_order.status == "out_of_order"

    sequence_rewind = decide_incident(
        evidence(
            event_id="sequence-rewind",
            observed_at=NOW + timedelta(minutes=1),
            source_sequence=1,
        ),
        now=NOW + timedelta(minutes=2),
        cursor=first.next_cursor,
    )
    assert sequence_rewind.status == "out_of_order"

    future = decide_incident(
        evidence(
            event_id="future",
            observed_at=NOW + timedelta(minutes=10),
            received_at=NOW + timedelta(minutes=10),
            source_sequence=2,
        ),
        now=NOW,
    )
    assert future.status == "clock_skew"

    sender_ahead = decide_incident(
        evidence(
            event_id="sender-ahead",
            observed_at=NOW + timedelta(minutes=10),
            received_at=NOW,
        ),
        now=NOW,
    )
    assert sender_ahead.status == "clock_skew"

    assert (
        decide_incident(
            evidence(
                event_id="scope",
                fingerprint="b" * 64,
            ),
            now=NOW,
            cursor=first.next_cursor,
        ).status
        == "scope_mismatch"
    )


def test_cursor_seen_ids_are_bounded_and_immutable():
    cursor = IncidentCursor(
        source=SOURCE,
        category=CATEGORY,
        fingerprint=FINGERPRINT,
        seen_event_ids=list(
            f"seen-{index}" for index in range(MAX_INCIDENT_SEEN_EVENT_IDS)
        ),
    )
    advanced = cursor.advance(evidence(event_id="seen-new"))
    assert isinstance(advanced.seen_event_ids, tuple)
    assert len(advanced.seen_event_ids) == MAX_INCIDENT_SEEN_EVENT_IDS
    assert advanced.seen_event_ids[-1] == "seen-new"
    with pytest.raises(ValueError):
        IncidentCursor(
            source=SOURCE,
            category=CATEGORY,
            fingerprint=FINGERPRINT,
            seen_event_ids=("duplicate", "duplicate"),
        )


def test_projection_parser_is_exact_and_aggregation_is_latest_stable_and_severity_aware():
    first = decide_incident(
        evidence(event_id="active-first", severity="warning"),
        now=NOW,
    )
    assert first.projection is not None
    current = decide_incident(
        evidence(
            event_id="active-current",
            observed_at=NOW,
            source_sequence=2,
            severity="warning",
        ),
        now=NOW + timedelta(minutes=1),
        cursor=first.next_cursor,
    )
    assert current.projection is not None
    critical = decide_incident(
        evidence(
            event_id="active-critical",
            fingerprint="b" * 64,
            severity="critical",
        ),
        now=NOW,
    )
    assert critical.projection is not None
    recovery_first = decide_incident(
        evidence(
            event_id="history-active",
            fingerprint="c" * 64,
        ),
        now=NOW,
    )
    assert recovery_first.next_cursor is not None
    recovered = decide_incident(
        evidence(
            event_id="history",
            fingerprint="c" * 64,
            state="recovered",
            observed_at=NOW,
            source_sequence=2,
        ),
        now=NOW + timedelta(minutes=1),
        cursor=recovery_first.next_cursor,
    )
    assert recovered.projection is not None

    projections = [
        first.projection,
        current.projection,
        critical.projection,
        recovered.projection,
    ]
    summary = aggregate_incidents(projections)
    reverse = aggregate_incidents(reversed(projections))
    assert summary == reverse
    assert summary["total_count"] == 3
    assert summary["counts"]["recovered_history"] == 1
    assert summary["counts"]["current"] == 1
    assert summary["active_severity"] == "critical"
    assert summary["active_severity_counts"]["critical"] == 1
    assert summary["active_severity_counts"]["warning"] == 1
    assert len(summary["projections"]) == 3
    assert summary["projections"][0]["schema_version"] == INCIDENT_PROJECTION_SCHEMA

    parsed = IncidentProjection.from_dict(summary["projections"][0])
    assert parsed.to_dict() == summary["projections"][0]
    malformed = dict(summary["projections"][0])
    malformed["message"] = "raw"
    with pytest.raises(ValueError):
        IncidentProjection.from_dict(malformed)


def test_aggregation_neutral_is_only_a_fallback_for_each_scope():
    active = decide_incident(
        evidence(event_id="active-before-neutral"),
        now=NOW,
    )
    assert active.projection is not None
    neutral = decide_incident(
        evidence(
            event_id="explicit-neutral",
            observed_at=NOW,
            source_sequence=2,
            state="neutral",
        ),
        now=NOW + timedelta(minutes=1),
        cursor=active.next_cursor,
    )
    assert neutral.status == "neutral"
    assert neutral.projection is not None

    neutral_only = decide_incident(
        evidence(
            event_id="neutral-only",
            fingerprint="b" * 64,
            state="neutral",
        ),
        now=NOW,
    )
    assert neutral_only.projection is not None

    summary = aggregate_incidents(
        [active.projection, neutral.projection, neutral_only.projection]
    )
    assert summary["total_count"] == 2
    assert summary["counts"] == {
        "new": 1,
        "current": 0,
        "recovered_history": 0,
        "neutral": 1,
    }
    assert summary["projections"][0]["state"] == "active"


def test_aggregation_keeps_recovery_history_through_stale_neutral_then_recurrence():
    active = decide_incident(
        evidence(event_id="history-active", fingerprint="c" * 64),
        now=NOW,
    )
    assert active.next_cursor is not None
    recovered = decide_incident(
        evidence(
            event_id="history-recovered",
            fingerprint="c" * 64,
            observed_at=NOW,
            source_sequence=2,
            state="recovered",
        ),
        now=NOW + timedelta(minutes=1),
        cursor=active.next_cursor,
    )
    assert recovered.status == "recovered_history"
    assert recovered.projection is not None
    assert recovered.projection["recovery_ref"] == "c" * 64
    assert recovered.next_cursor is not None
    stale = decide_incident(
        evidence(
            event_id="history-stale-recovery",
            fingerprint="c" * 64,
            observed_at=NOW + timedelta(minutes=1),
            source_sequence=3,
            state="recovered",
        ),
        now=NOW + timedelta(minutes=2),
        cursor=recovered.next_cursor,
    )
    assert stale.status == "neutral"
    assert stale.projection is not None

    history_summary = aggregate_incidents(
        [active.projection, recovered.projection, stale.projection]
    )
    assert history_summary["counts"] == {
        "new": 0,
        "current": 0,
        "recovered_history": 1,
        "neutral": 0,
    }
    assert history_summary["projections"][0]["event_id"] == "history-recovered"

    recurrence = decide_incident(
        evidence(
            event_id="history-recurrence",
            fingerprint="c" * 64,
            observed_at=NOW + timedelta(minutes=2),
            source_sequence=4,
            severity="critical",
        ),
        now=NOW + timedelta(minutes=3),
        cursor=stale.next_cursor,
    )
    assert recurrence.status == "new"
    assert recurrence.projection is not None
    recurrence_summary = aggregate_incidents(
        [
            active.projection,
            recovered.projection,
            stale.projection,
            recurrence.projection,
        ]
    )
    assert recurrence_summary["counts"] == {
        "new": 1,
        "current": 0,
        "recovered_history": 0,
        "neutral": 0,
    }
    assert recurrence_summary["active_severity"] == "critical"
    assert recurrence_summary["projections"][0]["event_id"] == "history-recurrence"
