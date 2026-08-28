from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from moonbite_plugin.effects import (
    EFFECT_RECEIPT_SCHEMA,
    EFFECT_SCHEMA,
    EffectLedger,
    EffectReceipt,
    _read_effect_history_lock_free,
)
from moonbite_plugin.runtime_core import JsonlLedger, StateError, isoformat

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
SHA = "a" * 64


def begin(ledger: EffectLedger, *, effect_id: str = "effect-1", **overrides):
    values = {
        "kind": "message",
        "source_event_id": "event-1",
        "idempotency_key": "idem-1",
        "epoch_id": "epoch-1",
        "content_sha256": SHA,
        "content_length": 12,
        "expires_at": NOW + timedelta(minutes=5),
    }
    values.update(overrides)
    return ledger.begin_intent(effect_id, **values)


def receipt(**overrides) -> EffectReceipt:
    values = {
        "receipt_id": "receipt-1",
        "event_id": "event-1",
        "observed_at": NOW,
        "content_sha256": SHA,
        "content_length": 12,
        "epoch_id": "epoch-1",
    }
    values.update(overrides)
    return EffectReceipt(**values)


def test_intent_pending_unverified_verified_exact(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    intent = begin(ledger)

    assert intent.state == "intent"
    assert list(tmp_path.iterdir()) != []
    pending = ledger.mark_pending("effect-1")
    unverified = ledger.mark_queue_accepted("effect-1")
    verified = ledger.verify("effect-1", receipt())

    assert [row["state"] for row in ledger.ledger.rows()] == [
        "intent",
        "pending",
        "executed_unverified",
        "verified",
    ]
    assert pending.receipt is None
    assert unverified.receipt is None
    assert unverified.evidence == {
        "schema_version": EFFECT_RECEIPT_SCHEMA,
        "kind": "delivery_receipt",
        "receipt_id": None,
        "event_id": None,
        "observed_at": None,
        "content_sha256": None,
        "content_length": None,
        "epoch_id": None,
    }
    assert verified.verified
    assert verified.receipt == receipt()


def test_constructor_does_not_write(tmp_path):
    EffectLedger(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_pristine_read_apis_do_not_create_files(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    assert ledger.records() == ()
    assert ledger.find_by_idempotency("missing") is None
    assert ledger.pending_for_reconciliation(NOW) == []
    assert ledger.get("missing") is None
    assert list(tmp_path.iterdir()) == []


def test_find_by_idempotency_validates_exact_key_and_returns_missing_none(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)

    assert ledger.find_by_idempotency("missing") is None
    for invalid in ("", "   ", None, 42):
        with pytest.raises(ValueError, match="idempotency_key"):
            ledger.find_by_idempotency(invalid)


def test_find_by_idempotency_returns_current_record_through_transitions(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    intent = begin(ledger)
    assert ledger.find_by_idempotency("idem-1") == intent

    pending = ledger.mark_pending("effect-1")
    assert ledger.find_by_idempotency("idem-1") == pending
    unverified = ledger.mark_queue_accepted("effect-1")
    assert ledger.find_by_idempotency("idem-1") == unverified
    verified = ledger.verify("effect-1", receipt())
    assert ledger.find_by_idempotency("idem-1") == verified

    begin(
        ledger,
        effect_id="effect-failed",
        idempotency_key="idem-failed",
    )
    failed = ledger.fail("effect-failed", "transport rejected", retryable=True)
    assert ledger.find_by_idempotency("idem-failed") == failed

    begin(
        ledger,
        effect_id="effect-expired",
        idempotency_key="idem-expired",
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("effect-expired")
    expired = ledger.expire("effect-expired", NOW)
    assert ledger.find_by_idempotency("idem-expired") == expired
    requeued = ledger.requeue("effect-expired", expires_at=NOW + timedelta(minutes=5))
    assert ledger.find_by_idempotency("idem-expired") == requeued


def test_find_by_idempotency_is_read_only_for_effect_ledger(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    path = tmp_path / "effects.jsonl"
    before_content = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns

    assert ledger.find_by_idempotency("idem-1").state == "pending"

    assert path.read_bytes() == before_content
    assert path.stat().st_mtime_ns == before_mtime


def test_all_existing_state_reads_preserve_content_and_mtime(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in tmp_path.iterdir()
    }

    assert ledger.records()[0].state == "pending"
    assert ledger.find_by_idempotency("idem-1").state == "pending"
    assert ledger.pending_for_reconciliation(NOW)[0].state == "pending"
    assert ledger.get("effect-1").state == "pending"

    after = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in tmp_path.iterdir()
    }
    assert after == before


def test_records_returns_sorted_latest_states_after_transitions(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(
        ledger,
        effect_id="later",
        idempotency_key="idem-later",
        source_event_id="event-later",
        created_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=2),
    )
    begin(
        ledger,
        effect_id="past",
        idempotency_key="idem-past",
        source_event_id="event-past",
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("later")
    ledger.mark_pending("past")
    ledger.mark_queue_accepted("past")

    assert [(record.effect_id, record.state) for record in ledger.records()] == [
        ("past", "executed_unverified"),
        ("later", "pending"),
    ]

    ledger.verify(
        "past",
        receipt(receipt_id="receipt-past", event_id="event-past"),
    )
    assert [(record.effect_id, record.state) for record in ledger.records()] == [
        ("past", "verified"),
        ("later", "pending"),
    ]
    assert EffectLedger(tmp_path, clock=lambda: NOW).records() == ledger.records()


def test_records_concurrent_reads_replay_one_consistent_view(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")

    with ThreadPoolExecutor(max_workers=8) as pool:
        views = list(pool.map(lambda _index: ledger.records(), range(24)))

    assert all(view == views[0] for view in views)


def test_begin_rejects_noninitial_attempt_without_append(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    with pytest.raises(ValueError, match="initial effect attempt must be 1"):
        begin(ledger, attempt=2)

    assert not (tmp_path / "effects.jsonl").exists()


def test_effect_intent_rejects_expiry_at_or_before_creation(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    with pytest.raises(ValueError, match="later than created_at"):
        begin(ledger, created_at=NOW, expires_at=NOW)

    assert not (tmp_path / "effects.jsonl").exists()


def test_direct_verified_from_pending_is_allowed(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")

    assert ledger.verify("effect-1", receipt()).state == "verified"


def test_failed_effect_is_terminal_and_preserves_unverified_evidence(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")

    failed = ledger.fail("effect-1", "transport rejected", retryable=True)

    assert failed.state == "failed"
    assert failed.reason == "transport rejected"
    assert failed.retryable is True
    assert failed.receipt is None
    with pytest.raises(ValueError):
        ledger.mark_pending("effect-1")


def test_fail_from_intent_reopens_as_failed_after_replay(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)

    failed = ledger.fail("effect-1", "before queue", retryable=False)

    assert failed.state == "failed"
    assert EffectLedger(tmp_path).get("effect-1") == failed


def test_fail_from_requeued_reopens_as_failed_after_replay(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(
        ledger,
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("effect-1")
    ledger.expire("effect-1", NOW)
    ledger.requeue("effect-1", expires_at=NOW + timedelta(minutes=5))

    failed = ledger.fail("effect-1", "retry rejected", retryable=True)

    assert failed.state == "failed"
    assert failed.attempt == 2
    assert EffectLedger(tmp_path).get("effect-1") == failed


def test_requeue_requires_explicit_future_expiry(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(
        ledger,
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("effect-1")
    ledger.expire("effect-1", NOW)
    before = len(ledger.ledger.rows())

    with pytest.raises(TypeError):
        ledger.requeue("effect-1")
    with pytest.raises(ValueError, match="later than current time"):
        ledger.requeue("effect-1", expires_at=NOW)

    assert len(ledger.ledger.rows()) == before
    assert ledger.get("effect-1").state == "expired"


def test_not_yet_expired_then_expired_requeued_and_pending_attempt_increments(
    tmp_path,
):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger, expires_at=NOW + timedelta(minutes=5))
    ledger.mark_pending("effect-1")

    with pytest.raises(ValueError, match="not expired"):
        ledger.expire("effect-1", NOW + timedelta(minutes=5))
    expired = ledger.expire("effect-1", NOW + timedelta(minutes=5, seconds=1))
    assert expired.state == "expired"

    new_deadline = NOW + timedelta(minutes=10)
    requeued = ledger.requeue(
        "effect-1",
        expires_at=new_deadline,
        idempotency_key="idem-1",
        source_event_id="event-1",
    )
    assert requeued.state == "requeued"
    assert requeued.attempt == 2
    pending = ledger.mark_pending("effect-1")
    assert pending.state == "pending"
    assert pending.expires_at == new_deadline
    with pytest.raises(ValueError, match="not expired"):
        ledger.expire("effect-1", new_deadline)
    expired_again = ledger.expire("effect-1", new_deadline + timedelta(seconds=1))
    assert expired_again.state == "expired"
    reopened = EffectLedger(tmp_path).get("effect-1")
    assert reopened.state == "expired"
    assert reopened.attempt == 2
    assert reopened.expires_at == new_deadline


def test_missing_pending_verify_rejects_without_append(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    with pytest.raises(ValueError):
        ledger.verify("missing", receipt())
    assert not (tmp_path / "effects.jsonl").exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "other-event"},
        {"content_sha256": "b" * 64},
        {"content_length": 13},
        {"epoch_id": "other-epoch"},
    ],
)
def test_stale_receipt_mismatch_rejected(tmp_path, changes):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    with pytest.raises(ValueError, match="match"):
        ledger.verify("effect-1", receipt(**changes))
    assert ledger.get("effect-1").state == "pending"


def test_same_receipt_replay_is_idempotent_but_conflict_rejects(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    original = ledger.verify("effect-1", receipt())
    assert ledger.verify("effect-1", receipt()) == original

    with pytest.raises(ValueError, match="conflicting or stale"):
        ledger.verify("effect-1", receipt(receipt_id="receipt-2"))
    assert len(ledger.ledger.rows()) == 3


def test_begin_idempotency_returns_existing_or_rejects_conflict(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    original = begin(ledger, effect_id="first")
    replay = begin(ledger, effect_id="different")
    assert replay == original
    assert len(ledger.ledger.rows()) == 1

    with pytest.raises(ValueError, match="idempotency"):
        begin(ledger, kind="different-kind")


def test_append_failure_does_not_advance_state(tmp_path, monkeypatch):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)

    def fail_append(_row):
        raise OSError("append fixture failure")

    monkeypatch.setattr(ledger.ledger, "append", fail_append)
    with pytest.raises(OSError, match="append fixture failure"):
        ledger.mark_pending("effect-1")
    monkeypatch.undo()
    assert ledger.get("effect-1").state == "intent"
    assert len(ledger.ledger.rows()) == 1


def test_corrupt_and_out_of_order_rows_are_rejected_on_replay(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    row = ledger.ledger.rows()[0]
    row["state"] = "executed_unverified"
    row["operation"] = "mark_queue_accepted"
    JsonlLedger(tmp_path / "effects.jsonl").append(row)
    with pytest.raises(StateError):
        ledger.get("effect-1")
    with pytest.raises(StateError):
        ledger.find_by_idempotency("idem-1")
    with pytest.raises(StateError):
        ledger.records()

    corrupt_root = tmp_path / "corrupt"
    JsonlLedger(corrupt_root / "effects.jsonl").append(
        {"schema_version": "moon.effect.unknown", "state": "intent"}
    )
    with pytest.raises(StateError):
        EffectLedger(corrupt_root).find_by_idempotency("missing")
    with pytest.raises(StateError):
        EffectLedger(corrupt_root).records()


def test_non_requeue_attempt_change_is_rejected_on_replay(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    row = ledger.ledger.rows()[-1]
    row["attempt"] = 2
    JsonlLedger(tmp_path / "effects.jsonl").append(row)

    with pytest.raises(StateError):
        ledger.get("effect-1")


def test_non_requeue_expiry_change_is_rejected_on_replay(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    row = ledger.ledger.rows()[-1]
    row["expires_at"] = isoformat(NOW + timedelta(minutes=6))
    JsonlLedger(tmp_path / "effects.jsonl").append(row)

    with pytest.raises(StateError):
        ledger.get("effect-1")


def test_concurrent_begin_same_key_appends_one_intent(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    def worker(index):
        return begin(
            ledger,
            effect_id=f"concurrent-{index}",
            idempotency_key="same-key",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(24)))

    assert len(ledger.ledger.rows()) == 1
    assert {result.effect_id for result in results} == {results[0].effect_id}


def test_pending_reconciliation_is_read_only_and_filters_terminal_states(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger, effect_id="one")
    begin(
        ledger,
        effect_id="two",
        idempotency_key="idem-2",
        source_event_id="event-2",
    )
    ledger.mark_pending("one")
    ledger.mark_pending("two")
    ledger.mark_queue_accepted("two")
    ledger.verify(
        "two",
        receipt(receipt_id="receipt-2", event_id="event-2"),
    )
    before = len(ledger.ledger.rows())

    pending = ledger.pending_for_reconciliation(NOW)

    assert [record.effect_id for record in pending] == ["one"]
    assert len(ledger.ledger.rows()) == before


def test_pending_reconciliation_retains_expired_and_sorts_stably(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(
        ledger,
        effect_id="later",
        idempotency_key="idem-later",
        source_event_id="event-later",
        created_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=2),
    )
    begin(
        ledger,
        effect_id="past",
        idempotency_key="idem-past",
        source_event_id="event-past",
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("later")
    ledger.mark_pending("past")

    pending = ledger.pending_for_reconciliation(NOW)

    assert [record.effect_id for record in pending] == ["past", "later"]


def test_every_row_contains_identity_state_and_no_body(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    row = ledger.ledger.rows()[0]
    assert row["schema_version"] == EFFECT_SCHEMA
    assert {
        "source_event_id",
        "idempotency_key",
        "attempt",
        "state",
    } <= set(row)
    assert "body" not in row
    assert "content" not in row


def test_observer_status_pristine_is_zero_write(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    assert ledger.observer_status(target_date=NOW.date(), now=NOW) == ()
    assert list(tmp_path.iterdir()) == []


def test_observer_status_is_lock_free_and_projects_recovery(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger, kind="delivery")
    ledger.mark_pending("effect-1")
    for path in (ledger.ledger.lock_path, ledger.mutation_lock):
        if path.exists():
            path.unlink()
    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in tmp_path.iterdir()
    }

    pending = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert pending[0].state == "current"
    assert pending[0].code == "effect_pending"
    assert {
        "body",
        "output",
        "reason",
    }.isdisjoint(pending[0].to_dict())
    after = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in tmp_path.iterdir()
    }
    assert after == before
    assert not ledger.ledger.lock_path.exists()
    assert not ledger.mutation_lock.exists()

    verified = ledger.verify("effect-1", receipt())
    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert verified.state == "verified"
    assert facts[0].state == "recovered_history"
    assert facts[0].recovery is not None
    assert facts[0].recovery.ref == "receipt-1"


def test_observer_status_requires_begin_first_and_fails_closed(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    row = ledger.ledger.rows()[0]
    row.update(operation="mark_pending", state="pending")
    ledger.ledger.path.write_text("", encoding="utf-8")
    ledger.ledger.append(row)

    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code.startswith("effect_integrity_error:")
    assert facts[0].key == "effect:integrity:effects.jsonl"


def test_observer_status_quarantines_bad_chain_and_pseudo_verified(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    rows = ledger.ledger.rows()

    bad = dict(rows[-1])
    bad.update(
        operation="fail",
        source_event_id="tampered-event",
        state="failed",
        reason="PRIVATE_FAILURE_BODY",
        retryable=True,
    )
    pseudo_verified = dict(rows[-1])
    pseudo_verified.update(
        operation="verify",
        state="verified",
        observed_at=isoformat(NOW),
        receipt=receipt().to_dict(),
    )
    independent = dict(rows[0])
    independent.update(
        effect_id="effect-independent",
        source_event_id="event-independent",
        idempotency_key="idem-independent",
    )
    independent_pending = dict(independent)
    independent_pending.update(operation="mark_pending", state="pending")
    for row in (bad, pseudo_verified, independent, independent_pending):
        ledger.ledger.append(row)
    for path in (ledger.ledger.lock_path, ledger.mutation_lock):
        if path.exists():
            path.unlink()

    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert "verified" not in facts[0].code
    assert "PRIVATE_FAILURE_BODY" not in str(facts)


def test_lock_free_reader_drops_rows_when_replay_integrity_fails(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    ledger.mark_pending("effect-1")
    rows = ledger.ledger.rows()
    bad = dict(rows[-1])
    bad.update(
        operation="fail",
        source_event_id="tampered-event",
        state="failed",
        reason="PRIVATE_FAILURE_BODY",
        retryable=True,
    )
    pseudo_verified = dict(rows[-1])
    pseudo_verified.update(
        operation="verify",
        state="verified",
        observed_at=isoformat(NOW),
        receipt=receipt().to_dict(),
    )
    ledger.ledger.append(bad)
    ledger.ledger.append(pseudo_verified)

    records, integrity = _read_effect_history_lock_free(ledger.ledger.path)

    assert records == ()
    assert integrity is not None
    assert "PRIVATE_FAILURE_BODY" not in str((records, integrity))


def test_observer_status_rejects_invalid_requeue_attempt(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(
        ledger,
        created_at=NOW - timedelta(minutes=2),
        expires_at=NOW - timedelta(minutes=1),
    )
    ledger.mark_pending("effect-1")
    ledger.expire("effect-1", NOW)
    expired = ledger.ledger.rows()[-1]
    invalid_requeue = dict(expired)
    invalid_requeue.update(
        operation="requeue",
        state="requeued",
        expires_at=isoformat(NOW + timedelta(minutes=5)),
        reason=None,
        retryable=None,
    )
    trailing_pending = dict(invalid_requeue)
    trailing_pending.update(operation="mark_pending", state="pending")
    ledger.ledger.append(invalid_requeue)
    ledger.ledger.append(trailing_pending)

    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code.startswith("effect_integrity_error:")


def test_observer_status_rejects_cross_effect_idempotency_reuse(tmp_path):
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)
    begin(ledger)
    first = ledger.ledger.rows()[0]
    second = dict(first)
    second.update(effect_id="effect-2", source_event_id="event-2")
    second_pending = dict(second)
    second_pending.update(operation="mark_pending", state="pending")
    ledger.ledger.append(second)
    ledger.ledger.append(second_pending)

    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert facts[0].code.startswith("effect_integrity_error:")


def test_observer_status_corrupt_ledger_is_current_integrity(tmp_path):
    path = tmp_path / "effects.jsonl"
    path.write_text('{"schema_version":"moon.effect.v1"}\ntruncated', encoding="utf-8")
    ledger = EffectLedger(tmp_path, clock=lambda: NOW)

    facts = ledger.observer_status(target_date=NOW.date(), now=NOW)

    assert len(facts) == 1
    assert facts[0].state == "current"
    assert "integrity" in facts[0].code
    assert facts[0].to_dict()["recovery"] is None
