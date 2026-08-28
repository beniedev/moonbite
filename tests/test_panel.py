from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor

import pytest

from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.panel import (
    AUTONOMY_COMPLETION_EFFECT_KIND,
    PANEL_SCHEMA,
    PanelStore,
    _observer_state_metadata,
)
from moonbite_plugin.runtime_core import EventBus, StateError, atomic_json_write


def store(tmp_path, now, *, owner_id="default"):
    bus = EventBus(tmp_path, clock=lambda: now[0])
    return PanelStore(
        tmp_path,
        bus=bus,
        timezone_name="UTC",
        anchor_hour=6,
        clock=lambda: now[0],
        owner_id=owner_id,
    )


def verified_effect(
    tmp_path,
    now,
    *,
    event_id="event_fixture",
    kind=AUTONOMY_COMPLETION_EFFECT_KIND,
):
    ledger = EffectLedger(tmp_path, clock=lambda: now[0])
    created = ledger.begin_intent(
        effect_id="effect_fixture",
        kind=kind,
        source_event_id=event_id,
        idempotency_key="idempotency_fixture",
        epoch_id="2026-08-22",
        content_sha256="a" * 64,
        content_length=3,
        created_at=now[0],
        expires_at=now[0] + timedelta(hours=1),
    )
    ledger.mark_pending(created.effect_id)
    receipt = EffectReceipt(
        receipt_id="receipt_fixture",
        event_id=event_id,
        observed_at=now[0],
        content_sha256="a" * 64,
        content_length=3,
        epoch_id="2026-08-22",
    )
    return ledger.verify(created.effect_id, receipt), receipt


def test_owner_scoped_same_name_has_explicit_records(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "status",
        "a",
        owner="owner_a",
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="event_a",
    )
    panel.set_field(
        "status",
        "b",
        owner="owner_b",
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="event_b",
    )

    snapshot = panel.snapshot()
    assert {value["owner"] for value in snapshot["fields"].values()} == {
        "owner_a",
        "owner_b",
    }
    assert {value["name"] for value in snapshot["fields"].values()} == {"status"}
    assert snapshot["owner_epochs"] == {
        "owner_a": "2026-08-22",
        "owner_b": "2026-08-22",
    }
    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    assert all(
        set(record)
        >= {
            "owner",
            "name",
            "schema_version",
            "source_event_id",
            "reset_policy",
            "consume_policy",
        }
        for record in raw["fields"].values()
    )


def test_snapshot_hides_ttl_without_mutating_state(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "presence",
        "active",
        source="fixture",
        ttl=timedelta(minutes=5),
        source_event_id="event_presence",
    )
    before = panel.path.read_bytes()
    now[0] += timedelta(minutes=6)

    assert panel.snapshot()["fields"] == {}
    assert panel.path.read_bytes() == before


def test_rollover_only_removes_requesting_owner_stale_daily_fields(tmp_path):
    now = [datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "daily_a",
        "old",
        owner="owner_a",
        source="fixture",
        ttl=timedelta(days=3),
        source_event_id="event_a",
    )
    panel.set_field(
        "daily_b",
        "old",
        owner="owner_b",
        source="fixture",
        ttl=timedelta(days=3),
        source_event_id="event_b",
    )
    panel.set_field(
        "persistent_a",
        "keep",
        owner="owner_a",
        source="fixture",
        persistent=True,
        source_event_id="event_p",
    )
    now[0] += timedelta(days=1)

    panel.rollover(owner="owner_a")
    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    records = list(raw["fields"].values())
    assert any(
        record["owner"] == "owner_b" and record["name"] == "daily_b"
        for record in records
    )
    assert not any(
        record["owner"] == "owner_a" and record["name"] == "daily_a"
        for record in records
    )
    assert any(
        record["owner"] == "owner_a" and record["name"] == "persistent_a"
        for record in records
    )
    assert set(panel.snapshot(owner="owner_b")["fields"]) == {"daily_b"}


def test_stale_event_is_a_noop_after_newer_event(tmp_path):
    now = [datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    newer = now[0]
    older = newer - timedelta(minutes=1)
    panel.set_field(
        "status",
        "new",
        owner="owner_a",
        source="fixture",
        ttl=timedelta(hours=1),
        observed_at=newer,
        source_event_id="event_new",
    )
    before = json.loads(panel.path.read_text(encoding="utf-8"))
    result = panel.set_field(
        "status",
        "old",
        owner="owner_a",
        source="fixture",
        ttl=timedelta(hours=1),
        observed_at=older,
        source_event_id="event_old",
    )
    after = json.loads(panel.path.read_text(encoding="utf-8"))

    assert result.value == "new"
    assert after["fields"] == before["fields"]
    assert after["owner_epochs"] == before["owner_epochs"]
    assert [event.kind for event in panel.bus.read_events()].count(
        "panel.field_updated"
    ) == 1


def test_stale_event_cannot_roll_owner_epoch_back(tmp_path):
    now = [datetime(2026, 8, 23, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "fresh",
        "value",
        owner="owner_a",
        source="fixture",
        ttl=timedelta(hours=1),
        observed_at=now[0],
        source_event_id="event_fresh",
    )
    before = json.loads(panel.path.read_text(encoding="utf-8"))

    with pytest.raises(ValueError, match="stale"):
        panel.set_field(
            "other",
            "old",
            owner="owner_a",
            source="fixture",
            ttl=timedelta(hours=1),
            observed_at=now[0] - timedelta(days=1),
            source_event_id="event_old",
        )

    after = json.loads(panel.path.read_text(encoding="utf-8"))
    assert after["owner_epochs"] == before["owner_epochs"] == {"owner_a": "2026-08-23"}
    assert after["fields"] == before["fields"]
    assert "other" not in panel.snapshot(owner="owner_a")["fields"]


def test_successful_updates_compact_bus_projection_markers(tmp_path):
    now = [datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    for index in range(5):
        panel.set_field(
            "counter",
            index,
            source="fixture",
            ttl=timedelta(hours=1),
            observed_at=now[0] + timedelta(minutes=index),
            source_event_id=f"event_{index}",
        )

    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    assert raw["bus_projections"] == {}
    assert (
        len(
            [
                event
                for event in panel.bus.read_events()
                if event.kind == "panel.field_updated"
            ]
        )
        == 5
    )


def test_owner_rollover_does_not_change_other_owner_visibility(tmp_path):
    now = [datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)]
    first = store(tmp_path, now)
    first.set_field(
        "daily_b",
        "old",
        owner="owner_b",
        source="fixture",
        ttl=timedelta(days=3),
        source_event_id="event_b",
    )
    now[0] += timedelta(days=1)
    second = store(tmp_path, now)
    before = first.snapshot(owner="owner_b")
    before_from_second_process = second.snapshot(owner="owner_b")
    first.rollover(owner="owner_a")
    after = first.snapshot(owner="owner_b")
    after_from_second_process = second.snapshot(owner="owner_b")

    assert before["fields"] == before_from_second_process["fields"]
    assert before["fields"] == after["fields"] == after_from_second_process["fields"]
    assert before["owner_epochs"]["owner_b"] == after["owner_epochs"]["owner_b"]


def test_consume_once_same_source_is_persisted_and_idempotent(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "one_shot",
        "value",
        source="fixture",
        ttl=timedelta(hours=1),
        consume_once=True,
        source_event_id="source_event",
    )

    first = panel.consume_once("one_shot", source_event_id="source_event")
    before = panel.path.read_bytes()
    second = panel.consume_once("one_shot", source_event_id="source_event")
    assert first.consumed_source_event_id == "source_event"
    assert second == first
    assert panel.path.read_bytes() == before
    assert panel.snapshot()["fields"] == {}
    with pytest.raises(ValueError, match="does not match"):
        panel.consume_once("one_shot", source_event_id="other_consumer")


def test_expired_consume_once_cannot_be_claimed(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "expired_one_shot",
        "value",
        source="fixture",
        ttl=timedelta(minutes=1),
        consume_once=True,
        source_event_id="expired_source",
    )
    now[0] += timedelta(minutes=2)
    with pytest.raises(ValueError, match="expired"):
        panel.consume_once("expired_one_shot", source_event_id="expired_source")


def test_verified_afterglow_is_accepted_and_same_event_does_not_refresh_ttl(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    effect, receipt = verified_effect(tmp_path, now)
    panel.record_activity_afterglow(
        effect_record=effect,
        effect_receipt=receipt,
        canonical_event_id="event_fixture",
        summary="topic",
    )
    before = json.loads(panel.path.read_text(encoding="utf-8"))["fields"]
    record_before = next(iter(before.values()))
    now[0] += timedelta(minutes=20)

    panel.record_activity_afterglow(
        effect_record=effect,
        effect_receipt=receipt,
        canonical_event_id="event_fixture",
        summary="topic",
    )
    record_after = next(
        iter(json.loads(panel.path.read_text(encoding="utf-8"))["fields"].values())
    )
    assert record_after["expires_at"] == record_before["expires_at"]
    assert panel.snapshot()["fields"]["activity_afterglow"]["value"] == {
        "event_id": "event_fixture",
        "summary": "topic",
    }


def test_unverified_or_mismatched_afterglow_does_not_write(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    ledger = EffectLedger(tmp_path, clock=lambda: now[0])
    effect = ledger.begin_intent(
        effect_id="effect_pending",
        kind="delivery_receipt",
        source_event_id="event_pending",
        idempotency_key="idempotency_pending",
        epoch_id="2026-08-22",
        content_sha256="b" * 64,
        content_length=2,
        created_at=now[0],
        expires_at=now[0] + timedelta(hours=1),
    )
    with pytest.raises((TypeError, ValueError), match="verified|receipt"):
        panel.record_activity_afterglow(
            effect_record=effect,
            effect_receipt=None,
            canonical_event_id="event_pending",
            summary="no",
        )
    assert not panel.path.exists()

    verified, receipt = verified_effect(tmp_path, now, event_id="event_verified")
    with pytest.raises(ValueError, match="canonical|match"):
        panel.record_activity_afterglow(
            effect_record=verified,
            effect_receipt=receipt,
            canonical_event_id="wrong",
            summary="no",
        )
    assert "activity_afterglow" not in panel.snapshot()["fields"]


def test_verified_wrong_kind_afterglow_does_not_write(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    effect, receipt = verified_effect(
        tmp_path,
        now,
        event_id="event_wrong_kind",
        kind="delivery_receipt",
    )

    with pytest.raises(ValueError, match="autonomy_completion"):
        panel.record_activity_afterglow(
            effect_record=effect,
            effect_receipt=receipt,
            canonical_event_id="event_wrong_kind",
            summary="no",
        )
    assert not panel.path.exists()


def test_v1_migration_is_explicit_and_keeps_backup(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    legacy = {
        "schema_version": "moon.panel.v1",
        "epoch": "2026-08-22",
        "fields": {
            "legacy_field": {
                "value": "old",
                "observed_at": now[0].isoformat(),
                "expires_at": (now[0] + timedelta(hours=1)).isoformat(),
                "confidence": 0.8,
                "source": "fixture",
                "daily": True,
            }
        },
    }
    atomic_json_write(panel.path, legacy)
    before = panel.path.read_bytes()
    with pytest.raises(StateError, match="migrate_v1"):
        panel.snapshot()
    assert panel.path.read_bytes() == before

    migrated = panel.migrate_v1({"legacy_field": "owner_legacy"})
    assert migrated["schema_version"] == PANEL_SCHEMA
    assert migrated["fields"]["owner_legacy/legacy_field"]["owner"] == "owner_legacy"
    assert json.loads(panel.backup_path.read_text(encoding="utf-8")) == legacy


def test_v1_migration_reuses_identical_backup_but_rejects_conflict(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    legacy = {
        "schema_version": "moon.panel.v1",
        "epoch": "2026-08-22",
        "fields": {
            "legacy_field": {
                "value": "old",
                "observed_at": now[0].isoformat(),
                "expires_at": (now[0] + timedelta(hours=1)).isoformat(),
                "confidence": 0.8,
                "source": "fixture",
                "daily": True,
            }
        },
    }
    atomic_json_write(panel.path, legacy)
    panel.migrate_v1({"legacy_field": "owner_legacy"})
    backup_before = panel.backup_path.read_bytes()

    atomic_json_write(panel.path, legacy)
    panel.migrate_v1({"legacy_field": "owner_legacy"})
    assert panel.backup_path.read_bytes() == backup_before

    conflict = json.loads(json.dumps(legacy))
    conflict["fields"]["legacy_field"]["value"] = "different"
    atomic_json_write(panel.path, conflict)
    with pytest.raises(StateError, match="backup conflicts"):
        panel.migrate_v1({"legacy_field": "owner_legacy"})
    assert json.loads(panel.path.read_text(encoding="utf-8")) == conflict


def test_bus_failure_after_panel_write_is_honest(tmp_path, monkeypatch):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)

    def fail(*_args, **_kwargs):
        raise OSError("event bus fixture")

    original_emit = panel.bus.emit
    monkeypatch.setattr(panel.bus, "emit", fail)
    with pytest.raises(OSError, match="event bus fixture"):
        panel.set_field(
            "durable",
            "written",
            source="fixture",
            ttl=timedelta(hours=1),
            source_event_id="event_write",
        )
    before_retry = json.loads(panel.path.read_text(encoding="utf-8"))
    before_record = next(
        record
        for record in before_retry["fields"].values()
        if record["name"] == "durable"
    )
    monkeypatch.setattr(panel.bus, "emit", original_emit)
    panel.set_field(
        "durable",
        "written",
        source="fixture",
        ttl=timedelta(hours=4),
        source_event_id="event_write",
    )
    after_retry = json.loads(panel.path.read_text(encoding="utf-8"))
    after_record = next(
        record
        for record in after_retry["fields"].values()
        if record["name"] == "durable"
    )
    assert after_record["value"] == before_record["value"] == "written"
    assert after_record["expires_at"] == before_record["expires_at"]
    assert [event.kind for event in panel.bus.read_events()].count(
        "panel.field_updated"
    ) == 1


def test_consume_bus_failure_retries_projection_without_rewriting_value(
    tmp_path, monkeypatch
):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "one_shot",
        "value",
        source="fixture",
        ttl=timedelta(hours=1),
        consume_once=True,
        source_event_id="consume_event",
    )

    def fail(*_args, **_kwargs):
        raise OSError("event bus fixture")

    original_emit = panel.bus.emit
    monkeypatch.setattr(panel.bus, "emit", fail)
    with pytest.raises(OSError, match="event bus fixture"):
        panel.consume_once("one_shot", source_event_id="consume_event")
    failed = json.loads(panel.path.read_text(encoding="utf-8"))
    failed_record = next(iter(failed["fields"].values()))
    consumed_at = failed_record["consumed_at"]

    now[0] += timedelta(days=1)
    monkeypatch.setattr(panel.bus, "emit", original_emit)
    retried = panel.consume_once("one_shot", source_event_id="consume_event")
    recovered = json.loads(panel.path.read_text(encoding="utf-8"))
    recovered_record = next(iter(recovered["fields"].values()))

    assert retried.consumed_at == datetime.fromisoformat(
        consumed_at.replace("Z", "+00:00")
    )
    assert recovered_record["value"] == failed_record["value"] == "value"
    assert recovered_record["expires_at"] == failed_record["expires_at"]
    assert recovered_record["consumed_at"] == consumed_at
    assert [event.kind for event in panel.bus.read_events()].count(
        "panel.field_consumed"
    ) == 1


def test_atomic_write_failure_does_not_create_partial_panel(tmp_path, monkeypatch):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    monkeypatch.setattr(
        "moonbite_plugin.panel.atomic_json_write",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("write fixture")),
    )
    with pytest.raises(OSError, match="write fixture"):
        panel.set_field("field", "value", source="fixture", ttl=timedelta(hours=1))
    assert not panel.path.exists()


def test_concurrent_owner_writes_preserve_both_records(tmp_path):
    now = [datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)]
    first = store(tmp_path, now)
    second = store(tmp_path, now)

    def write(panel, owner, value):
        panel.set_field(
            "same_name",
            value,
            owner=owner,
            source="fixture",
            ttl=timedelta(hours=1),
            source_event_id=f"event_{owner}",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(
            executor.map(
                lambda args: write(*args),
                [(first, "owner_a", "a"), (second, "owner_b", "b")],
            )
        )
    assert {
        record["owner"]
        for record in json.loads(first.path.read_text(encoding="utf-8"))[
            "fields"
        ].values()
    } == {"owner_a", "owner_b"}


@pytest.mark.parametrize(
    ("field", "value"),
    [("confidence", "0.8"), ("confidence", True), ("daily", "false")],
)
def test_panel_does_not_coerce_persisted_scalar_types(field, value, tmp_path):
    now = [datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    raw = {
        "schema_version": PANEL_SCHEMA,
        "owner": "default",
        "name": "broken",
        "value": "fixture",
        "observed_at": now[0].isoformat(),
        "expires_at": (now[0] + timedelta(hours=1)).isoformat(),
        "confidence": 0.8,
        "source": "fixture",
        "source_event_id": "event_broken",
        "epoch": "2026-08-22",
        "reset_policy": "persistent",
        "persistence_policy": "ttl",
        "consume_policy": "repeatable",
        "consumed_source_event_id": None,
        "consumed_at": None,
        "daily": False,
    }
    raw[field] = value
    atomic_json_write(
        panel.path,
        {
            "schema_version": PANEL_SCHEMA,
            "epoch": "2026-08-22",
            "fields": {"broken": raw},
        },
    )
    with pytest.raises(StateError, match="panel field"):
        panel.snapshot()


def _fact_mapping_keys(value):
    if isinstance(value, dict):
        keys = set(value)
        for child in value.values():
            keys.update(_fact_mapping_keys(child))
        return keys
    if isinstance(value, list):
        keys = set()
        for child in value:
            keys.update(_fact_mapping_keys(child))
        return keys
    return set()


class _HostileMapping(Mapping):
    def __getitem__(self, _key):
        raise AssertionError("observer traversed a private child")

    def __iter__(self):
        raise AssertionError("observer iterated a private child")

    def __len__(self):
        raise AssertionError("observer measured a private child")


def test_panel_observer_does_not_traverse_value_or_event_payload():
    now = datetime(2026, 8, 24, 19, 0, tzinfo=timezone.utc)
    field = {
        "schema_version": PANEL_SCHEMA,
        "owner": "default",
        "name": "private",
        "value": _HostileMapping(),
        "observed_at": now.isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "confidence": 1.0,
        "source": "fixture",
        "source_event_id": "event_private",
        "epoch": "2026-08-24",
        "reset_policy": "daily",
        "persistence_policy": "ttl",
        "consume_policy": "repeatable",
        "consumed_source_event_id": None,
        "consumed_at": None,
        "daily": True,
    }
    state = {
        "schema_version": PANEL_SCHEMA,
        "epoch": "2026-08-24",
        "owner_epochs": {"default": "2026-08-24"},
        "fields": {"private": field},
        "bus_projections": {},
    }
    assert _observer_state_metadata(state)["field_count"] == 1
    event = {
        "schema_version": "moon.event.v1",
        "event_id": "event_private",
        "created_at": now.isoformat(),
        "kind": "panel.field_updated",
        "source": "fixture",
        "payload": _HostileMapping(),
    }
    assert PanelStore._observer_event(event)[:3] == (
        "event_private",
        "panel.field_updated",
        "fixture",
    )


def test_panel_observer_pristine_and_existing_state_are_lock_free(
    tmp_path, monkeypatch
):
    now = [datetime(2026, 8, 24, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    before = sorted(
        (
            path.relative_to(tmp_path).as_posix(),
            path.stat().st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
        for path in tmp_path.rglob("*")
    )
    assert panel.observer_status(target_date=date(2026, 8, 24), now=now[0]) == ()
    assert (
        sorted(
            (
                path.relative_to(tmp_path).as_posix(),
                path.stat().st_mtime_ns,
                path.read_bytes() if path.is_file() else None,
            )
            for path in tmp_path.rglob("*")
        )
        == before
    )

    panel.set_field(
        "observer",
        {"body": "private", "summary": "private"},
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="observer_event",
    )
    before = sorted(
        (
            path.relative_to(tmp_path).as_posix(),
            path.stat().st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
        for path in tmp_path.rglob("*")
    )

    def fail_lock(*_args, **_kwargs):
        raise AssertionError("panel observer acquired a lock")

    def fail_write(*_args, **_kwargs):
        raise AssertionError("panel observer wrote state")

    monkeypatch.setattr("moonbite_plugin.panel.file_lock", fail_lock)
    monkeypatch.setattr("moonbite_plugin.panel.atomic_json_write", fail_write)
    facts = panel.observer_status(target_date=date(2026, 8, 24), now=now[0])
    assert facts
    state_fact = next(fact for fact in facts if fact.key == "panel:state")
    integrity_fact = next(fact for fact in facts if fact.key == "panel:integrity")
    assert state_fact.event_time is None
    assert integrity_fact.event_time is None
    assert (
        sorted(
            (
                path.relative_to(tmp_path).as_posix(),
                path.stat().st_mtime_ns,
                path.read_bytes() if path.is_file() else None,
            )
            for path in tmp_path.rglob("*")
        )
        == before
    )
    forbidden = {"body", "message", "content", "summary", "payload", "value", "output"}
    assert not forbidden & _fact_mapping_keys([fact.to_dict() for fact in facts])


def test_panel_observer_delivered_projection_requires_event_evidence(tmp_path):
    now = [datetime(2026, 8, 24, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "observer",
        "private",
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="observer_event",
    )
    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    projection = panel._new_projection(
        kind="panel.field_updated",
        source="fixture",
        field="delivered",
        owner="default",
        source_event_id="delivered_source",
        expires_at=(now[0] + timedelta(hours=1)).isoformat(),
    )
    projection["status"] = "delivered"
    raw["bus_projections"][projection["event_id"]] = projection
    atomic_json_write(panel.path, raw)

    missing = next(
        fact
        for fact in panel.observer_status(
            target_date=date(2026, 8, 24), now=now[0] + timedelta(minutes=5)
        )
        if fact.key.endswith(projection["event_id"])
    )
    assert missing.state == "current"
    assert missing.code == "panel_projection_delivery_evidence_missing"
    assert missing.event_time is None

    event_created_at = now[0] + timedelta(minutes=2)
    now[0] = event_created_at
    event = panel.bus.emit(
        projection["kind"],
        source=projection["source"],
        event_id=projection["event_id"],
        payload=projection["payload"],
    )
    delivered = next(
        fact
        for fact in panel.observer_status(
            target_date=date(2026, 8, 24), now=event_created_at + timedelta(minutes=5)
        )
        if fact.key.endswith(projection["event_id"])
    )
    assert delivered.state == "neutral"
    assert delivered.code == "panel_projection_delivered"
    assert delivered.event_time == event.created_at == event_created_at


@pytest.mark.parametrize("status", ["pending", "delivered"])
def test_panel_observer_projection_payload_conflict_is_integrity(tmp_path, status):
    now = [datetime(2026, 8, 24, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "observer",
        "private",
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="observer_event",
    )
    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    projection = panel._new_projection(
        kind="panel.field_updated",
        source="fixture",
        field="payload_conflict",
        owner="default",
        source_event_id="payload_conflict_source",
        expires_at=(now[0] + timedelta(hours=1)).isoformat(),
    )
    projection["status"] = status
    raw["bus_projections"][projection["event_id"]] = projection
    atomic_json_write(panel.path, raw)
    panel.bus.emit(
        projection["kind"],
        source=projection["source"],
        event_id=projection["event_id"],
        payload={**projection["payload"], "field": "not_canonical"},
    )

    facts = panel.observer_status(target_date=date(2026, 8, 24), now=now[0])
    integrity = next(fact for fact in facts if fact.key == "panel:integrity")
    assert integrity.state == "current"
    assert integrity.code.startswith("panel_integrity_error:")
    assert not any(fact.key.endswith(projection["event_id"]) for fact in facts)


def test_panel_observer_pending_projection_recovery_and_corrupt_state(tmp_path):
    now = [datetime(2026, 8, 24, 19, 0, tzinfo=timezone.utc)]
    panel = store(tmp_path, now)
    panel.set_field(
        "observer",
        "private",
        source="fixture",
        ttl=timedelta(hours=1),
        source_event_id="observer_event",
    )
    raw = json.loads(panel.path.read_text(encoding="utf-8"))
    projection = panel._new_projection(
        kind="panel.field_updated",
        source="fixture",
        field="pending",
        owner="default",
        source_event_id="pending_source",
        expires_at=(now[0] + timedelta(hours=1)).isoformat(),
    )
    raw["bus_projections"][projection["event_id"]] = projection
    atomic_json_write(panel.path, raw)
    facts = panel.observer_status(target_date=date(2026, 8, 24), now=now[0])
    pending = next(fact for fact in facts if fact.key.endswith(projection["event_id"]))
    assert pending.state == "current"
    assert pending.code == "panel_projection_pending"
    assert pending.target_date == date(2026, 8, 24)
    assert pending.event_time is None
    delivery_time = now[0] + timedelta(minutes=1)
    now[0] = delivery_time
    event = panel.bus.emit(
        projection["kind"],
        source=projection["source"],
        event_id=projection["event_id"],
        payload=projection["payload"],
    )
    recovered = next(
        fact
        for fact in panel.observer_status(
            target_date=date(2026, 8, 24), now=delivery_time
        )
        if fact.key.endswith(projection["event_id"])
    )
    assert recovered.state == "recovered_history"
    assert recovered.recovery is not None
    assert recovered.recovery.ref == projection["event_id"]
    assert recovered.event_time == event.created_at == delivery_time

    panel.path.write_text("{broken", encoding="utf-8")
    corrupt = panel.observer_status(target_date=date(2026, 8, 24), now=now[0])
    assert any(fact.state == "current" and "integrity" in fact.key for fact in corrupt)
