from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, tzinfo

import pytest

from moonbite_plugin.control import CONTROL_SCHEMA, ControlStore, evaluate_gate
from moonbite_plugin.runtime_core import JsonlLedger, StateError


class Clock:
    def __init__(self):
        self.now = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)

    def __call__(self):
        return self.now


def test_operator_control_wins_over_newer_self_control(tmp_path):
    clock = Clock()
    store = ControlStore(tmp_path, clock=clock)
    operator = store.put(feature="autonomy", mode="pause", source="operator")
    clock.now += timedelta(minutes=1)
    store.put(
        feature="autonomy",
        mode="play_next",
        source="self",
        expires_at=clock.now + timedelta(hours=1),
    )
    resolved = store.resolve("autonomy")
    assert resolved.intent == operator
    assert evaluate_gate(resolved).allowed is False


def test_background_bundle_controls_both_costly_features(tmp_path):
    store = ControlStore(tmp_path)
    store.put(feature="background_costly", mode="quota_save", source="operator")
    assert evaluate_gate(store.resolve("heartbeat")).allowed is False
    assert evaluate_gate(store.resolve("autonomy")).allowed is False


def test_background_costly_is_one_canonical_proactive_state(tmp_path):
    store = ControlStore(tmp_path)
    first = store.put(feature="background_costly", mode="quota_save", source="operator")
    second = store.put(feature="proactive", mode="pause", source="operator")

    assert first.feature == "proactive"
    assert second.feature == "proactive"
    assert [intent.control_id for intent in store.active()] == [second.control_id]
    assert store.resolve("background_costly").intent == second
    assert ControlStore(tmp_path).resolve("proactive").intent == second

    store.clear(feature="background_costly", source="operator")
    assert store.resolve("proactive").intent is None
    assert ControlStore(tmp_path).resolve("proactive").intent is None


def test_operator_proactive_pause_outranks_newer_child_control(tmp_path):
    clock = Clock()
    store = ControlStore(tmp_path, clock=clock)
    maintenance = store.put(feature="proactive", mode="pause", source="operator")
    clock.now += timedelta(minutes=1)
    store.put(feature="autonomy", mode="play_next", source="operator")

    assert store.resolve("autonomy").intent == maintenance


def test_self_control_requires_bounded_expiry(tmp_path):
    store = ControlStore(tmp_path)
    with pytest.raises(ValueError, match="require expires_at"):
        store.put(feature="autonomy", mode="rest", source="self")


def test_consumed_one_shot_disappears(tmp_path):
    store = ControlStore(tmp_path)
    intent = store.put(feature="autonomy", mode="play_next", source="operator")
    assert store.resolve("autonomy").intent == intent
    store.consume(intent.control_id)
    assert store.resolve("autonomy").intent is None


def test_resolve_uses_one_active_snapshot(tmp_path, monkeypatch):
    store = ControlStore(tmp_path)
    original_active = store.active
    calls = 0

    def recording_active(*, now=None):
        nonlocal calls
        calls += 1
        return original_active(now=now)

    monkeypatch.setattr(store, "active", recording_active)

    store.resolve("autonomy")

    assert calls == 1


def test_corrupt_control_row_is_normalized_to_state_error(tmp_path):
    JsonlLedger(tmp_path / "controls.jsonl").append(
        {
            "schema_version": "moon.runtime_control.v1",
            "action": "put",
            "control": {"control_id": "missing-required-fields"},
        }
    )

    with pytest.raises(StateError, match="controls.jsonl row 1 is invalid"):
        ControlStore(tmp_path).resolve("autonomy")


def test_non_mapping_control_payload_is_rejected(tmp_path):
    store = ControlStore(tmp_path)
    intent = store.put(feature="autonomy", mode="pause", source="operator")
    row = store.ledger.rows()[0]
    row["control"] = {**intent.to_dict(), "payload": ["not", "an", "object"]}
    store.ledger.path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    with pytest.raises(StateError, match="controls.jsonl row 1 is invalid"):
        store.resolve("autonomy")


def test_observer_active_pristine_does_not_create_directory_or_lock(tmp_path):
    store = ControlStore(tmp_path)
    before = sorted(
        path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")
    )
    assert store.observer_active(now=datetime(2026, 8, 22, 19, tzinfo=UTC)) == []
    assert (
        sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
        == before
    )
    assert not store.ledger.path.exists()
    assert not store.ledger.lock_path.exists()


def test_observer_active_reads_existing_ledger_without_lock_or_expire_append(tmp_path):
    clock = Clock()
    store = ControlStore(tmp_path, clock=clock)
    active = store.put(feature="autonomy", mode="pause", source="operator")
    store.put(
        feature="heartbeat",
        mode="rest",
        source="self",
        expires_at=clock.now + timedelta(minutes=1),
    )
    before = store.ledger.path.read_bytes()
    before_mtime = store.ledger.path.stat().st_mtime_ns
    if store.ledger.lock_path.exists():
        store.ledger.lock_path.unlink()
    result = store.observer_active(now=clock.now + timedelta(minutes=2))
    assert [item.control_id for item in result] == [active.control_id]
    assert store.ledger.path.read_bytes() == before
    assert store.ledger.path.stat().st_mtime_ns == before_mtime
    assert not store.ledger.lock_path.exists()


def test_observer_active_corrupt_ledger_fails_closed_without_write(tmp_path):
    path = tmp_path / "controls.jsonl"
    path.write_text("{broken\n", encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(StateError):
        ControlStore(tmp_path).observer_active(
            now=datetime(2026, 8, 22, 19, tzinfo=UTC)
        )
    assert path.read_bytes() == before
    assert not (tmp_path / "controls.jsonl.lock").exists()


def test_observer_active_requires_aware_now_even_when_pristine(tmp_path):
    store = ControlStore(tmp_path)
    with pytest.raises(ValueError, match="timezone-aware"):
        store.observer_active(now=datetime(2026, 8, 22, 19))  # noqa: DTZ001

    class HostileTimezone(tzinfo):
        def utcoffset(self, _value):
            raise RuntimeError("hostile timezone")

    with pytest.raises(ValueError, match="timezone-aware"):
        store.observer_active(now=datetime(2026, 8, 22, 19, tzinfo=HostileTimezone()))
    assert not store.ledger.path.exists()
    assert not store.ledger.lock_path.exists()


@pytest.mark.parametrize("missing", ["event_id", "created_at"])
def test_malformed_clear_cannot_clear_pause(missing, tmp_path):
    clock = Clock()
    store = ControlStore(tmp_path, clock=clock)
    intent = store.put(feature="autonomy", mode="pause", source="operator")
    assert store.active()[0].control_id == intent.control_id
    row = {
        "schema_version": CONTROL_SCHEMA,
        "event_id": "malformed-clear",
        "created_at": clock.now.isoformat().replace("+00:00", "Z"),
        "action": "clear",
        "feature": "autonomy",
        "source": "operator",
    }
    row.pop(missing)
    store.ledger.append(row)

    with pytest.raises(StateError):
        store.resolve("autonomy")
    with pytest.raises(StateError):
        store.observer_active(now=clock.now)


def test_unknown_clear_fails_closed(tmp_path):
    store = ControlStore(tmp_path)
    store.ledger.append(
        {
            "schema_version": CONTROL_SCHEMA,
            "event_id": "unknown-clear",
            "created_at": "2026-08-22T19:00:00Z",
            "action": "clear",
            "feature": "not-a-feature",
            "source": "operator",
        }
    )
    with pytest.raises(StateError):
        store.resolve("autonomy")


@pytest.mark.parametrize(
    "action_fields",
    [
        ("consume", {"control_id": ""}),
        ("consume", {"control_id": 7}),
        ("expire", {"control_ids": "control-1"}),
        ("expire", {"control_ids": [7]}),
        ("expire", {"control_ids": ["control-1", "control-1"]}),
    ],
)
def test_bad_consume_and_expire_rows_fail_closed(action_fields, tmp_path):
    action, fields = action_fields
    store = ControlStore(tmp_path)
    store.ledger.append(
        {
            "schema_version": CONTROL_SCHEMA,
            "event_id": "bad-terminal",
            "created_at": "2026-08-22T19:00:00Z",
            "action": action,
            **fields,
        }
    )
    with pytest.raises(StateError):
        store.resolve("autonomy")


def test_duplicate_event_id_fails_closed_in_both_replays(tmp_path):
    store = ControlStore(tmp_path)
    store.put(feature="autonomy", mode="pause", source="operator")
    store.ledger.append(dict(store.ledger.rows()[0]))
    with pytest.raises(StateError, match="duplicate event_id"):
        store.resolve("autonomy")
    with pytest.raises(StateError, match="duplicate event_id"):
        store.observer_active(now=datetime(2026, 8, 22, 19, tzinfo=UTC))


def test_observer_redacts_payload_but_active_preserves_it(tmp_path):
    body = "observer payload body must not escape"
    store = ControlStore(tmp_path)
    store.put(
        feature="autonomy",
        mode="pause",
        source="operator",
        payload={"body": body, "nested": {"body": body}},
    )

    active = store.active()
    observed = store.observer_active(now=datetime(2026, 8, 22, 19, tzinfo=UTC))
    assert active[0].payload["body"] == body
    assert observed[0].payload == {}
    assert body not in repr(observed[0])
    assert body not in repr(observed)
