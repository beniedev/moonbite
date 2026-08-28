from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
import stat

import pytest

from moonbite_plugin.runtime_core import (
    EventBus,
    FileRuntimeLocks,
    JsonlLedger,
    StateError,
    try_file_lock,
)


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=timezone.utc)


def hold_process_lock(path, ready, release):
    with try_file_lock(path) as acquired:
        ready.put(acquired)
        release.get(timeout=10)


def test_event_and_audit_ledgers_round_trip(tmp_path):
    bus = EventBus(tmp_path, clock=lambda: NOW)
    event = bus.emit("sensor.fixture", source="test", payload={"value": 7})
    audit = bus.record_audit("fixture", status="completed", source="test")

    assert bus.read_events() == [event]
    assert bus.read_audit() == [audit]
    assert event.created_at == NOW


def test_corrupt_ledger_fails_closed(tmp_path):
    (tmp_path / "events.jsonl").write_text("not-json\n", encoding="utf-8")
    with pytest.raises(StateError, match="invalid JSON"):
        EventBus(tmp_path).read_events()


def test_concurrent_ledger_append_keeps_every_complete_row(tmp_path):
    ledger = JsonlLedger(tmp_path / "concurrent.jsonl")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda value: ledger.append({"value": value}), range(80)))

    assert sorted(row["value"] for row in ledger.rows()) == list(range(80))


def test_execution_lock_is_nonblocking_across_processes(tmp_path):
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    release = context.Queue()
    lock_path = tmp_path / "execution.lock"
    worker = context.Process(target=hold_process_lock, args=(lock_path, ready, release))
    worker.start()
    assert ready.get(timeout=10) is True

    with try_file_lock(lock_path) as acquired:
        assert acquired is False

    release.put(True)
    worker.join(timeout=10)
    assert worker.exitcode == 0


def test_named_runtime_locks_keep_suffix_and_reject_unsafe_names(tmp_path):
    locks = FileRuntimeLocks(tmp_path)

    with locks.try_exclusive("memory_resurface.request") as acquired:
        assert acquired is True
    assert (tmp_path / "memory_resurface.request.lock").exists()

    for name in ("", ".", "..", "a/b", r"a\b", "a b"):
        with pytest.raises(ValueError, match="unsafe runtime lock name"):
            locks.try_exclusive(name)


def test_state_directory_and_files_are_owner_only(tmp_path):
    state_root = tmp_path / "state"
    bus = EventBus(state_root, clock=lambda: NOW)
    bus.emit("sensor.fixture", source="test")

    assert stat.S_IMODE(state_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(bus.events.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(bus.events.lock_path.stat().st_mode) == 0o600


def test_short_write_is_retried_until_the_row_is_complete(tmp_path, monkeypatch):
    ledger = JsonlLedger(tmp_path / "short.jsonl")
    original_write = os.write
    calls = 0

    def short_write(descriptor, value):
        nonlocal calls
        calls += 1
        return original_write(descriptor, value[: max(1, len(value) // 2)])

    monkeypatch.setattr("moonbite_plugin.runtime_core.os.write", short_write)

    ledger.append({"value": "fixture" * 20})

    assert calls > 1
    assert ledger.rows() == [{"value": "fixture" * 20}]


def test_invalid_event_shape_is_normalized_to_state_error(tmp_path):
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    ledger.append(
        {
            "schema_version": "wrong",
            "event_id": "event_fixture",
            "created_at": NOW.isoformat(),
            "kind": "fixture",
            "source": "test",
            "payload": {},
        }
    )

    with pytest.raises(StateError, match="event envelope is invalid"):
        EventBus(tmp_path).read_events()


def test_event_payload_size_is_bounded_before_persistence(tmp_path):
    bus = EventBus(tmp_path)

    with pytest.raises(ValueError, match="event payload exceeds"):
        bus.emit("fixture", source="test", payload={"text": "x" * (256 * 1024)})

    assert bus.read_events() == []
