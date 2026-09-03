from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from moonbite_plugin import (
    REQUIRED_STATE_DOMAINS,
    RESERVED_STATE_DOMAINS,
    RUNTIME_COMPONENTS_SCHEMA,
    RuntimeComponents,
    RuntimeComponentsError,
)
from moonbite_plugin.effects import EffectLedger
from moonbite_plugin.runtime_core import EventBus, FileRuntimeLocks
from moonbite_plugin.session import SessionLifecycleStore


class SyntheticLocks:
    def exclusive(self, _name):
        raise AssertionError("synthetic lock must not be acquired during validation")

    def try_exclusive(self, _name):
        raise AssertionError("synthetic lock must not be acquired during validation")


class SyntheticPanel:
    def __init__(self, bus):
        self.bus = bus


class SyntheticSession:
    def record_hook(self, *_args, **_kwargs):
        return None

    def record_host_turn_end(self, *_args, **_kwargs):
        return None

    def record_host_child_stop(self, *_args, **_kwargs):
        return None

    def record_host_shutdown(self, *_args, **_kwargs):
        return None

    def record_host_finalize(self, *_args, **_kwargs):
        return None

    def snapshot(self, *_args, **_kwargs):
        return None

    def replay(self, *_args, **_kwargs):
        return ()


class SyntheticEffects:
    def begin_intent(self, *_args, **_kwargs):
        return None

    def mark_pending(self, *_args, **_kwargs):
        return None

    def mark_queue_accepted(self, *_args, **_kwargs):
        return None

    def verify(self, *_args, **_kwargs):
        return None

    def fail(self, *_args, **_kwargs):
        return None

    def expire(self, *_args, **_kwargs):
        return None

    def requeue(self, *_args, **_kwargs):
        return None

    def get(self, *_args, **_kwargs):
        return None

    def find_by_idempotency(self, *_args, **_kwargs):
        return None

    def records(self, *_args, **_kwargs):
        return ()

    def pending_for_reconciliation(self, *_args, **_kwargs):
        return []


def bundle_kwargs(tmp_path: Path) -> dict[str, object]:
    bus = object()
    return {
        "schema_version": RUNTIME_COMPONENTS_SCHEMA,
        "owner_id": "fixture-owner",
        "mode": "injected",
        "owned_domains": REQUIRED_STATE_DOMAINS,
        "reserved_domains": RESERVED_STATE_DOMAINS,
        "writer_count": 1,
        "local_writes": False,
        "bus": bus,
        "controls": object(),
        "cadence": object(),
        "panel": SyntheticPanel(bus),
        "memory": object(),
        "session": SyntheticSession(),
        "effects": SyntheticEffects(),
        "locks": SyntheticLocks(),
        "state_root": None,
    }


def test_standalone_uses_one_bus_and_does_not_create_state_root(tmp_path):
    root = tmp_path / "state"

    bundle = RuntimeComponents.standalone(root, "UTC", 6)

    assert bundle.schema_version == RUNTIME_COMPONENTS_SCHEMA
    assert bundle.owner_id == "moonbite-standalone"
    assert bundle.mode == "standalone"
    assert bundle.owned_domains == REQUIRED_STATE_DOMAINS
    assert bundle.reserved_domains == RESERVED_STATE_DOMAINS
    assert bundle.writer_count == 1
    assert bundle.local_writes is True
    assert bundle.bus is bundle.panel.bus
    assert bundle.state_root is root
    assert isinstance(bundle.bus, EventBus)
    assert isinstance(bundle.locks, FileRuntimeLocks)
    assert bundle.controls.ledger.path == root / "controls.jsonl"
    assert bundle.cadence.path == root / "heartbeat_cadence.json"
    assert bundle.panel.path == root / "panel.json"
    assert bundle.memory.cards.path == root / "memory_cards.jsonl"
    assert isinstance(bundle.session, SessionLifecycleStore)
    assert bundle.session.ledger.path == root / "session_lifecycle.jsonl"
    assert isinstance(bundle.effects, EffectLedger)
    assert bundle.effects.ledger.path == root / "effects.jsonl"
    assert bundle.locks.root == root
    assert bundle.effect_ledger.find_by_idempotency("missing") is None
    assert bundle.effect_ledger.records() == ()
    assert not root.exists()


def test_standalone_passes_anchor_timezone_to_cadence(tmp_path):
    bundle = RuntimeComponents.standalone(tmp_path / "state", "America/Vancouver", 9)

    assert bundle.cadence.timezone_name == "America/Vancouver"
    assert bundle.cadence.anchor_hour == 9
    assert (
        bundle.cadence.daily_anchor_epoch(datetime(2026, 7, 1, 16, 0, tzinfo=UTC))
        == "2026-07-01"
    )


def test_previous_runtime_components_schema_is_rejected(tmp_path):
    values = bundle_kwargs(tmp_path)
    values["schema_version"] = "moon.runtime_components.v2"

    with pytest.raises(RuntimeComponentsError, match="unsupported runtime components"):
        RuntimeComponents(**values)


def test_injected_preserves_supplied_identities_without_creating_state_root(tmp_path):
    root = tmp_path / "host-state"
    values = bundle_kwargs(tmp_path)
    supplied = {
        name: values[name]
        for name in (
            "bus",
            "controls",
            "cadence",
            "panel",
            "memory",
            "session",
            "effects",
            "locks",
        )
    }

    bundle = RuntimeComponents.injected(
        "fixture-host-owner",
        **supplied,
        state_root=root,
    )

    assert bundle.owner_id == "fixture-host-owner"
    assert bundle.mode == "injected"
    assert bundle.local_writes is False
    assert bundle.state_root is root
    for name, value in supplied.items():
        assert getattr(bundle, name) is value
    assert bundle.effect_ledger is bundle.effects
    assert bundle.effect.find_by_idempotency("missing") is None
    assert bundle.effect.records() == ()
    assert not root.exists()


@pytest.mark.parametrize("method", ["find_by_idempotency", "records"])
def test_effect_owner_requires_public_read_ports(tmp_path, method):
    values = bundle_kwargs(tmp_path)
    effects = SyntheticEffects()
    setattr(effects, method, None)
    values["effects"] = effects

    with pytest.raises(RuntimeComponentsError, match=method):
        RuntimeComponents(**values)


@pytest.mark.parametrize(
    "method",
    [
        "record_host_turn_end",
        "record_host_child_stop",
        "record_host_shutdown",
        "record_host_finalize",
    ],
)
def test_session_owner_requires_canonical_terminal_ports(tmp_path, method):
    values = bundle_kwargs(tmp_path)
    session = SyntheticSession()
    setattr(session, method, None)
    values["session"] = session

    with pytest.raises(RuntimeComponentsError, match=method):
        RuntimeComponents(**values)


@pytest.mark.parametrize("missing", sorted(REQUIRED_STATE_DOMAINS))
def test_missing_owned_domain_is_rejected(tmp_path, missing):
    values = bundle_kwargs(tmp_path)
    values["owned_domains"] = REQUIRED_STATE_DOMAINS - {missing}

    with pytest.raises(RuntimeComponentsError, match="owned state domains"):
        RuntimeComponents(**values)


@pytest.mark.parametrize(
    "component",
    [
        "bus",
        "controls",
        "cadence",
        "panel",
        "memory",
        "session",
        "effects",
        "locks",
    ],
)
def test_required_component_cannot_be_none(tmp_path, component):
    values = bundle_kwargs(tmp_path)
    values[component] = None

    with pytest.raises(RuntimeComponentsError, match="may not be None"):
        RuntimeComponents(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "wrong"),
        ("mode", "other"),
        ("owner_id", "  "),
        ("owner_id", "x" * 257),
        ("writer_count", 0),
        ("writer_count", True),
        ("local_writes", "false"),
        ("local_writes", True),
        ("reserved_domains", frozenset({"session"})),
    ],
)
def test_invalid_contract_metadata_is_rejected(tmp_path, field, value):
    values = bundle_kwargs(tmp_path)
    values[field] = value

    with pytest.raises(RuntimeComponentsError):
        RuntimeComponents(**values)


def test_panel_bus_must_be_the_bundle_bus(tmp_path):
    values = bundle_kwargs(tmp_path)
    values["panel"] = SyntheticPanel(object())

    with pytest.raises(RuntimeComponentsError, match="panel.bus"):
        RuntimeComponents(**values)


@pytest.mark.parametrize("method", ["exclusive", "try_exclusive"])
def test_locks_require_both_callable_methods(tmp_path, method):
    values = bundle_kwargs(tmp_path)
    lock_methods = {
        "exclusive": lambda _self, _name: None,
        "try_exclusive": lambda _self, _name: None,
    }
    lock_methods[method] = None
    values["locks"] = type("IncompleteLocks", (), lock_methods)()

    with pytest.raises(RuntimeComponentsError, match=f"locks.{method}"):
        RuntimeComponents(**values)


def test_standalone_requires_path_state_root(tmp_path):
    values = bundle_kwargs(tmp_path)
    values["mode"] = "standalone"
    values["local_writes"] = True
    values["state_root"] = None

    with pytest.raises(RuntimeComponentsError, match="Path state_root"):
        RuntimeComponents(**values)


def test_standalone_requires_local_writes(tmp_path):
    values = bundle_kwargs(tmp_path)
    values["mode"] = "standalone"
    values["local_writes"] = False
    values["state_root"] = tmp_path / "state"

    with pytest.raises(RuntimeComponentsError, match="local_writes"):
        RuntimeComponents(**values)


def test_injected_state_root_must_be_path_when_present(tmp_path):
    values = bundle_kwargs(tmp_path)
    values["state_root"] = str(tmp_path / "state")

    with pytest.raises(RuntimeComponentsError, match="state_root"):
        RuntimeComponents(**values)
