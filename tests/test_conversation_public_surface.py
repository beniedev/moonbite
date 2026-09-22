from __future__ import annotations

import inspect
from datetime import UTC, date, datetime
from typing import get_type_hints

import pytest

from moonbite_plugin import conversation
from moonbite_plugin._conversation import checkpoints, contracts, engine


EXPECTED_EXPORTS = [
    "CHECKPOINT_FAILED_STATES",
    "CHECKPOINT_PENDING_STATES",
    "CONVERSATION_BRIDGE_KIND",
    "CONVERSATION_BRIDGE_SCHEMA",
    "CONVERSATION_OPERATIONS",
    "CONVERSATION_SCHEMA",
    "SCHEMA_VERSION",
    "CheckpointRequest",
    "ConversationBridge",
    "ConversationBridgeError",
    "ConversationGateError",
    "ConversationReceipt",
    "ConversationSnapshot",
]

PUBLIC_TYPES = (
    "CheckpointRequest",
    "ConversationBridge",
    "ConversationBridgeError",
    "ConversationGateError",
    "ConversationReceipt",
    "ConversationSnapshot",
)

PUBLIC_SIGNATURES = {
    "ConversationBridge.observer_status": (
        "(self, *, target_date: 'date', now: 'datetime') -> "
        "'tuple[ObservationFact, ...]'"
    ),
    "ConversationBridge.observe": (
        "(self, receipt: 'SessionHookReceipt') -> 'ConversationReceipt'"
    ),
    "ConversationBridge.snapshot": (
        "(self, lifecycle_id: 'str', *, now: 'datetime | None' = None) -> "
        "'ConversationSnapshot | None'"
    ),
    "ConversationBridge.request_checkpoint": (
        "(self, lifecycle_id: 'str', *, source_event_id: 'str', "
        "idempotency_key: 'str', epoch_id: 'str', content_sha256: 'str', "
        "content_length: 'int', expires_at: 'datetime', now: "
        "'datetime | None' = None) -> 'CheckpointRequest'"
    ),
    "ConversationBridge.reconcile": (
        "(self, lifecycle_id: 'str', *, now: 'datetime | None' = None) -> "
        "'ConversationSnapshot | None'"
    ),
}


def test_conversation_public_manifest_is_exact() -> None:
    assert conversation.__all__ == EXPECTED_EXPORTS


@pytest.mark.parametrize("name", PUBLIC_TYPES)
def test_moved_public_types_keep_identity_and_reflection(name: str) -> None:
    public_type = getattr(conversation, name)
    internal_type = (
        engine.ConversationBridge
        if name == "ConversationBridge"
        else getattr(contracts, name)
    )

    assert public_type is internal_type
    assert public_type.__module__ == "moonbite_plugin.conversation"
    assert public_type.__qualname__ == name


@pytest.mark.parametrize(("path", "expected_signature"), PUBLIC_SIGNATURES.items())
def test_public_signatures_and_type_hints_remain_compatible(
    path: str, expected_signature: str
) -> None:
    target: object = conversation
    for part in path.split("."):
        target = getattr(target, part)

    assert str(inspect.signature(target)) == expected_signature
    assert all(not isinstance(value, str) for value in get_type_hints(target).values())


def test_observer_contract_docstring_is_preserved() -> None:
    assert inspect.getdoc(conversation.ConversationBridge.observer_status) == (
        "Return content-free bridge health evidence with no lock or writes."
    )


def test_group_ports_still_use_dynamic_class_dispatch(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = conversation.ConversationBridge(tmp_path)
    calls: list[str] = []

    def replay_override(self):
        calls.append("replay")
        return {}

    monkeypatch.setattr(
        conversation.ConversationBridge, "_replay_unlocked", replay_override
    )
    assert bridge.snapshots(now=datetime(2026, 9, 22, tzinfo=UTC)) == ()

    def checkpoint_override(**_kwargs):
        calls.append("checkpoint")
        raise RuntimeError("dynamic checkpoint override")

    monkeypatch.setattr(
        conversation.ConversationBridge,
        "_validate_checkpoint_arguments",
        staticmethod(checkpoint_override),
    )
    with pytest.raises(RuntimeError, match="dynamic checkpoint override"):
        bridge.request_checkpoint(
            "lifecycle",
            source_event_id="source",
            idempotency_key="request",
            epoch_id="epoch",
            content_sha256="a" * 64,
            content_length=1,
            expires_at=datetime(2026, 9, 23, tzinfo=UTC),
        )

    assert calls == ["replay", "checkpoint"]


def test_observer_never_enters_owner_or_mutation_lock(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = conversation.ConversationBridge(tmp_path)
    bridge.path.parent.mkdir(parents=True, exist_ok=True)
    bridge.path.write_text("", encoding="utf-8")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("observer entered a writer or owner port")

    monkeypatch.setattr(engine, "file_lock", forbidden)
    monkeypatch.setattr(checkpoints, "file_lock", forbidden)
    monkeypatch.setattr(bridge.session_store, "snapshot", forbidden)
    monkeypatch.setattr(bridge.effect_ledger, "get", forbidden)
    monkeypatch.setattr(bridge.ledger, "rows", forbidden)

    facts = bridge.observer_status(
        target_date=date(2026, 9, 22),
        now=datetime(2026, 9, 22, tzinfo=UTC),
    )

    assert len(facts) == 1
    assert facts[0].code == "conversation_ledger_valid"
