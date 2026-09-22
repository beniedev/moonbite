"""Append-only conversation continuity bridge.

The public module remains a compatibility facade over focused internal
implementations. The bridge records only lifecycle/evidence metadata and never
accepts or persists a message body.
"""

from __future__ import annotations

from collections.abc import Mapping  # noqa: F401
from datetime import datetime  # noqa: F401
from typing import Any, ClassVar  # noqa: F401

from ._conversation.contracts import (
    CHECKPOINT_FAILED_STATES,
    CHECKPOINT_PENDING_STATES,
    CONVERSATION_BRIDGE_KIND,
    CONVERSATION_BRIDGE_SCHEMA,
    CONVERSATION_OPERATIONS,
    CONVERSATION_SCHEMA,
    SCHEMA_VERSION,
    CheckpointRequest,
    ConversationBridgeError,
    ConversationGateError,
    ConversationReceipt,
    ConversationSnapshot,
)
from ._conversation.engine import ConversationBridge

_PUBLIC_TYPES = (
    CheckpointRequest,
    ConversationBridge,
    ConversationBridgeError,
    ConversationGateError,
    ConversationReceipt,
    ConversationSnapshot,
)
# Public classes retain their historical import and pickle identity. Their
# definitions remain in the focused internal modules, so class-level source
# discovery is not part of this compatibility facade's contract.
for _public_type in _PUBLIC_TYPES:
    _public_type.__module__ = __name__

_METHOD_NAMES = [
    "__init__",
    "_window",
    "_new_state",
    "_same_checkpoint_identity",
    "_checkpoint_is_active",
    "_checkpoint_for_reconcile",
    "_new_cycle",
    "_current_cycle_for_dirty",
    "_apply_event",
    "_replay_unlocked",
    "_session_snapshot",
    "_effect_receipt",
    "_effect_record",
    "_effect_evidence",
    "_checkpoint_status",
    "_build_snapshot",
    "_receipt_event_matches",
    "_observer_apply_event",
    "_observer_replay",
    "_observer_checkpoint_fact",
    "observer_status",
    "observe",
    "_snapshot_unlocked",
    "snapshot",
    "evaluate",
    "snapshots",
    "replay",
    "_validate_checkpoint_arguments",
    "_effect_identity_matches_request",
    "request_checkpoint",
    "reconcile",
    "reconcile_all",
]
for _method_name in _METHOD_NAMES:
    _descriptor = next(
        base.__dict__[_method_name]
        for base in ConversationBridge.__mro__
        if _method_name in base.__dict__
    )
    _function = (
        _descriptor.__func__
        if isinstance(_descriptor, (staticmethod, classmethod))
        else _descriptor
    )
    _function.__module__ = __name__
    _function.__qualname__ = f"ConversationBridge.{_method_name}"

del _descriptor, _function, _method_name, _public_type

__all__ = [
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
