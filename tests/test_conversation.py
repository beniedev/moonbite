from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from moonbite_plugin.conversation import (
    CONVERSATION_BRIDGE_SCHEMA,
    ConversationBridge,
    ConversationBridgeError,
    ConversationGateError,
)
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.runtime_core import JsonlLedger, StateError
from moonbite_plugin.session import (
    HOOK_ORDER,
    SessionContext,
    SessionLifecycleStore,
)

NOW = datetime(2026, 8, 24, 19, 0, tzinfo=UTC)
ALL_HOOKS = frozenset(HOOK_ORDER)
SHA = "a" * 64


def context(
    source_id: str,
    *,
    lifecycle_id: str = "lifecycle-1",
    session_id: str = "session-1",
    source_kind: str = "private_inbound",
    turn_id: str | None = None,
    observed_at: datetime = NOW,
    fresh: bool = True,
) -> SessionContext:
    return SessionContext(
        session_id=session_id,
        lifecycle_id=lifecycle_id,
        source_id=source_id,
        source_kind=source_kind,
        observed_at=observed_at,
        fresh=fresh,
        supported_hooks=ALL_HOOKS,
        turn_id=turn_id,
    )


def stores(tmp_path, *, quiet=timedelta(minutes=5)):
    sessions = SessionLifecycleStore(tmp_path)
    effects = EffectLedger(tmp_path, clock=lambda: NOW)
    bridge = ConversationBridge(
        tmp_path,
        sessions,
        effects,
        quiet_window=quiet,
        overdue_window=timedelta(minutes=30),
        clock=lambda: NOW,
    )
    return sessions, effects, bridge


def private_lifecycle(
    sessions: SessionLifecycleStore,
    bridge: ConversationBridge,
    *,
    lifecycle_id: str = "lifecycle-1",
    session_id: str = "session-1",
):
    make_context = lambda source_id, **kwargs: context(
        source_id,
        lifecycle_id=lifecycle_id,
        session_id=session_id,
        **kwargs,
    )
    gateway = sessions.record_hook(make_context("gateway"), "pre_gateway_dispatch")
    bridge.observe(gateway)
    start = sessions.record_hook(
        make_context("start", source_kind="session_start"),
        "on_session_start",
    )
    bridge.observe(start)
    pre = sessions.record_hook(
        make_context("private", turn_id="turn-1"), "pre_llm_call"
    )
    bridge.observe(pre)
    return gateway, start, pre


def settle_lifecycle(
    sessions: SessionLifecycleStore,
    bridge: ConversationBridge,
    *,
    lifecycle_id: str = "lifecycle-1",
    session_id: str = "session-1",
):
    post = sessions.record_hook(
        context(
            "assistant",
            lifecycle_id=lifecycle_id,
            session_id=session_id,
            source_kind="assistant_response",
            turn_id="turn-1",
            observed_at=NOW + timedelta(minutes=1),
        ),
        "post_llm_call",
        settled=True,
    )
    return bridge.observe(post)


def request(bridge: ConversationBridge, *, now=NOW + timedelta(minutes=10)):
    return bridge.request_checkpoint(
        "lifecycle-1",
        source_event_id="checkpoint-source",
        idempotency_key="checkpoint-idem",
        epoch_id="epoch-1",
        content_sha256=SHA,
        content_length=17,
        expires_at=NOW + timedelta(hours=1),
        now=now,
    )


def request_values(
    bridge: ConversationBridge,
    *,
    lifecycle_id: str = "lifecycle-1",
    source_event_id: str,
    idempotency_key: str,
    now: datetime,
    expires_at: datetime = NOW + timedelta(hours=2),
):
    return bridge.request_checkpoint(
        lifecycle_id,
        source_event_id=source_event_id,
        idempotency_key=idempotency_key,
        epoch_id="epoch-1",
        content_sha256=SHA,
        content_length=17,
        expires_at=expires_at,
        now=now,
    )


def second_turn(sessions: SessionLifecycleStore, bridge: ConversationBridge):
    pre = sessions.record_hook(
        context(
            "private-2",
            turn_id="turn-2",
            observed_at=NOW + timedelta(minutes=20),
        ),
        "pre_llm_call",
    )
    bridge.observe(pre)
    post = sessions.record_hook(
        context(
            "assistant-2",
            source_kind="assistant_response",
            turn_id="turn-2",
            observed_at=NOW + timedelta(minutes=21),
        ),
        "post_llm_call",
        settled=True,
    )
    bridge.observe(post)
    return pre, post


def verify_effect(effects: EffectLedger, effect_id: str, source_event_id: str):
    effects.mark_pending(effect_id)
    effects.mark_queue_accepted(effect_id)
    return effects.verify(
        effect_id,
        EffectReceipt(
            receipt_id=f"receipt-{effect_id}",
            event_id=source_event_id,
            observed_at=NOW + timedelta(minutes=22),
            content_sha256=SHA,
            content_length=17,
            epoch_id="epoch-1",
        ),
    )


def test_schema_dirty_settled_and_non_private_receipts(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    snapshot = bridge.snapshot("lifecycle-1")
    assert snapshot.schema_version == CONVERSATION_BRIDGE_SCHEMA
    assert (snapshot.dirty, snapshot.settled, snapshot.active_chat) == (
        True,
        False,
        True,
    )

    settled = settle_lifecycle(sessions, bridge)
    assert settled.operation == "mark_settled"
    assert settled.snapshot.settled is True
    assert settled.snapshot.active_chat is False


@pytest.mark.parametrize(
    "source_kind",
    ["session_start", "assistant_response", "system", "cron", "workroom", "tool"],
)
def test_non_private_sources_never_mark_dirty(tmp_path, source_kind):
    sessions, _effects, bridge = stores(tmp_path)
    if source_kind == "session_start":
        gateway = sessions.record_hook(
            context("gateway"),
            "pre_gateway_dispatch",
        )
        bridge.observe(gateway)
        receipt = sessions.record_hook(
            context("start", source_kind=source_kind),
            "on_session_start",
        )
    elif source_kind == "assistant_response":
        gateway = sessions.record_hook(
            context("gateway"),
            "pre_gateway_dispatch",
        )
        bridge.observe(gateway)
        start = sessions.record_hook(
            context("start", source_kind="session_start"),
            "on_session_start",
        )
        bridge.observe(start)
        sessions.record_hook(
            context("pre", turn_id="turn-1"),
            "pre_llm_call",
        )
        receipt = sessions.record_hook(
            context("post", source_kind=source_kind, turn_id="turn-1"),
            "post_llm_call",
            settled=True,
        )
    else:
        receipt = sessions.record_hook(
            context("source", source_kind=source_kind),
            "pre_gateway_dispatch",
        )
    result = bridge.observe(receipt)
    assert result.operation in {"ignored", "mark_settled"}
    assert result.operation != "mark_dirty"


def test_only_valid_settled_post_receipt_settles(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    _gateway, _start, pre = private_lifecycle(sessions, bridge)
    invalid = replace(pre, settled=True)
    with pytest.raises(ConversationBridgeError):
        bridge.observe(invalid)
    assert bridge.snapshot("lifecycle-1").settled is False


def test_ignored_and_finalize_duplicates_are_deduplicated_without_new_cycle(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    gateway = sessions.record_hook(
        context("gateway", source_kind="system"),
        "pre_gateway_dispatch",
    )
    bridge.observe(gateway)
    ignored = sessions.record_hook(
        context("start", source_kind="session_start"),
        "on_session_start",
    )
    first_ignored = bridge.observe(ignored)
    duplicate_ignored = bridge.observe(ignored)
    assert first_ignored.operation == "ignored"
    assert first_ignored.deduplicated is False
    assert duplicate_ignored.deduplicated is True
    assert bridge.snapshot("lifecycle-1").dirty is False

    private_lifecycle(
        sessions,
        bridge,
        lifecycle_id="lifecycle-2",
        session_id="session-2",
    )
    settle_lifecycle(
        sessions,
        bridge,
        lifecycle_id="lifecycle-2",
        session_id="session-2",
    )
    finalize = sessions.record_hook(
        context(
            "finalize",
            lifecycle_id="lifecycle-2",
            session_id="session-2",
            source_kind="system",
        ),
        "on_session_finalize",
    )
    first_finalize = bridge.observe(finalize)
    duplicate_finalize = bridge.observe(finalize)
    assert first_finalize.operation == "ignored"
    assert duplicate_finalize.deduplicated is True
    assert bridge.snapshot("lifecycle-2").dirty is True
    assert (
        len([row for row in bridge.ledger.rows() if row["operation"] == "ignored"]) == 4
    )


def test_active_chat_quiet_and_overdue_gates(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    with pytest.raises(ConversationGateError, match="active_chat"):
        request(bridge)
    settle_lifecycle(sessions, bridge)

    quiet = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=3))
    assert quiet.quiet is False
    assert quiet.overdue is False
    with pytest.raises(ConversationGateError, match="quiet_window"):
        request(bridge, now=NOW + timedelta(minutes=3))

    overdue = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=31))
    assert overdue.overdue is True
    assert overdue.can_checkpoint is True


def test_overdue_or_quiet_allows_settled_checkpoint_but_not_active_chat(tmp_path):
    sessions, _effects, bridge = stores(
        tmp_path,
        quiet=timedelta(minutes=60),
    )
    private_lifecycle(sessions, bridge)
    active = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=31))
    assert active.overdue is True
    assert active.quiet is False
    assert active.can_checkpoint is False
    with pytest.raises(ConversationGateError, match="active_chat"):
        request_values(
            bridge,
            source_event_id="blocked",
            idempotency_key="blocked",
            now=NOW + timedelta(minutes=31),
        )

    settle_lifecycle(sessions, bridge)
    settled = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=31))
    assert settled.overdue is True
    assert settled.quiet is False
    assert settled.can_checkpoint is True
    assert (
        request_values(
            bridge,
            source_event_id="overdue",
            idempotency_key="overdue",
            now=NOW + timedelta(minutes=31),
        ).state
        == "intent"
    )


def test_checkpoint_intent_has_digest_only_and_replays(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    result = request(bridge)
    assert result.kind == "checkpoint"
    assert result.state == "intent"
    assert result.snapshot.checkpoint_pending is True
    assert effects.get(result.effect_id).content_sha256 == SHA
    rows = bridge.ledger.rows()
    assert len(rows) == 5
    assert all("body" not in row for row in rows)
    replay = ConversationBridge(
        tmp_path,
        SessionLifecycleStore(tmp_path),
        EffectLedger(tmp_path, clock=lambda: NOW),
        quiet_window=timedelta(minutes=5),
        overdue_window=timedelta(minutes=30),
        clock=lambda: NOW,
    )
    assert replay.snapshot("lifecycle-1").checkpoint_pending is True


def test_duplicate_hook_finalize_and_checkpoint_are_idempotent(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    gateway, start, pre = private_lifecycle(sessions, bridge)
    assert bridge.observe(gateway).deduplicated is True
    assert bridge.observe(start).deduplicated is True
    assert bridge.observe(pre).deduplicated is True
    settle_lifecycle(sessions, bridge)
    duplicate = bridge.observe(
        sessions.record_hook(
            context(
                "assistant",
                source_kind="assistant_response",
                turn_id="turn-1",
                observed_at=NOW + timedelta(minutes=3),
            ),
            "post_llm_call",
            settled=True,
        )
    )
    assert duplicate.deduplicated is True
    first = request(bridge)
    second = request(bridge)
    assert second.effect_id == first.effect_id
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 1
    )
    assert (
        len(
            [
                row
                for row in bridge.ledger.rows()
                if row["operation"] == "checkpoint_requested"
            ]
        )
        == 1
    )


def test_effect_state_requires_receipt_and_reconciliation_is_durable(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    result = request(bridge)
    effects.mark_pending(result.effect_id)
    assert bridge.snapshot("lifecycle-1").checkpoint_pending is True
    effects.mark_queue_accepted(result.effect_id)
    assert bridge.reconcile("lifecycle-1").checkpoint_pending is True
    receipt = EffectReceipt(
        receipt_id="receipt-1",
        event_id="checkpoint-source",
        observed_at=NOW + timedelta(minutes=11),
        content_sha256=SHA,
        content_length=17,
        epoch_id="epoch-1",
    )
    effects.verify(result.effect_id, receipt)
    assert bridge.snapshot("lifecycle-1").checkpoint_complete is True
    complete = bridge.reconcile("lifecycle-1")
    assert complete.checkpoint_complete is True
    assert (
        len([row for row in bridge.ledger.rows() if row["operation"] == "reconcile"])
        == 2
    )


def test_verified_checkpoint_requires_matching_session_evidence(tmp_path, monkeypatch):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    result = request(bridge)
    verify_effect(effects, result.effect_id, "checkpoint-source")

    monkeypatch.setattr(bridge.session_store, "snapshot", lambda _lifecycle_id: None)
    snapshot = bridge.snapshot("lifecycle-1")

    assert snapshot.checkpoint_state == "checkpoint_unverified"
    assert snapshot.checkpoint_complete is False
    assert snapshot.checkpoint_evidence is not None
    assert snapshot.checkpoint_evidence["receipt_id"].startswith("receipt-")
    assert snapshot.can_checkpoint is False
    assert snapshot.blocked_reason == "session_evidence_missing"


def test_partial_session_snapshot_is_missing_evidence(tmp_path, monkeypatch):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    result = request(bridge)
    verify_effect(effects, result.effect_id, "checkpoint-source")

    monkeypatch.setattr(
        bridge.session_store,
        "snapshot",
        lambda _lifecycle_id: SimpleNamespace(
            session_id="session-1", lifecycle_id="lifecycle-1"
        ),
    )
    snapshot = bridge.snapshot("lifecycle-1")

    assert snapshot.checkpoint_state == "checkpoint_unverified"
    assert snapshot.checkpoint_complete is False
    assert snapshot.can_checkpoint is False
    assert snapshot.blocked_reason == "session_evidence_missing"


def test_failed_and_expired_are_not_complete(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    result = request(bridge)
    effects.mark_pending(result.effect_id)
    effects.fail(result.effect_id, "timeout", retryable=True)
    assert bridge.snapshot("lifecycle-1").checkpoint_failed is True


def test_verified_checkpoint_allows_second_cycle_and_keeps_old_effect(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request(bridge)
    verify_effect(effects, first.effect_id, "checkpoint-source")
    assert bridge.snapshot("lifecycle-1").checkpoint_complete is True

    second_turn(sessions, bridge)
    dirty = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=30))
    assert dirty.dirty is True
    assert dirty.settled is True
    assert dirty.checkpoint_complete is False
    assert dirty.checkpoint_state == "idle"
    assert dirty.checkpoint_effect_id is None
    second = request_values(
        bridge,
        source_event_id="checkpoint-source-2",
        idempotency_key="checkpoint-idem-2",
        now=NOW + timedelta(minutes=30),
    )
    assert second.effect_id != first.effect_id
    assert effects.get(first.effect_id).state == "verified"
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 2
    )


def test_new_cycle_scopes_prior_cycle_idempotency_key(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request(bridge)
    verify_effect(effects, first.effect_id, "checkpoint-source")
    second_turn(sessions, bridge)
    second = request_values(
        bridge,
        source_event_id="checkpoint-source",
        idempotency_key="checkpoint-idem",
        now=NOW + timedelta(minutes=30),
    )
    assert second.effect_id != first.effect_id
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 2
    )


def test_checkpoint_ledger_idempotency_is_scoped_by_lifecycle_and_cycle(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request_values(
        bridge,
        source_event_id="shared-source",
        idempotency_key="shared-key",
        now=NOW + timedelta(minutes=10),
    )

    second_bridge = ConversationBridge(
        tmp_path,
        sessions,
        effects,
        quiet_window=timedelta(minutes=5),
        overdue_window=timedelta(minutes=30),
        clock=lambda: NOW,
    )
    private_lifecycle(
        sessions,
        second_bridge,
        lifecycle_id="lifecycle-2",
        session_id="session-2",
    )
    settle_lifecycle(
        sessions,
        second_bridge,
        lifecycle_id="lifecycle-2",
        session_id="session-2",
    )
    second = request_values(
        second_bridge,
        lifecycle_id="lifecycle-2",
        source_event_id="shared-source",
        idempotency_key="shared-key",
        now=NOW + timedelta(minutes=10),
    )
    replay = request_values(
        second_bridge,
        lifecycle_id="lifecycle-2",
        source_event_id="shared-source",
        idempotency_key="shared-key",
        now=NOW + timedelta(minutes=10),
    )

    assert first.effect_id != second.effect_id
    assert first.idempotency_key == second.idempotency_key == "shared-key"
    assert replay.effect_id == second.effect_id
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 2
    )


def test_pending_checkpoint_preserves_new_dirty_cycle_and_blocks_second_effect(
    tmp_path,
):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request(bridge)
    effects.mark_pending(first.effect_id)
    second_turn(sessions, bridge)
    blocked = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=30))
    assert blocked.dirty is True
    assert blocked.settled is True
    assert blocked.checkpoint_pending is True
    with pytest.raises(ConversationGateError, match="checkpoint_pending"):
        request_values(
            bridge,
            source_event_id="checkpoint-source-2",
            idempotency_key="checkpoint-idem-2",
            now=NOW + timedelta(minutes=30),
        )
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 1
    )

    verify_effect(effects, first.effect_id, "checkpoint-source")
    reopened = ConversationBridge(
        tmp_path,
        SessionLifecycleStore(tmp_path),
        EffectLedger(tmp_path, clock=lambda: NOW),
        quiet_window=timedelta(minutes=5),
        overdue_window=timedelta(minutes=30),
        clock=lambda: NOW,
    )
    assert reopened.snapshot(
        "lifecycle-1", now=NOW + timedelta(minutes=30)
    ).can_checkpoint
    assert request_values(
        reopened,
        source_event_id="checkpoint-source-2",
        idempotency_key="checkpoint-idem-2",
        now=NOW + timedelta(minutes=30),
    ).effect_id


def test_failed_expired_requeue_history_is_not_swallowed_by_new_cycle(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request(bridge)
    effects.mark_pending(first.effect_id)
    effects.fail(first.effect_id, "adapter timeout", retryable=True)
    assert bridge.snapshot("lifecycle-1").checkpoint_failed is True
    second_turn(sessions, bridge)
    second = request_values(
        bridge,
        source_event_id="checkpoint-source-2",
        idempotency_key="checkpoint-idem-2",
        now=NOW + timedelta(minutes=30),
    )
    assert second.effect_id != first.effect_id
    assert effects.get(first.effect_id).state == "failed"


def test_requeued_old_checkpoint_blocks_but_preserves_second_cycle(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    first = request(bridge)
    effects.mark_pending(first.effect_id)
    effects.expire(first.effect_id, NOW + timedelta(hours=2))
    effects.requeue(
        first.effect_id,
        expires_at=NOW + timedelta(hours=3),
        idempotency_key=first.effect.idempotency_key,
        source_event_id="checkpoint-source",
        epoch_id="epoch-1",
    )
    second_turn(sessions, bridge)
    blocked = bridge.snapshot("lifecycle-1", now=NOW + timedelta(minutes=30))
    assert blocked.dirty is True
    assert blocked.checkpoint_pending is True
    with pytest.raises(ConversationGateError, match="checkpoint_pending"):
        request_values(
            bridge,
            source_event_id="checkpoint-source-2",
            idempotency_key="checkpoint-idem-2",
            now=NOW + timedelta(minutes=30),
        )
    assert effects.get(first.effect_id).state == "requeued"


def test_bridge_append_failure_is_retryable(tmp_path, monkeypatch):
    sessions, _effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    original_append = bridge.ledger.append
    failed = {"once": True}

    def fail_once(row):
        if failed["once"] and row["operation"] == "checkpoint_requested":
            failed["once"] = False
            raise OSError("bridge append fixture")
        return original_append(row)

    monkeypatch.setattr(bridge.ledger, "append", fail_once)
    with pytest.raises(OSError, match="bridge append fixture"):
        request(bridge)
    monkeypatch.setattr(bridge.ledger, "append", original_append)
    result = request(bridge)
    assert result.effect_id
    assert (
        len(
            [
                row
                for row in bridge.ledger.rows()
                if row["operation"] == "checkpoint_requested"
            ]
        )
        == 1
    )


def test_effect_append_failure_leaves_no_bridge_reference(tmp_path, monkeypatch):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    original = effects.begin_intent

    def fail(*args, **kwargs):
        raise OSError("effect append fixture")

    monkeypatch.setattr(effects, "begin_intent", fail)
    with pytest.raises(OSError, match="effect append fixture"):
        request(bridge)
    assert not any(
        row["operation"] == "checkpoint_requested" for row in bridge.ledger.rows()
    )
    monkeypatch.setattr(effects, "begin_intent", original)
    assert request(bridge).effect_id


def test_concurrent_duplicate_checkpoint_request_appends_one_intent(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)

    def worker(_index):
        return request(
            ConversationBridge(
                tmp_path,
                SessionLifecycleStore(tmp_path),
                EffectLedger(tmp_path, clock=lambda: NOW),
                quiet_window=timedelta(minutes=5),
                overdue_window=timedelta(minutes=30),
                clock=lambda: NOW,
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(16)))
    assert {item.effect_id for item in results} == {results[0].effect_id}
    assert (
        len(
            [row for row in effects.ledger.rows() if row["operation"] == "begin_intent"]
        )
        == 1
    )


def test_corrupt_or_out_of_order_replay_fails_closed(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    request(bridge)
    row = bridge.ledger.rows()[-1]
    row["operation"] = "reconcile"
    JsonlLedger(bridge.path).append(row)
    with pytest.raises(StateError):
        bridge.snapshot("lifecycle-1")

    corrupt = tmp_path / "corrupt"
    JsonlLedger(corrupt / "conversation_bridge.jsonl").append(
        {"schema_version": "moon.conversation.unknown"}
    )
    with pytest.raises(StateError):
        ConversationBridge(
            corrupt,
            SessionLifecycleStore(corrupt),
            EffectLedger(corrupt, clock=lambda: NOW),
        ).snapshots()


def test_process_reopen_preserves_pending_and_active_block(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    reopened = ConversationBridge(
        tmp_path,
        SessionLifecycleStore(tmp_path),
        EffectLedger(tmp_path, clock=lambda: NOW),
        quiet_window=timedelta(minutes=5),
        overdue_window=timedelta(minutes=30),
        clock=lambda: NOW,
    )
    with pytest.raises(ConversationGateError, match="active_chat"):
        request(reopened)
    settle_lifecycle(sessions, bridge)
    result = request(reopened)
    effects.mark_pending(result.effect_id)
    assert (
        ConversationBridge(
            tmp_path,
            SessionLifecycleStore(tmp_path),
            EffectLedger(tmp_path, clock=lambda: NOW),
            quiet_window=timedelta(minutes=5),
            overdue_window=timedelta(minutes=30),
            clock=lambda: NOW,
        )
        .snapshot("lifecycle-1")
        .checkpoint_pending
    )


def _fact_keys(value):
    if isinstance(value, dict):
        keys = set(value)
        for child in value.values():
            keys.update(_fact_keys(child))
        return keys
    if isinstance(value, list):
        keys = set()
        for child in value:
            keys.update(_fact_keys(child))
        return keys
    return set()


def test_conversation_observer_pristine_and_corrupt_state_are_read_only(tmp_path):
    sessions, _effects, bridge = stores(tmp_path)
    before = sorted(
        (
            path.relative_to(tmp_path).as_posix(),
            path.stat().st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
        for path in tmp_path.rglob("*")
    )
    assert bridge.observer_status(target_date=date(2026, 8, 24), now=NOW) == ()
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

    private_lifecycle(sessions, bridge)
    before = sorted(
        (
            path.relative_to(tmp_path).as_posix(),
            path.stat().st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
        for path in tmp_path.rglob("*")
    )
    facts = bridge.observer_status(target_date=date(2026, 8, 24), now=NOW)
    assert any(fact.code == "conversation_dirty" for fact in facts)
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
    assert not forbidden & _fact_keys([fact.to_dict() for fact in facts])

    bridge.path.write_text("{broken", encoding="utf-8")
    corrupt = bridge.observer_status(target_date=date(2026, 8, 24), now=NOW)
    assert any(fact.state == "current" for fact in corrupt)
    assert all("broken" not in fact.code for fact in corrupt)


def test_conversation_observer_checkpoint_pending_and_verified_recovery(tmp_path):
    sessions, effects, bridge = stores(tmp_path)
    private_lifecycle(sessions, bridge)
    settle_lifecycle(sessions, bridge)
    request_result = request(bridge)
    pending = bridge.observer_status(target_date=date(2026, 8, 24), now=NOW)
    checkpoint = next(
        fact for fact in pending if fact.key.endswith(request_result.effect_id)
    )
    assert checkpoint.state == "current"
    assert checkpoint.code == "checkpoint_intent"
    effects.mark_pending(request_result.effect_id)
    bridge.reconcile("lifecycle-1", now=NOW + timedelta(minutes=1))
    checkpoint = next(
        fact
        for fact in bridge.observer_status(
            target_date=date(2026, 8, 24), now=NOW + timedelta(minutes=1)
        )
        if fact.key.endswith(request_result.effect_id)
    )
    assert checkpoint.code == "checkpoint_pending"
    receipt = EffectReceipt(
        receipt_id="observer-receipt",
        event_id="checkpoint-source",
        observed_at=NOW + timedelta(minutes=2),
        content_sha256=SHA,
        content_length=17,
        epoch_id="epoch-1",
    )
    effects.verify(request_result.effect_id, receipt)
    bridge.reconcile("lifecycle-1", now=NOW + timedelta(minutes=2))
    recovered = next(
        fact
        for fact in bridge.observer_status(
            target_date=date(2026, 8, 24), now=NOW + timedelta(minutes=2)
        )
        if fact.key.endswith(request_result.effect_id)
    )
    assert recovered.state == "recovered_history"
    assert recovered.recovery is not None
    assert recovered.recovery.ref == "observer-receipt"
    assert recovered.target_date == date(2026, 8, 24)
    assert recovered.event_time == NOW + timedelta(minutes=2)
