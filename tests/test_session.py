from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from moonbite_plugin.runtime_core import StateError
from moonbite_plugin.session import (
    HOOK_ORDER,
    SESSION_LIFECYCLE_SCHEMA,
    SESSION_TURN_TERMINAL_SCHEMA,
    SOURCE_KINDS,
    SessionContext,
    SessionLifecycleError,
    SessionLifecycleStore,
    SessionTurnTerminalReceipt,
)

NOW = datetime(2026, 8, 24, 19, 0, tzinfo=UTC)
ALL_HOOKS = frozenset(HOOK_ORDER)
TURN_HOOKS = frozenset({"pre_llm_call", "post_llm_call"})
FINALIZE_HOOKS = frozenset(HOOK_ORDER[1:])


def context(
    source_id: str,
    *,
    lifecycle_id: str = "lifecycle-1",
    source_kind: str = "private_inbound",
    turn_id: str | None = None,
    fresh: bool = True,
    supported_hooks: frozenset[str] = ALL_HOOKS,
    observed_at: datetime = NOW,
) -> SessionContext:
    return SessionContext(
        session_id="session-1",
        lifecycle_id=lifecycle_id,
        source_id=source_id,
        turn_id=turn_id,
        source_kind=source_kind,
        observed_at=observed_at,
        fresh=fresh,
        supported_hooks=supported_hooks,
    )


def open_turn_store(root):
    store = SessionLifecycleStore(root)
    store.record_hook(
        context("start", source_kind="session_start", supported_hooks=FINALIZE_HOOKS),
        "on_session_start",
    )
    store.record_hook(
        context("pre", turn_id="turn-1", supported_hooks=FINALIZE_HOOKS),
        "pre_llm_call",
    )
    return store


def open_child_store(root, supported_hooks=ALL_HOOKS):
    store = SessionLifecycleStore(root)
    if "pre_gateway_dispatch" in supported_hooks:
        store.record_hook(
            context("gateway", supported_hooks=supported_hooks),
            "pre_gateway_dispatch",
        )
    if "on_session_start" in supported_hooks:
        store.record_hook(
            context(
                "start",
                source_kind="session_start",
                supported_hooks=supported_hooks,
            ),
            "on_session_start",
        )
    store.record_hook(
        context("pre", turn_id="turn-1", supported_hooks=supported_hooks),
        "pre_llm_call",
    )
    return store


def finalize_context(*, observed_at=NOW):
    return context(
        "finalize",
        source_kind="system",
        supported_hooks=FINALIZE_HOOKS,
        observed_at=observed_at,
    )


def test_context_is_frozen_bounded_and_contact_classified() -> None:
    private = context("private")
    assert private.counts_as_private_contact is True
    assert isinstance(private.supported_hooks, frozenset)
    with pytest.raises(FrozenInstanceError):
        private.session_id = "changed"  # type: ignore[misc]
    with pytest.raises(ValueError):
        SessionContext(
            "",
            "lifecycle-1",
            "source",
            "system",
            NOW,
            True,
            ALL_HOOKS,
        )
    with pytest.raises(ValueError):
        SessionContext(
            "session-1",
            "lifecycle-1",
            "source",
            "unknown",
            NOW,
            True,
            ALL_HOOKS,
        )


def test_full_path_requires_settled_turn_before_finalize(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    calls = [
        (context("gateway"), HOOK_ORDER[0], False),
        (context("start", source_kind="session_start"), HOOK_ORDER[1], False),
        (context("pre", turn_id="turn-1"), HOOK_ORDER[2], False),
        (
            context("post", source_kind="assistant_response", turn_id="turn-1"),
            HOOK_ORDER[3],
            True,
        ),
    ]
    receipts = [
        store.record_hook(item, hook, settled=settled) for item, hook, settled in calls
    ]
    receipts.append(
        store.record_host_turn_end(
            context("end", source_kind="system", turn_id="turn-1"),
            "host_turn_completed",
        )
    )
    receipts.append(
        store.record_hook(
            context("finalize", source_kind="system"), "on_session_finalize"
        )
    )
    assert [receipt.hook for receipt in receipts] == list(HOOK_ORDER[:-1])
    assert receipts[-1].schema_version == SESSION_LIFECYCLE_SCHEMA
    assert receipts[-1].snapshot.finalized is True
    assert receipts[-1].snapshot.settled_turn_ids == ("turn-1",)
    assert len(store.ledger.rows()) == 6


def test_cli_like_order_can_omit_gateway(tmp_path) -> None:
    supported = frozenset(HOOK_ORDER[1:])
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("start", source_kind="session_start", supported_hooks=supported),
        "on_session_start",
    )
    store.record_hook(
        context("pre", turn_id="turn-1", supported_hooks=supported),
        "pre_llm_call",
    )
    store.record_hook(
        context(
            "post",
            source_kind="assistant_response",
            turn_id="turn-1",
            supported_hooks=supported,
        ),
        "post_llm_call",
        settled=True,
    )
    store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=supported,
        ),
        "host_turn_completed",
    )
    receipt = store.record_hook(
        context("finalize", source_kind="system", supported_hooks=supported),
        "on_session_finalize",
    )
    assert receipt.snapshot.finalized is True
    assert receipt.snapshot.hooks == HOOK_ORDER[1:-1]


@pytest.mark.parametrize(
    "reason",
    (
        "host_turn_failed",
        "host_turn_interrupted",
        "host_turn_completed",
        "host_turn_incomplete",
    ),
)
def test_host_turn_end_is_terminal_without_faking_post(tmp_path, reason) -> None:
    store = open_turn_store(tmp_path)
    end = context(
        "end",
        source_kind="system",
        turn_id="turn-1",
        supported_hooks=FINALIZE_HOOKS,
    )

    receipt = store.record_host_turn_end(end, reason)
    replay = store.record_host_turn_end(end, reason)

    assert receipt.snapshot.open_turn_id is None
    assert receipt.snapshot.settled_turn_ids == ()
    assert receipt.snapshot.abandoned_turn_ids == ("turn-1",)
    assert receipt.snapshot.hooks[-1] == "on_session_end"
    assert replay.deduplicated is True


def test_legacy_completed_terminal_reason_remains_readable(tmp_path) -> None:
    store = open_turn_store(tmp_path)
    end = context(
        "end",
        source_kind="system",
        turn_id="turn-1",
        supported_hooks=FINALIZE_HOOKS,
    )

    store.record_host_turn_end(end, "host_turn_completed_without_post")
    replay = SessionLifecycleStore(tmp_path).replay()

    assert replay[0].abandoned_turn_ids == ("turn-1",)
    assert [
        row["reason"] for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ] == ["host_turn_completed_without_post"]


def test_host_turn_end_rejects_conflicting_terminal_classification(tmp_path) -> None:
    store = open_turn_store(tmp_path)
    end = context(
        "end",
        source_kind="system",
        turn_id="turn-1",
        supported_hooks=FINALIZE_HOOKS,
    )

    store.record_host_turn_end(end, "host_turn_failed")

    with pytest.raises(SessionLifecycleError, match="conflicts"):
        store.record_host_turn_end(end, "host_turn_completed")
    assert store.ledger.rows()[-1]["terminal_reason"] == "host_turn_failed"


@pytest.mark.parametrize(
    "reason",
    ("host_turn_incomplete", "host_turn_failed"),
)
def test_settled_post_accepts_canonical_non_success_classification(
    tmp_path, reason
) -> None:
    store = open_turn_store(tmp_path)
    store.record_hook(
        context(
            "post",
            source_kind="assistant_response",
            turn_id="turn-1",
            supported_hooks=FINALIZE_HOOKS,
        ),
        "post_llm_call",
        settled=True,
    )

    receipt = store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=FINALIZE_HOOKS,
        ),
        reason,
    )

    assert receipt.hook == "on_session_end"
    assert receipt.snapshot.settled_turn_ids == ("turn-1",)
    assert receipt.snapshot.abandoned_turn_ids == ()
    assert [row for row in store.ledger.rows() if row["kind"] == "turn_terminal"] == []
    assert store.ledger.rows()[-1]["hook"] == "on_session_end"
    assert store.ledger.rows()[-1]["terminal_reason"] == reason
    assert SessionLifecycleStore(tmp_path).snapshot("lifecycle-1").settled_turn_ids == (
        "turn-1",
    )


def test_child_stop_first_and_session_end_later_share_one_terminal(tmp_path) -> None:
    store = open_child_store(tmp_path)

    child = store.record_host_child_stop(
        "session-1", "host_turn_interrupted", NOW + timedelta(seconds=1)
    )
    replay = store.record_host_child_stop(
        "session-1", "host_turn_interrupted", NOW + timedelta(seconds=2)
    )
    end = store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=ALL_HOOKS,
        ),
        "host_turn_interrupted",
    )

    assert child.hook == "subagent_stop"
    assert replay.deduplicated is True
    assert end.hook == "on_session_end"
    assert end.snapshot.open_turn_id is None
    terminal_rows = [
        row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ]
    assert len(terminal_rows) == 1
    assert terminal_rows[0]["schema_version"] == SESSION_TURN_TERMINAL_SCHEMA
    assert terminal_rows[0]["session_id"] == "session-1"
    assert terminal_rows[0]["lifecycle_id"] == "lifecycle-1"
    assert terminal_rows[0]["turn_id"] == "turn-1"
    assert terminal_rows[0]["outcome"] == "abandoned"
    assert terminal_rows[0]["reason"] == "host_turn_interrupted"
    assert terminal_rows[0]["observed_at"] == "2026-08-24T19:00:01Z"


def test_session_end_first_and_child_stop_later_add_one_callback(tmp_path) -> None:
    store = open_child_store(tmp_path)
    end = store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=ALL_HOOKS,
        ),
        "host_turn_failed",
    )

    child = store.record_host_child_stop("session-1", "host_turn_failed")
    replay = store.record_host_child_stop("session-1", "host_turn_failed")

    assert end.hook == "on_session_end"
    assert child.hook == "subagent_stop"
    assert replay.deduplicated is True
    assert [row["kind"] for row in store.ledger.rows()].count("turn_terminal") == 1
    assert [
        row["hook"]
        for row in store.ledger.rows()
        if row["kind"] == "hook" and row["hook"] == "subagent_stop"
    ] == ["subagent_stop"]


def test_session_end_incomplete_then_child_stop_failed_keeps_first_terminal(
    tmp_path,
) -> None:
    store = open_child_store(tmp_path)
    store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=ALL_HOOKS,
        ),
        "host_turn_incomplete",
    )

    child = store.record_host_child_stop("session-1", "host_turn_failed")
    replay = store.record_host_child_stop("session-1", "host_turn_failed")

    terminal_rows = [
        row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ]
    assert child.hook == "subagent_stop"
    assert replay.deduplicated is True
    assert [row["reason"] for row in terminal_rows] == ["host_turn_incomplete"]
    assert store.ledger.rows()[-1]["terminal_reason"] == "host_turn_failed"
    assert SessionLifecycleStore(tmp_path).snapshot(
        "lifecycle-1"
    ).abandoned_turn_ids == ("turn-1",)


def test_child_stop_failed_then_session_end_incomplete_keeps_first_terminal(
    tmp_path,
) -> None:
    store = open_child_store(tmp_path)
    store.record_host_child_stop("session-1", "host_turn_failed")

    end = store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=ALL_HOOKS,
        ),
        "host_turn_incomplete",
    )

    terminal_rows = [
        row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ]
    assert end.hook == "on_session_end"
    assert [row["reason"] for row in terminal_rows] == ["host_turn_failed"]
    assert store.ledger.rows()[-1]["terminal_reason"] == "host_turn_incomplete"
    assert SessionLifecycleStore(tmp_path).snapshot(
        "lifecycle-1"
    ).abandoned_turn_ids == ("turn-1",)


def test_child_stop_after_finalized_terminal_is_idempotent(tmp_path) -> None:
    store = open_child_store(tmp_path)
    store.record_host_turn_end(
        context(
            "end",
            source_kind="system",
            turn_id="turn-1",
            supported_hooks=ALL_HOOKS,
        ),
        "host_turn_failed",
    )
    store.record_hook(
        context("finalize", source_kind="system", supported_hooks=ALL_HOOKS),
        "on_session_finalize",
    )
    rows_before = store.ledger.rows()

    receipt = store.record_host_child_stop("session-1", "host_turn_failed")

    assert isinstance(receipt, SessionTurnTerminalReceipt)
    assert receipt.deduplicated is True
    assert store.ledger.rows() == rows_before


def test_child_stop_closes_legacy_lifecycle_without_rewriting_hooks(tmp_path) -> None:
    supported = frozenset(HOOK_ORDER[:-1])
    store = open_child_store(tmp_path, supported)

    first = store.record_host_child_stop("session-1", "host_turn_failed")
    replay = store.record_host_child_stop("session-1", "host_turn_failed")

    assert first.deduplicated is False
    assert first.reason == "host_turn_failed"
    assert replay.deduplicated is True
    assert first.snapshot.supported_hooks == supported
    assert "subagent_stop" not in first.snapshot.hooks
    assert [row["kind"] for row in store.ledger.rows()] == [
        "hook",
        "hook",
        "hook",
        "turn_terminal",
    ]


def test_child_stop_rejects_ambiguous_identity_and_conflicting_reason(tmp_path) -> None:
    supported = frozenset({"pre_llm_call"})
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context(
            "pre-a", lifecycle_id="life-a", turn_id="turn-a", supported_hooks=supported
        ),
        "pre_llm_call",
    )
    store.record_hook(
        context(
            "pre-b", lifecycle_id="life-b", turn_id="turn-b", supported_hooks=supported
        ),
        "pre_llm_call",
    )

    with pytest.raises(SessionLifecycleError, match="multiple Moonbite"):
        store.record_host_child_stop("session-1", "host_turn_failed")

    store = open_child_store(tmp_path / "conflict")
    store.record_host_child_stop("session-1", "host_turn_failed")
    with pytest.raises(SessionLifecycleError, match="conflicts"):
        store.record_host_child_stop("session-1", "host_turn_interrupted")


def test_late_post_cannot_replace_child_stop_terminal(tmp_path) -> None:
    store = open_child_store(tmp_path)
    store.record_host_child_stop("session-1", "host_turn_interrupted")

    with pytest.raises(SessionLifecycleError, match="already terminal"):
        store.record_hook(
            context(
                "post",
                source_kind="assistant_response",
                turn_id="turn-1",
                fresh=False,
                supported_hooks=ALL_HOOKS,
            ),
            "post_llm_call",
            settled=True,
        )


def test_multiple_turns_require_settled_previous_turn(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    for hook, source, turn, settled in (
        ("pre_gateway_dispatch", "gateway", None, False),
        ("on_session_start", "start", None, False),
        ("pre_llm_call", "pre-1", "turn-1", False),
        ("post_llm_call", "post-1", "turn-1", True),
        ("pre_llm_call", "pre-2", "turn-2", False),
        ("post_llm_call", "post-2", "turn-2", True),
        ("on_session_finalize", "finalize", None, False),
    ):
        source_kind = {
            "on_session_start": "session_start",
            "post_llm_call": "assistant_response",
            "on_session_finalize": "system",
        }.get(hook, "private_inbound")
        store.record_hook(
            context(source, source_kind=source_kind, turn_id=turn),
            hook,
            settled=settled,
        )
    assert store.snapshot("lifecycle-1").settled_turn_ids == ("turn-1", "turn-2")

    with pytest.raises(SessionLifecycleError, match="finalized"):
        store.record_hook(context("pre-repeat", turn_id="turn-1"), "pre_llm_call")


@pytest.mark.parametrize("source_kind", sorted(SOURCE_KINDS - {"private_inbound"}))
def test_non_private_sources_never_count_as_contact(tmp_path, source_kind) -> None:
    store = SessionLifecycleStore(tmp_path)
    if source_kind == "session_start":
        supported = frozenset({"on_session_start"})
        receipt = store.record_hook(
            context(
                "source",
                source_kind=source_kind,
                supported_hooks=supported,
            ),
            "on_session_start",
        )
    elif source_kind == "assistant_response":
        supported = frozenset({"pre_llm_call", "post_llm_call"})
        store.record_hook(
            context(
                "pre",
                source_kind="system",
                turn_id="turn-1",
                supported_hooks=supported,
            ),
            "pre_llm_call",
        )
        receipt = store.record_hook(
            context(
                "source",
                source_kind=source_kind,
                turn_id="turn-1",
                supported_hooks=supported,
            ),
            "post_llm_call",
            settled=True,
        )
    else:
        receipt = store.record_hook(
            context("source", source_kind=source_kind),
            "pre_gateway_dispatch",
        )
    assert receipt.context.counts_as_private_contact is False
    assert receipt.snapshot.private_contact_count == 0


@pytest.mark.parametrize(
    ("hook", "source_kind", "turn_id"),
    [
        ("on_session_start", "system", None),
        ("post_llm_call", "system", "turn-1"),
        ("on_session_finalize", "assistant_response", None),
        ("pre_gateway_dispatch", "session_start", None),
        ("pre_gateway_dispatch", "assistant_response", None),
        ("pre_llm_call", "session_start", "turn-1"),
        ("pre_llm_call", "assistant_response", "turn-1"),
    ],
)
def test_incompatible_hook_source_pairs_fail_closed(
    tmp_path, hook, source_kind, turn_id
) -> None:
    store = SessionLifecycleStore(tmp_path)
    with pytest.raises(SessionLifecycleError, match="source_kind"):
        store.record_hook(
            context("incompatible", source_kind=source_kind, turn_id=turn_id),
            hook,
        )
    assert store.ledger.rows() == []


def test_stale_private_source_is_rejected_without_a_row(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    with pytest.raises(SessionLifecycleError, match="stale private"):
        store.record_hook(context("stale", fresh=False), "pre_gateway_dispatch")
    assert store.ledger.rows() == []


def test_duplicate_callback_deduplicates_and_counts_contact_once(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    item = context("private-source")
    first = store.record_hook(item, "pre_gateway_dispatch")
    duplicate = store.record_hook(item, "pre_gateway_dispatch")
    assert first.deduplicated is False
    assert duplicate.deduplicated is True
    assert duplicate.event_id == first.event_id
    assert duplicate.snapshot.private_contact_count == 1
    assert len(store.ledger.rows()) == 1


def test_duplicate_identity_ignores_later_observed_at_and_returns_original(
    tmp_path,
) -> None:
    store = SessionLifecycleStore(tmp_path)
    first = store.record_hook(
        context("private-source", observed_at=NOW), "pre_gateway_dispatch"
    )
    duplicate = store.record_hook(
        context("private-source", observed_at=NOW + timedelta(seconds=30)),
        "pre_gateway_dispatch",
    )
    assert duplicate.deduplicated is True
    assert duplicate.event_id == first.event_id
    assert duplicate.context.observed_at == first.context.observed_at == NOW
    assert len(store.ledger.rows()) == 1


def test_duplicate_identity_rejects_source_semantic_conflict(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(context("private-source"), "pre_gateway_dispatch")
    with pytest.raises(SessionLifecycleError, match="identity conflicts"):
        store.record_hook(
            context("private-source", source_kind="system"),
            "pre_gateway_dispatch",
        )
    assert len(store.ledger.rows()) == 1


def test_private_contact_count_is_unique_by_source_id_within_lifecycle(
    tmp_path,
) -> None:
    store = SessionLifecycleStore(tmp_path)
    first = store.record_hook(context("private-source-a"), "pre_gateway_dispatch")
    start = store.record_hook(
        context("session-start", source_kind="session_start"),
        "on_session_start",
    )
    same_source = store.record_hook(
        context("private-source-a", turn_id="turn-1"), "pre_llm_call"
    )
    post = store.record_hook(
        context(
            "assistant-response",
            source_kind="assistant_response",
            turn_id="turn-1",
        ),
        "post_llm_call",
        settled=True,
    )
    distinct_source = store.record_hook(
        context("private-source-b", turn_id="turn-2"), "pre_llm_call"
    )
    assert first.snapshot.private_contact_count == 1
    assert start.snapshot.private_contact_count == 1
    assert same_source.snapshot.private_contact_count == 1
    assert post.snapshot.private_contact_count == 1
    assert distinct_source.snapshot.private_contact_count == 2


def test_missing_out_of_order_mismatched_turn_and_capabilities_do_not_append(
    tmp_path,
) -> None:
    store = SessionLifecycleStore(tmp_path)
    with pytest.raises(SessionLifecycleError, match="requires pre_gateway"):
        store.record_hook(
            context("start", source_kind="session_start"),
            "on_session_start",
        )
    assert store.ledger.rows() == []

    store.record_hook(context("gateway"), "pre_gateway_dispatch")
    with pytest.raises(SessionLifecycleError, match="requires on_session_start"):
        store.record_hook(context("pre", turn_id="turn-1"), "pre_llm_call")
    with pytest.raises(SessionLifecycleError, match="changed"):
        store.record_hook(
            context(
                "unsupported",
                source_kind="session_start",
                supported_hooks=frozenset({"pre_gateway_dispatch"}),
            ),
            "on_session_start",
        )
    with pytest.raises(SessionLifecycleError, match="match"):
        store.record_hook(
            context("post", source_kind="assistant_response", turn_id="other"),
            "post_llm_call",
        )
    assert len(store.ledger.rows()) == 1


def test_finalize_rejects_unsettled_turn(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(context("gateway"), "pre_gateway_dispatch")
    store.record_hook(context("start", source_kind="session_start"), "on_session_start")
    store.record_hook(context("pre", turn_id="turn-1"), "pre_llm_call")
    with pytest.raises(SessionLifecycleError, match="settled"):
        store.record_hook(
            context("finalize", source_kind="system"), "on_session_finalize"
        )
    assert len(store.ledger.rows()) == 3


def test_host_finalize_abandons_open_turn_without_settling(tmp_path) -> None:
    store = open_turn_store(tmp_path)

    receipt = store.record_host_finalize(finalize_context())

    assert receipt.snapshot.finalized is True
    assert receipt.snapshot.open_turn_id is None
    assert receipt.snapshot.settled_turn_ids == ()
    assert receipt.snapshot.abandoned_turn_ids == ("turn-1",)
    terminal_rows = [
        row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ]
    assert len(terminal_rows) == 1
    assert terminal_rows[0]["reason"] == "host_session_finalized"
    assert terminal_rows[0]["outcome"] == "abandoned"
    assert [row["hook"] for row in store.ledger.rows() if row["kind"] == "hook"] == [
        "on_session_start",
        "pre_llm_call",
        "on_session_finalize",
    ]


def test_host_finalize_completed_turn_only_records_finalize(tmp_path) -> None:
    store = open_turn_store(tmp_path)
    store.record_hook(
        context(
            "post",
            source_kind="assistant_response",
            turn_id="turn-1",
            fresh=False,
            supported_hooks=FINALIZE_HOOKS,
        ),
        "post_llm_call",
        settled=True,
    )

    receipt = store.record_host_finalize(finalize_context())

    assert receipt.snapshot.finalized is True
    assert receipt.snapshot.settled_turn_ids == ("turn-1",)
    assert receipt.snapshot.abandoned_turn_ids == ()
    assert [row for row in store.ledger.rows() if row["kind"] == "turn_terminal"] == []


def test_host_finalize_is_idempotent_and_late_post_cannot_replace_abandoned_turn(
    tmp_path,
) -> None:
    store = open_turn_store(tmp_path)
    first = store.record_host_finalize(finalize_context())
    rows_after_first = store.ledger.rows()
    second = store.record_host_finalize(
        finalize_context(observed_at=NOW + timedelta(minutes=1))
    )

    assert first.deduplicated is False
    assert second.deduplicated is True
    assert store.ledger.rows() == rows_after_first
    with pytest.raises(SessionLifecycleError, match="already finalized"):
        store.record_hook(
            context(
                "post",
                source_kind="assistant_response",
                turn_id="turn-1",
                fresh=False,
                supported_hooks=FINALIZE_HOOKS,
            ),
            "post_llm_call",
            settled=True,
        )


def test_host_finalize_finalize_append_failure_is_completed_on_retry(
    tmp_path, monkeypatch
) -> None:
    store = open_turn_store(tmp_path)
    original_append = store.ledger.append
    failed = {"value": True}

    def fail_finalize(row):
        if (
            failed["value"]
            and row["kind"] == "hook"
            and row["hook"] == "on_session_finalize"
        ):
            failed["value"] = False
            raise OSError("host finalize append fixture")
        return original_append(row)

    monkeypatch.setattr(store.ledger, "append", fail_finalize)
    with pytest.raises(OSError, match="host finalize append fixture"):
        store.record_host_finalize(finalize_context())
    assert [row["kind"] for row in store.ledger.rows()] == [
        "hook",
        "hook",
        "turn_terminal",
    ]
    assert store.snapshot("lifecycle-1").finalized is False

    monkeypatch.setattr(store.ledger, "append", original_append)
    retry = store.record_host_finalize(
        finalize_context(observed_at=NOW + timedelta(minutes=1))
    )
    assert retry.snapshot.finalized is True
    assert retry.snapshot.abandoned_turn_ids == ("turn-1",)
    assert [row["kind"] for row in store.ledger.rows()] == [
        "hook",
        "hook",
        "turn_terminal",
        "hook",
    ]


def test_successor_pre_abandons_missing_post_without_settling(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(context("gateway"), "pre_gateway_dispatch")
    store.record_hook(
        context("start", source_kind="session_start"),
        "on_session_start",
    )
    store.record_hook(context("pre-a", turn_id="turn-a"), "pre_llm_call")

    successor = store.record_hook(
        context(
            "pre-b",
            turn_id="turn-b",
            observed_at=NOW + timedelta(minutes=1),
        ),
        "pre_llm_call",
    )

    assert successor.snapshot.open_turn_id == "turn-b"
    assert successor.snapshot.settled_turn_ids == ()
    assert successor.snapshot.terminal_turn_ids == ("turn-a",)
    assert successor.snapshot.abandoned_turn_ids == ("turn-a",)
    rows = store.ledger.rows()
    terminal = rows[3]
    assert terminal["schema_version"] == SESSION_TURN_TERMINAL_SCHEMA
    assert terminal["kind"] == "turn_terminal"
    assert terminal["outcome"] == "abandoned"
    assert terminal["reason"] == "superseded_by_new_pre"
    assert terminal["superseded_by_turn_id"] == "turn-b"

    completed = store.record_hook(
        context(
            "post-b",
            source_kind="assistant_response",
            turn_id="turn-b",
            fresh=False,
            observed_at=NOW + timedelta(minutes=2),
        ),
        "post_llm_call",
        settled=True,
    )
    assert completed.snapshot.open_turn_id is None
    assert completed.snapshot.settled_turn_ids == ("turn-b",)
    assert completed.snapshot.terminal_turn_ids == ("turn-a", "turn-b")


def test_late_post_cannot_replace_abandoned_turn(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    store.record_hook(
        context(
            "pre-b",
            turn_id="turn-b",
            supported_hooks=TURN_HOOKS,
            observed_at=NOW + timedelta(minutes=1),
        ),
        "pre_llm_call",
    )
    before = list(store.ledger.rows())

    with pytest.raises(SessionLifecycleError, match="already terminal"):
        store.record_hook(
            context(
                "post-a",
                source_kind="assistant_response",
                turn_id="turn-a",
                fresh=False,
                supported_hooks=TURN_HOOKS,
                observed_at=NOW + timedelta(minutes=2),
            ),
            "post_llm_call",
            settled=True,
        )

    assert store.ledger.rows() == before
    assert store.snapshot("lifecycle-1").open_turn_id == "turn-b"


def test_operator_repair_is_exact_non_success_and_idempotent(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )

    first = store.abandon_open_turn(
        "lifecycle-1",
        "turn-a",
        observed_at=NOW + timedelta(minutes=1),
    )
    second = store.abandon_open_turn(
        "lifecycle-1",
        "turn-a",
        observed_at=NOW + timedelta(minutes=2),
    )

    assert first.deduplicated is False
    assert second.deduplicated is True
    assert second.reason == "operator_repair"
    assert second.outcome == "abandoned"
    assert second.snapshot.settled_turn_ids == ()
    assert second.snapshot.open_turn_id is None
    assert len(store.ledger.rows()) == 2

    with pytest.raises(SessionLifecycleError, match="not found"):
        store.abandon_open_turn("lifecycle-1", "unknown")


def test_repair_compare_and_set_rejects_changed_or_completed_turn(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    store.record_hook(
        context(
            "post-a",
            source_kind="assistant_response",
            turn_id="turn-a",
            fresh=False,
            supported_hooks=TURN_HOOKS,
            observed_at=NOW + timedelta(minutes=1),
        ),
        "post_llm_call",
        settled=True,
    )
    store.record_hook(
        context(
            "pre-b",
            turn_id="turn-b",
            supported_hooks=TURN_HOOKS,
            observed_at=NOW + timedelta(minutes=2),
        ),
        "pre_llm_call",
    )
    with pytest.raises(SessionLifecycleError, match="already terminal"):
        store.abandon_open_turn("lifecycle-1", "turn-a")


def test_finalize_accepts_completed_and_abandoned_terminal_mix(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(context("gateway"), "pre_gateway_dispatch")
    store.record_hook(
        context("start", source_kind="session_start"),
        "on_session_start",
    )
    store.record_hook(context("pre-a", turn_id="turn-a"), "pre_llm_call")
    store.record_hook(
        context(
            "post-a", source_kind="assistant_response", turn_id="turn-a", fresh=False
        ),
        "post_llm_call",
        settled=True,
    )
    store.record_hook(
        context(
            "pre-b",
            turn_id="turn-b",
            observed_at=NOW + timedelta(minutes=1),
        ),
        "pre_llm_call",
    )
    store.abandon_open_turn(
        "lifecycle-1",
        "turn-b",
        observed_at=NOW + timedelta(minutes=2),
    )
    finalized = store.record_hook(
        context("finalize", source_kind="system", fresh=False),
        "on_session_finalize",
    )
    assert finalized.snapshot.finalized is True
    assert finalized.snapshot.settled_turn_ids == ("turn-a",)
    assert finalized.snapshot.abandoned_turn_ids == ("turn-b",)


def test_terminal_append_failure_does_not_open_successor(tmp_path, monkeypatch) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    original_append = store.ledger.append

    def fail_terminal(row):
        if row["kind"] == "turn_terminal":
            raise OSError("terminal append fixture")
        return original_append(row)

    monkeypatch.setattr(store.ledger, "append", fail_terminal)
    with pytest.raises(OSError, match="terminal append fixture"):
        store.record_hook(
            context(
                "pre-b",
                turn_id="turn-b",
                supported_hooks=TURN_HOOKS,
                observed_at=NOW + timedelta(minutes=1),
            ),
            "pre_llm_call",
        )
    assert store.ledger.rows()[0]["turn_id"] == "turn-a"
    monkeypatch.setattr(store.ledger, "append", original_append)
    assert (
        store.record_hook(
            context(
                "pre-b",
                turn_id="turn-b",
                supported_hooks=TURN_HOOKS,
                observed_at=NOW + timedelta(minutes=1),
            ),
            "pre_llm_call",
        ).snapshot.open_turn_id
        == "turn-b"
    )


def test_successor_append_failure_leaves_terminal_for_retry(
    tmp_path, monkeypatch
) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    original_append = store.ledger.append
    failed = {"value": True}

    def fail_successor(row):
        if failed["value"] and row["kind"] == "hook" and row["turn_id"] == "turn-b":
            failed["value"] = False
            raise OSError("successor append fixture")
        return original_append(row)

    monkeypatch.setattr(store.ledger, "append", fail_successor)
    with pytest.raises(OSError, match="successor append fixture"):
        store.record_hook(
            context(
                "pre-b",
                turn_id="turn-b",
                supported_hooks=TURN_HOOKS,
                observed_at=NOW + timedelta(minutes=1),
            ),
            "pre_llm_call",
        )
    assert [row["kind"] for row in store.ledger.rows()] == ["hook", "turn_terminal"]
    monkeypatch.setattr(store.ledger, "append", original_append)
    retry = store.record_hook(
        context(
            "pre-b",
            turn_id="turn-b",
            supported_hooks=TURN_HOOKS,
            observed_at=NOW + timedelta(minutes=1),
        ),
        "pre_llm_call",
    )
    assert retry.deduplicated is False
    assert retry.snapshot.abandoned_turn_ids == ("turn-a",)
    assert len(store.ledger.rows()) == 3


def test_mixed_ledger_replays_old_hook_rows_and_new_terminal_rows(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    repaired = store.abandon_open_turn("lifecycle-1", "turn-a", NOW)
    assert repaired.deduplicated is False

    reopened = SessionLifecycleStore(tmp_path)
    snapshot = reopened.replay()[0]
    assert snapshot.open_turn_id is None
    assert snapshot.terminal_turn_ids == ("turn-a",)
    assert snapshot.abandoned_turn_ids == ("turn-a",)
    assert snapshot.settled_turn_ids == ()


def test_concurrent_successors_and_repairs_have_one_terminal_per_turn(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )

    def successor(turn_id: str):
        return store.record_hook(
            context(
                f"pre-{turn_id}",
                turn_id=turn_id,
                supported_hooks=TURN_HOOKS,
            ),
            "pre_llm_call",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(successor, ("turn-b", "turn-c")))
    assert all(receipt.snapshot.open_turn_id for receipt in receipts)
    terminal_rows = [
        row for row in store.ledger.rows() if row["kind"] == "turn_terminal"
    ]
    assert len(terminal_rows) == 2
    assert len({row["turn_id"] for row in terminal_rows}) == 2
    assert store.snapshot("lifecycle-1").open_turn_id in {"turn-b", "turn-c"}

    duplicate_store = SessionLifecycleStore(tmp_path / "duplicate-successor")
    duplicate_store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )

    def duplicate_successor(_index: int):
        return duplicate_store.record_hook(
            context("pre-b", turn_id="turn-b", supported_hooks=TURN_HOOKS),
            "pre_llm_call",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        duplicate_receipts = list(pool.map(duplicate_successor, range(16)))
    assert sum(not receipt.deduplicated for receipt in duplicate_receipts) == 1
    assert duplicate_store.snapshot("lifecycle-1").open_turn_id == "turn-b"
    assert [
        row["turn_id"]
        for row in duplicate_store.ledger.rows()
        if row["kind"] == "turn_terminal"
    ] == ["turn-a"]

    repair_store = SessionLifecycleStore(tmp_path / "repair")
    repair_store.record_hook(
        context("pre", turn_id="turn", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        repairs = list(
            pool.map(
                lambda _: repair_store.abandon_open_turn("lifecycle-1", "turn"),
                range(16),
            )
        )
    assert sum(not repair.deduplicated for repair in repairs) == 1
    assert len(repair_store.ledger.rows()) == 2

    race_store = SessionLifecycleStore(tmp_path / "repair-vs-pre")
    race_store.record_hook(
        context("pre-a", turn_id="turn-a", supported_hooks=TURN_HOOKS),
        "pre_llm_call",
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        successor_future = pool.submit(
            race_store.record_hook,
            context("pre-b", turn_id="turn-b", supported_hooks=TURN_HOOKS),
            "pre_llm_call",
        )
        repair_future = pool.submit(
            race_store.abandon_open_turn,
            "lifecycle-1",
            "turn-a",
        )
        successor_future.result()
        repair_future.result()
    assert race_store.snapshot("lifecycle-1").open_turn_id == "turn-b"
    assert [
        row["turn_id"]
        for row in race_store.ledger.rows()
        if row["kind"] == "turn_terminal"
    ] == ["turn-a"]


def test_append_failure_is_fail_closed_and_retryable(tmp_path, monkeypatch) -> None:
    store = SessionLifecycleStore(tmp_path)
    original_append = store.ledger.append

    def fail(_row):
        raise OSError("fixture append failure")

    monkeypatch.setattr(store.ledger, "append", fail)
    with pytest.raises(OSError, match="fixture append failure"):
        store.record_hook(context("gateway"), "pre_gateway_dispatch")
    assert store.ledger.rows() == []

    monkeypatch.setattr(store.ledger, "append", original_append)
    receipt = store.record_hook(context("gateway"), "pre_gateway_dispatch")
    assert receipt.deduplicated is False
    assert len(store.ledger.rows()) == 1


def test_corrupt_ledger_fails_closed(tmp_path) -> None:
    (tmp_path / "session_lifecycle.jsonl").write_text(
        json.dumps({"schema_version": "wrong"}) + "\n", encoding="utf-8"
    )
    with pytest.raises((StateError, SessionLifecycleError)):
        SessionLifecycleStore(tmp_path).replay()


def test_concurrent_duplicate_records_one_row(tmp_path) -> None:
    store = SessionLifecycleStore(tmp_path)
    item = context("same-source")
    with ThreadPoolExecutor(max_workers=12) as pool:
        receipts = list(
            pool.map(
                lambda _: store.record_hook(item, "pre_gateway_dispatch"),
                range(32),
            )
        )
    assert sum(not receipt.deduplicated for receipt in receipts) == 1
    assert len(store.ledger.rows()) == 1
    assert store.snapshot("lifecycle-1").private_contact_count == 1
