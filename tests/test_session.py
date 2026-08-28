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
    SOURCE_KINDS,
    SessionContext,
    SessionLifecycleError,
    SessionLifecycleStore,
)

NOW = datetime(2026, 8, 24, 19, 0, tzinfo=UTC)
ALL_HOOKS = frozenset(HOOK_ORDER)


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
        (context("finalize", source_kind="system"), HOOK_ORDER[4], False),
    ]
    receipts = [
        store.record_hook(item, hook, settled=settled) for item, hook, settled in calls
    ]
    assert [receipt.hook for receipt in receipts] == list(HOOK_ORDER)
    assert receipts[-1].schema_version == SESSION_LIFECYCLE_SCHEMA
    assert receipts[-1].snapshot.finalized is True
    assert receipts[-1].snapshot.settled_turn_ids == ("turn-1",)
    assert len(store.ledger.rows()) == 5


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
    receipt = store.record_hook(
        context("finalize", source_kind="system", supported_hooks=supported),
        "on_session_finalize",
    )
    assert receipt.snapshot.finalized is True
    assert receipt.snapshot.hooks == HOOK_ORDER[1:]


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
