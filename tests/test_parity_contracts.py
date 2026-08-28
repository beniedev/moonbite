from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "parity"

CONTRACT_VERSIONS = {
    "state_owner_contract": "moonbite.parity.state_owner_contract.v1",
    "session_lifecycle_contract": "moonbite.parity.session_lifecycle_contract.v1",
    "conversation_bridge_contract": "moonbite.parity.conversation_bridge_contract.v1",
    "effect_receipt_contract": "moonbite.parity.effect_receipt_contract.v1",
    "heartbeat_contract": "moonbite.parity.heartbeat_contract.v1",
    "memory_orchestration_contract": "moonbite.parity.memory_orchestration_contract.v1",
    "panel_autonomy_contract": "moonbite.parity.panel_autonomy_contract.v1",
    "observer_contract": "moonbite.parity.observer_contract.v1",
}

REQUIRED_TOP_LEVEL_KEYS = {
    "schema_version",
    "purpose",
    "owner_boundary",
    "portable_invariants",
    "scenarios",
}
REQUIRED_SCENARIO_KEYS = {"id", "given", "when", "expected"}
TERMINAL_STATES = {
    "accepted",
    "archived",
    "checkpointed",
    "consumed",
    "cooldown",
    "current",
    "degraded",
    "deduplicated",
    "due",
    "executed_unverified",
    "exposed",
    "expired",
    "failed",
    "fallback_selected",
    "isolated",
    "neutral",
    "no_op",
    "not_due",
    "observed",
    "opened",
    "pending",
    "projected",
    "recovered_history",
    "rejected",
    "requeued",
    "skipped",
    "verified",
}
REQUIRED_STATE_DOMAINS = {
    "event",
    "audit",
    "control",
    "cadence",
    "panel",
    "memory",
    "session",
    "effect",
}

EXPECTED_SCENARIOS = {
    "state_owner_contract": {
        "standalone_owner",
        "injected_owner",
        "partial_injection_reject",
        "double_writer_reject",
        "write_failure_fail_closed",
    },
    "session_lifecycle_contract": {
        "fresh_private_inbound",
        "cron_workroom_system_isolation",
        "duplicate_hook",
        "stale_source",
        "finalize_persistence_failure",
        "ordered_hook_lifecycle",
    },
    "conversation_bridge_contract": {
        "dirty_to_quiet_or_overdue_to_checkpoint",
        "unsettled_skip",
        "active_chat_gate",
        "checkpoint_retry_idempotency",
        "missing_source_failure",
    },
    "effect_receipt_contract": {
        "intent_to_pending",
        "queue_accepted_is_unverified",
        "verified_receipt",
        "failed_effect",
        "expired_pending",
        "expired_effect_requeued",
        "missing_pending_reject",
        "stale_replay_reject",
    },
    "heartbeat_contract": {
        "no_event_neutral_skip",
        "not_due_skip",
        "cooldown_skip",
        "recent_private_inbound_blocks",
        "recent_verified_visible_contact_blocks",
        "active_chat_gate",
        "daily_anchor_due",
        "pending_effect_awaits_receipt",
        "expired_effect_reconciliation",
        "queued_unverified_not_contact",
    },
    "memory_orchestration_contract": {
        "source_ref_to_exact_open",
        "exposed_not_used",
        "reply_use_marks_consumed",
        "writer_failure",
        "approval_required",
        "archive_only_retention",
        "historical_date_framing",
    },
    "panel_autonomy_contract": {
        "owner_scoped_reset",
        "consume_once_same_source",
        "same_event_afterglow_no_ttl_refresh",
        "provider_return_without_evidence_unverified",
        "verified_completion_projection",
        "one_shot_only_verified_completion_consume",
        "ineligible_provider_fallback",
        "selected_failure_no_reroll",
    },
    "observer_contract": {
        "neutral_observation",
        "current_incident",
        "recovered_history",
        "corrupt_ledger_degraded",
        "probe_is_read_only",
    },
}

FORBIDDEN_OPERATION = re.compile(r"(?i)(?:^|[_ -])(delete|purge)(?:$|[_ -])")
ABSOLUTE_OR_ENDPOINT = re.compile(
    r"(?:^[A-Za-z]:[\\/]|^/|^\\\\|https?://|ssh://|"
    r"(?:127\.0\.0\.1|10\.|192\.168\.|172\.(?:1[6-9]|2\d|3[01])\.)|"
    r"@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"
)
IDENTIFIER_KEY = re.compile(r"(?:^|_)(?:id|ref|owner|digest|version)$")


def load_contract(name: str) -> dict[str, Any]:
    path = FIXTURE_ROOT / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def scenario_map(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {scenario["id"]: scenario for scenario in contract["scenarios"]}


def scenario(contract: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    return scenario_map(contract)[scenario_id]


def walk_strings(value: Any, path: tuple[str, ...] = ()):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk_strings(child, path + (str(key),))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_strings(child, path + (str(index),))
    elif isinstance(value, str):
        yield path, value


def evidence_shape(evidence: dict[str, Any], verified: bool, *, kind: str) -> None:
    assert evidence["verified"] is verified
    assert evidence["kind"] == kind
    required = (
        "receipt_id",
        "event_id",
        "observed_at",
        "content_sha256",
        "content_length",
    )
    if verified:
        assert all(evidence[key] for key in required)
        assert re.fullmatch(r"[0-9a-f]{64}", evidence["content_sha256"])
        assert isinstance(evidence["content_length"], int)
        assert evidence["content_length"] > 0
    else:
        assert all(evidence[key] is None for key in required)


def test_eight_parity_contract_versions_and_shape_are_frozen():
    assert sorted(path.stem for path in FIXTURE_ROOT.glob("*.json")) == sorted(
        CONTRACT_VERSIONS
    )

    for name, version in CONTRACT_VERSIONS.items():
        contract = load_contract(name)
        assert set(contract) >= REQUIRED_TOP_LEVEL_KEYS
        assert contract["schema_version"] == version
        assert isinstance(contract["purpose"], str) and contract["purpose"]
        assert isinstance(contract["owner_boundary"], dict)
        assert set(contract["owner_boundary"]) >= {"portable", "host"}
        assert contract["portable_invariants"]
        assert isinstance(contract["scenarios"], list)

        ids = [item.get("id") for item in contract["scenarios"]]
        assert len(ids) == len(set(ids))
        assert set(ids) == EXPECTED_SCENARIOS[name]
        for item in contract["scenarios"]:
            assert set(item) == REQUIRED_SCENARIO_KEYS
            assert item["id"]
            assert isinstance(item["given"], dict)
            assert isinstance(item["when"], dict)
            assert isinstance(item["expected"], dict)
            assert item["expected"]["terminal"] in TERMINAL_STATES


def test_public_fixtures_are_synthetic_and_have_no_removal_operations():
    for path in sorted(FIXTURE_ROOT.glob("*.json")):
        contract = json.loads(path.read_text(encoding="utf-8"))
        for location, value in walk_strings(contract):
            assert not ABSOLUTE_OR_ENDPOINT.search(value), (path.name, location, value)
            assert not FORBIDDEN_OPERATION.search(value), (path.name, location, value)

            key = location[-1] if location else ""
            if key == "id" and "scenarios" in location:
                continue
            if IDENTIFIER_KEY.search(key) and key not in {"schema_version"}:
                assert value.startswith("fixture-") or value in {
                    "outside_contract",
                    "read_only",
                }, (path.name, location, value)


def test_state_owner_contract_rejects_parallel_or_partial_ownership():
    contract = load_contract("state_owner_contract")
    standalone = scenario(contract, "standalone_owner")
    injected = scenario(contract, "injected_owner")
    partial = scenario(contract, "partial_injection_reject")
    double = scenario(contract, "double_writer_reject")
    failure = scenario(contract, "write_failure_fail_closed")

    assert set(contract["required_domains"]) == REQUIRED_STATE_DOMAINS
    for item in (standalone, injected, double, failure):
        assert set(item["given"]["required_domains"]) == REQUIRED_STATE_DOMAINS
    assert set(standalone["given"]["owned_domains"]) == REQUIRED_STATE_DOMAINS
    assert set(standalone["given"]["injected_domains"]) == set()
    for item in (injected, double, failure):
        assert set(item["given"]["injected_domains"]) == REQUIRED_STATE_DOMAINS
    assert set(partial["given"]["injected_domains"]) < REQUIRED_STATE_DOMAINS

    assert standalone["expected"]["local_writes"] is True
    assert standalone["expected"]["writer_count"] == 1
    assert injected["expected"]["local_writes"] is False
    assert injected["expected"]["writer_count"] == 1
    assert partial["expected"]["terminal"] == "rejected"
    assert partial["expected"]["reason"] == "incomplete_owner_contract"
    assert partial["expected"]["local_writes"] is False
    assert double["expected"]["terminal"] == "rejected"
    assert double["expected"]["reason"] == "multiple_state_writers"
    assert double["expected"]["local_writes"] is False
    assert failure["expected"]["terminal"] == "failed"
    assert failure["expected"]["fail_closed"] is True
    assert failure["expected"]["commit_visible"] is False


def test_session_contract_separates_contact_from_non_user_sources():
    contract = load_contract("session_lifecycle_contract")
    fresh = scenario(contract, "fresh_private_inbound")
    isolated = scenario(contract, "cron_workroom_system_isolation")
    duplicate = scenario(contract, "duplicate_hook")
    stale = scenario(contract, "stale_source")
    finalize = scenario(contract, "finalize_persistence_failure")
    ordered = scenario(contract, "ordered_hook_lifecycle")

    assert fresh["given"]["source_kind"] == "private_inbound"
    assert fresh["expected"]["counts_as_private_contact"] is True
    assert isolated["expected"]["counts_as_private_contact"] is False
    assert set(isolated["given"]["sources"]) == {
        "cron",
        "workroom",
        "system",
        "tool",
        "assistant_response",
        "session_start",
    }
    assert set(isolated["expected"]["source_decisions"].values()) == {"isolated"}
    assert duplicate["expected"]["terminal"] == "deduplicated"
    assert duplicate["expected"]["persisted_source_count"] == 1
    assert stale["expected"]["terminal"] == "rejected"
    assert stale["expected"]["counts_as_private_contact"] is False
    assert finalize["expected"]["terminal"] == "failed"
    assert finalize["expected"]["fail_closed"] is True
    assert finalize["expected"]["raw_material_preserved"] is True
    expected_order = [
        "pre_gateway_dispatch",
        "on_session_start",
        "pre_llm_call",
        "post_llm_call",
        "on_session_finalize",
    ]
    assert ordered["expected"]["hook_order"] == expected_order
    assert ordered["expected"]["settled_before_finalize"] is True
    invalid_variants = {
        item["name"]: item for item in ordered["expected"]["invalid_variants"]
    }
    assert {"duplicate_hook", "missing_hook", "out_of_order"} == set(invalid_variants)
    assert all(item["terminal"] == "rejected" for item in invalid_variants.values())


def test_conversation_bridge_requires_settlement_source_and_idempotent_checkpoint():
    contract = load_contract("conversation_bridge_contract")
    transition = scenario(contract, "dirty_to_quiet_or_overdue_to_checkpoint")
    unsettled = scenario(contract, "unsettled_skip")
    active = scenario(contract, "active_chat_gate")
    retry = scenario(contract, "checkpoint_retry_idempotency")
    missing = scenario(contract, "missing_source_failure")

    paths = transition["expected"]["state_paths"]
    assert all(path[0:2] == ["clean", "dirty"] for path in paths)
    assert {path[2] for path in paths} == {"quiet", "overdue"}
    assert all(path[-1] == "checkpointed" for path in paths)
    assert transition["expected"]["checkpoint_count"] == 1
    for blocked in (unsettled, active):
        assert blocked["expected"]["terminal"] == "skipped"
        assert blocked["expected"]["checkpoint_written"] is False
        assert blocked["expected"]["side_effect_count"] == 0
    assert retry["expected"]["idempotent"] is True
    assert retry["expected"]["checkpoint_count"] == 1
    assert missing["expected"]["terminal"] == "failed"
    assert missing["expected"]["fail_closed"] is True


def test_effect_receipt_contract_covers_verified_evidence_and_replay_failures():
    contract = load_contract("effect_receipt_contract")
    all_states = set()
    for item in contract["scenarios"]:
        given_state = item["given"].get("state")
        if isinstance(given_state, str):
            all_states.add(given_state)
        elif isinstance(given_state, list):
            all_states.update(given_state)
        state_path = item["expected"].get("state_path", [])
        if isinstance(state_path, str):
            all_states.add(state_path)
        else:
            all_states.update(state_path)
        all_states.add(item["expected"]["terminal"])
        if state_path:
            assert item["expected"]["terminal"] in state_path
    assert {
        "intent",
        "pending",
        "executed_unverified",
        "verified",
        "failed",
        "expired",
        "requeued",
    } <= all_states

    unverified = scenario(contract, "queue_accepted_is_unverified")["expected"]
    verified = scenario(contract, "verified_receipt")["expected"]
    missing = scenario(contract, "missing_pending_reject")["expected"]
    stale = scenario(contract, "stale_replay_reject")["expected"]
    evidence_shape(unverified["evidence"], False, kind="delivery_receipt")
    evidence_shape(verified["evidence"], True, kind="delivery_receipt")
    for item in contract["scenarios"]:
        assert item["given"]["idempotency_key"].startswith("fixture-")
        assert item["given"]["source_event_id"].startswith("fixture-")
        assert item["expected"]["idempotency_key"] == item["given"]["idempotency_key"]
        assert item["expected"]["source_event_id"] == item["given"]["source_event_id"]
        evidence = item["expected"]["evidence"]
        evidence_shape(
            evidence, item["expected"].get("verified", False), kind="delivery_receipt"
        )
    assert unverified["queue_accepted"] is True
    assert unverified["verified"] is False
    assert missing["terminal"] == "rejected"
    assert missing["synthetic_success"] is False
    assert stale["terminal"] == "rejected"
    assert stale["synthetic_success"] is False


def test_heartbeat_contract_freezes_neutral_gates_contact_signals_and_reconciliation():
    contract = load_contract("heartbeat_contract")
    neutral = scenario(contract, "no_event_neutral_skip")["expected"]
    assert neutral["terminal"] == "neutral"
    assert neutral["model_calls"] == neutral["writes"] == 0
    assert neutral["wake_effects"] == neutral["delivery_effects"] == 0

    for scenario_id, reason in (
        ("not_due_skip", "cadence_not_due"),
        ("cooldown_skip", "effect_cooldown"),
        ("recent_private_inbound_blocks", "recent_private_inbound"),
        ("recent_verified_visible_contact_blocks", "recent_verified_visible_contact"),
        ("active_chat_gate", "active_chat"),
    ):
        expected = scenario(contract, scenario_id)["expected"]
        assert expected["reason"] == reason
        assert expected["next_judge_at"]
        assert expected["model_calls"] == expected["writes"] == 0
        assert expected["wake_effects"] == 0

    visible = scenario(contract, "recent_verified_visible_contact_blocks")
    assert visible["expected"]["requires_verified_receipt"] is True
    anchor = scenario(contract, "daily_anchor_due")["expected"]
    assert anchor["terminal"] == "due"
    assert anchor["reason"] == "daily_anchor"
    assert anchor["judge_required"] is True
    awaiting = scenario(contract, "pending_effect_awaits_receipt")["expected"]
    assert awaiting["terminal"] == "pending"
    assert awaiting["new_effect_selection"] is False
    assert awaiting["requeue_count"] == 0
    expired = scenario(contract, "expired_effect_reconciliation")["expected"]
    assert expired["terminal"] == "requeued"
    assert expired["new_effect_selection"] is False
    assert expired["requeue_count"] == 1
    queued = scenario(contract, "queued_unverified_not_contact")["expected"]
    assert queued["recent_visible_contact"] is False
    assert queued["requires_verified_receipt"] is True


def test_memory_contract_separates_exposure_use_and_archive_retention():
    contract = load_contract("memory_orchestration_contract")
    opened = scenario(contract, "source_ref_to_exact_open")["expected"]
    exposed = scenario(contract, "exposed_not_used")["expected"]
    consumed = scenario(contract, "reply_use_marks_consumed")["expected"]
    writer_failure = scenario(contract, "writer_failure")["expected"]
    approval = scenario(contract, "approval_required")["expected"]
    archived = scenario(contract, "archive_only_retention")["expected"]
    historical = scenario(contract, "historical_date_framing")["expected"]

    assert opened["source_ref"] and opened["exact_item_id"]
    assert opened["content_used"] is True
    assert exposed["exposed"] is True
    assert exposed["used"] is False
    assert exposed["consumed"] is False
    assert consumed["used"] is True
    assert consumed["consumed"] is True
    assert consumed["consumption_reason"] == "reply_use"
    assert writer_failure["terminal"] == "failed"
    assert writer_failure["fail_closed"] is True
    assert approval["terminal"] == "pending"
    assert approval["write_performed"] is False
    assert archived["terminal"] == "archived"
    assert archived["retention_mode"] == "archive_only"
    assert archived["removal_operation"] == "outside_contract"
    assert historical["framing"] == "historical"
    assert historical["framing_date_field"] == "event_date"
    assert historical["presented_as_current"] is False


def test_panel_autonomy_contract_requires_owner_scope_and_verified_completion():
    contract = load_contract("panel_autonomy_contract")
    reset = scenario(contract, "owner_scoped_reset")["expected"]
    consume = scenario(contract, "consume_once_same_source")["expected"]
    ttl = scenario(contract, "same_event_afterglow_no_ttl_refresh")["expected"]
    unverified = scenario(contract, "provider_return_without_evidence_unverified")[
        "expected"
    ]
    verified_case = scenario(contract, "verified_completion_projection")["expected"]
    one_shot = scenario(contract, "one_shot_only_verified_completion_consume")[
        "expected"
    ]
    fallback = scenario(contract, "ineligible_provider_fallback")["expected"]
    failure = scenario(contract, "selected_failure_no_reroll")["expected"]

    assert reset["owner_scope"] == "fixture-owner-a"
    assert reset["other_owner_changed"] is False
    assert consume["consume_count"] == 1
    assert consume["second_attempt"] == "no_op"
    assert ttl["ttl_refreshed"] is False
    assert ttl["recreated"] is False
    assert unverified["terminal"] == "executed_unverified"
    assert unverified["projection_written"] is False
    assert unverified["one_shot_consumed"] is False
    evidence_shape(
        scenario(contract, "provider_return_without_evidence_unverified")["given"][
            "evidence"
        ],
        False,
        kind="activity_execution_receipt",
    )
    assert verified_case["terminal"] == "projected"
    assert verified_case["projection_written"] is True
    evidence_shape(
        scenario(contract, "verified_completion_projection")["given"]["evidence"],
        True,
        kind="activity_execution_receipt",
    )
    assert one_shot["unverified_consume"] is False
    assert one_shot["verified_consume"] is True
    assert fallback["terminal"] == "fallback_selected"
    assert fallback["ineligible_run_count"] == 0
    assert failure["terminal"] == "failed"
    assert failure["rerolled"] is False


def test_observer_contract_distinguishes_current_recovery_and_degraded_read_only_states():
    contract = load_contract("observer_contract")
    assert (
        scenario(contract, "neutral_observation")["expected"]["terminal"] == "neutral"
    )
    current = scenario(contract, "current_incident")["expected"]
    recovered = scenario(contract, "recovered_history")["expected"]
    degraded = scenario(contract, "corrupt_ledger_degraded")["expected"]
    probe = scenario(contract, "probe_is_read_only")["expected"]

    assert current["terminal"] == "current"
    assert current["current_incident_ids"]
    assert recovered["terminal"] == "recovered_history"
    assert recovered["recovery_evidence_required"] is True
    assert degraded["terminal"] == "degraded"
    assert degraded["fail_closed"] is True
    assert probe["writes"] == probe["effects"] == probe["model_calls"] == 0
    assert probe["side_effects"] == 0


def test_verified_effect_and_panel_evidence_share_one_receipt_semantics():
    effect = load_contract("effect_receipt_contract")
    panel = load_contract("panel_autonomy_contract")

    effect_verified = scenario(effect, "verified_receipt")["expected"]["evidence"]
    panel_verified = scenario(panel, "verified_completion_projection")["given"][
        "evidence"
    ]
    evidence_shape(effect_verified, True, kind="delivery_receipt")
    evidence_shape(panel_verified, True, kind="activity_execution_receipt")
    assert set(effect_verified) == set(panel_verified)
    assert effect_verified["verified"] is panel_verified["verified"] is True
    assert effect_verified["kind"] != panel_verified["kind"]

    effect_unverified = scenario(effect, "queue_accepted_is_unverified")["expected"]
    panel_unverified = scenario(panel, "provider_return_without_evidence_unverified")[
        "expected"
    ]
    evidence_shape(effect_unverified["evidence"], False, kind="delivery_receipt")
    assert panel_unverified["verified"] is False
    assert panel_unverified["projection_written"] is False
    assert panel_unverified["one_shot_consumed"] is False
