from __future__ import annotations

import ast
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from moonbite_plugin.autonomy import ActivityResult
from moonbite_plugin.config import ConfigError, normalize_config
from moonbite_plugin.effects import EffectLedger, EffectReceipt
from moonbite_plugin.heartbeat import HeartbeatCadence
from moonbite_plugin.plugin import TOOL_NAMES
from moonbite_plugin.runtime_core import parse_time
from moonbite_plugin.session import HOOK_ORDER

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "compatibility"
REPO_ROOT = Path(__file__).parents[1]
HOST_REFERENCE = REPO_ROOT / "config" / "host-reference.example.yaml"
PLACEHOLDERS = {
    "PRIVATE_CHAT_REF",
    "MAIN_SESSION_REF",
    "SYSTEM_CHANNEL_ID",
    "AGENT_MAILBOX_CHANNEL_ID",
    "EFFECT_RECEIPT_REF",
}


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def load_host_reference() -> dict:
    value = yaml.safe_load(HOST_REFERENCE.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _assert_exact_keys(value: dict, expected: set[str]) -> None:
    assert set(value) == expected


def test_manifest_contract_script_and_registered_hooks_stay_aligned():
    manifest = yaml.safe_load((REPO_ROOT / "plugin.yaml").read_text(encoding="utf-8"))
    assert tuple(manifest["provides_tools"]) == TOOL_NAMES
    assert tuple(manifest["provides_hooks"]) == HOOK_ORDER

    source = ast.parse(
        (REPO_ROOT / "moonbite_plugin" / "plugin.py").read_text(encoding="utf-8")
    )
    registration_loops = [
        node
        for node in ast.walk(source)
        if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name)
        and node.target.id == "hook_name"
        and isinstance(node.iter, ast.Name)
        and node.iter.id == "HOOK_ORDER"
    ]
    assert len(registration_loops) == 1
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "register_hook"
        and len(node.args) >= 2
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "hook_name"
        for node in ast.walk(registration_loops[0])
    )

    contract_script = (REPO_ROOT / "scripts" / "check-hermes-contract.py").read_text(
        encoding="utf-8"
    )
    assert "from moonbite_plugin.session import HOOK_ORDER" in contract_script
    assert 'tuple(manifest["provides_hooks"]) == HOOK_ORDER' in contract_script
    assert "10 tools/3 hooks" not in contract_script


def test_host_reference_has_strict_schema_and_host_ownership_boundaries():
    reference = load_host_reference()
    _assert_exact_keys(
        reference,
        {
            "schema_version",
            "kind",
            "scope",
            "transport_owner",
            "topology",
            "ports",
            "boundary",
            "failure_outcomes",
            "interaction_capabilities",
        },
    )
    assert reference["schema_version"] == "moonbite.host_delivery_reference.v1"
    assert reference["kind"] == "host_deployment_template"
    assert reference["transport_owner"] == "hermes_official_platform_plugin"
    _assert_exact_keys(
        reference["scope"],
        {"normalized_by_moonbite", "runtime_config", "transport_config"},
    )
    assert reference["scope"] == {
        "normalized_by_moonbite": False,
        "runtime_config": False,
        "transport_config": False,
    }

    topology = reference["topology"]
    _assert_exact_keys(
        topology,
        {"private_chat_to_main_session", "system_channel", "agent_mailbox_channel"},
    )
    _assert_exact_keys(
        topology["private_chat_to_main_session"],
        {"source", "target", "direction", "role", "permission"},
    )
    assert topology["private_chat_to_main_session"]["source"] == "PRIVATE_CHAT_REF"
    assert topology["private_chat_to_main_session"]["target"] == "MAIN_SESSION_REF"
    for channel_name, placeholder in (
        ("system_channel", "SYSTEM_CHANNEL_ID"),
        ("agent_mailbox_channel", "AGENT_MAILBOX_CHANNEL_ID"),
    ):
        _assert_exact_keys(
            topology[channel_name], {"ref", "direction", "role", "permission"}
        )
        assert topology[channel_name]["ref"] == placeholder

    ports = reference["ports"]
    _assert_exact_keys(ports, {"target", "session", "effect_receipt"})
    for port_name, placeholder in (
        ("target", "PRIVATE_CHAT_REF"),
        ("session", "MAIN_SESSION_REF"),
        ("effect_receipt", "EFFECT_RECEIPT_REF"),
    ):
        _assert_exact_keys(ports[port_name], {"ref", "direction", "role", "permission"})
        assert ports[port_name]["ref"] == placeholder
        assert ports[port_name]["direction"] == "host_to_moonbite"
        assert ports[port_name]["permission"] == "host_owned_read_only"

    boundary = reference["boundary"]
    _assert_exact_keys(
        boundary,
        {
            "moonbite_consumes_ports",
            "moonbite_owns",
            "host_owns",
            "state_ownership",
            "effect_receipt_rule",
            "permission_rule",
        },
    )
    assert boundary["moonbite_consumes_ports"] == [
        "target",
        "session",
        "effect_receipt",
    ]
    assert boundary["moonbite_owns"] == [
        "lifecycle_semantics",
        "effect_semantics",
        "receipt_matching",
    ]
    assert "injected_state_storage" in boundary["host_owns"]
    assert "injected_state_writer" in boundary["host_owns"]
    assert boundary["state_ownership"] == {
        "standalone": {
            "owner": "moonbite",
            "storage": "moonbite_owned",
            "writer": "moonbite_owned",
        },
        "injected": {
            "owner": "host",
            "storage": "host_owned",
            "writer": "host_owned",
        },
    }
    assert (
        boundary["effect_receipt_rule"]
        == "only_matching_effect_receipt_can_mark_verified"
    )
    assert boundary["permission_rule"] == "host_owned_ports_are_read_only_to_moonbite"


def test_host_reference_failure_outcomes_are_empty_and_capability_scoped():
    reference = load_host_reference()
    outcomes = reference["failure_outcomes"]
    _assert_exact_keys(outcomes, {"contract", "examples"})
    assert outcomes["contract"] == "host_failure_outcome_not_verified_effect_receipt"
    receipts = outcomes["examples"]
    assert {receipt["code"] for receipt in receipts} == {
        "permission_denied",
        "transport_unavailable",
        "receipt_mismatch",
    }
    for receipt in receipts:
        _assert_exact_keys(receipt, {"code", "ref", "retryable", "state", "content"})
        assert receipt["ref"] == "EFFECT_RECEIPT_REF"
        assert type(receipt["retryable"]) is bool
        assert receipt["content"] is None

    capabilities = reference["interaction_capabilities"]
    _assert_exact_keys(
        capabilities, {"owner", "not_moonbite_capabilities", "statement"}
    )
    assert capabilities["owner"] == "separate_interaction_plugin_or_host_capability"
    assert set(capabilities["not_moonbite_capabilities"]) == {
        "choices",
        "sticker",
        "callback",
        "reaction",
    }
    assert "separate interaction plugin or host capability" in capabilities["statement"]


def test_host_failure_outcomes_cannot_mark_an_effect_verified(tmp_path):
    reference = load_host_reference()
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    ledger = EffectLedger(tmp_path, clock=lambda: now)
    digest = "a" * 64
    ledger.begin_intent(
        "effect-fixture",
        kind="message",
        source_event_id="event-fixture",
        idempotency_key="idem-fixture",
        epoch_id="epoch-fixture",
        content_sha256=digest,
        content_length=12,
        expires_at=now + timedelta(minutes=5),
    )
    ledger.mark_pending("effect-fixture")
    unverified = ledger.mark_queue_accepted("effect-fixture")
    assert unverified.verified is False
    assert all(
        item["content"] is None for item in reference["failure_outcomes"]["examples"]
    )

    mismatched = EffectReceipt(
        receipt_id="receipt-fixture",
        event_id="other-event",
        observed_at=now,
        content_sha256=digest,
        content_length=12,
        epoch_id="epoch-fixture",
    )
    with pytest.raises(ValueError, match="does not match"):
        ledger.verify("effect-fixture", mismatched)

    matching = EffectReceipt(
        receipt_id="receipt-fixture",
        event_id="event-fixture",
        observed_at=now,
        content_sha256=digest,
        content_length=12,
        epoch_id="epoch-fixture",
    )
    verified = ledger.verify("effect-fixture", matching)
    assert verified.verified is True
    assert verified.state == "verified"


def test_host_reference_has_only_synthetic_placeholders_and_no_private_material():
    reference = load_host_reference()
    text = HOST_REFERENCE.read_text(encoding="utf-8")
    assert not re.search(r"(?:https?|file|ssh)://", text, flags=re.IGNORECASE)
    private_shapes = "|".join(
        (
            re.escape("/" + "Users" + "/"),
            re.escape("/" + "home" + "/"),
            re.escape("\\" * 2),
            re.escape("192" + "." + "168" + "."),
            re.escape("172") + r"\.(?:1[6-9]|2[0-9]|3[01])\.",
            re.escape("10") + r"\.\d+\.",
        )
    )
    assert not re.search(private_shapes, text)
    assert not re.search(
        r"(?i)(?:api[_-]?key|token|cookie|oauth|password|secret|credential|endpoint)",
        text,
    )
    assert not re.search(
        r"(?i)(?:real[_ -]?id|actual[_ -]?id|private[_ -]?name|device[_ -]?name)",
        text,
    )

    def walk(value, key: str = ""):
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                yield from walk(child_value, child_key)
        elif isinstance(value, list):
            for child_value in value:
                yield from walk(child_value, key)
        else:
            yield key, value

    for key, value in walk(reference):
        if key in {"source", "target", "ref"}:
            assert value in PLACEHOLDERS
        if key.endswith(("id", "_id")):
            assert value in PLACEHOLDERS
        if isinstance(value, str):
            assert not re.fullmatch(r"\d+", value)

    assert "host_delivery_reference" not in (
        REPO_ROOT / "config" / "schema.json"
    ).read_text(encoding="utf-8")


def test_host_reference_is_not_accepted_as_core_runtime_config():
    with pytest.raises(ConfigError, match="unknown keys"):
        normalize_config(load_host_reference())


def test_contract_fixture_versions_are_frozen():
    assert load_fixture("heartbeat_contract.json")["schema_version"] == (
        "moonbite.heartbeat_contract.v2"
    )
    assert load_fixture("autonomy_contract.json")["schema_version"] == (
        "moonbite.autonomy_contract.v1"
    )
    assert load_fixture("continuity_contract.json")["schema_version"] == (
        "moonbite.continuity_contract.v1"
    )


def test_manual_snooze_contract_keeps_private_reply_non_releasing(tmp_path):
    fixture = load_fixture("heartbeat_contract.json")
    scenarios = {item["id"]: item for item in fixture["scenarios"]}
    now = parse_time(fixture["reference_now"])
    cadence = HeartbeatCadence(tmp_path, clock=lambda: now)
    cadence.snooze(60, manual=True)

    first = scenarios["manual_snooze_blocks_silence_check"]
    assert first["expected"]["action"] == "skip"
    assert cadence.blocked("silence_check") == (True, "manual_snooze")

    cadence.observe_private_reply()
    second = scenarios["fresh_input_keeps_manual_snooze"]
    assert second["expected"]["action"] == "skip"
    assert cadence.blocked("silence_check") == (True, "manual_snooze")


def test_kind_policy_contract_matches_explicit_cadence_bypasses(tmp_path):
    fixture = load_fixture("heartbeat_contract.json")
    scenarios = {item["id"]: item for item in fixture["scenarios"]}
    policies = fixture["kind_policies"]
    now = parse_time(fixture["reference_now"])
    cadence = HeartbeatCadence(tmp_path, clock=lambda: now)

    assert scenarios["daily_anchor_manual_snooze_blocked"]["expected"]["action"] == (
        "skip"
    )
    assert scenarios["urgent_explicit_bypass"]["expected"]["action"] == "judge"
    assert scenarios["emotional_repair_unconfigured_blocks"]["expected"]["action"] == (
        "skip"
    )
    assert scenarios["critical_ops_unconfigured_blocks"]["expected"]["action"] == (
        "skip"
    )
    assert (
        scenarios["connector_failed_suffix_unconfigured_blocks"]["expected"]["action"]
        == "skip"
    )

    cadence.snooze(60, manual=True)
    assert cadence.blocked("routine", bypass=policies["routine"]["bypass"]) == (
        True,
        "manual_snooze",
    )
    assert cadence.blocked(
        "daily_anchor", bypass=policies["daily_anchor"]["bypass"]
    ) == (True, "manual_snooze")
    assert cadence.blocked("urgent", bypass=policies["urgent"]["bypass"]) == (
        False,
        "open",
    )

    cadence.resume()
    cadence.snooze(60, manual=False)
    assert cadence.blocked(
        "daily_anchor", bypass=policies["daily_anchor"]["bypass"]
    ) == (False, "open")
    assert cadence.blocked("urgent", bypass=["manual_snooze"]) == (
        True,
        "automatic_cadence",
    )

    cadence.resume()
    cadence.snooze(60, manual=True)
    assert cadence.blocked("urgent", bypass=["automatic_cooldown"]) == (
        True,
        "manual_snooze",
    )
    assert cadence.blocked(
        "urgent", bypass=["automatic_cooldown", "manual_snooze"]
    ) == (False, "open")

    cadence.resume()
    cadence.snooze(60, manual=True)
    for omitted_kind in ("emotional_repair", "critical_ops", "connector.failed"):
        assert cadence.blocked(omitted_kind) == (True, "manual_snooze")
    assert cadence.blocked(
        "connector_failure_normalized",
        bypass=policies["connector_failure_normalized"]["bypass"],
    ) == (False, "open")


def test_autonomy_contract_keeps_strict_terminal_vocabulary():
    fixture = load_fixture("autonomy_contract.json")
    terminals = {item["id"]: item for item in fixture["terminals"]}

    for scenario_id in ("completed_dry_run", "failed_dry_run"):
        status = terminals[scenario_id]["expected"]["would_status"]
        assert ActivityResult(status, "fixture", "fixture").status == status

    invalid = terminals["invalid_status_rejected"]["kwargs"]["status"]
    with pytest.raises(ValueError, match="invalid activity terminal"):
        ActivityResult(invalid, "fixture", "fixture")


def test_tick_contract_freezes_gate_and_judge_call_expectations():
    ticks = {
        item["id"]: item["expected"]
        for item in load_fixture("autonomy_contract.json")["ticks"]
    }

    assert ticks["active_chat_blocks_autonomy"]["judge_tick_calls"] == 0
    assert ticks["judge_denied_skips_quiet"]["judge_tick_calls"] == 1
    assert ticks["normal_allowed_selects_activity"]["judge_tick_calls"] == 1
    assert ticks["normal_allowed_selects_activity"]["selected"] == "feed_browse"
