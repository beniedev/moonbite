from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from moonbite_plugin.config import ConfigError, normalize_config
from moonbite_plugin.heartbeat import (
    CADENCE_SCHEMA_V1,
    CADENCE_SCHEMA_V4,
    HeartbeatCadence,
    HeartbeatSilenceReceipt,
)
from moonbite_plugin.runtime_core import StateError
from moonbite_plugin.service import MoonbiteRuntime


NOW = datetime(2026, 8, 22, 19, 0, tzinfo=UTC)
POLICY = {
    "enabled": True,
    "first_minutes": 10,
    "repeat_minutes": 25,
    "max_minutes": 25,
}


def receipt(
    receipt_id: str = "silence-1",
    completed_at: datetime = NOW,
    **kwargs,
) -> HeartbeatSilenceReceipt:
    values = {
        "receipt_id": receipt_id,
        "completed_at": completed_at,
        "profile": "routine",
        "settled": True,
        "intentional_silence": True,
        "judge_terminal": "approved",
        "wake_terminal": "verified",
        "delivery_terminal": "not_requested",
        "manual_override": False,
    }
    values.update(kwargs)
    return HeartbeatSilenceReceipt(**values)


def test_config_policy_is_inert_and_strict():
    policy = normalize_config({})["heartbeat"]["silence_backoff"]
    assert policy == {
        "enabled": False,
        "first_minutes": 120,
        "repeat_minutes": 240,
        "max_minutes": 240,
    }
    with pytest.raises(ConfigError):
        normalize_config({"heartbeat": {"silence_backoff": {"unknown": True}}})
    with pytest.raises(ConfigError):
        normalize_config(
            {
                "heartbeat": {
                    "silence_backoff": {"first_minutes": 20, "repeat_minutes": 10}
                }
            }
        )


def test_receipt_is_content_free_and_strict():
    value = receipt().to_dict()
    assert set(value) == {
        "receipt_id",
        "completed_at",
        "profile",
        "settled",
        "intentional_silence",
        "judge_terminal",
        "wake_terminal",
        "delivery_terminal",
        "manual_override",
    }
    with pytest.raises(ValueError):
        receipt(receipt_id="bad receipt")
    with pytest.raises(ValueError):
        receipt(completed_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        receipt(judge_terminal="not-a-terminal")


def test_backoff_first_repeat_max_and_duplicate_are_durable(tmp_path):
    current = [NOW + timedelta(minutes=2)]
    cadence = HeartbeatCadence(tmp_path, clock=lambda: current[0])
    first = cadence.apply_silence_backoff(receipt(), policy=POLICY)
    assert first["status"] == "applied"
    assert first["streak"] == 1
    assert first["cooldown_until"] == "2026-08-22T19:10:00Z"

    duplicate = cadence.apply_silence_backoff(receipt(), policy=POLICY)
    assert duplicate["status"] == "duplicate"
    assert duplicate["cooldown_until"] == first["cooldown_until"]

    second = cadence.apply_silence_backoff(
        receipt("silence-2", NOW + timedelta(minutes=1)), policy=POLICY
    )
    assert second["streak"] == 2
    assert second["cooldown_until"] == "2026-08-22T19:26:00Z"

    third = cadence.apply_silence_backoff(
        receipt("silence-3", NOW + timedelta(minutes=2)), policy=POLICY
    )
    assert third["streak"] == 3
    assert third["duration_minutes"] == 25

    reloaded = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    assert reloaded.snapshot()["silence_backoff"]["processed_receipts"] == 3


def test_backoff_fail_closed_for_pending_future_order_and_ineligible(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    pending = cadence.apply_silence_backoff(
        receipt("pending", settled=False), policy=POLICY
    )
    assert pending["status"] == "pending_settlement"
    assert cadence.apply_silence_backoff(receipt("pending"), policy=POLICY)["applied"]

    future = cadence.apply_silence_backoff(
        receipt("future", NOW + timedelta(minutes=1)), policy=POLICY
    )
    assert future["status"] == "future_receipt"
    assert (
        cadence.apply_silence_backoff(
            receipt("denied", judge_terminal="denied"), policy=POLICY
        )["status"]
        == "ineligible"
    )
    assert (
        cadence.apply_silence_backoff(
            receipt("daily", profile="daily_anchor"), policy=POLICY
        )["status"]
        == "ineligible"
    )
    assert (
        cadence.apply_silence_backoff(
            receipt("manual", manual_override=True), policy=POLICY
        )["status"]
        == "ineligible"
    )

    out_of_order = cadence.apply_silence_backoff(
        receipt("old", NOW - timedelta(minutes=1)), policy=POLICY
    )
    assert out_of_order["status"] == "out_of_order"
    assert cadence.snapshot()["silence_backoff"]["streak"] == 1


def test_backoff_expiry_is_based_on_receipt_time(tmp_path):
    current = [NOW + timedelta(minutes=30)]
    cadence = HeartbeatCadence(tmp_path, clock=lambda: current[0])
    result = cadence.apply_silence_backoff(receipt(completed_at=NOW), policy=POLICY)
    assert result["status"] == "expired"
    assert result["applied"] is False
    assert result["cooldown_until"] is None


def test_contacts_clear_automatic_streak_but_not_manual(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.apply_silence_backoff(receipt(), policy=POLICY)
    cadence.snooze(30, manual=True)
    assert cadence.record_private_contact(
        source_id="contact-1", observed_at=NOW, source_kind="private_inbound"
    )
    snapshot = cadence.snapshot()
    assert snapshot["automatic_cooldown_until"] is None
    assert snapshot["manual_cooldown_until"] is not None
    assert snapshot["silence_backoff"]["streak"] == 0

    cadence.observe_private_reply(NOW + timedelta(minutes=1))
    snapshot = cadence.snapshot()
    assert snapshot["manual_cooldown_until"] is not None
    assert snapshot["last_private_contact_at"] == "2026-08-22T19:01:00Z"


def test_contact_watermark_survives_map_prune(tmp_path):
    current = [NOW]
    cadence = HeartbeatCadence(tmp_path, clock=lambda: current[0])
    current[0] = NOW + timedelta(minutes=20)
    assert cadence.record_private_contact(
        source_id="later-contact",
        observed_at=NOW + timedelta(minutes=20),
        source_kind="private_inbound",
    )
    current[0] = NOW + timedelta(minutes=51)
    assert cadence.recent_contact() == (None, None)
    assert cadence.snapshot()["private_contact_sources"] == []
    blocked = cadence.apply_silence_backoff(
        receipt("delayed", NOW + timedelta(minutes=10)), policy=POLICY
    )
    assert blocked["status"] == "contact_after_receipt"


def test_default_off_runtime_seam_does_not_write(tmp_path):
    runtime = MoonbiteRuntime({"state": {"directory": str(tmp_path)}})
    result = runtime.record_heartbeat_silence(receipt())
    assert result["status"] == "disabled"
    assert list(tmp_path.iterdir()) == []


def test_v1_state_migrates_on_next_write(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    cadence.path.parent.mkdir(parents=True, exist_ok=True)
    cadence.path.write_text(
        json.dumps(
            {
                "schema_version": CADENCE_SCHEMA_V1,
                "auto_until": None,
                "manual_until": None,
                "private_contacts": {},
                "verified_visible_contacts": {},
            }
        ),
        encoding="utf-8",
    )
    assert cadence.snapshot()["schema_version"] == CADENCE_SCHEMA_V4
    cadence.apply_silence_backoff(receipt(), policy=POLICY)
    assert (
        json.loads(cadence.path.read_text(encoding="utf-8"))["schema_version"]
        == CADENCE_SCHEMA_V4
    )


def test_unknown_policy_and_malformed_state_fail_closed(tmp_path):
    cadence = HeartbeatCadence(tmp_path, clock=lambda: NOW)
    with pytest.raises(ValueError):
        cadence.apply_silence_backoff(receipt(), policy={**POLICY, "extra": True})
    cadence.path.parent.mkdir(parents=True, exist_ok=True)
    cadence.path.write_text(
        json.dumps(
            {
                "schema_version": "moon.heartbeat.cadence.v3",
                "silence_backoff_streak": 999,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(StateError):
        cadence.snapshot()
