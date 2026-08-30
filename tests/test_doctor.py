from __future__ import annotations

from datetime import UTC, date, datetime

from moonbite_plugin.doctor import doctor_report
from moonbite_plugin.observer import Observer
from moonbite_plugin.service import MoonbiteRuntime


def test_doctor_is_side_effect_free_by_contract():
    report = doctor_report({})
    assert report["ok"] is True
    assert report["plugin_loaded"] is False
    assert report["config_valid"] is True
    assert report["network_probe"] == "not_performed"
    assert report["writes_performed"] is False
    assert report["enabled_modules"] == ["runtime_core"]
    assert report["scheduler"] == "host_owned"
    assert report["scheduler_configured"] == "unknown"
    assert report["gateway_injection"] == {
        "required": False,
        "configured": "unknown",
    }
    assert report["state_root_available"] == "not_probed"


def test_doctor_reports_invalid_config_without_claiming_success():
    report = doctor_report({"telemetry": "yes"})
    assert report["ok"] is False
    assert report["plugin_loaded"] is False
    assert report["config_valid"] is False
    assert report["error"] == "ConfigError"
    assert report["code"] == "invalid_config"
    assert report["message"] == "Moonbite configuration is invalid."
    assert "plugins.entries.moonbite.settings.config" in report["remediation"]
    assert report["network_probe"] == "not_performed"
    assert report["writes_performed"] is False


def test_doctor_requires_gateway_injection_for_hermes_session_delivery():
    report = doctor_report(
        {"delivery": {"adapter": "hermes_session", "target": "agent:test"}}
    )

    assert report["ok"] is True
    assert report["gateway_injection"] == {
        "required": True,
        "configured": "unknown",
    }


def test_doctor_rejects_hermes_session_delivery_without_target():
    report = doctor_report({"delivery": {"adapter": "hermes_session"}})

    assert report["ok"] is False
    assert report["code"] == "invalid_config"


def test_doctor_without_runtime_reports_neutral_unavailable_health():
    report = doctor_report({"state": {"directory": "/private/fixture"}})

    assert report["state_root"] == "private"
    assert report["health"]["available"] is False
    assert report["health"]["state"] == "neutral"
    assert report["telemetry"] == "moonbite_observer"
    assert report["telemetry_available"] is False


def test_doctor_accepts_health_snapshot_without_constructing_runtime():
    snapshot = Observer().snapshot(
        date(2026, 8, 24), datetime(2026, 8, 24, 12, tzinfo=UTC)
    )

    report = doctor_report(snapshot)

    assert report["health"]["available"] is True
    assert report["health"]["schema_version"] == "moon.observer.v1"


def test_doctor_runtime_health_failure_is_degraded_current():
    class BrokenRuntime:
        config = {}

        def health_snapshot(self):
            raise RuntimeError("private fixture")

    report = doctor_report(BrokenRuntime())

    assert report["health"]["available"] is False
    assert report["health"]["state"] == "current"
    assert report["health"]["degraded"] is True
    assert report["health"]["status"] == "degraded"
    assert "private fixture" not in repr(report)


def test_doctor_runtime_uses_existing_runtime_health(tmp_path):
    runtime = MoonbiteRuntime({}, root=tmp_path)
    report = doctor_report(
        runtime,
        target_date=date(2026, 8, 24),
        now=datetime(2026, 8, 24, 12, tzinfo=UTC),
    )

    assert report["health"]["available"] is True
    assert report["plugin_loaded"] is True
    assert report["health"]["target_date"] == "2026-08-24"
    assert not (tmp_path / "controls.jsonl").exists()


def test_doctor_exposes_redacted_runtime_resolution(tmp_path):
    runtime = MoonbiteRuntime(
        {
            "timezone": "America/Los_Angeles",
            "state": {"directory": "/private/state"},
            "delivery": {"target": "private-target"},
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "private_main"},
                "heartbeat": {"alias": "private_heartbeat"},
                "hippocampus": {"alias": "private_memory"},
            },
        },
        root=tmp_path,
    )

    report = doctor_report(runtime)

    assert report["scenario_pack"] is None
    effective = report["resolution"]["effective_config"]
    assert effective["timezone"] == "<redacted>"
    assert effective["state"]["directory"] == "<redacted>"
    assert effective["delivery"]["target"] == "<redacted>"
    assert effective["model_routes"]["main"]["alias"] == "<redacted>"
    assert report["model_routes"]["roles"]["main"] == "<redacted>"
