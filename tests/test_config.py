from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from moonbite_plugin.config import (
    DEFAULT_CONFIG,
    ConfigError,
    normalize_config,
    route_bindings,
    validate_known_aliases,
)


def test_defaults_are_inert_and_private_path_free():
    config = normalize_config({})
    assert config["modules"] == {
        "runtime_core": True,
        "heartbeat": False,
        "autonomy": False,
        "panel": False,
        "memory": False,
    }
    assert config["delivery"] == {"adapter": "noop", "target": None}
    assert config["model_routes"] is None
    assert config["autonomy"]["providers"]["paper_browse"] == {
        "enabled": False,
        "weight": 3,
    }
    assert config["autonomy"]["providers"]["x_browse"] == {
        "enabled": False,
        "weight": 2,
    }
    assert config["memory"] == {
        "search_limit": 8,
        "recall_enabled": False,
        "recall_limit": 4,
        "resurfacing_enabled": False,
        "resurfacing_limit": 1,
        "resurfacing_ttl_minutes": 180,
        "resurfacing_cooldown_minutes": 1440,
        "maintenance_enabled": False,
    }


def test_defaults_include_only_inert_heartbeat_kinds():
    kinds = normalize_config({})["heartbeat"]["kinds"]
    assert kinds == {
        "day_close": {
            "enabled": False,
            "profile": "daily_anchor",
            "judge": "required",
            "host_only": True,
            "bypass": [],
        },
        "day_open": {
            "enabled": False,
            "profile": "daily_anchor",
            "judge": "required",
            "host_only": True,
            "bypass": [],
        },
        "urgent_care": {
            "enabled": False,
            "profile": "urgent",
            "judge": "required",
            "host_only": True,
            "bypass": [],
        },
    }
    assert all(not descriptor["enabled"] for descriptor in kinds.values())
    assert "emotional_repair" not in kinds
    assert "critical_ops" not in kinds


def test_custom_heartbeat_kind_is_normalized_with_safe_defaults():
    config = normalize_config(
        {"heartbeat": {"kinds": {"custom.kind-1": {"enabled": True}}}}
    )
    assert config["heartbeat"]["kinds"] == {
        "custom.kind-1": {
            "enabled": True,
            "profile": "routine",
            "judge": "required",
            "host_only": True,
            "bypass": [],
        }
    }


@pytest.mark.parametrize(
    "profile,bypass,judge,host_only",
    [
        ("routine", [], "required", False),
        ("routine", [], "required", True),
        ("daily_anchor", [], "required", True),
        ("daily_anchor", ["automatic_cooldown"], "required", True),
        ("daily_anchor", ["manual_snooze"], "required", True),
        (
            "daily_anchor",
            ["automatic_cooldown", "manual_snooze"],
            "required",
            True,
        ),
        ("urgent", [], "required", True),
        ("urgent", ["automatic_cooldown"], "required", True),
        ("urgent", ["manual_snooze"], "required", True),
        ("urgent", ["recent_contact"], "required", True),
        ("urgent", ["active_chat"], "required", True),
        ("urgent", ["recent_contact", "active_chat"], "required", True),
        (
            "urgent",
            [
                "automatic_cooldown",
                "manual_snooze",
                "recent_contact",
                "active_chat",
            ],
            "required",
            True,
        ),
        ("maintenance", [], "required", True),
        ("maintenance", [], "skip", True),
    ],
)
def test_heartbeat_kind_profile_boundaries_are_accepted(
    profile, bypass, judge, host_only
):
    raw = {
        "heartbeat": {
            "kinds": {
                "custom_kind": {
                    "profile": profile,
                    "bypass": bypass,
                    "judge": judge,
                    "host_only": host_only,
                }
            }
        }
    }
    schema_path = Path(__file__).parents[1] / "config" / "schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert list(Draft202012Validator(schema).iter_errors(raw)) == []
    config = normalize_config(raw)
    assert config["heartbeat"]["kinds"]["custom_kind"] == {
        "enabled": False,
        "profile": profile,
        "judge": judge,
        "host_only": host_only,
        "bypass": bypass,
    }


@pytest.mark.parametrize(
    "descriptor,field",
    [
        ({"profile": "routine", "bypass": ["manual_snooze"]}, "routine"),
        ({"profile": "routine", "judge": "skip"}, "routine"),
        ({"profile": "daily_anchor", "bypass": ["active_chat"]}, "daily_anchor"),
        ({"profile": "daily_anchor", "host_only": False}, "daily_anchor"),
        ({"profile": "urgent", "judge": "skip"}, "urgent"),
        ({"profile": "urgent", "host_only": False}, "urgent"),
        (
            {"profile": "maintenance", "bypass": ["automatic_cooldown"]},
            "maintenance",
        ),
        ({"profile": "maintenance", "host_only": False}, "maintenance"),
    ],
)
def test_heartbeat_kind_illegal_profile_combinations_fail_closed(descriptor, field):
    with pytest.raises(ConfigError, match=field):
        normalize_config({"heartbeat": {"kinds": {"custom_kind": descriptor}}})


def test_heartbeat_kind_schema_default_matches_runtime_default():
    schema_path = Path(__file__).parents[1] / "config" / "schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema_default = schema["properties"]["heartbeat"]["properties"]["kinds"]["default"]
    assert schema_default == DEFAULT_CONFIG["heartbeat"]["kinds"]


def test_memory_accepts_declared_boundary_values():
    config = normalize_config(
        {
            "memory": {
                "search_limit": 1,
                "recall_enabled": True,
                "recall_limit": 20,
                "resurfacing_enabled": True,
                "resurfacing_limit": 8,
                "resurfacing_ttl_minutes": 1440,
                "resurfacing_cooldown_minutes": 10080,
                "maintenance_enabled": True,
            }
        }
    )
    assert config["memory"] == {
        "search_limit": 1,
        "recall_enabled": True,
        "recall_limit": 20,
        "resurfacing_enabled": True,
        "resurfacing_limit": 8,
        "resurfacing_ttl_minutes": 1440,
        "resurfacing_cooldown_minutes": 10080,
        "maintenance_enabled": True,
    }
    assert (
        normalize_config({"memory": {"search_limit": 100}})["memory"]["search_limit"]
        == 100
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("search_limit", 0),
        ("search_limit", 101),
        ("recall_enabled", 1),
        ("recall_limit", 0),
        ("recall_limit", 21),
        ("recall_limit", True),
        ("resurfacing_enabled", "false"),
        ("resurfacing_limit", 0),
        ("resurfacing_limit", 9),
        ("resurfacing_ttl_minutes", 0),
        ("resurfacing_ttl_minutes", 1441),
        ("resurfacing_cooldown_minutes", 0),
        ("resurfacing_cooldown_minutes", 10081),
        ("maintenance_enabled", "false"),
    ],
)
def test_memory_rejects_invalid_types_and_ranges(field, value):
    with pytest.raises(ConfigError, match=rf"memory\.{field}"):
        normalize_config({"memory": {field: value}})


def test_unknown_memory_key_fails_closed():
    with pytest.raises(ConfigError, match="unknown keys"):
        normalize_config({"memory": {"unknown": True}})


def test_three_roles_bind_to_host_aliases_and_can_share_one():
    config = normalize_config(
        {
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "primary"},
                "heartbeat": {"alias": "light"},
                "hippocampus": {"alias": "light"},
            }
        }
    )
    assert route_bindings(config).heartbeat == "light"
    validate_known_aliases(config, {"primary", "light"})


@pytest.mark.parametrize(
    "forbidden", ["provider", "model", "base_url", "fallback", "tier"]
)
def test_route_rejects_host_owned_details(forbidden):
    with pytest.raises(ConfigError, match="unknown keys"):
        normalize_config(
            {
                "model_routes": {
                    "schema_version": "moon.model_route_bindings.v1",
                    "main": {"alias": "primary", forbidden: "not-owned-here"},
                    "heartbeat": {"alias": "light"},
                    "hippocampus": {"alias": "light"},
                }
            }
        )


def test_host_alias_probe_fails_closed_when_registry_is_available():
    config = normalize_config(
        {
            "model_routes": {
                "schema_version": "moon.model_route_bindings.v1",
                "main": {"alias": "primary"},
                "heartbeat": {"alias": "light"},
                "hippocampus": {"alias": "light"},
            }
        }
    )
    with pytest.raises(ConfigError, match="unknown to the host: light"):
        validate_known_aliases(config, {"primary"})


def test_unknown_top_level_key_fails_closed():
    with pytest.raises(ConfigError, match="unknown keys"):
        normalize_config({"mystery_switch": True})


def test_boolean_is_not_accepted_as_config_version_one():
    with pytest.raises(ConfigError, match="config_version must be an integer"):
        normalize_config({"config_version": True})


def test_invalid_timezone_is_rejected():
    with pytest.raises(ConfigError, match="not available"):
        normalize_config({"timezone": "Nowhere/DefinitelyMissing"})


def test_portable_schema_tracks_runtime_top_level_keys():
    schema_path = Path(__file__).parents[1] / "config" / "schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert set(schema["properties"]) == set(DEFAULT_CONFIG)


def test_shared_config_corpus_has_runtime_and_schema_parity():
    root = Path(__file__).parents[1]
    schema = json.loads((root / "config" / "schema.json").read_text(encoding="utf-8"))
    cases = json.loads(
        (root / "tests" / "fixtures" / "config_cases.json").read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema)

    for case in cases["valid"]:
        assert list(validator.iter_errors(case["config"])) == [], case["id"]
        normalized = normalize_config(case["config"])
        assert list(validator.iter_errors(normalized)) == [], (
            case["id"],
            "normalized",
        )

    for case in cases["invalid"]:
        assert list(validator.iter_errors(case["config"])), case["id"]
        with pytest.raises(ConfigError):
            normalize_config(case["config"])


def test_presets_are_valid_and_externally_inert():
    root = Path(__file__).parents[1]
    preset_root = root / "config" / "presets"
    expected_modules = {
        "core-only": {"runtime_core"},
        "panel-only": {"runtime_core", "panel"},
        "memory-only": {"runtime_core", "memory"},
        "full-companion": {
            "runtime_core",
            "heartbeat",
            "autonomy",
            "panel",
            "memory",
        },
    }
    schema = json.loads((root / "config" / "schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    assert {path.stem for path in preset_root.glob("*.yaml")} == set(expected_modules)
    for name, modules in expected_modules.items():
        raw = yaml.safe_load((preset_root / f"{name}.yaml").read_text(encoding="utf-8"))
        assert list(validator.iter_errors(raw)) == [], name
        config = normalize_config(raw)
        assert {
            module for module, enabled in config["modules"].items() if enabled
        } == modules
        assert config["delivery"] == {"adapter": "noop", "target": None}
        assert config["model_routes"] is None
        assert not any(
            kind["enabled"] for kind in config["heartbeat"]["kinds"].values()
        )
        assert not any(
            provider["enabled"] for provider in config["autonomy"]["providers"].values()
        )
        assert config["memory"]["recall_enabled"] is False
        assert config["memory"]["resurfacing_enabled"] is False
        assert config["memory"]["maintenance_enabled"] is False


def test_install_examples_separate_settings_from_loader_state():
    root = Path(__file__).parents[1]
    settings = yaml.safe_load(
        (root / "config" / "example.yaml").read_text(encoding="utf-8")
    )
    effective = yaml.safe_load(
        (root / "config" / "effective-config.example.yaml").read_text(encoding="utf-8")
    )

    assert "enabled" not in settings["plugins"]
    assert effective["plugins"]["enabled"] == ["moonbite"]
    for example in (settings, effective):
        entry = example["plugins"]["entries"]["moonbite"]
        assert "enabled" not in entry
        normalize_config(entry["settings"]["config"])
