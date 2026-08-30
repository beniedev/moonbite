from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

import moonbite_plugin.scenarios as scenarios
from moonbite_plugin.config import ConfigError, normalize_config
from moonbite_plugin.plugin import build_runtime
from moonbite_plugin.scenarios import (
    ScenarioPackError,
    load_catalog,
    redact_config,
    resolve_config,
)


def _resource_fixture(monkeypatch, tmp_path: Path, *, overlay: dict, name="fixture"):
    (tmp_path / "index.json").write_text(
        json.dumps(
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {name: f"{name}.json"},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / f"{name}.json").write_text(
        json.dumps(
            {
                "schema_version": "moon.scenario-pack.v1",
                "name": name,
                "overlay": overlay,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(scenarios.resources, "files", lambda _package: tmp_path)


def test_no_pack_is_exact_normalizer_parity_and_does_not_read_resources(monkeypatch):
    raw = {
        "modules": {"panel": True},
        "memory": {"search_limit": 12},
    }
    original = deepcopy(raw)

    def fail_if_read(_package):
        raise AssertionError("no-pack must not load scenario resources")

    monkeypatch.setattr(scenarios.resources, "files", fail_if_read)
    resolution = resolve_config(raw)

    assert resolution.selected_pack is None
    assert resolution.effective_config == normalize_config(raw)
    assert raw == original
    assert resolution.provenance["/modules/panel"] == "user"
    assert resolution.provenance["/timezone"] == "default"


def test_pack_user_merge_provenance_and_redaction(monkeypatch, tmp_path):
    overlay = {
        "modules": {"heartbeat": True},
        "heartbeat": {
            "default_snooze_minutes": 60,
            "kinds": {"custom": {"enabled": True}},
        },
        "autonomy": {"providers": {"pack_provider": {"enabled": True}}},
    }
    _resource_fixture(monkeypatch, tmp_path, overlay=overlay)
    raw = {
        "modules": {"heartbeat": False},
        "heartbeat": {"default_snooze_minutes": 90},
        "state": {"directory": "/private/state"},
        "delivery": {"target": "private-target"},
        "model_routes": {
            "schema_version": "moon.model_route_bindings.v1",
            "main": {"alias": "private_main"},
            "heartbeat": {"alias": "private_heartbeat"},
            "hippocampus": {"alias": "private_hippocampus"},
        },
    }
    original = deepcopy(raw)

    resolution = resolve_config(raw, "fixture")

    assert resolution.effective_config["modules"]["heartbeat"] is False
    assert resolution.effective_config["heartbeat"]["default_snooze_minutes"] == 90
    assert resolution.effective_config["autonomy"]["providers"]["pack_provider"] == {
        "enabled": True,
        "weight": 1,
    }
    assert resolution.provenance["/modules/heartbeat"] == "user"
    assert resolution.provenance["/heartbeat/default_snooze_minutes"] == "user"
    assert resolution.provenance["/heartbeat/kinds/custom/enabled"] == "pack:fixture"
    assert resolution.provenance["/heartbeat/kinds/custom/profile"] == "default"
    assert raw == original
    safe = resolution.redacted_config
    assert safe["timezone"] == "<redacted>"
    assert safe["state"]["directory"] == "<redacted>"
    assert safe["delivery"]["target"] == "<redacted>"
    assert safe["model_routes"]["main"]["alias"] == "<redacted>"


def test_empty_mapping_replaces_pack_map_and_lists_are_literals(monkeypatch, tmp_path):
    _resource_fixture(
        monkeypatch,
        tmp_path,
        overlay={
            "autonomy": {
                "providers": {
                    "pack_provider": {"enabled": True},
                }
            },
            "heartbeat": {"kinds": {"custom": {"bypass": []}}},
        },
    )
    resolution = resolve_config(
        {
            "autonomy": {"providers": {}},
            "heartbeat": {
                "kinds": {
                    "custom": {
                        "profile": "urgent",
                        "bypass": ["active_chat"],
                    }
                }
            },
            "delivery": {"target": None},
        },
        "fixture",
    )

    assert "pack_provider" not in resolution.effective_config["autonomy"]["providers"]
    assert resolution.effective_config["heartbeat"]["kinds"]["custom"]["bypass"] == [
        "active_chat"
    ]
    assert resolution.provenance["/autonomy/providers"] == "user"
    assert resolution.provenance["/heartbeat/kinds/custom/bypass"] == "user"
    assert resolution.effective_config["delivery"]["target"] is None


@pytest.mark.parametrize(
    ("selector", "code"),
    [("../fixture", "invalid_selector"), ("unknown", "unknown_pack")],
)
def test_selector_validation_is_fail_closed(monkeypatch, tmp_path, selector, code):
    _resource_fixture(monkeypatch, tmp_path, overlay={})
    with pytest.raises(ScenarioPackError) as raised:
        resolve_config({}, selector)
    assert raised.value.code == code
    assert "/" not in str(raised.value).split(" at ", 1)[-1].removeprefix("/")


def test_forbidden_pack_root_and_merge_conflict_are_structured(monkeypatch, tmp_path):
    _resource_fixture(monkeypatch, tmp_path, overlay={"timezone": "UTC"})
    with pytest.raises(ScenarioPackError) as raised:
        resolve_config({}, "fixture")
    assert raised.value.code == "forbidden_pack_field"
    assert raised.value.path == "/timezone"

    _resource_fixture(monkeypatch, tmp_path, overlay={"panel": {"anchor_hour": 4}})
    with pytest.raises(ScenarioPackError) as raised:
        resolve_config({"panel": None}, "fixture")
    assert raised.value.code == "merge_type_conflict"
    assert raised.value.path == "/panel"


@pytest.mark.parametrize(
    ("index_payload", "pack_payload", "code", "path"),
    [
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            None,
            "missing_resource",
            "/packs/fixture",
        ),
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            "{not-json",
            "malformed_json",
            "/packs/fixture",
        ),
        (
            {
                "schema_version": "unsupported.index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            None,
            "unsupported_index_schema",
            "/schema_version",
        ),
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            {
                "schema_version": "unsupported.pack.v1",
                "name": "fixture",
                "overlay": {},
            },
            "unsupported_pack_schema",
            "/schema_version",
        ),
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            {
                "schema_version": "moon.scenario-pack.v1",
                "name": "other",
                "overlay": {},
            },
            "pack_name_mismatch",
            "/name",
        ),
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            {
                "schema_version": "moon.scenario-pack.v1",
                "name": "fixture",
                "overlay": {"unknown": {}},
            },
            "unknown_pack_field",
            "/unknown",
        ),
        (
            {
                "schema_version": "moon.scenario-pack-index.v1",
                "packs": {"fixture": "fixture.json"},
            },
            {
                "schema_version": "moon.scenario-pack.v1",
                "name": "fixture",
                "overlay": {"modules": {"runtime_core": True}},
            },
            "forbidden_pack_field",
            "/modules/runtime_core",
        ),
    ],
    ids=(
        "missing-resource",
        "malformed-json",
        "unsupported-index-schema",
        "unsupported-pack-schema",
        "pack-name-mismatch",
        "unknown-root",
        "forbidden-runtime-core",
    ),
)
def test_pack_loader_failure_matrix(
    monkeypatch,
    tmp_path,
    index_payload,
    pack_payload,
    code,
    path,
):
    (tmp_path / "index.json").write_text(json.dumps(index_payload), encoding="utf-8")
    if pack_payload is not None:
        if isinstance(pack_payload, str):
            pack_text = pack_payload
        else:
            pack_text = json.dumps(pack_payload)
        (tmp_path / "fixture.json").write_text(pack_text, encoding="utf-8")
    monkeypatch.setattr(scenarios.resources, "files", lambda _package: tmp_path)

    with pytest.raises(ScenarioPackError) as raised:
        resolve_config({}, "fixture")

    error = raised.value
    assert error.code == code
    assert error.path == path
    assert str(tmp_path) not in str(error)
    assert str(tmp_path) not in error.args[0]


def test_empty_production_catalog_is_a_valid_pack_resource():
    assert load_catalog() == {}
    with pytest.raises(ScenarioPackError, match="unknown_pack"):
        resolve_config({}, "companion")


def test_redaction_preserves_null_and_does_not_mutate_input():
    config = {
        "timezone": "UTC",
        "state": {"directory": None},
        "delivery": {"target": ""},
        "model_routes": None,
    }
    original = deepcopy(config)
    safe = redact_config(config)
    assert safe["timezone"] == "<redacted>"
    assert safe["state"]["directory"] is None
    assert safe["delivery"]["target"] == "<redacted>"
    assert config == original


def test_no_pack_invalid_input_keeps_config_error_message():
    with pytest.raises(ConfigError, match="config must be a mapping"):
        resolve_config("not-a-mapping")


def test_build_runtime_reads_sibling_selector_once(monkeypatch, tmp_path):
    _resource_fixture(
        monkeypatch,
        tmp_path,
        overlay={"modules": {"panel": True}},
    )

    calls = {"config": 0, "scenario_pack": 0}

    class Context:
        llm = object()

        def get_config(self, key, default=None):
            calls[key] += 1
            values = {
                "config": {
                    "state": {"directory": str(tmp_path / "state")},
                },
                "scenario_pack": "fixture",
            }
            return values.get(key, default)

    runtime = build_runtime(Context())
    assert runtime.resolution.selected_pack == "fixture"
    assert runtime.config["modules"]["panel"] is True
    assert calls == {"config": 1, "scenario_pack": 1}
