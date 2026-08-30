"""Scenario-pack loading, merging, provenance, and safe inspection."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from importlib import resources
from typing import Any

from .config import ConfigError, normalize_config


SCENARIO_PACK_RESOURCE_PACKAGE = "moonbite_plugin.scenario_packs"
SCENARIO_PACK_INDEX_RESOURCE = "index.json"
_INDEX_SCHEMA = "moon.scenario-pack-index.v1"
_PACK_SCHEMA = "moon.scenario-pack.v1"
_SELECTOR = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*\.json$")
_PACK_ROOTS = frozenset({"modules", "heartbeat", "autonomy", "panel", "memory"})
_PACK_MODULES = frozenset({"heartbeat", "autonomy", "panel", "memory"})
_FORBIDDEN_ROOTS = frozenset(
    {"config_version", "timezone", "state", "delivery", "model_routes"}
)
_MISSING = object()


def _pointer(parts: tuple[Any, ...]) -> str:
    if not parts:
        return "/"
    return "/" + "/".join(
        str(part).replace("~", "~0").replace("/", "~1") for part in parts
    )


class ScenarioPackError(ConfigError):
    """A safe, structured scenario-pack or merge failure."""

    def __init__(self, code: str, path: str = "/", message: str | None = None):
        self.code = code
        self.path = path if path.startswith("/") else f"/{path}"
        self.reason = message
        detail = "" if message is None else f": {message}"
        super().__init__(f"scenario pack {code} at {self.path}{detail}")


@dataclass(frozen=True, slots=True)
class ConfigResolution:
    """The one validated configuration result shared by runtime consumers."""

    selected_pack: str | None
    effective_config: dict[str, Any]
    provenance: dict[str, str]

    @property
    def redacted_config(self) -> dict[str, Any]:
        return redact_config(self.effective_config)

    def to_dict(self, *, redacted: bool = True) -> dict[str, Any]:
        return {
            "selected_pack": self.selected_pack,
            "effective_config": self.redacted_config
            if redacted
            else deepcopy(self.effective_config),
            "provenance": dict(self.provenance),
        }


def _validate_selector(selected_pack: Any) -> str:
    if type(selected_pack) is not str or _SELECTOR.fullmatch(selected_pack) is None:
        raise ScenarioPackError("invalid_selector", "/scenario_pack")
    return selected_pack


def _resource_root(package: str | None = None) -> Any:
    try:
        return resources.files(
            SCENARIO_PACK_RESOURCE_PACKAGE if package is None else package
        )
    except Exception as exc:  # noqa: BLE001 - package boundary
        raise ScenarioPackError("missing_resource", "/") from exc


def _read_json(root: Any, resource_name: str, *, code: str, path: str) -> Any:
    try:
        text = root.joinpath(resource_name).read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ScenarioPackError("missing_resource", path) from exc
    except Exception as exc:  # noqa: BLE001 - package resource boundary
        raise ScenarioPackError("resource_read_failed", path) from exc
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ScenarioPackError(code, path) from exc


def _resource_name(value: Any, path: str) -> str:
    if (
        type(value) is not str
        or "/" in value
        or "\\" in value
        or value in {"", ".", ".."}
        or _RESOURCE_NAME.fullmatch(value) is None
    ):
        raise ScenarioPackError("invalid_resource_name", path)
    return value


def _catalog(package: str | None = None) -> tuple[Any, dict[str, str]]:
    root = _resource_root(package)
    value = _read_json(
        root,
        SCENARIO_PACK_INDEX_RESOURCE,
        code="malformed_index",
        path="/",
    )
    if not isinstance(value, Mapping):
        raise ScenarioPackError("unsupported_index_schema", "/")
    if set(value) != {"schema_version", "packs"}:
        raise ScenarioPackError("unsupported_index_schema", "/")
    if value.get("schema_version") != _INDEX_SCHEMA:
        raise ScenarioPackError("unsupported_index_schema", "/schema_version")
    packs = value.get("packs")
    if not isinstance(packs, Mapping):
        raise ScenarioPackError("malformed_index", "/packs")
    catalog: dict[str, str] = {}
    for name, resource_name in packs.items():
        if type(name) is not str or _SELECTOR.fullmatch(name) is None:
            raise ScenarioPackError("invalid_selector", _pointer(("packs", name)))
        catalog[name] = _resource_name(resource_name, _pointer(("packs", name)))
    return root, catalog


def load_catalog(package: str | None = None) -> dict[str, str]:
    """Load the fixed public selector-to-resource catalog."""

    _root, catalog = _catalog(package)
    return catalog


def _validate_pack_overlay(overlay: Mapping[str, Any]) -> None:
    for key in overlay:
        path = _pointer((key,))
        if key not in _PACK_ROOTS:
            code = (
                "forbidden_pack_field"
                if key in _FORBIDDEN_ROOTS
                else "unknown_pack_field"
            )
            raise ScenarioPackError(code, path)
    modules = overlay.get("modules", _MISSING)
    if isinstance(modules, Mapping):
        for key in modules:
            if key not in _PACK_MODULES:
                code = (
                    "forbidden_pack_field"
                    if key == "runtime_core"
                    else "unknown_pack_field"
                )
                raise ScenarioPackError(code, _pointer(("modules", key)))


def load_pack(selected_pack: str, package: str | None = None) -> dict[str, Any]:
    """Load and validate one catalog-selected pack overlay."""

    selector = _validate_selector(selected_pack)
    root, catalog = _catalog(package)
    resource_name = catalog.get(selector)
    if resource_name is None:
        raise ScenarioPackError("unknown_pack", "/scenario_pack")
    value = _read_json(
        root,
        resource_name,
        code="malformed_json",
        path=_pointer(("packs", selector)),
    )
    if not isinstance(value, Mapping):
        raise ScenarioPackError("unsupported_pack_schema", "/")
    if set(value) != {"schema_version", "name", "overlay"}:
        raise ScenarioPackError("unsupported_pack_schema", "/")
    if value.get("schema_version") != _PACK_SCHEMA:
        raise ScenarioPackError("unsupported_pack_schema", "/schema_version")
    if value.get("name") != selector:
        raise ScenarioPackError("pack_name_mismatch", "/name")
    overlay = value.get("overlay")
    if not isinstance(overlay, Mapping):
        raise ScenarioPackError("malformed_pack", "/overlay")
    _validate_pack_overlay(overlay)
    return deepcopy(dict(overlay))


def _collect_explicit(
    value: Any,
    source: str,
    path: tuple[Any, ...],
    explicit: dict[tuple[Any, ...], str],
) -> None:
    if isinstance(value, Mapping):
        if not value:
            # The root object is structural, not a user-facing leaf.
            if path:
                explicit[path] = source
            return
        for key, child in value.items():
            _collect_explicit(child, source, (*path, key), explicit)
        return
    explicit[path] = source


def _merge_values(
    pack_value: Any,
    user_value: Any,
    path: tuple[Any, ...],
    explicit: dict[tuple[Any, ...], str],
    pack_source: str,
) -> Any:
    pack_mapping = isinstance(pack_value, Mapping)
    user_mapping = isinstance(user_value, Mapping)
    if pack_mapping != user_mapping:
        raise ScenarioPackError("merge_type_conflict", _pointer(path))
    if pack_mapping:
        if not user_value:
            if path:
                explicit[path] = "user"
                return {}
            # The top-level user object is structural; an omitted/empty user
            # config must leave the selected pack overlay intact.
            result = deepcopy(dict(pack_value))
            _collect_explicit(result, pack_source, path, explicit)
            return result
        result: dict[Any, Any] = {}
        keys = list(pack_value)
        keys.extend(key for key in user_value if key not in pack_value)
        for key in keys:
            in_pack = key in pack_value
            in_user = key in user_value
            child_path = (*path, key)
            if in_pack and in_user:
                result[key] = _merge_values(
                    pack_value[key],
                    user_value[key],
                    child_path,
                    explicit,
                    pack_source,
                )
            elif in_user:
                result[key] = deepcopy(user_value[key])
                _collect_explicit(user_value[key], "user", child_path, explicit)
            else:
                result[key] = deepcopy(pack_value[key])
                _collect_explicit(pack_value[key], pack_source, child_path, explicit)
        return result
    # Both scalar/list/null values are literals; user wins on collision.
    result = deepcopy(user_value)
    _collect_explicit(result, "user", path, explicit)
    return result


def _provenance(
    value: Any,
    path: tuple[Any, ...],
    explicit: Mapping[tuple[Any, ...], str],
    result: dict[str, str],
) -> None:
    source = explicit.get(path)
    if source is not None:
        result[_pointer(path)] = source
        # Explicit empty maps are intentional replacement leaves, even though
        # normalize_config may fill them with inert defaults.
        return
    if isinstance(value, Mapping):
        if not value:
            result[_pointer(path)] = "default"
            return
        for key, child in value.items():
            _provenance(child, (*path, key), explicit, result)
        return
    result[_pointer(path)] = "default"


def _normalized_provenance(
    raw: Any,
    effective: dict[str, Any],
    *,
    explicit: Mapping[tuple[Any, ...], str] | None = None,
) -> dict[str, str]:
    if explicit is None:
        explicit_map: dict[tuple[Any, ...], str] = {}
        if raw is not None:
            _collect_explicit(raw, "user", (), explicit_map)
    else:
        explicit_map = dict(explicit)
    result: dict[str, str] = {}
    _provenance(effective, (), explicit_map, result)
    return result


def resolve_config(
    raw_config: Any, selected_pack: str | None = None
) -> ConfigResolution:
    """Resolve one optional pack and user config through the existing validator."""

    if selected_pack is None:
        effective = normalize_config(raw_config)
        return ConfigResolution(
            selected_pack=None,
            effective_config=effective,
            provenance=_normalized_provenance(raw_config, effective),
        )

    selector = _validate_selector(selected_pack)
    if raw_config is not None and not isinstance(raw_config, Mapping):
        # Keep the legacy no-pack error wording for the same invalid input.
        raise ConfigError("config must be a mapping")
    user = {} if raw_config is None else raw_config
    overlay = load_pack(selector)
    explicit: dict[tuple[Any, ...], str] = {}
    merged = _merge_values(
        overlay,
        user,
        (),
        explicit,
        f"pack:{selector}",
    )
    effective = normalize_config(merged)
    return ConfigResolution(
        selected_pack=selector,
        effective_config=effective,
        provenance=_normalized_provenance(
            merged,
            effective,
            explicit=explicit,
        ),
    )


def redact_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Return a safe inspection copy with deployment-owned values redacted."""

    result = deepcopy(dict(config))

    timezone = result.get("timezone")
    if timezone is not None:
        result["timezone"] = "<redacted>"

    state = result.get("state")
    if isinstance(state, Mapping):
        state = dict(state)
        if state.get("directory") is not None:
            state["directory"] = "<redacted>"
        result["state"] = state

    delivery = result.get("delivery")
    if isinstance(delivery, Mapping):
        delivery = dict(delivery)
        if delivery.get("target") is not None:
            delivery["target"] = "<redacted>"
        result["delivery"] = delivery

    routes = result.get("model_routes")
    if isinstance(routes, Mapping):
        routes = dict(routes)
        for role, route in list(routes.items()):
            if not isinstance(route, Mapping):
                continue
            route = dict(route)
            if route.get("alias") is not None:
                route["alias"] = "<redacted>"
            routes[role] = route
        result["model_routes"] = routes
    return result


__all__ = [
    "ConfigResolution",
    "ScenarioPackError",
    "load_catalog",
    "load_pack",
    "redact_config",
    "resolve_config",
]
