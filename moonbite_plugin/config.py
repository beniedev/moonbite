"""Versioned, strict, host-neutral Moonbite configuration."""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class ConfigError(ValueError):
    """Raised when Moonbite cannot safely interpret its configuration."""


_ROLE_NAMES = ("main", "heartbeat", "hippocampus")
_PROVIDER_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_AUXILIARY_ALIAS = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_HEARTBEAT_KIND_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_HEARTBEAT_PROFILES = frozenset({"routine", "daily_anchor", "urgent", "maintenance"})
_HEARTBEAT_JUDGES = frozenset({"required", "skip"})
_HEARTBEAT_BYPASSES = frozenset(
    {"automatic_cooldown", "manual_snooze", "recent_contact", "active_chat"}
)
_HEARTBEAT_KIND_DEFAULTS = {
    "enabled": False,
    "profile": "routine",
    "judge": "required",
    "host_only": True,
    "bypass": [],
}
_DEFAULT_HEARTBEAT_KINDS = {
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

DEFAULT_CONFIG: dict[str, Any] = {
    "config_version": 1,
    "modules": {
        "runtime_core": True,
        "heartbeat": False,
        "autonomy": False,
        "panel": False,
        "memory": False,
    },
    "timezone": "UTC",
    "state": {"directory": None},
    "delivery": {"adapter": "noop", "target": None},
    "model_routes": None,
    "heartbeat": {
        "default_snooze_minutes": 120,
        "silence_backoff": {
            "enabled": False,
            "first_minutes": 120,
            "repeat_minutes": 240,
            "max_minutes": 240,
        },
        "kinds": deepcopy(_DEFAULT_HEARTBEAT_KINDS),
    },
    "autonomy": {
        "providers": {
            "local_reflection": {"enabled": False, "weight": 1},
            "model_reflection": {"enabled": False, "weight": 1},
            "paper_browse": {"enabled": False, "weight": 3},
            "x_browse": {"enabled": False, "weight": 2},
        },
    },
    "panel": {"anchor_hour": 6, "activity_afterglow_minutes": 180},
    "memory": {
        "search_limit": 8,
        "recall_enabled": False,
        "recall_limit": 4,
        "resurfacing_enabled": False,
        "resurfacing_limit": 1,
        "resurfacing_ttl_minutes": 180,
        "resurfacing_cooldown_minutes": 1440,
        "maintenance_enabled": False,
    },
}


@dataclass(frozen=True)
class RouteBindings:
    main: str
    heartbeat: str
    hippocampus: str


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{path} must be a mapping")
    return value


def _reject_unknown(value: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(f"{path} has unknown keys: {', '.join(unknown)}")


def _bool(value: Any, path: str) -> bool:
    if type(value) is not bool:
        raise ConfigError(f"{path} must be a boolean")
    return value


def _int(value: Any, path: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ConfigError(f"{path} must be an integer from {minimum} to {maximum}")
    return value


def _merge_section(
    raw: Mapping[str, Any], default: Mapping[str, Any], path: str
) -> dict[str, Any]:
    _reject_unknown(raw, set(default), path)
    result = deepcopy(dict(default))
    result.update(raw)
    return result


def _normalize_routes(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    routes = _mapping(value, "model_routes")
    _reject_unknown(routes, {"schema_version", *_ROLE_NAMES}, "model_routes")
    if routes.get("schema_version") != "moon.model_route_bindings.v1":
        raise ConfigError(
            "model_routes.schema_version must be moon.model_route_bindings.v1"
        )
    missing = [role for role in _ROLE_NAMES if role not in routes]
    if missing:
        raise ConfigError(f"model_routes is missing roles: {', '.join(missing)}")
    normalized: dict[str, Any] = {"schema_version": "moon.model_route_bindings.v1"}
    for role in _ROLE_NAMES:
        route = _mapping(routes[role], f"model_routes.{role}")
        _reject_unknown(route, {"alias"}, f"model_routes.{role}")
        alias = route.get("alias")
        if not isinstance(alias, str) or not _AUXILIARY_ALIAS.fullmatch(alias.strip()):
            raise ConfigError(
                f"model_routes.{role}.alias must match [a-z][a-z0-9_]{{0,63}}"
            )
        normalized[role] = {"alias": alias.strip()}
    return normalized


def _normalize_providers(value: Any) -> dict[str, dict[str, Any]]:
    providers = _mapping(value, "autonomy.providers")
    normalized: dict[str, dict[str, Any]] = {}
    for name, settings_value in providers.items():
        if not isinstance(name, str) or not _PROVIDER_NAME.fullmatch(name):
            raise ConfigError(
                "autonomy provider names must match [a-z][a-z0-9_-]{0,63}"
            )
        settings = _mapping(settings_value, f"autonomy.providers.{name}")
        _reject_unknown(settings, {"enabled", "weight"}, f"autonomy.providers.{name}")
        normalized[name] = {
            "enabled": _bool(
                settings.get("enabled", False), f"autonomy.providers.{name}.enabled"
            ),
            "weight": _int(
                settings.get("weight", 1), f"autonomy.providers.{name}.weight", 1, 100
            ),
        }
    return normalized


def _normalize_heartbeat_kinds(value: Any) -> dict[str, dict[str, Any]]:
    kinds = _mapping(value, "heartbeat.kinds")
    normalized: dict[str, dict[str, Any]] = {}
    names = list(kinds)
    for name in names:
        if type(name) is not str or _HEARTBEAT_KIND_NAME.fullmatch(name) is None:
            raise ConfigError("heartbeat kind names must match [a-z][a-z0-9_.-]{0,63}")
    for name in sorted(names):
        path = f"heartbeat.kinds.{name}"
        descriptor = _mapping(kinds[name], path)
        _reject_unknown(
            descriptor,
            {"enabled", "profile", "judge", "host_only", "bypass"},
            path,
        )
        enabled = _bool(
            descriptor.get("enabled", _HEARTBEAT_KIND_DEFAULTS["enabled"]),
            f"{path}.enabled",
        )
        profile = descriptor.get("profile", _HEARTBEAT_KIND_DEFAULTS["profile"])
        if type(profile) is not str or profile not in _HEARTBEAT_PROFILES:
            raise ConfigError(
                f"{path}.profile must be one of: {', '.join(sorted(_HEARTBEAT_PROFILES))}"
            )
        judge = descriptor.get("judge", _HEARTBEAT_KIND_DEFAULTS["judge"])
        if type(judge) is not str or judge not in _HEARTBEAT_JUDGES:
            raise ConfigError(f"{path}.judge must be required or skip")
        host_only = _bool(
            descriptor.get("host_only", _HEARTBEAT_KIND_DEFAULTS["host_only"]),
            f"{path}.host_only",
        )
        bypass = descriptor.get("bypass", _HEARTBEAT_KIND_DEFAULTS["bypass"])
        if type(bypass) is not list:
            raise ConfigError(f"{path}.bypass must be a list")
        if any(type(item) is not str for item in bypass):
            raise ConfigError(f"{path}.bypass must contain only strings")
        if len(set(bypass)) != len(bypass):
            raise ConfigError(f"{path}.bypass must contain unique values")
        unknown_bypass = sorted(set(bypass) - _HEARTBEAT_BYPASSES)
        if unknown_bypass:
            raise ConfigError(
                f"{path}.bypass has unknown values: {', '.join(unknown_bypass)}"
            )

        if profile == "routine":
            if bypass:
                raise ConfigError(f"{path}.routine cannot declare bypass")
            if judge != "required":
                raise ConfigError(f"{path}.routine requires judge=required")
        elif profile == "daily_anchor":
            if any(
                item not in {"automatic_cooldown", "manual_snooze"}
                for item in bypass
            ):
                raise ConfigError(
                    f"{path}.daily_anchor only allows automatic_cooldown and manual_snooze bypass"
                )
            if judge != "required":
                raise ConfigError(f"{path}.daily_anchor requires judge=required")
            if host_only is not True:
                raise ConfigError(f"{path}.daily_anchor requires host_only=true")
        elif profile == "urgent":
            if judge != "required":
                raise ConfigError(f"{path}.urgent requires judge=required")
            if host_only is not True:
                raise ConfigError(f"{path}.urgent requires host_only=true")
        elif profile == "maintenance":
            if bypass:
                raise ConfigError(f"{path}.maintenance cannot declare bypass")
            if host_only is not True:
                raise ConfigError(f"{path}.maintenance requires host_only=true")

        normalized[name] = {
            "enabled": enabled,
            "profile": profile,
            "judge": judge,
            "host_only": host_only,
            "bypass": list(bypass),
        }
    return normalized


def _normalize_silence_backoff(value: Any) -> dict[str, Any]:
    policy = _merge_section(
        _mapping(value, "heartbeat.silence_backoff"),
        DEFAULT_CONFIG["heartbeat"]["silence_backoff"],
        "heartbeat.silence_backoff",
    )
    normalized = {
        "enabled": _bool(policy["enabled"], "heartbeat.silence_backoff.enabled"),
        "first_minutes": _int(
            policy["first_minutes"],
            "heartbeat.silence_backoff.first_minutes",
            1,
            1440,
        ),
        "repeat_minutes": _int(
            policy["repeat_minutes"],
            "heartbeat.silence_backoff.repeat_minutes",
            1,
            1440,
        ),
        "max_minutes": _int(
            policy["max_minutes"],
            "heartbeat.silence_backoff.max_minutes",
            1,
            1440,
        ),
    }
    if normalized["first_minutes"] > normalized["repeat_minutes"]:
        raise ConfigError(
            "heartbeat.silence_backoff.first_minutes must be <= repeat_minutes"
        )
    if normalized["repeat_minutes"] > normalized["max_minutes"]:
        raise ConfigError(
            "heartbeat.silence_backoff.repeat_minutes must be <= max_minutes"
        )
    return normalized


def normalize_config(raw: Any) -> dict[str, Any]:
    source = _mapping({} if raw is None else raw, "config")
    _reject_unknown(source, set(DEFAULT_CONFIG), "config")
    _int(source.get("config_version", 1), "config.config_version", 1, 1)
    result = deepcopy(DEFAULT_CONFIG)

    modules = _merge_section(
        _mapping(source.get("modules", {}), "modules"),
        DEFAULT_CONFIG["modules"],
        "modules",
    )
    result["modules"] = {
        key: _bool(value, f"modules.{key}") for key, value in modules.items()
    }
    if result["modules"]["runtime_core"] is not True:
        raise ConfigError("modules.runtime_core cannot be disabled")

    timezone = source.get("timezone", "UTC")
    if not isinstance(timezone, str) or not timezone.strip():
        raise ConfigError("timezone must be a non-empty IANA timezone name")
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(f"timezone is not available: {timezone}") from exc
    result["timezone"] = timezone

    state = _merge_section(
        _mapping(source.get("state", {}), "state"), DEFAULT_CONFIG["state"], "state"
    )
    directory = state["directory"]
    if directory is not None and (
        not isinstance(directory, str) or not directory.strip()
    ):
        raise ConfigError("state.directory must be null or a non-empty path")
    result["state"] = {"directory": None if directory is None else directory.strip()}

    delivery = _merge_section(
        _mapping(source.get("delivery", {}), "delivery"),
        DEFAULT_CONFIG["delivery"],
        "delivery",
    )
    adapter, target = delivery["adapter"], delivery["target"]
    if adapter not in {"noop", "hermes_session"}:
        raise ConfigError("delivery.adapter must be noop or hermes_session")
    if target is not None and not isinstance(target, str):
        raise ConfigError("delivery.target must be null or a string")
    if adapter == "hermes_session" and (
        not isinstance(target, str) or not target.strip()
    ):
        raise ConfigError("delivery.target is required for hermes_session")
    result["delivery"] = {
        "adapter": adapter,
        "target": None if target is None else target.strip(),
    }

    result["model_routes"] = _normalize_routes(source.get("model_routes"))

    heartbeat = _merge_section(
        _mapping(source.get("heartbeat", {}), "heartbeat"),
        DEFAULT_CONFIG["heartbeat"],
        "heartbeat",
    )
    result["heartbeat"] = {
        "default_snooze_minutes": _int(
            heartbeat["default_snooze_minutes"],
            "heartbeat.default_snooze_minutes",
            1,
            1440,
        ),
        "silence_backoff": _normalize_silence_backoff(heartbeat["silence_backoff"]),
        "kinds": _normalize_heartbeat_kinds(heartbeat["kinds"]),
    }

    autonomy = _merge_section(
        _mapping(source.get("autonomy", {}), "autonomy"),
        DEFAULT_CONFIG["autonomy"],
        "autonomy",
    )
    result["autonomy"] = {
        "providers": _normalize_providers(autonomy["providers"]),
    }

    panel = _merge_section(
        _mapping(source.get("panel", {}), "panel"), DEFAULT_CONFIG["panel"], "panel"
    )
    result["panel"] = {
        "anchor_hour": _int(panel["anchor_hour"], "panel.anchor_hour", 0, 23),
        "activity_afterglow_minutes": _int(
            panel["activity_afterglow_minutes"],
            "panel.activity_afterglow_minutes",
            1,
            1440,
        ),
    }

    memory = _merge_section(
        _mapping(source.get("memory", {}), "memory"), DEFAULT_CONFIG["memory"], "memory"
    )
    result["memory"] = {
        "search_limit": _int(memory["search_limit"], "memory.search_limit", 1, 100),
        "recall_enabled": _bool(memory["recall_enabled"], "memory.recall_enabled"),
        "recall_limit": _int(memory["recall_limit"], "memory.recall_limit", 1, 20),
        "resurfacing_enabled": _bool(
            memory["resurfacing_enabled"], "memory.resurfacing_enabled"
        ),
        "resurfacing_limit": _int(
            memory["resurfacing_limit"], "memory.resurfacing_limit", 1, 8
        ),
        "resurfacing_ttl_minutes": _int(
            memory["resurfacing_ttl_minutes"],
            "memory.resurfacing_ttl_minutes",
            1,
            1440,
        ),
        "resurfacing_cooldown_minutes": _int(
            memory["resurfacing_cooldown_minutes"],
            "memory.resurfacing_cooldown_minutes",
            1,
            10080,
        ),
        "maintenance_enabled": _bool(
            memory["maintenance_enabled"], "memory.maintenance_enabled"
        ),
    }
    return result


def route_bindings(config: Mapping[str, Any]) -> RouteBindings | None:
    routes = config.get("model_routes")
    if routes is None:
        return None
    return RouteBindings(**{role: routes[role]["alias"] for role in _ROLE_NAMES})


def validate_known_aliases(
    config: Mapping[str, Any], known_aliases: set[str] | None
) -> None:
    bindings = route_bindings(config)
    if bindings is None or known_aliases is None:
        return
    missing = sorted(set(bindings.__dict__.values()) - known_aliases)
    if missing:
        raise ConfigError(
            f"model route aliases are unknown to the host: {', '.join(missing)}"
        )
