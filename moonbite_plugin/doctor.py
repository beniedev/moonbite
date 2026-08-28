"""Side-effect-free Moonbite diagnostics."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from .config import (
    ConfigError,
    normalize_config,
    route_bindings,
    validate_known_aliases,
)
from .observer import HealthSnapshot, ScheduleProof
from .platforms import UnsupportedPlatformError, detect_platform, state_root


def _looks_like_runtime(value: Any) -> bool:
    if value is None or isinstance(value, (Mapping, HealthSnapshot)):
        return False
    try:
        return isinstance(getattr(value, "config"), Mapping) and callable(
            getattr(value, "health_snapshot")
        )
    except Exception:
        return False


def _health_report(
    *,
    runtime: Any | None,
    snapshot: HealthSnapshot | None,
    target_date: date | None,
    now: datetime | None,
    schedule_proof: ScheduleProof | None,
) -> dict[str, Any]:
    if snapshot is None and runtime is not None:
        try:
            if target_date is None and now is None and schedule_proof is None:
                candidate = runtime.health_snapshot()
            else:
                candidate = runtime.health_snapshot(
                    target_date=target_date,
                    now=now,
                    schedule_proof=schedule_proof,
                )
        except Exception as exc:  # noqa: BLE001 - diagnostic boundary
            return {
                "available": False,
                "state": "current",
                "degraded": True,
                "status": "degraded",
                "reason": f"health_unavailable:{type(exc).__name__}",
            }
        if isinstance(candidate, HealthSnapshot):
            result = candidate.to_dict()
            result["available"] = True
            return result
        return {
            "available": False,
            "state": "current",
            "degraded": True,
            "status": "degraded",
            "reason": "health_unavailable:invalid_snapshot",
        }
    if snapshot is not None and not isinstance(snapshot, HealthSnapshot):
        return {
            "available": False,
            "state": "current",
            "degraded": True,
            "status": "degraded",
            "reason": "health_unavailable:invalid_snapshot",
        }
    if snapshot is not None:
        result = snapshot.to_dict()
        result["available"] = True
        return result
    return {
        "available": False,
        "state": "neutral",
        "reason": "health_unavailable:runtime_not_provided",
    }


def doctor_report(
    raw: Any = None,
    *,
    known_aliases: set[str] | None = None,
    runtime: Any | None = None,
    health: HealthSnapshot | None = None,
    health_snapshot: HealthSnapshot | None = None,
    target_date: date | None = None,
    now: datetime | None = None,
    schedule_proof: ScheduleProof | None = None,
    include_private_paths: bool = False,
) -> dict[str, Any]:
    """Return configuration and optional runtime health without side effects.

    ``raw`` remains the legacy configuration argument.  A runtime or an
    already-collected :class:`HealthSnapshot` may also be supplied; a missing
    runtime is reported as neutral/unavailable rather than being constructed
    from configuration.
    """

    snapshot = health_snapshot or health
    if isinstance(raw, HealthSnapshot):
        snapshot = raw if snapshot is None else snapshot
        raw = {}
    elif runtime is None and _looks_like_runtime(raw):
        runtime = raw
        raw = getattr(runtime, "config", {})
    elif raw is None and runtime is not None:
        raw = getattr(runtime, "config", {})

    health_result = _health_report(
        runtime=runtime,
        snapshot=snapshot,
        target_date=target_date,
        now=now,
        schedule_proof=schedule_proof,
    )
    try:
        config = normalize_config(raw)
        validate_known_aliases(config, known_aliases)
        platform = detect_platform()
    except (ConfigError, UnsupportedPlatformError) as exc:
        unsupported = isinstance(exc, UnsupportedPlatformError)
        return {
            "ok": False,
            "plugin_loaded": runtime is not None,
            "config_valid": False,
            "error": type(exc).__name__,
            "code": "unsupported_platform" if unsupported else "invalid_config",
            "message": (
                "Moonbite supports macOS and Linux/WSL."
                if unsupported
                else "Moonbite configuration is invalid."
            ),
            "remediation": (
                "Use a supported host platform."
                if unsupported
                else "Validate plugins.entries.moonbite.settings.config against config/schema.json."
            ),
            "network_probe": "not_performed",
            "writes_performed": False,
            "telemetry": "moonbite_observer",
            "telemetry_available": health_result.get("available") is True,
            "health": health_result,
        }

    bindings = route_bindings(config)
    health_available = health_result.get("available") is True
    return {
        "ok": True,
        "plugin_loaded": runtime is not None,
        "config_valid": True,
        "config_version": config["config_version"],
        "enabled_modules": sorted(
            name for name, enabled in config["modules"].items() if enabled
        ),
        "platform": {"family": platform.family, "is_wsl": platform.is_wsl},
        "state_root": (
            str(state_root(config["state"]["directory"]))
            if include_private_paths
            else "private"
        ),
        "available_modules": [
            "runtime_core",
            "heartbeat",
            "autonomy",
            "panel",
            "memory",
        ],
        "delivery_adapter": config["delivery"]["adapter"],
        "model_routes": (
            None
            if bindings is None
            else {
                "roles": bindings.__dict__,
                "resolution": (
                    "verified" if known_aliases is not None else "host_owned_not_probed"
                ),
            }
        ),
        "network_probe": "not_performed",
        "writes_performed": False,
        "scheduler": "host_owned",
        "scheduler_configured": "unknown",
        "gateway_injection": {
            "required": config["delivery"]["adapter"] == "hermes_session",
            "configured": "unknown",
        },
        "state_root_available": "not_probed",
        "telemetry": "moonbite_observer",
        "telemetry_available": health_available,
        "health": health_result,
    }
