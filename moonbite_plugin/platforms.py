"""Supported-host detection and profile-aware Moonbite paths.

Moonbite intentionally supports the two POSIX hosts used by the project:
macOS and Linux (including WSL2). Native Windows is not advertised because
the runtime relies on POSIX file locks.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import platform as std_platform
from typing import Mapping


class UnsupportedPlatformError(RuntimeError):
    """Raised when Moonbite is loaded on an unverified host."""


@dataclass(frozen=True)
class PlatformInfo:
    family: str
    system: str
    release: str
    is_wsl: bool


def detect_platform(
    *, system: str | None = None, release: str | None = None
) -> PlatformInfo:
    raw_system = (system or std_platform.system()).strip()
    raw_release = (release or std_platform.release()).strip()
    normalized = raw_system.lower()
    if normalized == "darwin":
        return PlatformInfo("macos", raw_system, raw_release, False)
    if normalized == "linux":
        marker = raw_release.lower()
        return PlatformInfo(
            "linux",
            raw_system,
            raw_release,
            "microsoft" in marker or "wsl" in marker,
        )
    raise UnsupportedPlatformError(
        f"Moonbite supports macOS and Linux/WSL, not {raw_system or 'unknown'}"
    )


def hermes_home(
    *, env: Mapping[str, str] | None = None, home: Path | None = None
) -> Path:
    source = os.environ if env is None else env
    override = source.get("HERMES_HOME", "").strip()
    if override:
        return Path(override).expanduser()
    return (Path.home() if home is None else home) / ".hermes"


def state_root(
    configured: str | Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
    home: Path | None = None,
) -> Path:
    base = hermes_home(env=env, home=home)
    if configured is None or not str(configured).strip():
        return base / "moonbite"
    candidate = Path(configured).expanduser()
    return candidate if candidate.is_absolute() else base / candidate
