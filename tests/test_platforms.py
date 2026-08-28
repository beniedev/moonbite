from __future__ import annotations

from pathlib import Path

import pytest

from moonbite_plugin.platforms import (
    UnsupportedPlatformError,
    detect_platform,
    hermes_home,
    state_root,
)


def test_detects_macos_linux_and_wsl():
    assert detect_platform(system="Darwin", release="24.6").family == "macos"
    linux = detect_platform(system="Linux", release="6.8.0-generic")
    assert (linux.family, linux.is_wsl) == ("linux", False)
    wsl = detect_platform(system="Linux", release="6.6.87.2-microsoft-standard-WSL2")
    assert (wsl.family, wsl.is_wsl) == ("linux", True)


def test_native_windows_is_explicitly_unsupported():
    with pytest.raises(UnsupportedPlatformError, match="macOS and Linux/WSL"):
        detect_platform(system="Windows", release="11")


def test_paths_follow_isolated_hermes_home():
    env = {"HERMES_HOME": "/tmp/hermes-fixture"}
    assert hermes_home(env=env) == Path("/tmp/hermes-fixture")
    assert state_root(env=env) == Path("/tmp/hermes-fixture/moonbite")
    assert state_root("state/moon", env=env) == Path("/tmp/hermes-fixture/state/moon")
