#!/usr/bin/env bash
set -euo pipefail

plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hermes_repo="${HERMES_REPO:-$plugin_root/../hermes-agent}"
expected_hermes="${HERMES_EXPECTED_COMMIT:-}"
uv_bin="${UV_BIN:-$(command -v uv || true)}"
python_bin="${PYTHON_BIN:-python3}"

if [[ -z "$uv_bin" ]] \
  || ! git -C "$hermes_repo" rev-parse --git-dir >/dev/null 2>&1; then
  echo "uv and an isolated HERMES_REPO checkout are required" >&2
  exit 2
fi
actual_hermes="$(git -C "$hermes_repo" rev-parse HEAD)"
if [[ -n "$expected_hermes" && "$actual_hermes" != "$expected_hermes" ]]; then
  echo "Hermes checkout mismatch: expected $expected_hermes, got $actual_hermes" >&2
  exit 2
fi

temporary="$(mktemp -d -t moonbite-clean-wheel-XXXXXX)"
trap 'rm -rf -- "$temporary"' EXIT
wheel_dir="$temporary/wheel"
home="$temporary/hermes-home"
mkdir -p "$wheel_dir" "$home"
chmod 700 "$home"

build_report="$(
  "$python_bin" "$plugin_root/scripts/build-clean-wheel.py" \
    --repo "$plugin_root" --output-dir "$wheel_dir" --uv "$uv_bin"
)"
wheel="$(
  "$python_bin" -c 'import json, sys; print(json.loads(sys.argv[1])["wheel"])' \
    "$build_report"
)"
test -f "$wheel"

"$python_bin" - "$wheel" <<'PY'
from pathlib import Path
import sys, zipfile

wheel = Path(sys.argv[1])
with zipfile.ZipFile(wheel) as archive:
    names = archive.namelist()
    assert any(name.endswith(".dist-info/entry_points.txt") for name in names)
    assert "moonbite_plugin/__init__.py" in names
    assert "moonbite_plugin/panel_api.py" in names
    assert not any(name.startswith(("tests/", "config/", "docs/", "examples/")) for name in names)
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)
PY

HERMES_HOME="$home" MOONBITE_SOURCE_ROOT="$plugin_root" \
  env -u PYTHONPATH "$uv_bin" run --isolated --frozen \
  --directory "$hermes_repo" --with "$wheel" python - <<'PY'
from pathlib import Path
import importlib.metadata
import json
import os
import subprocess
import sys

import moonbite_plugin

source = Path(os.environ["MOONBITE_SOURCE_ROOT"]).resolve()
origin = Path(moonbite_plugin.__file__).resolve()
assert source not in origin.parents, (source, origin)
entry_points = importlib.metadata.entry_points().select(group="hermes_agent.plugins")
entry_point = next(ep for ep in entry_points if ep.name == "moonbite")
module = entry_point.load()
assert callable(module.register)

home = Path(os.environ["HERMES_HOME"])
probe = r'''
import json, os
from hermes_cli.plugins import PluginManager
from moonbite_plugin.plugin import TOOL_NAMES
from moonbite_plugin.session import HOOK_ORDER

manager = PluginManager()
manager.discover_and_load()
loaded = manager._plugins["moonbite"]
expected = os.environ["EXPECT_ENABLED"] == "1"
assert loaded.manifest.source == "entrypoint"
assert loaded.manifest.kind == "standalone"
assert loaded.enabled is expected
if expected:
    assert tuple(loaded.tools_registered) == TOOL_NAMES
    assert tuple(loaded.hooks_registered) == HOOK_ORDER
    assert "moonbite" in manager._cli_commands
    assert "moon" in manager._plugin_commands
else:
    assert loaded.tools_registered == []
    assert loaded.hooks_registered == []
    assert "moonbite" not in manager._cli_commands
    assert "moon" not in manager._plugin_commands
print(json.dumps({"enabled": loaded.enabled, "kind": loaded.manifest.kind}))
'''

def run_probe(enabled):
    env = {**os.environ, "EXPECT_ENABLED": "1" if enabled else "0"}
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=home,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])

(home / "config.yaml").write_text("plugins:\n  enabled: []\n", encoding="utf-8")
assert run_probe(False) == {"enabled": False, "kind": "standalone"}
(home / "config.yaml").write_text(
    "plugins:\n"
    "  enabled:\n"
    "    - moonbite\n"
    "  entries:\n"
    "    moonbite:\n"
    "      allow_gateway_injection: false\n"
    "      settings:\n"
    "        config: {}\n",
    encoding="utf-8",
)
assert run_probe(True) == {"enabled": True, "kind": "standalone"}
assert not (home / "moonbite").exists()
PY

echo "Clean wheel: $wheel; Hermes entry-point discovery opt-in passed at $actual_hermes"
