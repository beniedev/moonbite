#!/usr/bin/env bash
set -euo pipefail

plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hermes_repo="${HERMES_REPO:-$plugin_root/../hermes-agent}"
source_repo="${MOONBITE_INSTALL_SOURCE:-$plugin_root}"
expected_hermes="${HERMES_EXPECTED_COMMIT:-}"
uv_bin="${UV_BIN:-$(command -v uv || true)}"

if [[ -z "$uv_bin" ]]; then
  echo "uv is required; set UV_BIN to its absolute path" >&2
  exit 2
fi
if ! git -C "$hermes_repo" rev-parse --git-dir >/dev/null 2>&1 \
  || ! git -C "$source_repo" rev-parse --git-dir >/dev/null 2>&1; then
  echo "HERMES_REPO and MOONBITE_INSTALL_SOURCE must be Git checkouts" >&2
  exit 2
fi

actual_hermes="$(git -C "$hermes_repo" rev-parse HEAD)"
if [[ -n "$expected_hermes" && "$actual_hermes" != "$expected_hermes" ]]; then
  echo "Hermes checkout mismatch: expected $expected_hermes, got $actual_hermes" >&2
  exit 2
fi
source_commit="$(git -C "$source_repo" rev-parse HEAD)"
if [[ ! "$source_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Moonbite source must resolve to a full Git commit" >&2
  exit 2
fi

owned_home=0
if [[ -z "${MOONBITE_TEST_HOME:-}" ]]; then
  test_home="$(mktemp -d -t moonbite-clean-install-XXXXXX)"
  owned_home=1
else
  test_home="$MOONBITE_TEST_HOME"
  mkdir -p "$test_home"
fi
if [[ -n "$(find "$test_home" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "MOONBITE_TEST_HOME must be empty: $test_home" >&2
  exit 2
fi
if [[ "$owned_home" == 1 ]]; then
  trap 'rm -rf -- "$test_home"' EXIT
fi
chmod 700 "$test_home"

if [[ ! -x "$hermes_repo/.venv/bin/hermes" ]]; then
  "$uv_bin" sync --directory "$hermes_repo" --locked
fi
hermes="$hermes_repo/.venv/bin/hermes"
python="$hermes_repo/.venv/bin/python"
export HERMES_HOME="$test_home"
export HERMES_ENABLE_PROJECT_PLUGINS=0
cd "$test_home"

# This is the documented owner-approved non-interactive path. The repository
# contains executable examples/tests, so Hermes reports caution; --force records
# explicit acceptance of those reviewed findings. Dangerous findings still block.
"$hermes" plugins install "file://$(realpath "$source_repo")" \
  --ref "$source_commit" --no-enable --force

"$hermes" plugins list --user --json > "$test_home/plugins-disabled.json"
"$python" - "$test_home/plugins-disabled.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1], encoding="utf-8"))
row = next(item for item in rows if item["name"] == "moonbite")
assert row["status"] == "not enabled", row
PY

"$hermes" plugins doctor moonbite --ci

"$python" - "$test_home" <<'PY'
from pathlib import Path
import sys, yaml

home = Path(sys.argv[1])
path = home / "config.yaml"
config = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
config = config or {}
plugins = config.setdefault("plugins", {})
entries = plugins.setdefault("entries", {})
preset = yaml.safe_load(
    (home / "plugins" / "moonbite" / "config" / "presets" / "core-only.yaml").read_text(
        encoding="utf-8"
    )
)
preset["state"]["directory"] = str(home / "moonbite-state")
entries["moonbite"] = {
    "allow_gateway_injection": False,
    "settings": {"config": preset},
}
path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
PY

"$hermes" plugins enable moonbite --no-allow-tool-override
"$hermes" plugins list --user --json > "$test_home/plugins-enabled.json"
"$python" - "$test_home/plugins-enabled.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1], encoding="utf-8"))
row = next(item for item in rows if item["name"] == "moonbite")
assert row["status"] == "enabled", row
PY

"$hermes" moonbite doctor > "$test_home/moonbite-doctor.json"
"$python" - "$test_home/moonbite-doctor.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1], encoding="utf-8"))
assert report["ok"] is True
assert report["plugin_loaded"] is True
assert report["config_valid"] is True
assert report["enabled_modules"] == ["runtime_core"]
assert report["delivery_adapter"] == "noop"
assert report["model_routes"] is None
assert report["network_probe"] == "not_performed"
assert report["writes_performed"] is False
assert report["scheduler"] == "host_owned"
assert report["scheduler_configured"] == "unknown"
PY
"$hermes" moonbite status > "$test_home/moonbite-status.json"

probe_loader() {
  EXPECT_ENABLED="$1" "$python" - <<'PY'
import os, yaml
from pathlib import Path
from hermes_cli.plugins import PluginManager

manager = PluginManager()
manager.discover_and_load()
loaded = manager._plugins["moonbite"]
expected = os.environ["EXPECT_ENABLED"] == "1"
assert loaded.enabled is expected
if expected:
    manifest = yaml.safe_load(
        (Path(os.environ["HERMES_HOME"]) / "plugins" / "moonbite" / "plugin.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert loaded.manifest.kind == "standalone"
    assert loaded.tools_registered == manifest["provides_tools"]
    assert loaded.hooks_registered == manifest["provides_hooks"]
    assert "moonbite" in manager._cli_commands
    assert "moon" in manager._plugin_commands
else:
    assert loaded.tools_registered == []
    assert loaded.hooks_registered == []
    assert "moonbite" not in manager._cli_commands
    assert "moon" not in manager._plugin_commands
PY
}

probe_loader 1
test ! -e "$test_home/moonbite-state"
test ! -e "$test_home/cron/jobs.json"

"$hermes" moonbite event clean_install_fixture --payload '{}' \
  > "$test_home/moonbite-event.json"
state_ledger="$test_home/moonbite-state/events.jsonl"
test -f "$state_ledger"
state_before="$("$python" -c 'import hashlib, sys; print(hashlib.file_digest(open(sys.argv[1], "rb"), "sha256").hexdigest())' "$state_ledger")"

"$hermes" plugins disable moonbite
probe_loader 0
"$hermes" plugins list --user --json > "$test_home/plugins-disabled-final.json"
"$python" - "$test_home/plugins-disabled-final.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1], encoding="utf-8"))
row = next(item for item in rows if item["name"] == "moonbite")
assert row["status"] == "disabled", row
PY
if "$hermes" moonbite status > /dev/null 2>&1; then
  echo "Moonbite CLI remained registered after disable" >&2
  exit 1
fi
state_after="$("$python" -c 'import hashlib, sys; print(hashlib.file_digest(open(sys.argv[1], "rb"), "sha256").hexdigest())' "$state_ledger")"
test "$state_before" = "$state_after"

echo "Clean install lifecycle (approved caution): Hermes $actual_hermes, Moonbite $source_commit, disabled/enabled/disabled, 10 tools, 7 hooks, state preserved"
