#!/usr/bin/env bash
set -euo pipefail

plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
hermes_repo="${HERMES_REPO:-$plugin_root/../hermes-agent}"
test_home="${MOONBITE_TEST_HOME:-$plugin_root/.hermes-test}"
expected_commit="${HERMES_EXPECTED_COMMIT:-}"
uv_bin="${UV_BIN:-$(command -v uv || true)}"

if [[ -z "$uv_bin" ]]; then
  echo "uv is required; set UV_BIN to its absolute path" >&2
  exit 2
fi

if [[ ! -d "$hermes_repo/.git" ]]; then
  git clone --depth 1 https://github.com/NousResearch/hermes-agent.git "$hermes_repo"
fi

if [[ ! -f "$hermes_repo/pyproject.toml" ]]; then
  echo "HERMES_REPO is not an official-style Hermes checkout: $hermes_repo" >&2
  exit 2
fi

actual_commit="$(git -C "$hermes_repo" rev-parse HEAD)"
if [[ -n "$expected_commit" && "$actual_commit" != "$expected_commit" ]]; then
  echo "Hermes checkout mismatch: expected $expected_commit, got $actual_commit" >&2
  exit 2
fi

mkdir -p "$test_home"
chmod 700 "$test_home"

echo "Hermes commit: $actual_commit"
"$uv_bin" sync --directory "$hermes_repo" --locked
HERMES_HOME="$test_home" "$hermes_repo/.venv/bin/hermes" \
  plugins doctor "$plugin_root" --ci
HERMES_HOME="$test_home" PYTHONPATH="$hermes_repo:$plugin_root" \
  "$hermes_repo/.venv/bin/python" "$plugin_root/scripts/check-hermes-contract.py"
