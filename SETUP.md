# Setup Guide

This guide describes how to install, configure, and safely run the Moonbite
persistent runtime plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent),
as well as how to set up a local development and testing environment.

> [!NOTE]
> **Pre-alpha public source preview:**
> No Moonbite package is published for this preview. Install from source through
> Hermes, pin a full 40-character Git commit SHA, and validate within an isolated
> `HERMES_HOME` before touching a live profile. Do not use floating `main` as a
> stable target.

---

## 1. Prerequisites and platform support

| Requirement | Specification |
|---|---|
| Operating System | Linux (including Ubuntu 24.04 and WSL2 on native Linux filesystem) or macOS. Native Windows is **unsupported** due to mandatory POSIX `fcntl` file locks. |
| Python | `>=3.11,<3.15` (Python 3.11, 3.12, 3.13, or 3.14 for Moonbite; pinned Hermes checkout supports 3.11–3.13). |
| Package Manager | [`uv`](https://docs.astral.sh/uv/) (recommended) or standard Python virtual environment tooling. |
| Target Hermes Agent | Pinned official Hermes Agent commit `987064caa4f8845f605ac7346fed5b72fddfb21c`. |
| Filesystem | On WSL2, use the native Linux filesystem (e.g. `~`), **not** Windows mounts such as `/mnt/c`. |

---

## 2. User installation (Hermes operator)

This section is for operators running Hermes Agent who want to install and use Moonbite.

### 2.1 Five distinct operational states

To operate Moonbite safely, understand that the runtime distinguishes five independent states:
1. **Source Installation:** Code checkout downloaded into Hermes plugins directory.
2. **Plugin Enablement:** Loader gate in `plugins.enabled: [moonbite]` (where `plugins.disabled` wins). Never use `plugins.entries.moonbite.enabled`, as it is not a loader switch.
3. **Module Enablement:** Fine-grained internal module switches under `plugins.entries.moonbite.settings.config.modules`.
4. **Scheduler Configuration:** External host-owned cron ticks triggering Moonbite CLI or tools.
5. **Delivery Readiness:** Verified delivery adapter (`noop` vs. configured `hermes_session` with explicit gateway injection consent).

### 2.2 Install pinned commit in disabled state

Install Moonbite into your Hermes environment pinned to an immutable 40-character Git commit using `--no-enable`:

```bash
export MOONBITE_COMMIT="<40-character-commit-sha>"
hermes plugins install beniedev/moonbite \
  --ref "${MOONBITE_COMMIT}" \
  --no-enable
```

If the security scan returns `caution`, inspect and present its findings. Add
`--force` only after the owner explicitly accepts those findings; automated
installers must not infer consent, and `dangerous` findings remain blocking.

> [!IMPORTANT]
> Replace `<40-character-commit-sha>` with a maintainer-reviewed commit that contains the version you intend to install. The recorded historical baseline is not an install recommendation.

### 2.3 Run pre-enable manifest doctor

Validate plugin manifest structure and discovery before enabling:

```bash
hermes plugins doctor moonbite --ci
```

### 2.4 Choose and merge a configuration preset

Choose one of four tested preset fragments from `config/presets/`:
- `core-only.yaml` — Minimal event bus and state core.
- `panel-only.yaml` — Enables daily RAM working state; requires no model routes.
- `memory-only.yaml` — Enables memory cards and diary storage; leaves recall, resurfacing, and maintenance disabled.
- `full-companion.yaml` — Enables companion modules with inert defaults (noop delivery, host-owned scheduling, unconfigured model routes).

Use `config/example.yaml` as the settings-only merge fragment. Refer to
`config/effective-config.example.yaml` only for the final post-enable loader
state.

Merge your selected preset into your Hermes configuration file (e.g. `$HERMES_HOME/config.yaml`) under `plugins.entries.moonbite.settings.config`, keeping `allow_gateway_injection: false`:

```yaml
plugins:
  entries:
    moonbite:
      allow_gateway_injection: false
      settings:
        config:
          config_version: 1
          timezone: UTC
          state:
            directory: null  # defaults to $HERMES_HOME/moonbite
          modules:
            runtime_core: true
            heartbeat: false
            autonomy: false
            panel: false
            memory: false
          delivery:
            adapter: noop
            target: null
          model_routes: null
```

The plugin remains disabled until the next CLI step writes the top-level
`plugins.enabled` allow-list entry.

### 2.5 Enable plugin and restart Hermes process

Enable the plugin in Hermes:

```bash
hermes plugins enable moonbite --no-allow-tool-override
```

Restart any long-running Hermes gateway or daemon process after installation, enablement,
or configuration changes so plugin discovery and hook registration execute in a
fresh process.

### 2.6 Safe first run & diagnostics

Verify that the plugin is recognized and completely inert before enabling behavioral modules:

```bash
# Run diagnostics without model calls, network probes, or state writes
hermes moonbite doctor

# Inspect effective runtime status
hermes moonbite status
```

Confirm that the output reports `ok: true`, `network_probe: "not_performed"`, `writes_performed: false`, and `delivery_adapter: "noop"`.

### 2.7 Manifest hooks lifecycle

Moonbite registers exactly 5 lifecycle hooks in Hermes (in actual `HOOK_ORDER`):
1. `pre_gateway_dispatch`: Runs before message authorization; provides a pre-authorization host context seam. Without an explicit typed host resolver, it acts as a no-op and never reads or records unauthorized message content.
2. `on_session_start`: Fires when a session starts; records normalized session-start lifecycle telemetry when resolvable context is available.
3. `pre_llm_call`: Fires before an LLM call; records the lifecycle step and may attach fresh Panel Afterglow or enabled Memory recall as bounded, untrusted context (never as instructions).
4. `post_llm_call`: Fires after an LLM call; records the settled post-model lifecycle state.
5. `on_session_finalize`: Fires when a session finishes; records normalized session finalization.

### 2.8 Enabling optional modules incrementally

Once the inert baseline is verified:
1. Choose exactly one optional module (for example, `modules.panel: true`).
2. If that module needs a model role, configure its host task route in Hermes (`moon_main` or `moon_support`, under `auxiliary.<alias>` or through `hermes model`).
3. Re-run `hermes moonbite doctor` and verify behavior before enabling further modules.

---

## 3. Mandatory owner decision checklist

Moonbite ships inert defaults; the host deployment retains complete authority over credentials, scheduling, delivery, and data retention. The operator must resolve the following decisions explicitly:

1. **Target Hermes Profile & Isolated HERMES_HOME:**
   - Always verify new configurations in an isolated test home before modifying production profiles.
2. **Timezone, daily anchor, and state directory:**
   - `timezone`: IANA timezone string (e.g. `UTC`, `America/Los_Angeles`, `Asia/Shanghai`).
   - `panel.anchor_hour`: Local hour (0–23) at which daily RAM fields roll over.
   - `state.directory`: Absolute path for private state storage, or `null` to use `$HERMES_HOME/moonbite`.
3. **Enabled modules:**
   - `modules.runtime_core`: Always `true` (core state and event bus).
   - `modules.heartbeat`: Enables the Heartbeat decision pipeline and Judge gating.
   - `modules.autonomy`: Enables weighted autonomous activity selection on host ticks.
   - `modules.panel`: Activates daily-RAM projection, sensor hooks, and Afterglow context.
   - `modules.memory`: Enables card/diary surfaces and search. Recall, resurfacing, and maintenance have separate opt-in flags; capture and diary synthesis still require an explicit caller.
4. **Model roles, task aliases, and host-owned routes:**
   - Moonbite binds three roles (`main`, `heartbeat`, `hippocampus`) to Hermes auxiliary task aliases. The inert example maps `main` to `moon_main` and both `heartbeat` and `hippocampus` to `moon_support`.
   - The host configures model providers, endpoints, tokens, and context limits using `hermes model` or `auxiliary.<alias>` blocks. Moonbite never stores model credentials or provider details.
5. **Autonomy providers and weights:**
   - Choose which providers to enable and configure their selection weights (e.g. `local_reflection`, `model_reflection`, host-fed `paper_browse`, `x_browse`).
6. **Host cron schedule and execution cadence:**
   - Moonbite contains no internal timer or scheduler. The owner chooses the cadence; host cron calls `hermes moonbite ...` CLI commands or triggers tools at those approved times.
7. **Delivery adapter, consent, and target:**
   - `delivery.adapter: noop` is safe and performs no external messaging.
   - `delivery.adapter: hermes_session` allows targeted main-session wake only when the host supports session key injection and `allow_gateway_injection: true` is configured.
8. **Retention, backup, and rollback policy:**
   - Local state files are POSIX owner-only (`0700`/`0600`). The operator is responsible for backup, compaction, and data retention schedules.

---

## 4. Developer workflow & test validation

This section is for contributors working on the Moonbite codebase itself.

### 4.1 Clone and virtual environment

```bash
git clone https://github.com/beniedev/moonbite.git
cd moonbite
uv venv
uv pip install -e '.[dev]'
```

### 4.2 Run tests and static checks

```bash
# Compile and run unit tests
.venv/bin/python -m compileall -q moonbite_plugin tests
.venv/bin/pytest

# Check code style with ruff
.venv/bin/ruff check moonbite_plugin tests scripts/check-hermes-contract.py --select F,E9
.venv/bin/ruff format --check moonbite_plugin tests scripts/check-hermes-contract.py
```

### 4.3 Verify against isolated Hermes checkout

Never use a live Hermes profile for plugin development or test runs. Validate against an isolated checkout using the contract test script:

```bash
# Clone the pinned Hermes repository into a sibling directory
git clone https://github.com/NousResearch/hermes-agent.git ../hermes-agent
git -C ../hermes-agent fetch --depth 1 origin 987064caa4f8845f605ac7346fed5b72fddfb21c
git -C ../hermes-agent checkout --detach 987064caa4f8845f605ac7346fed5b72fddfb21c

# Run contract verification with isolated test home
HERMES_REPO=../hermes-agent \
HERMES_EXPECTED_COMMIT=987064caa4f8845f605ac7346fed5b72fddfb21c \
MOONBITE_TEST_HOME=.hermes-test \
./scripts/test-hermes-contract.sh
```

---

## 5. Rollback and uninstallation

### 5.1 Rollback

To revert configuration changes, restore the previous Moonbite entry, restart any
long-running Hermes process, and run `hermes moonbite doctor`. To return to an
earlier plugin revision, reinstall its reviewed full SHA with `--force --ref`,
then repeat the same restart and diagnostic steps. Keep the previous SHA and
configuration backup before each pre-alpha upgrade.

### 5.2 Disabling or removing the plugin

Disable code loading with `hermes plugins disable moonbite`, or remove `moonbite`
from `plugins.enabled` (a matching `plugins.disabled` entry wins). Removing only
`plugins.entries.moonbite` deletes settings but does not disable an allow-listed
plugin. Use `hermes plugins remove moonbite` when you also want to uninstall the
plugin checkout.

> [!IMPORTANT]
> **State data is not automatically deleted.**
> Disabling or uninstalling the Moonbite plugin does not delete the private state
> directory (`$HERMES_HOME/moonbite` or the custom `state.directory`).
> Operators are responsible for managing, archiving, or securely removing state
> files according to their own data retention policy.

---

## 6. Related documentation

- [README.md](README.md) / [README.zh-CN.md](README.zh-CN.md) — Project overview and module summaries.
- [CONFIGURATION.md](CONFIGURATION.md) — Authoritative configuration specification.
- [DESIGN.md](DESIGN.md) — Architecture and boundary invariants.
- [COMPATIBILITY.md](COMPATIBILITY.md) — Platform and upstream Hermes compatibility matrix.
- [DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md) — Migration guidelines for established deployments.
- [SECURITY.md](SECURITY.md) — Security policy and release scanning procedures.
- [CHANGELOG.md](CHANGELOG.md) — Project change history.
- [docs/](docs/) — Automation-safe installation protocol and feature documentation.
- [docs/features/PANEL.md](docs/features/PANEL.md) — Panel (Daily RAM) architecture & API reference.
- [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md) — Pre-release owner actions & verification checklist.
