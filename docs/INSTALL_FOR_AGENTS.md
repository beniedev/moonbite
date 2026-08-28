# Agent Installation & Setup Protocol

This protocol defines the strict operational rules and step-by-step procedure that automated setup agents, orchestrators, and AI pair programmers must follow when installing, configuring, or upgrading Moonbite in a Hermes Agent environment.

> [!CAUTION]
> **Zero Invention Policy:**
> Automated agents must never invent, guess, or default private operational parameters (such as timezones, API keys, model providers, cron schedules, session keys, or delivery targets). All owner decisions must be explicitly presented to and confirmed by the operator before making changes to live configurations.

---

## 1. Mandatory Safe Execution Order

Agents must execute installation strictly in this sequence:

```text
1. Inspect host environment and platform support
2. Record base commits, Python version, platform, and git status --short
3. Set up an isolated HERMES_HOME for pre-flight testing
4. Install pinned Moonbite commit in disabled state (`--no-enable`)
5. If security verdict is caution, show findings and stop for owner approval
6. Only after approval, repeat install with `--force`; dangerous remains blocked
7. Run Hermes plugin doctor (syntax & manifest check)
8. Present owner decisions to the operator (never guess values)
9. Generate an exact configuration diff and request explicit operator approval
10. Back up the target Hermes configuration file
11. Apply approved settings under `plugins.entries.moonbite`
12. Enable with `--no-allow-tool-override`
13. Start a fresh Hermes process
14. Run side-effect-free smoke diagnostics (doctor and status)
15. Collect outputs, exit codes, security findings, and the effective diff
16. Maintain an explicit rollback plan
```

---

## 2. Non-Negotiable Owner Decisions

An automated agent must **never guess** the following values. They must be explicitly decided and confirmed by the human owner:

1. **Target Hermes Profile & HERMES_HOME path.**
2. **Timezone:** Local IANA timezone string (e.g. `UTC`, `America/Los_Angeles`, `Asia/Tokyo`).
3. **Anchor Hour:** Local hour (0–23) for daily rollover (e.g. `6` for 06:00).
4. **State Storage Location:** Path for private Moonbite state (or `null` for `$HERMES_HOME/moonbite`).
5. **Enabled Modules:** Selection from `runtime_core`, `heartbeat`, `autonomy`, `panel`, `memory`.
6. **Model Roles & Auxiliary Task Aliases:** Host route mappings for `main`, `heartbeat`, and `hippocampus` (`moon_main`, `moon_support`).
7. **Autonomy Providers & Weights:** Which providers to enable (`local_reflection`, `model_reflection`, `paper_browse`, `x_browse`) and their relative weights.
8. **Host Cron Cadence:** Frequency and timing of host ticks.
9. **Timeout & Cost Tier:** Host execution timeouts and realtime vs. discounted model tier assignments.
10. **Delivery Adapter & Target:** Selection of delivery adapter (`noop` vs. `hermes_session`) and specific destination session keys.
11. **Gateway Injection Consent:** Explicit consent for `allow_gateway_injection: true` (required only for session wake injection).
12. **Retention, Backup & Rollback Policy:** Retention rules for private state, backup paths, and physical file deletion policies.

---

## 3. Standard Setup Plan Schema (`moonbite.setup_plan.v1`)

Before modifying any configuration, agents should formulate and present a structured plan following the `moonbite.setup_plan.v1` schema:

```json
{
  "schema_version": "moonbite.setup_plan.v1",
  "source_commit": "<40-character-commit-sha>",
  "hermes_commit": "987064caa4f8845f605ac7346fed5b72fddfb21c",
  "target_profile": null,
  "preset": null,
  "owner_decisions_required": [
    "target_profile",
    "timezone",
    "anchor_hour",
    "state_directory",
    "enabled_modules",
    "model_routes",
    "autonomy_providers",
    "cron_cadence",
    "timeout_and_cost_tier",
    "delivery_adapter_and_target",
    "gateway_injection_consent",
    "retention_backup_rollback_and_physical_deletion"
  ],
  "config_patch": {},
  "expected_tools": [
    "moonbite_status",
    "control_moonbite_runtime",
    "record_moonbite_event",
    "run_moonbite_heartbeat",
    "run_moonbite_autonomy",
    "get_moonbite_panel",
    "search_moonbite_memory",
    "open_moonbite_memory",
    "capture_moonbite_memory_card",
    "synthesize_moonbite_diary"
  ],
  "expected_hooks": [
    "pre_gateway_dispatch",
    "on_session_start",
    "pre_llm_call",
    "post_llm_call",
    "on_session_finalize"
  ],
  "smoke_tests": [
    "hermes plugins doctor moonbite --ci",
    "hermes moonbite doctor",
    "hermes moonbite status"
  ],
  "rollback": [
    "hermes plugins disable moonbite",
    "restore the owner-approved configuration backup",
    "start a fresh Hermes process"
  ],
  "state_retention": "preserve"
}
```

Every `null`, placeholder, and empty object above means “unresolved”; it is not
permission for the installer to choose a value.

---

## 4. Detailed Step-by-Step Instructions

### Step 1: Pre-flight & Recording
Record the baseline state:
```bash
git status --short
python3 --version
```
Confirm that the host platform is Linux, WSL2 (on a native Linux filesystem), or macOS. If running on native Windows, abort immediately.

### Step 2: Isolated Installation
Always test first against an isolated temporary directory:
```bash
export HERMES_HOME="$(mktemp -d)"
export MOONBITE_COMMIT="<40-character-commit-sha>"
hermes plugins install beniedev/moonbite \
  --ref "${MOONBITE_COMMIT}" \
  --no-enable
```

No Moonbite package is published for this preview. Install from source through
Hermes, and do not substitute a floating branch or historical baseline for the
owner-approved commit.

Capture the security verdict and findings. A successful install continues to
Step 3. A `caution` verdict pauses here: show the exact findings and request
owner approval. Only after approval may the agent run the reviewed
non-interactive path:

```bash
hermes plugins install beniedev/moonbite \
  --ref "${MOONBITE_COMMIT}" \
  --no-enable \
  --force
```

`--force` records approval of caution findings; it does not bypass a
`dangerous` verdict. CI's clean-install lifecycle exercises this explicitly
approved non-interactive path.

### Step 3: Pre-Enable Doctor Check
Verify that the plugin manifest and registration are syntactically valid:
```bash
hermes plugins doctor moonbite --ci
```

### Step 4: Configuration Merge
Present the selected preset fragment (e.g. `core-only`, `panel-only`, `memory-only`, or `full-companion`) to the owner. Once approved, merge it under `plugins.entries.moonbite.settings.config`. `config/example.yaml` is settings-only; `config/effective-config.example.yaml` shows the separate final loader state.

### Step 5: Enable Plugin & Restart
Enable the plugin:
```bash
hermes plugins enable moonbite --no-allow-tool-override
```
Ensure any daemon/gateway process is cleanly restarted so Python loads fresh hooks and entry points.

### Step 6: Safe Smoke Diagnostics
Run side-effect-free diagnostic checks:
```bash
hermes moonbite doctor
hermes moonbite status
```
Verify that `doctor` reports `network_probe: "not_performed"` and `writes_performed: false`.

### Step 7: Rollback Procedure
If any validation fails:
1. Disable plugin: `hermes plugins disable moonbite`
2. Restore configuration from backup.
3. Restart the Hermes process.
4. Verify state retention: private state under `$HERMES_HOME/moonbite` is preserved and not deleted.
