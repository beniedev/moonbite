# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0a1] - 2026-08-28

### Added

- **Runtime core:** normalized events, append-only JSONL ledgers for events, audit, controls, and cadence, atomic JSON state writes, and POSIX `fcntl` file locks.
- **Controls engine:** priority-based runtime control intents (`pause`, `resume`, `quota_save`, `play_next`) and manual/automatic cadence snoozing with distinct release rules.
- **Heartbeat pipeline:** fail-closed execution order (`ControlGate → cadence → Judge → delivery/wake → terminal audit`), separating accepted, unverified, and verified effects.
- **Heartbeat contact guards:** explicitly configured urgent kinds may bypass `recent_contact` and/or `active_chat`; defaults remain guarded, and candidates still pass through Judge and normal effect/receipt handling.
- **Autonomy engine:** weighted single-choice provider selection, terminal failure handling per tick (no same-tick rerolls), and one-shot `play_next` consumption only after completed execution.
- **Activity providers:** built-in `local_reflection`, opt-in `model_reflection` via Hermes auxiliary routing, and disabled host-fed `paper_browse` / `x_browse` read-only examples.
- **Panel (Daily RAM):** typed values with source, confidence, TTL, daily epoch rollover at configurable anchor hour, sensor observations (`chat_rhythm`), consume-once lifecycles, and bounded autonomy afterglow pointers.
- **Standalone Panel API (`moonbite_plugin.panel_api`):** host-neutral `create_panel` factory and `PanelStore` for use outside Hermes Agent without constructing the full `MoonbiteRuntime`; it reuses Moonbite's minimal durable event-bus primitives.
- **Memory system:** append-only memory cards and daily diary entries with explicit provenance tracking (`user_explicit`, `agent_observation`, `agent_inference`), backward-compatible `moon.memory.card.v2` history/lifecycle fields, lexical current/active search with explicit historical/archive views, exact `open_ref` and bounded relation-history inspection, grounded diary synthesis (`synthesize_moonbite_diary`), and SHA-256-bound maintenance proposals.
- **Memory maintenance:** permission-gated operator apply for merge, retire/archive, and distill proposals, recorded as append-only `moon.memory.history.v1` receipts. The plugin exposes no delete/purge operation.
- **Configuration presets:** four tested inert preset fragments in `config/presets/` (`core-only.yaml`, `panel-only.yaml`, `memory-only.yaml`, `full-companion.yaml`).
- **Hermes integration:**
  - 10 tools: `moonbite_status`, `control_moonbite_runtime`, `record_moonbite_event`, `run_moonbite_heartbeat`, `run_moonbite_autonomy`, `get_moonbite_panel`, `search_moonbite_memory`, `open_moonbite_memory`, `capture_moonbite_memory_card`, and `synthesize_moonbite_diary`.
  - 5 manifest hooks in exact `HOOK_ORDER`:
    1. `pre_gateway_dispatch`: pre-authorization context seam; no-op without typed host resolver and never reads unauthorized content.
    2. `on_session_start`: records normalized lifecycle telemetry on session start.
    3. `pre_llm_call`: records pre-model step and attaches bounded untrusted Afterglow or recall context.
    4. `post_llm_call`: records settled post-model lifecycle state.
    5. `on_session_finalize`: records normalized session finalization.
  - 15 CLI commands under `hermes moonbite {status,doctor,control,event,heartbeat,autonomy,panel,memory-search,memory-recall,memory-resurface,memory-maintenance-propose,memory-maintenance-apply,memory-open,memory-add,diary-synthesize}`.
  - Slash command `/moon` for quick operator inspection and controls (`status`, `doctor`, `pause`, `resume`, `quota-save`).
  - Auxiliary task registration for `main`, `heartbeat`, and `hippocampus` model lanes.
  - Capability-checked targeted main-session wake adapter (`hermes_session`).
- **Diagnostics & testing:**
  - `doctor` command and side-effect-free diagnostic reports with stable structured fields.
  - Strict schema and configuration validation (`moonbite_plugin.config`).
  - Supported platform detection (macOS, Linux, WSL2; native Windows unsupported).
  - Isolated Hermes contract validation test runner (`scripts/test-hermes-contract.sh`).
- **Documentation package:**
  - English and Simplified Chinese READMEs (`README.md`, `README.zh-CN.md`).
  - Canonical English and Simplified Chinese design philosophy
    (`docs/DESIGN_PHILOSOPHY.md`, `docs/DESIGN_PHILOSOPHY.zh-CN.md`).
  - Setup and installation guide (`SETUP.md`).
  - Architecture and design boundaries (`DESIGN.md`).
  - Configuration reference and schemas (`CONFIGURATION.md`, `config/example.yaml`, `config/schema.json`, `config/presets/`).
  - Compatibility matrices and policies (`COMPATIBILITY.md`, `DEPLOYMENT_COMPATIBILITY.md`).
  - Security and privacy reporting policy (`SECURITY.md`).
  - Automation-safe installation protocol under `docs/`.
  - Panel feature documentation (`docs/features/PANEL.md`).
  - Owner release checklist (`docs/RELEASE_CHECKLIST.md`).
  - Standalone and harness Python examples (`examples/panel_standalone.py`, `examples/panel_generic_harness.py`).
