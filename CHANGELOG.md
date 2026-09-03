# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Session recovery:** `hermes moonbite session status` lists exact open turns, and `session repair` appends an idempotent `abandoned` terminal for the specified current turn without fabricating a successful model response.
- **HermesHostAdapter:** the public integration boundary now consumes Hermes `on_session_end` and normalizes successful, failed, interrupted, incomplete, session-rotation, and shutdown exits into canonical lifecycle evidence. The pinned Hermes 0.20.5 commit and official v0.21.0 release share the same tested contract.
- **Subagent terminal fallback:** the seventh manifest hook, `subagent_stop`, uses only child session identity and bounded status to close a non-success one-shot child turn in the existing canonical terminal ledger; completed children remain owned by their own post/end evidence.
- **Proactive maintenance control:** `proactive` is the canonical group gate for Heartbeat and Autonomy. `background_costly` remains a compatibility input alias backed by the same control state.
- **Per-activity composition:** host adapters can inject explicit `ActivityProvider` descriptors through the public runtime/registration boundary. Moonbite performs a replayable occurrence-keyed weighted selection, persists it as the effect intent, and remains the single generic control, selection, effect, audit, and afterglow owner.

### Fixed

- **Orphaned turns:** `on_session_end` now closes a turn whose success-only `post_llm_call` was omitted; a new `pre_llm_call` remains the crash-recovery fallback, preventing the Conversation Gate from remaining permanently active without a TTL.
- **Compression lifecycle correlation:** if Hermes rotates `session_id` during compression, Moonbite uses the stable `turn_id` and durable lifecycle ledger to attach post/end callbacks to the original turn; ambiguous evidence fails closed.
- **Canonical proactive terminals:** Heartbeat and Autonomy now persist one occurrence-keyed terminal audit for pre-Judge skips and settled effects. Exact duplicates reuse that terminal before Judge/provider execution, while conflicting source or effect identity fails closed.
- **Multi-effect Heartbeat settlement:** an occurrence with both delivery and wake effects is terminal only after every sibling effect settles; one early receipt can no longer make the whole occurrence appear complete.

### Compatibility

- Session ledgers may now contain additive `moon.session.turn_terminal.v1` rows. Moonbite `0.1.0a1` cannot read upgraded state; take a state snapshot before upgrading and retain the new reader or restore that snapshot when rolling back.

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
