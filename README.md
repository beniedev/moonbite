# Moonbite

> **Let AI persist in its own way.**

**Moonbite helps AI agents stay coherent over time.**

It gives long-running agents memory across sessions, short-lived working state,
bounded autonomy, and a way to decide when to act—or stay quiet. Moonbite also
records verified external actions instead of treating a model's claim as proof
that something happened.

Moonbite runs alongside the host agent rather than replacing it. The host still
owns models, tools, credentials, scheduling, and delivery.

Moonbite is an experimental project. Its first priority is to communicate a
design philosophy and explore how that philosophy might work in practice.

[![CI](https://github.com/beniedev/moonbite/actions/workflows/ci.yml/badge.svg)](https://github.com/beniedev/moonbite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.11–3.14](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](pyproject.toml)
[![Platform: Linux / WSL2 / macOS](https://img.shields.io/badge/Platform-Linux%20%7C%20WSL2%20%7C%20macOS-informational.svg)](COMPATIBILITY.md)
[![Status: Public Preview](https://img.shields.io/badge/Status-Public%20Preview-orange.svg)](#project-status)

[简体中文](README.zh-CN.md)

## Public Preview Scope

Supported in this preview:

- Hermes Agent as the only supported host
- Source installation pinned to an immutable commit SHA
- Linux, WSL2 on a native Linux filesystem, and macOS
- Runtime Core, Heartbeat, Autonomy, Panel / Daily RAM, Memory / Diary,
  controls, verified external actions, and audit records
- Host-owned model routing, credentials, scheduling, network access, gateway
  execution, and delivery

Not provided in this preview:

- Publication to PyPI or another package registry
- A stable public Python API guarantee
- Support for non-Hermes agent hosts or frameworks
- Production support or long-term compatibility guarantees
- An internal scheduler, credential store, network browser, or messaging channel

Moonbite is not published to a package registry. Preview releases are
source-only; install through Hermes from a GitHub release tag or pin an
immutable commit SHA.

## Why long-running agents need Moonbite

Most agent systems answer one prompt or work within one session. A long-running
agent might support a person over time, watch a system, or follow an ongoing
queue. To do that well, it needs to remember what happened before, keep track
of what matters now, decide whether something deserves action, and verify that
requested actions really occurred.

Moonbite provides those continuity mechanisms without becoming the agent or
the host. The main agent remains responsible for interpretation and final
decisions. The host remains responsible for execution and delivery.

## How Moonbite works

```text
            Main Agent
                ▲
                │
┌────────── Moonbite ──────────┐
│                              │
│  Memory       Panel          │
│  remembers    knows now      │
│                              │
│  Heartbeat    Autonomy       │
│  when to act  what to do     │
│                              │
└──────────────┬───────────────┘
               │
               ▼
         Hermes / Host
  models · tools · schedule · delivery
```

**What does Moonbite add?**

It gives a long-running agent:

**memory of the past / awareness of the present / judgment about when to act /
bounded autonomous action**

Hermes continues to own models, tools, credentials, scheduling, gateway
execution, and delivery. The main agent keeps final interpretive authority.

## Core components

- **Memory / Diary** remembers what matters across sessions. Search returns
  references that can be opened for exact evidence, and maintenance preserves
  provenance and history.
- **Panel / Daily RAM** keeps bounded, short-lived working state for what matters
  now, including daily rollover and verified activity Afterglow.
- **Heartbeat** decides whether something deserves attention, action, or
  escalation now. It is a decision pipeline, not a timer.
- **Autonomy** performs at most one eligible, host-triggered activity per tick.
  Host adapters explicitly inject per-activity providers; Moonbite makes one
  replayable weighted selection and owns its effect lifecycle. If the selected
  activity fails, Moonbite records the failure instead of choosing another one
  in the same tick.
- **Runtime Core** keeps events, state changes, controls, and decisions
  consistent in append-only records.
- **Controls and execution receipts** apply pause, quota, frequency, eligibility,
  and safety rules, then require a matching host receipt before an external
  action is considered accepted or completed. The `proactive` group is one
  maintenance gate for both Heartbeat and Autonomy; the legacy
  `background_costly` name resolves to that same state rather than creating a
  second control.

## What Moonbite leaves to the host

Moonbite does not include a scheduler, daemon, credential store, network
browser, model router, or messaging channel. The host owns those capabilities,
along with raw session history and search. The Hermes adapter registers exactly
10 tools and 7 lifecycle hooks; their exact interfaces are documented in
[SETUP.md](SETUP.md) and [plugin.yaml](plugin.yaml).

The hooks are `pre_gateway_dispatch`, `on_session_start`, `pre_llm_call`,
`post_llm_call`, `on_session_end`, `on_session_finalize`, and
`subagent_stop`.

With the default configuration:

- only `runtime_core` is enabled;
- `heartbeat`, `autonomy`, `panel`, and `memory` are disabled;
- delivery uses the `noop` adapter;
- model routes are unconfigured;
- installation alone starts no background task, model call, network request, or
  external message.

Every visible external action requires a matching execution receipt from the
host. Disabling or uninstalling Moonbite does not delete its state directory.

## For now, install through Hermes

Install a reviewed full 40-character commit in a disabled state:

```bash
hermes plugins install beniedev/moonbite \
  --ref "<40-character-commit-sha>" \
  --no-enable
```

Then inspect any installer security findings, run the manifest doctor, merge an
owner-reviewed configuration, and enable the plugin only when ready:

```bash
hermes plugins doctor moonbite --ci
hermes plugins enable moonbite --no-allow-tool-override
```

Do not install a floating `main` branch as a stable target. See
[SETUP.md](SETUP.md) for the complete isolated setup, consent, restart, and
rollback procedure.

## Verification, doctor, and rollback

After starting a fresh Hermes process:

```bash
hermes moonbite doctor
hermes moonbite status
hermes moonbite control pause proactive
hermes moonbite control resume proactive
```

The doctor is side-effect-free: it performs no model call, network probe, or
state write. A safe inert result reports `ok: true`,
`network_probe: "not_performed"`, `writes_performed: false`, and
`delivery_adapter: "noop"`.

To stop code loading, run `hermes plugins disable moonbite`. To remove the
installed checkout, run `hermes plugins remove moonbite`. Neither command
deletes Moonbite state; retention and physical deletion remain host-owned.

## Experimental surfaces

The repository includes an experimental, host-neutral Panel API for design
exploration and future adapters. It is tested but is not part of the initial
Hermes-only support contract and carries no stable API guarantee.

See [docs/features/PANEL.md](docs/features/PANEL.md).

## Design philosophy and technical references

- [Design philosophy](docs/DESIGN_PHILOSOPHY.md) explains why Moonbite exists.
- [设计理念（简体中文）](docs/DESIGN_PHILOSOPHY.zh-CN.md) is the canonical
  Chinese counterpart.
- [DESIGN.md](DESIGN.md) defines architecture and security boundaries.
- [CONFIGURATION.md](CONFIGURATION.md) documents configuration and presets.
- [COMPATIBILITY.md](COMPATIBILITY.md) records the tested platform and Hermes
  contract.
- [DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md) defines migration
  expectations for established deployments.
- [SECURITY.md](SECURITY.md) explains vulnerability reporting and release
  privacy gates.
- [CHANGELOG.md](CHANGELOG.md) records unreleased project changes.

## Project status

```text
Status: 0.1.0 Alpha 1 public preview
Supported host: Hermes Agent only
Distribution: source-only GitHub prerelease
Support: best effort
API stability: not guaranteed
```

This preview exposes the design, runtime contracts, and working implementation
early. Interfaces and compatibility may change as the project learns from
careful real-world use.

## License

Moonbite is available under the [MIT License](LICENSE).
