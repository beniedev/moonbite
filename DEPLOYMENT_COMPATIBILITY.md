# Deployment compatibility contract

## Audience and context

> [!NOTE]
> **New installations can safely ignore this document.**
> If you are setting up Moonbite for the first time on a fresh Hermes Agent instance, follow [SETUP.md](SETUP.md).

This document applies to operators migrating or integrating Moonbite into an
**established companion deployment**: an existing system that already owns
external schedules, persistent state, delivery adapters, and observable
companion behavior, and must adopt Moonbite incrementally without duplicate
control planes or split-brain state owners.

Repository compatibility fixtures (`tests/fixtures/compatibility/`) are synthetic test contracts designed to freeze portable behavioral semantics. They contain no private identities, paths, messages, credentials, endpoints, or deployment-specific names. Passing these fixtures proves that Moonbite's portable contracts remain intact, but **synthetic fixtures do not establish external deployment parity** or substitute for real-world verification.

---

## Frozen behavior

- Heartbeat controls run before Judge/model calls.
- Manual snooze blocks ordinary care, survives ordinary private replies, and
  ends only by expiry, explicit resume, or a later verified visible DM.
- Daily anchors, urgent care, emotional repair, critical operations, and
  connector failures bypass ordinary cadence.
- A queued wake is unverified until the host supplies a real completion
  receipt; it is not `effects_verified`.
- Autonomy runs at most one selected provider per tick. A selected failure does
  not reroll. `play_next` is consumed only after a completed run and remains
  retryable after failure.
- For viable Autonomy work the order remains control/platform gates, Judge,
  dynamic provider eligibility/selection, then one execution. An impossible
  static provider configuration may skip before Judge to avoid needless cost.
- Terminals stay `completed | failed | skipped`.
- Main remains the final cognitive authority. Continuity hints, Panel state,
  Memory evidence, and future Hippocampus work are bounded inputs, not peer
  authorities.
- Fresh Autonomy afterglow may enter chat as bounded ephemeral context. Expired
  fields are removed, and the main agent may ignore a topic that does not fit.

---

## Host-owned seams

The compatibility fixtures also mark behavior that a generic plugin must not fake:

- active-chat/afterglow gates and the activity descriptor catalog;
- scheduler timing and service tier;
- targeted main-session wake and direct-message delivery receipts;
- continuity hooks, session search, private platform adapters, and existing
  state ownership.

Moonbite exposes ports for these concerns. It must not replace a deployment's
dispatcher, cadence ledger, or delivery path with a parallel control plane.

## Alpha 1 to Alpha 2 state boundary

Before upgrading an established deployment from `0.1.0a1`, stop or pause every
process that can write the Moonbite state directory and take one consistent
snapshot of that complete directory plus the matching Moonbite configuration.
Retain the snapshot until the upgraded deployment has passed its own natural
validation cycle.

Alpha 2 may append `moon.session.turn_terminal.v1` rows and upgrade valid
Heartbeat cadence state to schema v4. Alpha 1 cannot read the new terminal rows
and must not be pointed at cadence state after a v4 write. Reinstalling Alpha 1
code alone is therefore not a valid rollback. A rollback to the Alpha 1 reader
requires all Moonbite writers to be stopped and the pre-upgrade state and
configuration snapshot to be restored together before the older code starts.
Disabling or uninstalling the plugin does not delete or downgrade state.

---

## Deployment validation gate

A candidate is eligible for single-module deployment validation only when its
adapter:

1. reuses the established owner/state for that module;
2. passes repository compatibility fixtures and corresponding host-focused
   tests;
3. runs from a pinned candidate commit/tag and has an explicit rollback anchor;
4. preserves the existing observable outputs and terminal meanings;
5. completes one natural deployment cycle without a manual trigger being used
   as proof.

Repository completion alone is not a deployment-validation pass.

---

## Related documentation

- [SETUP.md](SETUP.md) — First-time operator setup guide
- [CONFIGURATION.md](CONFIGURATION.md) — Authoritative configuration reference
- [COMPATIBILITY.md](COMPATIBILITY.md) — Upstream Hermes and platform compatibility
- [docs/](docs/) — Automation-safe setup protocol and feature documentation
- [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md) — Pre-release owner actions & verification checklist
