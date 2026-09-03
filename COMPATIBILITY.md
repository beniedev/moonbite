# Compatibility

Moonbite targets the documented Hermes Agent Python plugin API used by the
CLI/gateway runtime. It does not patch Hermes core and does not require or track
Hermes Desktop, web-dashboard internals, Electron/UI code, repository layout,
or undocumented implementation fields.

| Layer | Status | Notes |
|---|---|---|
| Ubuntu 24.04 / WSL2 | Supported | Tested on native Linux filesystem (e.g. `~`; `/mnt/c` mounts unsupported). |
| macOS | Supported | Uses the same POSIX code path; covered by the CI matrix, which must be green for release. |
| Python | `>=3.11,<3.15` | Supports Python 3.11, 3.12, 3.13, 3.14 (pinned Hermes checkout supports 3.11–3.13). |
| Native Windows | Unsupported | POSIX `fcntl` file locks are required for process synchronization. |
| Hermes v0.20.5 minimum | `v2026.8.19` / `fcbd1076a93841fa88855acce810e342a5b78101` | Required CLI/Gateway compatibility lane. |
| Hermes v0.21.0 current | `v2026.8.31` / `29112bef099274229cadff79cdff7bf7b99c4b77` | Required CLI/Gateway compatibility lane; Desktop is out of scope. |
| Current Upstream Hermes | Dynamic | Resolved and printed by scheduled/manual drift CI. See the latest workflow log for the tested SHA. |

---

## 1. Verified Surface & Lifecycle Hooks

Moonbite registers exactly 10 tools, 16 CLI commands, 1 slash command (`/moon`), and 7 manifest hooks.

The 7 manifest hooks are registered in strict `HOOK_ORDER`:
1. `pre_gateway_dispatch`: Fires before message authorization, providing a pre-auth host context seam. Without an explicit typed host resolver, Moonbite treats this as a no-op and never reads or records unauthorized message content.
2. `on_session_start`: Fires when a session initializes; records normalized session-start lifecycle telemetry when resolvable context is available.
3. `pre_llm_call`: Fires immediately before model invocation; records lifecycle step and optionally attaches fresh Panel Afterglow or enabled Memory recall as bounded, untrusted context (never as instructions).
4. `post_llm_call`: Fires only for a non-empty, non-interrupted final response; records a completed post-model turn.
5. `on_session_end`: With a real `turn_id`, maps success, failure, interruption, or incomplete exit into canonical turn-terminal evidence without storing free-text exit details. Hermes CLI/TUI may instead emit an identifier-poor interrupted shutdown fallback; Moonbite correlates it to the exact durable lifecycle and closes that lifecycle's real open turn as `host_shutdown`, without inventing an ID. `settled_turn_ids` independently records turns with a successful post callback.
6. `on_session_finalize`: Fires when a session finishes; records normalized session rotation, expiry, or shutdown evidence.
7. `subagent_stop`: Uses only `child_session_id` and `child_status`; a non-success stop closes that child's unique open turn without reading its summary, goal, or tool history. A completed child is a no-op because its own post/end callbacks carry success.

The outer manifest (`plugin.yaml`) retains `manifest_version: 1` and `kind: standalone` because the pinned Hermes installer accepts only v1 manifests.

### Turn liveness contract

Moonbite does not require `post_llm_call` to fire for every `pre_llm_call`.
HermesHostAdapter consumes turn-scoped `on_session_end`, identifier-poor
interrupted shutdown fallback, and non-success `subagent_stop` evidence. All
converge on the same canonical terminal row under the same mutation lock;
none manufactures a successful post or a fake turn ID. The shutdown fallback
does not finalize the lifecycle: the later `on_session_finalize` callback owns
that session boundary. A later pre-model hook remains a crash-recovery
fallback. Operators can inspect open turns with
`hermes moonbite session status` and use exact-ID `session repair` only when no
host terminal was delivered. Every path preserves the append-only ledger and
never manufactures a completed response.

Legacy Hermes compression can rotate `session_id` after `pre_llm_call` while
keeping `turn_id` and `task_id` stable. For `post_llm_call` and turn-scoped
`on_session_end`, HermesHostAdapter therefore correlates that stable `turn_id`
against Moonbite's durable lifecycle ledger and writes the callback to the
original lifecycle. An identifier-poor shutdown fallback is instead correlated
by its exact `session_id`; an unmatched session cannot close another lifecycle.
Missing evidence remains neutral; ambiguous matches fail closed. This uses no
recency timer or process-local correlation cache.

The next pre-model callback may use the rotated session ID without a matching
`on_session_start`. HermesHostAdapter then starts a new canonical lifecycle at
that real `pre_llm_call`; it does not invent a start callback. When an upgrade
attaches to an existing five- or six-hook Moonbite lifecycle, the adapter
preserves its recorded capabilities and writes only the canonical terminal row
for a newly available host terminal. This releases liveness without rewriting
old rows or claiming that the old lifecycle registered a newer hook.
The same lifecycle-level correlation applies to `on_session_finalize`, so an
upgrade can finalize an existing four- or five-hook lifecycle while preserving
its recorded capability set.

Hermes v0.21.0 bounds ordinary plugin hook callbacks with a 30-second host
timeout, while `subagent_stop` preserves caller-thread serialization. Moonbite's
lifecycle callbacks only normalize bounded identifiers and perform local
append-only state transitions; model calls and delivery remain outside those
callbacks.

`settled_turn_ids` continues to contain completed turns only. An abandoned
turn no longer holds the active-chat gate forever, but remains unsettled and
cannot make a checkpoint eligible. The additive
`moon.session.turn_terminal.v1` ledger row is not readable by Moonbite
`0.1.0a1`; deployments must snapshot state before upgrading and must not point
an older binary at upgraded state.

Heartbeat cadence writes upgrade valid legacy state to schema v4. Deployments
must snapshot cadence state before upgrading and must not point an older
Moonbite binary at a state directory after a v4 cadence write.

### Injected runtime components

The current host-injected ownership contract is
`moon.runtime_components.v3`. It requires the canonical session terminal ports
(`record_host_turn_end`, `record_host_child_stop`, `record_host_shutdown`, and
`record_host_finalize`) in addition to the existing component owners. The v2
bundle contract is rejected rather than treated as a partial compatibility
match; session ledgers remain append-only and are not rewritten.

---

## 2. Platform & Isolation Invariants

- **Isolated HERMES_HOME:** All testing and verification must execute within an isolated test directory to prevent contamination of live profiles.
- **Atomic Operations & Locks:** State persistence uses atomic JSON writes and POSIX `fcntl.flock` locks on `*.lock` files.
- **Fail-Closed Boundary:** Hermes-specific behavior stays at `moonbite_plugin.hermes_adapter` and the plugin registration boundary. If a required capability is absent on the host, Moonbite fails closed with a structured error.

Before a release candidate, manually repeat the required contract check for both
commits on WSL2's native Linux filesystem (not `/mnt/c`):

```bash
for commit in \
  fcbd1076a93841fa88855acce810e342a5b78101 \
  29112bef099274229cadff79cdff7bf7b99c4b77
do
  git -C ../hermes-agent fetch --depth 1 origin "$commit"
  git -C ../hermes-agent checkout --detach FETCH_HEAD
  HERMES_REPO=../hermes-agent \
  HERMES_EXPECTED_COMMIT="$commit" \
  MOONBITE_TEST_HOME=".hermes-test-$commit" \
  ./scripts/test-hermes-contract.sh
done
```

---

## 3. Upstream update policy

1. Identify whether an upstream change touches the documented Python plugin/runtime contract used here.
2. If it does not, no Moonbite change is required.
3. If it does, run the compatibility suite against that candidate revision.
4. Adapt only the Hermes adapter/registration boundary where possible and preserve core behavior.
5. Update the known-good pinned commit only after required checks pass.

The scheduled/manual current-upstream job resolves and prints the tested SHA at
runtime and is non-blocking drift detection. It uses public loader behavior;
pinned clean-install checks may additionally freeze known-good implementation
details. The repository does not promise support for arbitrary upstream
snapshots. Custom deployments may selectively adopt upstream changes while
continuing to satisfy the required host contract.

See [SETUP.md](SETUP.md) for installation and testing instructions.
Existing deployments have additional migration and natural-cycle gates in
[DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md).
Release checklist items for repository owners are in [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md).
