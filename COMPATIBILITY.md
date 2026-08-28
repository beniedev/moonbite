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
| Hermes Baseline | `987064caa4f8845f605ac7346fed5b72fddfb21c` | Exact recorded known-good commit; required CI validation job. |
| Current Upstream Hermes | Dynamic | Resolved and printed by scheduled/manual drift CI. See the latest workflow log for the tested SHA. |

---

## 1. Verified Surface & Lifecycle Hooks

Moonbite registers exactly 10 tools, 15 CLI commands, 1 slash command (`/moon`), and 5 manifest hooks.

The 5 manifest hooks execute in strict `HOOK_ORDER`:
1. `pre_gateway_dispatch`: Fires before message authorization, providing a pre-auth host context seam. Without an explicit typed host resolver, Moonbite treats this as a no-op and never reads or records unauthorized message content.
2. `on_session_start`: Fires when a session initializes; records normalized session-start lifecycle telemetry when resolvable context is available.
3. `pre_llm_call`: Fires immediately before model invocation; records lifecycle step and optionally attaches fresh Panel Afterglow or enabled Memory recall as bounded, untrusted context (never as instructions).
4. `post_llm_call`: Fires after model generation; records settled post-model lifecycle state.
5. `on_session_finalize`: Fires when a session finishes; records normalized session finalization.

The outer manifest (`plugin.yaml`) retains `manifest_version: 1` and `kind: standalone` because the pinned Hermes installer accepts only v1 manifests.

---

## 2. Platform & Isolation Invariants

- **Isolated HERMES_HOME:** All testing and verification must execute within an isolated test directory to prevent contamination of live profiles.
- **Atomic Operations & Locks:** State persistence uses atomic JSON writes and POSIX `fcntl.flock` locks on `*.lock` files.
- **Fail-Closed Boundary:** Hermes-specific behavior stays at `moonbite_plugin.hermes_adapter` and the plugin registration boundary. If a required capability is absent on the host, Moonbite fails closed with a structured error.

Before a release candidate, manually repeat the pinned contract check on WSL2's native Linux filesystem (not `/mnt/c`):

```bash
git -C ../hermes-agent fetch --depth 1 origin 987064caa4f8845f605ac7346fed5b72fddfb21c
git -C ../hermes-agent checkout --detach 987064caa4f8845f605ac7346fed5b72fddfb21c
HERMES_REPO=../hermes-agent \
HERMES_EXPECTED_COMMIT=987064caa4f8845f605ac7346fed5b72fddfb21c \
MOONBITE_TEST_HOME=.hermes-test ./scripts/test-hermes-contract.sh
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
