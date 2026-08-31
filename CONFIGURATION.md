# Configuration

Runtime validation in `moonbite_plugin.config` is authoritative. Moonbite reads
one mapping from:

```text
plugins.entries.moonbite.settings.config
```

Unknown keys, wrong types, incomplete role bindings, and inline host-route
details are errors. Defaults enable only `runtime_core`; every other module is
off. State defaults to `$HERMES_HOME/moonbite` (or `~/.hermes/moonbite`).

## Model task keys

The optional routing block is all-or-nothing:

```yaml
model_routes:
  schema_version: moon.model_route_bindings.v1
  main: {alias: moon_main}
  heartbeat: {alias: moon_support}
  hippocampus: {alias: moon_support}
```

Aliases are snake_case Hermes auxiliary task keys owned by this plugin. After
plugin discovery, configure their provider/model routes with `hermes model` or
the host's `auxiliary.<alias>` blocks. Sharing one key for heartbeat and
hippocampus is supported.

Never place provider, model, base URL, API key, context, pricing tier, or
fallback inside `model_routes`; Moonbite rejects those fields. The plugin does
not promise that a configured host route is online. Call failures remain
fail-closed and auditable.

## Modules and adapters

- `modules.heartbeat` enables Heartbeat candidates and its Judge call.
- `modules.autonomy` enables the weighted provider runtime.
- `modules.panel` activates the two registered chat-rhythm hooks and panel tools;
  the hooks are no-ops while disabled.
- `modules.memory` enables card/diary search, capture, exact history inspection,
  grounded diary synthesis, and operator maintenance commands.
- Local lexical search also uses a bounded literal-substring fallback for
  queries containing at least two Han, Hiragana, Katakana, or Hangul code
  points when token matching has no hit; single-character CJK queries return
  no results.
- `delivery.adapter: noop` is safe and cannot claim delivery.
- `delivery.adapter: hermes_session` can request a targeted main-session wake
  only when the host's `inject_message` surface explicitly supports a
  `session_key`. It needs a non-empty `target` and host-level
  `allow_gateway_injection: true`. A host without that extension returns
  `targeted_wake_adapter_unavailable`; it does not fall back to the current
  conversation. This is not a direct-message adapter.

## Heartbeat contact guards

`recent_contact` and `active_chat` remain enabled unless a heartbeat kind
explicitly lists them in `bypass`. Naming a profile `urgent` does not enable a
bypass by itself.

```yaml
heartbeat:
  kinds:
    urgent_signal:
      enabled: true
      profile: urgent
      judge: required
      host_only: true
      bypass: [recent_contact, active_chat]
```

Urgent policies may select either contact guard independently. The existing
`automatic_cooldown` and `manual_snooze` bypasses remain supported; unknown
values are rejected. A bypass only lets the candidate continue to later Judge
and policy evaluation. The bypass itself neither requests delivery or wake nor
counts as an effect receipt; any later effect follows the normal receipt
contract.

Moonbite v0.1 does not expose a third-party provider discovery contract.
Deployments register activity descriptors explicitly through a host adapter.
The bundled registry contains `local_reflection`, opt-in `model_reflection`,
and disabled host-fed `paper_browse` / `x_browse` examples. The two browse
examples perform no network or credential access; the host supplies verified
read-only candidates as facts. Missing, disabled, or ineligible providers do
not silently fall back.

`model_reflection` is the bundled, opt-in model activity and uses the `main`
task key. `synthesize_moonbite_diary` opens 1–20 exact card/diary references,
uses the `hippocampus` task key, and appends only a schema-valid grounded draft.
Writer errors are audited and do not append a diary row.

The memory lifecycle is append-only. Card history (`current`, `historical`,
`corrected`) is independent from lifecycle (`active`, `archived`); default
search and recall return only current, active cards. Operators may opt into
historical or archived search, and exact open can return a bounded relation
history. `memory.maintenance_enabled` defaults to `false`. When enabled,
maintenance is still proposal-first: `memory-maintenance-propose` records a
SHA-256-bound proposal, and `memory-maintenance-apply` requires an explicit
operator permission level (`safe` for merge, `reporting` for retire/archive,
or `manual` for distill). Moonbite writes an append-only receipt and never
deletes card or diary evidence; physical cleanup remains host-owned.

## Host-owned cron

Moonbite does not create or mutate cron jobs. A host cron can run a command such
as `hermes moonbite heartbeat <kind>` or ask the agent to invoke one Moonbite
tool. The host separately decides cadence, realtime versus discounted routing,
timeouts, and delivery. Disabling a Moonbite module makes its command/tool fail
visibly even if a stale cron still calls it.

## Change procedure

1. Back up the private host config by its normal process.
2. Change only the Moonbite entry and its host-owned auxiliary routes.
3. Run `hermes plugins doctor <checkout> --ci`.
4. Run `hermes moonbite doctor` in the target profile.
5. Exercise the intended command with an isolated or dry delivery adapter.
6. Inspect terminal audit/effect receipts; do not infer success from generated
   text.
7. Revert the namespaced config if validation or routing fails.

For an existing deployment, keep its scheduler, cadence ledger, descriptor
catalog, active-chat gate, and delivery receipts as the owners during
migration. Do not enable a parallel Moonbite store as a substitute. Follow
[DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md).

The complete inert fragment is [config/example.yaml](config/example.yaml); the
portable schema is [config/schema.json](config/schema.json). Four tested preset
fragments are provided under `config/presets/` (`core-only.yaml`, `panel-only.yaml`,
`memory-only.yaml`, `full-companion.yaml`). Established deployments should also
review [DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md).
See also the automation-safe installation protocol under `docs/` and [docs/features/PANEL.md](docs/features/PANEL.md).
