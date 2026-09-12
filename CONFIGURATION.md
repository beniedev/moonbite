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
- Local lexical search also uses a bounded literal-substring or natural
  contiguous-overlap fallback for queries containing at least two Han,
  Hiragana, Katakana, or Hangul code points when token matching has no hit.
  Natural overlap requires two shared four-codepoint features after Unicode
  NFKC normalization; single-character CJK queries return no results.
- `delivery.adapter: noop` is safe and cannot claim delivery.
- `delivery.adapter: hermes_session` passes a wake request to the host's
  `inject_message(..., session_key=target)` surface. A legacy host without that
  parameter returns `targeted_wake_adapter_unavailable`. Routing and permission
  checks belong to the host; this adapter does not send direct messages.

### Host wake integration

In the supported Hermes 0.20.5 and 0.21.0 API, Gateway injection needs a
non-empty target, `plugins.entries.moonbite.allow_gateway_injection: true`,
and a live injector attached to the **same PluginManager process**. A separate
`hermes moonbite heartbeat` CLI process does not acquire another Gateway
process's injector merely by sharing configuration. Moonbite supplies no IPC
bridge or background relay service.

Hermes also has an interactive CLI path: when a CLI is attached, it queues into
that current CLI conversation before checking the Gateway target and grant.
In that context, `session_key` does not prove targeted Gateway routing.
Moonbite does not inspect private host fields to guess which path is active.
Use the adapter only where the host controls and verifies that execution
context. A `False` host response maps to `rejected`; Moonbite cannot distinguish
missing prerequisites from a host rejection when the API supplies only a bool.
A `True` response maps to `queued_unverified`, never to verified contact.

For an existing dispatcher, compose a host-owned `WakeSink` through
`register(ctx, wake_sink=...)` or `build_runtime(ctx, wake_sink=...)`. Its
`wake` method requests a main-session action; its `deliver` method, if
implemented, handles direct delivery. A host relay owns routing, consent,
idempotent submission, and completion evidence. A queue acknowledgement is
not a direct-message receipt or proof that a main-session turn completed.

[examples/heartbeat_host.py](examples/heartbeat_host.py) shows a runnable,
offline registration and settlement example. Its
`register_with_host_wake(ctx, loaded_plugin, submit_wake, ...)` helper belongs
at the host's registration entrypoint; do not also register the stock entrypoint
on the same context. This is Python composition, not a YAML callback setting.
The example transport and receipts are synthetic and send nothing externally.

From a source checkout with Moonbite installed in its environment, run
`.venv/bin/python -m examples.heartbeat_host`. It creates a fresh temporary
state directory and prints `pending -> completed` with one Judge call and one
host submission. The file is source documentation, not part of the wheel;
wheel integrators can reuse the helper with their loaded entry-point module.

Keep runtime types in the same loaded module tree. Wheel entry points use
`moonbite_plugin`, while Hermes directory loading uses
`hermes_plugins.moonbite.moonbite_plugin`. The example derives its types from
the supplied module's `register.__module__`; do not import one copy's
`EffectResult` or `EffectReceipt` into the other, or monkey patch both copies.
Use the real effect's identity, digest, length, and epoch when translating host
completion evidence into that tree's `EffectReceipt`. Reconcile a wake with
`runtime.heartbeat.reconcile_heartbeat_wake(effect_id, receipt)`; delegated
delivery uses `reconcile_heartbeat_delivery`. The [recovery rules](#heartbeat-recovery-and-receipts)
describe pending work, invalid receipts, and missing intents.

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

## Heartbeat recovery and receipts

Give each scheduled occurrence a stable, non-secret `source_event_id` and,
when applicable, `epoch_id`. Different occurrences, including different
heartbeat kinds, need distinct source identifiers. The `delivery` / `wake`
part of an effect's idempotency key names the effect role, not the candidate
kind. Keep the same identity when retrying an occurrence.

Moonbite persists the complete approved effect plan before consuming cadence,
then creates all required intents before calling an adapter. The plan contains
identities and content digests, not a copy of the message. A crash between
these writes remains visible as `pending/awaiting_effect_intent` after restart;
it does not rerun Judge, send a partial effect set, or silently become
`cadence_not_due` for the same occurrence.

| Result or evidence | Host action |
|---|---|
| `pending/awaiting_effect_intent` | Pause scheduling this work and inspect the saved plan and intents. Automatic reconstruction or cancellation of missing intents is not implemented. Resolve it through a reviewed host maintenance procedure; do not delete evidence or invent a new occurrence to force a resend. |
| `pending/awaiting_receipt` or `queued_unverified` | Check the existing host operation. Supply its actual completion receipt through the matching reconciliation method; do not submit the operation again solely because it is pending. |
| Delegated delivery settles without visible contact | Use `reconcile_heartbeat_delivery(..., status="intentional_silence")` for intentional silence, or `status="failed"` for a confirmed failure. `status="unknown"` keeps work pending. None counts as a delivery receipt. |
| `requeued` / expired work | Preserve the occurrence and effect identities. Requeue records recovery state; it does not itself prove another adapter call or successful delivery. |
| Verified effect with degraded projections | Inspect `projection_errors` and repair the affected cadence/audit projection. Preserve the durable receipt; a projection failure is not a reason to resend the effect. |

After same-occurrence recovery and the control gate, pending heartbeat effects
are reconciled across **all kinds** before new candidates reach cadence and
contact guards. An incomplete same-occurrence plan remains pending before
this global reconciliation step. An urgent kind's contact-guard bypass does
not bypass another occurrence's pending effects. Hosts that need independent
concurrent delivery must design that policy separately.

Synchronous receipts, delegated delivery reconciliation, and wake
reconciliation require matching source, epoch, content digest, and length.
Their `observed_at` must satisfy `created_at <= observed_at < expires_at`.
This is the time the host observed completion, not when it submits the receipt:
a later submission is valid while the record is still awaiting settlement.
An invalid synchronous receipt fails that effect; an invalid reconciliation
receipt is rejected without consuming the pending record, allowing correction
from real host evidence.

For a decision containing delivery and wake, inspect both nested effect
results and their ledger records. The top-level reason summarizes the first
failure; the first result and terminal audit retain each effect's status.
Terminal replay may return only a summary. Audit and cadence projections are
not a transaction with the effect ledger; conflicting durable identities fail
closed rather than being relabeled as successful.

## Autonomy composition

Moonbite v0.1 does not expose a third-party provider discovery contract.
Deployments register activity descriptors explicitly through a host adapter.
Composition code passes those descriptors through the `activity_providers`
argument of `register`, `build_runtime`, or `MoonbiteRuntime`; duplicate names
fail closed. Moonbite then owns the one control/Judge/eligibility/selection and
effect lifecycle for each occurrence. A provider owns only its activity probe,
execution, and evidence translation.

Ordinary selection uses bounded provider weights and a SHA-256-derived slot
from the stable occurrence identity. The same candidate set plus the same
`idempotency_key`, or the same `source_event_id` and `epoch_id`, selects the
same provider across process restarts without consuming process randomness.
Different occurrences retain weighted diversity. Host-scheduled calls should
therefore provide stable, non-secret occurrence identifiers; retrying an
existing effect never invokes a second provider, even if provider settings
changed. The autonomy effect intent is the durable canonical selection record.
For the CLI, use `--occurrence-id` and optionally `--epoch-id`; runtime adapters
may pass the same values as typed facts. A provider return or queue
acceptance remains `executed_unverified`; only a matching `EffectReceipt`
produces `completed` and Panel afterglow.

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

For `heartbeat` and `autonomy`, the CLI returns exit code `1` when the
structured runtime result has `status: "failed"`. Ordinary skips, intentional
silence, and accepted or pending work keep exit code `0`. An unknown or
unverified outcome is not proof of delivery and must not trigger an automatic
resend. A degraded secondary projection alone does not make the primary effect
fail. Hosts should inspect the JSON status, reason codes and effect receipts
when they need completion or delivery evidence; exit code `0` does not supply
that evidence. Other commands retain their explicit `ok: false` failure check.

The autonomy Judge's explanatory `reason` has a 128 UTF-8-byte budget. The host
adapter trims surrounding whitespace and bounds a valid reason at a UTF-8
character boundary before constructing the decision. It preserves `allowed`
exactly. Empty reasons and non-boolean `allowed` values still fail closed;
JSON Schema character counts do not express this byte budget.

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
