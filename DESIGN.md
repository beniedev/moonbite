# Design boundaries

Moonbite owns portable persistent-agent policy. Hermes owns plugin lifecycle, model
routing, credentials, cron, and gateway execution. A deployment owns private
data and platform integrations.

| Concern | Moonbite | Host |
|---|---|---|
| Events, controls, TTL, audit | contracts and local state | chooses storage root |
| Heartbeat/Autonomy policy | gate, Judge contract, terminal state | schedules calls |
| Model roles | three roles bound to auxiliary task aliases | provider, model, endpoint, tier, fallback |
| Main wake | typed port and accepted/verified distinction | targeted adapter, consent, receipt |
| Direct message | effect port only | platform adapter and target |
| Memory/panel | portable private-state format | retention, backups, user data |

## Dependency direction

```text
runtime-core + controls
  ├─ heartbeat ──> Judge port ──> WakeSink port
  ├─ autonomy  ──> Judge port ──> ActivityProvider port
  ├─ panel     <── sensor events / autonomy evidence
  └─ memory    <── explicit capture / diary / exact evidence refs

Hermes adapters ──> ctx.llm, auxiliary task registry, optional targeted injection
```

No optional module reads another module's private file. They communicate by
typed calls or event/evidence IDs.

## Terminal contracts

Heartbeat orders its ports as `ControlGate → cadence → Judge → effects`. A
control or cadence skip prevents the model call. A Judge exception is
`failed/judge_error`; a rejected delivery or wake is `failed/effect_failed`.
Generated text is never treated as delivered.

The bundled targeted-session adapter is capability-checked. If the host does
not expose a `session_key` injection parameter, Moonbite reports
`targeted_wake_adapter_unavailable` and does not inject into an arbitrary active
conversation. Queue acceptance remains `pending` until a deployment supplies a
real completion receipt.

Autonomy runs at most one provider per tick. A selected failure is terminal for
that tick. `play_next` is consumed only after a completed provider, so a failed
or unavailable requested activity remains inspectable and retryable.

Panel freshness is decided by typed expiry timestamps, not prose. Daily fields
roll at the configured local anchor; non-daily fields survive the epoch only
until their own TTL. Memory search returns pointers that must be opened before
being treated as evidence. Hot memory is changed only through SHA-bound
proposals; this package does not edit a host memory file directly.

Process locks suppress overlapping Heartbeat and Autonomy execution. They do
not guarantee exactly-once external side effects across process crashes.
Side-effecting activity and delivery adapters own idempotency keys and durable
receipts where required.

## Memory authority

The main agent remains the final authority. It can directly search the
local card/diary ledger with lexical matching, open exact `open_ref` evidence,
and inspect Panel state. Hippocampus is a bounded worker used only for grounded
diary synthesis from already opened evidence. Hermes-native session search is
host-owned and is not copied into Moonbite.

New cards use `moon.memory.card.v2`; legacy `moon.memory.v1` card rows remain
readable with projected defaults. The v2 record adds `event_time`, `entities`,
optional `state_key`, separate history and lifecycle states, one-way
`supersedes`, `supersession_kind`, and `related_cards`. `superseded_by` is a
derived read view, not a second persisted relationship.

A new card may supersede only an existing current, active card. Relations name
existing card IDs, and the mutation lock prevents concurrent successors from
forking one current state. Default search and recall expose only current,
active cards. Exact open remains available for every card;
`memory-open --history` and `open_moonbite_memory(include_history=true)` return
the bounded connected card history component.

Maintenance remains proposal-first. `memory-maintenance-propose` records
`merge`, `retire`, or `distill` proposals bound to canonical evidence hashes.
The operator-only `memory-maintenance-apply` CLI applies a reviewed proposal as
one append-only `moon.memory.history.v1` receipt. Merge requires `safe`
permission, retire/archive requires `reporting`, and distill requires `manual`;
there is no model-facing apply tool.

Merge creates one evidence-backed superseding card, distill creates one
evidence-backed related card, and retire projects target cards as archived.
Original cards and diary evidence are never overwritten or physically deleted.
Moonbite exposes no delete/purge operation; physical cleanup belongs to a
separate host-owned, explicitly authorized and recoverable workflow.

Moonbite ships no vector index or generic RAG layer. A deployment may add a
vector retriever behind the Hippocampus boundary while preserving exact
`open_ref` evidence. Panel injects only a fresh, bounded Autonomy afterglow
through ephemeral user-message context; it does not inject the whole Panel or
alter the system prompt.

An `open_ref` is an exact Moonbite-local card or diary reference. A card's
`source_ref` is only an opaque host attribution pointer; when supplied by a
model-facing tool it is not automatically host-verified evidence. Verification
against raw session history remains host-owned.

## Routing and scheduling

`moon.model_route_bindings.v1` contains only the `main`, `heartbeat`, and
`hippocampus` roles and their auxiliary task aliases. Moonbite registers the
unique aliases through Hermes. Optional model reflection uses the `main` role,
bounded Heartbeat/Autonomy Judges use `heartbeat`, and evidence-grounded diary
synthesis uses `hippocampus`. Multiple roles may share one alias. Moonbite
neither knows nor repairs the route behind an alias. Provider fallback, context
limits, and discounted tiers remain observable host decisions.

Moonbite deliberately has no scheduler switch. An unused "enabled" flag would
be false control. Hosts schedule the CLI/tools they want and choose separate
routes or tiers for realtime and long-running jobs.

An established deployment remains its own migration oracle. Its active-chat
gate, descriptor catalog, continuity hooks, and delivery path stay host-owned;
see [DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md).
