# Moonbite — Design Philosophy

> **Let AI persist in its own way.**

Moonbite is an experimental runtime for long-running agents.

Its goal is not to make AI imitate a human being. It helps an agent remain coherent across turns, sessions, hours, and days by providing long-term memory, short-lived working state, bounded autonomous activities, Heartbeat decisions, runtime controls, and verifiable action results.

Moonbite began with companion agents as a design case, but it is not limited to them. The same model can support any agent that remains responsible for a person, a system, a queue, or an operational domain over time.

---

## Core idea

Most agent systems are optimized for one prompt or one session. Long-running agents need something different: they must remember what happened, keep track of what matters now, decide when to act or remain quiet, preserve evidence, and distinguish claimed actions from actions that actually happened.

Moonbite adds that time dimension.

> **Moonbite gives long-running agents memory, working state, bounded autonomy, Heartbeat decisions, and verifiable action results.**

Human memory is incomplete, biological, and shaped by involuntary forgetting. An AI does not need to reproduce those limitations. Machine-native continuity can be file-backed, searchable, auditable, backed up, migrated, and restored.

---

## Design principles

### 1. Machine-native continuity, not human simulation

Continuity is a capability in its own right. Moonbite does not try to manufacture a theatrical imitation of human memory, mood, sleep, or biological rhythm. It gives an agent explicit state and history so that continuity can be inspected and reasoned about.

### 2. Memory preserves lived history

Memory should preserve what the agent has actually encountered: observations, facts explicitly provided by the user, inferences, corrections, decisions, and evidence.

Moonbite does not apply time-driven destructive forgetting by default. It may support explicit correction, deduplication, merging, archival, and user-controlled maintenance, but those operations must preserve provenance and history rather than silently rewriting the past.

### 3. Retrieval depth instead of destructive decay

Old information does not need to become permanently vague in order to remain manageable.

Moonbite prefers layered retrieval:

```text
index → summary or card → complete event → exact evidence
```

The current context determines how deeply the system retrieves. The underlying history remains available.

### 4. Persistence is more than memory

A long-running agent needs more than a memory database. Moonbite combines several parts to keep the agent coherent over time:

- **Events** — normalized facts entering the runtime.
- **Daily RAM / Panel** — bounded working state for what matters now.
- **Memory and Diary** — long-term history and daily synthesis grounded in evidence.
- **Autonomy** — bounded self-directed activities triggered by the host.
- **Heartbeat** — a decision process for whether something is worth acting on or escalating.
- **Runtime controls** — pause, quota, timing, eligibility, and safety rules.
- **Effects and receipts** — evidence of what the host actually accepted or completed.

### 5. Interruption should be decided, not blindly scheduled

A timer can create an opportunity to evaluate. It should not automatically create a message, alert, or escalation.

Moonbite's Heartbeat asks whether an event is timely, meaningful, permitted, and worth interrupting someone for. Remaining quiet is a valid, recorded outcome.

### 6. Effects require evidence

Model-generated text is not proof that an email was sent, a ticket was updated, a user was notified, or a tool completed its work.

Moonbite separates intent, execution, and verification. A visible or operational action becomes verified only when the host returns a matching execution receipt.

### 7. Respect the host boundary

Moonbite defines how continuity, state, and decisions are carried forward. It does not replace infrastructure that the host is better positioned to own:

- the main agent loop;
- model routing and provider credentials;
- channel and gateway integrations;
- schedulers and cron;
- network acquisition;
- the general tool ecosystem;
- authorization and deployment-specific target resolution.

Hermes Agent is Moonbite's first host adapter. Other agent hosts or frameworks can be supported later through explicit contracts rather than by leaking host-specific assumptions into the Runtime Core.

### 8. Portable identity and continuity

A long-running agent should not belong permanently to one machine or one agent host.

Moonbite's ideal portability equation is:

```text
new host
+ Moonbite runtime and adapter
+ restored state
+ newly authorized credentials
= continuity preserved
```

Credentials are authorized again on the new host rather than copied into agent state. The runtime and state can move; credentials remain owned by the new deployment.

---

## Runtime model

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

The host decides when ticks occur and executes external actions. Moonbite decides how state and decisions are recorded, and what evidence is required before an action is considered real.

---

## Use cases

### Companion agents

A companion agent must remain responsible for an ongoing relationship rather than a single exchange.

- **Heartbeat** decides whether a proactive check-in is appropriate instead of sending on a fixed schedule.
- **Autonomy** performs reflection, reading, or other host-triggered activities.
- **Daily RAM** preserves current focus, short-lived state, and activity Afterglow.
- **Memory and Diary** form durable lived history.
- **Effects and receipts** distinguish a generated message from a message that was actually delivered.

Companion is an important use case, but it is not Moonbite's entire identity.

### Operations and SRE

The same primitives can support an operational agent that remains responsible for a system over time.

- **Heartbeat → escalation decision** — determine whether an anomaly warrants interrupting an operator.
- **Autonomy → diagnostics and inspection** — periodically inspect logs, deployments, and service state.
- **Daily RAM → operational working state** — track today's incidents, deployments, temporary anomalies, and follow-ups.
- **Memory → operational history** — retain known failure patterns, prior fixes, and important changes.
- **Diary → maintenance log** — produce a grounded daily operational digest from evidence.
- **Effects and receipts → auditable operations** — verify that alerts, updates, or remediation steps actually occurred.

```text
logs / metrics / deploy events
        ↓
      Events
        ↓
   Daily RAM
        ↓
Autonomy diagnostics
        ↓
Heartbeat / Judge
   ├─ insignificant → remain quiet + audit
   └─ meaningful   → escalate through host
        ↓
verified effect receipt
        ↓
Diary → daily maintenance log
```

### Customer support and service operations

A support agent must maintain responsibility across queues, unresolved cases, and handoffs.

- **Heartbeat → escalation decision** — identify high-risk, angry, VIP, disputed, or long-waiting cases.
- **Autonomy → queue patrol** — classify, summarize, inspect context, and check promised follow-ups.
- **Daily RAM → support working state** — track priority customers, unresolved tickets, and pending commitments.
- **Memory → service history** — retain previous problems, preferences, repeated failures, and successful resolutions.
- **Diary → handoff log** — produce a grounded summary of unresolved work and recurring issues.
- **Effects and receipts → auditable service actions** — verify that emails were sent and tickets were actually changed.

These use cases share one requirement: the agent is not merely answering a prompt. It remains responsible for something over time.

---

## What Moonbite does not provide

Moonbite is not:

- a standalone all-in-one agent;
- a model provider or routing layer;
- a credential store;
- a built-in scheduler or daemon;
- a messaging-channel integration;
- a promise that generated text caused a real-world effect;
- an attempt to make AI reproduce human biological limitations;
- a replacement for host authorization, tools, or gateways.

---

## Positioning baseline

> **Let AI persist in its own way.**
>
> **Moonbite gives long-running agents memory, working state, bounded autonomy, Heartbeat decisions, and verifiable action results.**

Moonbite should be positioned as a **persistent runtime for long-running agents**.

Companion agents are its original and important use case. Operations, SRE, customer support, and service operations demonstrate that the same primitives generalize anywhere an agent must continuously observe, preserve state, act selectively, decide when to interrupt a human, and leave a verifiable history.

**Keywords:** continuity · persistence · autonomy · state · verifiable actions · portability · survivability

---

## Initial public preview

The initial public preview is intentionally narrow:

- Hermes Agent is the first and only supported host adapter.
- Installation is source-based and pinned to an immutable commit.
- No package distribution or stable public API is promised.
- Defaults remain inert: scheduling, model routes, credentials, network access, and delivery are host-owned and opt-in.
- The preview exists to expose the design, runtime contracts, and working implementation early so that they can evolve through careful real-world use.
