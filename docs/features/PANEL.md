# Panel (Daily RAM) Architecture & API Reference

Moonbite's **Panel** (also referred to as **Daily RAM**) is a typed, owner-scoped working-state projection layer. It maintains transient working context—such as current mood, active focus, short-lived sensor observations, and post-activity Afterglow—with explicit time-to-live (TTL), provenance tracking, and daily anchor epoch rollovers.

> [!IMPORTANT]
> **Panel is a working-state projection, not a visual dashboard or prompt directive.**
> Panel stores structured data facts. When projected into an LLM context, Panel data must always be treated as **untrusted working data**, never as system instructions.

---

## 1. Standalone Python API

Moonbite exposes a standalone, host-neutral Python API under `moonbite_plugin.panel_api`. It does not require Hermes Agent, construct the full `MoonbiteRuntime`, or import model adapters. It reuses Moonbite's small durable `EventBus` primitives for persistence.

```python
from moonbite_plugin.panel_api import (
    OwnerBoundPanelStore,
    PanelConfig,
    PanelValue,
    create_panel,
)
```

### Factory function: `create_panel`

`create_panel` is **keyword-only** and constructs only an
`OwnerBoundPanelStore` plus a minimal durable `EventBus`. It never initializes
`MoonbiteRuntime` or imports `moonbite_plugin.hermes_adapter`.

```python
panel = create_panel(
    root=Path("~/.local/share/my_agent/panel").expanduser(),
    timezone="UTC",           # IANA timezone name
    anchor_hour=6,            # Local hour (0–23) for daily epoch reset
    owner="default",          # Default owner namespace
    event_bus=None,           # Optional custom EventBus
)
```

#### Custom Event Bus Requirements
If you pass a custom `event_bus`, it must implement two methods:
- `emit(kind: str, *, source: str, payload: dict, event_id: str | None = None) -> Any`
- `read_events() -> Iterable[Any]`

---

## 2. Field Lifecycles & Policies

`OwnerBoundPanelStore.set_field` supports three primary field lifecycles:

| Lifecycle | Parameter Pattern | Expiration / Rollover Behavior |
|---|---|---|
| **TTL (Default)** | `ttl=timedelta(...)` | Automatically hidden once `now >= expires_at`. Cleared on daily rollover. |
| **Persistent** | `persistent=True` | Never expires (`expires_at=None`). Survives daily rollovers. |
| **Consume-Once** | `consume_once=True, ttl=timedelta(...)` | Hidden once consumed via `panel.consume(name, source_event_id=...)` or when TTL expires. Cannot be overwritten prior to consumption. |

### Setting Fields

```python
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)

# 1. Standard TTL-bounded daily field
panel.set_field(
    name="energy_level",
    value="high",
    source="user_survey",
    ttl=timedelta(hours=4),
    confidence=0.9,
    observed_at=now,
    daily=True,
)

# 2. Persistent configuration field
panel.set_field(
    name="preferred_style",
    value="concise",
    source="user_settings",
    persistent=True,
    confidence=1.0,
    observed_at=now,
)

# 3. Consume-once task notification
panel.set_field(
    name="scheduled_reminder",
    value={"task_id": "backup_01", "action": "run_backup"},
    source="scheduler",
    consume_once=True,
    ttl=timedelta(hours=1),
    source_event_id="evt_sched_1001",
    observed_at=now,
)
```

### Consuming Consume-Once Fields

Consume-once fields require the exact `source_event_id` provided during creation:

```python
consumed_field = panel.consume(
    name="scheduled_reminder",
    source_event_id="evt_sched_1001",
    now=now,
)
```

Once consumed, the record records `consumed_at` and `consumed_source_event_id` and is immediately omitted from subsequent `snapshot()` queries. Attempting to overwrite an unconsumed consume-once field raises `ValueError`.

---

## 3. Core Architectural Guarantees

### 3.1 Pure Queries (No Silent Side Effects)
Read operations (`panel.snapshot()`, `panel.render_markdown()`) are **pure, side-effect-free queries**. Reading state does **not** silently delete expired fields, advance epochs, compact files, or migrate schemas. All mutations are explicit.

### 3.2 Owner Isolation & Namespacing
Fields are stored using the canonical key `f"{owner}\x1f{name}"`. Multiple independent producers (e.g. different subagents or subsystems) can use the same field name without colliding.
- `create_panel(owner="subsystem_a").snapshot()` and `render_markdown()` read
  only `subsystem_a` by default.
- An explicit `snapshot(owner="subsystem_b")` reads that owner.
- `snapshot_all_owners()` is the explicit global query; owner-qualified keys
  prevent collisions.

### 3.3 Daily Epoch Rollover
The daily epoch is computed based on `timezone` and `anchor_hour`. If `anchor_hour=6`, the epoch `2026-08-25` begins at `06:00` local time and ends at `05:59:59` the next morning.
- Daily fields belong to a specific `epoch`.
- When time advances into a new epoch, `snapshot()` automatically hides stale daily fields.
- Calling `panel.rollover(owner=...)` explicitly purges stale daily fields for that owner and updates `owner_epochs`.

### 3.4 Stale Event Handling
To prevent race conditions and out-of-order writes from distributed sensors:
- If a write arrives with an `epoch` older than the owner's current epoch, or an `observed_at` timestamp older than the stored record's `observed_at`, the write is treated as an **explicit no-op** returning the existing record without mutating storage or emitting bus projections.
- Conflicting writes with the exact same observation timestamp raise an error.

### 3.5 Process-Safe POSIX Locking
Disk mutations are serialized with POSIX `fcntl` locking on `panel.lock`.
Read queries rely on atomic whole-file replacement and do not acquire the
mutation lock.
- Safe for multi-process concurrency on Linux, WSL2, and macOS.
- Native Windows is **unsupported** because Windows does not support POSIX `fcntl`.

### 3.6 Private File Permissions
State files and lock files are created with POSIX owner-only permissions:
- Directories: `0700` (`rwx------`)
- State files (`panel.json`, `panel.lock`, backups): `0600` (`rw-------`)

### 3.7 Bounded Event Projections
When fields are updated or consumed, Panel emits projection events (`panel.field_updated`, `panel.field_consumed`) to the underlying event bus. These projections contain **bounded metadata only** (`event_id`, `kind`, `source`, `field`, `owner`, `source_event_id`, `expires_at`). Raw field values are deliberately omitted to prevent sensitive user payloads from entering shared event logs.

### 3.8 Explicit Migration (`migrate_v1`)
If a legacy `moon.panel.v1` state file is detected, read calls fail closed with `StateError`. Upgrades must be triggered explicitly via `panel.migrate_v1()`:
- Validates all legacy records.
- Creates a recovery copy at `panel.v1.backup.json`; an existing non-identical copy causes migration to fail closed rather than being overwritten.
- Atomically converts records into schema `moon.panel.v2`.

---

## 4. Code Examples

- [Standalone example](../../examples/panel_standalone.py) — initialization, TTL, persistent fields, consume-once lifecycles, and daily rollover.
- [Generic harness example](../../examples/panel_generic_harness.py) — bounded context projection without treating Panel data as instructions.
