"""Validated runtime component bundles for standalone and host-injected use."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .control import ControlStore
from .effects import EffectLedger
from .heartbeat import HeartbeatCadence
from .memory import MemoryStore
from .panel import PanelStore
from .runtime_core import EventBus, FileRuntimeLocks, RuntimeLocks, ensure_bounded_text
from .session import SessionLifecycleStore

RUNTIME_COMPONENTS_SCHEMA = "moon.runtime_components.v2"
REQUIRED_STATE_DOMAINS = frozenset(
    {
        "event",
        "audit",
        "control",
        "cadence",
        "panel",
        "memory",
        "session",
        "effect",
    }
)
# MB-45 left these two domains reserved while their owners were being built.
# The combined MB-46/MB-48 bundle owns all eight domains concretely now.
RESERVED_STATE_DOMAINS = frozenset()
MAX_OWNER_ID_BYTES = 256
STANDALONE_OWNER_ID = "moonbite-standalone"
SESSION_OWNER_METHODS = (
    "record_hook",
    "record_host_turn_end",
    "record_host_child_stop",
    "record_host_shutdown",
    "record_host_finalize",
    "snapshot",
    "replay",
)
EFFECT_OWNER_METHODS = (
    "begin_intent",
    "mark_pending",
    "mark_queue_accepted",
    "verify",
    "fail",
    "expire",
    "requeue",
    "get",
    "find_by_idempotency",
    "records",
    "pending_for_reconciliation",
)


class RuntimeComponentsError(ValueError):
    """Raised when a runtime component bundle violates its ownership contract."""


@dataclass(frozen=True)
class RuntimeComponents:
    """All stateful components used by one Moonbite runtime owner.

    The bundle is deliberately a wiring and ownership contract.  Validation
    only inspects values and object identities; it does not touch the state
    root or probe any backend.
    """

    schema_version: str
    owner_id: str
    mode: str
    owned_domains: frozenset[str]
    reserved_domains: frozenset[str]
    writer_count: int
    local_writes: bool
    bus: EventBus
    controls: ControlStore
    cadence: HeartbeatCadence
    panel: PanelStore
    memory: MemoryStore
    session: Any
    effects: Any
    locks: RuntimeLocks
    state_root: Path | None = None

    @property
    def session_store(self) -> Any:
        """Compatibility alias for the injected session lifecycle owner."""

        return self.session

    @property
    def effect_ledger(self) -> Any:
        """Compatibility alias for the injected effect owner."""

        return self.effects

    @property
    def effect(self) -> Any:
        """Short alias matching the effect state-domain name."""

        return self.effects

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate the bundle without probing or mutating any component."""

        if (
            type(self.schema_version) is not str
            or self.schema_version != RUNTIME_COMPONENTS_SCHEMA
        ):
            raise RuntimeComponentsError(
                f"unsupported runtime components schema: {self.schema_version!r}"
            )
        if type(self.mode) is not str or self.mode not in {"standalone", "injected"}:
            raise RuntimeComponentsError(
                f"unsupported runtime components mode: {self.mode!r}"
            )
        if self.owned_domains != REQUIRED_STATE_DOMAINS:
            raise RuntimeComponentsError(
                f"owned state domains must be {sorted(REQUIRED_STATE_DOMAINS)!r}"
            )
        if self.reserved_domains != RESERVED_STATE_DOMAINS:
            raise RuntimeComponentsError(
                f"reserved state domains must be {sorted(RESERVED_STATE_DOMAINS)!r}"
            )
        if not self.reserved_domains <= self.owned_domains:
            raise RuntimeComponentsError(
                "reserved state domains must be a subset of owned state domains"
            )
        if type(self.owner_id) is not str or not self.owner_id.strip():
            raise RuntimeComponentsError("owner_id must be a non-empty string")
        try:
            ensure_bounded_text(
                self.owner_id,
                "owner_id",
                max_bytes=MAX_OWNER_ID_BYTES,
            )
        except ValueError as exc:
            raise RuntimeComponentsError(str(exc)) from exc
        if type(self.writer_count) is not int or self.writer_count != 1:
            raise RuntimeComponentsError("writer_count must be exactly 1")
        if type(self.local_writes) is not bool:
            raise RuntimeComponentsError("local_writes must be a boolean")
        if self.local_writes is not (self.mode == "standalone"):
            raise RuntimeComponentsError(
                "local_writes must be true only for standalone runtimes"
            )

        required_components = {
            "bus": self.bus,
            "controls": self.controls,
            "cadence": self.cadence,
            "panel": self.panel,
            "memory": self.memory,
            "session": self.session,
            "effects": self.effects,
            "locks": self.locks,
        }
        missing = [
            name for name, component in required_components.items() if component is None
        ]
        if missing:
            raise RuntimeComponentsError(
                f"runtime components may not be None: {', '.join(missing)}"
            )
        missing_session_methods = [
            name
            for name in SESSION_OWNER_METHODS
            if not callable(getattr(self.session, name, None))
        ]
        if missing_session_methods:
            raise RuntimeComponentsError(
                "session owner is missing callable methods: "
                + ", ".join(missing_session_methods)
            )
        missing_effect_methods = [
            name
            for name in EFFECT_OWNER_METHODS
            if not callable(getattr(self.effects, name, None))
        ]
        if missing_effect_methods:
            raise RuntimeComponentsError(
                "effects owner is missing callable methods: "
                + ", ".join(missing_effect_methods)
            )
        if not callable(getattr(self.locks, "exclusive", None)):
            raise RuntimeComponentsError("locks.exclusive must be callable")
        if not callable(getattr(self.locks, "try_exclusive", None)):
            raise RuntimeComponentsError("locks.try_exclusive must be callable")

        panel_bus_missing = object()
        panel_bus = getattr(self.panel, "bus", panel_bus_missing)
        if panel_bus is not panel_bus_missing and panel_bus is not self.bus:
            raise RuntimeComponentsError("panel.bus must be the bundle bus")

        if self.mode == "standalone":
            if not isinstance(self.state_root, Path):
                raise RuntimeComponentsError(
                    "standalone runtime components require a Path state_root"
                )
        elif self.state_root is not None and not isinstance(self.state_root, Path):
            raise RuntimeComponentsError("injected state_root must be a Path or None")

    @classmethod
    def standalone(
        cls,
        root: Path,
        timezone_name: str,
        anchor_hour: int,
    ) -> RuntimeComponents:
        """Construct the local, single-writer component bundle."""

        if not isinstance(root, Path):
            raise RuntimeComponentsError("standalone root must be a Path")
        bus = EventBus(root)
        controls = ControlStore(root)
        cadence = HeartbeatCadence(
            root, timezone_name=timezone_name, anchor_hour=anchor_hour
        )
        panel = PanelStore(
            root,
            bus=bus,
            timezone_name=timezone_name,
            anchor_hour=anchor_hour,
        )
        memory = MemoryStore(root)
        session = SessionLifecycleStore(root)
        effects = EffectLedger(root)
        locks = FileRuntimeLocks(root)
        return cls(
            schema_version=RUNTIME_COMPONENTS_SCHEMA,
            owner_id=STANDALONE_OWNER_ID,
            mode="standalone",
            owned_domains=REQUIRED_STATE_DOMAINS,
            reserved_domains=RESERVED_STATE_DOMAINS,
            writer_count=1,
            local_writes=True,
            bus=bus,
            controls=controls,
            cadence=cadence,
            panel=panel,
            memory=memory,
            session=session,
            effects=effects,
            locks=locks,
            state_root=root,
        )

    @classmethod
    def injected(
        cls,
        owner_id: str,
        bus: Any,
        controls: Any,
        cadence: Any,
        panel: Any,
        memory: Any,
        locks: RuntimeLocks,
        session: Any = None,
        effects: Any = None,
        state_root: Path | None = None,
        *,
        session_store: Any = None,
        effect_ledger: Any = None,
        effect: Any = None,
    ) -> RuntimeComponents:
        """Wrap host-owned components without constructing local state."""

        if session is None:
            session = session_store
        elif session_store is not None and session_store is not session:
            raise RuntimeComponentsError("conflicting session owner aliases")
        if effects is None:
            effects = effect_ledger if effect_ledger is not None else effect
        elif (effect_ledger is not None and effect_ledger is not effects) or (
            effect is not None and effect is not effects
        ):
            raise RuntimeComponentsError("conflicting effect owner aliases")

        return cls(
            schema_version=RUNTIME_COMPONENTS_SCHEMA,
            owner_id=owner_id,
            mode="injected",
            owned_domains=REQUIRED_STATE_DOMAINS,
            reserved_domains=RESERVED_STATE_DOMAINS,
            writer_count=1,
            local_writes=False,
            bus=bus,
            controls=controls,
            cadence=cadence,
            panel=panel,
            memory=memory,
            session=session,
            effects=effects,
            locks=locks,
            state_root=state_root,
        )
