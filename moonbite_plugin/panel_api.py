"""Stable, host-neutral API for Moonbite's typed Panel projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .panel import DEFAULT_OWNER, PanelStore, PanelValue
from .runtime_core import EventBus, utc_now


@dataclass(frozen=True, slots=True)
class PanelConfig:
    """Filesystem and day-boundary settings for one Panel owner."""

    root: Path
    timezone: str = "UTC"
    anchor_hour: int = 6
    owner: str = DEFAULT_OWNER

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))
        ZoneInfo(self.timezone)
        if not 0 <= self.anchor_hour <= 23:
            raise ValueError("anchor_hour must be from 0 to 23")
        if not isinstance(self.owner, str) or not self.owner.strip():
            raise ValueError("owner must be a non-empty string")


class OwnerBoundPanelStore(PanelStore):
    """PanelStore whose public read default is its configured owner."""

    def snapshot(
        self, *, now: datetime | None = None, owner: str | None = None
    ) -> dict[str, Any]:
        owner_value = self.owner_id if owner is None else owner
        result = super().snapshot(
            now=now,
            owner=owner_value,
        )
        result["owner_epochs"] = {
            name: epoch
            for name, epoch in result["owner_epochs"].items()
            if name == owner_value
        }
        result["owner_reset_policies"] = {
            name: policies
            for name, policies in result["owner_reset_policies"].items()
            if name == owner_value
        }
        return result

    def snapshot_all_owners(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Explicitly read every owner sharing this state root."""

        return PanelStore.snapshot(self, now=now)

    def render_markdown(
        self, *, now: datetime | None = None, owner: str | None = None
    ) -> str:
        return super().render_markdown(
            now=now,
            owner=self.owner_id if owner is None else owner,
        )


def create_panel(
    *,
    root: Path,
    timezone: str = "UTC",
    anchor_hour: int = 6,
    owner: str = DEFAULT_OWNER,
    event_bus: Any | None = None,
    clock: Callable[[], datetime] = utc_now,
) -> OwnerBoundPanelStore:
    """Create only the Panel and its minimal durable event bus.

    A custom ``event_bus`` must provide ``emit(...)`` and ``read_events()``.
    MoonbiteRuntime and Hermes adapters are never constructed here.
    """

    config = PanelConfig(
        root=Path(root),
        timezone=timezone,
        anchor_hour=anchor_hour,
        owner=owner,
    )
    bus = event_bus if event_bus is not None else EventBus(config.root, clock=clock)
    if not callable(getattr(bus, "emit", None)) or not callable(
        getattr(bus, "read_events", None)
    ):
        raise TypeError("event_bus must provide emit() and read_events()")
    return OwnerBoundPanelStore(
        config.root,
        bus=bus,
        timezone_name=config.timezone,
        anchor_hour=config.anchor_hour,
        clock=clock,
        owner_id=config.owner,
    )


__all__ = [
    "OwnerBoundPanelStore",
    "PanelConfig",
    "PanelStore",
    "PanelValue",
    "create_panel",
]
