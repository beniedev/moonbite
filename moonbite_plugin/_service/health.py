"""Read-only runtime health and status projections."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..observer import HealthSnapshot, ObservationFact, Observer, ScheduleProof
from ..runtime_core import utc_now
from .contracts import MISSING as _MISSING


def _health_context(
    target_date: date | None,
    now: datetime | None,
    timezone_name: str = "UTC",
) -> tuple[date, datetime]:
    """Resolve the optional observer context without touching runtime state."""

    effective_now = utc_now() if now is None else now
    effective_date = (
        effective_now.astimezone(ZoneInfo(timezone_name)).date()
        if target_date is None
        else target_date
    )
    return effective_date, effective_now


def _unavailable_observer_fact(
    source_name: str, *, target_date: date
) -> tuple[ObservationFact, ...]:
    """Represent an optional owner without turning absence into an incident."""

    return (
        ObservationFact(
            key=f"source:{source_name}",
            code="source_unavailable",
            state="neutral",
            target_date=target_date,
            refs=(source_name,),
        ),
    )


def _raise_observer_error(error: BaseException) -> Any:
    raise error


class HealthObservationMethods:
    def _health_sources(self, *, target_date: date) -> dict[str, Any]:
        """Build the content-free owner source map for :class:`Observer`.

        Standalone and injected runtimes share this path.  Optional injected
        owners are represented by neutral facts when their observer port is
        absent; an advertised callable remains visible to ``Observer`` so a
        thrown error becomes a current integrity fact.
        """

        candidates = {
            "autonomy": self.autonomy,
            "conversation_bridge": self.conversation_bridge,
            "effects": self.effects,
            "heartbeat": self.heartbeat,
            "memory_orchestrator": self.memory_orchestrator,
            "panel": self.panel,
        }
        sources: dict[str, Any] = {}
        seen: set[int] = set()

        for source_name, owner in sorted(candidates.items()):
            if owner is not None:
                identity = id(owner)
                if identity in seen:
                    continue
                seen.add(identity)

            if owner is None:
                sources[source_name] = (
                    lambda *, target_date, now, source_name=source_name: (
                        _unavailable_observer_fact(
                            source_name,
                            target_date=target_date,
                        )
                    )
                )
                continue

            try:
                port = getattr(owner, "observer_status", _MISSING)
            except Exception as exc:  # pragma: no cover - hostile descriptor
                # Keep the failure inside Observer's redacted integrity path.
                sources[source_name] = lambda *, target_date, now, exc=exc: (
                    _raise_observer_error(exc)
                )
                continue

            if port is _MISSING or not callable(port):
                sources[source_name] = (
                    lambda *, target_date, now, source_name=source_name: (
                        _unavailable_observer_fact(
                            source_name,
                            target_date=target_date,
                        )
                    )
                )
            else:
                # Pass the owner object, rather than the bound method, so the
                # Observer remains the sole boundary that classifies errors.
                sources[source_name] = owner
        controls_identity = id(self.controls)
        if controls_identity not in seen:
            seen.add(controls_identity)
            sources["controls"] = self._control_health_facts
        return sources

    def _control_health_facts(
        self, *, target_date: date, now: datetime
    ) -> tuple[ObservationFact, ...]:
        """Project only safe active-control metadata into observer facts."""

        observer = getattr(self.controls, "observer_active", None)
        if not callable(observer):
            return _unavailable_observer_fact("controls", target_date=target_date)
        intents = observer(now=now)
        if isinstance(intents, (str, bytes, bytearray, Mapping)):
            raise TypeError("control observer result must be an iterable")
        values = tuple(intents)
        facts: list[ObservationFact] = []
        for intent in values:
            metadata = self._safe_control_metadata(intent)
            if metadata is None:
                raise TypeError("control observer result contains malformed metadata")
            facts.append(
                ObservationFact(
                    key=f"controls:{metadata['control_id']}",
                    code="control_active",
                    state="neutral",
                    target_date=target_date,
                    refs=(
                        metadata["control_id"],
                        metadata["feature"],
                        metadata["mode"],
                        metadata["source"],
                    ),
                    counts={"active_controls": 1},
                )
            )
        return tuple(facts)

    def health_snapshot(
        self,
        target_date: date | None = None,
        now: datetime | None = None,
        schedule_proof: ScheduleProof | None = None,
    ) -> HealthSnapshot:
        """Return a read-only aggregate of Moonbite-owned health evidence."""

        effective_date, effective_now = _health_context(
            target_date,
            now,
            self.config["timezone"],
        )
        return Observer(
            sources=self._health_sources(target_date=effective_date)
        ).snapshot(
            effective_date,
            effective_now,
            schedule_proof=schedule_proof,
        )

    @staticmethod
    def _safe_control_time(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return value if type(value) is str else None

    @classmethod
    def _safe_control_metadata(cls, intent: Any) -> dict[str, Any] | None:
        """Expose stable control metadata while excluding intent payloads."""

        if isinstance(intent, Mapping):
            get = intent.get
        else:
            get = lambda key, default=None: getattr(intent, key, default)
        control_id = get("control_id")
        feature = get("feature")
        mode = get("mode")
        source = get("source")
        if not all(
            type(value) is str and value
            for value in (control_id, feature, mode, source)
        ):
            return None
        return {
            "control_id": control_id,
            "feature": feature,
            "mode": mode,
            "source": source,
            "created_at": cls._safe_control_time(get("created_at")),
            "expires_at": cls._safe_control_time(get("expires_at")),
        }

    def _active_control_metadata(self, *, now: datetime) -> list[dict[str, Any]]:
        """Read active controls through the lock-free observer port only."""

        observer = getattr(self.controls, "observer_active", None)
        if not callable(observer):
            return []
        try:
            intents = observer(now=now)
            if isinstance(intents, (str, bytes, bytearray, Mapping)):
                raise TypeError("control observer result must be an iterable")
            values = tuple(intents)
        except Exception:
            # Status is an operator surface; a malformed control ledger must
            # not cause a fallback to the mutating ``active`` path.
            return []
        result = []
        for intent in values:
            metadata = self._safe_control_metadata(intent)
            if metadata is not None:
                result.append(metadata)
        return result

    def status(
        self,
        *,
        include_private_paths: bool = False,
        target_date: date | None = None,
        now: datetime | None = None,
        schedule_proof: ScheduleProof | None = None,
    ) -> dict[str, Any]:
        effective_date, effective_now = _health_context(
            target_date,
            now,
            self.config["timezone"],
        )
        health = self.health_snapshot(
            target_date=effective_date,
            now=effective_now,
            schedule_proof=schedule_proof,
        )
        bindings = self.config["model_routes"]
        result = {
            "ok": True,
            "platform": {
                "family": self.platform.family,
                "is_wsl": self.platform.is_wsl,
            },
            "state_root": "private",
            "enabled_modules": sorted(
                name for name, enabled in self.config["modules"].items() if enabled
            ),
            "scheduler": "host_owned",
            "delivery_adapter": self.config["delivery"]["adapter"],
            "scenario_pack": self.resolution.selected_pack,
            "model_routes": bindings,
            "registered_activity_providers": list(self.providers.names()),
            "active_controls": self._active_control_metadata(now=effective_now),
            "last_session_hook_error": self.last_session_hook_error,
            "health": health.to_dict(),
        }
        if include_private_paths:
            result["state_root"] = (
                str(self.root) if isinstance(self.root, Path) else "host_owned"
            )
        return result

    def session_status(self) -> dict[str, Any]:
        """Return exact identifiers for currently open session turns.

        This deliberately reports ownership, not an orphan classification:
        an open turn may still be making progress.
        """

        open_turns = [
            {
                "session_id": snapshot.session_id,
                "lifecycle_id": snapshot.lifecycle_id,
                "turn_id": snapshot.open_turn_id,
            }
            for snapshot in self.session_store.snapshots()
            if snapshot.open_turn_id is not None
        ]
        return {"ok": True, "open_turns": open_turns}
