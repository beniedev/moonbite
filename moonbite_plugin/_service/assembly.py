"""Runtime component assembly using the injected owner bundle."""

from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any

from ..autonomy import (
    ActivityProvider,
    AutonomyEngine,
    AutonomyJudge,
    DenyAutonomyJudge,
    ProviderRegistry,
)
from ..components import RuntimeComponents, RuntimeComponentsError
from ..conversation import ConversationBridge
from ..example_providers import example_activity_providers
from ..heartbeat import HeartbeatEngine, Judge, NoopWakeSink, SilentJudge, WakeSink
from ..hermes_adapter import HermesHostAdapter
from ..memory import DiaryWriter, ExternalRetriever
from ..memory_adapters import MemoryStoreSourceAdapter
from ..memory_orchestration import MemoryOrchestrator, SourceRegistry
from ..platforms import PlatformInfo, detect_platform, state_root
from ..scenarios import ConfigResolution, resolve_config
from .contracts import MISSING as _MISSING, SessionContextResolver


class ComponentAssemblyMethods:
    def __init__(
        self,
        raw_config: Any,
        *,
        heartbeat_judge: Judge | None = None,
        autonomy_judge: AutonomyJudge | None = None,
        wake_sink: WakeSink | None = None,
        diary_writer: DiaryWriter | None = None,
        external_retriever: ExternalRetriever | None = None,
        components: RuntimeComponents | None = None,
        root: Path | None = None,
        platform_info: PlatformInfo | None = None,
        session_context_resolver: SessionContextResolver | None = None,
        conversation_bridge: ConversationBridge | None = None,
        memory_orchestrator: MemoryOrchestrator | None = None,
        source_registry: SourceRegistry | None = None,
        approval_adapter: Any = None,
        activity_providers: Iterable[ActivityProvider] = (),
        resolution: ConfigResolution | None = None,
        selected_pack: str | None = None,
        resolution_raw_config: Any = _MISSING,
    ):
        if resolution is None:
            resolution = resolve_config(raw_config, selected_pack)
        elif not isinstance(resolution, ConfigResolution):
            raise TypeError("resolution must be a ConfigResolution")
        self.resolution = resolution
        self.config = resolution.effective_config
        resolution_input = (
            raw_config if resolution_raw_config is _MISSING else resolution_raw_config
        )
        self._resolution_raw_config = deepcopy(resolution_input)
        self._resolution_selected_pack = resolution.selected_pack
        provided_components = components is not None
        if provided_components and root is not None:
            raise RuntimeComponentsError("multiple_state_writers")
        if not provided_components:
            resolved_root = (
                root
                if root is not None
                else state_root(self.config["state"]["directory"])
            )
            components = RuntimeComponents.standalone(
                resolved_root,
                self.config["timezone"],
                self.config["panel"]["anchor_hour"],
            )
        else:
            assert components is not None
            if not isinstance(components, RuntimeComponents):
                raise RuntimeComponentsError(
                    "components must be a RuntimeComponents bundle"
                )
            try:
                components.validate()
            except RuntimeComponentsError:
                raise
            except (AttributeError, TypeError) as exc:
                raise RuntimeComponentsError(
                    "invalid runtime components bundle"
                ) from exc
            if components.mode != "injected":
                raise RuntimeComponentsError("injected_components_required")
            if (
                self.config["autonomy"]["providers"]
                .get("paper_browse", {})
                .get("enabled", False)
            ):
                raise RuntimeComponentsError(
                    "paper_browse provider requires standalone state ownership"
                )

        self.platform = platform_info or detect_platform()
        self.components = components
        self.root = components.state_root
        self.bus = components.bus
        self.controls = components.controls
        self.cadence = components.cadence
        self.panel = components.panel
        self.memory = components.memory
        # These are always the bundle's owners.  In injected mode they are
        # host-owned compatible ports; no local root fallback is permitted.
        self.session = components.session
        self.session_store = components.session
        self.effects = components.effects
        self.effect_ledger = components.effects
        self.effect = components.effects
        self.external_retriever = external_retriever
        self.diary_writer = diary_writer
        self.approval_adapter = approval_adapter
        self.source_registry = source_registry
        self.memory_orchestrator = memory_orchestrator
        self._validate_memory_orchestrator_owners(memory_orchestrator)
        if memory_orchestrator is None and components.mode == "standalone":
            if source_registry is None:
                memory_adapter = MemoryStoreSourceAdapter(
                    self.memory,
                    self.external_retriever,
                )
                source_registry = SourceRegistry(
                    retriever=memory_adapter,
                    opener=memory_adapter,
                )
                self.source_registry = source_registry
            self.memory_orchestrator = MemoryOrchestrator(
                self.root,
                memory_store=self.memory,
                session_store=self.session,
                effect_ledger=self.effects,
                source_registry=source_registry,
                approval_adapter=approval_adapter,
            )
        elif memory_orchestrator is not None and source_registry is None:
            self.source_registry = getattr(memory_orchestrator, "sources", None)
        if conversation_bridge is None and components.mode == "standalone":
            conversation_bridge = ConversationBridge(
                self.root,
                session_store=components.session,
                effect_ledger=components.effects,
            )
        self._validate_conversation_bridge_owners(conversation_bridge)
        self.conversation_bridge = conversation_bridge
        if session_context_resolver is not None and not callable(
            session_context_resolver
        ):
            raise RuntimeComponentsError(
                "session_context_resolver must be callable or None"
            )
        self.hermes_host_adapter = HermesHostAdapter()
        self.session_context_resolver = session_context_resolver
        self._last_session_hook_error: dict[str, str] | None = None
        self.providers = ProviderRegistry()
        self.providers.register(
            ActivityProvider("local_reflection", self._local_reflection)
        )
        provider_root = self.root if components.mode == "standalone" else None
        for provider in example_activity_providers(provider_root):
            self.providers.register(provider)
        for provider in activity_providers:
            self.providers.register(provider)
        self.heartbeat = HeartbeatEngine(
            bus=self.bus,
            controls=self.controls,
            cadence=self.cadence,
            judge=heartbeat_judge or SilentJudge(),
            sink=wake_sink or NoopWakeSink(),
            locks=components.locks,
            effect_ledger=self.effects,
            kind_policies=self.config.get("heartbeat", {}).get("kinds"),
        )
        self.autonomy = AutonomyEngine(
            bus=self.bus,
            controls=self.controls,
            registry=self.providers,
            judge=autonomy_judge or DenyAutonomyJudge(),
            locks=components.locks,
            effect_ledger=self.effects,
        )

    def _validate_memory_orchestrator_owners(self, orchestrator: Any) -> None:
        """Reject a concrete orchestrator that would introduce split owners."""

        if not isinstance(orchestrator, MemoryOrchestrator):
            return
        expected = {
            "memory_store": self.memory,
            "session_store": self.session,
            "effect_ledger": self.effects,
        }
        missing = object()
        for name, owner in expected.items():
            actual = getattr(orchestrator, name, missing)
            if actual is missing or actual is not owner:
                raise RuntimeComponentsError(
                    f"memory orchestrator {name} must be the bundle owner"
                )

    def _validate_conversation_bridge_owners(self, bridge: Any) -> None:
        """Reject a concrete bridge that would introduce split lifecycle owners."""

        if not isinstance(bridge, ConversationBridge):
            return
        expected = {
            "session_store": self.session,
            "effect_ledger": self.effects,
        }
        missing = object()
        for name, owner in expected.items():
            actual = getattr(bridge, name, missing)
            if actual is not missing and actual is not None and actual is not owner:
                raise RuntimeComponentsError(
                    f"conversation bridge {name} must be the bundle owner"
                )

    def _local_reflection(self, context) -> dict[str, Any]:
        facts = dict(context.facts)
        return {
            "kind": "local_reflection",
            "at": context.now.isoformat(),
            "fact_keys": sorted(facts),
        }
