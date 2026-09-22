"""Wired Moonbite runtime used by CLI, tools, hooks, and host adapters."""

# Keep the historical module-level dependency names available for compatibility.
# The runtime implementations themselves live in responsibility-specific modules.
# ruff: noqa: F401

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .autonomy import (
    ActivityProvider,
    ActivityResult,
    AutonomyEngine,
    AutonomyJudge,
    DenyAutonomyJudge,
    ProviderRegistry,
)
from .components import RuntimeComponents, RuntimeComponentsError
from .conversation import ConversationBridge
from .control import canonical_feature
from .effects import EffectReceipt, EffectRecord
from .example_providers import example_activity_providers
from .heartbeat import (
    EffectResult,
    HeartbeatCandidate,
    HeartbeatEngine,
    HeartbeatResult,
    HeartbeatSilenceReceipt,
    Judge,
    NoopWakeSink,
    SilentJudge,
    WakeSink,
)
from .hermes_adapter import HermesHostAdapter, SessionHookMappingError
from .memory import (
    DiaryWriter,
    ExternalRetriever,
    RecallCandidate,
    ResurfaceCandidate,
)
from .memory_adapters import MemoryStoreSourceAdapter
from .memory_orchestration import (
    ExposedSource,
    ExposureContext,
    MemoryOrchestrator,
    SourceMaterial,
    SourceRegistry,
    WriterHandoff,
    content_descriptor,
)
from .observer import HealthSnapshot, ObservationFact, Observer, ScheduleProof
from .platforms import PlatformInfo, detect_platform, state_root
from .runtime_core import StateError, new_id, parse_time, utc_now
from .session import (
    HOOK_ORDER,
    SessionContext,
    SessionHookReceipt,
    SessionTurnTerminalReceipt,
)
from .scenarios import ConfigResolution, resolve_config

logger = logging.getLogger(__name__)

from ._service.assembly import ComponentAssemblyMethods as _ComponentAssemblyMethods
from ._service.contracts import (
    DEFAULT_SESSION_HOOKS,
    MISSING as _MISSING,
    SESSION_HOOK_ORDER,
    SUPPORTED_SESSION_HOOKS,
    SessionContextResolver,
)
from ._service.diary import DiaryUseCaseMethods as _DiaryUseCaseMethods
from ._service.health import (
    HealthObservationMethods as _HealthObservationMethods,
    _health_context,
    _raise_observer_error,
    _unavailable_observer_fact,
)
from ._service.memory_use_cases import MemoryUseCaseMethods as _MemoryUseCaseMethods
from ._service.operations import RuntimeOperationMethods as _RuntimeOperationMethods
from ._service.session_lifecycle import (
    SessionLifecycleMethods as _SessionLifecycleMethods,
)


class MoonbiteRuntime:
    pass


for _method_group in (
    _ComponentAssemblyMethods,
    _SessionLifecycleMethods,
    _HealthObservationMethods,
    _RuntimeOperationMethods,
    _MemoryUseCaseMethods,
    _DiaryUseCaseMethods,
):
    for _method_name, _descriptor in _method_group.__dict__.items():
        if _method_name in {"__module__", "__dict__", "__weakref__", "__doc__"}:
            continue
        setattr(MoonbiteRuntime, _method_name, _descriptor)
        if isinstance(_descriptor, (classmethod, staticmethod)):
            _function = _descriptor.__func__
        elif isinstance(_descriptor, property):
            _function = _descriptor.fget
        else:
            _function = _descriptor
        if _function is not None and hasattr(_function, "__module__"):
            _function.__module__ = __name__
            _function.__qualname__ = f"MoonbiteRuntime.{_method_name}"

del _descriptor, _function, _method_group, _method_name
