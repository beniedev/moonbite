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
from types import FunctionType as _FunctionType
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
    _health_context as _health_context_impl,
    _raise_observer_error as _raise_observer_error_impl,
    _unavailable_observer_fact as _unavailable_observer_fact_impl,
)
from ._service.memory_use_cases import MemoryUseCaseMethods as _MemoryUseCaseMethods
from ._service.operations import RuntimeOperationMethods as _RuntimeOperationMethods
from ._service.session_lifecycle import (
    SessionLifecycleMethods as _SessionLifecycleMethods,
)


def _facade_function(function, qualname: str):
    """Bind extracted code to this module's historical runtime globals."""

    rebound = _FunctionType(
        function.__code__,
        globals(),
        function.__name__,
        function.__defaults__,
        function.__closure__,
    )
    rebound.__kwdefaults__ = function.__kwdefaults__
    rebound.__annotations__ = dict(function.__annotations__)
    rebound.__dict__.update(function.__dict__)
    rebound.__doc__ = function.__doc__
    rebound.__module__ = __name__
    rebound.__qualname__ = qualname
    return rebound


def _facade_descriptor(name: str, descriptor):
    qualname = f"MoonbiteRuntime.{name}"
    if isinstance(descriptor, classmethod):
        return classmethod(_facade_function(descriptor.__func__, qualname))
    if isinstance(descriptor, staticmethod):
        return staticmethod(_facade_function(descriptor.__func__, qualname))
    if isinstance(descriptor, property):
        return property(
            None
            if descriptor.fget is None
            else _facade_function(descriptor.fget, qualname),
            None
            if descriptor.fset is None
            else _facade_function(descriptor.fset, qualname),
            None
            if descriptor.fdel is None
            else _facade_function(descriptor.fdel, qualname),
            descriptor.__doc__,
        )
    return _facade_function(descriptor, qualname)


_health_context = _facade_function(_health_context_impl, "_health_context")
_raise_observer_error = _facade_function(
    _raise_observer_error_impl, "_raise_observer_error"
)
_unavailable_observer_fact = _facade_function(
    _unavailable_observer_fact_impl, "_unavailable_observer_fact"
)


class MoonbiteRuntime:
    pass


def _install_method_group(method_group) -> None:
    for method_name, descriptor in method_group.__dict__.items():
        if not isinstance(
            descriptor,
            (_FunctionType, classmethod, staticmethod, property),
        ):
            continue
        setattr(
            MoonbiteRuntime,
            method_name,
            _facade_descriptor(method_name, descriptor),
        )


for _method_group in (
    _ComponentAssemblyMethods,
    _SessionLifecycleMethods,
    _HealthObservationMethods,
    _RuntimeOperationMethods,
    _MemoryUseCaseMethods,
    _DiaryUseCaseMethods,
):
    _install_method_group(_method_group)

del _method_group
