from __future__ import annotations

import inspect
import typing

import moonbite_plugin.service as service
from moonbite_plugin._service.assembly import ComponentAssemblyMethods
from moonbite_plugin._service.diary import DiaryUseCaseMethods
from moonbite_plugin._service.health import HealthObservationMethods
from moonbite_plugin._service.memory_use_cases import MemoryUseCaseMethods
from moonbite_plugin._service.operations import RuntimeOperationMethods
from moonbite_plugin._service.session_lifecycle import SessionLifecycleMethods


EXPECTED_PUBLIC_NAMES = {
    "ActivityProvider",
    "ActivityResult",
    "Any",
    "AutonomyEngine",
    "AutonomyJudge",
    "Callable",
    "ConfigResolution",
    "ConversationBridge",
    "DEFAULT_SESSION_HOOKS",
    "DenyAutonomyJudge",
    "DiaryWriter",
    "EffectReceipt",
    "EffectRecord",
    "EffectResult",
    "ExposedSource",
    "ExposureContext",
    "ExternalRetriever",
    "HOOK_ORDER",
    "HealthSnapshot",
    "HeartbeatCandidate",
    "HeartbeatEngine",
    "HeartbeatResult",
    "HeartbeatSilenceReceipt",
    "HermesHostAdapter",
    "Iterable",
    "Judge",
    "Mapping",
    "MemoryOrchestrator",
    "MemoryStoreSourceAdapter",
    "MoonbiteRuntime",
    "NoopWakeSink",
    "ObservationFact",
    "Observer",
    "Path",
    "PlatformInfo",
    "ProviderRegistry",
    "RecallCandidate",
    "ResurfaceCandidate",
    "RuntimeComponents",
    "RuntimeComponentsError",
    "SESSION_HOOK_ORDER",
    "SUPPORTED_SESSION_HOOKS",
    "ScheduleProof",
    "SessionContext",
    "SessionContextResolver",
    "SessionHookMappingError",
    "SessionHookReceipt",
    "SessionTurnTerminalReceipt",
    "SilentJudge",
    "SourceMaterial",
    "SourceRegistry",
    "StateError",
    "WakeSink",
    "WriterHandoff",
    "ZoneInfo",
    "annotations",
    "canonical_feature",
    "content_descriptor",
    "date",
    "datetime",
    "deepcopy",
    "detect_platform",
    "example_activity_providers",
    "hashlib",
    "json",
    "logger",
    "logging",
    "new_id",
    "parse_time",
    "resolve_config",
    "state_root",
    "timedelta",
    "utc_now",
}

METHOD_GROUPS = (
    ComponentAssemblyMethods,
    SessionLifecycleMethods,
    HealthObservationMethods,
    RuntimeOperationMethods,
    MemoryUseCaseMethods,
    DiaryUseCaseMethods,
)


def _implementation(descriptor):
    if isinstance(descriptor, (classmethod, staticmethod)):
        return descriptor.__func__
    if isinstance(descriptor, property):
        return descriptor.fget
    return descriptor


def test_service_preserves_historical_public_module_names():
    assert {name for name in vars(service) if not name.startswith("_")} == (
        EXPECTED_PUBLIC_NAMES
    )


def test_runtime_facade_preserves_descriptors_and_reflection():
    installed = set()
    for group in METHOD_GROUPS:
        for name, descriptor in vars(group).items():
            if name in {"__module__", "__dict__", "__weakref__", "__doc__"}:
                continue
            public_descriptor = inspect.getattr_static(service.MoonbiteRuntime, name)
            assert public_descriptor is descriptor
            implementation = _implementation(public_descriptor)
            assert implementation.__module__ == "moonbite_plugin.service"
            assert implementation.__qualname__ == f"MoonbiteRuntime.{name}"
            typing.get_type_hints(implementation)
            installed.add(name)

    runtime_names = {
        name
        for name in vars(service.MoonbiteRuntime)
        if name not in {"__module__", "__dict__", "__weakref__", "__doc__"}
    }
    assert runtime_names == installed
    assert (
        inspect.signature(service.MoonbiteRuntime.__init__)
        .parameters["resolution_raw_config"]
        .default
        is service._MISSING
    )


def test_runtime_facade_keeps_cross_group_dynamic_dispatch(monkeypatch):
    calls = []

    def panel_context(owner):
        calls.append(("panel", owner))
        return {"context": "panel context"}

    def memory_context(owner, user_message, *, session_receipt=None):
        calls.append(("memory", owner, user_message, session_receipt))
        return {"context": "memory context"}

    monkeypatch.setattr(service.MoonbiteRuntime, "panel_prompt_context", panel_context)
    monkeypatch.setattr(
        service.MoonbiteRuntime, "memory_prompt_context", memory_context
    )
    runtime = object.__new__(service.MoonbiteRuntime)

    assert runtime.pre_llm_context("hello") == {
        "context": "panel context\n\nmemory context"
    }
    assert calls == [("panel", runtime), ("memory", runtime, "hello", None)]
