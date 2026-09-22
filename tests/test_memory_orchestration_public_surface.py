from __future__ import annotations

import inspect
import typing

from moonbite_plugin import memory_orchestration
from moonbite_plugin._memory_orchestration import contracts
from moonbite_plugin._memory_orchestration import engine
from moonbite_plugin._memory_orchestration import exposure
from moonbite_plugin._memory_orchestration import maintenance
from moonbite_plugin._memory_orchestration import sources
from moonbite_plugin._memory_orchestration import writer


EXPECTED_EXPORTS = (
    "EXPOSURE_EVENTS",
    "EXPOSURE_STATES",
    "ORCHESTRATION_SCHEMA",
    "WRITER_OPERATIONS",
    "ExpiredEvidenceError",
    "ExposureConflictError",
    "ExposureContext",
    "ExposureLedger",
    "ExposurePlan",
    "ExposurePolicy",
    "ExposureRecord",
    "ExposedSource",
    "MemoryMaintenanceFacade",
    "MemoryOrchestrator",
    "MaintenanceApprovalAdapter",
    "MissingEvidenceError",
    "OrchestrationError",
    "PolicyDeniedError",
    "ReplyUseEvidence",
    "SourceCandidate",
    "SourceMaterial",
    "SourceOpener",
    "SourceRegistry",
    "SourceRetriever",
    "WriterCoordinator",
    "WriterHandoff",
    "WriterRequest",
    "content_descriptor",
)

MOVED_TYPES = (
    "ExpiredEvidenceError",
    "ExposureConflictError",
    "ExposureContext",
    "ExposureRecord",
    "ExposedSource",
    "MissingEvidenceError",
    "OrchestrationError",
    "PolicyDeniedError",
    "ReplyUseEvidence",
    "SourceCandidate",
    "SourceMaterial",
    "SourceOpener",
    "SourceRetriever",
)


def test_public_exports_and_moved_type_identity_remain_stable() -> None:
    assert tuple(memory_orchestration.__all__) == EXPECTED_EXPORTS
    for name in MOVED_TYPES:
        public = getattr(memory_orchestration, name)
        assert public is getattr(contracts, name)
        assert public.__module__ == "moonbite_plugin.memory_orchestration"
        assert public.__qualname__ == name
    assert memory_orchestration.content_descriptor is contracts.content_descriptor
    assert (
        memory_orchestration.content_descriptor.__module__
        == "moonbite_plugin.memory_orchestration"
    )
    assert memory_orchestration.SourceRegistry is sources.SourceRegistry
    assert memory_orchestration.SourceRegistry.__module__ == (
        "moonbite_plugin.memory_orchestration"
    )
    assert memory_orchestration.SourceRegistry.__qualname__ == "SourceRegistry"
    for name in ("ExposureLedger", "ExposurePlan", "ExposurePolicy"):
        public = getattr(memory_orchestration, name)
        assert public is getattr(exposure, name)
        assert public.__module__ == "moonbite_plugin.memory_orchestration"
        assert public.__qualname__ == name
    for name in ("MaintenanceApprovalAdapter", "MemoryMaintenanceFacade"):
        public = getattr(memory_orchestration, name)
        assert public is getattr(maintenance, name)
        assert public.__module__ == "moonbite_plugin.memory_orchestration"
        assert public.__qualname__ == name
    for name in ("WriterCoordinator", "WriterHandoff", "WriterRequest"):
        public = getattr(memory_orchestration, name)
        assert public is getattr(writer, name)
        assert public.__module__ == "moonbite_plugin.memory_orchestration"
        assert public.__qualname__ == name
    assert memory_orchestration.WRITER_OPERATIONS is writer.WRITER_OPERATIONS
    assert memory_orchestration.MemoryOrchestrator is engine.MemoryOrchestrator
    assert memory_orchestration.MemoryOrchestrator.__module__ == (
        "moonbite_plugin.memory_orchestration"
    )
    assert memory_orchestration.MemoryOrchestrator.__qualname__ == "MemoryOrchestrator"


def test_moved_contract_signatures_and_type_hints_remain_resolvable() -> None:
    expected = {
        "content_descriptor": "(value: 'Any') -> 'tuple[str, int]'",
        "SourceCandidate": (
            "(source_ref: 'str', source_class: 'str', "
            "source_event_time: 'datetime', created_at: 'datetime', "
            "expires_at: 'datetime | None' = None, "
            "content_sha256: 'str | None' = None, "
            "content_length: 'int | None' = None, relevance: 'float' = 0.0, "
            "metadata: 'Mapping[str, Any]' = <factory>) -> None"
        ),
        "SourceCandidate.from_mapping": (
            "(value: 'Mapping[str, Any]') -> \"'SourceCandidate'\""
        ),
        "SourceMaterial.from_mapping": (
            "(value: 'Mapping[str, Any]', *, fallback: 'SourceCandidate') "
            "-> \"'SourceMaterial'\""
        ),
        "ExposureContext.from_session": (
            "(value: 'Any', *, observed_at: 'datetime | None' = None, "
            "turn_index: 'int' = 0) -> \"'ExposureContext'\""
        ),
        "ReplyUseEvidence.from_content": (
            "(reply_use_id: 'str', content: 'Any') -> \"'ReplyUseEvidence'\""
        ),
        "SourceRetriever.retrieve": (
            "(self, query: 'str', *, limit: 'int') -> \"Iterable['SourceCandidate']\""
        ),
        "SourceOpener.open": (
            "(self, source_ref: 'str', *, max_bytes: 'int') "
            "-> \"'SourceMaterial | None'\""
        ),
        "SourceRegistry": "(retriever: 'Any' = None, opener: 'Any' = None) -> 'None'",
        "SourceRegistry.retrieve": (
            "(self, query: 'str', *, limit: 'int') -> 'tuple[SourceCandidate, ...]'"
        ),
        "SourceRegistry.exact_open": (
            "(self, candidate: 'SourceCandidate', *, max_bytes: 'int' = 65536) "
            "-> 'SourceMaterial | None'"
        ),
        "ExposureLedger.record_selected": (
            "(self, candidate: 'SourceCandidate', *, context: 'ExposureContext', "
            "exposure_id: 'str | None' = None, event_id: 'str | None' = None, "
            "now: 'datetime | None' = None) -> 'ExposureRecord'"
        ),
        "ExposureLedger.observer_status": (
            "(self, *, target_date: 'date', now: 'datetime') "
            "-> 'tuple[ObservationFact, ...]'"
        ),
        "ExposurePolicy.choose": (
            "(self, candidates: 'Iterable[SourceCandidate]', *, "
            "context: 'ExposureContext', ledger: 'ExposureLedger', "
            "now: 'datetime', continuity_policy: "
            "'Callable[[str, str], bool] | None' = None, "
            "first_turn: 'bool | None' = None) -> \"'ExposurePlan'\""
        ),
        "MaintenanceApprovalAdapter.approval_required": (
            "(self, proposal: 'Mapping[str, Any]') -> 'bool'"
        ),
        "MemoryMaintenanceFacade": (
            "(memory_store: 'Any', *, approval_adapter: 'Any' = None, "
            "root: 'Path | None' = None, clock: 'Callable[[], datetime]' = "
            "<function utc_now at "
        ),
        "MemoryMaintenanceFacade.propose": (
            "(self, *, request_id: 'str', operation: 'str', "
            "evidence_refs: 'Iterable[str]', reason: 'str', "
            "proposed_value: 'Any' = None, approval_required: 'bool | None' = None, "
            "sensitive: 'bool | None' = None) -> 'Mapping[str, Any]'"
        ),
        "MemoryMaintenanceFacade.apply": (
            "(self, proposal_id: 'str', *, activity: 'str', permission: 'str', "
            "approval_evidence: 'Any' = None, approval: 'bool | None' = None) "
            "-> 'Mapping[str, Any]'"
        ),
        "MemoryMaintenanceFacade.observer_status": (
            "(self, *, target_date: 'date', now: 'datetime') "
            "-> 'tuple[ObservationFact, ...]'"
        ),
        "WriterRequest": (
            "(effect_id: 'str', operation: 'str', source_event_id: 'str', "
            "idempotency_key: 'str', epoch_id: 'str', content_sha256: 'str', "
            "content_length: 'int', attempt: 'int', content: 'Any') -> None"
        ),
        "WriterHandoff": (
            "(operation: 'str', effect_id: 'str', record: 'Any', "
            "error_type: 'str | None' = None, request: 'WriterRequest | None' = None) "
            "-> None"
        ),
        "WriterCoordinator.create_intent": (
            "(self, operation: 'str', *, source_event_id: 'str', "
            "idempotency_key: 'str', epoch_id: 'str', content: 'Any', "
            "expires_at: 'datetime | None' = None, ttl: 'timedelta' = "
            "datetime.timedelta(seconds=300), effect_id: 'str | None' = None) "
            "-> 'EffectRecord'"
        ),
        "WriterCoordinator.handoff": (
            "(self, effect_id: 'str', writer: 'Any', *, content: 'Any' = None, "
            "operation: 'str | None' = None) -> 'WriterHandoff'"
        ),
        "WriterCoordinator.verify": (
            "(self, effect_id: 'str', receipt: 'EffectReceipt') -> 'WriterHandoff'"
        ),
        "WriterCoordinator.observer_status": (
            "(self, *, target_date: 'date', now: 'datetime') "
            "-> 'tuple[ObservationFact, ...]'"
        ),
        "MemoryOrchestrator.retrieve": (
            "(self, query: 'str', *, context: 'ExposureContext | Any', "
            "limit: 'int | None' = None) -> 'tuple[SourceCandidate, ...]'"
        ),
        "MemoryOrchestrator.plan": (
            "(self, candidates: 'Iterable[SourceCandidate]', *, "
            "context: 'ExposureContext | Any', first_turn: 'bool | None' = None, "
            "now: 'datetime | None' = None) -> 'ExposurePlan'"
        ),
        "MemoryOrchestrator.expose_candidates": (
            "(self, candidates: 'Iterable[SourceCandidate]', *, "
            "context: 'ExposureContext | Any', first_turn: 'bool | None' = None, "
            "now: 'datetime | None' = None) -> 'tuple[ExposedSource, ...]'"
        ),
        "MemoryOrchestrator.open_selected": (
            "(self, exposure_id: 'str', *, candidate: 'SourceCandidate | None' = None, "
            "opener: 'Any' = None, max_bytes: 'int' = 65536, "
            "context: 'ExposureContext | Any | None' = None, "
            "now: 'datetime | None' = None) -> 'SourceMaterial'"
        ),
        "MemoryOrchestrator.mark_used": (
            "(self, *args: 'Any', **kwargs: 'Any') -> 'ExposureRecord'"
        ),
        "MemoryOrchestrator.observer_status": (
            "(self, *, target_date: 'date', now: 'datetime') "
            "-> 'tuple[ObservationFact, ...]'"
        ),
    }
    for path, signature in expected.items():
        value = memory_orchestration
        for part in path.split("."):
            value = getattr(value, part)
        actual = str(inspect.signature(value))
        if path == "MemoryMaintenanceFacade":
            assert actual.startswith(signature)
            assert actual.endswith(") -> 'None'")
        else:
            assert actual == signature
        typing.get_type_hints(value)


def test_moved_contract_docstrings_remain_available_from_public_types() -> None:
    assert inspect.getdoc(memory_orchestration.SourceCandidate) == (
        "Opaque retriever output; it intentionally has no source body field."
    )
    assert inspect.getdoc(memory_orchestration.SourceMaterial) == (
        "Bounded transient result of an exact open; never written to a ledger."
    )
    assert inspect.getdoc(memory_orchestration.ExposureContext) == (
        "The minimum lifecycle context needed to bind one exposure."
    )
    assert inspect.getdoc(memory_orchestration.content_descriptor) == (
        "Return the only content information persisted by this module."
    )
