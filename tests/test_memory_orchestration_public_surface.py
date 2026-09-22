from __future__ import annotations

import inspect
import typing

from moonbite_plugin import memory_orchestration
from moonbite_plugin._memory_orchestration import contracts


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
    }
    for path, signature in expected.items():
        value = memory_orchestration
        for part in path.split("."):
            value = getattr(value, part)
        assert str(inspect.signature(value)) == signature
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
