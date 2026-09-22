"""Host-neutral memory orchestration primitives for MB-50.

This module is deliberately a port layer.  A retriever can return opaque
references and bounded metadata, while an opener is called only after a
reference has been selected.  The durable part of the module is an
append-only exposure ledger: it records the reference and evidence
descriptor, never source material.  Memory records and writer effects are
delegated to their injected stores.
"""

from __future__ import annotations

# Compatibility namespace for annotations on public classes whose implementation
# lives in the internal modules below.
from collections.abc import Callable, Iterable, Mapping  # noqa: F401
from datetime import date, datetime, timedelta  # noqa: F401
from pathlib import Path  # noqa: F401
from typing import Any  # noqa: F401

from ._memory_orchestration.contracts import (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureRecord,
    ExposedSource,
    MissingEvidenceError,
    ORCHESTRATION_SCHEMA,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRetriever,
    content_descriptor,
)
from ._memory_orchestration.engine import MemoryOrchestrator
from ._memory_orchestration.exposure import (
    EXPOSURE_EVENTS,
    EXPOSURE_STATES,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
)
from ._memory_orchestration.maintenance import (
    MaintenanceApprovalAdapter,
    MemoryMaintenanceFacade,
)
from ._memory_orchestration.sources import SourceRegistry
from ._memory_orchestration.writer import (
    WRITER_OPERATIONS,
    WriterCoordinator,
    WriterHandoff,
    WriterRequest,
)


for _public_contract in (
    ExpiredEvidenceError,
    ExposureConflictError,
    ExposureContext,
    ExposureLedger,
    ExposurePlan,
    ExposurePolicy,
    ExposureRecord,
    ExposedSource,
    MaintenanceApprovalAdapter,
    MemoryMaintenanceFacade,
    MemoryOrchestrator,
    MissingEvidenceError,
    OrchestrationError,
    PolicyDeniedError,
    ReplyUseEvidence,
    SourceCandidate,
    SourceMaterial,
    SourceOpener,
    SourceRegistry,
    SourceRetriever,
    WriterCoordinator,
    WriterHandoff,
    WriterRequest,
    content_descriptor,
):
    _public_contract.__module__ = __name__
del _public_contract


__all__ = [
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
]
