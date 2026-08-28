"""Moonbite's stable Python entry point."""

from importlib import import_module

from .components import (
    RESERVED_STATE_DOMAINS,
    REQUIRED_STATE_DOMAINS,
    RUNTIME_COMPONENTS_SCHEMA,
    RuntimeComponents,
    RuntimeComponentsError,
)
from .observations import (
    DEFAULT_MAX_CLOCK_SKEW,
    DEFAULT_MAX_SENDER_CLOCK_JUMP,
    MAX_CURSOR_EVENT_IDS,
    OBSERVATION_AVAILABILITIES,
    OBSERVATION_DECISIONS,
    OBSERVATION_FRESHNESSES,
    OBSERVATION_PROJECTION_SCHEMA,
    OBSERVATION_SCHEMA,
    ObservationCursor,
    ObservationDecision,
    ObservationEvidence,
    decide_observation,
)
from .incidents import (
    DEFAULT_INCIDENT_MAX_CLOCK_SKEW,
    INCIDENT_DECISIONS,
    INCIDENT_LIFECYCLES,
    INCIDENT_PROJECTION_SCHEMA,
    INCIDENT_SCHEMA,
    INCIDENT_SEVERITIES,
    INCIDENT_STATES,
    INCIDENT_SUMMARY_SCHEMA,
    IncidentCursor,
    IncidentDecision,
    IncidentEvidence,
    IncidentProjection,
    MAX_INCIDENT_AGGREGATION,
    MAX_INCIDENT_CODES,
    MAX_INCIDENT_SEEN_EVENT_IDS,
    aggregate_incidents,
    decide_incident,
    fingerprint_codes,
)
from .heartbeat import (
    CADENCE_SCHEMA_V1,
    CADENCE_SCHEMA_V2,
    CADENCE_SCHEMA_V3,
    HeartbeatSilenceReceipt,
)

_PLUGIN_EXPORTS = frozenset(
    {"RegistrationPlan", "build_runtime", "register", "register_runtime"}
)


def __getattr__(name: str):
    """Load Hermes integration only when a host asks for it.

    Keeping the host adapter lazy lets ``moonbite_plugin.panel_api`` remain a
    lightweight Python-library entry point with no Hermes imports.
    """

    if name not in _PLUGIN_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(".plugin", __name__), name)
    globals()[name] = value
    return value


__all__ = [
    "register",
    "build_runtime",
    "register_runtime",
    "RegistrationPlan",
    "RuntimeComponents",
    "RuntimeComponentsError",
    "RUNTIME_COMPONENTS_SCHEMA",
    "REQUIRED_STATE_DOMAINS",
    "RESERVED_STATE_DOMAINS",
    "DEFAULT_MAX_CLOCK_SKEW",
    "DEFAULT_MAX_SENDER_CLOCK_JUMP",
    "MAX_CURSOR_EVENT_IDS",
    "OBSERVATION_AVAILABILITIES",
    "OBSERVATION_DECISIONS",
    "OBSERVATION_FRESHNESSES",
    "OBSERVATION_PROJECTION_SCHEMA",
    "OBSERVATION_SCHEMA",
    "ObservationCursor",
    "ObservationDecision",
    "ObservationEvidence",
    "decide_observation",
    "DEFAULT_INCIDENT_MAX_CLOCK_SKEW",
    "INCIDENT_DECISIONS",
    "INCIDENT_LIFECYCLES",
    "INCIDENT_PROJECTION_SCHEMA",
    "INCIDENT_SCHEMA",
    "INCIDENT_SEVERITIES",
    "INCIDENT_STATES",
    "INCIDENT_SUMMARY_SCHEMA",
    "IncidentCursor",
    "IncidentDecision",
    "IncidentEvidence",
    "IncidentProjection",
    "MAX_INCIDENT_AGGREGATION",
    "MAX_INCIDENT_CODES",
    "MAX_INCIDENT_SEEN_EVENT_IDS",
    "aggregate_incidents",
    "decide_incident",
    "fingerprint_codes",
    "CADENCE_SCHEMA_V1",
    "CADENCE_SCHEMA_V2",
    "CADENCE_SCHEMA_V3",
    "HeartbeatSilenceReceipt",
]
