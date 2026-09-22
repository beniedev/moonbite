from __future__ import annotations

import inspect
import re
from typing import get_type_hints

from moonbite_plugin import autonomy


AUTONOMY_PUBLIC_MANIFEST = [
    "AUTONOMY_EFFECT_KIND",
    "ActivityProvider",
    "ActivityResult",
    "AllowAutonomyJudge",
    "AutonomyContext",
    "AutonomyDecision",
    "AutonomyEngine",
    "AutonomyExecutionRequest",
    "AutonomyJudge",
    "DenyAutonomyJudge",
    "ProviderEligibilityError",
    "ProviderRegistry",
]

PUBLIC_AUTONOMY_TYPES = (
    "ActivityProvider",
    "ActivityResult",
    "AllowAutonomyJudge",
    "AutonomyContext",
    "AutonomyDecision",
    "AutonomyEngine",
    "AutonomyExecutionRequest",
    "AutonomyJudge",
    "DenyAutonomyJudge",
    "ProviderEligibilityError",
    "ProviderRegistry",
)

PUBLIC_ENGINE_SIGNATURES = {
    "__init__": (
        "(self, *, bus: 'EventBus', controls: 'ControlStore', registry: "
        "'ProviderRegistry', judge: 'AutonomyJudge', rng: 'random.Random | None' "
        "= None, clock: 'Callable[[], datetime]' = utc_now, locks: "
        "'RuntimeLocks | None' = None, effect_ledger: 'Any | None' = None)"
    ),
    "observer_status": (
        "(self, *, target_date: 'date', now: 'datetime') -> "
        "'tuple[ObservationFact, ...]'"
    ),
    "reconcile": (
        "(self, effect_id: 'str', receipt: 'EffectReceipt', *, control_id: "
        "'str | None' = None) -> 'ActivityResult'"
    ),
    "fail": "(self, effect_id: 'str', reason: 'str') -> 'ActivityResult'",
    "run_once": (
        "(self, settings: 'Mapping[str, Mapping[str, Any]]', *, facts: "
        "'Mapping[str, Any] | None' = None) -> 'ActivityResult'"
    ),
}

PUBLIC_ENGINE_HINT_KEYS = {
    "__init__": (
        "bus",
        "controls",
        "registry",
        "judge",
        "rng",
        "clock",
        "locks",
        "effect_ledger",
    ),
    "observer_status": ("target_date", "now", "return"),
    "reconcile": ("effect_id", "receipt", "control_id", "return"),
    "fail": ("effect_id", "reason", "return"),
    "run_once": ("settings", "facts", "return"),
}


def _stable_signature(value) -> str:
    return re.sub(
        r"<function utc_now at 0x[0-9a-fA-F]+>",
        "utc_now",
        str(inspect.signature(value)),
    )


def test_autonomy_public_manifest_and_type_identity_are_stable() -> None:
    assert autonomy.__all__ == AUTONOMY_PUBLIC_MANIFEST
    for name in PUBLIC_AUTONOMY_TYPES:
        value = getattr(autonomy, name)
        assert value.__module__ == "moonbite_plugin.autonomy"
        assert value.__qualname__ == name


def test_autonomy_engine_public_signatures_and_hints_are_stable() -> None:
    for name, expected in PUBLIC_ENGINE_SIGNATURES.items():
        value = getattr(autonomy.AutonomyEngine, name)
        assert _stable_signature(value) == expected
        assert tuple(get_type_hints(value)) == PUBLIC_ENGINE_HINT_KEYS[name]


def test_autonomy_engine_public_docstrings_are_stable() -> None:
    assert inspect.getdoc(autonomy.AutonomyEngine.observer_status) == (
        "Return provider/effect telemetry without invoking autonomy actors.\n\n"
        "The observer consumes only the effect ledger and the already-written\n"
        "audit stream.  It never calls the Judge, a provider runner, a sink, or\n"
        "reconciliation, and it never exposes ``ActivityResult.output``,\n"
        "context facts, or raw exception/reason text."
    )
    assert inspect.getdoc(autonomy.AutonomyEngine.reconcile) == (
        "Write explicit host evidence for an existing autonomy effect.\n\n"
        "Reconciliation never invokes a provider.  A mismatched receipt is\n"
        "fail-closed and cannot be used to consume a play-next control."
    )
    assert inspect.getdoc(autonomy.AutonomyEngine.fail) == (
        "Settle an asynchronous provider with an explicit host failure."
    )
