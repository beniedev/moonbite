from __future__ import annotations

import inspect
from typing import get_type_hints

import pytest

import moonbite_plugin
from moonbite_plugin import heartbeat


HEARTBEAT_PUBLIC_MANIFEST = [
    "CADENCE_SCHEMA_V1",
    "CADENCE_SCHEMA_V2",
    "CADENCE_SCHEMA_V3",
    "CADENCE_SCHEMA_V4",
    "CADENCE_SCHEMA",
    "HEARTBEAT_CADENCE_SCHEMA",
    "HEARTBEAT_BYPASSES",
    "HeartbeatReasonCode",
    "HeartbeatKindPolicy",
    "HeartbeatCandidate",
    "JudgeDecision",
    "EffectResult",
    "HeartbeatResult",
    "Judge",
    "WakeSink",
    "SilentJudge",
    "NoopWakeSink",
    "HeartbeatCadence",
    "HeartbeatEngine",
]

PUBLIC_HEARTBEAT_TYPES = {
    "HeartbeatReasonCode": ("moonbite_plugin.heartbeat", "HeartbeatReasonCode"),
    "HeartbeatKindPolicy": ("moonbite_plugin.heartbeat", "HeartbeatKindPolicy"),
    "HeartbeatSilenceReceipt": (
        "moonbite_plugin.heartbeat",
        "HeartbeatSilenceReceipt",
    ),
    "HeartbeatCandidate": ("moonbite_plugin.heartbeat", "HeartbeatCandidate"),
    "JudgeDecision": ("moonbite_plugin.heartbeat", "JudgeDecision"),
    "EffectResult": ("moonbite_plugin.heartbeat", "EffectResult"),
    "HeartbeatResult": ("moonbite_plugin.heartbeat", "HeartbeatResult"),
    "Judge": ("moonbite_plugin.heartbeat", "Judge"),
    "WakeSink": ("moonbite_plugin.heartbeat", "WakeSink"),
    "SilentJudge": ("moonbite_plugin.heartbeat", "SilentJudge"),
    "NoopWakeSink": ("moonbite_plugin.heartbeat", "NoopWakeSink"),
    "HeartbeatCadence": ("moonbite_plugin.heartbeat", "HeartbeatCadence"),
    "HeartbeatEngine": ("moonbite_plugin.heartbeat", "HeartbeatEngine"),
}

PUBLIC_SIGNATURES = {
    "HeartbeatCadence.__init__": (
        "(self, root: 'Path', *, clock: 'Callable[[], datetime]' = utc_now, "
        "judge_interval: 'timedelta | int | float' = datetime.timedelta(seconds=3600), "
        "automatic_cooldown: 'timedelta | int | float' = "
        "datetime.timedelta(seconds=3600), manual_cooldown: "
        "'timedelta | int | float' = datetime.timedelta(seconds=3600), "
        "recent_contact_window: 'timedelta | int | float' = "
        "datetime.timedelta(seconds=1800), effect_ttl: 'timedelta | int | float' = "
        "datetime.timedelta(seconds=3600), anchor_hour: 'int' = 6, "
        "timezone_name: 'str' = 'UTC', **kwargs: 'Any')",
        (
            "root",
            "clock",
            "judge_interval",
            "automatic_cooldown",
            "manual_cooldown",
            "recent_contact_window",
            "effect_ttl",
            "anchor_hour",
            "timezone_name",
            "kwargs",
        ),
    ),
    "HeartbeatCadence.observer_status": (
        "(self, *, target_date: 'date', now: 'datetime') -> "
        "'tuple[ObservationFact, ...]'",
        ("target_date", "now", "return"),
    ),
    "HeartbeatCadence.snooze": (
        "(self, minutes: 'int', *, manual: 'bool') -> 'datetime'",
        ("minutes", "manual", "return"),
    ),
    "HeartbeatCadence.resume": (
        "(self) -> 'None'",
        ("return",),
    ),
    "HeartbeatCadence.observe_private_reply": (
        "(self, observed_at: 'datetime | None' = None) -> 'None'",
        ("observed_at", "return"),
    ),
    "HeartbeatCadence.apply_silence_backoff": (
        "(self, receipt: 'HeartbeatSilenceReceipt', *, policy: "
        "'Mapping[str, Any]', now: 'datetime | None' = None) -> 'dict[str, Any]'",
        ("receipt", "policy", "now", "return"),
    ),
    "HeartbeatCadence.cooldown": (
        "(self, kind: 'str', *, now: 'datetime | None' = None, bypass: "
        "'Iterable[str] | None' = None) -> 'tuple[bool, str, datetime | None]'",
        ("kind", "now", "bypass", "return"),
    ),
    "HeartbeatCadence.daily_anchor_due": (
        "(self, now: 'datetime | None' = None, *, kind: 'str' = "
        "'daily_anchor') -> 'bool'",
        ("now", "kind", "return"),
    ),
    "HeartbeatCadence.mark_daily_anchor": (
        "(self, epoch: 'str | None' = None, *, kind: 'str' = 'daily_anchor', "
        "now: 'datetime | None' = None) -> 'str'",
        ("epoch", "kind", "now", "return"),
    ),
    "HeartbeatCadence.next_judge_at": (
        "(self, now: 'datetime | None' = None) -> 'datetime'",
        ("now", "return"),
    ),
    "HeartbeatCadence.mark_judge": (
        "(self, *, now: 'datetime | None' = None, next_judge_at: "
        "'datetime | str | None' = None, cadence_minutes: 'int | None' = None, "
        "anchor_epoch: 'str | None' = None, anchor_kind: 'str | None' = None) "
        "-> 'datetime'",
        (
            "now",
            "next_judge_at",
            "cadence_minutes",
            "anchor_epoch",
            "anchor_kind",
            "return",
        ),
    ),
    "HeartbeatCadence.snapshot": (
        "(self, *, now: 'datetime | None' = None) -> 'dict[str, Any]'",
        ("now", "return"),
    ),
    "HeartbeatCadence.status": (
        "(self, *, now: 'datetime | None' = None) -> 'dict[str, Any]'",
        ("now", "return"),
    ),
    "HeartbeatKindPolicy": (
        "(enabled: 'bool', profile: 'str', judge: 'str', host_only: 'bool', "
        "bypass: 'frozenset[str]' = frozenset()) -> None",
        ("enabled", "profile", "judge", "host_only", "bypass"),
    ),
    "HeartbeatSilenceReceipt": (
        "(receipt_id: 'str', completed_at: 'datetime', profile: 'str', settled: "
        "'bool', intentional_silence: 'bool', judge_terminal: 'str', "
        "wake_terminal: 'str', delivery_terminal: 'str', manual_override: 'bool' "
        "= False) -> None",
        (
            "receipt_id",
            "completed_at",
            "profile",
            "settled",
            "intentional_silence",
            "judge_terminal",
            "wake_terminal",
            "delivery_terminal",
            "manual_override",
        ),
    ),
    "HeartbeatCandidate": (
        "(kind: 'str', context: 'Mapping[str, Any]' = <factory>, candidate_id: "
        "'str' = '', session_receipt: 'SessionHookReceipt | None' = None) -> None",
        ("kind", "context", "candidate_id", "session_receipt"),
    ),
    "JudgeDecision": (
        "(wake_main: 'bool', dm_user: 'bool', reason: 'str', message: 'str' = '', "
        "allow_autonomy: 'bool | None' = None, maintenance: 'bool | None' = None, "
        "next_judge_at: 'datetime | str | None' = None, cadence_minutes: "
        "'int | None' = None, delivery_mode: 'str' = 'direct') -> None",
        (
            "wake_main",
            "dm_user",
            "reason",
            "message",
            "allow_autonomy",
            "maintenance",
            "next_judge_at",
            "cadence_minutes",
            "delivery_mode",
        ),
    ),
    "EffectResult": (
        "(ok: 'bool', status: 'str', receipt: 'EffectReceipt | None' = None, "
        "verified: 'bool' = False, effect_id: 'str | None' = None, terminal: "
        "'str | None' = None, reason_code: 'HeartbeatReasonCode | None' = None, "
        "degraded: 'bool' = False, projection_errors: 'tuple[str, ...]' = ()) -> "
        "None",
        (
            "ok",
            "status",
            "receipt",
            "verified",
            "effect_id",
            "terminal",
            "reason_code",
            "degraded",
            "projection_errors",
        ),
    ),
    "HeartbeatResult": (
        "(status: 'str', reason: 'str', candidate_id: 'str', gate: 'GateResult', "
        "decision: 'JudgeDecision | None' = None, delivery: 'EffectResult | None' "
        "= None, wake: 'EffectResult | None' = None, reason_code: "
        "'HeartbeatReasonCode | None' = None, next_judge_at: 'datetime | None' = "
        "None, snapshot: 'Mapping[str, Any] | None' = None, degraded: 'bool' = "
        "False, projection_errors: 'tuple[str, ...]' = (), epoch_id: 'str | None' "
        "= None) -> None",
        (
            "status",
            "reason",
            "candidate_id",
            "gate",
            "decision",
            "delivery",
            "wake",
            "reason_code",
            "next_judge_at",
            "snapshot",
            "degraded",
            "projection_errors",
            "epoch_id",
        ),
    ),
}


def _resolve_public_target(path: str) -> object:
    target: object = heartbeat
    for part in path.split("."):
        target = getattr(target, part)
    return target


def _stable_signature(target: object) -> str:
    # inspect includes the clock function's process-local address in its repr.
    clock_default = (
        inspect.signature(heartbeat.HeartbeatCadence.__init__)
        .parameters["clock"]
        .default
    )
    return str(inspect.signature(target)).replace(repr(clock_default), "utc_now")


def test_heartbeat_public_manifest_is_exact() -> None:
    assert heartbeat.__all__ == HEARTBEAT_PUBLIC_MANIFEST


@pytest.mark.parametrize(
    "name",
    [
        "CADENCE_SCHEMA_V1",
        "CADENCE_SCHEMA_V2",
        "CADENCE_SCHEMA_V3",
        "CADENCE_SCHEMA_V4",
        "HeartbeatSilenceReceipt",
    ],
)
def test_package_root_reuses_heartbeat_objects(name: str) -> None:
    assert getattr(moonbite_plugin, name) is getattr(heartbeat, name)


@pytest.mark.parametrize(("name", "identity"), PUBLIC_HEARTBEAT_TYPES.items())
def test_public_heartbeat_type_reflection_identity(
    name: str,
    identity: tuple[str, str],
) -> None:
    public_type = getattr(heartbeat, name)
    assert (public_type.__module__, public_type.__qualname__) == identity


def test_cadence_status_remains_snapshot_alias() -> None:
    assert heartbeat.HeartbeatCadence.status is heartbeat.HeartbeatCadence.snapshot


@pytest.mark.parametrize(
    ("path", "expected_signature", "expected_hint_keys"),
    (
        (path, signature, hint_keys)
        for path, (signature, hint_keys) in PUBLIC_SIGNATURES.items()
    ),
)
def test_public_signatures_and_type_hints_remain_compatible(
    path: str,
    expected_signature: str,
    expected_hint_keys: tuple[str, ...],
) -> None:
    target = _resolve_public_target(path)
    hints = get_type_hints(target)

    assert _stable_signature(target) == expected_signature
    assert tuple(hints) == expected_hint_keys
    assert all(not isinstance(value, str) for value in hints.values())


def test_cadence_observer_status_keeps_read_only_contract_docstring() -> None:
    assert inspect.getdoc(heartbeat.HeartbeatCadence.observer_status) == (
        "Project cadence state without normalising, pruning, or locking."
    )


def test_cadence_silence_backoff_keeps_atomic_contract_docstring() -> None:
    assert inspect.getdoc(heartbeat.HeartbeatCadence.apply_silence_backoff) == (
        "Atomically dedupe a settled silence and update cadence cooldown."
    )
