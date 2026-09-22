"""Locked Heartbeat coordination after a Judge decision is validated."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any

from ..runtime_core import StateError


def execute_decision(
    candidate: Any,
    decision: Any,
    now: datetime,
    gate: Any,
    policy: Any,
    *,
    make_result: Callable[..., Any],
    cadence: Any,
    ensure_effect_plan: Callable[[Any, Any, datetime], Mapping[str, Any] | None],
    accepts_keyword: Callable[[Callable[..., Any], str], bool],
    optional_time: Callable[[Any, str], datetime | None],
    prepare_effect_intent: Callable[..., Any],
    plan_effect: Callable[[Mapping[str, Any] | None, str], Mapping[str, Any] | None],
    run_effect: Callable[..., Any],
    reason_codes: Any,
    default_recent_contact_window: timedelta,
) -> Any:
    """Persist the approved plan, consume cadence, and execute all effects."""

    candidate_id = candidate.candidate_id
    anchor_epoch = None
    if policy.profile == "daily_anchor":
        daily_anchor_epoch = getattr(cadence, "daily_anchor_epoch", None)
        if callable(daily_anchor_epoch):
            try:
                anchor_epoch = daily_anchor_epoch(now)
            except Exception:
                return make_result(
                    "failed",
                    "cadence_state_error",
                    candidate_id,
                    gate,
                    code=reason_codes.CADENCE_ERROR,
                    decision=decision,
                )
    # Keep the approved effect set durable before consuming cadence. A crash
    # after mark_judge must replay as pending rather than silently skipping a
    # daily anchor whose effect intents were never created.
    try:
        effect_plan = ensure_effect_plan(candidate, decision, now)
    except (StateError, TypeError, ValueError) as exc:
        return make_result(
            "failed",
            "effect_replay_error",
            candidate_id,
            gate,
            code=reason_codes.EFFECT_REPLAY_ERROR,
            decision=decision,
            projection_errors=(f"effect_plan:{type(exc).__name__}",),
        )
    mark_judge = getattr(cadence, "mark_judge", None)
    if callable(mark_judge):
        mark_kwargs = {
            "now": now,
            "next_judge_at": decision.next_judge_at,
            "cadence_minutes": decision.cadence_minutes,
            "anchor_epoch": anchor_epoch,
        }
        if anchor_epoch is not None and accepts_keyword(mark_judge, "anchor_kind"):
            mark_kwargs["anchor_kind"] = candidate.kind
    if not callable(mark_judge):
        next_judge = (
            optional_time(decision.next_judge_at, "next_judge_at")
            if decision.next_judge_at is not None
            else None
        )
    else:
        try:
            next_judge = mark_judge(**mark_kwargs)
        except Exception:
            return make_result(
                "failed",
                "cadence_state_error",
                candidate_id,
                gate,
                code=reason_codes.CADENCE_ERROR,
                decision=decision,
            )
    if not decision.wake_main and not decision.dm_user:
        if decision.allow_autonomy is True or decision.maintenance is True:
            return make_result(
                "allowed",
                "allowed",
                candidate_id,
                gate,
                code=reason_codes.ALLOWED,
                decision=decision,
                next_judge_at=next_judge,
            )
        return make_result(
            "skipped",
            decision.reason,
            candidate_id,
            gate,
            code=reason_codes.DENIED,
            decision=decision,
            next_judge_at=next_judge,
        )
    prepared: dict[str, Any] = {}
    try:
        for kind, enabled in (
            ("delivery", decision.dm_user),
            ("wake", decision.wake_main),
        ):
            if enabled:
                prepared[kind] = prepare_effect_intent(
                    kind,
                    candidate,
                    decision,
                    now,
                    planned=plan_effect(effect_plan, kind),
                )
    except (StateError, TypeError, ValueError) as exc:
        if effect_plan is not None:
            return make_result(
                "pending",
                "awaiting_effect_intent",
                candidate_id,
                gate,
                code=reason_codes.EFFECT_PENDING,
                decision=decision,
                next_judge_at=now
                + getattr(
                    cadence,
                    "recent_contact_window",
                    default_recent_contact_window,
                ),
                projection_errors=(f"effect_intent:{type(exc).__name__}",),
            )
        return make_result(
            "failed",
            "effect_intent_error",
            candidate_id,
            gate,
            code=reason_codes.EFFECT_REPLAY_ERROR,
            decision=decision,
            next_judge_at=next_judge,
            projection_errors=(f"effect_intent:{type(exc).__name__}",),
        )
    delivery = (
        run_effect(
            "delivery",
            candidate,
            decision,
            now,
            planned=plan_effect(effect_plan, "delivery"),
            prepared=prepared.get("delivery"),
        )
        if decision.dm_user
        else None
    )
    wake = (
        run_effect(
            "wake",
            candidate,
            decision,
            now,
            planned=plan_effect(effect_plan, "wake"),
            prepared=prepared.get("wake"),
        )
        if decision.wake_main
        else None
    )
    effects = [effect for effect in (delivery, wake) if effect is not None]
    if any(not effect.ok for effect in effects):
        failure_code = next(
            (
                effect.reason_code
                for effect in effects
                if not effect.ok and effect.reason_code is not None
            ),
            reason_codes.EFFECT_ERROR,
        )
        return make_result(
            "failed",
            "effect_failed",
            candidate_id,
            gate,
            code=failure_code,
            decision=decision,
            delivery=delivery,
            wake=wake,
            next_judge_at=next_judge,
        )
    if all(effect.verified for effect in effects):
        return make_result(
            "completed",
            "effects_verified",
            candidate_id,
            gate,
            code=reason_codes.ALLOWED,
            decision=decision,
            delivery=delivery,
            wake=wake,
            next_judge_at=next_judge,
        )
    return make_result(
        "pending",
        "effects_accepted_unverified",
        candidate_id,
        gate,
        code=reason_codes.EFFECT_PENDING,
        decision=decision,
        delivery=delivery,
        wake=wake,
        next_judge_at=next_judge,
    )
