"""Provider admission, history, and stable selection helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Callable


def audit_history(
    *,
    bus: Any,
    effect_ledger: Any,
    effect_kind: str,
    record_value: Callable[..., Any],
    record_provider: Callable[[Any], str | None],
    record_state: Callable[[Any], str],
) -> list[dict[str, Any]]:
    """Return EventBus telemetry with durable effect fallback.

    EventBus remains the primary telemetry owner.  Effect records fill
    only the gap where an audit projection was unavailable, so limits
    cannot be bypassed by a failed audit write.
    """

    rows: list[dict[str, Any]] = []
    seen_effect_ids: set[str] = set()
    try:
        events = bus.read_audit()
    except Exception:
        events = ()
    for event in events:
        if getattr(event, "kind", None) != "audit.autonomy":
            continue
        payload = getattr(event, "payload", {})
        if not isinstance(payload, Mapping):
            continue
        if payload.get("status") not in {
            "completed",
            "executed_unverified",
            "failed",
        }:
            continue
        provider = payload.get("provider")
        if not isinstance(provider, str) or not provider:
            continue
        row = dict(payload)
        row["created_at"] = getattr(event, "created_at", None)
        rows.append(row)

        effect_id = payload.get("effect_id")
        if isinstance(effect_id, str) and effect_id:
            seen_effect_ids.add(effect_id)

    try:
        records = effect_ledger.records()
    except Exception:
        if rows:
            return rows
        raise
    record_iter = records.values() if isinstance(records, Mapping) else records
    for record in record_iter:
        if record_value(record, "kind") != effect_kind:
            continue
        effect_id = record_value(record, "effect_id")
        if not isinstance(effect_id, str) or not effect_id:
            continue
        if effect_id in seen_effect_ids:
            continue
        provider = record_provider(record)
        if not provider:
            continue
        state = record_state(record)
        rows.append(
            {
                "provider": provider,
                "status": "completed" if state == "verified" else state,
                "effect_id": effect_id,
                "source_event_id": record_value(record, "source_event_id"),
                "idempotency_key": record_value(record, "idempotency_key"),
                "created_at": record_value(record, "created_at"),
            }
        )
    return rows


def provider_history(
    name: str,
    *,
    exclude_effect_id: str | None = None,
    audit_history: Callable[[], list[dict[str, Any]]],
    record_value: Callable[..., Any],
    record_provider: Callable[[Any], str | None],
) -> list[Any]:
    def is_excluded(record: Any) -> bool:
        return (
            exclude_effect_id is not None
            and record_value(record, "effect_id") == exclude_effect_id
        )

    rows = [
        record
        for record in audit_history()
        if record_provider(record) == name and not is_excluded(record)
    ]
    latest_by_effect: dict[str, Any] = {}
    without_effect: list[Any] = []
    for record in rows:
        effect_id = record_value(record, "effect_id")
        if is_excluded(record):
            continue
        if isinstance(effect_id, str) and effect_id:
            latest_by_effect[effect_id] = record
        else:
            without_effect.append(record)
    return without_effect + list(latest_by_effect.values())


def validate_provider_settings(
    settings: Mapping[str, Mapping[str, Any]],
    *,
    bounded: Callable[..., str],
    optional_gate_set: Callable[[Any, str], frozenset[str]],
    nonnegative_number: Callable[[Any, str], float | None],
    positive_limit: Callable[[Any, str], int | None],
    settings_error: type[Exception],
    cost_units: Mapping[str, int],
) -> None:
    if not isinstance(settings, Mapping):
        raise settings_error(None, "settings")
    for name, provider_settings in settings.items():
        try:
            provider_name = bounded(name, "provider name", max_bytes=128)
        except (TypeError, ValueError) as exc:
            raise settings_error(None, "provider_name") from exc
        if not isinstance(provider_settings, Mapping):
            raise settings_error(provider_name, "settings")

        known_fields = {
            "enabled",
            "weight",
            "allowed_sources",
            "allowed_channels",
            "cooldown",
            "effect_ttl",
            "daily_limit",
            "repeat_limit",
            "cost_budget",
            "cost",
            "cost_class",
        }
        if any(field not in known_fields for field in provider_settings):
            raise settings_error(provider_name, "unknown")

        if (
            "enabled" in provider_settings
            and type(provider_settings["enabled"]) is not bool
        ):
            raise settings_error(provider_name, "enabled")
        if "weight" in provider_settings:
            weight = provider_settings["weight"]
            if type(weight) is not int or not 1 <= weight <= 100:
                raise settings_error(provider_name, "weight")
        for field_name in ("allowed_sources", "allowed_channels"):
            if field_name in provider_settings:
                try:
                    optional_gate_set(provider_settings[field_name], field_name)
                except (TypeError, ValueError) as exc:
                    raise settings_error(provider_name, field_name) from exc

        for field_name in ("cooldown", "effect_ttl"):
            if field_name not in provider_settings:
                continue
            value = provider_settings[field_name]
            if value is None:
                continue
            try:
                seconds = (
                    value.total_seconds()
                    if isinstance(value, timedelta)
                    else nonnegative_number(value, field_name)
                )
            except (TypeError, ValueError) as exc:
                raise settings_error(provider_name, field_name) from exc
            if (
                seconds is None
                or seconds < 0
                or seconds > 31 * 24 * 60 * 60
                or field_name == "effect_ttl"
                and seconds <= 0
            ):
                raise settings_error(provider_name, field_name)

        for field_name in (
            "daily_limit",
            "repeat_limit",
            "cost_budget",
            "cost",
        ):
            if field_name not in provider_settings:
                continue
            try:
                positive_limit(provider_settings[field_name], field_name)
            except (TypeError, ValueError) as exc:
                raise settings_error(provider_name, field_name) from exc

        if "cost_class" in provider_settings:
            cost_class = provider_settings["cost_class"]
            if type(cost_class) is not str or cost_class not in cost_units:
                raise settings_error(provider_name, "cost_class")


def eligible_reason(
    provider: Any,
    provider_settings: Mapping[str, Any],
    context: Any,
    *,
    exclude_effect_id: str | None = None,
    optional_gate_set: Callable[[Any, str], frozenset[str]],
    setting: Callable[[Any, Mapping[str, Any], str, Any], Any],
    provider_history: Callable[..., list[Any]],
    eligibility_error: type[Exception],
    cooldown_seconds: Callable[[Any], float],
    record_time: Callable[[Any], datetime | None],
    positive_limit: Callable[[Any, str], int | None],
    record_value: Callable[..., Any],
    cost_units: Mapping[str, int],
) -> str | None:
    facts = context.facts
    source = facts.get("source", facts.get("source_kind"))
    channel = facts.get("channel")
    allowed_sources = optional_gate_set(
        setting(
            provider, provider_settings, "allowed_sources", provider.allowed_sources
        ),
        "allowed_sources",
    )
    allowed_channels = optional_gate_set(
        setting(
            provider,
            provider_settings,
            "allowed_channels",
            provider.allowed_channels,
        ),
        "allowed_channels",
    )
    if allowed_sources and source not in allowed_sources:
        return "source_not_allowed"
    if allowed_channels and channel not in allowed_channels:
        return "channel_not_allowed"

    try:
        history = provider_history(provider.name, exclude_effect_id=exclude_effect_id)
    except Exception as exc:
        raise eligibility_error(provider.name, exc) from exc
    now = context.now
    cooldown = setting(provider, provider_settings, "cooldown", provider.cooldown)
    if cooldown is not None:
        seconds = cooldown_seconds(cooldown)
        recent = [record_time(record) for record in history]
        recent = [item for item in recent if item is not None]
        if recent and (now - max(recent)).total_seconds() < seconds:
            return "cooldown"

    daily_limit = setting(
        provider, provider_settings, "daily_limit", provider.daily_limit
    )
    if daily_limit is not None:
        daily_limit = positive_limit(daily_limit, "daily_limit")
        count = sum(
            1
            for record in history
            if record_time(record) is not None
            and record_time(record).date() == now.date()
        )
        if count >= daily_limit:
            return "daily_limit"

    repeat_limit = setting(
        provider, provider_settings, "repeat_limit", provider.repeat_limit
    )
    if repeat_limit is not None:
        repeat_limit = positive_limit(repeat_limit, "repeat_limit")
        repeat_key = facts.get("repeat_key", facts.get("source_event_id"))
        if repeat_key is not None:
            repeated = sum(
                1
                for record in history
                if record_value(record, "source_event_id") == repeat_key
            )
            if repeated >= repeat_limit:
                return "repeat_limit"

    cost_class = setting(provider, provider_settings, "cost_class", provider.cost_class)
    units = cost_units.get(str(cost_class).casefold(), 1)
    cost = setting(provider, provider_settings, "cost", units)
    try:
        units = max(1, int(cost))
    except (TypeError, ValueError):
        units = 1
    remaining = facts.get("cost_budget_remaining", facts.get("cost_budget"))
    if remaining is not None:
        try:
            if float(remaining) < units:
                return "cost_budget"
        except (TypeError, ValueError):
            return "cost_budget"
    budget = setting(provider, provider_settings, "cost_budget", provider.cost_budget)
    if budget is not None:
        budget = positive_limit(budget, "cost_budget")
        spent = 0
        for record in history:
            when = record_time(record)
            if when is not None and when.date() == now.date():
                spent += units
        if spent + units > budget:
            return "cost_budget"

    try:
        if not provider.eligible(context):
            return "provider_ineligible"
    except Exception as exc:
        raise eligibility_error(provider.name, exc) from exc
    return None


def eligible(
    settings: Mapping[str, Mapping[str, Any]],
    context: Any,
    *,
    registry_get: Callable[[str], Any],
    eligible_reason: Callable[[Any, Mapping[str, Any], Any], str | None],
) -> list[tuple[str, int]]:
    result: list[tuple[str, int]] = []
    for name, provider_settings in sorted(settings.items()):
        if provider_settings.get("enabled") is not True:
            continue
        provider = registry_get(name)
        if provider is None:
            continue
        reason = eligible_reason(provider, provider_settings, context)
        if reason is not None:
            continue
        try:
            weight = int(provider_settings.get("weight", 1))
        except (TypeError, ValueError):
            weight = 1
        result.append((name, max(1, min(weight, 100))))
    return result


def eligible_with_reasons(
    settings: Mapping[str, Mapping[str, Any]],
    context: Any,
    *,
    registry_get: Callable[[str], Any],
    eligible_reason: Callable[[Any, Mapping[str, Any], Any], str | None],
) -> tuple[list[tuple[str, int]], dict[str, str]]:
    candidates: list[tuple[str, int]] = []
    reasons: dict[str, str] = {}
    for name, provider_settings in sorted(settings.items()):
        if provider_settings.get("enabled") is not True:
            reasons[name] = "disabled"
            continue
        provider = registry_get(name)
        if provider is None:
            reasons[name] = "provider_not_registered"
            continue
        reason = eligible_reason(provider, provider_settings, context)
        if reason is not None:
            reasons[name] = reason
            continue
        try:
            weight = int(provider_settings.get("weight", 1))
        except (TypeError, ValueError):
            weight = 1
        candidates.append((name, max(1, min(weight, 100))))
    return candidates, reasons


def weighted_selection(
    candidates: list[tuple[str, int]], occurrence_identity: str
) -> str:
    """Choose reproducibly from bounded weights for one occurrence."""

    ordered = sorted(candidates)
    identity = json.dumps(
        {
            "candidates": ordered,
            "occurrence": occurrence_identity,
            "selection_contract": "moon.autonomy.weighted.v1",
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    slot = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big") % sum(
        weight for _name, weight in ordered
    )
    for name, weight in ordered:
        if slot < weight:
            return name
        slot -= weight
    raise AssertionError("bounded weighted selection exhausted")


__all__ = ()
