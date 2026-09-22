"""Locked autonomy execution coordinator.

The public engine owns the runtime lock and durable stores; this module only
coordinates one already-locked attempt through those existing ports.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any


def run_once_locked(
    owner: Any,
    settings: Mapping[str, Mapping[str, Any]],
    *,
    facts: Mapping[str, Any] | None = None,
    result_type: type[Any],
    context_type: type[Any],
    request_type: type[Any],
    effect_record_type: type[Any],
    effect_kind: str,
    provider_eligibility_error_type: type[Exception],
    provider_settings_error_type: type[Exception],
    default_effect_ttl: timedelta,
    new_effect_id: Callable[[str], str],
    evaluate_gate_fn: Callable[[Any], Any],
    new_id_fn: Callable[[str], str],
) -> Any:
    resolution = owner.controls.resolve("autonomy")
    gate = evaluate_gate_fn(resolution)
    now = owner.clock()
    context = context_type(now, {} if facts is None else dict(facts))
    run_id = new_id_fn("autonomy_run")
    try:
        source_override, epoch_override, idempotency_override = (
            owner._identity_overrides(context.facts)
        )
    except ValueError as exc:
        return owner._finish(result_type("failed", None, str(exc), run_id=run_id), gate)
    source_event_id = source_override or run_id
    # ``epoch_id`` is optional at the public terminal boundary.  The
    # effect ledger still needs a durable epoch for its immutable schema;
    # legacy callers use the date-derived internal value while retaining
    # their original no-epoch audit/key identity.
    epoch_id = epoch_override
    effect_epoch_id = epoch_override or (f"autonomy:{context.now.date().isoformat()}")

    def finish(
        result: Any,
        _gate: Any | None = None,
        *,
        record_terminal: bool = True,
    ) -> Any:
        if (
            result.source_event_id is None
            or result.canonical_event_id is None
            or result.epoch_id is None
            and epoch_id is not None
        ):
            result = replace(
                result,
                run_id=result.run_id or run_id,
                source_event_id=result.source_event_id or source_event_id,
                canonical_event_id=result.canonical_event_id or source_event_id,
                epoch_id=result.epoch_id or epoch_id,
            )
        return owner._finish(result, gate, record_terminal=record_terminal)

    requested: str | None = None
    payload = resolution.intent.payload if resolution.intent is not None else {}
    if gate.mode == "play_next" and isinstance(payload, Mapping):
        value = payload.get("provider")
        if isinstance(value, str) and value.strip():
            requested = value
    existing_selection: Any | None = None
    try:
        if idempotency_override is not None:
            existing_selection = owner._find_by_idempotency(idempotency_override)
        if (
            existing_selection is None
            and source_override is not None
            and epoch_override is not None
        ):
            existing_selection = owner._find_by_occurrence(
                source_event_id, effect_epoch_id
            )
        if (
            existing_selection is None
            and source_override is not None
            and epoch_override is None
        ):
            existing_selection = owner._find_implicit_occurrence(
                source_event_id,
                requested_epoch_id=effect_epoch_id,
            )
        if existing_selection is not None:
            if owner._record_value(existing_selection, "kind") != effect_kind:
                raise ValueError("occurrence_conflict")
            recorded_source = owner._record_value(existing_selection, "source_event_id")
            recorded_epoch = owner._record_value(existing_selection, "epoch_id")
            if source_override is not None and source_override != recorded_source:
                raise ValueError("occurrence_conflict")
            if epoch_override is not None and epoch_override != recorded_epoch:
                raise ValueError("occurrence_conflict")
            source_event_id = recorded_source
            effect_epoch_id = recorded_epoch
            if (
                idempotency_override is not None
                and epoch_override is None
                and owner._audit_identity_for_record(
                    source_event_id, existing_selection
                )
                != "implicit"
            ):
                # An idempotency key identifies a durable effect, but it
                # does not supply the missing public epoch.  Refuse an
                # explicit effect under a legacy request instead of
                # replaying it across public identity boundaries.
                raise ValueError("implicit_identity_unavailable")
            if (
                epoch_override is not None
                and epoch_override == recorded_epoch
                and owner._public_epoch_from_record(existing_selection) is None
                and owner._audit_identity_for_record(
                    source_event_id, existing_selection
                )
                != "explicit"
            ):
                # A date-shaped internal epoch can belong either to a
                # legacy occurrence or to an explicit caller value.  A
                # source-plus-epoch retry cannot resolve that collision
                # from the ledger alone, so leave the record untouched.
                raise ValueError("implicit_identity_unavailable")
    except ValueError as exc:
        if str(exc) == "implicit_identity_unavailable":
            # The ledger candidate remains untouched, but its internal
            # epoch is not enough to prove a legacy public identity.  Do
            # not turn that uncertainty into a permanent terminal that
            # would poison a later retry once valid evidence exists.
            return finish(
                result_type(
                    "awaiting_reconciliation",
                    None,
                    "implicit_identity_unavailable",
                    run_id=run_id,
                    source_event_id=source_event_id,
                    canonical_event_id=source_event_id,
                ),
                gate,
                record_terminal=False,
            )
        reason = "occurrence_conflict"
        return finish(result_type("failed", None, reason), gate)
    except Exception as exc:
        return finish(
            result_type("failed", None, f"effect_lookup_error:{type(exc).__name__}"),
            gate,
        )

    # Resolve an existing canonical terminal before current admission
    # gates.  A prior no-effect skip is still the durable fact for this
    # occurrence, while an effect-bearing terminal is checked against the
    # exact ledger epoch before its state is replayed.
    try:
        existing_terminal = owner._existing_terminal_result(
            source_event_id,
            epoch_id=epoch_id,
            ledger_epoch_id=effect_epoch_id,
        )
    except Exception as exc:
        return finish(
            result_type(
                "failed",
                None,
                f"terminal_integrity_error:{type(exc).__name__}",
            ),
            gate,
        )
    if existing_terminal is not None:
        return existing_terminal

    unexecuted_intent = (
        existing_selection is not None
        and owner._record_state(existing_selection) == "intent"
    )
    intent_provider = (
        owner._record_provider(existing_selection) if unexecuted_intent else None
    )
    intent_effect_id = (
        owner._record_value(existing_selection, "effect_id")
        if unexecuted_intent
        else None
    )

    def intent_skip(reason: str) -> Any:
        return result_type(
            "skipped",
            intent_provider,
            reason,
            run_id=run_id,
            effect_id=intent_effect_id,
            evidence=owner._record_evidence(existing_selection),
            source_event_id=source_event_id,
            idempotency_key=owner._record_value(existing_selection, "idempotency_key"),
            effect_record=(
                existing_selection
                if isinstance(existing_selection, effect_record_type)
                else None
            ),
            canonical_event_id=source_event_id,
            epoch_id=epoch_id,
        )

    owner._settle_expired_unverified(now=now, gate=gate)

    # Resolve the durable effect before evaluating current gates.  A
    # pending or executed effect belongs to the host's reconciliation
    # path; replaying it through a current chat/control gate would append
    # a misleading terminal skip for work that already started.
    if existing_selection is not None:
        refreshed = owner.effect_ledger.get(
            owner._record_value(existing_selection, "effect_id")
        )
        if refreshed is not None:
            existing_selection = refreshed
        if owner._record_state(existing_selection) != "intent":
            selected_existing = owner._record_provider(existing_selection)
            reconciled = owner._existing_result(
                existing_selection,
                provider=selected_existing or "unknown",
                gate=gate,
                run_id=owner._record_value(existing_selection, "effect_id"),
                public_epoch_id=epoch_id,
            )
            if reconciled is not None:
                return reconciled
            if selected_existing is None:
                return finish(
                    result_type(
                        "awaiting_reconciliation",
                        None,
                        "selection_provider_unavailable",
                        run_id=run_id,
                        effect_id=owner._record_value(existing_selection, "effect_id"),
                        evidence=owner._record_evidence(existing_selection),
                        source_event_id=source_event_id,
                        idempotency_key=owner._record_value(
                            existing_selection, "idempotency_key"
                        ),
                        effect_record=(
                            existing_selection
                            if isinstance(existing_selection, effect_record_type)
                            else None
                        ),
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                )

    if not gate.allowed:
        if unexecuted_intent:
            return finish(
                intent_skip(gate.reason),
                gate,
                record_terminal=False,
            )
        return finish(result_type("skipped", None, gate.reason))

    # Active-chat is a hard gate for new effects and unexecuted intents.
    # Existing pending/executed effects were replayed above so that the
    # host can reconcile an already-started operation.
    for chat_key in ("active_chat", "chat_active"):
        if chat_key in context.facts and type(context.facts[chat_key]) is not bool:
            return finish(result_type("failed", None, f"{chat_key}_invalid"), gate)
        if context.facts.get(chat_key) is True:
            if unexecuted_intent:
                return finish(
                    intent_skip("active_chat"),
                    gate,
                    record_terminal=False,
                )
            return finish(result_type("skipped", None, "active_chat"))

    selected: str | None = None
    selection_reason = "selected"
    if existing_selection is not None:
        selected = owner._record_provider(existing_selection)
        selection_state = owner._record_state(existing_selection)
        if selection_state == "intent":
            effect_id = owner._record_value(existing_selection, "effect_id")
            try:
                owner._validate_provider_settings(settings)
            except provider_settings_error_type as exc:
                return finish(
                    result_type(
                        "failed",
                        selected,
                        f"invalid_provider_settings:{exc.field}",
                        run_id=run_id,
                        effect_id=effect_id,
                        evidence=owner._record_evidence(existing_selection),
                        source_event_id=source_event_id,
                        idempotency_key=owner._record_value(
                            existing_selection, "idempotency_key"
                        ),
                        effect_record=(
                            existing_selection
                            if isinstance(existing_selection, effect_record_type)
                            else None
                        ),
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                )
            provider_settings = settings.get(selected) if selected is not None else None
            provider = owner.registry.get(selected) if selected is not None else None
            if (
                provider is None
                or not isinstance(provider_settings, Mapping)
                or provider_settings.get("enabled") is not True
            ):
                return finish(
                    intent_skip("no_eligible_provider"),
                    gate,
                    record_terminal=False,
                )
            try:
                eligibility_reason = owner._eligible_reason(
                    provider,
                    provider_settings,
                    context,
                    exclude_effect_id=effect_id,
                )
            except provider_eligibility_error_type as exc:
                return finish(
                    result_type(
                        "failed",
                        selected,
                        f"eligibility_error:{type(exc.cause).__name__}",
                        run_id=run_id,
                        effect_id=effect_id,
                        evidence=owner._record_evidence(existing_selection),
                        source_event_id=source_event_id,
                        idempotency_key=owner._record_value(
                            existing_selection, "idempotency_key"
                        ),
                        effect_record=(
                            existing_selection
                            if isinstance(existing_selection, effect_record_type)
                            else None
                        ),
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                )
            except (TypeError, ValueError):
                return finish(
                    result_type(
                        "failed",
                        selected,
                        "invalid_provider_settings",
                        run_id=run_id,
                        effect_id=effect_id,
                        evidence=owner._record_evidence(existing_selection),
                        source_event_id=source_event_id,
                        idempotency_key=owner._record_value(
                            existing_selection, "idempotency_key"
                        ),
                        effect_record=(
                            existing_selection
                            if isinstance(existing_selection, effect_record_type)
                            else None
                        ),
                        canonical_event_id=source_event_id,
                    ),
                    gate,
                )
            if eligibility_reason is not None:
                return finish(
                    intent_skip("no_eligible_provider"),
                    gate,
                    record_terminal=False,
                )
        selection_reason = "resumed_intent"
    else:
        try:
            owner._validate_provider_settings(settings)
        except provider_settings_error_type as exc:
            return finish(
                result_type(
                    "failed",
                    exc.provider,
                    f"invalid_provider_settings:{exc.field}",
                ),
                gate,
            )
        if requested is None and not any(
            provider_settings.get("enabled") is True
            and owner.registry.get(name) is not None
            for name, provider_settings in settings.items()
        ):
            return finish(result_type("skipped", None, "no_eligible_provider"), gate)
        try:
            decision = owner._validate_judge_decision(owner.judge.decide(context))
        except Exception as exc:
            return finish(
                result_type("failed", None, f"judge_error:{type(exc).__name__}"),
                gate,
            )
        if decision is None:
            return finish(result_type("failed", None, "judge_invalid_result"), gate)
        if not decision.allowed:
            return finish(result_type("skipped", None, decision.reason), gate)
        decision_weights = dict(decision.provider_weights)
        if any(
            name not in settings or owner.registry.get(name) is None
            for name in decision_weights
        ):
            return finish(result_type("failed", None, "judge_unknown_provider"), gate)
        try:
            candidates, reasons = owner._eligible_with_reasons(settings, context)
        except provider_eligibility_error_type as exc:
            return finish(
                result_type(
                    "failed",
                    exc.provider,
                    f"eligibility_error:{type(exc.cause).__name__}",
                ),
                gate,
            )
        except (TypeError, ValueError):
            return finish(
                result_type("failed", None, "invalid_provider_settings"), gate
            )
        candidates = [
            (name, decision_weights.get(name, weight))
            for name, weight in candidates
            if decision_weights.get(name, weight) > 0
        ]

        if idempotency_override is not None:
            occurrence_identity = idempotency_override
        else:
            occurrence_identity = json.dumps(
                {
                    "epoch_id": effect_epoch_id,
                    "source_event_id": source_event_id,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        if requested is not None:
            if any(name == requested for name, _weight in candidates):
                selected = requested
            else:
                fallback = (
                    payload.get("fallback_provider")
                    if isinstance(payload, Mapping)
                    else None
                )
                allow_fallback = isinstance(payload, Mapping) and (
                    payload.get("allow_fallback") is True
                    or payload.get("fallback") in {"first", "deterministic"}
                )
                if isinstance(fallback, str) and any(
                    name == fallback for name, _ in candidates
                ):
                    selected = fallback
                    selection_reason = "play_next_fallback"
                elif allow_fallback and candidates:
                    selected = candidates[0][0]
                    selection_reason = "play_next_fallback"
                else:
                    return finish(
                        result_type(
                            "skipped",
                            requested,
                            "play_next_unavailable:"
                            f"{reasons.get(requested, 'ineligible')}",
                        ),
                        gate,
                    )
        elif candidates:
            # The stable occurrence identity preserves weighted diversity
            # without process randomness. The runner is never re-rolled.
            selected = owner._weighted_selection(candidates, occurrence_identity)
            selection_reason = "weighted_replayable"
    if selected is None:
        return finish(result_type("skipped", None, "no_eligible_provider"), gate)

    provider = owner.registry.get(selected)
    if provider is None:
        if existing_selection is not None:
            return finish(
                result_type(
                    "awaiting_reconciliation",
                    selected,
                    "selected_provider_unavailable",
                    run_id=run_id,
                    effect_id=owner._record_value(existing_selection, "effect_id"),
                    evidence=owner._record_evidence(existing_selection),
                    source_event_id=source_event_id,
                    idempotency_key=owner._record_value(
                        existing_selection, "idempotency_key"
                    ),
                    effect_record=(
                        existing_selection
                        if isinstance(existing_selection, effect_record_type)
                        else None
                    ),
                    canonical_event_id=source_event_id,
                ),
                gate,
            )
        return finish(
            result_type(
                "failed",
                selected,
                "provider_not_registered",
                run_id=run_id,
            ),
            gate,
        )

    if existing_selection is not None:
        record = existing_selection
        digest = owner._record_value(record, "content_sha256")
        content_length = owner._record_value(record, "content_length")
        idempotency_key = owner._record_value(record, "idempotency_key")
    else:
        digest, generated_key, content_length = owner._effect_identity(
            selected, source_event_id, effect_epoch_id
        )
        idempotency_key = idempotency_override or generated_key
        provider_settings = settings.get(selected, {})
        ttl_value = provider_settings.get("effect_ttl", default_effect_ttl)
        if isinstance(ttl_value, timedelta):
            ttl = ttl_value
        else:
            try:
                ttl = timedelta(seconds=float(ttl_value))
            except (TypeError, ValueError):
                ttl = default_effect_ttl
        if ttl <= timedelta(0) or ttl > timedelta(days=31):
            ttl = default_effect_ttl
        try:
            record = owner.effect_ledger.begin_intent(
                effect_id=new_effect_id(selected),
                kind=effect_kind,
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                epoch_id=effect_epoch_id,
                content_sha256=digest,
                content_length=content_length,
                expires_at=context.now + ttl,
                created_at=context.now,
            )
        except Exception as exc:
            return finish(
                result_type(
                    "failed",
                    selected,
                    f"effect_intent_error:{type(exc).__name__}",
                    run_id=run_id,
                ),
                gate,
            )
        reconciled = owner._existing_result(
            record,
            provider=selected,
            gate=gate,
            run_id=run_id,
            public_epoch_id=epoch_id,
        )
        if reconciled is not None:
            return reconciled
    effect_id = owner._record_value(record, "effect_id")
    try:
        owner.bus.emit(
            "autonomy.started",
            source="autonomy",
            payload={
                "provider": selected,
                "selection": selection_reason,
                "effect_id": effect_id,
                "occurrence_id": source_event_id,
                "epoch_id": effect_epoch_id,
                "idempotency_key": idempotency_key,
            },
        )
    except Exception as exc:
        return finish(
            result_type(
                "failed",
                selected,
                f"started_event_error:{type(exc).__name__}",
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(record),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=record
                if isinstance(record, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )
    try:
        pending = owner.effect_ledger.mark_pending(effect_id)
    except Exception as exc:
        try:
            owner.effect_ledger.fail(effect_id, "effect_pending_error", False)
        except Exception:
            pass
        return finish(
            result_type(
                "failed",
                selected,
                f"effect_pending_error:{type(exc).__name__}",
                run_id=run_id,
                effect_id=effect_id,
            ),
            gate,
        )

    request = request_type(
        provider=selected,
        effect_id=effect_id,
        idempotency_key=idempotency_key,
        source_event_id=source_event_id,
        epoch_id=effect_epoch_id,
        content_sha256=digest,
        content_length=content_length,
        attempt=int(owner._record_value(pending, "attempt", 1)),
        context=context,
    )
    try:
        output = provider.run(request)
    except Exception as exc:
        try:
            failed = owner.effect_ledger.fail(
                effect_id, f"provider_error:{type(exc).__name__}", True
            )
        except Exception:
            failed = pending
        return finish(
            result_type(
                "failed",
                selected,
                f"provider_error:{type(exc).__name__}",
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(failed),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=failed
                if isinstance(failed, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )

    receipt, evidence_error = owner._receipt_from_output(output)
    if evidence_error is not None:
        try:
            failed = owner.effect_ledger.fail(effect_id, evidence_error, False)
        except Exception:
            failed = pending
        return finish(
            result_type(
                "failed",
                selected,
                evidence_error,
                output=output,
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(failed),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=failed
                if isinstance(failed, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )
    if receipt is None:
        try:
            unverified = owner.effect_ledger.mark_queue_accepted(effect_id)
        except Exception as exc:
            return finish(
                result_type(
                    "failed",
                    selected,
                    f"effect_queue_error:{type(exc).__name__}",
                    output=output,
                    run_id=run_id,
                    effect_id=effect_id,
                    source_event_id=source_event_id,
                    idempotency_key=idempotency_key,
                ),
                gate,
            )
        return finish(
            result_type(
                "executed_unverified",
                selected,
                "awaiting_receipt",
                output=output,
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(unverified),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=unverified
                if isinstance(unverified, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )
    try:
        verified = owner.effect_ledger.verify(effect_id, receipt)
    except Exception as exc:
        try:
            failed = owner.effect_ledger.fail(effect_id, "receipt_mismatch", False)
        except Exception:
            failed = pending
        return finish(
            result_type(
                "failed",
                selected,
                f"receipt_mismatch:{type(exc).__name__}",
                output=output,
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(failed),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=failed
                if isinstance(failed, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )
    try:
        owner._consume_verified(gate, effect_id=effect_id, allow_current=True)
    except Exception:
        return finish(
            result_type(
                "failed",
                selected,
                "control_consume_error",
                output=output,
                run_id=run_id,
                effect_id=effect_id,
                evidence=owner._record_evidence(verified),
                source_event_id=source_event_id,
                idempotency_key=idempotency_key,
                effect_record=verified
                if isinstance(verified, effect_record_type)
                else None,
                canonical_event_id=source_event_id,
            ),
            gate,
        )
    return finish(
        result_type(
            "completed",
            selected,
            "verified",
            output=output,
            run_id=run_id,
            effect_id=effect_id,
            evidence=owner._record_evidence(verified),
            source_event_id=source_event_id,
            idempotency_key=idempotency_key,
            effect_record=verified
            if isinstance(verified, effect_record_type)
            else None,
            canonical_event_id=source_event_id,
        ),
        gate,
    )


__all__ = ()
