"""Session hook mapping and lifecycle projection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..components import RuntimeComponentsError
from ..hermes_adapter import SessionHookMappingError
from ..session import SessionContext, SessionHookReceipt, SessionTurnTerminalReceipt
from .contracts import DEFAULT_SESSION_HOOKS, SUPPORTED_SESSION_HOOKS


class SessionLifecycleMethods:
    @property
    def last_session_hook_error(self) -> dict[str, str] | None:
        """Return a bounded in-memory fact about the most recent hook failure."""

        if self._last_session_hook_error is None:
            return None
        return dict(self._last_session_hook_error)

    def _remember_session_hook_error(self, hook: str, error: BaseException) -> None:
        self._last_session_hook_error = {
            "hook": hook,
            "error": type(error).__name__,
        }

    def _resolve_session_context(
        self, hook: str, payload: Mapping[str, Any]
    ) -> SessionContext | None:
        if self.session_context_resolver is None:
            context = self.hermes_host_adapter.session_context(
                hook, payload, DEFAULT_SESSION_HOOKS
            )
        else:
            context = self.session_context_resolver(
                hook, payload, SUPPORTED_SESSION_HOOKS
            )
        if context is not None and not isinstance(context, SessionContext):
            raise SessionHookMappingError(
                "session_context_resolver must return SessionContext or None"
            )
        if context is not None and hook in {
            "pre_llm_call",
            "post_llm_call",
            "on_session_end",
            "on_session_finalize",
        }:
            snapshots = self.session.replay()
            if hook == "pre_llm_call":
                context = self.hermes_host_adapter.pre_turn_context(context, snapshots)
            elif hook == "on_session_finalize":
                context = self.hermes_host_adapter.correlate_lifecycle(
                    context, snapshots
                )
            else:
                context = self.hermes_host_adapter.correlate_turn(context, snapshots)
        return context

    def _project_session_receipt(self, hook: str, receipt: SessionHookReceipt):
        try:
            if self.conversation_bridge is not None:
                self.conversation_bridge.observe(receipt)
            if receipt.context.counts_as_private_contact:
                self.cadence.record_private_contact(receipt)
        except Exception as exc:
            # The session owner already accepted this receipt. Projection
            # retries are safe because both consumers are idempotent.
            self._remember_session_hook_error(hook, exc)
            raise
        self._last_session_hook_error = None
        return receipt

    def record_session_hook(
        self,
        hook: str,
        kwargs: Mapping[str, Any] | None = None,
        *,
        settled: bool = False,
    ):
        """Map one public hook and append it to the injected session owner.

        Missing public identifiers are an explicit, non-mutating rejection and
        become a bounded in-memory degraded fact.  Owner append failures are
        re-raised after recording the same fact so direct adapters and the host
        can observe the failure without a second risky state write.
        """

        payload = {} if kwargs is None else kwargs
        try:
            context = self._resolve_session_context(hook, payload)
            if context is None:
                return None
            event = payload.get("event")
            if hook == "pre_gateway_dispatch" and getattr(event, "internal", False):
                # Internal/system events are never contact, even when a
                # resolver is present.
                return None
            if (
                hook == "on_session_finalize"
                and self.session.snapshot(context.lifecycle_id) is None
            ):
                # Hermes can finalize a session that never reached a first turn.
                return None
            receipt = self.session.record_hook(context, hook, settled=settled)
        except SessionHookMappingError as exc:
            self._remember_session_hook_error(hook, exc)
            return None
        except Exception as exc:
            self._remember_session_hook_error(hook, exc)
            raise
        return self._project_session_receipt(hook, receipt)

    def record_hermes_turn_end(
        self, kwargs: Mapping[str, Any] | None = None
    ) -> SessionHookReceipt | SessionTurnTerminalReceipt | None:
        """Normalize Hermes' unconditional turn end into canonical evidence."""

        hook = "on_session_end"
        payload = {} if kwargs is None else kwargs
        try:
            fallback_context = self.hermes_host_adapter.session_end_shutdown_fallback(
                payload,
                supported_hooks=DEFAULT_SESSION_HOOKS,
            )
            if fallback_context is not None:
                if self.session_context_resolver is not None:
                    fallback_context = self.session_context_resolver(
                        hook,
                        payload,
                        SUPPORTED_SESSION_HOOKS,
                    )
                    if fallback_context is None:
                        raise SessionHookMappingError(
                            "session_context_resolver did not map the on_session_end shutdown fallback"
                        )
                    if not isinstance(fallback_context, SessionContext):
                        raise SessionHookMappingError(
                            "session_context_resolver must return SessionContext or None"
                        )
                snapshots = self.session.replay()
                fallback_context = self.hermes_host_adapter.correlate_lifecycle(
                    fallback_context,
                    snapshots,
                )
                if not any(
                    snapshot.lifecycle_id == fallback_context.lifecycle_id
                    for snapshot in snapshots
                ):
                    raise SessionHookMappingError(
                        "on_session_end shutdown fallback did not match a durable lifecycle"
                    )
                record_host_shutdown = getattr(
                    self.session, "record_host_shutdown", None
                )
                if not callable(record_host_shutdown):
                    raise RuntimeComponentsError(
                        "session owner is missing record_host_shutdown"
                    )
                receipt = record_host_shutdown(fallback_context)
                self._last_session_hook_error = None
                return receipt

            context = self._resolve_session_context(hook, payload)
            if context is None:
                return None
            terminal = self.hermes_host_adapter.turn_terminal(
                payload,
                supported_hooks=context.supported_hooks,
                context=context,
            )
            record_host_turn_end = getattr(self.session, "record_host_turn_end", None)
            if not callable(record_host_turn_end):
                raise RuntimeComponentsError(
                    "session owner is missing record_host_turn_end"
                )
            receipt = record_host_turn_end(terminal.context, terminal.reason)
            if receipt is None:
                return None
        except SessionHookMappingError as exc:
            self._remember_session_hook_error(hook, exc)
            return None
        except Exception as exc:
            self._remember_session_hook_error(hook, exc)
            raise
        if isinstance(receipt, SessionHookReceipt):
            return self._project_session_receipt(hook, receipt)
        if not isinstance(receipt, SessionTurnTerminalReceipt):
            raise RuntimeComponentsError(
                "session owner returned an invalid host turn terminal receipt"
            )
        self._last_session_hook_error = None
        return receipt

    def record_hermes_subagent_stop(
        self, kwargs: Mapping[str, Any] | None = None
    ) -> SessionHookReceipt | SessionTurnTerminalReceipt | None:
        """Normalize Hermes child-stop fallback into canonical evidence."""

        hook = "subagent_stop"
        payload = {} if kwargs is None else kwargs
        try:
            child_stop = self.hermes_host_adapter.subagent_stop_terminal(payload)
            if child_stop is None:
                self._last_session_hook_error = None
                return None
            record_host_child_stop = getattr(
                self.session, "record_host_child_stop", None
            )
            if not callable(record_host_child_stop):
                raise RuntimeComponentsError(
                    "session owner is missing record_host_child_stop"
                )
            receipt = record_host_child_stop(
                child_stop.child_session_id,
                child_stop.reason,
            )
        except SessionHookMappingError as exc:
            self._remember_session_hook_error(hook, exc)
            return None
        except Exception as exc:
            self._remember_session_hook_error(hook, exc)
            raise
        if receipt is None:
            self._last_session_hook_error = None
            return None
        if isinstance(receipt, SessionHookReceipt):
            return self._project_session_receipt(hook, receipt)
        if not isinstance(receipt, SessionTurnTerminalReceipt):
            raise RuntimeComponentsError(
                "session owner returned an invalid child terminal receipt"
            )
        self._last_session_hook_error = None
        return receipt

    def record_hermes_session_finalize(self, kwargs: Mapping[str, Any] | None = None):
        """Normalize Hermes session rotation and shutdown boundaries."""

        hook = "on_session_finalize"
        payload = {} if kwargs is None else kwargs
        try:
            context = self._resolve_session_context(hook, payload)
            if context is None or self.session.snapshot(context.lifecycle_id) is None:
                return None
            disposition = self.hermes_host_adapter.finalize_disposition(payload)
            if disposition == "shutdown":
                record_host_shutdown = getattr(
                    self.session, "record_host_shutdown", None
                )
                if not callable(record_host_shutdown):
                    raise RuntimeComponentsError(
                        "session owner is missing record_host_shutdown"
                    )
                receipt = record_host_shutdown(context)
                self._last_session_hook_error = None
                return receipt
            if disposition == "definitive":
                record_host_finalize = getattr(
                    self.session, "record_host_finalize", None
                )
                if not callable(record_host_finalize):
                    raise RuntimeComponentsError(
                        "session owner is missing record_host_finalize"
                    )
                receipt = record_host_finalize(context)
            else:
                receipt = self.session.record_hook(
                    context, "on_session_finalize", settled=False
                )
        except SessionHookMappingError as exc:
            self._remember_session_hook_error(hook, exc)
            return None
        except Exception as exc:
            self._remember_session_hook_error(hook, exc)
            raise
        return self._project_session_receipt(hook, receipt)
