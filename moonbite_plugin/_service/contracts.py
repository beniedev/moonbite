"""Shared service facade contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..session import HOOK_ORDER, SessionContext

SESSION_HOOK_ORDER = HOOK_ORDER
SUPPORTED_SESSION_HOOKS = frozenset(SESSION_HOOK_ORDER)
DEFAULT_SESSION_HOOKS = frozenset(
    hook for hook in SUPPORTED_SESSION_HOOKS if hook != "pre_gateway_dispatch"
)
SessionContextResolver = Callable[
    [str, Mapping[str, Any], frozenset[str]], SessionContext | None
]
MISSING = object()
