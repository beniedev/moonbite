"""Hermes registration and operator surfaces for the complete Moonbite runtime."""

from __future__ import annotations

import argparse
import json
import shlex
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from .autonomy import ActivityProvider, AutonomyJudge
from .components import RuntimeComponents
from .config import route_bindings
from .conversation import ConversationBridge
from .doctor import doctor_report
from .hermes_adapter import (
    HermesAutonomyJudge,
    HermesDiaryWriter,
    HermesHeartbeatJudge,
    HermesModelReflection,
    HermesSessionWakeSink,
)
from .heartbeat import Judge, WakeSink
from .memory import DiaryWriter
from .memory_orchestration import MemoryOrchestrator, SourceRegistry
from .service import MoonbiteRuntime, SessionContextResolver
from .session import HOOK_ORDER
from .scenarios import resolve_config

TOOL_NAMES = (
    "moonbite_status",
    "control_moonbite_runtime",
    "record_moonbite_event",
    "run_moonbite_heartbeat",
    "run_moonbite_autonomy",
    "get_moonbite_panel",
    "search_moonbite_memory",
    "open_moonbite_memory",
    "capture_moonbite_memory_card",
    "synthesize_moonbite_diary",
)

_RAW_CONFIG_MISSING = object()
_SELECTED_PACK_MISSING = object()
_MOONBITE_RUNTIME_TYPE = MoonbiteRuntime


@dataclass(frozen=True, slots=True)
class RegistrationPlan:
    """The exact Hermes surface a Moonbite runtime may own."""

    tool_names: frozenset[str]
    hook_names: frozenset[str]
    cli: bool
    slash: bool
    auxiliary_tasks: bool

    def __post_init__(self) -> None:
        if type(self.tool_names) is not frozenset:
            raise TypeError("tool_names must be a frozenset")
        if type(self.hook_names) is not frozenset:
            raise TypeError("hook_names must be a frozenset")
        if type(self.cli) is not bool:
            raise TypeError("cli must be a bool")
        if type(self.slash) is not bool:
            raise TypeError("slash must be a bool")
        if type(self.auxiliary_tasks) is not bool:
            raise TypeError("auxiliary_tasks must be a bool")

        non_string_tools = frozenset(
            name for name in self.tool_names if type(name) is not str
        )
        if non_string_tools:
            raise ValueError("tool_names must contain only strings")
        non_string_hooks = frozenset(
            name for name in self.hook_names if type(name) is not str
        )
        if non_string_hooks:
            raise ValueError("hook_names must contain only strings")

        unknown_tools = tuple(sorted(self.tool_names - frozenset(TOOL_NAMES)))
        if unknown_tools:
            raise ValueError(f"unknown Moonbite tools: {unknown_tools!r}")
        unknown_hooks = tuple(sorted(self.hook_names - frozenset(HOOK_ORDER)))
        if unknown_hooks:
            raise ValueError(f"unknown Moonbite hooks: {unknown_hooks!r}")

    @classmethod
    def all(cls) -> RegistrationPlan:
        return cls(
            tool_names=frozenset(TOOL_NAMES),
            hook_names=frozenset(HOOK_ORDER),
            cli=True,
            slash=True,
            auxiliary_tasks=True,
        )

    @classmethod
    def shadow(cls) -> RegistrationPlan:
        return cls(
            tool_names=frozenset(),
            hook_names=frozenset(),
            cli=False,
            slash=False,
            auxiliary_tasks=False,
        )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _guarded(call: Callable[[], Any]) -> str:
    try:
        value = call()
        if hasattr(value, "to_dict"):
            value = value.to_dict()
        elif hasattr(value, "__dict__"):
            value = dict(value.__dict__)
        return _json({"ok": True, "result": value})
    except Exception as exc:  # noqa: BLE001 - public operator boundary
        message = str(exc)
        if isinstance(exc, RuntimeError) and message.endswith(" module is disabled"):
            module = message.removesuffix(" module is disabled")
            public = {
                "code": "module_disabled",
                "message": f"{module.title()} is disabled.",
                "remediation": (
                    f"Enable modules.{module} in "
                    "plugins.entries.moonbite.settings.config."
                ),
            }
        elif isinstance(exc, RuntimeError) and message.endswith(" is disabled"):
            public = {
                "code": "feature_disabled",
                "message": "The requested Moonbite feature is disabled.",
                "remediation": "Enable the feature in the Moonbite configuration.",
            }
        elif isinstance(exc, (TypeError, ValueError)):
            public = {
                "code": "invalid_request",
                "message": "Request rejected by Moonbite validation.",
                "remediation": "Correct the request fields and retry.",
            }
        else:
            public = {
                "code": "operation_failed",
                "message": "Moonbite could not complete the operation.",
                "remediation": "Run `hermes moonbite doctor` and inspect operator logs.",
            }
        return _json(
            {
                "ok": False,
                "error": type(exc).__name__,
                **public,
            }
        )


def _json_object(raw: str | None, label: str) -> dict[str, Any]:
    if raw is None or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")  # noqa: TRY004
    return value


def _json_value(raw: str, label: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON") from exc


def _setup_cli(parser: argparse.ArgumentParser) -> None:
    commands = parser.add_subparsers(dest="moonbite_command")
    commands.add_parser("status", help="Show effective runtime status")
    commands.add_parser("doctor", help="Validate configuration without model calls")

    session = commands.add_parser(
        "session", help="Inspect or repair session lifecycle turn ownership"
    )
    session_commands = session.add_subparsers(dest="session_command")
    session_commands.add_parser(
        "status", help="List exact identifiers for currently open turns"
    )
    repair = session_commands.add_parser(
        "repair", help="Abandon one exact open turn as non-success"
    )
    repair.add_argument("--lifecycle-id", required=True)
    repair.add_argument("--turn-id", required=True)

    control = commands.add_parser("control", help="Inspect or change runtime controls")
    control.add_argument("action", choices=("status", "pause", "resume", "quota_save"))
    control.add_argument(
        "feature",
        nargs="?",
        default="background_costly",
        choices=("heartbeat", "autonomy", "background_costly"),
    )
    control.add_argument("--source", choices=("operator", "self"), default="operator")
    control.add_argument("--minutes", type=int)

    event = commands.add_parser("event", help="Append one normalized event")
    event.add_argument("kind")
    event.add_argument("--source", default="operator")
    event.add_argument("--payload", default="{}")

    heartbeat = commands.add_parser("heartbeat", help="Run one Heartbeat candidate")
    heartbeat.add_argument("kind")
    heartbeat.add_argument("--context", default="{}")

    autonomy = commands.add_parser("autonomy", help="Run at most one autonomy provider")
    autonomy.add_argument("--facts", default="{}")

    commands.add_parser("panel", help="Render the fresh daily-RAM projection")
    search = commands.add_parser("memory-search", help="Search cards and diary rows")
    search.add_argument("query")
    search.add_argument("--limit", type=int)
    search.add_argument("--include-archived", action="store_true")
    search.add_argument("--include-historical", action="store_true")
    recall = commands.add_parser(
        "memory-recall", help="Return bounded, evidence-backed recall candidates"
    )
    recall.add_argument("query")
    recall.add_argument("--limit", type=int)
    resurface = commands.add_parser(
        "memory-resurface", help="Return optional non-delivery resurface candidates"
    )
    resurface.add_argument("query")
    resurface.add_argument(
        "--active-chat",
        action="store_true",
        help="Request hint only; the durable conversation gate still applies",
    )
    resurface.add_argument("--limit", type=int)
    maintenance = commands.add_parser(
        "memory-maintenance-propose",
        help="Append one proposal-only memory maintenance request",
    )
    maintenance.add_argument("operation", choices=("merge", "retire", "distill"))
    maintenance.add_argument("--request-id", required=True)
    maintenance.add_argument("--evidence-ref", action="append", required=True)
    maintenance.add_argument("--reason", required=True)
    maintenance.add_argument("--proposed-value", required=True)
    maintenance_apply = commands.add_parser(
        "memory-maintenance-apply",
        help="Apply one reviewed proposal as an append-only history receipt",
    )
    maintenance_apply.add_argument("--proposal-id", required=True)
    maintenance_apply.add_argument("--activity", default="operator_review")
    maintenance_apply.add_argument(
        "--permission", choices=("safe", "reporting", "manual"), required=True
    )
    opened = commands.add_parser(
        "memory-open", help="Open an exact card/diary reference"
    )
    opened.add_argument("open_ref")
    opened.add_argument("--history", action="store_true")
    add = commands.add_parser("memory-add", help="Append an explicit memory card")
    add.add_argument("summary")
    add.add_argument(
        "--provenance",
        required=True,
        choices=("user_explicit", "agent_observation", "agent_inference"),
    )
    add.add_argument("--source-ref", required=True)
    add.add_argument("--tag", action="append", default=[])
    add.add_argument("--event-time")
    add.add_argument("--entity", action="append", default=[])
    add.add_argument("--state-key")
    add.add_argument(
        "--history-status",
        choices=("current", "historical", "corrected"),
        default="current",
    )
    add.add_argument(
        "--lifecycle-status", choices=("active", "archived"), default="active"
    )
    add.add_argument("--supersedes", action="append", default=[])
    add.add_argument(
        "--supersession-kind", choices=("evolution", "correction", "dedupe")
    )
    add.add_argument("--related-card", action="append", default=[])
    diary = commands.add_parser(
        "diary-synthesize", help="Write a grounded diary from exact evidence refs"
    )
    diary.add_argument("day")
    diary.add_argument("--evidence-ref", action="append", required=True)
    diary.add_argument("--title-hint", default="")


def _cli_handler(
    args: argparse.Namespace, *, runtime: MoonbiteRuntime, raw_config: Any
) -> int:
    command = getattr(args, "moonbite_command", None)
    if command == "status":
        result = runtime.status(include_private_paths=True)
    elif command == "doctor":
        result = doctor_report(raw_config, runtime=runtime)
    elif command == "session":
        if args.session_command == "status":
            result = runtime.session_status()
        elif args.session_command == "repair":
            result = runtime.repair_session_turn(
                args.lifecycle_id,
                args.turn_id,
            )
        else:
            print(
                "usage: hermes moonbite session {status,repair "
                "--lifecycle-id ID --turn-id ID}"
            )
            return 2
    elif command == "control":
        result = runtime.control(
            args.action,
            feature=args.feature,
            source=args.source,
            minutes=args.minutes,
        )
    elif command == "event":
        result = runtime.emit_event(
            args.kind,
            source=args.source,
            payload=_json_object(args.payload, "payload"),
        )
    elif command == "heartbeat":
        result = runtime.run_heartbeat(
            args.kind, context=_json_object(args.context, "context")
        ).to_dict()
    elif command == "autonomy":
        result = runtime.run_autonomy(facts=_json_object(args.facts, "facts")).__dict__
    elif command == "panel":
        result = runtime.get_panel()
    elif command == "memory-search":
        result = [
            hit.__dict__
            for hit in runtime.search_memory(
                args.query,
                limit=args.limit,
                include_archived=args.include_archived,
                include_historical=args.include_historical,
            )
        ]
    elif command == "memory-recall":
        result = [
            candidate.to_dict()
            for candidate in runtime.recall_memory(args.query, limit=args.limit)
        ]
    elif command == "memory-resurface":
        result = [
            candidate.to_dict()
            for candidate in runtime.resurface_memory(
                args.query,
                active_chat=args.active_chat,
                limit=args.limit,
            )
        ]
    elif command == "memory-maintenance-propose":
        result = runtime.propose_memory_maintenance(
            request_id=args.request_id,
            operation=args.operation,
            evidence_refs=args.evidence_ref,
            reason=args.reason,
            proposed_value=_json_value(args.proposed_value, "proposed-value"),
        )
    elif command == "memory-maintenance-apply":
        result = runtime.apply_memory_maintenance(
            args.proposal_id,
            activity=args.activity,
            permission=args.permission,
        )
    elif command == "memory-open":
        result = runtime.open_memory(args.open_ref, include_history=args.history)
    elif command == "memory-add":
        result = runtime.add_memory_card(
            args.summary,
            provenance=args.provenance,
            source_ref=args.source_ref,
            tags=args.tag,
            event_time=args.event_time,
            entities=args.entity,
            state_key=args.state_key,
            history_status=args.history_status,
            lifecycle_status=args.lifecycle_status,
            supersedes=args.supersedes,
            supersession_kind=args.supersession_kind,
            related_cards=args.related_card,
        )
    elif command == "diary-synthesize":
        result = runtime.synthesize_diary(
            day=date.fromisoformat(args.day),
            evidence_refs=args.evidence_ref,
            title_hint=args.title_hint,
        )
    else:
        print(
            "usage: hermes moonbite "
            "{status,doctor,session,control,event,heartbeat,autonomy,panel,"
            "memory-search,memory-recall,memory-resurface,"
            "memory-maintenance-propose,memory-maintenance-apply,memory-open,"
            "memory-add,diary-synthesize}"
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    if isinstance(result, Mapping) and result.get("ok") is False:
        return 1
    return 0


def _slash_handler(raw_args: str, *, runtime: MoonbiteRuntime, raw_config: Any) -> str:
    parts = shlex.split(raw_args)
    if not parts or parts == ["status"]:
        return _json(runtime.status())
    if parts == ["doctor"]:
        return _json(doctor_report(raw_config, runtime=runtime))
    action = parts[0].replace("-", "_")
    if action not in {"pause", "resume", "quota_save"}:
        return "Usage: /moon [status|doctor|pause|resume|quota-save] [heartbeat|autonomy|background_costly] [minutes]"
    feature = parts[1] if len(parts) > 1 else "background_costly"
    minutes = int(parts[2]) if len(parts) > 2 else None
    return _json(
        runtime.control(action, feature=feature, source="operator", minutes=minutes)
    )


def _schema(
    name: str,
    description: str,
    properties: dict[str, Any],
    required: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
            "required": required or [],
        },
    }


def _record_model_event(runtime: MoonbiteRuntime, args: Mapping[str, Any]) -> Any:
    kind = str(args["kind"])
    policy = runtime.heartbeat.kind_policy(kind)
    if policy is not None and policy.host_only:
        raise ValueError("host-only heartbeat kinds require an operator or host event")
    return runtime.emit_event(
        kind, source="model_tool", payload=args.get("payload", {})
    )


def _run_model_heartbeat(runtime: MoonbiteRuntime, args: Mapping[str, Any]) -> Any:
    kind = str(args["kind"])
    policy = runtime.heartbeat.kind_policy(kind)
    if policy is not None and policy.host_only:
        raise ValueError("host-only heartbeat kinds require an operator or host event")
    return runtime.run_heartbeat(kind, context=args.get("context", {})).to_dict()


def _capture_model_memory(runtime: MoonbiteRuntime, args: Mapping[str, Any]) -> Any:
    provenance = str(args["provenance"])
    if provenance == "user_explicit":
        raise ValueError("model tools cannot assert user_explicit provenance")
    if provenance not in {"agent_observation", "agent_inference"}:
        raise ValueError("unsupported model-tool provenance")
    return runtime.add_memory_card(
        args["summary"],
        provenance=provenance,
        source_ref=args["source_ref"],
        tags=args.get("tags", []),
        event_time=args.get("event_time"),
        entities=args.get("entities", []),
        state_key=args.get("state_key"),
        history_status=args.get("history_status", "current"),
        lifecycle_status=args.get("lifecycle_status", "active"),
        supersedes=args.get("supersedes", []),
        supersession_kind=args.get("supersession_kind"),
        related_cards=args.get("related_cards", []),
    )


def _register_tools(
    ctx: Any,
    runtime: MoonbiteRuntime,
    tool_names: frozenset[str] | None = None,
) -> None:
    definitions: list[
        tuple[str, str, dict[str, Any], Callable[[dict[str, Any]], Any]]
    ] = [
        (
            "moonbite_status",
            "Inspect enabled Moonbite modules and controls.",
            _schema("moonbite_status", "Inspect Moonbite runtime status.", {}),
            lambda _args: runtime.status(),
        ),
        (
            "control_moonbite_runtime",
            "Pause, resume, or inspect Heartbeat and Autonomy controls.",
            _schema(
                "control_moonbite_runtime",
                "Control Moonbite runtime features.",
                {
                    "action": {
                        "type": "string",
                        "enum": ["status", "pause", "resume", "quota_save"],
                    },
                    "feature": {
                        "type": "string",
                        "enum": ["heartbeat", "autonomy", "background_costly"],
                    },
                    "minutes": {"type": "integer", "minimum": 1, "maximum": 1440},
                },
                ["action"],
            ),
            lambda args: runtime.control(
                args["action"],
                feature=args.get("feature", "background_costly"),
                source="self",
                minutes=args.get("minutes"),
            ),
        ),
        (
            "record_moonbite_event",
            "Append one normalized event without interpreting private state.",
            _schema(
                "record_moonbite_event",
                "Record a normalized Moonbite event.",
                {
                    "kind": {"type": "string", "minLength": 1},
                    "payload": {"type": "object"},
                },
                ["kind"],
            ),
            lambda args: _record_model_event(runtime, args),
        ),
        (
            "run_moonbite_heartbeat",
            "Evaluate one Heartbeat candidate through control, Judge, and effect auditing.",
            _schema(
                "run_moonbite_heartbeat",
                "Run one Moonbite Heartbeat candidate.",
                {
                    "kind": {"type": "string", "minLength": 1},
                    "context": {"type": "object"},
                },
                ["kind"],
            ),
            lambda args: _run_model_heartbeat(runtime, args),
        ),
        (
            "run_moonbite_autonomy",
            "Run at most one eligible autonomy provider.",
            _schema(
                "run_moonbite_autonomy",
                "Run one Moonbite autonomy tick.",
                {},
            ),
            lambda _args: runtime.run_autonomy().__dict__,
        ),
        (
            "get_moonbite_panel",
            "Read the fresh typed daily-RAM projection.",
            _schema("get_moonbite_panel", "Read Moonbite panel state.", {}),
            lambda _args: runtime.get_panel(),
        ),
        (
            "search_moonbite_memory",
            "Search cards and diary rows and return exact open references.",
            _schema(
                "search_moonbite_memory",
                "Search Moonbite memory.",
                {
                    "query": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "include_archived": {"type": "boolean"},
                    "include_historical": {"type": "boolean"},
                },
                ["query"],
            ),
            lambda args: [
                hit.__dict__
                for hit in runtime.search_memory(
                    args["query"],
                    limit=args.get("limit"),
                    include_archived=args.get("include_archived", False),
                    include_historical=args.get("include_historical", False),
                )
            ],
        ),
        (
            "open_moonbite_memory",
            "Open one exact memory evidence reference.",
            _schema(
                "open_moonbite_memory",
                "Open a Moonbite memory reference.",
                {
                    "open_ref": {"type": "string", "minLength": 1},
                    "include_history": {"type": "boolean"},
                },
                ["open_ref"],
            ),
            lambda args: runtime.open_memory(
                args["open_ref"], include_history=args.get("include_history", False)
            ),
        ),
        (
            "capture_moonbite_memory_card",
            "Append an explicitly sourced memory card with provenance.",
            _schema(
                "capture_moonbite_memory_card",
                "Capture a Moonbite memory card.",
                {
                    "summary": {"type": "string", "minLength": 1},
                    "provenance": {
                        "type": "string",
                        "enum": [
                            "agent_observation",
                            "agent_inference",
                        ],
                    },
                    "source_ref": {"type": "string", "minLength": 1},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "event_time": {"type": "string"},
                    "entities": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": 64,
                    },
                    "state_key": {"type": "string"},
                    "history_status": {
                        "type": "string",
                        "enum": ["current", "historical", "corrected"],
                    },
                    "lifecycle_status": {
                        "type": "string",
                        "enum": ["active", "archived"],
                    },
                    "supersedes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": 64,
                    },
                    "supersession_kind": {
                        "type": "string",
                        "enum": ["evolution", "correction", "dedupe"],
                    },
                    "related_cards": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": 64,
                    },
                },
                ["summary", "provenance", "source_ref"],
            ),
            lambda args: _capture_model_memory(runtime, args),
        ),
        (
            "synthesize_moonbite_diary",
            "Write one grounded diary entry from exact memory evidence refs.",
            _schema(
                "synthesize_moonbite_diary",
                "Synthesize a Moonbite diary entry through the hippocampus route.",
                {
                    "day": {"type": "string", "format": "date"},
                    "evidence_refs": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                        "maxItems": 20,
                    },
                    "title_hint": {"type": "string"},
                },
                ["day", "evidence_refs"],
            ),
            lambda args: runtime.synthesize_diary(
                day=date.fromisoformat(args["day"]),
                evidence_refs=args["evidence_refs"],
                title_hint=args.get("title_hint", ""),
            ),
        ),
    ]
    selected_names = frozenset(TOOL_NAMES) if tool_names is None else tool_names
    unknown_tools = selected_names - frozenset(TOOL_NAMES)
    if unknown_tools:
        raise ValueError(f"unknown Moonbite tools: {unknown_tools!r}")
    for name, description, schema, call in definitions:
        if name not in selected_names:
            continue
        ctx.register_tool(
            name=name,
            toolset="moonbite",
            schema=schema,
            handler=lambda args, _call=call, **_kwargs: _guarded(
                lambda: _call(args or {})
            ),
            description=description,
            emoji="🌙",
        )


def _resolve_scenario_pack(ctx: Any) -> str | None:
    return ctx.get_config("scenario_pack", default=None)


def _resolve_config_inputs(
    ctx: Any,
    raw_config: Any,
    selected_pack: Any = _SELECTED_PACK_MISSING,
) -> tuple[Any, str | None]:
    explicit_raw = raw_config is not _RAW_CONFIG_MISSING
    resolved_raw = raw_config if explicit_raw else ctx.get_config("config", default={})
    if selected_pack is _SELECTED_PACK_MISSING:
        resolved_pack = None if explicit_raw else _resolve_scenario_pack(ctx)
    else:
        resolved_pack = selected_pack
    return resolved_raw, resolved_pack


def _validated_plan(plan: RegistrationPlan | None) -> RegistrationPlan:
    if plan is None:
        return RegistrationPlan.all()
    if not isinstance(plan, RegistrationPlan):
        raise TypeError("plan must be a RegistrationPlan")
    return plan


def _preflight_registration(ctx: Any, plan: RegistrationPlan, bindings: Any) -> None:
    required: list[str] = []
    if plan.tool_names:
        required.append("register_tool")
    if plan.hook_names:
        required.append("register_hook")
    if plan.cli:
        required.append("register_cli_command")
    if plan.slash:
        required.append("register_command")
    if plan.auxiliary_tasks and bindings is not None:
        required.append("register_auxiliary_task")
    missing = [name for name in required if not callable(getattr(ctx, name, None))]
    if missing:
        raise TypeError(
            "Hermes context is missing registration capabilities: " + ", ".join(missing)
        )


def build_runtime(
    ctx: Any,
    *,
    raw_config: Any = _RAW_CONFIG_MISSING,
    selected_pack: str | None | object = _SELECTED_PACK_MISSING,
    components: RuntimeComponents | None = None,
    heartbeat_judge: Judge | None = None,
    autonomy_judge: AutonomyJudge | None = None,
    wake_sink: WakeSink | None = None,
    diary_writer: DiaryWriter | None = None,
    session_context_resolver: SessionContextResolver | None = None,
    conversation_bridge: ConversationBridge | None = None,
    memory_orchestrator: MemoryOrchestrator | None = None,
    source_registry: SourceRegistry | None = None,
    approval_adapter: Any = None,
) -> MoonbiteRuntime:
    """Construct the Moonbite runtime without registering host surfaces."""
    raw_config, selected_pack = _resolve_config_inputs(ctx, raw_config, selected_pack)
    resolution = resolve_config(raw_config, selected_pack)
    config = resolution.effective_config
    bindings = route_bindings(config)

    if heartbeat_judge is None and bindings is not None:
        heartbeat_judge = HermesHeartbeatJudge(ctx.llm, task=bindings.heartbeat)
    if autonomy_judge is None and bindings is not None:
        autonomy_judge = HermesAutonomyJudge(ctx.llm, task=bindings.heartbeat)
    if wake_sink is None and config["delivery"]["adapter"] == "hermes_session":
        wake_sink = HermesSessionWakeSink(
            ctx,
            session_key=config["delivery"]["target"],
        )
    if diary_writer is None and bindings is not None:
        diary_writer = HermesDiaryWriter(ctx.llm, task=bindings.hippocampus)
    runtime = MoonbiteRuntime(
        config,
        heartbeat_judge=heartbeat_judge,
        autonomy_judge=autonomy_judge,
        wake_sink=wake_sink,
        diary_writer=diary_writer,
        components=components,
        session_context_resolver=session_context_resolver,
        conversation_bridge=conversation_bridge,
        memory_orchestrator=memory_orchestrator,
        source_registry=source_registry,
        approval_adapter=approval_adapter,
        resolution=resolution,
        resolution_raw_config=raw_config,
    )

    if bindings is not None:
        runtime.providers.register(
            ActivityProvider(
                "model_reflection",
                HermesModelReflection(ctx.llm, task=bindings.main),
            )
        )
    runtime._registration_context_owner = ctx
    return runtime


def register_runtime(
    ctx: Any,
    runtime: MoonbiteRuntime,
    *,
    raw_config: Any = _RAW_CONFIG_MISSING,
    selected_pack: str | None | object = _SELECTED_PACK_MISSING,
    plan: RegistrationPlan | None = None,
) -> None:
    """Register one planned Hermes surface for a same-context prebuilt runtime.

    This is deliberately a one-shot host operation.  The caller must be the
    unique registration owner because Hermes exposes no portable registration
    transaction or rollback API.
    """
    if not isinstance(runtime, _MOONBITE_RUNTIME_TYPE):
        raise TypeError("runtime must be a MoonbiteRuntime")
    if getattr(runtime, "_registration_context_owner", None) is not ctx:
        raise ValueError("runtime was built for a different Hermes context")
    raw_config, selected_pack = _resolve_config_inputs(ctx, raw_config, selected_pack)
    if (
        runtime._resolution_raw_config != raw_config
        or runtime._resolution_selected_pack != selected_pack
    ):
        raise ValueError("runtime config does not match registration config")
    if runtime.config != runtime.resolution.effective_config:
        raise ValueError("runtime config does not match registration resolution")
    config = runtime.config
    bindings = route_bindings(config)
    plan = _validated_plan(plan)

    # This preflight intentionally protects only this call's zero-to-one
    # transition.  The host registry remains responsible for duplicate names.
    _preflight_registration(ctx, plan, bindings)

    if plan.auxiliary_tasks and bindings is not None:
        descriptions = {
            bindings.main: "Moonbite visible/main-model lane",
            bindings.heartbeat: "Moonbite Heartbeat and bounded Judge lane",
            bindings.hippocampus: "Moonbite bounded diary-synthesis lane",
        }
        for alias, description in sorted(descriptions.items()):
            ctx.register_auxiliary_task(
                alias,
                display_name=alias.replace("_", " ").title(),
                description=description,
                defaults={"provider": "auto", "model": "", "timeout": 60},
            )

    _register_tools(ctx, runtime, plan.tool_names)

    def pre_gateway_dispatch(**kwargs: Any) -> None:
        # This public hook fires before authorization and has no stable
        # authorized session ID.  The runtime's default mapping is therefore an
        # explicit no-op; a host resolver may opt in with a typed context.
        runtime.record_session_hook("pre_gateway_dispatch", kwargs)

    def on_session_start(**kwargs: Any) -> None:
        runtime.record_session_hook("on_session_start", kwargs)

    def pre_llm_call(**kwargs: Any) -> dict[str, str] | None:
        session_receipt = runtime.record_session_hook("pre_llm_call", kwargs)
        return runtime.pre_llm_context(
            kwargs.get("user_message"),
            session_receipt=session_receipt,
        )

    def post_llm_call(**kwargs: Any) -> None:
        runtime.record_session_hook("post_llm_call", kwargs, settled=True)

    def on_session_finalize(**kwargs: Any) -> None:
        runtime.record_session_hook("on_session_finalize", kwargs)

    hook_handlers = {
        "pre_gateway_dispatch": pre_gateway_dispatch,
        "on_session_start": on_session_start,
        "pre_llm_call": pre_llm_call,
        "post_llm_call": post_llm_call,
        "on_session_finalize": on_session_finalize,
    }
    for hook_name in HOOK_ORDER:
        if hook_name in plan.hook_names:
            ctx.register_hook(hook_name, hook_handlers[hook_name])

    def cli_handler(args: argparse.Namespace) -> int:
        return _cli_handler(args, runtime=runtime, raw_config=raw_config)

    def slash_handler(raw_args: str) -> str:
        return _slash_handler(raw_args, runtime=runtime, raw_config=raw_config)

    if plan.cli:
        ctx.register_cli_command(
            name="moonbite",
            help="Run and inspect the Moonbite companion runtime",
            setup_fn=_setup_cli,
            handler_fn=cli_handler,
            description="Portable Heartbeat, Autonomy, Panel, Memory, and runtime controls.",
        )
    if plan.slash:
        ctx.register_command(
            "moon",
            handler=slash_handler,
            description="Inspect or control Moonbite without changing host model routes.",
            args_hint="[status|doctor|pause|resume|quota-save]",
        )


def register(
    ctx: Any,
    *,
    components: RuntimeComponents | None = None,
    heartbeat_judge: Judge | None = None,
    autonomy_judge: AutonomyJudge | None = None,
    wake_sink: WakeSink | None = None,
    diary_writer: DiaryWriter | None = None,
    session_context_resolver: SessionContextResolver | None = None,
    conversation_bridge: ConversationBridge | None = None,
    memory_orchestrator: MemoryOrchestrator | None = None,
    source_registry: SourceRegistry | None = None,
    approval_adapter: Any = None,
    plan: RegistrationPlan | None = None,
) -> MoonbiteRuntime:
    raw_config, selected_pack = _resolve_config_inputs(ctx, _RAW_CONFIG_MISSING)
    plan = _validated_plan(plan)
    runtime = build_runtime(
        ctx,
        raw_config=raw_config,
        selected_pack=selected_pack,
        components=components,
        heartbeat_judge=heartbeat_judge,
        autonomy_judge=autonomy_judge,
        wake_sink=wake_sink,
        diary_writer=diary_writer,
        session_context_resolver=session_context_resolver,
        conversation_bridge=conversation_bridge,
        memory_orchestrator=memory_orchestrator,
        source_registry=source_registry,
        approval_adapter=approval_adapter,
    )
    register_runtime(
        ctx,
        runtime,
        raw_config=raw_config,
        selected_pack=selected_pack,
        plan=plan,
    )
    return runtime
