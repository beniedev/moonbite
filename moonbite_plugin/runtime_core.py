"""Portable event, audit, clock, and state primitives for Moonbite."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, ContextManager, Iterator, Mapping, Protocol
import uuid


SCHEMA_VERSION = "moon.event.v1"
MAX_EVENT_PAYLOAD_BYTES = 256 * 1024


class StateError(RuntimeError):
    """Raised when durable Moonbite state cannot be interpreted safely."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def isoformat(value: datetime) -> str:
    return as_utc(value).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return as_utc(parsed)
    except (TypeError, ValueError) as exc:
        raise StateError(f"invalid timestamp: {value!r}") from exc


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def ensure_private_directory(path: Path) -> None:
    """Create a Moonbite state directory and keep it owner-only."""

    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def ensure_bounded_text(value: str, label: str, *, max_bytes: int) -> None:
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} UTF-8 bytes")


def ensure_bounded_json(value: Any, label: str, *, max_bytes: int) -> None:
    try:
        encoded = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be JSON-serializable") from exc
    if len(encoded) > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} UTF-8 bytes")


@dataclass(frozen=True)
class EventEnvelope:
    kind: str
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: new_id("event"))
    created_at: datetime = field(default_factory=utc_now)
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"unsupported event schema: {self.schema_version}")
        if (
            not self.event_id.strip()
            or not self.kind.strip()
            or not self.source.strip()
        ):
            raise ValueError("event_id, kind, and source must be non-empty")
        as_utc(self.created_at)
        ensure_bounded_text(self.event_id, "event_id", max_bytes=256)
        ensure_bounded_text(self.kind, "event kind", max_bytes=128)
        ensure_bounded_text(self.source, "event source", max_bytes=128)
        ensure_bounded_json(
            dict(self.payload),
            "event payload",
            max_bytes=MAX_EVENT_PAYLOAD_BYTES,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "created_at": isoformat(self.created_at),
            "kind": self.kind,
            "source": self.source,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EventEnvelope":
        required = {
            "schema_version",
            "event_id",
            "created_at",
            "kind",
            "source",
            "payload",
        }
        missing = sorted(required - set(value))
        unknown = sorted(set(value) - required)
        if missing or unknown:
            raise StateError(
                f"event envelope keys invalid; missing={missing}, unknown={unknown}"
            )
        payload = value["payload"]
        if not isinstance(payload, Mapping):
            raise StateError("event payload must be a mapping")
        try:
            return cls(
                schema_version=str(value["schema_version"]),
                event_id=str(value["event_id"]),
                created_at=parse_time(str(value["created_at"])),
                kind=str(value["kind"]),
                source=str(value["source"]),
                payload=dict(payload),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StateError("event envelope is invalid") from exc


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    ensure_private_directory(path.parent)
    with path.open("a+", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def try_file_lock(path: Path) -> Iterator[bool]:
    """Acquire an exclusive process lock without queuing duplicate work."""

    ensure_private_directory(path.parent)
    with path.open("a+", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class RuntimeLocks(Protocol):
    def exclusive(self, name: str) -> ContextManager[None]: ...

    def try_exclusive(self, name: str) -> ContextManager[bool]: ...


class FileRuntimeLocks:
    """Named runtime locks rooted in a private state directory."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def _path(self, name: str) -> Path:
        if (
            not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or any(
                not (character.isascii() and character.isalnum())
                and character not in "_.-"
                for character in name
            )
        ):
            raise ValueError("unsafe runtime lock name")
        return self.root / f"{name}.lock"

    def exclusive(self, name: str) -> ContextManager[None]:
        return file_lock(self._path(name))

    def try_exclusive(self, name: str) -> ContextManager[bool]:
        return try_file_lock(self._path(name))


def atomic_json_write(path: Path, value: Mapping[str, Any]) -> None:
    ensure_private_directory(path.parent)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    descriptor, raw_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw_path)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_descriptor = os.open(path.parent, directory_flags)
        try:
            try:
                os.fsync(directory_descriptor)
            except OSError:
                # Some otherwise supported filesystems do not implement
                # directory fsync. The file itself was already synced above.
                pass
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()


class JsonlLedger:
    """Append-only JSONL ledger with an adjacent process lock."""

    def __init__(self, path: Path):
        self.path = path
        self.lock_path = path.with_suffix(path.suffix + ".lock")

    def append(self, value: Mapping[str, Any]) -> None:
        line = json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n"
        with file_lock(self.lock_path):
            self._append_unlocked(line)

    def _append_unlocked(self, line: str) -> None:
        ensure_private_directory(self.path.parent)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        descriptor = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            payload = line.encode("utf-8")
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise StateError("ledger write made no progress")
                offset += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _rows_unlocked(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        result: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise StateError(
                        f"ledger row {line_number} contains invalid JSON"
                    ) from exc
                if not isinstance(value, dict):
                    raise StateError(f"ledger row {line_number} must contain an object")
                result.append(value)
        return result

    def get_or_append(
        self,
        value: Mapping[str, Any],
        *,
        matcher: Callable[[Mapping[str, Any]], bool],
    ) -> tuple[dict[str, Any], bool]:
        """Atomically return a matching row or append ``value``.

        The callback runs while this ledger's process lock is held.  It is
        intentionally small and read-only: callers use it to make an
        occurrence's terminal audit projection exactly-once without creating
        another state store.
        """

        if not isinstance(value, Mapping):
            raise TypeError("ledger value must be a mapping")
        if not callable(matcher):
            raise TypeError("ledger matcher must be callable")
        line = json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n"
        with file_lock(self.lock_path):
            for row in self._rows_unlocked():
                if matcher(row):
                    return row, False
            self._append_unlocked(line)
            return dict(value), True

    def rows(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with file_lock(self.lock_path):
            return self._rows_unlocked()


class EventBus:
    """Small durable event bus shared by all optional Moonbite modules."""

    def __init__(self, root: Path, *, clock: Callable[[], datetime] = utc_now):
        self.root = root
        self.clock = clock
        self.events = JsonlLedger(root / "events.jsonl")
        self.audit = JsonlLedger(root / "audit.jsonl")

    def emit(
        self,
        kind: str,
        *,
        source: str,
        payload: Mapping[str, Any] | None = None,
        event_id: str | None = None,
    ) -> EventEnvelope:
        event = EventEnvelope(
            kind=kind,
            source=source,
            payload={} if payload is None else dict(payload),
            event_id=event_id or new_id("event"),
            created_at=self.clock(),
        )
        self.events.append(event.to_dict())
        return event

    def record_audit(
        self,
        action: str,
        *,
        status: str,
        source: str,
        details: Mapping[str, Any] | None = None,
    ) -> EventEnvelope:
        event = EventEnvelope(
            kind=f"audit.{action}",
            source=source,
            payload={"status": status, **({} if details is None else dict(details))},
            created_at=self.clock(),
        )
        # ``epoch_id=None`` is the legacy identity, not an explicit field.
        # Keep non-terminal rows in the old shape so public telemetry does not
        # manufacture an epoch merely because a result serializer included a
        # nullable field.
        if event.payload.get("epoch_id") is None:
            event.payload.pop("epoch_id", None)
        self.audit.append(event.to_dict())
        return event

    def find_audit_terminal(
        self,
        action: str,
        occurrence_id: str,
        *,
        epoch_id: str | None = None,
    ) -> EventEnvelope | None:
        """Find one canonical terminal for an exact occurrence and epoch."""

        if type(action) is not str or not action.strip():
            raise ValueError("audit action must be non-empty")
        if type(occurrence_id) is not str or not occurrence_id.strip():
            raise ValueError("audit occurrence_id must be non-empty")
        if epoch_id is not None and (type(epoch_id) is not str or not epoch_id.strip()):
            raise ValueError("audit epoch_id must be non-empty when provided")
        found: EventEnvelope | None = None
        for row in self.audit.rows():
            event = EventEnvelope.from_dict(row)
            if event.kind != f"audit.{action}":
                continue
            payload = event.payload
            if payload.get("occurrence_id") != occurrence_id:
                continue
            stored_epoch = payload.get("epoch_id")
            if stored_epoch is not None and (
                type(stored_epoch) is not str or not stored_epoch.strip()
            ):
                raise StateError("audit epoch_id is invalid")
            # Legacy rows omit epoch_id and therefore belong only to the
            # legacy (None) identity.  An explicit epoch must never replay or
            # conflict with an old no-epoch terminal.
            if stored_epoch != epoch_id:
                continue
            terminal = payload.get("terminal")
            if terminal is None:
                continue
            if type(terminal) is not str or not terminal.strip():
                raise StateError("audit terminal is invalid")
            if found is not None and found.payload.get("terminal") != terminal:
                raise StateError("audit occurrence terminal conflict")
            found = event
        return found

    def record_audit_terminal(
        self,
        action: str,
        *,
        occurrence_id: str,
        epoch_id: str | None = None,
        terminal: str,
        status: str,
        source: str,
        details: Mapping[str, Any] | None = None,
    ) -> EventEnvelope:
        """Append one canonical terminal audit for an exact occurrence.

        Existing terminal evidence is returned unchanged.  A different
        terminal for the same occurrence and epoch is a durable integrity
        conflict and fails closed; non-terminal audit rows remain ordinary
        telemetry.
        """

        if type(action) is not str or not action.strip():
            raise ValueError("audit action must be non-empty")
        if type(occurrence_id) is not str or not occurrence_id.strip():
            raise ValueError("audit occurrence_id must be non-empty")
        if epoch_id is not None and (type(epoch_id) is not str or not epoch_id.strip()):
            raise ValueError("audit epoch_id must be non-empty when provided")
        if type(terminal) is not str or not terminal.strip():
            raise ValueError("audit terminal must be non-empty")
        if type(status) is not str or not status.strip():
            raise ValueError("audit status must be non-empty")
        extras = {} if details is None else dict(details)
        for field_name, expected in (
            ("status", status),
            ("occurrence_id", occurrence_id),
            ("terminal", terminal),
            ("epoch_id", epoch_id),
        ):
            if field_name in extras and extras[field_name] != expected:
                raise ValueError(f"audit {field_name} conflicts with terminal identity")
            extras.pop(field_name, None)
        payload = {
            "status": status,
            "occurrence_id": occurrence_id,
            "terminal": terminal,
            **extras,
        }
        if epoch_id is not None:
            payload["epoch_id"] = epoch_id
        event = EventEnvelope(
            kind=f"audit.{action}",
            source=source,
            payload=payload,
            created_at=self.clock(),
        )

        def matcher(row: Mapping[str, Any]) -> bool:
            existing = EventEnvelope.from_dict(row)
            if existing.kind != event.kind:
                return False
            existing_payload = existing.payload
            if existing_payload.get("occurrence_id") != occurrence_id:
                return False
            existing_epoch = existing_payload.get("epoch_id")
            if existing_epoch is not None and (
                type(existing_epoch) is not str or not existing_epoch.strip()
            ):
                raise StateError("audit epoch_id is invalid")
            if existing_epoch != epoch_id:
                return False
            existing_terminal = existing_payload.get("terminal")
            if existing_terminal is None:
                return False
            if existing_terminal != terminal:
                raise StateError("audit occurrence terminal conflict")
            if existing.source != source:
                raise StateError("audit occurrence source conflict")
            for identity_field in (
                "status",
                "source_event_id",
                "effect_id",
                "effect_ids",
                "epoch_id",
                "idempotency_key",
                "provider",
            ):
                if (
                    identity_field in existing_payload or identity_field in payload
                ) and existing_payload.get(identity_field) != payload.get(
                    identity_field
                ):
                    raise StateError("audit occurrence identity conflict")
            return True

        row, created = self.audit.get_or_append(event.to_dict(), matcher=matcher)
        # Keep the established instance-level ``record_audit`` fault
        # injection seam useful for callers that test projection failure.
        # Only a newly appended terminal invokes the seam; a duplicate is a
        # no-op even when that compatibility hook is unavailable.
        patched_record_audit = self.__dict__.get("record_audit")
        if created and patched_record_audit is not None:
            patched_record_audit(
                action,
                status=status,
                source=source,
                details=payload,
            )
        return EventEnvelope.from_dict(row)

    def read_events(self) -> list[EventEnvelope]:
        return [EventEnvelope.from_dict(row) for row in self.events.rows()]

    def read_audit(self) -> list[EventEnvelope]:
        return [EventEnvelope.from_dict(row) for row in self.audit.rows()]
