"""Disabled-by-default, host-fed Autonomy provider examples.

These providers perform no network access. A deployment supplies already-read,
bounded candidates through ``AutonomyContext.facts`` and keeps credentials in
its own adapter.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from .autonomy import ActivityProvider, AutonomyContext
from .runtime_core import JsonlLedger, ensure_bounded_text, isoformat, utc_now

EXAMPLE_PROVIDER_SCHEMA = "moon.example_provider.v1"
_ARXIV_VERSION = re.compile(r"v\d+$", re.IGNORECASE)


def _required_text(value: Any, label: str, *, max_bytes: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    result = " ".join(value.split())
    ensure_bounded_text(result, label, max_bytes=max_bytes)
    return result


def _optional_text(value: Any, label: str, *, max_bytes: int) -> str:
    if value in (None, ""):
        return ""
    return _required_text(value, label, max_bytes=max_bytes)


def _https_url(value: Any, label: str) -> str:
    result = _required_text(value, label, max_bytes=4096)
    parsed = urlsplit(result)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{label} must be an HTTP(S) URL")
    return urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, "")
    ).rstrip("/")


def _candidate_rows(context: AutonomyContext, key: str) -> list[Mapping[str, Any]]:
    value = context.facts.get(key, [])
    if not isinstance(value, list):
        raise TypeError(f"{key} must be a list")
    rows: list[Mapping[str, Any]] = []
    for item in value[:20]:
        if not isinstance(item, Mapping):
            raise TypeError(f"{key} items must be mappings")
        rows.append(item)
    return rows


class XBrowseExample:
    """Read one host-verified short post without owning login or transport."""

    fact_key = "x_posts"

    def _verified(self, context: AutonomyContext) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        for item in _candidate_rows(context, self.fact_key):
            if item.get("read_verified") is not True:
                continue
            result.append(
                {
                    "text": _required_text(
                        item.get("text"), "x post text", max_bytes=4096
                    ),
                    "source_url": _https_url(
                        item.get("source_url"), "x post source_url"
                    ),
                }
            )
        return result

    def eligible(self, context: AutonomyContext) -> bool:
        return bool(self._verified(context))

    def run(self, context: AutonomyContext) -> dict[str, str]:
        rows = self._verified(context)
        if not rows:
            raise RuntimeError("no verified short-post candidate")
        selected = rows[0]
        return {
            "kind": "x_browse_example",
            "source_url": selected["source_url"],
            "conversation_topic": selected["text"],
        }


def paper_identity(candidate: Mapping[str, Any]) -> str:
    """Return a stable paper identity, preferring canonical public identifiers."""

    doi = _optional_text(candidate.get("doi"), "paper doi", max_bytes=512)
    if doi:
        normalized = doi.casefold()
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix) :]
                break
        return f"doi:{normalized.strip()}"

    arxiv_id = _optional_text(
        candidate.get("arxiv_id"), "paper arxiv_id", max_bytes=256
    )
    if arxiv_id:
        normalized = arxiv_id.casefold()
        for prefix in ("https://arxiv.org/abs/", "http://arxiv.org/abs/", "arxiv:"):
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix) :]
                break
        return f"arxiv:{_ARXIV_VERSION.sub('', normalized.strip())}"

    source_url = candidate.get("source_url")
    if source_url:
        return f"url:{_https_url(source_url, 'paper source_url')}"

    title = _required_text(candidate.get("title"), "paper title", max_bytes=2048)
    return f"title:{title.casefold()}"


class PaperBrowseExample:
    """Choose one verified unseen paper, with at most one alternate candidate."""

    fact_key = "papers"

    def __init__(self, root: Path, *, clock=utc_now):
        self.seen = JsonlLedger(root / "paper_browse_seen.jsonl")
        self.clock = clock

    def _verified(self, context: AutonomyContext) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        for item in _candidate_rows(context, self.fact_key):
            if item.get("read_verified") is not True:
                continue
            title = _required_text(item.get("title"), "paper title", max_bytes=2048)
            summary = _required_text(
                item.get("summary"), "paper summary", max_bytes=8192
            )
            source_url = _optional_text(
                item.get("source_url"), "paper source_url", max_bytes=4096
            )
            if source_url:
                source_url = _https_url(source_url, "paper source_url")
            identity = paper_identity(item)
            result.append(
                {
                    "identity": identity,
                    "title": title,
                    "summary": summary,
                    "source_url": source_url,
                }
            )
        return result[:2]

    def _seen_identities(self) -> set[str]:
        result: set[str] = set()
        for row in self.seen.rows():
            if row.get("schema_version") != EXAMPLE_PROVIDER_SCHEMA:
                raise ValueError("paper seen ledger has an unsupported schema")
            identity = row.get("identity")
            if not isinstance(identity, str) or not identity:
                raise ValueError("paper seen ledger row is missing identity")
            result.add(identity)
        return result

    def _select(self, context: AutonomyContext) -> dict[str, str] | None:
        seen = self._seen_identities()
        return next(
            (item for item in self._verified(context) if item["identity"] not in seen),
            None,
        )

    def eligible(self, context: AutonomyContext) -> bool:
        return self._select(context) is not None

    def run(self, context: AutonomyContext) -> dict[str, str]:
        selected = self._select(context)
        if selected is None:
            raise RuntimeError("no verified unseen paper candidate")
        self.seen.append(
            {
                "schema_version": EXAMPLE_PROVIDER_SCHEMA,
                "identity": selected["identity"],
                "title": selected["title"],
                "source_url": selected["source_url"],
                "seen_at": isoformat(self.clock()),
            }
        )
        return {
            "kind": "paper_browse_example",
            "identity": selected["identity"],
            "title": selected["title"],
            "source_url": selected["source_url"],
            "conversation_topic": selected["summary"],
        }


def example_activity_providers(root: Path | None) -> tuple[ActivityProvider, ...]:
    """Build stateless examples for hosts, and stateful examples when standalone."""

    short_posts = XBrowseExample()
    providers = [
        ActivityProvider("x_browse", short_posts.run, eligible=short_posts.eligible)
    ]
    if root is not None:
        papers = PaperBrowseExample(root)
        providers.append(
            ActivityProvider("paper_browse", papers.run, eligible=papers.eligible)
        )
    return tuple(providers)
