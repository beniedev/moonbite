from __future__ import annotations

from datetime import datetime, timezone

from moonbite_plugin.autonomy import AutonomyContext
from moonbite_plugin.example_providers import (
    PaperBrowseExample,
    XBrowseExample,
    paper_identity,
)

NOW = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)


def context(**facts):
    return AutonomyContext(NOW, facts)


def test_x_example_requires_host_verified_read_only_candidate():
    provider = XBrowseExample()
    unverified = context(
        x_posts=[
            {
                "read_verified": False,
                "text": "Synthetic post",
                "source_url": "https://example.org/posts/1",
            }
        ]
    )
    verified = context(
        x_posts=[
            {
                "read_verified": True,
                "text": "Synthetic post",
                "source_url": "https://example.org/posts/1",
            }
        ]
    )

    assert provider.eligible(unverified) is False
    assert provider.eligible(verified) is True
    assert provider.run(verified)["conversation_topic"] == "Synthetic post"


def test_paper_identity_prefers_stable_public_identifiers():
    assert paper_identity({"doi": "https://doi.org/10.0000/EXAMPLE"}) == (
        "doi:10.0000/example"
    )
    assert paper_identity({"arxiv_id": "arXiv:0000.00000v2"}) == ("arxiv:0000.00000")


def test_paper_example_skips_seen_first_choice_and_uses_one_alternate(tmp_path):
    provider = PaperBrowseExample(tmp_path, clock=lambda: NOW)
    first = {
        "read_verified": True,
        "title": "First synthetic paper",
        "summary": "First summary.",
        "source_url": "https://example.org/papers/first",
    }
    second = {
        "read_verified": True,
        "title": "Second synthetic paper",
        "summary": "Second summary.",
        "source_url": "https://example.org/papers/second",
    }

    first_result = provider.run(context(papers=[first]))
    alternate_result = provider.run(context(papers=[first, second]))

    assert first_result["title"] == "First synthetic paper"
    assert alternate_result["title"] == "Second synthetic paper"
    assert len(provider.seen.rows()) == 2
    assert provider.eligible(context(papers=[first, second])) is False


def test_paper_example_considers_only_first_choice_and_one_alternate(tmp_path):
    provider = PaperBrowseExample(tmp_path, clock=lambda: NOW)
    candidates = [
        {
            "read_verified": True,
            "title": f"Synthetic paper {index}",
            "summary": f"Summary {index}.",
            "source_url": f"https://example.org/papers/{index}",
        }
        for index in range(3)
    ]
    provider.run(context(papers=[candidates[0]]))
    provider.run(context(papers=[candidates[1]]))

    assert provider.eligible(context(papers=candidates)) is False
