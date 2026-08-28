from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime, timedelta

from moonbite_plugin.panel_api import PanelConfig, create_panel


def test_panel_api_import_does_not_load_hermes_adapter():
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "from moonbite_plugin.panel_api import create_panel; "
                "assert callable(create_panel); "
                "assert 'moonbite_plugin.hermes_adapter' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_create_panel_is_owner_scoped_by_default_and_uses_typed_lifecycle(tmp_path):
    now = datetime(2026, 8, 25, 12, tzinfo=UTC)
    config = PanelConfig(tmp_path, timezone="UTC", anchor_hour=6, owner="host-a")
    panel = create_panel(
        root=config.root,
        timezone=config.timezone,
        anchor_hour=config.anchor_hour,
        owner=config.owner,
        clock=lambda: now,
    )

    panel.set_field(
        "current_focus",
        {"topic": "writing"},
        source="host",
        ttl=timedelta(hours=3),
    )
    panel.set_field(
        "profile",
        {"mode": "quiet"},
        source="host",
        persistent=True,
    )

    snapshot = panel.snapshot(now=now)
    assert snapshot["fields"]["current_focus"]["value"] == {"topic": "writing"}
    assert snapshot["fields"]["profile"]["persistence_policy"] == "persistent"
    assert panel.snapshot(owner="host-b", now=now)["fields"] == {}
    assert panel.snapshot(now=now + timedelta(hours=4))["fields"] == {
        "profile": snapshot["fields"]["profile"]
    }


def test_create_panel_requires_explicit_all_owner_snapshot(tmp_path):
    now = datetime(2026, 8, 25, 12, tzinfo=UTC)
    panel_a = create_panel(root=tmp_path, owner="a", clock=lambda: now)
    panel_b = create_panel(root=tmp_path, owner="b", clock=lambda: now)

    panel_a.set_field("focus", "alpha", source="host", persistent=True)
    panel_b.set_field("focus", "beta", source="host", persistent=True)

    assert {field["owner"] for field in panel_a.snapshot()["fields"].values()} == {"a"}
    assert {field["owner"] for field in panel_b.snapshot()["fields"].values()} == {"b"}
    assert set(panel_a.snapshot()["owner_epochs"]) == {"a"}
    assert set(panel_a.snapshot()["owner_reset_policies"]) == {"a"}
    assert {
        field["owner"] for field in panel_a.snapshot_all_owners()["fields"].values()
    } == {"a", "b"}
