"""Standalone example demonstrating Moonbite's typed Panel (Daily RAM) API.

This script uses Moonbite's standalone Panel API directly as a Python library.
It requires no Hermes Agent installation and performs no network access. The
factory reuses Moonbite's small durable EventBus primitives but never creates
the full MoonbiteRuntime.

Requirements:
    - Python >= 3.11
    - POSIX environment (Linux, WSL2, macOS) for fcntl file locking
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from moonbite_plugin.panel_api import OwnerBoundPanelStore, PanelValue, create_panel


def main() -> None:
    # 1. Create a dedicated directory for panel state.
    # In production, this would be a persistent directory such as ~/.config/my_app/panel.
    with tempfile.TemporaryDirectory() as temp_dir:
        state_dir = Path(temp_dir) / "panel_data"
        state_dir.mkdir(parents=True, exist_ok=True)

        print(f"Initializing PanelStore in {state_dir}...")

        # 2. Create an owner-bound Panel store and minimal EventBus.
        panel: OwnerBoundPanelStore = create_panel(
            root=state_dir,
            timezone="UTC",
            anchor_hour=6,  # Daily rollover epoch occurs at 06:00 UTC
            owner="companion_core",
        )

        now = datetime.now(timezone.utc)

        # 3. Set a standard TTL-bounded field.
        # TTL fields become invisible after the specified timedelta; reads do
        # not delete the durable record.
        print("\n--- 1. Setting TTL field ('energy_level') ---")
        energy: PanelValue = panel.set_field(
            name="energy_level",
            value="high",
            source="morning_survey",
            ttl=timedelta(hours=4),
            confidence=0.95,
            observed_at=now,
            daily=True,
        )
        print(
            f"Set TTL field: {energy.name}={energy.value} (expires at {energy.expires_at})"
        )

        # 4. Set a persistent field.
        # Persistent fields never expire and survive daily rollovers.
        print("\n--- 2. Setting Persistent field ('user_theme') ---")
        theme: PanelValue = panel.set_field(
            name="user_theme",
            value="nordic_dark",
            source="user_preference",
            persistent=True,
            confidence=1.0,
            observed_at=now,
        )
        print(
            f"Set Persistent field: {theme.name}={theme.value} (lifecycle={theme.persistence_policy})"
        )

        # 5. Set a consume-once field.
        # Consume-once fields can only be consumed once with the matching source_event_id.
        print("\n--- 3. Setting Consume-Once field ('pending_notification') ---")
        alert_event_id = "evt_notify_20260825_001"
        alert: PanelValue = panel.set_field(
            name="pending_notification",
            value={"title": "Scheduled Maintenance", "urgency": "medium"},
            source="scheduler",
            consume_once=True,
            ttl=timedelta(hours=2),
            source_event_id=alert_event_id,
            observed_at=now,
        )
        print(
            f"Set Consume-Once field: {alert.name} (event_id={alert.source_event_id})"
        )

        # 6. Read a snapshot of active fields.
        # Reads are pure queries and never mutate, compact, or expire state.
        print("\n--- 4. Reading initial snapshot ---")
        snapshot = panel.snapshot(now=now)
        print(f"Current Epoch: {snapshot['epoch']}")
        print(f"Active fields count: {len(snapshot['fields'])}")
        for key, field_data in snapshot["fields"].items():
            print(
                f"  - {key}: {field_data['value']} (policy={field_data['persistence_policy']})"
            )

        # 7. Consume the consume-once field.
        print("\n--- 5. Consuming consume-once field ---")
        consumed: PanelValue = panel.consume(
            name="pending_notification",
            source_event_id=alert_event_id,
            now=now,
        )
        print(f"Consumed field: {consumed.name} at {consumed.consumed_at}")

        # Verify that consumed field is no longer visible in snapshot
        snapshot_after_consume = panel.snapshot(now=now)
        print(f"Fields count after consume: {len(snapshot_after_consume['fields'])}")
        assert "pending_notification" not in snapshot_after_consume["fields"]

        # 8. Render a markdown summary suitable for context injection.
        print("\n--- 6. Markdown representation ---")
        markdown_view = panel.render_markdown(now=now)
        print(markdown_view)

        # 9. Demonstrate explicit daily rollover.
        # Advancing time past the anchor hour and triggering rollover clears yesterday's daily fields.
        tomorrow = now + timedelta(days=1)
        print(
            f"--- 7. Performing explicit rollover for new epoch ({tomorrow.date()}) ---"
        )
        rollover_snapshot = panel.rollover(now=tomorrow)
        print(f"New Epoch after rollover: {rollover_snapshot['epoch']}")
        print(
            f"Remaining fields (persistent only): {list(rollover_snapshot['fields'].keys())}"
        )
        assert "energy_level" not in rollover_snapshot["fields"]
        assert "user_theme" in rollover_snapshot["fields"]

        print("\nAll standalone Panel operations completed successfully.")


if __name__ == "__main__":
    main()
