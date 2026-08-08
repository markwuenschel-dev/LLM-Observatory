import json
from pathlib import Path
import tempfile
import unittest

from observatory.contracts import NormalizedEvent
from observatory.store import EventStore


ROOT = Path(__file__).resolve().parents[1]


class SyntheticFixtureTests(unittest.TestCase):
    def test_fixture_exercises_projects_providers_hierarchy_and_outcomes(self) -> None:
        fixture = ROOT / "examples" / "synthetic-events.jsonl"
        values = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(values), 6)
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                for value in values:
                    store.append(NormalizedEvent.from_mapping(value))
                summary = store.summary()
                self.assertEqual(summary["events"], 6)
                self.assertEqual(summary["projects"], 3)
                self.assertEqual(summary["tool_calls"], 5)
                self.assertEqual(summary["files_inspected"], 13)
                self.assertEqual(summary["files_changed"], 2)
                self.assertEqual(summary["commands_executed"], 5)
                self.assertEqual(summary["tests_invoked"], 3)
                self.assertEqual(summary["agent_failures"], 1)
                self.assertEqual(summary["reassessments"], 2)
                self.assertEqual(summary["rework_loops"], 1)
                self.assertEqual(summary["outcomes"], [{"kind": "tests", "status": "passed", "count": 1}])
                self.assertGreaterEqual(store.measurement_count(), 10)
                dimensions = store.metric_dimensions()
                self.assertTrue(any(item["workflow"] == "workflow-implementation" for item in dimensions["workflow"]))
                self.assertTrue(any(item["agent"] == "agent-parent" for item in dimensions["agent"]))
                for dimension in ("execution", "workflow", "agent"):
                    self.assertTrue(dimensions[dimension])
                    self.assertTrue({"event_type", "project", "repository", "branch"}.issubset(dimensions[dimension][0]))

    def test_bitemporal_fixture_preserves_late_arrival_and_event_time_order(self) -> None:
        fixture = ROOT / "examples" / "synthetic-bitemporal.jsonl"
        values = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines() if line.strip()]
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                for value in values:
                    self.assertEqual(store.append(NormalizedEvent.from_mapping(value)).status, "inserted")

                observed_order = [event.event_id for event in store.list_events()]
                self.assertEqual(observed_order, [
                    "bitemporal-observed-1359",
                    "bitemporal-observed-1400",
                    "bitemporal-observed-1405",
                ])
                rows = store.connection.execute(
                    "SELECT event_id, observed_at, received_at FROM events ORDER BY observed_at"
                ).fetchall()
                self.assertEqual(rows[0]["observed_at"], "2026-08-07T13:59:00+00:00")
                self.assertEqual(rows[0]["received_at"], "2026-08-07T14:11:00+00:00")
                self.assertEqual(
                    [event.event_id for event in store.list_events({"start": "2026-08-07T14:00:00Z"})],
                    ["bitemporal-observed-1400", "bitemporal-observed-1405"],
                )
                facts = store.measurement_facts(event_id="bitemporal-observed-1359")
                self.assertTrue(facts)
                self.assertTrue(all(item["received_at"] == "2026-08-07T14:11:00+00:00" for item in facts))


if __name__ == "__main__":
    unittest.main()
