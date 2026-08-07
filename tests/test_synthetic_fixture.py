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
                self.assertEqual(summary["outcomes"], [{"kind": "tests", "status": "passed", "count": 1}])
                self.assertGreaterEqual(store.measurement_count(), 10)
                dimensions = store.metric_dimensions()
                self.assertTrue(any(item["workflow"] == "workflow-implementation" for item in dimensions["workflow"]))
                self.assertTrue(any(item["agent"] == "agent-parent" for item in dimensions["agent"]))


if __name__ == "__main__":
    unittest.main()
