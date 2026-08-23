"""One projected outcome per outcome, no matter how often projection re-runs.

`002_evidence_ledger.sql:50` declares `UNIQUE (event_id, kind, status,
correlation_id)` over three NULLABLE columns, and SQLite treats NULLs as
distinct in a UNIQUE. Most projected outcomes carry a NULL correlation_id, so
the constraint never matched and the `INSERT OR IGNORE` behaved as a plain
INSERT. `_backfill_projections` runs on EVERY store open, so the duplicates
accumulated for as long as the deployment kept running -- measured on the live
store at 1382 rows for 858 real outcomes before migration 019.

These counts feed `/v1/summary` and the Prometheus exporter, and the append-only
triggers meant nothing could take them back out.
"""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from observatory.contracts import NormalizedEvent
from observatory.store import EventStore

NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def _outcome(event_id, *, kind="commit", status="succeeded", correlation_id=None, basis=None):
    outcome = {"kind": kind, "status": status}
    if correlation_id is not None:
        outcome["correlation_id"] = correlation_id
    if basis is not None:
        outcome["correlation_basis"] = basis
    return NormalizedEvent.from_mapping(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "outcome.commit",
            "observed_at": NOW,
            "received_at": NOW,
            "project": {"project_id": "repo_sha256:abc"},
            "execution": {"session_id": "S"},
            "outcome": outcome,
        }
    )


class ProjectionIdentityTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = Path(self._dir.name) / "events.sqlite3"
        self.store = EventStore(self.path)
        # Late-bound: a test may reopen the store, and Windows refuses to unlink
        # the database if the earlier handle is the one that gets closed.
        self.addCleanup(lambda: self.store.close())

    def _rows(self, store=None):
        target = store or self.store
        return target.connection.execute("SELECT COUNT(*) FROM outcome_events").fetchone()[0]

    def test_a_null_correlation_id_still_dedupes(self):
        # The exact live shape: kind and status set, correlation_id NULL.
        self.store.append(_outcome("e1"))
        self.assertEqual(self._rows(), 1)
        self.store.append(_outcome("e1"))
        self.assertEqual(self._rows(), 1, "a NULL correlation_id defeated the UNIQUE")

    def test_reopening_the_store_does_not_reproject(self):
        # `_backfill_projections` runs on every open. Before migration 019 this
        # added one duplicate row per open, forever.
        for index in range(5):
            self.store.append(_outcome(f"e{index}"))
        self.store.close()
        for _ in range(4):
            store = EventStore(self.path)
            count = self._rows(store)
            store.close()
            self.assertEqual(count, 5, "reopening the store reprojected the outcomes")
        self.store = EventStore(self.path)

    def test_recorrelation_does_not_duplicate(self):
        for index in range(3):
            self.store.append(_outcome(f"e{index}", correlation_id="task-1", basis="task_id"))
        self.store.recorrelate_outcomes()
        self.store.recorrelate_outcomes()
        self.assertEqual(self._rows(), 3)

    def test_genuinely_different_outcomes_are_still_distinct(self):
        # The fix must not collapse real outcomes into one row.
        self.store.append(_outcome("e1", status="succeeded"))
        self.store.append(_outcome("e2", status="failed"))
        self.store.append(_outcome("e3", kind="ci", status="succeeded"))
        self.assertEqual(self._rows(), 3)

    def test_the_correlation_basis_is_part_of_the_identity(self):
        # Two projections of one event that disagree about the basis of their
        # correlation are different claims; collapsing them would erase the
        # disagreement rather than record it.
        rows = self.store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_outcome_events_identity'"
        ).fetchall()
        self.assertEqual(len(rows), 1, "the identity index was not created")
        sql = self.store.connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='idx_outcome_events_identity'"
        ).fetchone()["sql"]
        self.assertIn("correlation_basis", sql)

    def test_the_append_only_triggers_survive_the_migration(self):
        names = sorted(r["name"] for r in self.store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='outcome_events'"))
        self.assertEqual(names, ["prevent_outcome_events_delete", "prevent_outcome_events_update"])
        self.store.append(_outcome("e1"))
        with self.assertRaises(Exception):
            self.store.connection.execute("DELETE FROM outcome_events")


if __name__ == "__main__":
    unittest.main()
