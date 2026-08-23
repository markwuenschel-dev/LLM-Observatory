"""Snapshot identity in the append-only observation record.

The longitudinal record is the only evidence that observation was ever working,
and its tables are append-only by trigger. So the two failure modes that matter
are (a) silently keeping the wrong observation under an id, and (b) writing rows
that belong to an observation the store then rejected -- neither of which can be
repaired through the application afterwards.

An earlier guard hashed timestamp/verdict/blockers and then "verified" the write
by re-reading those same three fields. That check is tautologically satisfied on
any collision its own id function can produce, and it ran after the commit.
"""

import tempfile
import unittest
from pathlib import Path

from observatory.store import EventStore

RECORDED_AT = "2026-08-23T00:00:00+00:00"


def _report(*, verdict="OBSERVATION_CAPABLE", capable=True, blockers=None, clients=None, **evidence):
    base = {
        "clients_observing": 3, "clients_degraded": 0, "events_in_window": 999,
        "newest_event_age_seconds": 5.0, "store_capacity_ratio": 0.1,
    }
    base.update(evidence)
    return {
        "generated_at": RECORDED_AT,
        "verdict": verdict,
        "observation_capable": capable,
        "evidence": base,
        "blockers": list(blockers or []),
        "clients": list(clients or []),
    }


def _client(name="claude-code", state="OBSERVING", ratio=0.9):
    return {
        "client": name, "state": state, "events_observed": 10,
        "last_event_at": RECORDED_AT, "last_event_age_seconds": 1.0,
        "project_attribution": {"ratio": ratio},
        "blockers": [], "signal_families": {},
    }


class SnapshotIdentityTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.store = EventStore(Path(self._temp.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _count(self, table):
        return self.store.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_observations_differing_only_in_evidence_are_both_kept(self):
        # Same instant, same verdict, same blockers -- but every client has gone
        # degraded and the store has filled up. Hashing only verdict+blockers
        # gave these the same id, and the post-write check compared only those
        # same two fields, so the degradation was discarded without any error.
        healthy = _report(clients=[_client()])
        degraded = _report(
            clients=[_client(state="DEGRADED", ratio=0.0)],
            clients_observing=0, clients_degraded=3, events_in_window=0,
            newest_event_age_seconds=900000.0, store_capacity_ratio=0.99,
        )
        first = self.store.record_observation(healthy)
        second = self.store.record_observation(degraded)
        self.assertNotEqual(first, second, "two different observations were given one id")
        self.assertEqual(self._count("observation_snapshots"), 2)
        stored = dict(self.store.connection.execute(
            "SELECT clients_observing, clients_degraded, events_in_window, store_capacity_ratio"
            " FROM observation_snapshots WHERE snapshot_id = ?", (second,)).fetchone())
        self.assertEqual(stored["clients_observing"], 0)
        self.assertEqual(stored["clients_degraded"], 3)
        self.assertEqual(stored["events_in_window"], 0)
        self.assertAlmostEqual(stored["store_capacity_ratio"], 0.99)

    def test_blocker_order_is_treated_consistently_by_the_id_and_the_check(self):
        # The id hashed sorted(blockers) while the stored column and the
        # comparison used list(blockers). Same id, different stored text, so a
        # mere permutation raised RuntimeError -- and `observe --record` turns
        # that into a failed exit that discards the whole report.
        first = self.store.record_observation(_report(blockers=["alpha", "beta"]))
        second = self.store.record_observation(_report(blockers=["beta", "alpha"]))
        self.assertEqual(self._count("observation_snapshots"), 2)
        self.assertNotEqual(first, second)

    def test_recording_the_same_report_twice_is_an_idempotent_replay(self):
        report = _report(clients=[_client()])
        first = self.store.record_observation(report)
        second = self.store.record_observation(report)
        self.assertEqual(first, second)
        self.assertEqual(self._count("observation_snapshots"), 1)
        self.assertEqual(self._count("observation_client_snapshots"), 1)

    def test_a_rejected_observation_writes_nothing_at_all(self):
        # The check used to run after the commit, so client rows from the
        # rejected observation stayed attached to the accepted snapshot. Both
        # `(id, c1)` and `(id, c2)` ended up under one id, in a table whose
        # `_no_update` trigger makes that unrepairable through the application.
        self.store.record_observation(_report(clients=[_client("c1")]), snapshot_id="fixed")
        rejected = _report(verdict="NOT_OBSERVATION_CAPABLE", capable=False,
                           clients=[_client("c2", state="DEGRADED")])
        with self.assertRaises(RuntimeError):
            self.store.record_observation(rejected, snapshot_id="fixed")

        clients = [r["client"] for r in self.store.connection.execute(
            "SELECT client FROM observation_client_snapshots WHERE snapshot_id = 'fixed'")]
        self.assertEqual(clients, ["c1"], "a rejected observation left rows behind")
        verdict = self.store.connection.execute(
            "SELECT verdict FROM observation_snapshots WHERE snapshot_id = 'fixed'").fetchone()["verdict"]
        self.assertEqual(verdict, "OBSERVATION_CAPABLE")

    def test_client_rows_are_recorded_for_every_client_in_the_report(self):
        self.store.record_observation(_report(clients=[_client("a"), _client("b"), _client("c")]))
        self.assertEqual(self._count("observation_client_snapshots"), 3)

    def test_coverage_drift_never_mixes_two_observations_under_one_id(self):
        # `coverage_drift` reads client rows by snapshot; rows leaked from a
        # rejected observation would be compared as if they were one sample.
        self.store.record_observation(_report(clients=[_client("a")]), snapshot_id="s1")
        with self.assertRaises(RuntimeError):
            self.store.record_observation(
                _report(verdict="X", clients=[_client("b")]), snapshot_id="s1")
        rows = self.store.connection.execute(
            "SELECT snapshot_id, COUNT(DISTINCT client) n FROM observation_client_snapshots"
            " GROUP BY snapshot_id").fetchall()
        self.assertEqual([(r["snapshot_id"], r["n"]) for r in rows], [("s1", 1)])


if __name__ == "__main__":
    unittest.main()
