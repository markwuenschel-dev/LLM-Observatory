"""Ranking resolution is the operator's to choose.

Grouping on all five dimensions at once splits a modest corpus into many groups
of one or two, so every group falls under `min_outcomes` and nothing ranks --
which reads as "not enough data yet" when the truth may be "asked at too fine a
grain". Measured live, 11 groups of 1-3 collapsed to a single group once asked
by client alone.

Coarsening must not invent evidence: the same outcomes are reported at a
resolution they can support, and the coverage fractions travel either way. These
tests pin that, and pin that the dimension names can never reach the SQL as
caller-supplied text.
"""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from observatory.contracts import NormalizedEvent
from observatory.outcomes import make_outcome_event
from observatory.store import RANKING_DIMENSIONS, EventStore

NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def _llm(event_id, session, model, *, agent, cost=0.10):
    return NormalizedEvent.from_mapping({
        "schema_version": "1.0",
        "event_id": event_id,
        "event_type": "model.operation",
        "observed_at": NOW,
        "received_at": NOW,
        "project": {"project_id": "repo_sha256:abc"},
        "execution": {"session_id": session, "agent_id": agent},
        "llm": {"provider": "anthropic", "model": model, "client": "claude-code"},
        "usage": {"cost": cost, "total_tokens": 100},
        "performance": {"latency_ms": 10},
    })


class RankingDimensionTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)
        # One model, one client, six outcomes -- but split across six agents,
        # which is exactly the live shape that prevented any group reaching the
        # threshold.
        for index in range(6):
            session = f"s{index}"
            self.store.append(_llm(f"e{index}", session, "claude-opus-5", agent=f"agent-{index}"))
            self.store.append(make_outcome_event(
                "tests", "passed" if index < 5 else "failed",
                correlation_basis="session_id", correlation_id=session, evidence_source="pytest"))

    def test_the_finest_grouping_withholds_everything(self):
        report = self.store.engineering_value()
        self.assertEqual(report["ranked"], [])
        self.assertEqual(len(report["insufficient_evidence"]), 6)

    def test_asking_by_model_alone_produces_the_ranking(self):
        report = self.store.engineering_value(dimensions=("model",))
        self.assertEqual(len(report["ranked"]), 1, report["insufficient_evidence"])
        row = report["ranked"][0]
        self.assertEqual(row["model"], "claude-opus-5")
        self.assertEqual(row["outcomes_evaluated"], 6)
        self.assertAlmostEqual(row["success_rate"], 5 / 6)

    def test_coarsening_does_not_invent_outcomes(self):
        # The same six outcomes, however they are grouped.
        fine = sum(r["outcomes_evaluated"] for r in self.store.engineering_value()["insufficient_evidence"])
        coarse = sum(r["outcomes_evaluated"] for r in self.store.engineering_value(dimensions=("model",))["ranked"])
        self.assertEqual(fine, coarse, "regrouping changed the number of outcomes")

    def test_coverage_still_travels_at_every_resolution(self):
        row = self.store.engineering_value(dimensions=("model",))["ranked"][0]
        self.assertIn("coverage", row)
        self.assertEqual(row["coverage"]["cost"]["ratio"], 1.0)
        self.assertTrue(row["association_only"])

    def test_the_default_is_unchanged(self):
        self.assertEqual(self.store.engineering_value(dimensions=None)["ranked"],
                         self.store.engineering_value()["ranked"])

    def test_only_allowlisted_dimensions_are_accepted(self):
        # The names are interpolated into SQL, so caller text must never reach it.
        for bad in ("cost", "e.model; DROP TABLE events", "", "1=1"):
            with self.subTest(dimension=bad):
                with self.assertRaises(ValueError):
                    self.store.engineering_value(dimensions=(bad,))

    def test_an_empty_dimension_list_is_refused(self):
        with self.assertRaises(ValueError):
            self.store.engineering_value(dimensions=())

    def test_duplicates_and_order_do_not_change_the_result(self):
        canonical = self.store.engineering_value(dimensions=("model", "client"))["ranked"]
        shuffled = self.store.engineering_value(dimensions=("client", "model", "model"))["ranked"]
        self.assertEqual(canonical, shuffled)

    def test_every_declared_dimension_actually_works(self):
        for name in RANKING_DIMENSIONS:
            with self.subTest(dimension=name):
                report = self.store.engineering_value(dimensions=(name,))
                self.assertIn("ranked", report)
                rows = report["ranked"] + report["insufficient_evidence"]
                self.assertTrue(rows)
                self.assertIn(name, rows[0])


class DimensionCoverageTests(unittest.TestCase):
    """An unanswerable question must say so, not return silence.

    `skill` was captured on 4 of 42,577 events and workflow/task_class on none,
    so "which skills deliver value" returned an empty result indistinguishable
    from "not enough data yet". One reads as "wait longer"; the other means
    "this can never be answered until the client reports it".
    """

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)
        for index in range(3):
            session = f"s{index}"
            self.store.append(_llm(f"e{index}", session, "claude-opus-5", agent="planner"))
            self.store.append(make_outcome_event(
                "tests", "passed", correlation_basis="session_id",
                correlation_id=session, evidence_source="pytest"))

    def _coverage(self):
        return self.store.engineering_value()["dimension_coverage"]

    def test_every_declared_dimension_is_reported(self):
        coverage = self._coverage()
        for name in RANKING_DIMENSIONS:
            self.assertIn(name, coverage)

    def test_a_captured_dimension_is_marked_rankable(self):
        model = self._coverage()["model"]
        self.assertTrue(model["rankable"])
        self.assertEqual(model["ratio"], 1.0)
        self.assertIsNone(model["note"])

    def test_an_uncaptured_dimension_states_why_it_cannot_rank(self):
        skill = self._coverage()["skill"]
        self.assertFalse(skill["rankable"])
        self.assertEqual(skill["events_with_dimension"], 0)
        self.assertIn("cannot be ranked", skill["note"])
        self.assertIn("until the client reports it", skill["note"])

    def test_one_report_uses_one_denominator(self):
        # Computed per-dimension against a live store, the denominators
        # disagreed between rows of the same report.
        denominators = {c["associated_events"] for c in self._coverage().values()}
        self.assertEqual(len(denominators), 1, f"inconsistent denominators: {denominators}")

    def test_the_grouping_actually_used_is_reported(self):
        report = self.store.engineering_value(dimensions=("model",))
        self.assertEqual(report["grouped_by"], ["model"])
        self.assertEqual(self.store.engineering_value()["grouped_by"], list(RANKING_DIMENSIONS))

    def test_coverage_is_independent_of_the_grouping_asked_for(self):
        # What the telemetry carries does not change with how it is sliced.
        fine = self.store.engineering_value()["dimension_coverage"]
        coarse = self.store.engineering_value(dimensions=("model",))["dimension_coverage"]
        self.assertEqual(fine, coarse)


if __name__ == "__main__":
    unittest.main()
