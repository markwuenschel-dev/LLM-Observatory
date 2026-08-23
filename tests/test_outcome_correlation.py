"""Tests for outcome correlation and cost-per-outcome analysis.

Two failures are guarded here. First, the shipped outcome collectors used to
produce *zero* attribution edges: they recorded an outcome but never declared
which shared identifier it should be joined on, so no analysis could ever reach
them. Second, an aggregation that sums cost across clients which do not all
report cost would rank a silent client as the cheapest.
"""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from observatory.contracts import NormalizedEvent
from observatory.hooks import build_hook_event
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore

NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def _llm(event_id, session, model, *, cost=None, tokens=1000, retries=0, agent="planner", skill="tdd"):
    usage = {"total_tokens": tokens, "input_tokens": tokens}
    if cost is not None:
        usage["cost"] = cost
    return NormalizedEvent.from_mapping(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "model.operation",
            "observed_at": NOW,
            "received_at": NOW,
            "project": {"project_id": "repo_sha256:abc"},
            "execution": {"session_id": session, "agent_id": agent, "skill": skill},
            "llm": {"provider": "anthropic", "model": model, "client": "claude-code"},
            "usage": usage,
            "performance": {"latency_ms": 900},
            "reliability": {"retry_count": retries},
        }
    )


class StoreCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _edges(self):
        return self.store.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE relation = 'outcome_correlation'"
        ).fetchone()[0]


class CollectorsProduceEdgesTests(StoreCase):
    def test_a_client_hook_correlates_through_its_session(self):
        # Hook events previously carried no basis and were unjoinable.
        self.store.append(_llm("llm-1", "S", "claude-opus-5"))
        self.store.append(
            build_hook_event(
                "claude-code",
                {"session_id": "S", "hook_event_name": "SessionEnd", "cwd": self._dir.name},
                project_path=self._dir.name,
            )
        )
        self.assertGreater(self._edges(), 0)
        basis = self.store.connection.execute(
            "SELECT correlation_basis FROM outcome_events WHERE kind = 'client-hook'"
        ).fetchone()["correlation_basis"]
        self.assertEqual(basis, "session_id")

    def test_a_hook_without_a_session_declares_no_basis(self):
        # Better to be honestly unjoinable than to invent a correlation.
        self.store.append(
            build_hook_event(
                "claude-code",
                {"hook_event_name": "Notification", "cwd": self._dir.name},
                project_path=self._dir.name,
            )
        )
        basis = self.store.connection.execute(
            "SELECT correlation_basis FROM outcome_events"
        ).fetchone()["correlation_basis"]
        self.assertIsNone(basis)
        self.assertEqual(self._edges(), 0)

    def test_an_outcome_declaring_a_session_basis_links_that_sessions_work(self):
        for index in range(3):
            self.store.append(_llm(f"llm-{index}", "S", "claude-opus-5"))
        self.store.append(
            make_outcome_event(
                "tests", "passed",
                correlation_basis="session_id", correlation_id="S", evidence_source="pytest",
            )
        )
        # One edge per direction, per associated operation.
        self.assertEqual(self._edges(), 6)


class OutcomeValueTests(StoreCase):
    def _seed(self):
        for index in range(3):
            self.store.append(_llm(f"a{index}", "A", "claude-opus-5", cost=0.10))
        for index in range(3):
            self.store.append(_llm(f"b{index}", "B", "claude-haiku-4-5", cost=0.01, retries=2))
        for index in range(2):
            self.store.append(_llm(f"c{index}", "C", "mystery-model", cost=None))
        for session, status in (("A", "passed"), ("B", "failed"), ("C", "passed")):
            self.store.append(
                make_outcome_event(
                    "tests", status,
                    correlation_basis="session_id", correlation_id=session, evidence_source="pytest",
                )
            )

    def test_spend_is_attributed_to_the_outcome_it_is_associated_with(self):
        self._seed()
        rows = {(r["model"], r["outcome_status"]): r for r in self.store.outcome_value()}
        self.assertAlmostEqual(rows[("claude-opus-5", "passed")]["cost"], 0.30, places=6)
        self.assertAlmostEqual(rows[("claude-haiku-4-5", "failed")]["cost"], 0.03, places=6)
        self.assertEqual(rows[("claude-haiku-4-5", "failed")]["retries"], 6)
        self.assertEqual(rows[("claude-opus-5", "passed")]["retries"], 0)

    def test_a_client_that_never_reports_cost_is_not_reported_as_free(self):
        self._seed()
        row = next(r for r in self.store.outcome_value() if r["model"] == "mystery-model")
        self.assertIsNone(row["cost"])
        self.assertEqual(row["coverage"]["cost"]["ratio"], 0.0)
        self.assertEqual(row["coverage"]["cost"]["reported_events"], 0)
        self.assertEqual(row["coverage"]["cost"]["observed_events"], 2)

    def test_every_row_carries_its_correlation_basis_and_refuses_causal_framing(self):
        self._seed()
        for row in self.store.outcome_value():
            self.assertEqual(row["correlation_basis"], "session_id")
            self.assertTrue(row["association_only"])
            self.assertNotIn("caused_by", row)

    def test_coverage_is_reported_for_every_aggregated_measure(self):
        self._seed()
        for row in self.store.outcome_value():
            for measure in ("cost", "tokens", "latency"):
                coverage = row["coverage"][measure]
                self.assertIn("reported_events", coverage)
                self.assertIn("observed_events", coverage)
                self.assertEqual(coverage["observed_events"], row["associated_events"])

    def test_the_outcome_event_itself_is_not_counted_as_llm_activity(self):
        self._seed()
        for row in self.store.outcome_value():
            self.assertNotEqual(row["model"], "unknown")

    def test_uncorrelated_work_does_not_appear(self):
        # Operations with no associated outcome must not be silently folded in.
        self.store.append(_llm("lonely", "Z", "claude-opus-5", cost=5.0))
        self.assertEqual(self.store.outcome_value(), [])


class CiOutcomeTests(unittest.TestCase):
    """CI is the only widely available source of a real pass/fail result.

    Commits say work landed; they never say whether it worked, so a store fed
    only by commits can never produce a success rate however long it runs.
    """

    def _run(self, payload, returncode=0):
        from unittest.mock import patch

        import observatory.outcomes as outcomes

        completed = type("R", (), {"returncode": returncode, "stdout": payload, "stderr": ""})()
        return patch.object(outcomes.subprocess, "run", return_value=completed)

    def test_conclusions_map_onto_the_outcome_vocabulary(self):
        import json as _json

        from observatory.outcomes import ci_run_outcomes

        payload = _json.dumps([
            {"conclusion": "success", "headSha": "a" * 40, "createdAt": "2026-08-22T10:00:00Z", "name": "CI", "databaseId": 1},
            {"conclusion": "failure", "headSha": "b" * 40, "createdAt": "2026-08-22T11:00:00Z", "name": "CI", "databaseId": 2},
            {"conclusion": "cancelled", "headSha": "c" * 40, "createdAt": "2026-08-22T12:00:00Z", "name": "CI", "databaseId": 3},
        ])
        with self._run(payload):
            events = ci_run_outcomes(".")
        self.assertEqual([e.outcome.status for e in events], ["passed", "failed", "aborted"])
        self.assertTrue(all(e.outcome.kind == "ci" for e in events))
        self.assertTrue(all(e.outcome.correlation_basis == "project_window" for e in events))

    def test_a_run_still_in_progress_is_not_an_outcome(self):
        import json as _json

        from observatory.outcomes import ci_run_outcomes

        payload = _json.dumps([
            {"conclusion": None, "headSha": "d" * 40, "createdAt": "2026-08-22T13:00:00Z", "name": "CI", "databaseId": 4},
        ])
        with self._run(payload):
            self.assertEqual(ci_run_outcomes("."), [])

    def test_an_unavailable_cli_is_not_an_error(self):
        from unittest.mock import patch

        import observatory.outcomes as outcomes
        from observatory.outcomes import ci_run_outcomes

        with patch.object(outcomes.subprocess, "run", side_effect=OSError("gh not found")):
            self.assertEqual(ci_run_outcomes("."), [])
        with self._run("", returncode=1):
            self.assertEqual(ci_run_outcomes("."), [])

    def test_only_metadata_is_retained(self):
        import json as _json

        from observatory.outcomes import ci_run_outcomes

        payload = _json.dumps([
            {"conclusion": "success", "headSha": "e" * 40, "createdAt": "2026-08-22T10:00:00Z", "name": "CI", "databaseId": 7},
        ])
        with self._run(payload):
            event = ci_run_outcomes(".")[0]
        # A short SHA, workflow name, and run id -- never job logs or output.
        self.assertEqual(event.attributes["commit"], "e" * 12)
        self.assertEqual(event.attributes["workflow"], "CI")
        self.assertNotIn("logs", event.attributes)
        self.assertNotIn("output", event.attributes)


class TemporalAssociationTests(StoreCase):
    """Commits carry no session or task, so they are associated by time.

    This is the weakest basis the store accepts, so it must be bounded, labelled,
    and must never reach across projects.
    """

    def _event(self, event_id, project_id, minutes_before):
        from datetime import datetime, timedelta, timezone

        stamp = (datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc) - timedelta(minutes=minutes_before)).isoformat()
        return NormalizedEvent.from_mapping(
            {
                "schema_version": "1.0",
                "event_id": event_id,
                "event_type": "model.operation",
                "observed_at": stamp,
                "received_at": stamp,
                "project": {"project_id": project_id},
                "execution": {"session_id": f"s-{event_id}"},
                "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
                "usage": {"cost": 0.5, "total_tokens": 100},
            }
        )

    def _commit(self, project_id, *, window_seconds=4 * 3600):
        return make_outcome_event(
            "commit", "landed",
            correlation_id=project_id, correlation_basis="project_window",
            evidence_source="git", observed_at="2026-08-22T12:00:00+00:00",
            attributes={"commit": "abc123", "correlation_window_seconds": window_seconds},
        )

    def test_work_shortly_before_a_commit_in_the_same_project_is_associated(self):
        self.store.append(self._event("recent", "repo_sha256:abc", 30))
        self.store.append(self._commit("repo_sha256:abc"))
        self.assertGreater(self._edges(), 0)

    def test_work_outside_the_window_is_not_associated(self):
        self.store.append(self._event("old", "repo_sha256:abc", 60 * 12))
        self.store.append(self._commit("repo_sha256:abc"))
        self.assertEqual(self._edges(), 0)

    def test_work_in_another_project_is_never_associated(self):
        self.store.append(self._event("elsewhere", "repo_sha256:other", 30))
        self.store.append(self._commit("repo_sha256:abc"))
        self.assertEqual(self._edges(), 0)

    def test_work_after_the_commit_is_not_associated(self):
        # A commit cannot be the outcome of work that had not happened yet.
        self.store.append(self._event("later", "repo_sha256:abc", -30))
        self.store.append(self._commit("repo_sha256:abc"))
        self.assertEqual(self._edges(), 0)

    def test_an_oversized_declared_window_is_clamped(self):
        from observatory.store import MAX_PROJECT_WINDOW_SECONDS, _project_window_seconds

        self.assertEqual(_project_window_seconds({"correlation_window_seconds": 10**9}), MAX_PROJECT_WINDOW_SECONDS)
        self.assertGreater(_project_window_seconds({}), 0)

    def test_the_declared_window_is_recorded_on_the_outcome(self):
        # A reader must be able to see how loose the association was.
        self.store.append(self._event("recent", "repo_sha256:abc", 30))
        commit = self._commit("repo_sha256:abc", window_seconds=1800)
        self.store.append(commit)
        self.assertEqual(commit.attributes["correlation_window_seconds"], 1800)
        basis = self.store.connection.execute(
            "SELECT correlation_basis FROM outcome_events WHERE kind = 'commit'"
        ).fetchone()["correlation_basis"]
        self.assertEqual(basis, "project_window")


class EngineeringValueRankingTests(StoreCase):
    """The question this store exists to answer is the easiest to answer badly."""

    def _config(self, prefix, model, count, passed, *, cost=None, retries=0):
        for index in range(count):
            session = f"{prefix}{index}"
            self.store.append(_llm(f"{prefix}e{index}", session, model, cost=cost, retries=retries))
            self.store.append(
                make_outcome_event(
                    "tests", "passed" if index < passed else "failed",
                    correlation_basis="session_id", correlation_id=session, evidence_source="pytest",
                )
            )

    def test_configurations_are_ranked_by_success_then_cost(self):
        self._config("A", "reliable-model", 8, 7, cost=0.20)
        self._config("B", "cheap-model", 6, 2, cost=0.02, retries=3)
        ranked = self.store.engineering_value()["ranked"]
        self.assertEqual([r["model"] for r in ranked], ["reliable-model", "cheap-model"])
        self.assertAlmostEqual(ranked[0]["success_rate"], 7 / 8)
        self.assertGreater(ranked[1]["retries"], 0)

    def test_a_thin_sample_is_withheld_from_the_ranking_with_a_reason(self):
        self._config("A", "reliable-model", 8, 7, cost=0.20)
        self._config("N", "brand-new-model", 2, 2, cost=0.01)
        report = self.store.engineering_value()
        self.assertEqual([r["model"] for r in report["ranked"]], ["reliable-model"])
        withheld = report["insufficient_evidence"]
        self.assertEqual([r["model"] for r in withheld], ["brand-new-model"])
        self.assertIn("at least", withheld[0]["evidence_note"])

    def test_a_configuration_with_partial_cost_cannot_appear_cheap(self):
        self._config("A", "reliable-model", 6, 6, cost=0.20)
        self._config("Q", "quiet-model", 6, 6, cost=None)
        ranked = {r["model"]: r for r in self.store.engineering_value()["ranked"]}
        self.assertIsNone(ranked["quiet-model"]["cost_per_success"])
        self.assertEqual(ranked["quiet-model"]["coverage"]["cost"]["ratio"], 0.0)
        self.assertIn("withheld rather than understated", ranked["quiet-model"]["evidence_note"])
        self.assertIsNotNone(ranked["reliable-model"]["cost_per_success"])

    def test_effort_is_not_double_counted_across_multiple_outcomes(self):
        self.store.append(_llm("solo", "S", "reliable-model", cost=1.0))
        for index in range(6):
            self.store.append(
                make_outcome_event(
                    "tests", "passed", correlation_basis="session_id", correlation_id="S",
                    evidence_source=f"pytest-{index}",
                )
            )
        row = self.store.engineering_value()["ranked"][0]
        self.assertEqual(row["associated_events"], 1)
        self.assertAlmostEqual(row["cost"], 1.0)

    def test_outcomes_without_a_pass_fail_result_do_not_create_a_success_rate(self):
        """A landed commit is neither a pass nor a failure.

        Counting it as "not passed" drags every configuration toward zero and
        invents a precise number out of a category error.
        """

        for index in range(6):
            session = f"C{index}"
            self.store.append(_llm(f"Ce{index}", session, "commit-only-model", cost=0.05))
            self.store.append(
                make_outcome_event(
                    "commit", "landed", correlation_basis="session_id",
                    correlation_id=session, evidence_source="git",
                )
            )
        report = self.store.engineering_value()
        self.assertEqual(report["ranked"], [])
        withheld = report["insufficient_evidence"]
        self.assertEqual(len(withheld), 1)
        self.assertIsNone(withheld[0]["success_rate"])
        self.assertEqual(withheld[0]["outcomes_non_binary"], 6)
        self.assertIn("none with a pass/fail result", withheld[0]["evidence_note"])

    def test_a_success_rate_uses_only_pass_fail_outcomes_as_its_denominator(self):
        self._config("A", "mixed-model", 6, 4, cost=0.10)
        for index in range(10):
            session = f"K{index}"
            self.store.append(_llm(f"Ke{index}", session, "mixed-model", cost=0.10))
            self.store.append(
                make_outcome_event(
                    "commit", "landed", correlation_basis="session_id",
                    correlation_id=session, evidence_source="git",
                )
            )
        row = self.store.engineering_value()["ranked"][0]
        # 4 of 6 tests passed; the 10 commits must not dilute that to 4/16.
        self.assertEqual(row["outcomes_evaluated"], 6)
        self.assertEqual(row["outcomes_non_binary"], 10)
        self.assertAlmostEqual(row["success_rate"], 4 / 6)

    def test_every_row_refuses_causal_framing(self):
        self._config("A", "reliable-model", 6, 6, cost=0.20)
        report = self.store.engineering_value()
        self.assertTrue(report["association_only"])
        self.assertIn("not shown to cause", report["note"])
        for row in report["ranked"]:
            self.assertTrue(row["association_only"])
            self.assertNotIn("caused_by", row)


class CorrelationBoundTests(StoreCase):
    """Edge growth must be bounded in BOTH directions, not just forward.

    The forward branches (outcome -> events) were capped on the argument that a
    long session would otherwise link every hook to every event in it. That
    argument is symmetric, and the reverse branches were left uncapped: this one
    runs for every non-outcome event on the append path, so each new event
    re-linked itself to every outcome the session had ever produced. Capping one
    direction left the session total unbounded regardless.
    """

    def setUp(self):
        super().setUp()
        import observatory.store as store_module

        self._module = store_module
        self._original = store_module.CORRELATION_MAX_LINKS
        store_module.CORRELATION_MAX_LINKS = 5
        self.addCleanup(setattr, store_module, "CORRELATION_MAX_LINKS", self._original)

    def _edges_from(self, event_id):
        return self.store.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE child_event_id = ?"
            " AND relation = 'outcome_correlation'",
            (event_id,),
        ).fetchone()[0]

    def _seed_hooks(self, count=20):
        """Distinct outcomes in one session, one minute apart."""
        from datetime import datetime, timedelta, timezone

        base = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
        stamps = []
        for index in range(count):
            stamp = (base + timedelta(minutes=index)).isoformat()
            stamps.append(stamp)
            self.store.append(
                make_outcome_event(
                    "client-hook", "post_tool_use",
                    correlation_basis="session_id", correlation_id="S",
                    evidence_source="hook", observed_at=stamp,
                )
            )
        return stamps

    def test_a_new_event_does_not_link_to_every_outcome_in_its_session(self):
        self._seed_hooks()
        self.store.append(_llm("llm-late", "S", "claude-opus-5"))
        linked = self._edges_from("llm-late")
        self.assertLessEqual(
            linked, self._module.CORRELATION_MAX_LINKS,
            f"the reverse direction linked {linked} outcomes with a cap of "
            f"{self._module.CORRELATION_MAX_LINKS}",
        )
        self.assertGreater(linked, 0, "the cap must bound the work, not remove correlation")

    def test_the_bound_keeps_the_outcomes_nearest_the_event(self):
        # When the cap bites, the closest-in-time outcomes are the most
        # probative; ascending order discarded exactly those.
        stamps = self._seed_hooks()
        self.store.append(_llm("llm-late", "S", "claude-opus-5"))
        linked = [
            r["observed_at"] for r in self.store.connection.execute(
                """
                SELECT events.observed_at AS observed_at
                FROM attribution_edges
                JOIN events ON events.event_id = attribution_edges.parent_event_id
                WHERE attribution_edges.child_event_id = 'llm-late'
                  AND attribution_edges.relation = 'outcome_correlation'
                """
            )
        ]
        self.assertTrue(linked, "expected the reverse direction to link something")
        newest = set(stamps[-self._module.CORRELATION_MAX_LINKS:])
        self.assertTrue(
            set(linked) <= newest,
            f"the cap kept outcomes outside the newest {self._module.CORRELATION_MAX_LINKS}: {sorted(linked)}",
        )


if __name__ == "__main__":
    unittest.main()
