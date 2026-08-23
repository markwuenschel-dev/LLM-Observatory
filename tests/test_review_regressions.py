"""Regressions for defects found by adversarial review of this change set.

Each test corresponds to a bug that shipped with a green suite. Several were
invisible because every existing test exercised only the default-argument path.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from observatory.contracts import NormalizedEvent, ProjectIdentity
from observatory.hooks import build_hook_event
from observatory.maintenance import purge_events
from observatory.observation import DEGRADED, NOT_CONFIGURED, client_observation
from observatory.outcomes import make_outcome_event
from observatory.store import CORRELATION_MAX_LINKS, EventStore

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def _llm(event_id, session, *, cost=None, project="repo_sha256:abc", observed=None):
    stamp = (observed or NOW).isoformat()
    usage = {"total_tokens": 1000}
    if cost is not None:
        usage["cost"] = cost
    return NormalizedEvent.from_mapping(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "model.operation",
            "observed_at": stamp,
            "received_at": stamp,
            "project": {"project_id": project},
            "execution": {"session_id": session},
            "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
            "usage": usage,
        }
    )


class StoreCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)


class FilteredAnalyticsTests(StoreCase):
    """Both flagship routes raised a SQL syntax error on any filter, because
    every test called them with no filters."""

    def _seed(self):
        self.store.append(_llm("e1", "S", cost=1.0))
        self.store.append(
            make_outcome_event("tests", "passed", correlation_basis="session_id",
                               correlation_id="S", evidence_source="pytest")
        )

    def test_outcome_value_accepts_a_filter(self):
        self._seed()
        self.assertIsInstance(self.store.outcome_value({"provider": "anthropic"}), list)

    def test_engineering_value_accepts_a_filter(self):
        self._seed()
        report = self.store.engineering_value({"provider": "anthropic"})
        self.assertIn("ranked", report)

    def test_a_filter_actually_narrows(self):
        self._seed()
        self.assertEqual(self.store.outcome_value({"provider": "nonexistent"}), [])
        self.assertNotEqual(self.store.outcome_value({"provider": "anthropic"}), [])


class EveryDeclaredFilterTests(StoreCase):
    """Exercise every declared filter against every filtered method.

    Two separate bugs shipped with a green suite because the tests only ever
    passed default arguments, and a third shipped because the fix for the first
    was itself only tested with one filter key. A loop over the declared filter
    set cannot go stale when a key is added, which is the point.
    """

    RANGE_VALUES = {"start": "2026-08-01T00:00:00Z", "end": "2026-09-01T00:00:00Z"}

    def _methods(self):
        return {
            "outcome_value": self.store.outcome_value,
            "engineering_value": self.store.engineering_value,
            "comparison": self.store.comparison,
            "summary": self.store.summary,
            "list_events": self.store.list_events,
        }

    def _seed(self):
        self.store.append(_llm("e1", "S", cost=1.0))
        self.store.append(
            make_outcome_event("tests", "passed", correlation_basis="session_id",
                               correlation_id="S", evidence_source="pytest")
        )

    def test_no_declared_filter_raises_on_any_filtered_method(self):
        self._seed()
        keys = sorted(set(EventStore._FILTER_COLUMNS) | {"start", "end"})
        self.assertGreater(len(keys), 20, "filter set unexpectedly small")
        for name, method in self._methods().items():
            for key in keys:
                with self.subTest(method=name, filter=key):
                    value = self.RANGE_VALUES.get(key, "sentinel-value")
                    method({key: value})

    def test_a_sentinel_filter_narrows_to_nothing(self):
        # A filter that raises is one failure mode; a filter silently ignored is
        # the other, and it is the quieter of the two.
        self._seed()
        for key in ("provider", "model", "client", "outcome_kind", "outcome_status"):
            with self.subTest(filter=key):
                self.assertEqual(self.store.outcome_value({key: "no-such-value"}), [])

    def test_an_outcome_side_filter_selects_by_the_outcome(self):
        self._seed()
        self.assertNotEqual(self.store.outcome_value({"outcome_kind": "tests"}), [])
        self.assertEqual(self.store.outcome_value({"outcome_kind": "commit"}), [])

    def test_an_unknown_filter_is_still_rejected(self):
        with self.assertRaises(ValueError):
            self.store.outcome_value({"not-a-filter": "x"})


class NullOutcomeKeyTests(StoreCase):
    """An outcome may carry no kind or status; a USING join on NULL keys
    silently dropped that group and its spend from the analytics."""

    def test_an_outcome_with_no_status_is_not_dropped(self):
        # `outcome_events.status` is nullable and edges are created whenever
        # `kind` OR `status` is set, so kind-without-status is reachable. A USING
        # join on that NULL key silently dropped the group and its spend.
        self.store.append(_llm("llm1", "S", cost=7.0))
        self.store.append(
            NormalizedEvent.from_mapping({
                "schema_version": "1.0", "event_id": "kind-only-outcome",
                "event_type": "outcome.commit",
                "observed_at": NOW.isoformat(), "received_at": NOW.isoformat(),
                "project": {"project_id": "repo_sha256:abc"},
                "outcome": {"kind": "commit", "correlation_basis": "session_id",
                            "correlation_id": "S"},
            })
        )
        edges = self.store.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE relation = 'outcome_correlation'"
        ).fetchone()[0]
        self.assertGreater(edges, 0, "precondition: the outcome must be correlated")
        rows = self.store.outcome_value()
        self.assertTrue(rows, "a NULL-status outcome vanished from the analytics")
        self.assertAlmostEqual(sum(r["cost"] or 0 for r in rows), 7.0)
        self.assertEqual(rows[0]["outcome_status"], "unknown")


class EffortDoubleCountTests(StoreCase):
    """One operation linked to several outcomes was counted once per link, so
    cost was multiplied by the link count while coverage reported 1.0."""

    def test_cost_is_not_multiplied_by_the_number_of_linked_outcomes(self):
        self.store.append(_llm("solo", "S", cost=10.0))
        for index in range(3):
            self.store.append(
                make_outcome_event("tests", "passed", correlation_basis="session_id",
                                   correlation_id="S", evidence_source=f"pytest-{index}")
            )
        rows = self.store.outcome_value()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["cost"], 10.0)
        self.assertEqual(rows[0]["associated_events"], 1)


class CorrelationBoundTests(StoreCase):
    """Client hooks now declare a session basis, which routed every hook into an
    uncapped correlation query on the append path."""

    def test_an_explicit_basis_link_is_bounded(self):
        # Seeded ABOVE the cap: the previous version used 60 events against a cap
        # of 500, so it passed with the LIMIT deleted entirely.
        for index in range(CORRELATION_MAX_LINKS + 50):
            self.store.append(_llm(f"e{index:05d}", "S"))
        self.store.append(
            build_hook_event("claude-code", {"session_id": "S", "hook_event_name": "SessionEnd",
                                             "cwd": self._dir.name}, project_path=self._dir.name)
        )
        edges = self.store.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE relation = 'outcome_correlation'"
        ).fetchone()[0]
        self.assertEqual(edges, CORRELATION_MAX_LINKS * 2)

    def test_the_cap_keeps_the_work_closest_to_the_outcome(self):
        # Ascending order discarded the most probative associations -- the work
        # immediately before the outcome -- and kept the oldest instead.
        for index in range(CORRELATION_MAX_LINKS + 50):
            self.store.append(_llm(f"e{index:05d}", "S"))
        self.store.append(
            build_hook_event("claude-code", {"session_id": "S", "hook_event_name": "SessionEnd",
                                             "cwd": self._dir.name}, project_path=self._dir.name)
        )
        linked = {
            row[0]
            for row in self.store.connection.execute(
                "SELECT child_event_id FROM attribution_edges WHERE relation = 'outcome_correlation'"
                " UNION SELECT parent_event_id FROM attribution_edges WHERE relation = 'outcome_correlation'"
            )
        }
        newest = f"e{CORRELATION_MAX_LINKS + 49:05d}"
        oldest = "e00000"
        self.assertIn(newest, linked, "the work nearest the outcome was discarded")
        self.assertNotIn(oldest, linked, "the oldest work was kept over the nearest")


class BehaviouralCoverageTests(StoreCase):
    """Coverage was attached to cost/tokens/latency only, so a client that never
    reported retries ranked better than one that did."""

    def test_every_summed_measure_carries_coverage(self):
        self.store.append(_llm("e1", "S", cost=1.0))
        self.store.append(
            make_outcome_event("tests", "passed", correlation_basis="session_id",
                               correlation_id="S", evidence_source="pytest")
        )
        coverage = self.store.outcome_value()[0]["coverage"]
        for measure in ("cost", "tokens", "latency", "retries", "rework_loops",
                        "reassessments", "agent_failures"):
            self.assertIn(measure, coverage, f"{measure} is summed without coverage")


class PurgeScaleTests(StoreCase):
    """The edge delete binds two parameters per event, so an unchunked purge
    exceeded SQLite's variable limit above ~16k events."""

    def test_a_purge_larger_than_the_sqlite_variable_limit_succeeds(self):
        old = (NOW - timedelta(days=90)).isoformat()
        for index in range(17_000):
            self.store.append(_llm(f"old{index}", "S", observed=NOW - timedelta(days=90)))
        cutoff = (NOW - timedelta(days=30)).isoformat()
        result = purge_events(self.store, before=cutoff, confirm=True)
        self.assertEqual(result["affected_events"], 17_000)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)

    def test_a_skipped_compaction_still_checkpoints(self):
        # Without a checkpoint the freed pages stay in the WAL, which counts
        # toward the budget, so a purge could report the store as larger.
        self.store.append(_llm("old1", "S", observed=NOW - timedelta(days=90)))
        cutoff = (NOW - timedelta(days=30)).isoformat()
        result = purge_events(self.store, before=cutoff, confirm=True)
        self.assertEqual(result["compaction"]["status"], "skipped")
        self.assertTrue(result["compaction"].get("checkpointed"))


class TrailingGapTests(StoreCase):
    """An unwatched gap was only detected between two snapshots, so a recorder
    that dies leaves silence that reads as health."""

    def test_a_dead_recorder_is_reported_as_an_ongoing_unwatched_gap(self):
        base = NOW - timedelta(days=13)
        for hour in range(3):
            stamp = (base + timedelta(hours=hour)).isoformat()
            self.store.record_observation({
                "generated_at": stamp, "verdict": "OBSERVATION_CAPABLE",
                "observation_capable": True, "evidence": {}, "blockers": [], "clients": [],
            })
        gaps = self.store.observation_gaps(expected_interval_seconds=3600, now=NOW.isoformat())
        self.assertEqual(len(gaps["unwatched_intervals"]), 1)
        interval = gaps["unwatched_intervals"][0]
        self.assertTrue(interval.get("ongoing"))
        self.assertGreater(interval["seconds"], 12 * 86400)


class StaleBeforeConfiguredTests(unittest.TestCase):
    """A client that worked for months and stopped was reported as
    NOT_CONFIGURED whenever its manifest entry was missing."""

    def test_lifetime_is_checked_before_the_manifest(self):
        state = client_observation(
            "claude-code", None, configured=False, now=NOW,
            lifetime={"events": 50_000, "last_received": (NOW - timedelta(days=13)).isoformat()},
        )
        self.assertEqual(state["state"], DEGRADED)
        self.assertIn("delivered 50,000 events", " ".join(state["blockers"]))

    def test_a_client_that_never_delivered_is_still_not_configured(self):
        state = client_observation("claude-code", None, configured=False, now=NOW, lifetime={"events": 0})
        self.assertEqual(state["state"], NOT_CONFIGURED)


class CapabilityIdentifierTests(unittest.TestCase):
    """Skill/workflow names are read out of tool input, which is otherwise
    excluded, so the value must look like an identifier."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)

    def _skill(self, tool_input):
        event = build_hook_event(
            "claude-code",
            {"session_id": "S", "hook_event_name": "PostToolUse", "tool_name": "Skill",
             "tool_input": tool_input, "cwd": self._dir.name},
            project_path=self._dir.name,
        )
        return event.execution.skill

    def test_a_real_identifier_is_kept(self):
        self.assertEqual(self._skill({"skill": "code-review"}), "code-review")
        self.assertEqual(self._skill({"skill": "apps/web:deploy"}), "apps/web:deploy")

    def test_free_text_smuggled_through_name_is_refused(self):
        # `_label` turns spaces into underscores, so the guard must inspect the
        # raw value or a whole sentence lands in an indexed column.
        self.assertIsNone(self._skill({"name": "please review C:/secrets/keys.txt and summarise"}))

    def test_overlong_and_multiline_values_are_refused(self):
        self.assertIsNone(self._skill({"skill": "x" * 300}))
        self.assertIsNone(self._skill({"skill": "has\nnewline"}))


class StableOutcomeIdentityTests(unittest.TestCase):
    """Commit outcomes were stamped with HEAD at collection time, so every
    commit got a new event_id whenever anything landed."""

    LOG = ("\x01aaaa111\x1f2026-08-01T10:00:00+00:00\n1\t0\tf.py\n"
           "\x01bbbb222\x1f2026-08-02T10:00:00+00:00\n2\t0\tg.py\n")

    def _ids(self, head):
        import observatory.outcomes as outcomes

        completed = type("R", (), {"returncode": 0, "stdout": self.LOG, "stderr": ""})()
        identity = ProjectIdentity(project_id="repo_sha256:abc", repository="demo",
                                   branch="main", commit=head)
        with patch.object(outcomes.subprocess, "run", return_value=completed), \
             patch.object(outcomes, "resolve_project", return_value=identity):
            return [event.event_id for event in outcomes.git_commit_outcomes(".")]

    def test_identity_survives_a_head_move(self):
        self.assertEqual(self._ids("HEAD-OLD"), self._ids("HEAD-NEW"))

    def test_each_commit_carries_its_own_sha(self):
        import observatory.outcomes as outcomes

        completed = type("R", (), {"returncode": 0, "stdout": self.LOG, "stderr": ""})()
        identity = ProjectIdentity(project_id="repo_sha256:abc", commit="HEAD-SHA")
        with patch.object(outcomes.subprocess, "run", return_value=completed), \
             patch.object(outcomes, "resolve_project", return_value=identity):
            events = outcomes.git_commit_outcomes(".")
        self.assertEqual([e.project.commit for e in events], ["aaaa111", "bbbb222"])


class RetentionHonestyTests(StoreCase):
    """Retention that frees nothing must not report success: the operator is
    told the budget was acted on while intake stays refused."""

    def test_a_purge_that_frees_nothing_is_reported_as_ineffective(self):
        from observatory.maintenance import enforce_normalized_retention

        stamp = NOW.isoformat()
        for index in range(200):
            self.store.append(_llm(f"e{index}", "S", observed=NOW))
        report = enforce_normalized_retention(self.store, max_bytes=200_000)
        self.assertEqual(report["outcome"], "ineffective")
        self.assertEqual(report["deleted_events"], 0)
        self.assertIn("cannot free space", report["blocker"])


class TrailingGapWindowTests(StoreCase):
    """Both live callers pass a 7-day window, so a recorder dead longer than the
    window emptied the history and the blackout check never ran."""

    def test_a_recorder_dead_longer_than_the_window_is_still_reported(self):
        stamp = (NOW - timedelta(days=210)).isoformat()
        self.store.record_observation({
            "generated_at": stamp, "verdict": "OBSERVATION_CAPABLE",
            "observation_capable": True, "evidence": {}, "blockers": [], "clients": [],
        })
        gaps = self.store.observation_gaps(
            since=(NOW - timedelta(days=7)).isoformat(),
            expected_interval_seconds=3600,
            now=NOW.isoformat(),
        )
        self.assertEqual(len(gaps["unwatched_intervals"]), 1)
        self.assertTrue(gaps["unwatched_intervals"][0].get("ongoing"))
        self.assertGreater(gaps["unwatched_intervals"][0]["seconds"], 200 * 86400)


if __name__ == "__main__":
    unittest.main()
