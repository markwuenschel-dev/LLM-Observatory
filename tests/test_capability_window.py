"""Attributing a skill to the work it was active for.

No client stamps the skill on the operation itself. It is invoked once and then
governs what follows, so it arrives on a separate hook event -- which is itself
an outcome, and the ranking's effort side excludes outcomes. Left as a plain
column, skill could never rank however much telemetry accumulated: measured live
at 0 of 4,176 associated events.

The resolution is the window the capability was active for: the most recent
invocation in the SAME session at or before the operation. That is a bounded
temporal association inside a shared session -- the evidence standard this store
already applies to commits and CI.

What it must NOT become is a session-wide join, which would charge a skill
invoked once near the end of a session for the entire session's cost. These
tests pin the boundaries, because the difference between the two is the
difference between an association and an invented finding.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observatory.contracts import NormalizedEvent
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore

BASE = datetime(2026, 8, 23, 12, 0, 0, tzinfo=timezone.utc)


def _at(minutes):
    return (BASE + timedelta(minutes=minutes)).isoformat()


def _operation(event_id, session, *, minutes, cost=1.0):
    stamp = _at(minutes)
    return NormalizedEvent.from_mapping({
        "schema_version": "1.0",
        "event_id": event_id,
        "event_type": "model.operation",
        "observed_at": stamp,
        "received_at": stamp,
        "project": {"project_id": "repo_sha256:abc"},
        "execution": {"session_id": session},
        "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
        "usage": {"cost": cost, "total_tokens": 100},
        "performance": {"latency_ms": 10},
    })


def _invocation(event_id, session, skill, *, minutes):
    """A skill invocation as the hook records it: an outcome event."""
    stamp = _at(minutes)
    return NormalizedEvent.from_mapping({
        "schema_version": "1.0",
        "event_id": event_id,
        "event_type": "tool.operation",
        "observed_at": stamp,
        "received_at": stamp,
        "project": {"project_id": "repo_sha256:abc"},
        "execution": {"session_id": session, "skill": skill},
        "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
        "outcome": {"kind": "client-hook", "status": "post_tool_use"},
    })


class CapabilityWindowTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _record_outcome(self, session, *, minutes, status="passed"):
        self.store.append(make_outcome_event(
            "tests", status, correlation_basis="session_id", correlation_id=session,
            evidence_source="pytest", observed_at=_at(minutes)))

    def _skills(self):
        report = self.store.engineering_value(dimensions=("skill",), min_outcomes=1)
        return {r["skill"]: r for r in report["ranked"] + report["insufficient_evidence"]}

    def test_work_after_an_invocation_is_attributed_to_it(self):
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        self.store.append(_operation("e1", "S", minutes=5))
        self._record_outcome("S", minutes=10)
        self.assertIn("tdd", self._skills())

    def test_work_before_any_invocation_is_not_attributed(self):
        # The decisive boundary. A session-wide join would claim this.
        self.store.append(_operation("early", "S", minutes=0))
        self.store.append(_invocation("h1", "S", "tdd", minutes=10))
        self._record_outcome("S", minutes=20)
        skills = self._skills()
        self.assertIn("unknown", skills, f"work before the invocation was claimed by a skill: {list(skills)}")
        self.assertEqual(skills["unknown"]["associated_events"], 1)

    def test_a_later_invocation_supersedes_an_earlier_one(self):
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        self.store.append(_operation("e1", "S", minutes=5))
        self.store.append(_invocation("h2", "S", "review", minutes=10))
        self.store.append(_operation("e2", "S", minutes=15))
        self._record_outcome("S", minutes=20)
        skills = self._skills()
        self.assertEqual(skills["tdd"]["associated_events"], 1)
        self.assertEqual(skills["review"]["associated_events"], 1)

    def test_a_skill_never_reaches_another_session(self):
        self.store.append(_invocation("h1", "S1", "tdd", minutes=0))
        self.store.append(_operation("e1", "S2", minutes=5))
        self._record_outcome("S2", minutes=10)
        skills = self._skills()
        self.assertNotIn("tdd", skills, "a skill leaked across sessions")
        self.assertIn("unknown", skills)

    def test_an_operation_without_a_session_is_never_attributed(self):
        # `session_id IS NOT NULL` guards this. Correlate the unsessioned
        # operation through a DIFFERENT basis so it genuinely reaches the
        # ranking -- otherwise the test passes on an empty result and proves
        # nothing about the guard.
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        loose = _operation("loose", "S", minutes=5)
        payload = dict(loose.to_mapping())
        payload["execution"] = {**payload.get("execution", {}), "session_id": None}
        payload["event_id"] = "loose2"
        self.store.append(NormalizedEvent.from_mapping(payload))
        self.store.append(make_outcome_event(
            "tests", "passed", correlation_basis="event_id", correlation_id="loose2",
            evidence_source="pytest", observed_at=_at(10)))
        skills = self._skills()
        self.assertIn("unknown", skills, f"the unsessioned operation was not ranked at all: {list(skills)}")
        self.assertNotIn("tdd", skills, "a skill was applied across a NULL session")

    def test_the_invocation_itself_is_not_counted_as_effort(self):
        # It is an outcome event; charging its cost to the skill would double
        # count the hook as work.
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        self.store.append(_operation("e1", "S", minutes=5))
        self._record_outcome("S", minutes=10)
        self.assertEqual(self._skills()["tdd"]["associated_events"], 1)

    def test_coverage_agrees_with_what_the_ranking_resolved(self):
        # Reporting raw-column presence while the ranking resolves over a window
        # would have the report call a dimension unrankable as it ranks it.
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        self.store.append(_operation("e1", "S", minutes=5))
        self._record_outcome("S", minutes=10)
        report = self.store.engineering_value(min_outcomes=1)
        coverage = report["dimension_coverage"]["skill"]
        self.assertTrue(coverage["rankable"])
        self.assertEqual(coverage["events_with_dimension"], 1)

    def test_no_invocation_anywhere_still_reports_unrankable(self):
        self.store.append(_operation("e1", "S", minutes=5))
        self._record_outcome("S", minutes=10)
        coverage = self.store.engineering_value(min_outcomes=1)["dimension_coverage"]["skill"]
        self.assertFalse(coverage["rankable"])
        self.assertIn("cannot be ranked", coverage["note"])

    def test_unwindowed_dimensions_are_untouched(self):
        self.store.append(_invocation("h1", "S", "tdd", minutes=0))
        self.store.append(_operation("e1", "S", minutes=5))
        self._record_outcome("S", minutes=10)
        coverage = self.store.engineering_value(min_outcomes=1)["dimension_coverage"]
        self.assertEqual(coverage["model"]["events_with_dimension"], 1)
        self.assertTrue(coverage["client"]["rankable"])


if __name__ == "__main__":
    unittest.main()
