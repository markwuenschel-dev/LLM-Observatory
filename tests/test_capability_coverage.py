"""Skill / workflow / task-class coverage, and zero-signal grading.

The primary question this store exists to answer ranks by "models, agents,
skills, workflows, orchestration patterns". Measured on the live store, skill
appeared on 4 of 42,577 cost-bearing events, workflow_id and task_class on none
-- so a ranking of skills could never emerge, at any duration. Nothing said so:
no signal family covered those dimensions, and the verdict's other families all
looked healthy.

Worse, a family with zero signal graded PARTIAL, identical to one at 94%.
PARTIAL invites a reader to average it in as a low score, which is precisely
"comparing missing telemetry as though it were zero telemetry". Zero signal must
forbid the comparison instead.
"""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from observatory.contracts import NormalizedEvent
from observatory.observation import (
    COMPLETE,
    PARTIAL,
    SIGNAL_FAMILIES,
    UNKNOWN,
    _grade,
    client_observation,
)
from observatory.store import EventStore

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=timezone.utc)


def _event(event_id, *, skill=None, workflow=None, task_class=None, lane=None):
    stamp = NOW.isoformat()
    execution = {"session_id": "S"}
    if skill:
        execution["skill"] = skill
    if workflow:
        execution["workflow_id"] = workflow
    if task_class:
        execution["task_class"] = task_class
    if lane:
        execution["lane"] = lane
    return NormalizedEvent.from_mapping({
        "schema_version": "1.0",
        "event_id": event_id,
        "event_type": "model.operation",
        "observed_at": stamp,
        "received_at": stamp,
        "project": {"project_id": "repo_sha256:abc", "repository": "demo"},
        "execution": execution,
        "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
        "usage": {"cost": 1.0, "total_tokens": 100},
        "performance": {"latency_ms": 50},
    })


class ZeroSignalGradingTests(unittest.TestCase):
    def test_zero_signal_in_a_real_sample_is_unknown_not_partial(self):
        graded = _grade(0, 29062, supported=True)
        self.assertEqual(graded["coverage"], UNKNOWN)
        self.assertEqual(graded["ratio"], 0.0)
        self.assertEqual(graded["events_observed"], 29062)

    def test_a_thin_but_real_signal_is_still_partial(self):
        # The guard must fire only at exactly zero; one event IS evidence.
        self.assertEqual(_grade(1, 29062, supported=True)["coverage"], PARTIAL)

    def test_an_empty_sample_stays_unknown(self):
        self.assertEqual(_grade(0, 0, supported=True)["coverage"], UNKNOWN)

    def test_full_coverage_is_unaffected(self):
        self.assertEqual(_grade(100, 100, supported=True)["coverage"], COMPLETE)

    def test_the_evidence_travels_with_every_grade(self):
        # The grade alone must never be the whole story.
        for present, observed in ((0, 100), (1, 100), (100, 100)):
            graded = _grade(present, observed, supported=True)
            self.assertEqual(graded["events_with_signal"], present)
            self.assertEqual(graded["events_observed"], observed)


class CapabilityIdentityFamilyTests(unittest.TestCase):
    """The ranked dimensions need a coverage family of their own."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _sample(self):
        return self.store.observation_samples()["claude-code"]

    def test_the_family_is_declared(self):
        self.assertIn("capability_identity", SIGNAL_FAMILIES)

    def test_the_store_emits_it(self):
        self.store.append(_event("e1"))
        self.assertIn("capability_identity", self._sample())

    def test_telemetry_without_any_capability_dimension_reports_zero(self):
        # The live shape: cost and model present, skill/workflow/task absent.
        for index in range(10):
            self.store.append(_event(f"e{index}"))
        state = client_observation("claude-code", self._sample(), configured=True)
        family = state["signal_families"]["capability_identity"]
        self.assertEqual(family["events_with_signal"], 0)
        self.assertEqual(
            family["coverage"], UNKNOWN,
            "a dimension the ranking needs, with no signal at all, must not read as PARTIAL",
        )

    def test_any_of_the_four_dimensions_counts(self):
        for index, kwargs in enumerate((
            {"skill": "tdd"},
            {"workflow": "wf-1"},
            {"task_class": "refactor"},
            {"lane": "review"},
        )):
            self.store.append(_event(f"c{index}", **kwargs))
        self.assertEqual(self._sample()["capability_identity"], 4)

    def test_it_is_graded_against_the_same_denominator_as_the_others(self):
        self.store.append(_event("with", skill="tdd"))
        for index in range(3):
            self.store.append(_event(f"without{index}"))
        state = client_observation("claude-code", self._sample(), configured=True)
        family = state["signal_families"]["capability_identity"]
        self.assertEqual((family["events_with_signal"], family["events_observed"]), (1, 4))
        self.assertEqual(family["coverage"], PARTIAL)


if __name__ == "__main__":
    unittest.main()
