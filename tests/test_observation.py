"""Tests for the authoritative observation verdict.

The failure this guards against is a deployment that reports healthy while
persisting nothing. Every assertion here is about telling the truth in that
situation rather than about happy-path reporting.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observatory.observation import (
    COMPLETE,
    CONFIGURED_UNVERIFIED,
    DEGRADED,
    NOT_CONFIGURED,
    OBSERVING,
    PARTIAL,
    SIGNAL_FAMILIES,
    UNKNOWN,
    UNSUPPORTED,
    client_observation,
    observation_report,
)

NOW = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)


def _sample(events, *, age_hours=0.0, attributed=None, **families):
    stamp = (NOW - timedelta(hours=age_hours)).isoformat()
    sample = {
        "events": events,
        "last_observed": stamp,
        "last_received": stamp,
        "project_attribution": events if attributed is None else attributed,
    }
    for family in SIGNAL_FAMILIES:
        sample.setdefault(family, families.get(family, events))
    sample.update({k: v for k, v in families.items() if k in SIGNAL_FAMILIES})
    return sample


class ClientStateTests(unittest.TestCase):
    def test_configured_client_with_fresh_attributed_telemetry_is_observing(self):
        state = client_observation("claude-code", _sample(100), configured=True, now=NOW)
        self.assertEqual(state["state"], OBSERVING)
        self.assertEqual(state["blockers"], [])

    def test_stale_telemetry_is_degraded_with_the_age_named(self):
        state = client_observation("claude-code", _sample(100, age_hours=48), configured=True, now=NOW)
        self.assertEqual(state["state"], DEGRADED)
        self.assertTrue(any("48.0h old" in b for b in state["blockers"]), state["blockers"])

    def test_unattributed_telemetry_is_degraded_even_when_it_is_flowing(self):
        # The exact live condition: events arriving, none resolvable to a repo.
        state = client_observation("claude-code", _sample(1000, attributed=0), configured=True, now=NOW)
        self.assertEqual(state["state"], DEGRADED)
        self.assertTrue(any("resolve to a project" in b for b in state["blockers"]), state["blockers"])
        self.assertEqual(state["project_attribution"]["ratio"], 0.0)

    def test_configured_but_silent_client_is_distinguished_from_unconfigured(self):
        configured = client_observation("kimi", None, configured=True, now=NOW)
        unconfigured = client_observation("kimi", None, configured=False, now=NOW)
        self.assertEqual(configured["state"], CONFIGURED_UNVERIFIED)
        self.assertEqual(unconfigured["state"], NOT_CONFIGURED)
        self.assertTrue(any("configure kimi" in b for b in unconfigured["blockers"]))

    def test_caller_owned_clients_are_unsupported_not_misconfigured(self):
        # Reporting these as NOT_CONFIGURED would imply a fix that does not exist.
        state = client_observation("direct-openai-api", None, configured=False, now=NOW)
        self.assertEqual(state["state"], UNSUPPORTED)
        self.assertTrue(any("caller-owned" in b for b in state["blockers"]))

    def test_telemetry_without_an_ownership_record_is_called_out(self):
        state = client_observation("claude-code", _sample(100), configured=False, now=NOW)
        self.assertTrue(any("ownership manifest" in b for b in state["blockers"]), state["blockers"])


class StaleClientTests(unittest.TestCase):
    """A client that delivered and stopped is a different problem from one that
    never worked, and the two need opposite responses."""

    def test_a_client_that_delivered_and_went_quiet_is_degraded_not_unverified(self):
        state = client_observation(
            "grok", None, configured=True, now=NOW,
            lifetime={"events": 128, "last_received": (NOW - timedelta(days=9)).isoformat()},
        )
        self.assertEqual(state["state"], DEGRADED)
        blocker = " ".join(state["blockers"])
        self.assertIn("delivered 128 events", blocker)
        self.assertIn("hook may no longer be firing", blocker)
        self.assertNotIn("has ever been persisted", blocker)

    def test_a_client_that_never_delivered_stays_unverified(self):
        state = client_observation("kimi", None, configured=True, now=NOW, lifetime={"events": 0})
        self.assertEqual(state["state"], CONFIGURED_UNVERIFIED)
        blocker = " ".join(state["blockers"])
        self.assertIn("has ever been persisted", blocker)
        self.assertNotIn("delivered", blocker)

    def test_the_stale_report_carries_when_it_was_last_seen(self):
        state = client_observation(
            "grok", None, configured=True, now=NOW,
            lifetime={"events": 5, "last_received": (NOW - timedelta(days=3)).isoformat()},
        )
        self.assertIsNotNone(state["last_event_at"])
        self.assertAlmostEqual(state["last_event_age_seconds"] / 86400, 3.0, places=1)


class SessionCoverageBlockerTests(unittest.TestCase):
    """Presence is not coverage: a session on 2 of 345,000 events is noise."""

    def _state(self, events, sessions):
        sample = _sample(events, attributed=0)
        sample["session_identity"] = sessions
        return client_observation("codex", sample, configured=True, recent=sample, now=NOW)

    def test_a_negligible_session_rate_is_reported_as_unbindable(self):
        blockers = " ".join(self._state(345_000, 2)["blockers"])
        self.assertIn("emits no session", blockers)
        self.assertIn("only 2 of 345,000", blockers)
        self.assertNotIn("bind-sessions", blockers)

    def test_a_real_session_rate_points_at_binding(self):
        blockers = " ".join(self._state(1000, 900)["blockers"])
        self.assertIn("bind-sessions", blockers)
        self.assertNotIn("emits no session", blockers)


class CoverageGradingTests(unittest.TestCase):
    def test_a_family_present_on_nearly_every_event_is_complete(self):
        state = client_observation("claude-code", _sample(100, cost=100), configured=True, now=NOW)
        self.assertEqual(state["signal_families"]["cost"]["coverage"], COMPLETE)

    def test_a_partly_present_family_is_partial_and_reports_its_ratio(self):
        state = client_observation("claude-code", _sample(100, cost=30), configured=True, now=NOW)
        cost = state["signal_families"]["cost"]
        self.assertEqual(cost["coverage"], PARTIAL)
        self.assertAlmostEqual(cost["ratio"], 0.30)

    def test_no_sample_is_unknown_rather_than_zero(self):
        # Never compare a silent client as though it reported zeroes.
        state = client_observation("kimi", None, configured=True, now=NOW)
        for family in SIGNAL_FAMILIES:
            graded = state["signal_families"][family]
            self.assertEqual(graded["coverage"], UNKNOWN)
            self.assertIsNone(graded["ratio"])
            self.assertEqual(graded["events_observed"], 0)

    def test_every_grade_carries_its_numerator_and_denominator(self):
        state = client_observation("claude-code", _sample(80, cost=20), configured=True, now=NOW)
        cost = state["signal_families"]["cost"]
        self.assertEqual((cost["events_with_signal"], cost["events_observed"]), (20, 80))


class DeploymentVerdictTests(unittest.TestCase):
    def test_healthy_deployment_is_observation_capable(self):
        report = observation_report(
            {"claude-code": _sample(500)},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.2},
            now=NOW,
        )
        self.assertTrue(report["observation_capable"])
        self.assertEqual(report["verdict"], "OBSERVATION_CAPABLE")
        self.assertEqual(report["blockers"], [])

    def test_healthy_containers_with_an_empty_store_are_not_capable(self):
        # The thirteen-day failure: everything up, nothing persisted.
        report = observation_report(
            {},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.0},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertTrue(any("has ever delivered persisted telemetry" in b for b in report["blockers"]), report["blockers"])

    def test_an_exhausted_store_blocks_the_verdict(self):
        report = observation_report(
            {"claude-code": _sample(500)},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": True, "ratio": 1.0},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertTrue(any("byte budget" in b for b in report["blockers"]))

    def test_an_unreachable_store_blocks_the_verdict(self):
        report = observation_report(
            {"claude-code": _sample(500)},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": False},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertTrue(any("not reachable" in b for b in report["blockers"]))

    def test_stale_telemetry_across_every_client_blocks_the_verdict(self):
        report = observation_report(
            {"claude-code": _sample(500, age_hours=72)},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.2},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertTrue(any("looks alive but is not observing" in b for b in report["blockers"]))

    def test_one_unidentified_record_cannot_declare_the_deployment_capable(self):
        """A single event with no client identity must not flip the verdict.

        Outcome records and stray telemetry group under the pseudo-client
        `unknown`. Counting that as an observing client would report a healthy
        deployment while every recognized client was degraded.
        """

        report = observation_report(
            {
                "unknown": _sample(1),
                "claude-code": _sample(5000, attributed=0),
            },
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.3},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertEqual(report["evidence"]["clients_observing"], 0)
        self.assertEqual(report["evidence"]["unidentified_client_events"], 1)

    def test_unidentified_telemetry_alone_is_reported_as_such(self):
        report = observation_report(
            {"unknown": _sample(42)},
            configured_clients={},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.1},
            now=NOW,
        )
        self.assertFalse(report["observation_capable"])
        self.assertTrue(
            any("without a recognized client identity" in b for b in report["blockers"]),
            report["blockers"],
        )

    def test_a_client_reported_under_a_telemetry_alias_is_not_listed_twice(self):
        # codex telemetry arrives as `codex-app-server`; one client, one row.
        report = observation_report(
            {"codex-app-server": _sample(400)},
            configured_clients={"codex": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.2},
            now=NOW,
        )
        rows = [c for c in report["clients"] if c["events_observed"] > 0]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["client"], "codex")
        self.assertEqual(rows[0]["state"], OBSERVING)
        self.assertEqual(rows[0]["reported_as"], ["codex-app-server"])

    def test_every_client_appears_exactly_once(self):
        report = observation_report(
            {"codex-app-server": _sample(10), "claude-code": _sample(10)},
            configured_clients={"codex": {"applied": True}, "claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.1},
            now=NOW,
        )
        names = [c["client"] for c in report["clients"]]
        self.assertEqual(len(names), len(set(names)), names)


class LongitudinalReliabilityTests(unittest.TestCase):
    """A verdict nobody records cannot reveal how long a blackout lasted."""

    def setUp(self):
        from observatory.store import EventStore

        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _snapshot(self, minutes, capable, blockers=()):
        stamp = (NOW + timedelta(minutes=minutes)).isoformat()
        return self.store.record_observation(
            {
                "generated_at": stamp,
                "verdict": "OBSERVATION_CAPABLE" if capable else "NOT_OBSERVATION_CAPABLE",
                "observation_capable": capable,
                "evidence": {"clients_observing": 1 if capable else 0, "clients_degraded": 0 if capable else 1,
                             "events_in_window": 100, "newest_event_age_seconds": 1.0, "store_capacity_ratio": 0.3},
                "blockers": list(blockers),
                "clients": [{"client": "claude-code", "state": OBSERVING if capable else DEGRADED,
                             "events_observed": 100, "last_event_at": stamp, "last_event_age_seconds": 1.0,
                             "project_attribution": {"ratio": 1.0 if capable else 0.0}, "blockers": list(blockers)}],
            }
        )

    def test_snapshots_are_recorded_and_returned_newest_first(self):
        for minute in (0, 5, 10):
            self._snapshot(minute, True)
        history = self.store.observation_history()
        self.assertEqual(len(history), 3)
        self.assertGreater(history[0]["recorded_at"], history[-1]["recorded_at"])

    def test_a_recorded_blackout_is_reported_with_its_duration(self):
        self._snapshot(0, True)
        for minute in (5, 10, 15):
            self._snapshot(minute, False, blockers=["store is at its byte budget"])
        self._snapshot(20, True)
        gaps = self.store.observation_gaps(expected_interval_seconds=600)
        self.assertEqual(len(gaps["not_capable_intervals"]), 1)
        interval = gaps["not_capable_intervals"][0]
        self.assertEqual(interval["snapshots"], 3)
        self.assertAlmostEqual(interval["seconds"], 600.0, places=1)
        self.assertIn("store is at its byte budget", interval["blockers"])

    def test_an_ongoing_blackout_is_marked_ongoing(self):
        self._snapshot(0, True)
        self._snapshot(5, False)
        gaps = self.store.observation_gaps(expected_interval_seconds=600)
        self.assertTrue(gaps["not_capable_intervals"][0].get("ongoing"))

    def test_a_stretch_with_no_snapshot_is_reported_as_unwatched(self):
        # The thirteen-day failure was this kind: not a recorded degradation,
        # but nobody looking. Silence must never read as "no problems".
        self._snapshot(0, True)
        self._snapshot(60 * 24, True)
        # Pin the clock to the last snapshot.  observation_gaps also reports a
        # trailing gap to *now* by design, so leaving that to the wall clock
        # made this assertion start counting two intervals an hour after the
        # anchor date passed.
        gaps = self.store.observation_gaps(
            expected_interval_seconds=3600,
            now=(NOW + timedelta(minutes=60 * 24)).isoformat(),
        )
        self.assertEqual(len(gaps["unwatched_intervals"]), 1)
        self.assertAlmostEqual(gaps["unwatched_intervals"][0]["seconds"], 86400.0, places=1)
        self.assertEqual(gaps["not_capable_intervals"], [])

    def test_snapshots_cannot_be_rewritten(self):
        import sqlite3

        snapshot_id = self._snapshot(0, False, blockers=["telemetry stopped"])
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.connection:
                self.store.connection.execute(
                    "UPDATE observation_snapshots SET verdict = 'OBSERVATION_CAPABLE' WHERE snapshot_id = ?",
                    (snapshot_id,),
                )


class CoverageDriftTests(unittest.TestCase):
    """Provider schema drift is silent: a field just stops arriving."""

    def setUp(self):
        from observatory.store import EventStore

        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _snapshot(self, minutes, families, client="claude-code"):
        stamp = (NOW + timedelta(minutes=minutes)).isoformat()
        self.store.record_observation(
            {
                "generated_at": stamp,
                "verdict": "OBSERVATION_CAPABLE",
                "observation_capable": True,
                "evidence": {},
                "blockers": [],
                "clients": [
                    {
                        "client": client,
                        "state": OBSERVING,
                        "events_observed": 100,
                        "last_event_at": stamp,
                        "last_event_age_seconds": 1.0,
                        "project_attribution": {"ratio": 1.0},
                        "signal_families": {
                            name: {"coverage": COMPLETE, "ratio": ratio}
                            for name, ratio in families.items()
                        },
                        "blockers": [],
                    }
                ],
            }
        )

    def test_a_field_that_stops_arriving_is_reported(self):
        for minute in (0, 5, 10, 15):
            self._snapshot(minute, {"cost": 0.95, "token_usage": 0.9})
        self._snapshot(20, {"cost": 0.0, "token_usage": 0.9})
        drift = self.store.coverage_drift()
        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["signal_family"], "cost")
        self.assertAlmostEqual(drift[0]["baseline_ratio"], 0.95)
        self.assertEqual(drift[0]["current_ratio"], 0.0)
        self.assertIn("changed telemetry schema", drift[0]["note"])

    def test_stable_coverage_reports_nothing(self):
        for minute in (0, 5, 10, 15, 20):
            self._snapshot(minute, {"cost": 0.9, "token_usage": 0.9})
        self.assertEqual(self.store.coverage_drift(), [])

    def test_a_small_dip_is_not_a_finding(self):
        for minute in (0, 5, 10, 15):
            self._snapshot(minute, {"cost": 0.90})
        self._snapshot(20, {"cost": 0.80})
        self.assertEqual(self.store.coverage_drift(), [])

    def test_one_bad_sample_cannot_hide_a_real_drop(self):
        # The baseline is a median, so a single outlier does not move it.
        for minute, ratio in ((0, 0.95), (5, 0.10), (10, 0.95), (15, 0.95)):
            self._snapshot(minute, {"cost": ratio})
        self._snapshot(20, {"cost": 0.0})
        drift = self.store.coverage_drift()
        self.assertEqual(len(drift), 1)
        self.assertAlmostEqual(drift[0]["baseline_ratio"], 0.95)

    def test_a_single_snapshot_yields_no_finding(self):
        self._snapshot(0, {"cost": 0.9})
        self.assertEqual(self.store.coverage_drift(), [])


class SnapshotIdentityTests(unittest.TestCase):
    """A snapshot id derived only from the timestamp collided, and the
    INSERT OR IGNORE then silently discarded the second verdict -- including a
    recorded degradation, which is what the append-only trigger exists to stop."""

    def setUp(self):
        from observatory.store import EventStore

        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _report(self, verdict, capable, blockers=()):
        return {
            "generated_at": NOW.isoformat(), "verdict": verdict,
            "observation_capable": capable, "evidence": {},
            "blockers": list(blockers), "clients": [],
        }

    def test_two_verdicts_at_the_same_instant_are_both_recorded(self):
        first = self.store.record_observation(self._report("OBSERVATION_CAPABLE", True))
        second = self.store.record_observation(
            self._report("NOT_OBSERVATION_CAPABLE", False, ["store exhausted"])
        )
        self.assertNotEqual(first, second)
        stored = sorted(
            row[0] for row in self.store.connection.execute("SELECT verdict FROM observation_snapshots")
        )
        self.assertEqual(stored, ["NOT_OBSERVATION_CAPABLE", "OBSERVATION_CAPABLE"])

    def test_recording_the_same_observation_twice_is_idempotent(self):
        first = self.store.record_observation(self._report("OBSERVATION_CAPABLE", True))
        second = self.store.record_observation(self._report("OBSERVATION_CAPABLE", True))
        self.assertEqual(first, second)
        count = self.store.connection.execute("SELECT COUNT(*) FROM observation_snapshots").fetchone()[0]
        self.assertEqual(count, 1)

    def test_a_snapshot_that_was_not_stored_is_reported_rather_than_assumed(self):
        # An explicit id that is already taken must not be reported as recorded.
        self.store.record_observation(self._report("OBSERVATION_CAPABLE", True), snapshot_id="fixed")
        with self.assertRaises(RuntimeError):
            self.store.connection.execute("DELETE FROM observation_snapshots WHERE 0")
            self.store.record_observation(self._report("X", False), snapshot_id="fixed")


class ThresholdBoundaryTests(unittest.TestCase):
    """Every threshold was exercised only at its extremes, so none was pinned.

    `COMPLETE_RATIO`, `MIN_ATTRIBUTION_RATIO` and `MIN_SESSION_RATIO` could each
    be changed to any value without failing a test.
    """

    def _families(self, present, observed):
        sample = _sample(observed)
        for family in SIGNAL_FAMILIES:
            sample[family] = present
        return sample

    def test_complete_ratio_boundary_is_pinned(self):
        from observatory.observation import COMPLETE_RATIO

        self.assertAlmostEqual(COMPLETE_RATIO, 0.95)
        just_over = client_observation("claude-code", self._families(95, 100), configured=True, now=NOW)
        just_under = client_observation("claude-code", self._families(94, 100), configured=True, now=NOW)
        self.assertEqual(just_over["signal_families"]["cost"]["coverage"], COMPLETE)
        self.assertEqual(just_under["signal_families"]["cost"]["coverage"], PARTIAL)

    def test_attribution_ratio_boundary_is_pinned(self):
        from observatory.observation import MIN_ATTRIBUTION_RATIO

        self.assertAlmostEqual(MIN_ATTRIBUTION_RATIO, 0.5)
        over = client_observation("claude-code", _sample(100, attributed=51), configured=True, now=NOW)
        under = client_observation("claude-code", _sample(100, attributed=49), configured=True, now=NOW)
        self.assertEqual(over["state"], OBSERVING)
        self.assertEqual(under["state"], DEGRADED)

    def test_session_ratio_boundary_selects_the_right_blocker(self):
        from observatory.observation import MIN_SESSION_RATIO

        self.assertAlmostEqual(MIN_SESSION_RATIO, 0.1)
        def blockers(sessions):
            sample = _sample(100, attributed=0)
            sample["session_identity"] = sessions
            return " ".join(
                client_observation("codex", sample, configured=True, recent=sample, now=NOW)["blockers"]
            )
        self.assertIn("emits no session", blockers(9))
        self.assertIn("bind-sessions", blockers(11))

    def test_freshness_budget_is_honoured_when_passed_explicitly(self):
        # The parameter was never passed a non-default value anywhere.
        fresh = client_observation("claude-code", _sample(100, age_hours=1.5), configured=True,
                                   now=NOW, freshness_seconds=7200)
        stale = client_observation("claude-code", _sample(100, age_hours=2.5), configured=True,
                                   now=NOW, freshness_seconds=7200)
        self.assertEqual(fresh["state"], OBSERVING)
        self.assertEqual(stale["state"], DEGRADED)


class DriftSampleFloorTests(unittest.TestCase):
    """The drift floor had no test: every fixture used events_observed=100."""

    def setUp(self):
        from observatory.store import EventStore

        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _snapshot(self, minutes, ratio, events_observed):
        stamp = (NOW + timedelta(minutes=minutes)).isoformat()
        self.store.record_observation({
            "generated_at": stamp, "verdict": "OBSERVATION_CAPABLE",
            "observation_capable": True, "evidence": {}, "blockers": [],
            "clients": [{
                "client": "claude-code", "state": OBSERVING,
                "events_observed": events_observed, "last_event_at": stamp,
                "last_event_age_seconds": 1.0, "project_attribution": {"ratio": 1.0},
                "signal_families": {"cost": {"coverage": COMPLETE, "ratio": ratio}},
                "blockers": [],
            }],
        })

    def test_a_thin_sample_cannot_manufacture_drift(self):
        from observatory.store import DRIFT_MIN_SAMPLE_EVENTS

        # One quiet snapshot grades 1.0 at 1-of-1; without a floor the next
        # honest sample reads as a collapse.
        for minute in range(0, 20, 5):
            self._snapshot(minute, 1.0, DRIFT_MIN_SAMPLE_EVENTS - 1)
        self._snapshot(25, 0.10, DRIFT_MIN_SAMPLE_EVENTS - 1)
        self.assertEqual(self.store.coverage_drift(), [])

    def test_a_real_sample_still_reports_drift(self):
        from observatory.store import DRIFT_MIN_SAMPLE_EVENTS

        for minute in range(0, 20, 5):
            self._snapshot(minute, 0.95, DRIFT_MIN_SAMPLE_EVENTS + 1)
        self._snapshot(25, 0.10, DRIFT_MIN_SAMPLE_EVENTS + 1)
        drift = self.store.coverage_drift()
        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["signal_family"], "cost")

def _skewed_sample(events, *, observed_age_hours, received_skew_seconds, attributed=None):
    """A sample whose newest `received_at` sits in the future.

    `received_at` is payload-supplied -- `contracts.py:391` resolves it as
    `value.get("received_at", received_at or utc_now())`, so the payload wins
    over the server stamp -- and `store.py:527` aggregates it with
    `MAX(received_at)`. One skewed row is therefore elected newest for a whole
    client, which is exactly how a future stamp reaches this module.
    """

    sample = _sample(events, age_hours=observed_age_hours, attributed=attributed)
    sample["last_received"] = (NOW + timedelta(seconds=received_skew_seconds)).isoformat()
    return sample


class UnusableTimestampTests(unittest.TestCase):
    """The same skipped check, reached through an absent stamp instead.

    Fixing the future-stamp route left the sibling case live: a client with
    events whose stamps are missing or unparseable also gets `age is None`, so
    `age is not None and age > budget` skips the staleness check and the client
    is reported OBSERVING with no blocker at all.
    """

    def _client(self, sample):
        return client_observation("claude-code", sample, configured=True, now=NOW)

    def _sample(self, **overrides):
        sample = {"events": 5000, "last_observed": None, "last_received": None}
        for family in SIGNAL_FAMILIES:
            sample.setdefault(family, 5000)
        sample.update(overrides)
        return sample

    def test_events_with_no_timestamp_are_not_reported_as_observing(self):
        state = self._client(self._sample())
        self.assertEqual(state["state"], DEGRADED)
        self.assertTrue(
            any("usable timestamp" in b for b in state["blockers"]),
            f"no blocker names the missing timestamp: {state['blockers']}",
        )

    def test_an_unparseable_timestamp_is_not_reported_as_observing(self):
        state = self._client(self._sample(last_received="not-a-timestamp", last_observed="also-bad"))
        self.assertEqual(state["state"], DEGRADED)
        self.assertTrue(any("usable timestamp" in b for b in state["blockers"]))

    def test_a_usable_stamp_on_either_field_is_still_accepted(self):
        # The guard must fire only when nothing usable exists, or it would
        # degrade every client whose `received_at` happens to be absent.
        state = self._client(self._sample(last_received=None, last_observed=NOW.isoformat()))
        self.assertEqual(state["state"], OBSERVING)
        self.assertFalse(any("usable timestamp" in b for b in state["blockers"]))

    def test_a_client_with_no_events_is_unaffected(self):
        # Absent stamps are the normal, correct state for a client that has
        # never delivered; that case is already reported by its own branch.
        state = self._client(self._sample(events=0, **{f: 0 for f in SIGNAL_FAMILIES}))
        self.assertNotEqual(state["state"], OBSERVING)
        self.assertFalse(any("usable timestamp" in b for b in state["blockers"]))


class FutureTimestampTests(unittest.TestCase):
    """A timestamp from the future is not evidence of freshness.

    Clamping it to age 0 elected one skewed row as the freshest telemetry in the
    whole deployment. Replacing the clamp with `None` was the same false green by
    another route: the staleness check reads `age is not None and age > budget`,
    so a `None` age SKIPS it, and the global `min(... if ... is not None)` DROPS
    that client from the deployment verdict entirely. An unusable stamp must
    become a stated blocker, never a skipped check.
    """

    def _report(self, sample):
        return observation_report(
            {"claude-code": sample},
            configured_clients={"claude": {"applied": True}},
            store_health={"reachable": True, "exhausted": False, "ratio": 0.3},
            now=NOW,
        )

    def test_a_future_timestamp_alone_cannot_report_a_client_as_observing(self):
        sample = _skewed_sample(5000, observed_age_hours=0.0, received_skew_seconds=3600)
        sample["last_observed"] = (NOW + timedelta(hours=1)).isoformat()
        state = client_observation("claude-code", sample, configured=True, now=NOW)
        self.assertNotEqual(state["state"], OBSERVING)
        self.assertEqual(state["state"], DEGRADED)
        blockers = " ".join(state["blockers"])
        self.assertIn("in the future", blockers)
        self.assertIn("clock", blockers)
        self.assertIsNone(state["last_event_age_seconds"])
        self.assertAlmostEqual(state["clock_skew_seconds"], 3600.0, places=1)

    def test_one_future_row_cannot_mask_genuinely_stale_telemetry(self):
        # The reproduced defect: 5,000 events genuinely 30 days stale, plus one
        # row stamped an hour ahead. The conclusion must match the control.
        control = client_observation(
            "claude-code", _sample(5000, age_hours=24 * 30), configured=True, now=NOW
        )
        skewed = client_observation(
            "claude-code",
            _skewed_sample(5000, observed_age_hours=24 * 30, received_skew_seconds=3600),
            configured=True,
            now=NOW,
        )
        self.assertEqual(control["state"], DEGRADED)
        self.assertEqual(skewed["state"], control["state"])
        self.assertTrue(
            any("freshness budget" in b for b in skewed["blockers"]), skewed["blockers"]
        )
        self.assertTrue(any("in the future" in b for b in skewed["blockers"]), skewed["blockers"])

    def test_the_deployment_is_not_capable_on_a_future_timestamp_alone(self):
        report = self._report(
            _skewed_sample(5000, observed_age_hours=24 * 30, received_skew_seconds=3600)
        )
        self.assertFalse(report["observation_capable"])
        self.assertEqual(report["verdict"], "NOT_OBSERVATION_CAPABLE")
        self.assertEqual(report["evidence"]["clients_observing"], 0)
        self.assertTrue(report["blockers"])

    def test_the_skewed_deployment_reaches_the_same_verdict_as_the_control(self):
        control = self._report(_sample(5000, age_hours=24 * 30))
        skewed = self._report(
            _skewed_sample(5000, observed_age_hours=24 * 30, received_skew_seconds=3600)
        )
        self.assertEqual(skewed["verdict"], control["verdict"])
        self.assertEqual(
            skewed["evidence"]["clients_observing"], control["evidence"]["clients_observing"]
        )

    def test_a_client_that_looks_fresh_only_because_of_skew_is_still_blocked(self):
        # Nothing usable at all: every stamp is ahead of the tolerance, so the
        # deployment has no freshness evidence and must not read as capable.
        sample = _skewed_sample(5000, observed_age_hours=0.0, received_skew_seconds=7200)
        sample["last_observed"] = (NOW + timedelta(hours=2)).isoformat()
        report = self._report(sample)
        self.assertFalse(report["observation_capable"])
        self.assertEqual(report["evidence"]["clients_observing"], 0)

    def test_sub_tolerance_jitter_is_unchanged(self):
        # Ordinary skew below the tolerance is normal and must not regress.
        state = client_observation(
            "claude-code",
            _skewed_sample(500, observed_age_hours=0.0, received_skew_seconds=240),
            configured=True,
            now=NOW,
        )
        self.assertEqual(state["state"], OBSERVING)
        self.assertEqual(state["blockers"], [])
        self.assertEqual(state["last_event_age_seconds"], 0.0)
        self.assertIsNone(state["clock_skew_seconds"])

    def test_the_future_tolerance_boundary_is_pinned(self):
        from observatory.observation import FUTURE_TIMESTAMP_TOLERANCE_SECONDS

        self.assertEqual(FUTURE_TIMESTAMP_TOLERANCE_SECONDS, 300)
        inside = client_observation(
            "claude-code",
            _skewed_sample(500, observed_age_hours=0.0, received_skew_seconds=299),
            configured=True,
            now=NOW,
        )
        outside = client_observation(
            "claude-code",
            _skewed_sample(500, observed_age_hours=0.0, received_skew_seconds=301),
            configured=True,
            now=NOW,
        )
        self.assertEqual(inside["state"], OBSERVING)
        self.assertEqual(inside["blockers"], [])
        self.assertEqual(outside["state"], DEGRADED)
        self.assertTrue(any("in the future" in b for b in outside["blockers"]), outside["blockers"])

    def test_a_usable_stamp_still_supplies_the_age_when_another_is_skewed(self):
        # `last_received` is unusable, `last_observed` is not: report the age the
        # usable stamp gives AND the skew, rather than discarding both.
        state = client_observation(
            "claude-code",
            _skewed_sample(500, observed_age_hours=2.0, received_skew_seconds=3600),
            configured=True,
            now=NOW,
        )
        self.assertAlmostEqual(state["last_event_age_seconds"] / 3600, 2.0, places=2)
        self.assertTrue(any("in the future" in b for b in state["blockers"]), state["blockers"])
        self.assertEqual(state["state"], DEGRADED)

    def test_a_skewed_lifetime_stamp_is_named_rather_than_dropped(self):
        # The delivered-then-silent branch reads the same payload-supplied stamp.
        state = client_observation(
            "grok",
            None,
            configured=True,
            now=NOW,
            lifetime={"events": 128, "last_received": (NOW + timedelta(hours=1)).isoformat()},
        )
        self.assertEqual(state["state"], DEGRADED)
        self.assertTrue(any("in the future" in b for b in state["blockers"]), state["blockers"])


if __name__ == "__main__":
    unittest.main()
