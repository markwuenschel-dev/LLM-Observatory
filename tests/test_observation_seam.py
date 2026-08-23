"""End-to-end coverage of the store -> verdict -> HTTP seam.

`observation_report` was well covered, but every one of those tests fed it a
hand-written sample dict. Nothing verified that `EventStore` actually produces
the shape the report consumes, and nothing exercised the two routes or the
`observe` subcommand at all. That is exactly how a guaranteed HTTP 503 shipped
with a green suite.
"""

import json
import tempfile
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observatory.api import ObservatoryApplication, ObservatoryHTTPServer
from observatory.contracts import NormalizedEvent
from observatory.observation import (
    DEFAULT_FRESHNESS_SECONDS,
    DEFAULT_WINDOW_SECONDS,
    OBSERVING,
    observation_report,
    window_start,
)
from observatory.store import EventStore


def _event(event_id, *, client="claude-code", session="S", attributed=True, observed=None, cost=1.0):
    stamp = (observed or datetime.now(timezone.utc)).isoformat()
    return NormalizedEvent.from_mapping(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "model.operation",
            "observed_at": stamp,
            "received_at": stamp,
            "project": {
                "project_id": "repo_sha256:abc" if attributed else "project:unknown",
                "repository": "demo" if attributed else None,
            },
            "execution": {"session_id": session, "agent_id": "planner"},
            "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": client},
            "usage": {"cost": cost, "total_tokens": 100},
            "performance": {"latency_ms": 50},
        }
    )


class StoreToVerdictSeamTests(unittest.TestCase):
    """The producer and the consumer must actually agree."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = Path(self._dir.name) / "events.sqlite3"
        self.store = EventStore(self.path)
        self.addCleanup(self.store.close)

    def _verdict(self, configured=None):
        samples = self.store.observation_samples(window_start(DEFAULT_WINDOW_SECONDS))
        recent = self.store.observation_samples(window_start(DEFAULT_FRESHNESS_SECONDS))
        lifetime = self.store.client_lifetime()
        return observation_report(
            samples,
            recent_samples=recent,
            lifetime_totals=lifetime,
            configured_clients=configured if configured is not None else {"claude": {"applied": True}},
            store_health={"reachable": True, **self.store.capacity()},
        )

    def test_the_store_produces_every_key_the_report_consumes(self):
        self.store.append(_event("e1"))
        sample = self.store.observation_samples(window_start(DEFAULT_WINDOW_SECONDS))["claude-code"]
        from observatory.observation import SIGNAL_FAMILIES

        for family in SIGNAL_FAMILIES:
            self.assertIn(family, sample, f"store does not emit {family!r} for the report to grade")
        for key in ("events", "last_observed", "last_received"):
            self.assertIn(key, sample)

    def test_fresh_attributed_telemetry_yields_a_capable_verdict(self):
        for index in range(20):
            self.store.append(_event(f"e{index}"))
        report = self._verdict()
        self.assertTrue(report["observation_capable"], report["blockers"])
        states = {c["client"]: c["state"] for c in report["clients"]}
        self.assertEqual(states["claude-code"], OBSERVING)

    def test_unattributed_telemetry_yields_a_degraded_client(self):
        for index in range(20):
            self.store.append(_event(f"e{index}", attributed=False))
        report = self._verdict()
        client = next(c for c in report["clients"] if c["client"] == "claude-code")
        self.assertEqual(client["project_attribution"]["ratio"], 0.0)
        self.assertFalse(report["observation_capable"])

    def test_an_empty_store_is_not_capable(self):
        report = self._verdict()
        self.assertFalse(report["observation_capable"])
        self.assertTrue(report["blockers"])

    def test_lifetime_totals_survive_the_window(self):
        # A client outside the coverage window must still be seen as having
        # delivered, which is what separates a stale hook from a dead one.
        old = datetime.now(timezone.utc) - timedelta(days=30)
        self.store.append(_event("old-1", observed=old))
        lifetime = self.store.client_lifetime()
        self.assertEqual(lifetime["claude-code"]["events"], 1)
        samples = self.store.observation_samples(window_start(DEFAULT_WINDOW_SECONDS))
        self.assertNotIn("claude-code", samples)


class ObservationRouteTests(unittest.TestCase):
    """Neither observation route was ever requested by a test."""

    def _serve(self, seed):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name) / "events.sqlite3"
        store = EventStore(path)
        seed(store)
        store.close()
        application = ObservatoryApplication(EventStore(path))
        # Cleanups run last-registered-first, so the store must close before the
        # temp directory is removed or Windows refuses to unlink the database.
        self.addCleanup(application.store.close)
        server = ObservatoryHTTPServer(("127.0.0.1", 0), application, plane="read")
        self.addCleanup(server.server_close)
        import threading

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def _get(self, base, path):
        try:
            with urllib.request.urlopen(f"{base}{path}", timeout=30) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_observation_reports_not_capable_on_an_empty_store(self):
        base = self._serve(lambda store: None)
        status, body = self._get(base, "/v1/observation")
        self.assertEqual(status, 503)
        self.assertEqual(body["verdict"], "NOT_OBSERVATION_CAPABLE")
        self.assertTrue(body["blockers"])

    def test_observation_history_is_served_and_reports_its_gaps(self):
        def seed(store):
            store.record_observation({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "verdict": "OBSERVATION_CAPABLE", "observation_capable": True,
                "evidence": {}, "blockers": [], "clients": [],
            })

        base = self._serve(seed)
        status, body = self._get(base, "/v1/observation/history")
        self.assertEqual(status, 200)
        self.assertIn("reliability", body)
        self.assertIn("coverage_drift", body)
        self.assertEqual(body["reliability"]["snapshots"], 1)

    def test_every_client_row_carries_the_fields_the_objective_requires(self):
        base = self._serve(lambda store: store.append(_event("e1")))
        _status, body = self._get(base, "/v1/observation")
        self.assertTrue(body["clients"])
        for client in body["clients"]:
            for key in ("client", "state", "last_event_at", "project_attribution",
                        "signal_families", "blockers"):
                self.assertIn(key, client, f"{key} missing from the client verdict")
            self.assertIn(client["state"],
                          ("OBSERVING", "DEGRADED", "CONFIGURED_UNVERIFIED",
                           "NOT_CONFIGURED", "UNSUPPORTED"))


if __name__ == "__main__":
    unittest.main()
