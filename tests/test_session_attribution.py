"""Regressions for host-level attribution and store durability defects.

Each test corresponds to a defect observed on a live host, where the Observatory
looked healthy while silently losing or mis-attributing telemetry.
"""

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from observatory.contracts import NormalizedEvent
from observatory.store import (
    UNKNOWN_PROJECT_ID,
    WAL_AUTOCHECKPOINT_PAGES,
    WAL_SIZE_LIMIT_BYTES,
    EventStore,
)


def _event(event_id, project_id, session_id, **kw):
    now = datetime.now(timezone.utc).isoformat()
    return NormalizedEvent.from_mapping(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "model.operation",
            "observed_at": now,
            "received_at": now,
            "project": {
                "project_id": project_id,
                "repository": kw.get("repository"),
                "branch": kw.get("branch"),
            },
            "execution": {"session_id": session_id},
            "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
        }
    )


class SessionProjectAttributionTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _stored(self, event_id):
        return self.store.connection.execute(
            "SELECT project_id, repository, branch FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()

    def test_native_event_inherits_project_from_its_session_binding(self):
        # Native OTLP carries a session id but no working directory.
        self.store.append(
            _event("hook", "repo_sha256:abc", "s1", repository="LLM-Observatory", branch="main")
        )
        self.store.append(_event("otlp", UNKNOWN_PROJECT_ID, "s1"))
        row = self._stored("otlp")
        self.assertEqual(row["project_id"], "repo_sha256:abc")
        self.assertEqual(row["repository"], "LLM-Observatory")
        self.assertEqual(row["branch"], "main")

    def test_unbound_session_is_left_unknown_rather_than_guessed(self):
        self.store.append(_event("orphan", UNKNOWN_PROJECT_ID, "never-seen"))
        self.assertEqual(self._stored("orphan")["project_id"], UNKNOWN_PROJECT_ID)

    def test_event_without_a_session_is_left_unknown(self):
        self.store.append(_event("hook", "repo_sha256:abc", "s1"))
        self.store.append(_event("sessionless", UNKNOWN_PROJECT_ID, None))
        self.assertEqual(self._stored("sessionless")["project_id"], UNKNOWN_PROJECT_ID)

    def test_derived_attribution_is_recorded_in_field_provenance(self):
        # A derived project must never look like something the client reported.
        self.store.append(_event("hook", "repo_sha256:abc", "s1"))
        self.store.append(_event("otlp", UNKNOWN_PROJECT_ID, "s1"))
        row = self.store.connection.execute(
            "SELECT payload_json FROM events WHERE event_id = 'otlp'"
        ).fetchone()
        payload = json.loads(row["payload_json"])
        self.assertEqual(
            payload["provenance"]["fields"].get("project.project_id"),
            "derived:session_binding",
        )

    def test_an_unknown_project_never_becomes_a_binding(self):
        self.store.append(_event("otlp", UNKNOWN_PROJECT_ID, "s1"))
        count = self.store.connection.execute("SELECT COUNT(*) FROM session_projects").fetchone()[0]
        self.assertEqual(count, 0)

    def test_first_binding_wins_so_a_session_is_not_reattributed(self):
        self.store.append(_event("a", "repo_sha256:first", "s1"))
        self.store.append(_event("b", "repo_sha256:second", "s1"))
        bound = self.store.connection.execute(
            "SELECT project_id FROM session_projects WHERE session_id = 's1'"
        ).fetchone()["project_id"]
        self.assertEqual(bound, "repo_sha256:first")


class StoreDurabilityTests(unittest.TestCase):
    def test_wal_growth_is_bounded(self):
        # An untruncated WAL counts toward the budget and silently stops intake.
        with tempfile.TemporaryDirectory() as temp:
            store = EventStore(Path(temp) / "events.sqlite3")
            try:
                self.assertEqual(
                    store.connection.execute("PRAGMA journal_size_limit").fetchone()[0],
                    WAL_SIZE_LIMIT_BYTES,
                )
                self.assertEqual(
                    store.connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0],
                    WAL_AUTOCHECKPOINT_PAGES,
                )
            finally:
                # Windows will not remove the temp dir while the file is open.
                store.close()

    def test_reopening_a_store_does_not_rescan_every_event(self):
        # Backfill was O(total events) per open, which stalled API startup.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.sqlite3"
            store = EventStore(path)
            for index in range(50):
                store.append(_event("e%d" % index, "repo_sha256:abc", "s1"))
            store.close()

            reopened = EventStore(path)
            try:
                statements = []
                reopened.connection.set_trace_callback(
                    lambda sql: statements.append(" ".join(str(sql).split()))
                )
                try:
                    reopened._backfill_projections()
                finally:
                    reopened.connection.set_trace_callback(None)

                scans = [s for s in statements if "payload_json" in s and "FROM events" in s]
                self.assertEqual(len(scans), 1, "expected exactly one bounded probe query")
                self.assertIn("NOT EXISTS", scans[0])
                # A full rescan would re-read every stored payload; the bounded
                # probe must not touch them at all on a healthy store.
                self.assertFalse([s for s in statements if s.startswith("INSERT")])
                remaining = reopened.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                self.assertEqual(remaining, 50)
            finally:
                reopened.close()


class ManagedClaudeHookTests(unittest.TestCase):
    def _settings(self, temp):
        path = temp / "settings.json"
        path.write_text(
            json.dumps(
                {
                    "model": "opus",
                    "env": {"EXISTING": "keep-me"},
                    "hooks": {
                        "SessionStart": [
                            {"matcher": "", "hooks": [{"type": "command", "command": "user-own.sh"}]}
                        ]
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_apply_adds_a_session_start_hook_and_remove_restores_user_config(self):
        import observatory.clients as clients

        with tempfile.TemporaryDirectory() as temp:
            path = self._settings(Path(temp))
            spec = clients.client_spec("claude")
            with patch.object(clients, "config_path", return_value=path):
                applied = clients._apply_json(spec, enable_traces=True, force=False)
                after = json.loads(path.read_text(encoding="utf-8"))
                commands = [
                    handler["command"]
                    for group in after["hooks"]["SessionStart"]
                    for handler in group["hooks"]
                ]
                self.assertTrue(any("hook --client claude-code" in c for c in commands))
                self.assertIn("user-own.sh", commands)

                clients._remove_json(
                    spec,
                    managed_keys=applied["managed_keys"],
                    managed_state=applied["managed_state"],
                )
                restored = json.loads(path.read_text(encoding="utf-8"))

            remaining = [
                handler["command"]
                for group in restored.get("hooks", {}).get("SessionStart", [])
                for handler in group["hooks"]
            ]
            self.assertEqual(remaining, ["user-own.sh"])
            self.assertEqual(restored["env"], {"EXISTING": "keep-me"})
            self.assertEqual(restored["model"], "opus")

    def test_managed_hook_declares_a_timeout(self):
        # An unbounded hook would let telemetry delay every session start.
        import observatory.clients as clients

        handler = clients._claude_hook_handler()
        self.assertEqual(handler["type"], "command")
        self.assertIsInstance(handler["timeout"], int)
        self.assertLessEqual(handler["timeout"], 10)


class HostProcessLivenessTests(unittest.TestCase):
    # os.kill(pid, 0) raises WinError 87 for a detached Windows process, which
    # made stop/uninstall orphan a healthy API and delete its process record.
    def test_a_running_process_is_reported_alive(self):
        from observatory.cli import _api_pid_alive

        self.assertTrue(_api_pid_alive(os.getpid()))

    def test_an_absent_process_is_reported_dead(self):
        from observatory.cli import _api_pid_alive

        self.assertFalse(_api_pid_alive(0))


if __name__ == "__main__":
    unittest.main()
