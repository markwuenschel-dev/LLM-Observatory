"""Tests for host-level session-to-project discovery.

Native OTLP telemetry reports a session but never a working directory, so the
client's own per-project session directory is the only host-level source that
can say which repository a session belonged to. Two properties matter: it must
not fabricate a project, and it must not read session contents.
"""

import tempfile
import unittest
from pathlib import Path

from observatory.sessions import (
    DiscoveredSession,
    decode_project_directory,
    discover_sessions,
)
from observatory.store import UNKNOWN_PROJECT_ID, EventStore

SESSION_A = "0d1145c6-fde5-421d-a356-a3ad2b65c561"
SESSION_B = "8b87580e-00ae-4abf-aa40-97a3aa762e30"


def _encode(path: Path) -> str:
    return str(path).replace(":", "-").replace("\\", "-").replace("/", "-")


class DirectoryDecodingTests(unittest.TestCase):
    def test_a_project_directory_resolves_to_the_real_working_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "my-project"
            target.mkdir()
            self.assertEqual(decode_project_directory(_encode(target)), target)

    def test_a_hyphenated_repository_name_is_resolved_against_the_filesystem(self):
        # The encoding is ambiguous: separators and literal hyphens both become
        # "-". Guessing would split "ibkr-auto-trader" into three directories.
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "ibkr-auto-trader" / "nested-thing"
            target.mkdir(parents=True)
            self.assertEqual(decode_project_directory(_encode(target)), target)

    def test_a_directory_that_no_longer_exists_yields_no_project(self):
        # A deleted or moved repository must not be reported under a guess.
        self.assertIsNone(decode_project_directory("C--Users-nobody-does-not-exist-anywhere-at-all"))

    def test_a_traversal_that_resolves_to_a_real_directory_is_refused(self):
        # The existing negative case uses a path that does not exist, so it
        # passes with the guard deleted. This one resolves: without the guard
        # it yields a real directory outside the tree the name describes, and
        # `resolve_project` would then shell out to git there.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "outer" / "inner"
            root.mkdir(parents=True)
            escaped = _encode(root) + "-..-.."
            self.assertIsNone(decode_project_directory(escaped))
            # sanity: the same name without the traversal does resolve
            self.assertEqual(decode_project_directory(_encode(root)), root)

    def test_decoding_a_pathological_name_terminates_quickly(self):
        # Backtracking without memoisation is exponential: ~20 hyphens took
        # 11 seconds and 75,000 stat calls before the work ceiling was added.
        import time

        with tempfile.TemporaryDirectory() as temp:
            deep = Path(temp)
            for _ in range(6):
                deep = deep / "a"
            deep.mkdir(parents=True)
            name = _encode(Path(temp)) + "-" + "-".join(["a"] * 24)
            started = time.monotonic()
            decode_project_directory(name)
            self.assertLess(time.monotonic() - started, 2.0)

    def test_empty_and_dotted_names_are_ignored(self):
        self.assertIsNone(decode_project_directory(""))
        self.assertIsNone(decode_project_directory(".hidden"))


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name) / "projects"
        self.root.mkdir()
        self.workdir = Path(self._temp.name) / "work-space"
        self.workdir.mkdir()
        self.project_dir = self.root / _encode(self.workdir)
        self.project_dir.mkdir()

    def test_sessions_are_discovered_from_file_names(self):
        (self.project_dir / f"{SESSION_A}.jsonl").write_text("unused", encoding="utf-8")
        found = list(discover_sessions(self.root))
        self.assertEqual([s.session_id for s in found], [SESSION_A])
        self.assertEqual(found[0].project_root, str(self.workdir))
        self.assertEqual(found[0].evidence_source, "claude-session-directory")

    def test_a_session_recorded_as_both_a_file_and_a_folder_is_reported_once(self):
        (self.project_dir / f"{SESSION_A}.jsonl").write_text("unused", encoding="utf-8")
        (self.project_dir / SESSION_A).mkdir()
        self.assertEqual(len(list(discover_sessions(self.root))), 1)

    def test_non_session_entries_are_ignored(self):
        (self.project_dir / "memory").mkdir()
        (self.project_dir / "notes.txt").write_text("unused", encoding="utf-8")
        self.assertEqual(list(discover_sessions(self.root)), [])

    def test_session_contents_are_never_opened(self):
        # Transcripts contain prompts and completions. Discovery reads names.
        transcript = self.project_dir / f"{SESSION_A}.jsonl"
        transcript.write_text('{"prompt": "a secret"}', encoding="utf-8")
        opened: list[str] = []
        real_open = Path.open

        def _tracking_open(self, *args, **kwargs):
            opened.append(str(self))
            return real_open(self, *args, **kwargs)

        Path.open = _tracking_open
        try:
            found = list(discover_sessions(self.root))
        finally:
            Path.open = real_open
        self.assertEqual(len(found), 1)
        self.assertNotIn(str(transcript), opened)

    def test_a_missing_root_yields_nothing_rather_than_failing(self):
        self.assertEqual(list(discover_sessions(Path(self._temp.name) / "absent")), [])


class BindingTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.store = EventStore(Path(self._temp.name) / "events.sqlite3")
        self.addCleanup(self.store.close)

    def _identity(self, project_id="repo_sha256:abc", repository="demo", branch="main"):
        from observatory.contracts import ProjectIdentity

        return ProjectIdentity(project_id=project_id, repository=repository, branch=branch)

    def test_a_discovered_binding_is_recorded(self):
        self.assertTrue(self.store.bind_session_project(SESSION_A, self._identity()))
        row = self.store.connection.execute(
            "SELECT project_id, repository, evidence_source FROM session_projects WHERE session_id = ?",
            (SESSION_A,),
        ).fetchone()
        self.assertEqual(row["project_id"], "repo_sha256:abc")
        self.assertEqual(row["repository"], "demo")
        self.assertEqual(row["evidence_source"], "session-directory")

    def test_rebinding_is_idempotent_and_never_reattributes(self):
        self.store.bind_session_project(SESSION_A, self._identity("repo_sha256:first"))
        self.assertFalse(self.store.bind_session_project(SESSION_A, self._identity("repo_sha256:second")))
        bound = self.store.connection.execute(
            "SELECT project_id FROM session_projects WHERE session_id = ?", (SESSION_A,)
        ).fetchone()["project_id"]
        self.assertEqual(bound, "repo_sha256:first")

    def test_an_unresolved_project_is_never_bound(self):
        self.assertFalse(self.store.bind_session_project(SESSION_B, self._identity(UNKNOWN_PROJECT_ID)))
        self.assertFalse(self.store.bind_session_project("", self._identity()))
        count = self.store.connection.execute("SELECT COUNT(*) FROM session_projects").fetchone()[0]
        self.assertEqual(count, 0)

    def test_a_discovered_binding_attributes_later_native_telemetry(self):
        from datetime import datetime, timezone

        from observatory.contracts import NormalizedEvent

        self.store.bind_session_project(SESSION_A, self._identity())
        now = datetime.now(timezone.utc).isoformat()
        self.store.append(
            NormalizedEvent.from_mapping(
                {
                    "schema_version": "1.0",
                    "event_id": "otlp-1",
                    "event_type": "model.operation",
                    "observed_at": now,
                    "received_at": now,
                    "project": {"project_id": UNKNOWN_PROJECT_ID},
                    "execution": {"session_id": SESSION_A},
                    "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
                }
            )
        )
        row = self.store.connection.execute(
            "SELECT project_id, repository FROM events WHERE event_id = 'otlp-1'"
        ).fetchone()
        self.assertEqual(row["project_id"], "repo_sha256:abc")
        self.assertEqual(row["repository"], "demo")


if __name__ == "__main__":
    unittest.main()
