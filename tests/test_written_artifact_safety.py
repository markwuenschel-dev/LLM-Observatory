"""What `configure --apply` actually leaves on disk, for every client.

`tests/test_inference_route_safety.py` checks the *string builders*. That is a
weaker claim than it reads as: it never calls `apply_configuration`, so it does
not cover the Claude `SessionStart` hook (written by a different code path than
any builder it inspects), it never exercises `enable_traces=False`, and it can
say nothing about where files land. The objective's hardest invariant -- that
inference stays independent of telemetry, and that no repository is touched --
is a property of the bytes on disk, so this drives the real writer into a
throwaway HOME and reads the files back.
"""

import json
import os
import re
import tempfile
import unittest
from pathlib import Path

from observatory import clients
from observatory.clients import CLIENT_SPECS, apply_configuration

PROVIDER_ROUTE = re.compile(
    r"(ANTHROPIC_BASE_URL|ANTHROPIC_API_URL|OPENAI_BASE_URL|OPENAI_API_BASE|"
    r"AZURE_OPENAI_ENDPOINT|GOOGLE_API_BASE|GEMINI_BASE_URL|XAI_BASE_URL|"
    r"MOONSHOT_BASE_URL|OPENROUTER_BASE_URL|BEDROCK_ENDPOINT|VERTEX_ENDPOINT|"
    r"HTTPS?_PROXY|ALL_PROXY|NO_PROXY|api_base|base_url|proxy_url)",
    re.I,
)
CREDENTIAL = re.compile(
    r"(API_KEY|ACCESS_TOKEN|SECRET|PASSWORD|BEARER|sk-[A-Za-z0-9]{8,}|"
    r"xai-[A-Za-z0-9]{8,}|AIza[A-Za-z0-9_\-]{10,})",
    re.I,
)
LOOPBACK = re.compile(r"^https?://(127\.0\.0\.1|localhost|\[::1\])(:\d+)?(/|$)")
URL = re.compile(r"https?://[^\s\"',\}\]]+")

# Clients that write a file at all; the rest are plan-only by design.
NATIVE = sorted(key for key, spec in CLIENT_SPECS.items() if spec.native_config)


class _TemporaryHome(unittest.TestCase):
    """Drive the real writer into a throwaway HOME."""

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.home = Path(self._temp.name) / "home"
        self.home.mkdir()
        self.repo = Path(self._temp.name) / "repo"
        self.repo.mkdir()

        saved = {
            name: os.environ.get(name)
            for name in (
                "USERPROFILE", "HOME", "CODEX_HOME",
                "OBSERVATORY_OTLP_GRPC_ENDPOINT", "OBSERVATORY_OTLP_HTTP_ENDPOINT",
            )
        }

        def restore():
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

        self.addCleanup(restore)
        os.environ["USERPROFILE"] = str(self.home)
        os.environ["HOME"] = str(self.home)
        os.environ["CODEX_HOME"] = str(self.home / ".codex")
        os.environ.pop("OBSERVATORY_OTLP_GRPC_ENDPOINT", None)
        os.environ.pop("OBSERVATORY_OTLP_HTTP_ENDPOINT", None)

        # Discovery shells out to each client's --version probe. Running real
        # client binaries ten times is a side effect this test does not need;
        # what is under test is the writer, not the prober.
        real_discover = clients.discover_client
        clients.discover_client = lambda name: {
            "client": name, "installed": True, "version": "test",
            "version_probe_status": "verified",
        }
        self.addCleanup(setattr, clients, "discover_client", real_discover)

    def _written(self):
        return sorted(p for p in self.home.rglob("*") if p.is_file())

    def _configure_all(self, *, enable_traces):
        for name in NATIVE:
            result = apply_configuration(name, enable_traces=enable_traces, force=True)
            self.assertTrue(result.get("applied"), f"{name}: {result.get('conflicts')}")


class WrittenArtifactTests(_TemporaryHome):
    def test_nothing_written_sets_a_provider_route_or_carries_a_credential(self):
        for enable_traces in (False, True):
            with self.subTest(traces=enable_traces):
                self._configure_all(enable_traces=enable_traces)
                files = self._written()
                self.assertTrue(files, "configure --apply wrote nothing at all")
                for path in files:
                    text = path.read_text(encoding="utf-8", errors="replace")
                    route = PROVIDER_ROUTE.search(text)
                    self.assertIsNone(route, f"{path.name} sets {route.group(0) if route else ''}")
                    secret = CREDENTIAL.search(text)
                    self.assertIsNone(secret, f"{path.name} carries {secret.group(0) if secret else ''}")

    def test_every_url_written_to_disk_stays_on_loopback(self):
        self._configure_all(enable_traces=True)
        found = [(p.name, url) for p in self._written()
                 for url in URL.findall(p.read_text(encoding="utf-8", errors="replace"))]
        self.assertTrue(found, "expected a telemetry endpoint somewhere on disk")
        for name, url in found:
            with self.subTest(file=name, url=url):
                self.assertRegex(url, LOOPBACK, f"{name} points telemetry off-host: {url}")

    def test_configuring_every_client_touches_no_repository(self):
        # Zero-repository-contamination is the whole premise of adding a repo by
        # doing nothing to it. A writer that resolved a relative path would land
        # in the working directory, so check the working directory.
        previous = Path.cwd()
        os.chdir(self.repo)
        try:
            self._configure_all(enable_traces=True)
        finally:
            os.chdir(previous)
        self.assertEqual(list(self.repo.rglob("*")), [])

    def test_the_claude_hook_is_written_bounded_and_route_free(self):
        # The SessionStart hook is a command line the client executes at every
        # session start. No builder-level test covers it.
        self._configure_all(enable_traces=False)
        settings = json.loads((self.home / ".claude" / "settings.json").read_text(encoding="utf-8"))
        groups = settings["hooks"][clients.CLAUDE_HOOK_EVENT]
        handlers = [h for group in groups for h in group.get("hooks", [])
                    if clients._is_observatory_claude_hook(h)]
        self.assertEqual(len(handlers), 1, "expected exactly one managed hook")
        handler = handlers[0]
        self.assertEqual(handler["timeout"], clients.CLAUDE_HOOK_TIMEOUT_SECONDS)
        self.assertGreater(handler["timeout"], 0)
        self.assertLessEqual(handler["timeout"], 30, "an unbounded hook can delay a session start")
        self.assertIsNone(PROVIDER_ROUTE.search(handler["command"]))
        self.assertIsNone(CREDENTIAL.search(handler["command"]))
        self.assertNotIn("--proxy", handler["command"])

    def test_applying_twice_converges_instead_of_duplicating(self):
        # A hook appended on every run would multiply the per-session cost of
        # telemetry without changing any declared flag.
        self._configure_all(enable_traces=True)
        first = {p: p.read_text(encoding="utf-8") for p in self._written()}
        self._configure_all(enable_traces=True)
        second = {p: p.read_text(encoding="utf-8") for p in self._written()}
        self.assertEqual(sorted(first), sorted(second), "second apply created new files")
        for path, text in first.items():
            with self.subTest(file=path.name):
                self.assertEqual(text, second[path], f"{path.name} changed on a no-op re-apply")

    def test_disabling_traces_writes_no_trace_endpoint(self):
        self._configure_all(enable_traces=False)
        blob = "\n".join(p.read_text(encoding="utf-8", errors="replace") for p in self._written())
        self.assertNotIn("/v1/traces", blob)


class CapabilityHookTests(_TemporaryHome):
    """The skill/workflow hook must stay cheap, or it cannot be enabled at all.

    Which skill ran is the one dimension the ranking needs and no client reports
    natively. It exists only in a tool-call hook payload -- and one hook
    invocation costs ~307 ms here (interpreter start, imports, and a git
    subprocess in `resolve_project`). Unscoped, `PostToolUse` pays that on every
    tool call: 8,619 in one observed session, ~44 minutes of added wall clock.

    The matcher is the whole reason this is affordable, so it is pinned here
    rather than left as a comment.
    """

    def _settings(self):
        import json

        apply_configuration("claude", enable_traces=False)
        return json.loads((self.home / ".claude" / "settings.json").read_text(encoding="utf-8"))

    def _managed(self, settings, event):
        return [
            (group, handler)
            for group in settings.get("hooks", {}).get(event, [])
            for handler in group.get("hooks", [])
            if clients._is_observatory_claude_hook(handler)
        ]

    def test_both_hook_events_are_installed(self):
        settings = self._settings()
        self.assertEqual(len(self._managed(settings, clients.CLAUDE_HOOK_EVENT)), 1)
        self.assertEqual(len(self._managed(settings, clients.CLAUDE_CAPABILITY_HOOK_EVENT)), 1)

    def test_the_capability_hook_is_scoped_by_a_matcher(self):
        group, _ = self._managed(self._settings(), clients.CLAUDE_CAPABILITY_HOOK_EVENT)[0]
        self.assertEqual(group["matcher"], clients.CLAUDE_CAPABILITY_HOOK_MATCHER)
        self.assertTrue(group["matcher"], "an empty matcher fires on every tool call")

    def test_the_matcher_names_only_tools_the_extractor_can_read(self):
        # A matcher naming a tool `_invoked_capability` ignores would pay the
        # hook cost for nothing; one omitting a tool it reads loses the capture.
        from observatory.hooks import _SKILL_TOOLS, _WORKFLOW_TOOLS

        matched = {name.casefold() for name in clients.CLAUDE_CAPABILITY_HOOK_MATCHER.split("|")}
        readable = set(_SKILL_TOOLS) | set(_WORKFLOW_TOOLS)
        self.assertTrue(matched <= readable, f"matcher names tools the extractor ignores: {matched - readable}")
        self.assertTrue(matched, "the matcher must name at least one tool")

    def test_the_session_hook_stays_unscoped(self):
        # SessionStart has no tool to match on; a matcher there would suppress it.
        group, _ = self._managed(self._settings(), clients.CLAUDE_HOOK_EVENT)[0]
        self.assertEqual(group["matcher"], "")

    def test_reapplying_does_not_duplicate_either_hook(self):
        self._settings()
        settings = self._settings()
        for event in (clients.CLAUDE_HOOK_EVENT, clients.CLAUDE_CAPABILITY_HOOK_EVENT):
            with self.subTest(event=event):
                self.assertEqual(len(self._managed(settings, event)), 1)

    def test_a_drifted_matcher_is_corrected(self):
        import json

        path = self.home / ".claude" / "settings.json"
        self._settings()
        settings = json.loads(path.read_text(encoding="utf-8"))
        settings["hooks"][clients.CLAUDE_CAPABILITY_HOOK_EVENT][0]["matcher"] = ""
        path.write_text(json.dumps(settings), encoding="utf-8")
        group, _ = self._managed(self._settings(), clients.CLAUDE_CAPABILITY_HOOK_EVENT)[0]
        self.assertEqual(group["matcher"], clients.CLAUDE_CAPABILITY_HOOK_MATCHER,
                         "a widened matcher was left in place")

    def test_both_hooks_are_bounded_and_route_free(self):
        settings = self._settings()
        for event in (clients.CLAUDE_HOOK_EVENT, clients.CLAUDE_CAPABILITY_HOOK_EVENT):
            _, handler = self._managed(settings, event)[0]
            with self.subTest(event=event):
                self.assertEqual(handler["timeout"], clients.CLAUDE_HOOK_TIMEOUT_SECONDS)
                self.assertIsNone(PROVIDER_ROUTE.search(handler["command"]))
                self.assertIsNone(CREDENTIAL.search(handler["command"]))

    def test_remove_drops_both_events(self):
        import json

        from observatory.clients import remove_configuration

        applied = apply_configuration("claude", enable_traces=False)
        remove_configuration(
            "claude",
            managed_keys=applied.get("managed_keys"),
            managed_hash=applied.get("managed_hash"),
            managed_state=applied.get("managed_state"),
        )
        settings = json.loads((self.home / ".claude" / "settings.json").read_text(encoding="utf-8"))
        for event in (clients.CLAUDE_HOOK_EVENT, clients.CLAUDE_CAPABILITY_HOOK_EVENT):
            with self.subTest(event=event):
                self.assertEqual(self._managed(settings, event), [],
                                 "a removed client left a hook firing with nothing listening")


class ForceTests(_TemporaryHome):
    """`--force` is documented as the way out of a conflicting managed value.

    It was honored only by the JSON writer. `_apply_codex` accepted `force` and
    never read it, and `_apply_hook` did not accept it at all, so a codex, kimi,
    or grok block that had drifted was unresolvable by any command -- the only
    remedy was to hand-edit the client's own config file.
    """

    END_MARKER = {
        "codex": "# END LLM Observatory managed telemetry",
        "kimi": clients.HOOK_MARKER_END,
        "grok": clients.HOOK_MARKER_END,
    }

    def _tamper(self, name):
        path = clients.config_path(clients.client_spec(name))
        text = path.read_text(encoding="utf-8")
        marker = self.END_MARKER[name]
        self.assertIn(marker, text, f"{name} wrote no managed block to drift")
        path.write_text(text.replace(marker, "# hand edit\n" + marker), encoding="utf-8")
        return path

    def test_a_drifted_block_conflicts_without_force_and_is_replaced_with_it(self):
        for name in self.END_MARKER:
            with self.subTest(client=name):
                apply_configuration(name, enable_traces=False)
                path = self._tamper(name)
                tampered = path.read_text(encoding="utf-8")

                blocked = apply_configuration(name, enable_traces=False)
                self.assertFalse(blocked["applied"], f"{name} overwrote a drifted block without --force")
                self.assertTrue(blocked["conflicts"])
                self.assertEqual(path.read_text(encoding="utf-8"), tampered,
                                 f"{name} modified the file while reporting a conflict")

                forced = apply_configuration(name, enable_traces=False, force=True)
                self.assertTrue(forced["applied"], f"{name}: --force did not resolve {forced.get('conflicts')}")
                self.assertEqual(forced.get("overwritten"), ["managed_block"])
                self.assertNotIn("# hand edit", path.read_text(encoding="utf-8"))

    def test_force_reports_no_overwrite_when_nothing_had_drifted(self):
        # Otherwise "overwritten" would claim a user edit was discarded on every
        # routine re-apply.
        for name in self.END_MARKER:
            with self.subTest(client=name):
                apply_configuration(name, enable_traces=False)
                again = apply_configuration(name, enable_traces=False, force=True)
                self.assertEqual(again.get("overwritten"), [])

    def test_force_never_overwrites_a_users_own_otel_table(self):
        # Not our block: a second [otel] table would be invalid TOML, and the
        # existing one is the user's configuration, not a drifted copy of ours.
        path = clients.config_path(clients.client_spec("codex"))
        path.parent.mkdir(parents=True, exist_ok=True)
        mine = '[otel]\nenvironment = "mine"\n'
        path.write_text(mine, encoding="utf-8")
        result = apply_configuration("codex", enable_traces=False, force=True)
        self.assertFalse(result["applied"])
        self.assertEqual(result["conflicts"], ["otel table already exists"])
        self.assertEqual(path.read_text(encoding="utf-8"), mine)


if __name__ == "__main__":
    unittest.main()
