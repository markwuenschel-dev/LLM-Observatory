"""Applying must never install a second copy of a hook that is already there.

Found on the live host: `~/.grok/config.toml` carries three
`observatory.exe hook --client grok` entries written in a different TOML shape
from the one Observatory emits, and with no BEGIN/END markers. `_apply_hook`
branched only on its own marker, so `configure grok --apply` would have appended
a marked block beside them -- leaving the client invoking the hook twice for
every event, and doubling grok's apparent activity against every other client it
is compared with. That is precisely the "missing telemetry is not zero
telemetry" rule failing in the other direction: duplicated telemetry is not
double the work.

The ownership manifest could not catch it either, because it correctly records
`applied: false` for grok -- Observatory does not own those hooks. Recognition
therefore has to come from the command line itself, not from bookkeeping.
"""

import os
import tempfile
import unittest
from pathlib import Path

from observatory import clients
from observatory.clients import (
    HOOK_MARKER_END,
    HOOK_MARKER_START,
    apply_configuration,
    client_spec,
    config_path,
    configuration_drift,
)

# The real shape found on disk: different TOML layout, no markers.
UNMARKED_GROK_HOOKS = """[models]
default = "grok-build"

[[hooks.SessionStart]]

[[hooks.SessionStart.hooks]]
type = "command"
command = 'C:\\Users\\someone\\Scripts\\observatory.exe hook --client grok --quiet'
timeout = 2
"""


class _TempHome(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.home = Path(self._temp.name) / "home"
        self.home.mkdir()
        saved = {n: os.environ.get(n) for n in ("USERPROFILE", "HOME", "CODEX_HOME")}

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
        real = clients.discover_client
        clients.discover_client = lambda name: {
            "client": name, "installed": True, "version": "test", "version_probe_status": "verified"}
        self.addCleanup(setattr, clients, "discover_client", real)

    def _write(self, name, text):
        path = config_path(client_spec(name))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path


class UnmanagedHookGuardTests(_TempHome):
    def test_existing_unmarked_hooks_block_the_apply(self):
        path = self._write("grok", UNMARKED_GROK_HOOKS)
        result = apply_configuration("grok", enable_traces=False)
        self.assertFalse(result["applied"])
        self.assertIn("duplicate", " ".join(result["conflicts"]).lower())
        self.assertEqual(path.read_text(encoding="utf-8"), UNMARKED_GROK_HOOKS,
                         "the config was modified while reporting a conflict")

    def test_the_hook_is_not_installed_twice(self):
        path = self._write("grok", UNMARKED_GROK_HOOKS)
        apply_configuration("grok", enable_traces=False)
        text = path.read_text(encoding="utf-8")
        self.assertEqual(text.count("hook --client grok"), 1,
                         "applying installed a second copy of the hook")

    def test_force_is_the_documented_way_through(self):
        path = self._write("grok", UNMARKED_GROK_HOOKS)
        result = apply_configuration("grok", enable_traces=False, force=True)
        self.assertTrue(result["applied"], result.get("conflicts"))
        self.assertIn(HOOK_MARKER_START, path.read_text(encoding="utf-8"))

    def test_a_clean_config_still_applies(self):
        path = self._write("grok", "[models]\ndefault = \"grok-build\"\n")
        result = apply_configuration("grok", enable_traces=False)
        self.assertTrue(result["applied"], result.get("conflicts"))
        self.assertIn(HOOK_MARKER_START, path.read_text(encoding="utf-8"))

    def test_our_own_marked_block_is_not_mistaken_for_an_intruder(self):
        # The managed-block path must keep working; a false conflict here would
        # make every routine re-apply fail.
        self._write("kimi", "")
        first = apply_configuration("kimi", enable_traces=False)
        self.assertTrue(first["applied"], first.get("conflicts"))
        second = apply_configuration("kimi", enable_traces=False)
        self.assertTrue(second["applied"], second.get("conflicts"))
        text = config_path(client_spec("kimi")).read_text(encoding="utf-8")
        self.assertEqual(text.count(HOOK_MARKER_START), 1)
        self.assertEqual(text.count(HOOK_MARKER_END), 1)

    def test_someone_elses_hooks_are_not_our_business(self):
        # Only a hook invoking *our* command counts. A client's own unrelated
        # hooks must not block configuration.
        path = self._write("grok", "[[hooks.SessionStart.hooks]]\ncommand = 'ruff check'\n")
        result = apply_configuration("grok", enable_traces=False)
        self.assertTrue(result["applied"], result.get("conflicts"))
        self.assertIn("ruff check", path.read_text(encoding="utf-8"))

    def test_a_hook_for_a_different_client_does_not_block_this_one(self):
        path = self._write("grok", "[[hooks.SessionStart.hooks]]\n"
                                   "command = 'observatory.exe hook --client kimi --quiet'\n")
        result = apply_configuration("grok", enable_traces=False)
        self.assertTrue(result["applied"], result.get("conflicts"))


class ConfigurationDriftTests(_TempHome):
    def test_a_client_we_do_not_own_is_never_reported(self):
        self._write("grok", UNMARKED_GROK_HOOKS)
        self.assertEqual(configuration_drift("grok", applied=False, managed_keys=["managed_block"]), [])

    def test_a_vanished_config_file_is_reported(self):
        problems = configuration_drift("grok", applied=True, managed_keys=["managed_block"])
        self.assertEqual(len(problems), 1)
        self.assertIn("no longer exists", problems[0])

    def test_a_managed_block_that_was_removed_is_reported(self):
        self._write("grok", UNMARKED_GROK_HOOKS)
        problems = configuration_drift("grok", applied=True, managed_keys=["managed_block"])
        self.assertEqual(len(problems), 1)
        self.assertIn("no Observatory marker", problems[0])

    def test_an_edited_managed_block_is_reported(self):
        self._write("kimi", "")
        result = apply_configuration("kimi", enable_traces=False)
        path = config_path(client_spec("kimi"))
        path.write_text(path.read_text(encoding="utf-8").replace(
            HOOK_MARKER_END, "# hand edit\n" + HOOK_MARKER_END), encoding="utf-8")
        problems = configuration_drift(
            "kimi", applied=True, managed_keys=["managed_block"],
            managed_hash=result.get("managed_hash"))
        self.assertEqual(len(problems), 1)
        self.assertIn("edited", problems[0])

    def test_an_intact_managed_block_reports_nothing(self):
        self._write("kimi", "")
        result = apply_configuration("kimi", enable_traces=False)
        self.assertEqual(
            configuration_drift("kimi", applied=True, managed_keys=["managed_block"],
                                managed_hash=result.get("managed_hash")),
            [],
        )

    def test_missing_json_keys_are_reported(self):
        import json

        path = config_path(client_spec("claude"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"env": {"OTEL_METRICS_EXPORTER": "otlp"}}), encoding="utf-8")
        problems = configuration_drift(
            "claude", applied=True,
            managed_keys=["OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT"])
        self.assertEqual(len(problems), 1)
        self.assertIn("are gone", problems[0])
        self.assertIn("telemetry may be silently off", problems[0])


if __name__ == "__main__":
    unittest.main()
