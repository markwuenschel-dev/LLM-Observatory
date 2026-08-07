from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
import sys
import tomllib

from observatory.clients import apply_configuration, client_spec, plan_configuration, remove_configuration
from observatory.cli import main
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore


class ClientAndOutcomeTests(unittest.TestCase):
    def test_native_client_confidence_matches_capability_matrix(self) -> None:
        expected = {
            "claude": "PARTIAL",
            "codex": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "gemini": "SUPPORTED_NOT_LOCALLY_VERIFIED",
        }
        matrix = (Path(__file__).resolve().parents[1] / "docs" / "capability-matrix.yaml").read_text(encoding="utf-8")
        for name, confidence in expected.items():
            spec = client_spec(name)
            self.assertEqual(spec.confidence, confidence)
            client_marker = f"    client: {spec.name}\n"
            start = matrix.index(client_marker)
            end = matrix.find("  - provider:", start + len(client_marker))
            block = matrix[start:] if end == -1 else matrix[start:end]
            self.assertIn(f"    confidence: {confidence}", block)

    def test_configuration_plan_is_explicit_and_contains_no_credentials(self) -> None:
        plan = plan_configuration("claude")
        self.assertTrue(plan["supported"])
        self.assertFalse(plan["inference_proxy"])
        encoded = json.dumps(plan, sort_keys=True).casefold()
        self.assertNotIn("authorization", encoded)
        self.assertNotIn("api_key", encoded)
        self.assertNotIn("bearer", encoded)

    def test_codex_trace_setting_stays_in_the_root_otel_table(self) -> None:
        block = plan_configuration("codex", enable_traces=True)["changes"]["toml_block"]
        self.assertIn('[otel]\n', block)
        parsed = tomllib.loads(block)
        self.assertEqual(parsed["otel"]["exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/logs")
        self.assertEqual(parsed["otel"]["metrics_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/metrics")
        self.assertEqual(parsed["otel"]["trace_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/traces")
        self.assertEqual(tomllib.loads(plan_configuration("codex")["changes"]["toml_block"])["otel"]["trace_exporter"], "none")

    def test_json_client_apply_is_atomic_and_preserves_unrelated_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                settings = home / ".gemini" / "settings.json"
                settings.parent.mkdir(parents=True)
                settings.write_text(json.dumps({"theme": "dark", "telemetry": {"custom": True}}), encoding="utf-8")
                result = apply_configuration("gemini", force=False)
                self.assertTrue(result["changed"])
                value = json.loads(settings.read_text(encoding="utf-8"))
                self.assertEqual(value["theme"], "dark")
                self.assertTrue(value["telemetry"]["custom"])
                self.assertFalse(value["telemetry"]["logPrompts"])
                self.assertFalse(result["inference_proxy"])
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_codex_nested_otel_conflict_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                config = home / ".codex" / "config.toml"
                config.parent.mkdir(parents=True)
                config.write_text("[otel.exporter.existing]\nendpoint = 'http://example.invalid'\n", encoding="utf-8")
                result = apply_configuration("codex")
                self.assertEqual(result["conflicts"], ["otel table already exists"])
                self.assertIn("example.invalid", config.read_text(encoding="utf-8"))
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_codex_removal_refuses_a_user_modified_managed_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                applied = apply_configuration("codex")
                config = home / ".codex" / "config.toml"
                config.write_text(config.read_text(encoding="utf-8").replace("log_user_prompt = false", "log_user_prompt = true"), encoding="utf-8")
                result = remove_configuration("codex", managed_keys=["managed_block"], managed_hash=applied["managed_hash"])
                self.assertFalse(result["changed"])
                self.assertTrue(result["conflicts"])
                self.assertIn("log_user_prompt = true", config.read_text(encoding="utf-8"))
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_outcome_is_correlated_but_does_not_claim_causality(self) -> None:
        event = make_outcome_event("tests", "passed", correlation_id="run-1", correlation_basis="task_id", evidence_source="ci")
        self.assertEqual(event.outcome.correlation_id, "run-1")
        self.assertEqual(event.outcome.correlation_basis, "task_id")
        self.assertNotIn("caused_by", event.to_mapping())
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self.assertEqual(store.append(event).status, "inserted")
                self.assertEqual(store.outcomes()[0]["evidence_source"], "ci")
                self.assertEqual(store.outcomes()[0]["correlation_basis"], "task_id")

    def test_cli_record_outcome_offline_uses_user_state_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = io.StringIO()
            errors = io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(main(["--json", "--state-dir", temp, "install"]), 0)
                code = main([
                    "--json", "--state-dir", temp, "record-outcome", "--kind", "tests",
                    "--status", "passed", "--correlation-id", "run-2", "--offline",
                ])
            self.assertEqual(code, 0)
            self.assertTrue(Path(temp, "data", "events.sqlite3").exists())
            self.assertFalse((Path.cwd() / ".telemetry").exists())

    def test_cli_run_outcome_captures_status_without_command_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = io.StringIO()
            errors = io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(main(["--json", "--state-dir", temp, "install"]), 0)
                code = main([
                    "--json", "--state-dir", temp, "run-outcome", "--kind", "tests", "--offline", "--",
                    sys.executable, "-c", "print('RUN_OUTCOME_CANARY')",
                ])
            self.assertEqual(code, 0)
            with EventStore(Path(temp, "data", "events.sqlite3")) as store:
                event = store.list_events()[0]
                self.assertEqual(event.outcome.status, "passed")
                self.assertNotIn("RUN_OUTCOME_CANARY", event.to_json())
                self.assertEqual(event.attributes.get("command_name"), Path(sys.executable).name)
                self.assertEqual(event.attributes.get("command_arg_count"), 2)


if __name__ == "__main__":
    unittest.main()
