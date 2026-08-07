from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from observatory.cli import main

from tests.test_contracts import event_mapping


class CliTests(unittest.TestCase):
    def run_cli(self, arguments: list[str]) -> tuple[int, dict]:
        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["--json", *arguments])
        return code, json.loads(output.getvalue())

    def test_install_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            args = ["--state-dir", temp, "install"]
            first_code, first = self.run_cli(args)
            second_code, second = self.run_cli(args)
            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertTrue(first["data"]["changed"])
            self.assertFalse(second["data"]["changed"])
            self.assertIn("005_task_dimensions", first["data"]["schema_versions"])
            self.assertTrue(Path(temp, "config.json").exists())
            self.assertTrue(Path(temp, "data", "events.sqlite3").exists())
            self.assertTrue(Path(temp, "compose.env").exists())
            self.assertTrue(Path(temp, "secrets", "grafana_admin_password").exists())
            compose_env = Path(temp, "compose.env").read_text(encoding="utf-8")
            self.assertIn("OBSERVATORY_STATE_DIR=", compose_env)
            self.assertIn("OBSERVATORY_SECRET_FILE=", compose_env)

    def test_doctor_reports_engine_degradation_without_claiming_inference_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "doctor", "--compose-file", str(Path(__file__).resolve().parents[1] / "compose.yaml")])
            self.assertIn(code, (0, 5))
            self.assertIn(result["outcome"], ("success", "degraded"))
            self.assertTrue(any(check["id"] == "docker.engine" for check in result["data"]["checks"]))
            self.assertTrue(any(check["id"] == "state.compose_env_alignment" and check["status"] == "pass" for check in result["data"]["checks"]))

    def test_offline_ingest_redacts_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "input.jsonl"
            value = event_mapping()
            value["prompt"] = "CLI_CANARY"
            fixture.write_text(json.dumps(value) + "\n", encoding="utf-8")
            code, result = self.run_cli(["--state-dir", temp, "ingest", "--file", str(fixture), "--offline"])
            self.assertEqual(code, 0)
            self.assertEqual(result["data"]["inserted"], 1)
            database = Path(temp, "data", "events.sqlite3")
            self.assertTrue(database.exists())
            import sqlite3
            connection = sqlite3.connect(database)
            try:
                payload = connection.execute("SELECT payload_json FROM events").fetchone()[0]
            finally:
                connection.close()
            self.assertNotIn("CLI_CANARY", payload)

    def test_configure_is_explicitly_partial_and_never_proxy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "configure", "codex"])
            self.assertEqual(code, 5)
            self.assertFalse(result["data"]["inference_proxy"])

    def test_open_print_url_is_machine_testable(self) -> None:
        code, result = self.run_cli(["open", "--print-url"])
        self.assertEqual(code, 0)
        self.assertIn("http://127.0.0.1:3000", result["data"]["url"])

    def test_status_separates_api_and_dashboard_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "status", "--url", "http://127.0.0.1:1/healthz", "--grafana-url", "http://127.0.0.1:1/api/health"])
            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["observatory"], "unavailable")
            self.assertEqual(result["data"]["dashboard"]["status"], "unavailable")
            self.assertEqual(result["data"]["collector"]["status"], "unavailable")

    def test_update_check_is_read_only_and_reports_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "update", "--check"])
            self.assertEqual(code, 0)
            self.assertTrue(result["data"]["check"])
            self.assertEqual(result["data"]["image_pull"], "not_requested")
            self.assertIn("001_initial", result["data"]["schema_versions"])

    def test_retention_updates_state_and_compose_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "retention", "--prometheus-days", "45", "--tempo-hours", "360", "--loki-hours", "168"])
            self.assertEqual(code, 0)
            self.assertTrue(result["data"]["changed"])
            config = json.loads(Path(temp, "config.json").read_text(encoding="utf-8"))
            self.assertEqual(config["retention"]["prometheus_days"], 45)
            compose_env = Path(temp, "compose.env").read_text(encoding="utf-8")
            self.assertIn("PROMETHEUS_RETENTION_TIME=45d", compose_env)
            self.assertIn("TEMPO_RETENTION=360h", compose_env)
            self.assertIn("LOKI_RETENTION=168h", compose_env)

    def test_update_pull_backups_before_compose_and_requires_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            with patch("observatory.cli._compose", side_effect=[(0, "pulled"), (0, "started")]) as compose, patch("observatory.cli._wait_http", return_value=(True, "HTTP 200")):
                code, result = self.run_cli(["--state-dir", temp, "--timeout", "1", "update", "--pull"])
            self.assertEqual(code, 0)
            self.assertEqual(result["data"]["readiness"], "HTTP 200")
            self.assertTrue(Path(result["data"]["backup"]["target"]).exists())
            self.assertEqual(compose.call_count, 2)

    def test_doctor_reports_read_only_integrity_and_service_checks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "doctor", "--compose-file", str(Path(__file__).resolve().parents[1] / "compose.yaml")])
            self.assertIn(code, (0, 5))
            checks = {check["id"]: check for check in result["data"]["checks"]}
            self.assertEqual(checks["state.database_integrity"]["status"], "pass")
            self.assertIn(checks["service.api"]["status"], ("pass", "warn"))
            self.assertIn(checks["service.grafana"]["status"], ("pass", "warn"))


if __name__ == "__main__":
    unittest.main()
