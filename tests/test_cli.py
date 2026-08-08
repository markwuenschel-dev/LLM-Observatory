from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from observatory.cli import DEFAULT_OPERATION_TIMEOUT, _command_start, _inspect_live_compose_state, _snapshot_compose_images, build_parser, main

from tests.test_contracts import event_mapping


class CliTests(unittest.TestCase):
    def run_cli(self, arguments: list[str]) -> tuple[int, dict]:
        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["--json", *arguments])
        return code, json.loads(output.getvalue())

    def test_start_reuses_local_images_without_forcing_a_rebuild(self) -> None:
        args = build_parser().parse_args(["--state-dir", tempfile.gettempdir(), "start"])
        with patch("observatory.cli._compose", return_value=(0, "started")) as compose, patch(
            "observatory.cli._wait_http", return_value=(True, "HTTP 200")
        ):
            result = _command_start(args)
        self.assertEqual(result["outcome"], "success")
        compose.assert_called_once_with(args, "up -d --wait")

    def test_start_refuses_an_already_over_budget_backend(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            args = build_parser().parse_args(["--state-dir", temp, "start"])
            over_budget = {"status": "fail", "bytes": 11, "max_bytes": 10, "ratio": 1.1}
            with patch("observatory.cli._start_backend_capacity", return_value=over_budget), patch("observatory.cli._compose") as compose:
                result = _command_start(args)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["exit_code"], 5)
            self.assertEqual(result["data"]["capacity"], over_budget)
            compose.assert_not_called()

    def test_lifecycle_timeout_default_covers_cold_compose_start(self) -> None:
        args = build_parser().parse_args(["start"])
        self.assertEqual(args.timeout, DEFAULT_OPERATION_TIMEOUT)
        self.assertGreaterEqual(args.timeout, 120.0)

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
            self.assertNotIn(b"\r\n", Path(temp, "secrets", "grafana_admin_password").read_bytes())
            self.assertTrue(Path(temp, "secrets", "grafana_admin_password").read_bytes().endswith(b"\n"))
            self.assertIn("compose_file", json.loads(Path(temp, "config.json").read_text(encoding="utf-8")))
            compose_env = Path(temp, "compose.env").read_text(encoding="utf-8")
            self.assertIn("OBSERVATORY_STATE_DIR=", compose_env)
            self.assertIn("OBSERVATORY_SECRET_FILE=", compose_env)
            self.assertIn("OBSERVATORY_MAX_BACKEND_VOLUME_BYTES=", compose_env)

    def test_demo_seed_is_idempotent_and_populates_the_walkthrough(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            first_code, first = self.run_cli(["--state-dir", temp, "demo"])
            second_code, second = self.run_cli(["--state-dir", temp, "demo"])
            self.assertEqual(first_code, 0)
            self.assertEqual(second_code, 0)
            self.assertEqual(first["data"]["inserted"], 6)
            self.assertEqual(second["data"]["duplicate"], 6)
            self.assertTrue(first["data"]["demo"])

    def test_install_demo_seeds_without_changing_the_clean_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            code, result = self.run_cli(["--state-dir", temp, "install", "--demo"])
            self.assertEqual(code, 0)
            self.assertEqual(result["data"]["demo"]["inserted"], 6)

    def test_non_loopback_api_bind_requires_explicit_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            code, result = self.run_cli(["--state-dir", temp, "run-api", "--host", "0.0.0.0"])
            self.assertEqual(code, 2)
            self.assertEqual(result["outcome"], "failed")
            self.assertIn("allow-remote", result["errors"][0])

    def test_non_loopback_api_bind_requires_authentication_or_explicit_private_network_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            code, result = self.run_cli(["--state-dir", temp, "run-api", "--host", "0.0.0.0", "--allow-remote"])
            self.assertEqual(code, 2)
            self.assertIn("auth-token-file", result["errors"][0])

    def test_doctor_reports_engine_degradation_without_claiming_inference_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            code, result = self.run_cli(["--state-dir", temp, "doctor", "--compose-file", str(Path(__file__).resolve().parents[1] / "compose.yaml")])
            self.assertIn(code, (0, 5))
            self.assertIn(result["outcome"], ("success", "degraded"))
            self.assertTrue(any(check["id"] == "docker.engine" for check in result["data"]["checks"]))
            self.assertTrue(any(check["id"] == "clients.capability_catalog" for check in result["data"]["checks"]))
            self.assertTrue(any(check["id"] == "state.database_capacity" for check in result["data"]["checks"]))
            self.assertTrue(any(check["id"] == "backend.volume_capacity" for check in result["data"]["checks"]))
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

    def test_offline_ingest_keeps_valid_jsonl_siblings_and_auto_attributes_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "input.jsonl"
            first = event_mapping()
            first["event_id"] = "cli-sibling-1"
            second = event_mapping()
            second["event_id"] = "cli-sibling-2"
            fixture.write_text(
                json.dumps(first) + "\nnot-json\n" + json.dumps(second) + "\n",
                encoding="utf-8",
            )
            code, result = self.run_cli(["--state-dir", temp, "ingest", "--file", str(fixture), "--offline"])
            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["inserted"], 2)
            self.assertTrue(any("invalid JSON" in item for item in result["errors"]))
            import sqlite3
            connection = sqlite3.connect(Path(temp, "data", "events.sqlite3"))
            try:
                projects = {row[0] for row in connection.execute("SELECT project_id FROM events")}
            finally:
                connection.close()
            self.assertEqual(len(projects), 1)
            self.assertNotIn("project:unknown", projects)

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
            code, result = self.run_cli(["--state-dir", temp, "status", "--url", "http://127.0.0.1:1/healthz", "--grafana-url", "http://127.0.0.1:1/api/health", "--collector-url", "http://127.0.0.1:1/"])
            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["observatory"], "unavailable")
            self.assertEqual(result["data"]["dashboard"]["status"], "unavailable")
            self.assertEqual(result["data"]["collector"]["status"], "unavailable")

    def test_status_probes_live_surfaces_when_local_state_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            code, result = self.run_cli(["--state-dir", temp, "status", "--url", "http://127.0.0.1:1/healthz", "--grafana-url", "http://127.0.0.1:1/api/health", "--collector-url", "http://127.0.0.1:1/"])
            self.assertEqual(code, 5)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["data"]["state"]["status"], "missing")
            self.assertIn("local Observatory state is missing", " ".join(result["warnings"]))

    def test_status_reports_stale_live_compose_state_without_mutating_it(self) -> None:
        stale = {
            "project": "llm-observatory",
            "status": "present",
            "stale": True,
            "stale_reasons": ["environment_file_missing", "state_bind_mismatch"],
            "services": [{"name": "observatory-api-1", "service": "observatory-api", "state": "restarting"}],
            "environment_files": [],
            "working_directories": [],
            "binds": [],
        }
        with tempfile.TemporaryDirectory() as temp, patch("observatory.cli._inspect_live_compose_state", return_value=stale):
            code, result = self.run_cli(["--state-dir", temp, "status", "--url", "http://127.0.0.1:1/healthz", "--grafana-url", "http://127.0.0.1:1/api/health", "--collector-url", "http://127.0.0.1:1/"])
        self.assertEqual(code, 5)
        self.assertEqual(result["data"]["live_compose"]["stale"], True)
        self.assertIn("stale generated state", " ".join(result["warnings"]))

    def test_doctor_reports_stale_live_compose_state_when_local_state_is_missing(self) -> None:
        stale = {
            "project": "llm-observatory",
            "status": "present",
            "stale": True,
            "stale_reasons": ["environment_file_missing"],
            "services": [],
            "environment_files": [],
            "working_directories": [],
            "binds": [],
        }
        with tempfile.TemporaryDirectory() as temp, patch("observatory.cli._inspect_live_compose_state", return_value=stale):
            code, result = self.run_cli(["--state-dir", temp, "doctor"])
        self.assertEqual(code, 3)
        self.assertEqual(result["outcome"], "not_initialized")
        self.assertEqual(result["data"]["live_compose"]["stale"], True)
        self.assertTrue(any(check["id"] == "compose.live_state" for check in result["data"]["checks"]))
        self.assertIn("stale generated state", " ".join(result["warnings"]))

    def test_live_compose_inspection_detects_missing_environment_and_bind_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            stale_root = Path(temp) / "old-state"
            listing = "api-1|restarting|" + str(stale_root / "compose.env") + "|C:/repo|observatory-api\n"
            mounts = json.dumps([{"Destination": "/var/lib/observatory", "Source": str(stale_root)}]) + "\n"
            with patch(
                "observatory.cli.subprocess.run",
                side_effect=[
                    SimpleNamespace(returncode=0, stdout=listing, stderr=""),
                    SimpleNamespace(returncode=0, stdout=mounts, stderr=""),
                ],
            ):
                result = _inspect_live_compose_state(Path(temp))
        self.assertEqual(result["status"], "present")
        self.assertTrue(result["stale"])
        self.assertEqual(result["stale_reasons"], ["environment_file_missing", "environment_file_mismatch", "state_bind_mismatch"])
        self.assertEqual(result["binds"][0]["destination"], "/var/lib/observatory")
        self.assertFalse(result["binds"][0]["matches_state"])

    def test_resolve_project_cli_returns_machine_readable_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            code, result = self.run_cli(["resolve-project", temp])
            self.assertEqual(code, 0)
            self.assertEqual(result["command"], "resolve-project")
            self.assertIn("project_id", result["data"])

    def test_restore_refuses_to_overwrite_a_live_api_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            with patch("observatory.cli._probe_http", return_value=(True, "HTTP 200")):
                code, result = self.run_cli(["--state-dir", temp, "restore", str(Path(temp, "data", "events.sqlite3")), "--overwrite"])
            self.assertEqual(code, 6)
            self.assertEqual(result["outcome"], "conflict")

    def test_backend_volume_backup_and_restore_are_explicit_full_state_operations(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            target = str(Path(temp).parent / "backend-state.zip")
            code, result = self.run_cli(["--state-dir", temp, "backup", target, "--backend-volumes"])
            self.assertEqual(code, 2)
            self.assertIn("requires --full-state", result["errors"][0])
            code, result = self.run_cli(["--state-dir", temp, "backup", target, "--full-state", "--backend-volumes"])
            self.assertEqual(code, 2)
            self.assertIn("requires --include-secret", result["errors"][0])
            code, result = self.run_cli(["--state-dir", temp, "restore", target, "--backend-volumes"])
            self.assertEqual(code, 2)
            self.assertIn("requires --full-state", result["errors"][0])

    def test_full_state_restore_rebases_target_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source-state"
            target_state = root / "target-state"
            source_compose = root / "source-compose.yaml"
            target_compose = root / "target-compose.yaml"
            source_compose.write_text("name: source\n", encoding="utf-8")
            target_compose.write_text("name: target\n", encoding="utf-8")
            self.run_cli(["--state-dir", str(source), "install", "--compose-file", str(source_compose)])
            archive = root / "portable-state.zip"
            code, result = self.run_cli(["--state-dir", str(source), "backup", str(archive), "--full-state"])
            self.assertEqual(code, 0)
            self.assertEqual(result["outcome"], "success")
            self.run_cli(["--state-dir", str(target_state), "install", "--compose-file", str(target_compose)])
            code, result = self.run_cli([
                "--state-dir", str(target_state), "restore", str(archive), "--full-state", "--overwrite",
                "--compose-file", str(target_compose), "--api-health-url", "http://127.0.0.1:1/healthz",
            ])
            self.assertEqual(code, 0)
            config = json.loads((target_state / "config.json").read_text(encoding="utf-8"))
            compose_env = (target_state / "compose.env").read_text(encoding="utf-8")
            self.assertEqual(Path(config["compose_file"]).resolve(), target_compose.resolve())
            self.assertIn(f"OBSERVATORY_STATE_DIR={target_state.resolve().as_posix()}", compose_env)
            self.assertIn(f"OBSERVATORY_SECRET_FILE={(target_state / 'secrets' / 'grafana_admin_password').resolve().as_posix()}", compose_env)

    def test_update_check_is_read_only_and_reports_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            database = Path(temp, "data", "events.sqlite3")
            before = (database.stat().st_size, database.stat().st_mtime_ns)
            code, result = self.run_cli(["--state-dir", temp, "update", "--check"])
            self.assertEqual(code, 0)
            self.assertTrue(result["data"]["check"])
            self.assertEqual(result["data"]["image_pull"], "not_requested")
            self.assertIn("001_initial", result["data"]["schema_versions"])
            self.assertEqual((database.stat().st_size, database.stat().st_mtime_ns), before)

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
            with patch("observatory.cli._snapshot_compose_images", return_value=[{"id": "sha256:" + "a" * 64, "reference": "example/service:1"}]), patch("observatory.cli._compose", side_effect=[(0, "pulled"), (0, "started")]) as compose, patch("observatory.cli._wait_http", return_value=(True, "HTTP 200")):
                code, result = self.run_cli(["--state-dir", temp, "--timeout", "1", "update", "--pull"])
            self.assertEqual(code, 0)
            self.assertEqual(result["data"]["readiness"], "HTTP 200")
            self.assertTrue(Path(result["data"]["backup"]["target"]).exists())
            self.assertEqual(result["data"]["rollback"]["status"], "available")
            self.assertEqual(compose.call_count, 2)

    def test_update_failure_attempts_image_and_database_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            snapshot = [{"id": "sha256:" + "b" * 64, "reference": "example/service:1"}]
            with patch("observatory.cli._snapshot_compose_images", return_value=snapshot), patch(
                "observatory.cli._compose",
                side_effect=[(0, "pulled"), (5, "new stack failed"), (0, "rolled down"), (0, "rolled up")],
            ) as compose, patch("observatory.cli._restore_compose_images", return_value={"status": "success", "restored": ["example/service:1"]}), patch(
                "observatory.cli._restore_update_database", return_value={"status": "success", "integrity": "ok"},
            ), patch("observatory.cli._wait_http", return_value=(True, "HTTP 200")):
                code, result = self.run_cli(["--state-dir", temp, "--timeout", "1", "update", "--pull"])
            self.assertEqual(code, 5)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["data"]["rollback"]["status"], "success")
            self.assertEqual(compose.call_count, 4)

    def test_update_pull_failure_attempts_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            snapshot = [{"id": "sha256:" + "c" * 64, "reference": "example/service:1"}]
            with patch("observatory.cli._snapshot_compose_images", return_value=snapshot), patch(
                "observatory.cli._compose",
                side_effect=[(5, "partial pull failed"), (0, "rolled down"), (0, "rolled up")],
            ) as compose, patch(
                "observatory.cli._restore_compose_images",
                return_value={"status": "success", "restored": ["example/service:1"]},
            ), patch("observatory.cli._restore_update_database", return_value={"status": "success", "integrity": "ok"}), patch(
                "observatory.cli._wait_http", return_value=(True, "HTTP 200")
            ):
                code, result = self.run_cli(["--state-dir", temp, "--timeout", "1", "update", "--pull"])
            self.assertEqual(code, 5)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["data"]["image_pull"], "failed")
            self.assertEqual(result["data"]["rollback"]["status"], "success")
            self.assertEqual(compose.call_count, 3)

    def test_update_image_snapshot_accepts_compose_json_and_deduplicates_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            args = build_parser().parse_args(["--state-dir", temp, "--timeout", "1", "update", "--pull"])
            payload = json.dumps([
                {"ID": "sha256:" + "c" * 64, "Repository": "example/api", "Tag": "1"},
                {"ID": "sha256:" + "c" * 64, "Repository": "example/api", "Tag": "1"},
                {"ID": "sha256:" + "d" * 64, "Repository": "example/worker", "Tag": "<none>"},
            ])
            with patch("observatory.cli.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout=payload, stderr="")):
                snapshot = _snapshot_compose_images(args)
            self.assertEqual(snapshot, [
                {"id": "sha256:" + "c" * 64, "reference": "example/api:1"},
                {"id": "sha256:" + "d" * 64, "reference": "example/worker"},
            ])

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
