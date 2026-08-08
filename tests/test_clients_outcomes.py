from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
import sys
import tomllib
from unittest.mock import patch

from observatory.clients import CANONICAL_CAPABILITY_FIELDS, CLIENT_SPECS, apply_configuration, client_spec, discover_client, plan_configuration, remove_configuration
from observatory.cli import main
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore


class ClientAndOutcomeTests(unittest.TestCase):
    def test_uninstall_parser_requires_explicit_cleanup_flags(self) -> None:
        from observatory.cli import build_parser

        args = build_parser().parse_args(["uninstall", "--apply", "--delete-state", "--remove-volumes"])
        self.assertEqual(args.command, "uninstall")
        self.assertTrue(args.apply)
        self.assertTrue(args.delete_state)
        self.assertTrue(args.remove_volumes)

    def test_uninstall_can_remove_an_exact_disposable_state_directory(self) -> None:
        from observatory.cli import _command_uninstall, build_parser

        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "observatory-state"
            state.mkdir()
            (state / "config.json").write_text(json.dumps({"schema_version": "1.0", "managed_clients": {}}), encoding="utf-8")
            args = build_parser().parse_args(["--state-dir", str(state), "uninstall", "--apply", "--delete-state"])
            with patch("observatory.cli._compose", return_value=(0, "stopped")):
                result = _command_uninstall(args)
            self.assertEqual(result["outcome"], "success")
            self.assertEqual(result["data"]["state"], "deleted")
            self.assertFalse(state.exists())

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

    def test_capability_matrix_records_latency_reliability_and_proxy_boundaries(self) -> None:
        matrix = (Path(__file__).resolve().parents[1] / "docs" / "capability-matrix.yaml").read_text(encoding="utf-8")
        for client in (
            "claude-code", "codex", "gemini-cli", "cursor", "kimi", "grok",
            "openrouter-api", "jsonl", "direct-anthropic-api", "direct-openai-api",
            "direct-google-api", "direct-xai-api",
        ):
            client_marker = f"    client: {client}\n"
            start = matrix.index(client_marker)
            end = matrix.find("  - provider:", start + len(client_marker))
            block = matrix[start:] if end == -1 else matrix[start:end]
            for field in ("signals", "request_latency", "errors_retries", "inference_proxy"):
                with self.subTest(client=client, field=field):
                    self.assertIn(f"      {field}:", block)

    def test_executable_catalog_matches_every_matrix_target_and_keeps_isolation_conservative(self) -> None:
        matrix = (Path(__file__).resolve().parents[1] / "docs" / "capability-matrix.yaml").read_text(encoding="utf-8")
        expected_isolation = {
            "claude": "PARTIAL",
            "codex": "PARTIAL",
            "gemini": "PARTIAL",
            "cursor": "PARTIAL",
            "kimi": "PARTIAL",
            "grok": "PARTIAL",
            "jsonl": "SUPPORTED",
            "openrouter": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "direct-openai": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "direct-anthropic": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "direct-google": "SUPPORTED_NOT_LOCALLY_VERIFIED",
            "direct-xai": "SUPPORTED_NOT_LOCALLY_VERIFIED",
        }
        self.assertEqual(set(expected_isolation), set(CLIENT_SPECS))
        for name, spec in CLIENT_SPECS.items():
            with self.subTest(client=name):
                self.assertIn(f"    client: {spec.name}\n", matrix)
                capabilities = spec.capabilities_record().capabilities
                self.assertEqual(capabilities["zero_repository_contamination"], expected_isolation[name])

    def test_executable_catalog_matches_every_canonical_matrix_field(self) -> None:
        matrix = (Path(__file__).resolve().parents[1] / "docs" / "capability-matrix.yaml").read_text(encoding="utf-8")
        for name, spec in CLIENT_SPECS.items():
            with self.subTest(client=name):
                marker = f"    client: {spec.name}\n"
                start = matrix.index(marker)
                end = matrix.find("  - provider:", start + len(marker))
                block = matrix[start:] if end == -1 else matrix[start:end]
                actual = spec.capabilities_record().capabilities
                for field in CANONICAL_CAPABILITY_FIELDS:
                    match = re.search(rf"^      {re.escape(field)}:\s*(.+)$", block, re.MULTILINE)
                    self.assertIsNotNone(match, f"matrix is missing {field} for {spec.name}")
                    self.assertEqual(actual[field], match.group(1).strip().strip('"'), field)

    def test_executable_catalog_matches_matrix_auth_modes(self) -> None:
        matrix = (Path(__file__).resolve().parents[1] / "docs" / "capability-matrix.yaml").read_text(encoding="utf-8")
        for name, spec in CLIENT_SPECS.items():
            with self.subTest(client=name):
                marker = f"    client: {spec.name}\n"
                start = matrix.index(marker)
                end = matrix.find("  - provider:", start + len(marker))
                block = matrix[start:] if end == -1 else matrix[start:end]
                match = re.search(r"^    auth_modes:\s*(.+)$", block, re.MULTILINE)
                self.assertIsNotNone(match, f"matrix is missing auth_modes for {spec.name}")
                actual = tuple(item.strip() for item in match.group(1).strip().strip('"').split(",") if item.strip())
                self.assertEqual(actual, spec.auth_modes)

    def test_configure_all_is_derived_from_the_executable_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            def run_cli(arguments):
                output = io.StringIO()
                errors = io.StringIO()
                with redirect_stdout(output), redirect_stderr(errors):
                    code = main(["--json", *arguments])
                return code, json.loads(output.getvalue())

            run_cli(["--state-dir", temp, "install"])
            code, result = run_cli(["--state-dir", temp, "configure", "all"])
            self.assertEqual(code, 5)
            self.assertEqual(set(result["data"]["clients"]), set(CLIENT_SPECS))

    def test_configuration_plan_is_explicit_and_contains_no_credentials(self) -> None:
        plan = plan_configuration("claude")
        self.assertTrue(plan["supported"])
        self.assertFalse(plan["inference_proxy"])
        self.assertEqual(plan["capabilities"]["contract_version"], "1")
        self.assertIn("hooks_or_events", CANONICAL_CAPABILITY_FIELDS)
        self.assertTrue(set(CANONICAL_CAPABILITY_FIELDS).issubset(plan["capabilities"]["capabilities"]))
        encoded = json.dumps(plan, sort_keys=True).casefold()
        self.assertNotIn("authorization", encoded)
        self.assertNotIn("api_key", encoded)
        self.assertNotIn("bearer", encoded)

    @patch("observatory.clients.subprocess.run")
    @patch("observatory.clients.shutil.which", return_value="C:\\Tools\\claude.exe")
    def test_discovery_reports_bounded_safe_version_probe(self, _which, run) -> None:
        run.return_value = type("Completed", (), {"returncode": 0, "stdout": "claude 2.1.224\n", "stderr": ""})()
        result = discover_client("claude")
        self.assertTrue(result["installed"])
        self.assertEqual(result["version"], "claude 2.1.224")
        self.assertEqual(result["version_probe_status"], "verified")
        run.assert_called_once_with(
            ["C:\\Tools\\claude.exe", "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
            shell=False,
        )

    @patch("observatory.clients.subprocess.run")
    @patch("observatory.clients.shutil.which", return_value="C:\\Tools\\codex.exe")
    def test_discovery_preserves_blocked_version_probe_as_distinct_from_not_installed(self, _which, run) -> None:
        run.side_effect = PermissionError("access denied")
        result = discover_client("codex")
        self.assertTrue(result["installed"])
        self.assertIsNone(result["version"])
        self.assertEqual(result["version_probe_status"], "blocked")
        self.assertEqual(result["capabilities"]["capabilities"]["installed"], "INSTALLED_NOT_VERIFIED")

    @patch("observatory.clients.shutil.which", return_value=None)
    def test_discovery_does_not_probe_missing_client(self, _which) -> None:
        result = discover_client("gemini")
        self.assertFalse(result["installed"])
        self.assertIsNone(result["version"])
        self.assertEqual(result["version_probe_status"], "not_installed")

    def test_codex_trace_setting_stays_in_the_root_otel_table(self) -> None:
        block = plan_configuration("codex", enable_traces=True)["changes"]["toml_block"]
        self.assertIn('[otel]\n', block)
        parsed = tomllib.loads(block)
        self.assertEqual(parsed["otel"]["exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/logs")
        self.assertEqual(parsed["otel"]["metrics_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/metrics")
        self.assertEqual(parsed["otel"]["trace_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:4318/v1/traces")
        self.assertEqual(tomllib.loads(plan_configuration("codex")["changes"]["toml_block"])["otel"]["trace_exporter"], "none")

    def test_acceptance_endpoint_overrides_are_credential_free_and_scoped_to_telemetry(self) -> None:
        with patch.dict(
            __import__("os").environ,
            {
                "OBSERVATORY_OTLP_GRPC_ENDPOINT": "http://127.0.0.1:15431",
                "OBSERVATORY_OTLP_HTTP_ENDPOINT": "http://127.0.0.1:15432",
            },
            clear=False,
        ):
            claude = plan_configuration("claude")["changes"]["env"]
            gemini = plan_configuration("gemini")["changes"]["telemetry"]
            codex = tomllib.loads(plan_configuration("codex", enable_traces=True)["changes"]["toml_block"])["otel"]
        self.assertEqual(claude["OTEL_EXPORTER_OTLP_ENDPOINT"], "http://127.0.0.1:15431")
        self.assertEqual(gemini["otlpEndpoint"], "http://127.0.0.1:15431")
        self.assertEqual(codex["exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:15432/v1/logs")
        self.assertEqual(codex["metrics_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:15432/v1/metrics")
        self.assertEqual(codex["trace_exporter"]["otlp-http"]["endpoint"], "http://127.0.0.1:15432/v1/traces")

    def test_acceptance_endpoint_override_rejects_embedded_credentials(self) -> None:
        with patch.dict(__import__("os").environ, {"OBSERVATORY_OTLP_GRPC_ENDPOINT": "http://user:pass@127.0.0.1:15431"}, clear=False):
            with self.assertRaises(ValueError):
                plan_configuration("claude")

    def test_gemini_trace_flag_is_not_silently_ignored(self) -> None:
        self.assertFalse(plan_configuration("gemini", enable_traces=False)["changes"]["telemetry"]["traces"])
        self.assertTrue(plan_configuration("gemini", enable_traces=True)["changes"]["telemetry"]["traces"])

    def test_kimi_and_grok_global_hook_blocks_are_valid_and_reversible(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            environment = __import__("os").environ
            old_userprofile = environment.get("USERPROFILE")
            old_codex_home = environment.get("CODEX_HOME")
            environment["USERPROFILE"] = str(home)
            environment.pop("CODEX_HOME", None)
            try:
                with patch("observatory.clients.shutil.which", return_value=None):
                    kimi = apply_configuration("kimi")
                    grok = apply_configuration("grok")
                kimi_path = home / ".kimi-code" / "config.toml"
                grok_path = home / ".grok" / "config.toml"
                self.assertEqual(kimi["mode"], "global-hook")
                self.assertEqual(grok["mode"], "global-hook")
                self.assertEqual(tomllib.loads(kimi_path.read_text(encoding="utf-8"))["hooks"][0]["event"], "Notification")
                self.assertEqual(tomllib.loads(grok_path.read_text(encoding="utf-8"))["hooks"]["SessionStart"][0]["hooks"][0]["type"], "command")

                kimi_removed = remove_configuration("kimi", managed_keys=kimi["managed_keys"], managed_hash=kimi["managed_hash"])
                grok_removed = remove_configuration("grok", managed_keys=grok["managed_keys"], managed_hash=grok["managed_hash"])
                self.assertTrue(kimi_removed["removed"])
                self.assertTrue(grok_removed["removed"])
                self.assertEqual(kimi_path.read_text(encoding="utf-8").strip(), "")
                self.assertEqual(grok_path.read_text(encoding="utf-8").strip(), "")
            finally:
                if old_userprofile is None:
                    environment.pop("USERPROFILE", None)
                else:
                    environment["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    environment.pop("CODEX_HOME", None)
                else:
                    environment["CODEX_HOME"] = old_codex_home

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

    def test_json_client_removal_restores_owned_settings_without_deleting_preexisting_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                settings = home / ".gemini" / "settings.json"
                settings.parent.mkdir(parents=True)
                settings.write_text(json.dumps({"telemetry": {"logPrompts": False, "custom": True}}), encoding="utf-8")
                applied = apply_configuration("gemini")
                removed = remove_configuration(
                    "gemini",
                    managed_keys=applied["managed_keys"],
                    managed_state=applied["managed_state"],
                )
                self.assertTrue(removed["changed"])
                value = json.loads(settings.read_text(encoding="utf-8"))
                self.assertEqual(value["telemetry"], {"logPrompts": False, "custom": True})
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_json_client_removal_refuses_user_modified_owned_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                applied = apply_configuration("gemini")
                settings = home / ".gemini" / "settings.json"
                value = json.loads(settings.read_text(encoding="utf-8"))
                value["telemetry"]["logPrompts"] = True
                settings.write_text(json.dumps(value), encoding="utf-8")
                removed = remove_configuration(
                    "gemini",
                    managed_keys=applied["managed_keys"],
                    managed_state=applied["managed_state"],
                )
                self.assertFalse(removed["changed"])
                self.assertIn("logPrompts", removed["conflicts"])
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_force_apply_reports_reviewed_non_secret_overwrite_as_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            environment = __import__("os").environ
            old_userprofile = environment.get("USERPROFILE")
            old_codex_home = environment.get("CODEX_HOME")
            environment["USERPROFILE"] = str(home)
            environment.pop("CODEX_HOME", None)
            try:
                settings = home / ".gemini" / "settings.json"
                settings.parent.mkdir(parents=True)
                settings.write_text(json.dumps({"telemetry": {"traces": False}}), encoding="utf-8")
                result = apply_configuration("gemini", enable_traces=True, force=True)
                self.assertTrue(result["applied"])
                self.assertEqual(result["conflicts"], [])
                self.assertEqual(result["overwritten"], ["traces"])
                self.assertTrue(json.loads(settings.read_text(encoding="utf-8"))["telemetry"]["traces"])
            finally:
                if old_userprofile is None:
                    environment.pop("USERPROFILE", None)
                else:
                    environment["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    environment.pop("CODEX_HOME", None)
                else:
                    environment["CODEX_HOME"] = old_codex_home

    def test_force_does_not_persist_embedded_credentials_from_existing_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                settings = home / ".claude" / "settings.json"
                settings.parent.mkdir(parents=True)
                settings.write_text(json.dumps({"env": {"OTEL_EXPORTER_OTLP_ENDPOINT": "https://user:secret@example.invalid/otlp"}}), encoding="utf-8")
                result = apply_configuration("claude", force=True)
                self.assertFalse(result["changed"])
                self.assertTrue(result["conflicts"])
                self.assertIn("secret", settings.read_text(encoding="utf-8"))
                self.assertNotIn("secret", json.dumps(result, sort_keys=True))
            finally:
                if old_userprofile is None:
                    __import__("os").environ.pop("USERPROFILE", None)
                else:
                    __import__("os").environ["USERPROFILE"] = old_userprofile
                if old_codex_home is None:
                    __import__("os").environ.pop("CODEX_HOME", None)
                else:
                    __import__("os").environ["CODEX_HOME"] = old_codex_home

    def test_json_client_reconfiguration_keeps_trace_keys_reversible(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                first = apply_configuration("claude", enable_traces=True)
                second = apply_configuration("claude", enable_traces=False, managed_state=first["managed_state"])
                self.assertIn("OTEL_TRACES_EXPORTER", second["managed_keys"])
                removed = remove_configuration(
                    "claude",
                    managed_keys=second["managed_keys"],
                    managed_state=second["managed_state"],
                )
                self.assertTrue(removed["changed"])
                settings = json.loads((home / ".claude" / "settings.json").read_text(encoding="utf-8"))
                self.assertNotIn("env", settings)
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

    def test_codex_force_does_not_duplicate_an_existing_otel_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            old_userprofile = __import__("os").environ.get("USERPROFILE")
            old_codex_home = __import__("os").environ.get("CODEX_HOME")
            __import__("os").environ["USERPROFILE"] = str(home)
            __import__("os").environ.pop("CODEX_HOME", None)
            try:
                config = home / ".codex" / "config.toml"
                config.parent.mkdir(parents=True)
                config.write_text("[otel]\nexporter = 'existing'\n", encoding="utf-8")
                result = apply_configuration("codex", force=True)
                self.assertEqual(result["conflicts"], ["otel table already exists"])
                self.assertEqual(config.read_text(encoding="utf-8").count("[otel]"), 1)
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

    def test_command_timeout_is_visible_in_reliability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            from observatory.outcomes import run_command_outcome
            result = run_command_outcome([sys.executable, "-c", "import time; time.sleep(1)"], project_path=temp, timeout_seconds=0.01)
            self.assertEqual(result.event.outcome.status, "timeout")
            self.assertTrue(result.event.reliability.timeout)
            with EventStore(Path(temp, "events.sqlite3")) as store:
                store.append(result.event)
                self.assertEqual(store.summary()["timeouts"], 1)

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
