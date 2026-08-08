from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import re
import shutil
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DeploymentTests(unittest.TestCase):
    def test_static_verifier_covers_the_latest_migration(self) -> None:
        verifier = (ROOT / "scripts" / "verify.py").read_text(encoding="utf-8")
        self.assertIn("src/observatory/migrations/012_parent_agent.sql", verifier)

    def test_required_deployment_surfaces_exist(self) -> None:
        for relative in (
            "compose.yaml",
            "Dockerfile",
            "deployment/otel-collector/config.yaml",
            "deployment/tempo/tempo.yaml",
            "deployment/loki/loki.yaml",
            "deployment/prometheus/prometheus.yml",
            "deployment/grafana/provisioning/datasources/datasources.yaml",
            "deployment/grafana/provisioning/dashboards/dashboards.yaml",
            "dashboards/global-observatory.json",
        ):
            with self.subTest(relative=relative):
                self.assertTrue((ROOT / relative).exists())

    def test_runtime_acceptance_harness_is_reproducible_and_fail_closed(self) -> None:
        script = (ROOT / "scripts" / "runtime-acceptance.ps1").read_text(encoding="utf-8")
        for marker in (
            "observatory.runtime-acceptance/v1",
            "docker info",
            "install",
            "start",
            "synthetic-events.jsonl",
            "synthetic_agent_behavior",
            "OtlpHttpBase/v1/traces",
            "Send-ClaudeShapedOtlpLog",
            "claude_plain_key_mapping",
            "runtime-claude-model",
            "Collector-to-normalizer trace delivery",
            "collector_privacy_boundary",
            "restart_recovery",
            "grafana_metrics",
            "event_time_query_range",
            "grafana_failure_isolation",
            "collector_failure_isolation",
            "api_failure_isolation",
            "storage_failure_isolation",
            "malformed_telemetry",
            "Invoke-InferenceSentinel",
            "v1/chat/completions",
            "Send-SyntheticOtlpMetric",
            "runtime_privacy_metric",
            "api/traces/fedcba9876543210fedcba9876543210",
            "full_disaster_recovery",
            "backend-volumes",
            "v1/logs",
            "dashboard_query_probe",
            "dashboard_filter_probe",
            "collector_self_observability",
            "otelcol_process_uptime",
            "session_trace_query",
            "Invoke-GrafanaTempoSessionProbe",
            "runtime-session",
            "ProjectWorkingDirectoryOnly",
            "project_attribution_working_directory",
            "api/v1/query",
            "query_range",
            "/api/search?q=",
            "result_count",
            "data-bearing target",
            "--remove-orphans",
            "--volumes",
            "KeepVolumes",
            "api_build",
            "OBSERVATORY_API_IMAGE",
            "llm-observatory-api:acceptance-",
            "docker image ls --quiet",
        ):
            self.assertIn(marker, script)

    def test_provider_acceptance_harness_requires_explicit_client_command_and_is_privacy_checked(self) -> None:
        script = (ROOT / "scripts" / "provider-acceptance.ps1").read_text(encoding="utf-8")
        for marker in (
            "observatory.provider-acceptance/v1",
            "ClientCommand",
            "ApplyConfiguration",
            "inference_proxy",
            "repository_contamination",
            "privacyViolations",
            "Invoke-ExplicitClient",
            "COMPOSE_PROJECT_NAME",
            "WorkingDirectory",
            "Get-FileHash",
            "expectedProvider",
            "ExpectedSourceName",
            "configurationConflicts",
            "configuration_cleanup",
            "Get-FreeLoopbackPort",
            "compose.acceptance.yaml",
            "OBSERVATORY_OTLP_GRPC_ENDPOINT",
            "OBSERVATORY_OTLP_HTTP_ENDPOINT",
            "OTEL_RESOURCE_ATTRIBUTES",
            "llm.observatory.acceptance.run_id",
            "--volumes",
            "Write-RuntimeCompose",
            "Stop-IsolatedCompose",
            "port_retry_limit",
            "port is already allocated",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, script)

    def test_queue_saturation_harness_is_bounded_and_inference_isolated(self) -> None:
        script = (ROOT / "scripts" / "queue-saturation-acceptance.ps1").read_text(encoding="utf-8")
        for marker in ("observatory.queue-saturation/v1", "@sha256:", "queue_size: 2", "block_on_overflow: false", "inference-sentinel", "v1/chat/completions", "ThreadingHTTPServer", "Get-FreeLoopbackPort", "self_metrics_endpoint", "Get-CollectorMetrics", "otelcol_exporter_enqueue_failed_spans", "otelcol_exporter_send_failed_spans", "evidencePattern", "docker logs", "--pull=never"):
            with self.subTest(marker=marker):
                self.assertIn(marker, script)

    def test_compose_is_loopback_only_and_has_persistence(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn("restart: unless-stopped", compose)
        self.assertIn("stop_grace_period: 30s", compose)
        self.assertIn("mem_limit: 512m", compose)
        self.assertIn("cpus: 1.0", compose)
        self.assertIn("OBSERVATORY_STATE_DIR", compose)
        self.assertIn("OBSERVATORY_API_IMAGE", compose)
        self.assertIn("OBSERVATORY_MAX_DATABASE_BYTES", compose)
        self.assertIn("--allow-remote", compose)
        self.assertIn("--allow-insecure-remote", compose)
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("--allow-insecure-remote", dockerfile)
        self.assertIn("max-database-bytes", compose)
        self.assertNotIn("observatory-data:", compose)
        self.assertIn("otel-queue:", compose)
        self.assertIn("otel-queue-init:", compose)
        self.assertIn("service_completed_successfully", compose)
        self.assertIn("127.0.0.1:4317:4317", compose)
        self.assertIn("127.0.0.1:4318:4318", compose)
        self.assertIn("127.0.0.1:13133:13133", compose)
        self.assertIn("PROMETHEUS_RETENTION_TIME", compose)
        self.assertIn("config.expand-env=true", compose)
        self.assertIn("/otelcol-contrib", compose)
        self.assertIn("validate", compose)
        self.assertIn("127.0.0.1:3000:3000", compose)
        self.assertNotIn("network_mode: host", compose)
        self.assertNotIn("observatory-api:\n        condition: service_healthy", compose)
        self.assertNotIn("prometheus:\n        condition: service_healthy", compose)
        self.assertIn('      - "8888"', compose)
        for image_marker in ("OTEL_COLLECTOR_IMAGE:-otel/opentelemetry-collector-contrib@sha256:", "ALPINE_IMAGE:-alpine@sha256:", "TEMPO_IMAGE:-grafana/tempo@sha256:", "LOKI_IMAGE:-grafana/loki@sha256:", "PROMETHEUS_IMAGE:-prom/prometheus@sha256:", "GRAFANA_IMAGE:-grafana/grafana@sha256:"):
            with self.subTest(image_marker=image_marker):
                self.assertIn(image_marker, compose)
        for line in compose.splitlines():
            stripped = line.strip()
            if stripped.startswith('- "') and ":" in stripped:
                self.assertIn("127.0.0.1:", stripped)

    def test_collector_has_bounded_fail_open_controls(self) -> None:
        collector = (ROOT / "deployment/otel-collector/config.yaml").read_text(encoding="utf-8")
        for marker in ("memory_limiter", "transform/project_identity", "SHA256", "process.cwd", "current_working_directory", "redaction/privacy", "allow_all_keys: false", "allowed_keys:", "event.name", "event.sequence", "- model", "- provider", "- client", "session_id", "workflow_id", "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "cost_usd", "duration_ms", "ttft_ms", "agent_id", "parent_agent_id", "workflow.run_id", "llm.observatory.project.id", "llm.observatory.tool.call.count", "llm.observatory.files.changed.count", "llm.observatory.acceptance.run_id", "llm.observatory.extensions", "llm.observatory.error.kind", "llm.observatory.rate_limited", "blocked_key_patterns:", "auth[_-]?token", "client[_-]?secret", "prompt[_-]?text", "tool[_-]?arguments?", "process[._-]?", "user[._-]?", "blocked_values:", "redact_all_types: true", "summary: silent", "batch:", "sending_queue:", "file_storage:", "create_directory: true", "max_elapsed_time", "max_size: 268435456", "fsync: true", "out_of_band", "otlphttp/normalizer", "encoding: json", "http://observatory-api:8787", "resource/privacy", "attributes/privacy", "gen_ai.prompt", "llm.observatory.project.root", "transform/privacy", "set(log.body, \"[CONTENT_REDACTED]\")", "context: spanevent", "check_collector_pipeline", "exporter_failure_threshold", "max_request_body_size: 8388608", "telemetry:", "level: normal", "readers:", "host: 0.0.0.0", "port: 8888"):
            self.assertIn(marker, collector)
        self.assertNotIn("allow_all_keys: true", collector)
        self.assertNotIn("block_on_overflow: true", collector)
        self.assertNotIn("debug:", collector)

    def test_retention_is_operator_configurable(self) -> None:
        for relative, marker in (("deployment/tempo/tempo.yaml", "TEMPO_RETENTION"), ("deployment/loki/loki.yaml", "LOKI_RETENTION")):
            self.assertIn(marker, (ROOT / relative).read_text(encoding="utf-8"))
        tempo = (ROOT / "deployment/tempo/tempo.yaml").read_text(encoding="utf-8")
        self.assertIn("usage_report:", tempo)
        self.assertIn("reporting_enabled: false", tempo)

    def test_prometheus_scrapes_collector_self_observability(self) -> None:
        prometheus = (ROOT / "deployment/prometheus/prometheus.yml").read_text(encoding="utf-8")
        self.assertIn("job_name: otel-collector-internal", prometheus)
        self.assertIn("otel-collector:8888", prometheus)

    def test_dashboard_is_valid_and_provisioned(self) -> None:
        dashboard = json.loads((ROOT / "dashboards/global-observatory.json").read_text(encoding="utf-8"))
        self.assertEqual(dashboard["uid"], "global-observatory")
        self.assertEqual(dashboard["title"], "Global Observatory")
        self.assertTrue(any("ALL PROJECTS" in json.dumps(panel) for panel in dashboard["panels"]))
        dashboard_text = (ROOT / "dashboards/global-observatory.json").read_text(encoding="utf-8")
        for dimension in ("repository", "branch", "agent", "subagent", "parent_agent", "role", "skill", "workflow", "task_class", "status", "observatory_events_by_context_total"):
            self.assertIn(dimension, dashboard_text)
        self.assertIn("prometheus", json.dumps(dashboard))
        self.assertIn("loki", json.dumps(dashboard))
        datasource_text = (ROOT / "deployment/grafana/provisioning/datasources/datasources.yaml").read_text(encoding="utf-8")
        self.assertIn("Observatory Events", datasource_text)
        self.assertIn("url: http://observatory-api:8787", datasource_text)
        self.assertIn("system-prometheus", datasource_text)
        self.assertIn("datasourceUid: system-prometheus", datasource_text)
        self.assertIn("tempo", datasource_text)
        self.assertIn("Open model comparison", dashboard_text)
        self.assertIn("(global)", dashboard_text)

    def test_metric_dashboards_have_truthful_context_filters(self) -> None:
        contracts = {
            "efficiency.json": ("observatory_input_tokens_by_context_total", "project=~\\\"$project\\\"", "event_type=~\\\"$event_type\\\""),
            "reliability.json": ("observatory_retries_by_context_total", "observatory_agent_failures_by_context_total", "observatory_rework_loops_by_context_total", "observatory_outcomes_by_kind_status_total{", "outcome_status"),
            "execution-explorer.json": ("observatory_events_by_execution_total{", "observatory_tool_calls_by_context_total", "observatory_files_changed_by_context_total", "project=~\\\"$project\\\"", "event_type=~\\\"$event_type\\\""),
            "skill-workflow.json": ("observatory_events_by_workflow_total{", "observatory_events_by_agent_total{", "project=~\\\"$project\\\""),
            "agent-hierarchy.json": ("observatory_events_by_agent_total{", "parent_agent=~\\\"$parent_agent\\\"", "observatory_agent_failures_by_context_total"),
            "outcome-analysis.json": ("sum(observatory_outcomes_by_kind_status_total{", "observatory_events_by_execution_total{", "correlation_basis=~\\\"$correlation_basis\\\""),
            "model-comparison.json": ("observatory_cost_by_context", "observatory_latency_average_by_context_ms", "usage_source"),
        }
        for relative, markers in contracts.items():
            with self.subTest(relative=relative):
                dashboard = json.loads((ROOT / "dashboards" / relative).read_text(encoding="utf-8"))
                variable_names = {item["name"] for item in dashboard.get("templating", {}).get("list", [])}
                self.assertIn("project", variable_names)
                self.assertIn("repository", variable_names)
                self.assertIn("branch", variable_names)
                text = json.dumps(dashboard)
                for marker in markers:
                    self.assertIn(marker, text)
        efficiency = json.loads((ROOT / "dashboards/efficiency.json").read_text(encoding="utf-8"))
        event_type = next(item for item in efficiency["templating"]["list"] if item["name"] == "event_type")
        self.assertEqual(event_type["current"]["value"], "model.operation")
        for relative, contracts in {
            "efficiency.json": {"Model operation volume": "observatory_events_by_context_total"},
            "reliability.json": {"Provider/model operational volume": "observatory_events_by_context_total"},
            "model-comparison.json": {
                "Observed model operations": "observatory_events_by_context_total",
                "Reported cost by execution context and evidence": "observatory_cost_by_context",
                "Average latency by execution context and evidence": "observatory_latency_average_by_context_ms",
            },
        }.items():
            dashboard = json.loads((ROOT / "dashboards" / relative).read_text(encoding="utf-8"))
            for panel in dashboard["panels"]:
                expected_metric = contracts.get(panel.get("title"))
                if expected_metric is None:
                    continue
                target_text = json.dumps(panel.get("targets", []))
                with self.subTest(relative=relative, title=panel.get("title")):
                    self.assertIn("observatory_", target_text)
                    self.assertIn(expected_metric, target_text)
                    self.assertIn("repository=~\\\"$repository\\\"", target_text)
                    self.assertIn("branch=~\\\"$branch\\\"", target_text)

    def test_system_dashboard_panels_use_real_prometheus(self) -> None:
        global_dashboard = json.loads((ROOT / "dashboards/global-observatory.json").read_text(encoding="utf-8"))
        global_panels = {panel["title"]: panel for panel in global_dashboard["panels"]}
        for title in ("Normalizer ready (global)", "Rejected intake records (global)", "Normalizer uptime (global)"):
            self.assertEqual(global_panels[title]["datasource"]["uid"], "system-prometheus")
        reliability = json.loads((ROOT / "dashboards/reliability.json").read_text(encoding="utf-8"))
        for panel in reliability["panels"]:
            if str(panel.get("title", "")).startswith("Collector exporter"):
                self.assertEqual(panel["datasource"]["uid"], "system-prometheus")

    def test_dashboard_targets_do_not_reference_undefined_variables(self) -> None:
        for path in sorted((ROOT / "dashboards").glob("*.json")):
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            variables = {item.get("name") for item in dashboard.get("templating", {}).get("list", [])}
            for panel in dashboard.get("panels", []):
                for target in panel.get("targets", []):
                    for field in ("expr", "query"):
                        value = target.get(field)
                        if not isinstance(value, str):
                            continue
                        for name in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", value):
                            if name.startswith("__"):
                                continue
                            self.assertIn(name, variables, f"{path.name} target references undefined ${name}")

    def test_session_explorer_has_an_explicit_session_trace_filter(self) -> None:
        dashboard = json.loads((ROOT / "dashboards/session-explorer.json").read_text(encoding="utf-8"))
        variables = {item["name"]: item for item in dashboard.get("templating", {}).get("list", [])}
        self.assertEqual(variables["session_id"]["type"], "textbox")
        trace_targets = [target for panel in dashboard["panels"] for target in panel.get("targets", [])]
        self.assertTrue(any("span.llm.observatory.session.id" in target.get("query", "") for target in trace_targets))
        self.assertIn("event detail and attribution endpoints", json.dumps(dashboard))

    def test_no_provider_credentials_are_in_deployment_files(self) -> None:
        for relative in ("compose.yaml", "deployment/otel-collector/config.yaml", "deployment/grafana/provisioning/datasources/datasources.yaml"):
            text = (ROOT / relative).read_text(encoding="utf-8")
            for forbidden in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "XAI_API_KEY", "Bearer "):
                self.assertNotIn(forbidden, text)

    def test_static_verifier_rejects_a_deliberate_dashboard_contract_violation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            isolated_root = Path(temp) / "repo"
            shutil.copytree(
                ROOT,
                isolated_root,
                ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"),
            )
            dashboard_path = isolated_root / "dashboards" / "global-observatory.json"
            dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
            dashboard["title"] = "Broken contract"
            dashboard_path.write_text(json.dumps(dashboard), encoding="utf-8")

            module_spec = importlib.util.spec_from_file_location("observatory_verify_negative", isolated_root / "scripts" / "verify.py")
            self.assertIsNotNone(module_spec)
            self.assertIsNotNone(module_spec.loader)
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            module.ROOT = isolated_root
            output = io.StringIO()
            with redirect_stdout(output):
                exit_code = module.main()
            self.assertEqual(exit_code, 1)
            self.assertIn("dashboard title must be Global Observatory", output.getvalue())


if __name__ == "__main__":
    unittest.main()
