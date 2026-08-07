import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DeploymentTests(unittest.TestCase):
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

    def test_compose_is_loopback_only_and_has_persistence(self) -> None:
        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn("restart: unless-stopped", compose)
        self.assertIn("mem_limit: 512m", compose)
        self.assertIn("cpus: 1.0", compose)
        self.assertIn("OBSERVATORY_STATE_DIR", compose)
        self.assertNotIn("observatory-data:", compose)
        self.assertIn("otel-queue:", compose)
        self.assertIn("127.0.0.1:4317:4317", compose)
        self.assertIn("127.0.0.1:4318:4318", compose)
        self.assertIn("127.0.0.1:13133:13133", compose)
        self.assertIn("PROMETHEUS_RETENTION_TIME", compose)
        self.assertIn("config.expand-env=true", compose)
        self.assertIn("/otelcol-contrib", compose)
        self.assertIn("validate", compose)
        self.assertIn("127.0.0.1:3000:3000", compose)
        self.assertNotIn("network_mode: host", compose)
        for line in compose.splitlines():
            stripped = line.strip()
            if stripped.startswith('- "') and ":" in stripped:
                self.assertIn("127.0.0.1:", stripped)

    def test_collector_has_bounded_fail_open_controls(self) -> None:
        collector = (ROOT / "deployment/otel-collector/config.yaml").read_text(encoding="utf-8")
        for marker in ("memory_limiter", "batch:", "sending_queue:", "file_storage:", "max_elapsed_time", "max_size: 268435456", "fsync: true", "out_of_band", "otlphttp/normalizer", "encoding: json", "http://observatory-api:8787", "resource/privacy", "attributes/privacy", "gen_ai.prompt", "llm.observatory.project.root", "transform/privacy", "set(log.body, \"[CONTENT_REDACTED]\")", "context: spanevent", "check_collector_pipeline", "exporter_failure_threshold", "max_request_body_size: 8388608"):
            self.assertIn(marker, collector)
        self.assertNotIn("block_on_overflow: true", collector)
        self.assertNotIn("debug:", collector)

    def test_retention_is_operator_configurable(self) -> None:
        for relative, marker in (("deployment/tempo/tempo.yaml", "TEMPO_RETENTION"), ("deployment/loki/loki.yaml", "LOKI_RETENTION")):
            self.assertIn(marker, (ROOT / relative).read_text(encoding="utf-8"))

    def test_dashboard_is_valid_and_provisioned(self) -> None:
        dashboard = json.loads((ROOT / "dashboards/global-observatory.json").read_text(encoding="utf-8"))
        self.assertEqual(dashboard["uid"], "global-observatory")
        self.assertEqual(dashboard["title"], "Global Observatory")
        self.assertTrue(any("ALL PROJECTS" in json.dumps(panel) for panel in dashboard["panels"]))
        dashboard_text = (ROOT / "dashboards/global-observatory.json").read_text(encoding="utf-8")
        for dimension in ("repository", "branch", "agent", "subagent", "role", "skill", "workflow", "task_class", "status", "observatory_events_by_context_total"):
            self.assertIn(dimension, dashboard_text)
        self.assertIn("prometheus", json.dumps(dashboard))
        self.assertIn("loki", json.dumps(dashboard))
        self.assertIn("tempo", (ROOT / "deployment/grafana/provisioning/datasources/datasources.yaml").read_text(encoding="utf-8"))

    def test_no_provider_credentials_are_in_deployment_files(self) -> None:
        for relative in ("compose.yaml", "deployment/otel-collector/config.yaml", "deployment/grafana/provisioning/datasources/datasources.yaml"):
            text = (ROOT / relative).read_text(encoding="utf-8")
            for forbidden in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "XAI_API_KEY", "Bearer "):
                self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()
