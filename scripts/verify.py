"""Repository-local static verification gates for the Observatory foundation."""

from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "pyproject.toml",
    "README.md",
    "compose.yaml",
    "deployment/otel-collector/config.yaml",
    "deployment/tempo/tempo.yaml",
    "deployment/loki/loki.yaml",
    "deployment/prometheus/prometheus.yml",
    "deployment/grafana/provisioning/datasources/datasources.yaml",
    "deployment/grafana/provisioning/dashboards/dashboards.yaml",
    "dashboards/global-observatory.json",
    "examples/synthetic-events.jsonl",
    "docs/capability-matrix.yaml",
    "docs/production-readiness.md",
    "src/observatory/migrations/002_evidence_ledger.sql",
    "src/observatory/migrations/003_maintenance.sql",
    "src/observatory/migrations/004_analytics_dimensions.sql",
    "src/observatory/migrations/005_task_dimensions.sql",
    "src/observatory/migrations/006_outcome_correlation_basis.sql",
    "src/observatory/migrations/007_usage_performance_dimensions.sql",
    "src/observatory/clients.py",
    "src/observatory/outcomes.py",
    "src/observatory/maintenance.py",
    "src/observatory/adapters/provider_response.py",
)


def main() -> int:
    failures: list[str] = []
    for relative in REQUIRED:
        if not (ROOT / relative).exists():
            failures.append(f"missing required file: {relative}")

    dashboard_path = ROOT / "dashboards/global-observatory.json"
    if dashboard_path.exists():
        dashboard_text = dashboard_path.read_text(encoding="utf-8")
        try:
            dashboard = json.loads(dashboard_text)
        except json.JSONDecodeError as exc:
            failures.append(f"dashboard is invalid JSON: {exc}")
        else:
            if dashboard.get("uid") != "global-observatory":
                failures.append("dashboard UID must be global-observatory")
            if dashboard.get("title") != "Global Observatory":
                failures.append("dashboard title must be Global Observatory")
            if not any("ALL PROJECTS" in json.dumps(panel) for panel in dashboard.get("panels", [])):
                failures.append("dashboard must state ALL PROJECTS scope")
            for required_dimension in ("repository", "branch", "agent", "subagent", "role", "skill", "workflow", "task_class", "status", "observatory_events_by_context_total"):
                if required_dimension not in dashboard_text:
                    failures.append(f"global dashboard missing context dimension: {required_dimension}")

    for dashboard_path in sorted((ROOT / "dashboards").glob("*.json")):
        try:
            dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            failures.append(f"dashboard is invalid JSON: {dashboard_path.name}: {exc}")
            continue
        if not dashboard.get("uid"):
            failures.append(f"dashboard has no stable UID: {dashboard_path.name}")

    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8") if (ROOT / "compose.yaml").exists() else ""
    for forbidden in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "XAI_API_KEY", "Bearer ", "network_mode: host", "block_on_overflow: true"):
        if forbidden in compose:
            failures.append(f"forbidden deployment value present: {forbidden}")
    published_lines = [line.strip() for line in compose.splitlines() if line.strip().startswith('- "') and ":" in line]
    for line in published_lines:
        if "127.0.0.1:" not in line:
            failures.append(f"published port is not loopback-only: {line}")
    if "/otelcol-contrib" not in compose or "validate" not in compose:
        failures.append("Compose must validate the Collector configuration in its healthcheck")
    for required in ("127.0.0.1:13133:13133", "PROMETHEUS_RETENTION_TIME", "config.expand-env=true"):
        if required not in compose:
            failures.append(f"Compose missing operational control: {required}")

    for relative, required in (("deployment/tempo/tempo.yaml", "TEMPO_RETENTION"), ("deployment/loki/loki.yaml", "LOKI_RETENTION")):
        text = (ROOT / relative).read_text(encoding="utf-8") if (ROOT / relative).exists() else ""
        if required not in text:
            failures.append(f"{relative} missing configurable retention: {required}")

    collector = (ROOT / "deployment/otel-collector/config.yaml").read_text(encoding="utf-8") if (ROOT / "deployment/otel-collector/config.yaml").exists() else ""
    for required in ("memory_limiter", "batch:", "sending_queue:", "file_storage:", "max_size: 268435456", "fsync: true", "out_of_band", "otlphttp/normalizer", "encoding: json", "resource/privacy", "attributes/privacy", "transform/privacy", "context: spanevent", "gen_ai.prompt", "llm.observatory.project.root", "check_collector_pipeline", "exporter_failure_threshold", "max_request_body_size: 8388608"):
        if required not in collector:
            failures.append(f"collector missing required fail-open control: {required}")
    if "debug:" in collector:
        failures.append("production collector must not use the debug exporter")

    privacy = (ROOT / "src/observatory/privacy.py").read_text(encoding="utf-8") if (ROOT / "src/observatory/privacy.py").exists() else ""
    if "content_capture: bool = False" not in privacy:
        failures.append("content capture is not disabled by default")

    migration = (ROOT / "src/observatory/migrations/002_evidence_ledger.sql").read_text(encoding="utf-8") if (ROOT / "src/observatory/migrations/002_evidence_ledger.sql").exists() else ""
    for required in ("ingest_ledger", "measurement_facts", "outcome_events", "attribution_edges", "append-only"):
        if required not in migration:
            failures.append(f"evidence migration missing required contract: {required}")

    result = {"schema": "observatory.verify/v1", "status": "pass" if not failures else "fail", "failures": failures}
    print(json.dumps(result, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
