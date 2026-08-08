"""Repository-local static verification gates for the Observatory foundation."""

from __future__ import annotations

import json
from pathlib import Path
import re
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
    "scripts/runtime-acceptance.ps1",
    "scripts/provider-acceptance.ps1",
    "scripts/queue-saturation-acceptance.ps1",
    "src/observatory/migrations/002_evidence_ledger.sql",
    "src/observatory/migrations/003_maintenance.sql",
    "src/observatory/migrations/004_analytics_dimensions.sql",
    "src/observatory/migrations/005_task_dimensions.sql",
    "src/observatory/migrations/006_outcome_correlation_basis.sql",
    "src/observatory/migrations/007_usage_performance_dimensions.sql",
    "src/observatory/migrations/008_model_variant.sql",
    "src/observatory/migrations/009_outcome_correlation_indexes.sql",
    "src/observatory/migrations/010_agent_behavior.sql",
    "src/observatory/migrations/011_reliability_dimensions.sql",
    "src/observatory/migrations/012_parent_agent.sql",
    "src/observatory/clients.py",
    "src/observatory/outcomes.py",
    "src/observatory/maintenance.py",
    "src/observatory/adapters/provider_response.py",
    "src/observatory/prometheus.py",
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
            for required_dimension in ("repository", "branch", "agent", "subagent", "parent_agent", "provider", "model", "model_family", "model_variant", "role", "skill", "workflow", "task_class", "status", "observatory_events_by_context_total"):
                if required_dimension not in dashboard_text:
                    failures.append(f"global dashboard missing context dimension: {required_dimension}")
            for required_link in ("Open model comparison", "(global)"):
                if required_link not in dashboard_text:
                    failures.append(f"global dashboard missing honesty/drill-down marker: {required_link}")

    for dashboard_path in sorted((ROOT / "dashboards").glob("*.json")):
        try:
            dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            failures.append(f"dashboard is invalid JSON: {dashboard_path.name}: {exc}")
            continue
        if not dashboard.get("uid"):
            failures.append(f"dashboard has no stable UID: {dashboard_path.name}")
        variables = {item.get("name") for item in dashboard.get("templating", {}).get("list", [])}
        for panel in dashboard.get("panels", []):
            for target in panel.get("targets", []):
                for field in ("expr", "query"):
                    value = target.get(field)
                    if not isinstance(value, str):
                        continue
                    for name in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", value):
                        if not name.startswith("__") and name not in variables:
                            failures.append(f"{dashboard_path.name} target references undefined dashboard variable: ${name}")

    filtered_dashboard_contracts = {
        "efficiency.json": ("observatory_input_tokens_by_context_total", "project=~\\\"$project\\\"", "event_type=~\\\"$event_type\\\""),
        "reliability.json": ("observatory_retries_by_context_total", "observatory_agent_failures_by_context_total", "observatory_rework_loops_by_context_total", "observatory_outcomes_by_kind_status_total{", "outcome_status"),
        "execution-explorer.json": ("observatory_events_by_execution_total{", "project=~\\\"$project\\\"", "event_type=~\\\"$event_type\\\""),
        "skill-workflow.json": ("observatory_events_by_workflow_total{", "observatory_events_by_agent_total{", "project=~\\\"$project\\\""),
        "agent-hierarchy.json": ("observatory_events_by_agent_total{", "parent_agent=~\\\"$parent_agent\\\"", "observatory_agent_failures_by_context_total"),
        "outcome-analysis.json": ("sum(observatory_outcomes_by_kind_status_total{", "observatory_events_by_execution_total{", "correlation_basis=~\\\"$correlation_basis\\\""),
        "model-comparison.json": ("observatory_cost_by_context", "observatory_latency_average_by_context_ms", "usage_source"),
    }
    for relative, required_markers in filtered_dashboard_contracts.items():
        path = ROOT / "dashboards" / relative
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        for required in required_markers:
            if required not in text:
                failures.append(f"{relative} missing scoped dashboard contract: {required}")

    outcome_dashboard = ROOT / "dashboards/outcome-analysis.json"
    if outcome_dashboard.exists():
        outcome_text = outcome_dashboard.read_text(encoding="utf-8")
        for required in ("outcome_kind", "outcome_status", "evidence_source", "correlation_basis", "project=~\\\"$project\\\""):
                if required not in outcome_text:
                    failures.append(f"outcome dashboard missing filter contract: {required}")

    efficiency_dashboard = ROOT / "dashboards/efficiency.json"
    if efficiency_dashboard.exists():
        try:
            efficiency = json.loads(efficiency_dashboard.read_text(encoding="utf-8"))
            event_type = next(item for item in efficiency.get("templating", {}).get("list", []) if item.get("name") == "event_type")
            if event_type.get("current", {}).get("value") != "model.operation":
                failures.append("efficiency dashboard must default to model.operation")
        except (json.JSONDecodeError, StopIteration, AttributeError):
            failures.append("efficiency dashboard has no valid model-operation default")

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
    for required in ("127.0.0.1:13133:13133", "PROMETHEUS_RETENTION_TIME", "OBSERVATORY_MAX_DATABASE_BYTES", "config.expand-env=true", "otel-queue-init", "service_completed_successfully", "--allow-insecure-remote", '"8888"'):
        if required not in compose:
            failures.append(f"Compose missing operational control: {required}")
    for required in (
        "OTEL_COLLECTOR_IMAGE:-otel/opentelemetry-collector-contrib@sha256:",
        "ALPINE_IMAGE:-alpine@sha256:",
        "TEMPO_IMAGE:-grafana/tempo@sha256:",
        "LOKI_IMAGE:-grafana/loki@sha256:",
        "PROMETHEUS_IMAGE:-prom/prometheus@sha256:",
        "GRAFANA_IMAGE:-grafana/grafana@sha256:",
    ):
        if required not in compose:
            failures.append(f"Compose image is not digest-pinned by default: {required}")

    for relative, required in (("deployment/tempo/tempo.yaml", "TEMPO_RETENTION"), ("deployment/loki/loki.yaml", "LOKI_RETENTION")):
        text = (ROOT / relative).read_text(encoding="utf-8") if (ROOT / relative).exists() else ""
        if required not in text:
            failures.append(f"{relative} missing configurable retention: {required}")
    tempo = (ROOT / "deployment/tempo/tempo.yaml").read_text(encoding="utf-8") if (ROOT / "deployment/tempo/tempo.yaml").exists() else ""
    if "usage_report:" not in tempo or "reporting_enabled: false" not in tempo:
        failures.append("Tempo anonymous usage reporting must be disabled")

    collector = (ROOT / "deployment/otel-collector/config.yaml").read_text(encoding="utf-8") if (ROOT / "deployment/otel-collector/config.yaml").exists() else ""
    for required in ("memory_limiter", "transform/project_identity", "SHA256", "process.cwd", "current_working_directory", "redaction/privacy", "allow_all_keys: false", "allowed_keys:", "llm.observatory.tool.call.count", "llm.observatory.files.changed.count", "llm.observatory.reassessment.count", "llm.observatory.rework.count", "llm.observatory.extensions", "llm.observatory.error.kind", "llm.observatory.rate_limited", "blocked_key_patterns:", "blocked_values:", "redact_all_types: true", "summary: silent", "batch:", "sending_queue:", "block_on_overflow: false", "file_storage:", "create_directory: true", "max_size: 268435456", "fsync: true", "out_of_band", "otlphttp/normalizer", "encoding: json", "resource/privacy", "attributes/privacy", "transform/privacy", "context: spanevent", "gen_ai.prompt", "llm.observatory.project.root", "check_collector_pipeline", "exporter_failure_threshold", "max_request_body_size: 8388608", "telemetry:", "level: normal", "readers:", "port: 8888"):
        if required not in collector:
            failures.append(f"collector missing required fail-open control: {required}")
    if "allow_all_keys: true" in collector:
        failures.append("collector redaction must fail closed with an explicit allowlist")
    if "debug:" in collector:
        failures.append("production collector must not use the debug exporter")

    datasources = (ROOT / "deployment/grafana/provisioning/datasources/datasources.yaml").read_text(encoding="utf-8") if (ROOT / "deployment/grafana/provisioning/datasources/datasources.yaml").exists() else ""
    for required in ("Observatory Events", "uid: prometheus", "url: http://observatory-api:8787", "name: Prometheus", "uid: system-prometheus", "url: http://prometheus:9090"):
        if required not in datasources:
            failures.append(f"Grafana datasource provisioning missing event-time/system contract: {required}")

    capability_matrix = (ROOT / "docs/capability-matrix.yaml").read_text(encoding="utf-8") if (ROOT / "docs/capability-matrix.yaml").exists() else ""
    if "    - hooks_or_events" not in capability_matrix:
        failures.append("capability matrix contract must declare hooks_or_events")

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
