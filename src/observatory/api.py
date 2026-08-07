"""Loopback-only HTTP intake and query API."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from threading import Lock
import time
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from .intake import Intake
from .otel_bridge import OTLPJsonBridge
from .store import EventStore


def _ingest_status(result: Any) -> HTTPStatus:
    """Make malformed records visible to exporters instead of acknowledging them."""

    return HTTPStatus.BAD_REQUEST if result.rejected else HTTPStatus.OK


def _ingest_outcome(result: Any) -> str:
    if not result.rejected:
        return "accepted"
    return "accepted_with_rejections" if result.inserted or result.duplicate or result.conflict else "rejected"


class ObservatoryApplication:
    def __init__(self, store: EventStore, *, max_request_bytes: int = 8_388_608, max_records: int = 256) -> None:
        if max_request_bytes < 1024:
            raise ValueError("max_request_bytes must be at least 1024")
        self.store = store
        self.intake = Intake(store, max_records=max_records)
        self.otel = OTLPJsonBridge(store, max_records=max_records)
        self.max_request_bytes = max_request_bytes
        self.lock = Lock()
        self.started_at = store.connection.execute("SELECT datetime('now')").fetchone()[0]
        self.started_monotonic = time.monotonic()
        self.ingest_batches = 0
        self.ingest_records = 0
        self.ingest_rejected = 0

    def ingest_json(self, value: Any) -> tuple[int, dict[str, Any]]:
        records = value if isinstance(value, list) else [value]
        with self.lock:
            result = self.intake.ingest(records)
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
        return _ingest_status(result), {"schema": "observatory.intake/v1", "outcome": _ingest_outcome(result), **result.to_mapping()}

    def ingest_otlp(self, signal: str, value: Any) -> tuple[int, dict[str, Any]]:
        if not isinstance(value, Mapping):
            return HTTPStatus.BAD_REQUEST, {"error": "OTLP payload must be an object"}
        with self.lock:
            result = self.otel.ingest(signal, value)
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
        return _ingest_status(result), {"schema": "observatory.otlp/v1", "signal": signal, "outcome": _ingest_outcome(result), **result.to_mapping()}

    def summary(self, filters: Mapping[str, str]) -> dict[str, Any]:
        with self.lock:
            return {"schema": "observatory.summary/v1", "filters": dict(filters), "data": self.store.summary(filters)}

    def events(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        with self.lock:
            values = [event.to_mapping() for event in self.store.list_events(filters, limit=limit)]
        return {"schema": "observatory.events/v1", "count": len(values), "events": values}

    def event_detail(self, event_id: str) -> tuple[int, dict[str, Any]]:
        with self.lock:
            value = self.store.event_detail(event_id)
        if value is None:
            return HTTPStatus.NOT_FOUND, {"error": "event_not_found", "event_id": event_id}
        return HTTPStatus.OK, {"schema": "observatory.event-detail/v1", **value}

    def measurements(self, event_id: str | None, limit: int) -> dict[str, Any]:
        with self.lock:
            values = self.store.measurement_facts(event_id=event_id, limit=limit)
        return {"schema": "observatory.measurements/v1", "count": len(values), "measurements": values}

    def outcomes(self, event_id: str | None, limit: int) -> dict[str, Any]:
        with self.lock:
            values = self.store.outcomes(event_id=event_id, limit=limit)
        return {"schema": "observatory.outcomes/v1", "count": len(values), "outcomes": values}

    def attribution(self, event_id: str | None, limit: int) -> dict[str, Any]:
        with self.lock:
            values = self.store.attribution_edges(event_id=event_id, limit=limit)
        return {"schema": "observatory.attribution/v1", "count": len(values), "edges": values}

    def comparison(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        with self.lock:
            values = self.store.comparison(filters, limit=limit)
        return {"schema": "observatory.analytics-comparison/v1", "count": len(values), "comparisons": values}

    def health(self) -> dict[str, Any]:
        with self.lock:
            ingest = {"batches": self.ingest_batches, "records": self.ingest_records, "rejected": self.ingest_rejected}
            try:
                self.store.connection.execute("SELECT 1").fetchone()
            except sqlite3.Error:
                return {
                    "schema": "observatory.health/v1",
                    "status": "degraded",
                    "store": "unavailable",
                    "started_at": self.started_at,
                    "uptime_seconds": round(max(time.monotonic() - self.started_monotonic, 0.0), 3),
                    "ingest": ingest,
                    "inference_path": "unmanaged/no-proxy",
                }
        return {
            "schema": "observatory.health/v1",
            "status": "ok",
            "store": "ready",
            "started_at": self.started_at,
            "uptime_seconds": round(max(time.monotonic() - self.started_monotonic, 0.0), 3),
            "ingest": ingest,
            "inference_path": "unmanaged/no-proxy",
        }

    def metrics(self) -> str:
        with self.lock:
            summary = self.store.summary()
            conflicts = self.store.conflict_count()
            dimensions = self.store.metric_dimensions()
            ingest_batches = self.ingest_batches
            ingest_records = self.ingest_records
            ingest_rejected = self.ingest_rejected
        lines = [
            "# HELP observatory_process_ready Whether the normalizer process and store are ready.",
            "# TYPE observatory_process_ready gauge",
            "observatory_process_ready 1",
            "# HELP observatory_process_uptime_seconds Normalizer process uptime in seconds.",
            "# TYPE observatory_process_uptime_seconds gauge",
            f"observatory_process_uptime_seconds {max(time.monotonic() - self.started_monotonic, 0.0)}",
            "# HELP observatory_ingest_batches_total Intake batches seen by this process.",
            "# TYPE observatory_ingest_batches_total counter",
            f"observatory_ingest_batches_total {ingest_batches}",
            "# HELP observatory_ingest_records_total Intake records seen by this process.",
            "# TYPE observatory_ingest_records_total counter",
            f"observatory_ingest_records_total {ingest_records}",
            "# HELP observatory_ingest_rejected_total Intake records rejected by this process.",
            "# TYPE observatory_ingest_rejected_total counter",
            f"observatory_ingest_rejected_total {ingest_rejected}",
            "# HELP observatory_events_total Canonical normalized events accepted by the local store.",
            "# TYPE observatory_events_total gauge",
            f"observatory_events_total {summary['events']}",
            "# HELP observatory_event_conflicts_total Conflicting replays retained for diagnosis.",
            "# TYPE observatory_event_conflicts_total gauge",
            f"observatory_event_conflicts_total {conflicts}",
            "# HELP observatory_event_successes_total Successful normalized operations.",
            "# TYPE observatory_event_successes_total gauge",
            f"observatory_event_successes_total {summary['successes']}",
            "# HELP observatory_event_failures_total Failed normalized operations.",
            "# TYPE observatory_event_failures_total gauge",
            f"observatory_event_failures_total {summary['failures']}",
            "# HELP observatory_ingest_ledger_entries_total Append-only intake attempts retained for audit.",
            "# TYPE observatory_ingest_ledger_entries_total gauge",
            f"observatory_ingest_ledger_entries_total {self.store.ledger_count()}",
            "# HELP observatory_measurement_facts_total Field-level evidence facts retained.",
            "# TYPE observatory_measurement_facts_total gauge",
            f"observatory_measurement_facts_total {self.store.measurement_count()}",
            "# HELP observatory_outcomes_total Correlated outcome observations retained.",
            "# TYPE observatory_outcomes_total gauge",
            f"observatory_outcomes_total {self.store.outcome_count()}",
            "# HELP observatory_input_tokens_total Total input tokens reported in normalized events.",
            "# TYPE observatory_input_tokens_total gauge",
            f"observatory_input_tokens_total {summary['input_tokens']}",
            "# HELP observatory_output_tokens_total Total output tokens reported in normalized events.",
            "# TYPE observatory_output_tokens_total gauge",
            f"observatory_output_tokens_total {summary['output_tokens']}",
            "# HELP observatory_cached_tokens_total Total cached tokens reported in normalized events.",
            "# TYPE observatory_cached_tokens_total gauge",
            f"observatory_cached_tokens_total {summary['cached_tokens']}",
            "# HELP observatory_cache_creation_tokens_total Total cache-creation tokens reported in normalized events.",
            "# TYPE observatory_cache_creation_tokens_total gauge",
            f"observatory_cache_creation_tokens_total {summary['cache_creation_tokens']}",
            "# HELP observatory_cache_read_tokens_total Total cache-read tokens reported in normalized events.",
            "# TYPE observatory_cache_read_tokens_total gauge",
            f"observatory_cache_read_tokens_total {summary['cache_read_tokens']}",
            "# HELP observatory_reasoning_tokens_total Total reasoning tokens reported in normalized events.",
            "# TYPE observatory_reasoning_tokens_total gauge",
            f"observatory_reasoning_tokens_total {summary['reasoning_tokens']}",
            "# HELP observatory_compactions_total Total context compaction observations.",
            "# TYPE observatory_compactions_total gauge",
            f"observatory_compactions_total {summary['compactions']}",
            "# HELP observatory_cost_total Total reported cost in the source currency or unit.",
            "# TYPE observatory_cost_total gauge",
            f"observatory_cost_total {summary['cost']}",
            "# HELP observatory_latency_average_ms Average reported operation latency in milliseconds.",
            "# TYPE observatory_latency_average_ms gauge",
            f"observatory_latency_average_ms {summary['average_latency_ms']}",
            "# HELP observatory_time_to_first_token_average_ms Average time to first token in milliseconds.",
            "# TYPE observatory_time_to_first_token_average_ms gauge",
            f"observatory_time_to_first_token_average_ms {summary['average_time_to_first_token_ms']}",
            "# HELP observatory_duration_average_ms Average total generation duration in milliseconds.",
            "# TYPE observatory_duration_average_ms gauge",
            f"observatory_duration_average_ms {summary['average_duration_ms']}",
            "# HELP observatory_context_size_average Average reported context size in tokens.",
            "# TYPE observatory_context_size_average gauge",
            f"observatory_context_size_average {summary['average_context_size']}",
            "# HELP observatory_context_utilization_average Average reported context utilization ratio.",
            "# TYPE observatory_context_utilization_average gauge",
            f"observatory_context_utilization_average {summary['average_context_utilization']}",
            "# HELP observatory_concurrency_average Average reported concurrency.",
            "# TYPE observatory_concurrency_average gauge",
            f"observatory_concurrency_average {summary['average_concurrency']}",
            "# HELP observatory_parallel_utilization_average Average reported parallel utilization ratio.",
            "# TYPE observatory_parallel_utilization_average gauge",
            f"observatory_parallel_utilization_average {summary['average_parallel_utilization']}",
            "# HELP observatory_retries_total Total retry attempts reported in normalized events.",
            "# TYPE observatory_retries_total gauge",
            f"observatory_retries_total {summary['retries']}",
            "# HELP observatory_rate_limited_total Total rate-limited operations reported in normalized events.",
            "# TYPE observatory_rate_limited_total gauge",
            f"observatory_rate_limited_total {summary['rate_limited']}",
            "# HELP observatory_timeouts_total Total timeout operations reported in normalized events.",
            "# TYPE observatory_timeouts_total gauge",
            f"observatory_timeouts_total {summary['timeouts']}",
            "# HELP observatory_tool_failures_total Total tool failures reported in normalized events.",
            "# TYPE observatory_tool_failures_total gauge",
            f"observatory_tool_failures_total {summary['tool_failures']}",
        ]
        lines.extend([
            "# HELP observatory_events_by_provider_model_total Observed events by bounded provider, model, family, client, auth, route, and task dimensions.",
            "# TYPE observatory_events_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "task_class": item["task_class"]})
            lines.append(f"observatory_events_by_provider_model_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_success_rate_by_provider_model Operation success ratio by bounded provider/model/client dimensions.",
            "# TYPE observatory_success_rate_by_provider_model gauge",
            "# HELP observatory_tokens_by_provider_model_total Total tokens by bounded provider/model/client dimensions.",
            "# TYPE observatory_tokens_by_provider_model_total gauge",
            "# HELP observatory_cost_by_provider_model Reported cost by bounded provider/model/client dimensions.",
            "# TYPE observatory_cost_by_provider_model gauge",
            "# HELP observatory_latency_average_by_provider_model_ms Average latency by bounded provider/model/client dimensions.",
            "# TYPE observatory_latency_average_by_provider_model_ms gauge",
            "# HELP observatory_retries_by_provider_model_total Retry attempts by bounded provider/model/client dimensions.",
            "# TYPE observatory_retries_by_provider_model_total gauge",
            "# HELP observatory_rate_limited_by_provider_model_total Rate-limited operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_rate_limited_by_provider_model_total gauge",
            "# HELP observatory_timeouts_by_provider_model_total Timeout operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_timeouts_by_provider_model_total gauge",
            "# HELP observatory_tool_failures_by_provider_model_total Tool failures by bounded provider/model/client dimensions.",
            "# TYPE observatory_tool_failures_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "task_class": item["task_class"]})
            count = item["count"] or 0
            success_rate = (item["successes"] or 0) / count if count else 0
            lines.append(f"observatory_success_rate_by_provider_model{{{labels}}} {success_rate}")
            lines.append(f"observatory_tokens_by_provider_model_total{{{labels}}} {item['total_tokens'] or 0}")
            lines.append(f"observatory_cost_by_provider_model{{{labels}}} {item['cost'] or 0}")
            lines.append(f"observatory_latency_average_by_provider_model_ms{{{labels}}} {item['average_latency_ms'] or 0}")
            lines.append(f"observatory_retries_by_provider_model_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_provider_model_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_provider_model_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_provider_model_total{{{labels}}} {item['tool_failures'] or 0}")
        lines.extend([
            "# HELP observatory_events_by_context_total Observed events by bounded project, repository, branch, provider, model, client, execution, workflow, task, and status context.",
            "# TYPE observatory_events_by_context_total gauge",
            "# HELP observatory_tokens_by_context_total Total tokens by bounded event context.",
            "# TYPE observatory_tokens_by_context_total gauge",
            "# HELP observatory_cache_creation_tokens_by_context_total Cache-creation tokens by bounded event context.",
            "# TYPE observatory_cache_creation_tokens_by_context_total gauge",
            "# HELP observatory_cache_read_tokens_by_context_total Cache-read tokens by bounded event context.",
            "# TYPE observatory_cache_read_tokens_by_context_total gauge",
            "# HELP observatory_compactions_by_context_total Context compactions by bounded event context.",
            "# TYPE observatory_compactions_by_context_total gauge",
            "# HELP observatory_cost_by_context Reported cost by bounded event context.",
            "# TYPE observatory_cost_by_context gauge",
            "# HELP observatory_latency_average_by_context_ms Average latency by bounded event context.",
            "# TYPE observatory_latency_average_by_context_ms gauge",
            "# HELP observatory_time_to_first_token_average_by_context_ms Average time to first token by bounded event context.",
            "# TYPE observatory_time_to_first_token_average_by_context_ms gauge",
            "# HELP observatory_duration_average_by_context_ms Average duration by bounded event context.",
            "# TYPE observatory_duration_average_by_context_ms gauge",
            "# HELP observatory_context_size_average_by_context Average context size by bounded event context.",
            "# TYPE observatory_context_size_average_by_context gauge",
            "# HELP observatory_context_utilization_average_by_context Average context utilization by bounded event context.",
            "# TYPE observatory_context_utilization_average_by_context gauge",
            "# HELP observatory_concurrency_average_by_context Average concurrency by bounded event context.",
            "# TYPE observatory_concurrency_average_by_context gauge",
            "# HELP observatory_parallel_utilization_average_by_context Average parallel utilization by bounded event context.",
            "# TYPE observatory_parallel_utilization_average_by_context gauge",
            "# HELP observatory_retries_by_context_total Retry attempts by bounded event context.",
            "# TYPE observatory_retries_by_context_total gauge",
            "# HELP observatory_rate_limited_by_context_total Rate-limited operations by bounded event context.",
            "# TYPE observatory_rate_limited_by_context_total gauge",
            "# HELP observatory_timeouts_by_context_total Timeout operations by bounded event context.",
            "# TYPE observatory_timeouts_by_context_total gauge",
            "# HELP observatory_tool_failures_by_context_total Tool failures by bounded event context.",
            "# TYPE observatory_tool_failures_by_context_total gauge",
        ])
        for item in dimensions["context"]:
            labels = _labels({
                "project": item["project"], "repository": item["repository"], "branch": item["branch"],
                "provider": item["provider"], "model": item["model"], "model_family": item["model_family"],
                "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"],
                "agent": item["agent"], "subagent": item["subagent"], "role": item["role"],
                "skill": item["skill"], "lane": item["lane"], "workflow": item["workflow"],
                "task_class": item["task_class"], "status": item["status"],
            })
            lines.append(f"observatory_events_by_context_total{{{labels}}} {item['count'] or 0}")
            lines.append(f"observatory_tokens_by_context_total{{{labels}}} {item['total_tokens'] or 0}")
            lines.append(f"observatory_cache_creation_tokens_by_context_total{{{labels}}} {item['cache_creation_tokens'] or 0}")
            lines.append(f"observatory_cache_read_tokens_by_context_total{{{labels}}} {item['cache_read_tokens'] or 0}")
            lines.append(f"observatory_compactions_by_context_total{{{labels}}} {item['compactions'] or 0}")
            lines.append(f"observatory_cost_by_context{{{labels}}} {item['cost'] or 0}")
            lines.append(f"observatory_latency_average_by_context_ms{{{labels}}} {item['average_latency_ms'] or 0}")
            lines.append(f"observatory_time_to_first_token_average_by_context_ms{{{labels}}} {item['average_time_to_first_token_ms'] or 0}")
            lines.append(f"observatory_duration_average_by_context_ms{{{labels}}} {item['average_duration_ms'] or 0}")
            lines.append(f"observatory_context_size_average_by_context{{{labels}}} {item['average_context_size'] or 0}")
            lines.append(f"observatory_context_utilization_average_by_context{{{labels}}} {item['average_context_utilization'] or 0}")
            lines.append(f"observatory_concurrency_average_by_context{{{labels}}} {item['average_concurrency'] or 0}")
            lines.append(f"observatory_parallel_utilization_average_by_context{{{labels}}} {item['average_parallel_utilization'] or 0}")
            lines.append(f"observatory_retries_by_context_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_context_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_context_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_context_total{{{labels}}} {item['tool_failures'] or 0}")
        lines.extend([
            "# HELP observatory_events_by_usage_source_total Observed events grouped by usage evidence source.",
            "# TYPE observatory_events_by_usage_source_total gauge",
        ])
        for item in dimensions["usage_source"]:
            lines.append(f"observatory_events_by_usage_source_total{{source=\"{_label(item['source'])}\"}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_project_total Observed events grouped by pseudonymous project identity.",
            "# TYPE observatory_events_by_project_total gauge",
        ])
        for item in dimensions["project"]:
            lines.append(f"observatory_events_by_project_total{{project=\"{_label(item['project'])}\"}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_client_route_total Observed events by client, route, and auth mode.",
            "# TYPE observatory_events_by_client_route_total gauge",
        ])
        for item in dimensions["client_route"]:
            labels = _labels({"project": item["project"], "client": item["client"], "route": item["route"], "auth_mode": item["auth_mode"]})
            lines.append(f"observatory_events_by_client_route_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_execution_total Observed events by bounded execution dimensions.",
            "# TYPE observatory_events_by_execution_total gauge",
        ])
        for item in dimensions["execution"]:
            labels = _labels({"role": item["role"], "skill": item["skill"], "lane": item["lane"]})
            lines.append(f"observatory_events_by_execution_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_workflow_total Observed events by bounded workflow identity.",
            "# TYPE observatory_events_by_workflow_total gauge",
        ])
        for item in dimensions["workflow"]:
            lines.append(f"observatory_events_by_workflow_total{{workflow=\"{_label(item['workflow'])}\"}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_agent_total Observed events by bounded agent identity.",
            "# TYPE observatory_events_by_agent_total gauge",
        ])
        for item in dimensions["agent"]:
            labels = _labels({"agent": item["agent"], "subagent": item["subagent"]})
            lines.append(f"observatory_events_by_agent_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_outcomes_by_kind_status_total Correlated outcomes by kind and status.",
            "# TYPE observatory_outcomes_by_kind_status_total gauge",
        ])
        for item in dimensions["outcome"]:
            labels = _labels({"kind": item["kind"], "status": item["status"]})
            lines.append(f"observatory_outcomes_by_kind_status_total{{{labels}}} {item['count']}")
        return "\n".join(lines) + "\n"


def _query_filters(query: str) -> dict[str, str]:
    allowed = {
        "project", "project_id", "provider", "model", "model_family", "client", "auth_mode", "route", "trace_id", "span_id",
        "event_type", "status", "evidence_source", "branch", "commit", "worktree", "session_id",
        "workflow_id", "agent_id", "subagent_id", "role", "skill", "lane", "outcome_kind",
        "outcome_status", "task_id", "task_class", "start", "end",
    }
    parsed = parse_qs(query, keep_blank_values=False)
    filters: dict[str, str] = {}
    for key, values in parsed.items():
        if key == "limit":
            continue
        if key not in allowed:
            raise ValueError(f"unsupported filter: {key}")
        if len(values) != 1 or len(values[0]) > 256:
            raise ValueError(f"invalid filter: {key}")
        filters[key] = values[0]
    return filters


def _query_limit(query: str, *, default: int = 100, maximum: int = 1000) -> int:
    values = parse_qs(query, keep_blank_values=False).get("limit", [str(default)])
    if len(values) != 1:
        raise ValueError("limit must occur once")
    try:
        limit = int(values[0])
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    if limit < 1 or limit > maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return limit


def _query_event_id(query: str) -> str | None:
    values = parse_qs(query, keep_blank_values=False).get("event_id")
    if not values:
        return None
    if len(values) != 1 or not values[0] or len(values[0]) > 256:
        raise ValueError("event_id must occur once and be 1..256 characters")
    return values[0]


def _query_evidence_endpoint(query: str, *, maximum: int = 5000) -> tuple[str | None, int]:
    parsed = parse_qs(query, keep_blank_values=False)
    unsupported = set(parsed) - {"event_id", "limit"}
    if unsupported:
        raise ValueError(f"unsupported query parameter: {sorted(unsupported)[0]}")
    return _query_event_id(query), _query_limit(query, maximum=maximum)


def _label(value: Any) -> str:
    text = str(value if value is not None else "unknown")
    text = text[:128]
    return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


def _labels(values: Mapping[str, Any]) -> str:
    return ",".join(f'{key}="{_label(value)}"' for key, value in values.items())


class _Handler(BaseHTTPRequestHandler):
    server: "ObservatoryHTTPServer"

    def _send_json(self, status: int, value: Mapping[str, Any]) -> None:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, value: str, content_type: str) -> None:
        body = value.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        try:
            if parsed.path.startswith("/v1/events/"):
                event_id = parsed.path.removeprefix("/v1/events/")
                if not event_id or "/" in event_id or len(event_id) > 256:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_event_id"})
                    return
                status, value = self.server.application.event_detail(event_id)
                self._send_json(status, value)
                return
            if parsed.path in ("/healthz", "/readyz"):
                health = self.server.application.health()
                status = HTTPStatus.SERVICE_UNAVAILABLE if parsed.path == "/readyz" and health.get("status") != "ok" else HTTPStatus.OK
                self._send_json(status, health)
                return
            if parsed.path == "/metrics":
                self._send_text(HTTPStatus.OK, self.server.application.metrics(), "text/plain; version=0.0.4")
                return
            if parsed.path == "/v1/summary":
                self._send_json(HTTPStatus.OK, self.server.application.summary(_query_filters(parsed.query)))
                return
            if parsed.path == "/v1/events":
                filters = _query_filters(parsed.query)
                limit = _query_limit(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.events(filters, limit))
                return
            if parsed.path == "/v1/measurements":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.measurements(event_id, limit))
                return
            if parsed.path == "/v1/outcomes":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.outcomes(event_id, limit))
                return
            if parsed.path == "/v1/attribution":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.attribution(event_id, limit))
                return
            if parsed.path == "/v1/analytics/comparison":
                self._send_json(HTTPStatus.OK, self.server.application.comparison(_query_filters(parsed.query), _query_limit(parsed.query, maximum=500)))
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except (ValueError, RuntimeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if parsed.path not in ("/v1/events", "/v1/traces", "/v1/metrics", "/v1/logs"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        length_text = self.headers.get("Content-Length")
        try:
            length = int(length_text or "-1")
        except ValueError:
            length = -1
        if length < 0 or length > self.server.application.max_request_bytes:
            if length > 0:
                drain_limit = self.server.application.max_request_bytes * 2
                remaining = min(length, drain_limit)
                while remaining > 0:
                    chunk = self.rfile.read(min(65_536, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                if length > drain_limit:
                    self.close_connection = True
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request_too_large"})
            return
        body = self.rfile.read(length)
        try:
            value = json.loads(body.decode("utf-8"))
            if parsed.path == "/v1/events":
                status, result = self.server.application.ingest_json(value)
            else:
                signal = parsed.path.removeprefix("/v1/")
                status, result = self.server.application.ingest_otlp(signal, value)
            self._send_json(status, result)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"invalid_json: {exc}"})

    def log_message(self, format: str, *args: Any) -> None:
        # Never log request bodies, query credentials, or arbitrary client text.
        return


class ObservatoryHTTPServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], application: ObservatoryApplication) -> None:
        self.application = application
        super().__init__(address, _Handler)


def create_server(host: str, port: int, db_path: str | Path) -> ObservatoryHTTPServer:
    return ObservatoryHTTPServer((host, port), ObservatoryApplication(EventStore(db_path)))


def serve(host: str = "127.0.0.1", port: int = 8787, db_path: str | Path = "observatory.sqlite3") -> None:
    server = create_server(host, port, db_path)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        server.application.store.close()
