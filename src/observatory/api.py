"""Loopback-only HTTP intake and query API."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import secrets
import socket
from pathlib import Path
import sqlite3
from threading import BoundedSemaphore, Lock
import time
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from .intake import Intake
from .otel_bridge import OTLPJsonBridge
from .prometheus import PrometheusQueryEngine, PrometheusQueryError
from .store import DEFAULT_MAX_DATABASE_BYTES, EventStore


MAX_QUERY_BYTES = 64 * 1024
MAX_QUERY_FIELDS = 128


def _ingest_status(result: Any) -> HTTPStatus:
    """Make malformed records visible to exporters instead of acknowledging them."""

    if getattr(result, "unavailable", 0):
        return HTTPStatus.SERVICE_UNAVAILABLE
    return HTTPStatus.BAD_REQUEST if result.rejected else HTTPStatus.OK


def _ingest_outcome(result: Any) -> str:
    if getattr(result, "unavailable", 0):
        return "degraded"
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
        self.prometheus = PrometheusQueryEngine(store)
        self.max_request_bytes = max_request_bytes
        self.lock = Lock()
        self.started_at = store.connection.execute("SELECT datetime('now')").fetchone()[0]
        self.started_monotonic = time.monotonic()
        self.ingest_batches = 0
        self.ingest_records = 0
        self.ingest_rejected = 0
        self.ingest_unavailable = 0

    def ingest_json(self, value: Any) -> tuple[int, dict[str, Any]]:
        records = value if isinstance(value, list) else [value]
        with self.lock:
            result = self.intake.ingest(records)
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
            self.ingest_unavailable += result.unavailable
        return _ingest_status(result), {"schema": "observatory.intake/v1", "outcome": _ingest_outcome(result), **result.to_mapping()}

    def ingest_otlp(self, signal: str, value: Any) -> tuple[int, dict[str, Any]]:
        if not isinstance(value, Mapping):
            return HTTPStatus.BAD_REQUEST, {"error": "OTLP payload must be an object"}
        with self.lock:
            result = self.otel.ingest(signal, value)
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
            self.ingest_unavailable += result.unavailable
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
            ingest = {"batches": self.ingest_batches, "records": self.ingest_records, "rejected": self.ingest_rejected, "unavailable": self.ingest_unavailable}
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
                    "store_capacity": self.store.capacity(),
                    "inference_path": "unmanaged/no-proxy",
                }
            capacity = self.store.capacity()
            status = "degraded" if capacity["exhausted"] else "ok"
        return {
            "schema": "observatory.health/v1",
            "status": status,
            "store": "ready",
            "started_at": self.started_at,
            "uptime_seconds": round(max(time.monotonic() - self.started_monotonic, 0.0), 3),
            "ingest": ingest,
            "store_capacity": capacity,
            "inference_path": "unmanaged/no-proxy",
        }

    def metrics(self) -> str:
        with self.lock:
            summary = self.store.summary()
            conflicts = self.store.conflict_count()
            # Keep Prometheus bounded while making the default dashboard catalog
            # large enough that normal multi-project installations do not hide
            # dimensions behind the old top-100 cutoff.
            dimensions = self.store.metric_dimensions(limit=500)
            ingest_batches = self.ingest_batches
            ingest_records = self.ingest_records
            ingest_rejected = self.ingest_rejected
            ingest_unavailable = self.ingest_unavailable
            capacity = self.store.capacity()
        lines = [
            "# HELP observatory_process_ready Whether the normalizer process and store are ready.",
            "# TYPE observatory_process_ready gauge",
            f"observatory_process_ready {0 if capacity['exhausted'] else 1}",
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
            "# HELP observatory_ingest_unavailable_total Intake records rejected because the normalized store was unavailable or at capacity.",
            "# TYPE observatory_ingest_unavailable_total counter",
            f"observatory_ingest_unavailable_total {ingest_unavailable}",
            "# HELP observatory_store_bytes Current SQLite database and WAL sidecar bytes.",
            "# TYPE observatory_store_bytes gauge",
            f"observatory_store_bytes {capacity['bytes']}",
            "# HELP observatory_store_capacity_bytes Configured normalized store byte budget.",
            "# TYPE observatory_store_capacity_bytes gauge",
            f"observatory_store_capacity_bytes {_sample(capacity['max_bytes'])}",
            "# HELP observatory_store_capacity_ratio Current normalized store bytes divided by its configured budget.",
            "# TYPE observatory_store_capacity_ratio gauge",
            f"observatory_store_capacity_ratio {_sample(capacity['ratio'])}",
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
            f"observatory_input_tokens_total {_sample(summary['input_tokens'])}",
            "# HELP observatory_output_tokens_total Total output tokens reported in normalized events.",
            "# TYPE observatory_output_tokens_total gauge",
            f"observatory_output_tokens_total {_sample(summary['output_tokens'])}",
            "# HELP observatory_cached_tokens_total Total cached tokens reported in normalized events.",
            "# TYPE observatory_cached_tokens_total gauge",
            f"observatory_cached_tokens_total {_sample(summary['cached_tokens'])}",
            "# HELP observatory_cache_creation_tokens_total Total cache-creation tokens reported in normalized events.",
            "# TYPE observatory_cache_creation_tokens_total gauge",
            f"observatory_cache_creation_tokens_total {_sample(summary['cache_creation_tokens'])}",
            "# HELP observatory_cache_read_tokens_total Total cache-read tokens reported in normalized events.",
            "# TYPE observatory_cache_read_tokens_total gauge",
            f"observatory_cache_read_tokens_total {_sample(summary['cache_read_tokens'])}",
            "# HELP observatory_reasoning_tokens_total Total reasoning tokens reported in normalized events.",
            "# TYPE observatory_reasoning_tokens_total gauge",
            f"observatory_reasoning_tokens_total {_sample(summary['reasoning_tokens'])}",
            "# HELP observatory_compactions_total Total context compaction observations.",
            "# TYPE observatory_compactions_total gauge",
            f"observatory_compactions_total {_sample(summary['compactions'])}",
            "# HELP observatory_cost_total Total reported cost in the source currency or unit.",
            "# TYPE observatory_cost_total gauge",
            f"observatory_cost_total {_sample(summary['cost'])}",
            "# HELP observatory_latency_average_ms Average reported operation latency in milliseconds.",
            "# TYPE observatory_latency_average_ms gauge",
            f"observatory_latency_average_ms {_sample(summary['average_latency_ms'])}",
            "# HELP observatory_time_to_first_token_average_ms Average time to first token in milliseconds.",
            "# TYPE observatory_time_to_first_token_average_ms gauge",
            f"observatory_time_to_first_token_average_ms {_sample(summary['average_time_to_first_token_ms'])}",
            "# HELP observatory_duration_average_ms Average total generation duration in milliseconds.",
            "# TYPE observatory_duration_average_ms gauge",
            f"observatory_duration_average_ms {_sample(summary['average_duration_ms'])}",
            "# HELP observatory_context_size_average Average reported context size in tokens.",
            "# TYPE observatory_context_size_average gauge",
            f"observatory_context_size_average {_sample(summary['average_context_size'])}",
            "# HELP observatory_context_utilization_average Average reported context utilization ratio.",
            "# TYPE observatory_context_utilization_average gauge",
            f"observatory_context_utilization_average {_sample(summary['average_context_utilization'])}",
            "# HELP observatory_concurrency_average Average reported concurrency.",
            "# TYPE observatory_concurrency_average gauge",
            f"observatory_concurrency_average {_sample(summary['average_concurrency'])}",
            "# HELP observatory_parallel_utilization_average Average reported parallel utilization ratio.",
            "# TYPE observatory_parallel_utilization_average gauge",
            f"observatory_parallel_utilization_average {_sample(summary['average_parallel_utilization'])}",
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
            "# HELP observatory_agent_failures_total Total agent failures reported in normalized events.",
            "# TYPE observatory_agent_failures_total gauge",
            f"observatory_agent_failures_total {summary['agent_failures']}",
            "# HELP observatory_reassessments_total Total reassessment-loop observations reported in normalized events.",
            "# TYPE observatory_reassessments_total gauge",
            f"observatory_reassessments_total {_sample(summary['reassessments'])}",
            "# HELP observatory_rework_loops_total Total rework-loop observations reported in normalized events.",
            "# TYPE observatory_rework_loops_total gauge",
            f"observatory_rework_loops_total {_sample(summary['rework_loops'])}",
            "# HELP observatory_tool_calls_total Total bounded tool-call observations.",
            "# TYPE observatory_tool_calls_total gauge",
            f"observatory_tool_calls_total {_sample(summary['tool_calls'])}",
            "# HELP observatory_files_inspected_total Total bounded file-inspection observations.",
            "# TYPE observatory_files_inspected_total gauge",
            f"observatory_files_inspected_total {_sample(summary['files_inspected'])}",
            "# HELP observatory_files_changed_total Total bounded file-change observations.",
            "# TYPE observatory_files_changed_total gauge",
            f"observatory_files_changed_total {_sample(summary['files_changed'])}",
            "# HELP observatory_commands_executed_total Total bounded command-execution observations.",
            "# TYPE observatory_commands_executed_total gauge",
            f"observatory_commands_executed_total {_sample(summary['commands_executed'])}",
            "# HELP observatory_tests_invoked_total Total bounded test-invocation observations.",
            "# TYPE observatory_tests_invoked_total gauge",
            f"observatory_tests_invoked_total {_sample(summary['tests_invoked'])}",
        ]
        lines.extend([
            "# HELP observatory_events_by_provider_model_total Model operations by bounded project, provider, model, family, variant, client, auth, route, and task dimensions.",
            "# TYPE observatory_events_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"project": item["project"], "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"], "task_class": item["task_class"]})
            lines.append(f"observatory_events_by_provider_model_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_success_rate_by_provider_model Operation success ratio by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_success_rate_by_provider_model gauge",
            "# HELP observatory_tokens_by_provider_model_total Total tokens by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_tokens_by_provider_model_total gauge",
            "# HELP observatory_cost_by_provider_model Reported cost by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_cost_by_provider_model gauge",
            "# HELP observatory_latency_average_by_provider_model_ms Average latency by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_latency_average_by_provider_model_ms gauge",
            "# HELP observatory_retries_by_provider_model_total Retry attempts by bounded provider/model/client dimensions.",
            "# TYPE observatory_retries_by_provider_model_total gauge",
            "# HELP observatory_rate_limited_by_provider_model_total Rate-limited operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_rate_limited_by_provider_model_total gauge",
            "# HELP observatory_timeouts_by_provider_model_total Timeout operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_timeouts_by_provider_model_total gauge",
            "# HELP observatory_tool_failures_by_provider_model_total Tool failures by bounded provider/model/client dimensions.",
            "# TYPE observatory_tool_failures_by_provider_model_total gauge",
            "# HELP observatory_agent_failures_by_provider_model_total Agent failures by bounded provider/model/client dimensions.",
            "# TYPE observatory_agent_failures_by_provider_model_total gauge",
            "# HELP observatory_reassessments_by_provider_model_total Reassessment loops by bounded provider/model/client dimensions.",
            "# TYPE observatory_reassessments_by_provider_model_total gauge",
            "# HELP observatory_rework_loops_by_provider_model_total Rework loops by bounded provider/model/client dimensions.",
            "# TYPE observatory_rework_loops_by_provider_model_total gauge",
            "# HELP observatory_tool_calls_by_provider_model_total Tool calls by bounded provider/model/client dimensions.",
            "# TYPE observatory_tool_calls_by_provider_model_total gauge",
            "# HELP observatory_files_changed_by_provider_model_total File changes by bounded provider/model/client dimensions.",
            "# TYPE observatory_files_changed_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"project": item["project"], "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"], "task_class": item["task_class"]})
            count = item["count"] or 0
            success_rate = (item["successes"] or 0) / count if count else 0
            lines.append(f"observatory_success_rate_by_provider_model{{{labels}}} {success_rate}")
            lines.append(f"observatory_tokens_by_provider_model_total{{{labels}}} {_sample(item['total_tokens'])}")
            lines.append(f"observatory_cost_by_provider_model{{{labels}}} {_sample(item['cost'])}")
            lines.append(f"observatory_latency_average_by_provider_model_ms{{{labels}}} {_sample(item['average_latency_ms'])}")
            lines.append(f"observatory_retries_by_provider_model_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_provider_model_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_provider_model_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_provider_model_total{{{labels}}} {item['tool_failures'] or 0}")
            lines.append(f"observatory_agent_failures_by_provider_model_total{{{labels}}} {item['agent_failures'] or 0}")
            lines.append(f"observatory_reassessments_by_provider_model_total{{{labels}}} {_sample(item['reassessments'])}")
            lines.append(f"observatory_rework_loops_by_provider_model_total{{{labels}}} {_sample(item['rework_loops'])}")
            lines.append(f"observatory_tool_calls_by_provider_model_total{{{labels}}} {_sample(item['tool_calls'])}")
            lines.append(f"observatory_files_changed_by_provider_model_total{{{labels}}} {_sample(item['files_changed'])}")
        lines.extend([
            "# HELP observatory_events_by_context_total Observed events by event type and bounded project, repository, branch, provider, model, variant, client, execution, workflow, task, and status context.",
            "# TYPE observatory_events_by_context_total gauge",
            "# HELP observatory_input_tokens_by_context_total Input tokens by bounded event context.",
            "# TYPE observatory_input_tokens_by_context_total gauge",
            "# HELP observatory_output_tokens_by_context_total Output tokens by bounded event context.",
            "# TYPE observatory_output_tokens_by_context_total gauge",
            "# HELP observatory_cached_tokens_by_context_total Cached tokens by bounded event context.",
            "# TYPE observatory_cached_tokens_by_context_total gauge",
            "# HELP observatory_reasoning_tokens_by_context_total Reasoning tokens by bounded event context.",
            "# TYPE observatory_reasoning_tokens_by_context_total gauge",
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
            "# HELP observatory_agent_failures_by_context_total Agent failures by bounded event context.",
            "# TYPE observatory_agent_failures_by_context_total gauge",
            "# HELP observatory_reassessments_by_context_total Reassessment loops by bounded event context.",
            "# TYPE observatory_reassessments_by_context_total gauge",
            "# HELP observatory_rework_loops_by_context_total Rework loops by bounded event context.",
            "# TYPE observatory_rework_loops_by_context_total gauge",
            "# HELP observatory_tool_calls_by_context_total Tool calls by bounded event context.",
            "# TYPE observatory_tool_calls_by_context_total gauge",
            "# HELP observatory_files_inspected_by_context_total File inspections by bounded event context.",
            "# TYPE observatory_files_inspected_by_context_total gauge",
            "# HELP observatory_files_changed_by_context_total File changes by bounded event context.",
            "# TYPE observatory_files_changed_by_context_total gauge",
            "# HELP observatory_commands_executed_by_context_total Command executions by bounded event context.",
            "# TYPE observatory_commands_executed_by_context_total gauge",
            "# HELP observatory_tests_invoked_by_context_total Test invocations by bounded event context.",
            "# TYPE observatory_tests_invoked_by_context_total gauge",
        ])
        for item in dimensions["context"]:
            labels = _labels({
                "event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"],
                "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"],
                "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"],
                "agent": item["agent"], "subagent": item["subagent"], "parent_agent": item["parent_agent"], "role": item["role"],
                "skill": item["skill"], "lane": item["lane"], "workflow": item["workflow"],
                "task_class": item["task_class"], "status": item["status"],
            })
            lines.append(f"observatory_events_by_context_total{{{labels}}} {item['count'] or 0}")
            lines.append(f"observatory_input_tokens_by_context_total{{{labels}}} {_sample(item['input_tokens'])}")
            lines.append(f"observatory_output_tokens_by_context_total{{{labels}}} {_sample(item['output_tokens'])}")
            lines.append(f"observatory_cached_tokens_by_context_total{{{labels}}} {_sample(item['cached_tokens'])}")
            lines.append(f"observatory_reasoning_tokens_by_context_total{{{labels}}} {_sample(item['reasoning_tokens'])}")
            lines.append(f"observatory_tokens_by_context_total{{{labels}}} {_sample(item['total_tokens'])}")
            lines.append(f"observatory_cache_creation_tokens_by_context_total{{{labels}}} {_sample(item['cache_creation_tokens'])}")
            lines.append(f"observatory_cache_read_tokens_by_context_total{{{labels}}} {_sample(item['cache_read_tokens'])}")
            lines.append(f"observatory_compactions_by_context_total{{{labels}}} {_sample(item['compactions'])}")
            lines.append(f"observatory_cost_by_context{{{labels}}} {_sample(item['cost'])}")
            lines.append(f"observatory_latency_average_by_context_ms{{{labels}}} {_sample(item['average_latency_ms'])}")
            lines.append(f"observatory_time_to_first_token_average_by_context_ms{{{labels}}} {_sample(item['average_time_to_first_token_ms'])}")
            lines.append(f"observatory_duration_average_by_context_ms{{{labels}}} {_sample(item['average_duration_ms'])}")
            lines.append(f"observatory_context_size_average_by_context{{{labels}}} {_sample(item['average_context_size'])}")
            lines.append(f"observatory_context_utilization_average_by_context{{{labels}}} {_sample(item['average_context_utilization'])}")
            lines.append(f"observatory_concurrency_average_by_context{{{labels}}} {_sample(item['average_concurrency'])}")
            lines.append(f"observatory_parallel_utilization_average_by_context{{{labels}}} {_sample(item['average_parallel_utilization'])}")
            lines.append(f"observatory_retries_by_context_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_context_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_context_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_context_total{{{labels}}} {item['tool_failures'] or 0}")
            lines.append(f"observatory_agent_failures_by_context_total{{{labels}}} {item['agent_failures'] or 0}")
            lines.append(f"observatory_reassessments_by_context_total{{{labels}}} {_sample(item['reassessments'])}")
            lines.append(f"observatory_rework_loops_by_context_total{{{labels}}} {_sample(item['rework_loops'])}")
            lines.append(f"observatory_tool_calls_by_context_total{{{labels}}} {_sample(item['tool_calls'])}")
            lines.append(f"observatory_files_inspected_by_context_total{{{labels}}} {_sample(item['files_inspected'])}")
            lines.append(f"observatory_files_changed_by_context_total{{{labels}}} {_sample(item['files_changed'])}")
            lines.append(f"observatory_commands_executed_by_context_total{{{labels}}} {_sample(item['commands_executed'])}")
            lines.append(f"observatory_tests_invoked_by_context_total{{{labels}}} {_sample(item['tests_invoked'])}")
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
            "# HELP observatory_events_by_execution_total Observed events by event type and bounded project, repository, branch, role, skill, and lane dimensions.",
            "# TYPE observatory_events_by_execution_total gauge",
        ])
        for item in dimensions["execution"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "role": item["role"], "skill": item["skill"], "lane": item["lane"]})
            lines.append(f"observatory_events_by_execution_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_workflow_total Observed events by event type and bounded project, repository, branch, and workflow identity.",
            "# TYPE observatory_events_by_workflow_total gauge",
        ])
        for item in dimensions["workflow"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "workflow": item["workflow"]})
            lines.append(f"observatory_events_by_workflow_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_agent_total Observed events by event type and bounded project, repository, branch, agent, and subagent identity.",
            "# TYPE observatory_events_by_agent_total gauge",
        ])
        for item in dimensions["agent"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "agent": item["agent"], "subagent": item["subagent"], "parent_agent": item["parent_agent"]})
            lines.append(f"observatory_events_by_agent_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_outcomes_by_kind_status_total Observed outcomes by kind, status, evidence source, project, and correlation basis.",
            "# TYPE observatory_outcomes_by_kind_status_total gauge",
        ])
        for item in dimensions["outcome"]:
            labels = _labels({
                "kind": item["kind"],
                "status": item["status"],
                "evidence_source": item["evidence_source"],
                "project": item["project"],
                "repository": item["repository"],
                "branch": item["branch"],
                "correlation_basis": item["correlation_basis"],
            })
            lines.append(f"observatory_outcomes_by_kind_status_total{{{labels}}} {item['count']}")
        return "\n".join(lines) + "\n"

    def prometheus_api(self, path: str, params: Mapping[str, list[str]]) -> dict[str, Any]:
        with self.lock:
            return self._prometheus_api_unlocked(path, params)

    def _prometheus_api_unlocked(self, path: str, params: Mapping[str, list[str]]) -> dict[str, Any]:
        """Serve the bounded event-time Prometheus compatibility surface."""

        if path == "/api/v1/query":
            query = _prometheus_param(params, "query")
            return self.prometheus.query(query, params)
        if path == "/api/v1/query_range":
            query = _prometheus_param(params, "query")
            return self.prometheus.query_range(query, params)
        if path == "/api/v1/labels":
            return {"status": "success", "data": self.prometheus.labels()}
        if path == "/api/v1/metadata":
            return self.prometheus.metadata()
        if path == "/api/v1/status/buildinfo":
            return {
                "status": "success",
                "data": {"version": "observatory-event-facade/v1", "revision": "local", "branch": "local"},
            }
        if path == "/api/v1/series":
            selectors = params.get("match[]", [])
            if not selectors:
                raise PrometheusQueryError("series requires at least one match[] selector")
            return {"status": "success", "data": self.prometheus.series(selectors, params)}
        label_prefix = "/api/v1/label/"
        if path.startswith(label_prefix) and path.endswith("/values"):
            label = path[len(label_prefix) : -len("/values")]
            if not label or "/" in label:
                raise PrometheusQueryError("invalid Prometheus label path")
            matchers = params.get("match[]", [])
            metric_names = []
            for selector in matchers:
                metric_names.append(selector.split("{", 1)[0].strip())
            return {
                "status": "success",
                "data": self.prometheus.label_values(label, metric_names or None, params),
            }
        raise PrometheusQueryError("unsupported Prometheus API path")


def _parse_query(query: str, *, keep_blank_values: bool = False) -> dict[str, list[str]]:
    if len(query.encode("utf-8")) > MAX_QUERY_BYTES:
        raise ValueError(f"query exceeds {MAX_QUERY_BYTES} bytes")
    try:
        return parse_qs(
            query,
            keep_blank_values=keep_blank_values,
            max_num_fields=MAX_QUERY_FIELDS,
        )
    except ValueError as exc:
        raise ValueError(f"query contains more than {MAX_QUERY_FIELDS} fields") from exc


def _query_filters(query: str) -> dict[str, str]:
    allowed = {
        "project", "project_id", "repository", "provider", "model", "model_family", "model_variant", "client", "auth_mode", "route", "trace_id", "span_id",
        "event_type", "status", "evidence_source", "branch", "commit", "worktree", "session_id",
        "workflow_id", "agent_id", "subagent_id", "parent_agent_id", "parent_agent", "role", "skill", "lane", "outcome_kind",
        "outcome_status", "task_id", "task_class", "usage_source", "start", "end",
    }
    parsed = _parse_query(query, keep_blank_values=False)
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
    values = _parse_query(query, keep_blank_values=False).get("limit", [str(default)])
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
    values = _parse_query(query, keep_blank_values=False).get("event_id")
    if not values:
        return None
    if len(values) != 1 or not values[0] or len(values[0]) > 256:
        raise ValueError("event_id must occur once and be 1..256 characters")
    return values[0]


def _query_evidence_endpoint(query: str, *, maximum: int = 5000) -> tuple[str | None, int]:
    parsed = _parse_query(query, keep_blank_values=False)
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


def _sample(value: Any) -> str:
    """Render missing numeric observations as NaN rather than false zeroes."""

    return "NaN" if value is None else str(value)


def _prometheus_param(params: Mapping[str, list[str]], name: str) -> str:
    values = params.get(name, [])
    if len(values) != 1 or not values[0]:
        raise PrometheusQueryError(f"Prometheus parameter {name} must occur exactly once")
    if len(values[0]) > 64_000:
        raise PrometheusQueryError(f"Prometheus parameter {name} is too large")
    return values[0]


class _Handler(BaseHTTPRequestHandler):
    server: "ObservatoryHTTPServer"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(self.server.request_timeout)

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

    def _require_authentication(self) -> bool:
        """Require the configured bearer token without exposing its value."""

        expected = self.server.auth_token
        if expected is None:
            return True
        header = self.headers.get("Authorization", "")
        scheme, separator, presented = header.partition(" ")
        if separator and scheme.casefold() == "bearer" and presented and secrets.compare_digest(presented, expected):
            return True
        self.close_connection = True
        body = b'{"error":"authentication_required"}'
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        return False

    def _handle_prometheus(self, path: str, params: Mapping[str, list[str]]) -> None:
        try:
            self._send_json(HTTPStatus.OK, self.server.application.prometheus_api(path, params))
        except sqlite3.Error:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                "status": "error",
                "errorType": "unavailable",
                "error": "store_unavailable",
            })
        except (PrometheusQueryError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {
                "status": "error",
                "errorType": "bad_data",
                "error": str(exc),
            })

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if parsed.path not in ("/healthz", "/readyz") and not self._require_authentication():
            return
        try:
            if parsed.path.startswith("/api/v1/"):
                try:
                    params = _parse_query(parsed.query, keep_blank_values=True)
                except ValueError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {
                        "status": "error",
                        "errorType": "bad_data",
                        "error": str(exc),
                    })
                    return
                self._handle_prometheus(parsed.path, params)
                return
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
        except sqlite3.Error:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "store_unavailable"})
        except (ValueError, RuntimeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if parsed.path.startswith("/api/v1/"):
            if not self._require_authentication():
                return
            length_text = self.headers.get("Content-Length")
            try:
                length = int(length_text or "0")
            except ValueError:
                length = -1
            if length < 0 or length > self.server.application.max_request_bytes:
                self.close_connection = length > self.server.application.max_request_bytes * 2
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": "request_too_large",
                })
                return
            try:
                body = self.rfile.read(length)
                form = _parse_query(body.decode("utf-8"), keep_blank_values=True)
            except (UnicodeDecodeError, ValueError) as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": f"invalid_form: {exc}",
                })
                return
            try:
                params = _parse_query(parsed.query, keep_blank_values=True)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": str(exc),
                })
                return
            for key, values in form.items():
                params.setdefault(key, []).extend(values)
            if sum(len(values) for values in params.values()) > MAX_QUERY_FIELDS:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": f"query contains more than {MAX_QUERY_FIELDS} fields",
                })
                return
            self._handle_prometheus(parsed.path, params)
            return
        if parsed.path not in ("/v1/events", "/v1/traces", "/v1/metrics", "/v1/logs"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if not self._require_authentication():
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
        try:
            body = self.rfile.read(length)
        except (TimeoutError, socket.timeout):
            self.close_connection = True
            self._send_json(HTTPStatus.REQUEST_TIMEOUT, {"error": "request_read_timeout"})
            return
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
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address: tuple[str, int],
        application: ObservatoryApplication,
        *,
        request_timeout: float = 10.0,
        max_concurrent_requests: int = 64,
        auth_token: str | None = None,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be positive")
        if auth_token is not None and not auth_token:
            raise ValueError("auth_token must not be empty")
        self.application = application
        self.request_timeout = request_timeout
        self._request_slots = BoundedSemaphore(max_concurrent_requests)
        self.auth_token = auth_token
        super().__init__(address, _Handler)

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self._request_slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def create_server(
    host: str,
    port: int,
    db_path: str | Path,
    *,
    max_database_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
    request_timeout: float = 10.0,
    max_concurrent_requests: int = 64,
    auth_token: str | None = None,
) -> ObservatoryHTTPServer:
    return ObservatoryHTTPServer(
        (host, port),
        ObservatoryApplication(EventStore(db_path, max_bytes=max_database_bytes)),
        request_timeout=request_timeout,
        max_concurrent_requests=max_concurrent_requests,
        auth_token=auth_token,
    )


def serve(
    host: str = "127.0.0.1",
    port: int = 8787,
    db_path: str | Path = "observatory.sqlite3",
    *,
    max_database_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
    request_timeout: float = 10.0,
    max_concurrent_requests: int = 64,
    auth_token: str | None = None,
) -> None:
    server = create_server(
        host,
        port,
        db_path,
        max_database_bytes=max_database_bytes,
        request_timeout=request_timeout,
        max_concurrent_requests=max_concurrent_requests,
        auth_token=auth_token,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
        server.application.store.close()
