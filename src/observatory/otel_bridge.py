"""Minimal OTLP/JSON-to-normalized-event bridge.

The Collector remains responsible for OTLP transport, batching, retry, and
durable queues. This module only converts the metadata-safe JSON representation
received from the Collector into the Observatory contract.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import math
from typing import Any, Iterator, Mapping

from .clock import utc_now
from .contracts import stable_event_id
from .intake import Intake, IntakeResult
from .project import sanitize_remote
from .store import EventStore


def _attribute_value(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    for key in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
        if key in value:
            raw = value[key]
            if key == "intValue":
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    return None
            if key == "doubleValue":
                try:
                    number = float(raw)
                except (TypeError, ValueError):
                    return None
                return number if math.isfinite(number) else None
            return raw
    if "arrayValue" in value and isinstance(value["arrayValue"], Mapping):
        values = value["arrayValue"].get("values", [])
        return [_attribute_value(item) for item in values] if isinstance(values, list) else []
    if "kvlistValue" in value and isinstance(value["kvlistValue"], Mapping):
        values = value["kvlistValue"].get("values", [])
        return _attributes(values) if isinstance(values, list) else {}
    return None


def _attributes(values: Any) -> dict[str, Any]:
    if not isinstance(values, list):
        return {}
    result: dict[str, Any] = {}
    for item in values:
        if not isinstance(item, Mapping) or not isinstance(item.get("key"), str):
            continue
        result[item["key"]] = _attribute_value(item.get("value"))
    return result


def _time(value: Any) -> str:
    try:
        nanos = int(value)
        return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError, OverflowError):
        return utc_now().isoformat().replace("+00:00", "Z")


def _duration_ms(start: Any, end: Any) -> float | None:
    try:
        delta = int(end) - int(start)
        return max(delta, 0) / 1_000_000
    except (TypeError, ValueError):
        return None


def _resource_attributes(resource: Any) -> dict[str, Any]:
    return _attributes(resource.get("attributes", [])) if isinstance(resource, Mapping) else {}


def _source_name(resource_attrs: Mapping[str, Any], scope: Mapping[str, Any]) -> str:
    return str(resource_attrs.get("service.name") or scope.get("name") or "otlp")


def _first_value(values: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = values.get(key)
        if value is not None and value != "":
            return value
    return default


def _text(value: Any, default: str | None = None) -> str | None:
    if value is None or value == "":
        return default
    return str(value)


def _flag(value: Any) -> bool:
    return value is True or value == 1 or (isinstance(value, str) and value.casefold() in {"true", "1", "yes"})


def _rate_limited(value: Any) -> bool:
    if isinstance(value, list):
        return any("rate" in str(item).casefold() for item in value)
    return "rate" in str(value or "").casefold()


def _safe_attributes(resource_attrs: Mapping[str, Any], attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Retain bounded diagnostic dimensions without copying arbitrary payloads."""

    allowed = {
        "service.name", "service.version", "gen_ai.request.type", "gen_ai.response.finish_reasons",
        "gen_ai.request.model", "gen_ai.response.model", "gen_ai.operation.name", "gen_ai.tool.name",
        "gen_ai.tool.type", "error.type", "error.code", "llm.observatory.evidence.source",
    }
    merged = {**resource_attrs, **attrs}
    return {
        key: value
        for key, value in merged.items()
        if key in allowed or key.startswith("llm.observatory.")
    }


def _project_identity(identity: Mapping[str, Any]) -> dict[str, Any]:
    """Accept only explicit safe project dimensions from a client resource."""

    root = _first_value(identity, "llm.observatory.project.root", "llm.observatory.project.path")
    root_text = _text(root)
    explicit_id = _text(identity.get("llm.observatory.project.id"))
    if explicit_id:
        project_id = explicit_id
    elif root_text:
        digest = hashlib.sha256(root_text.replace("\\", "/").casefold().encode("utf-8")).hexdigest()
        project_id = f"local_sha256:{digest}"
    else:
        project_id = "project:unknown"
    repository = _text(identity.get("llm.observatory.project.repository"))
    remote = sanitize_remote(_text(identity.get("llm.observatory.project.remote")))
    branch = _text(identity.get("llm.observatory.project.branch"))
    commit = _text(identity.get("llm.observatory.project.commit"))
    worktree = _text(identity.get("llm.observatory.project.worktree"))
    if not worktree and root_text:
        digest = hashlib.sha256(root_text.replace("\\", "/").casefold().encode("utf-8")).hexdigest()
        worktree = f"worktree_sha256:{digest}"
    return {
        "project_id": project_id,
        "repository": repository,
        "root": root_text,
        "remote": remote,
        "branch": branch,
        "commit": commit,
        "worktree": worktree,
    }


class OTLPJsonBridge:
    def __init__(self, store: EventStore, *, max_records: int = 256) -> None:
        self.store = store
        self.intake = Intake(store, max_records=max_records)

    def ingest(self, signal: str, payload: Mapping[str, Any]) -> IntakeResult:
        # Stream the generator directly into bounded intake. Materializing an
        # attacker-controlled OTLP batch before Intake enforces max_records
        # would defeat the memory guard.
        return self.intake.ingest(self.iter_records(signal, payload))

    def iter_records(self, signal: str, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        if signal == "traces":
            yield from self._trace_records(payload)
        elif signal == "logs":
            yield from self._log_records(payload)
        elif signal == "metrics":
            yield from self._metric_records(payload)
        else:
            raise ValueError(f"unsupported OTLP signal: {signal}")

    def _trace_records(self, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        for resource_spans in payload.get("resourceSpans", []):
            if not isinstance(resource_spans, Mapping):
                continue
            resource = resource_spans.get("resource", {})
            resource_attrs = _resource_attributes(resource)
            schema_url = resource_spans.get("schemaUrl") or "gen_ai.experimental"
            for scope_spans in resource_spans.get("scopeSpans", []):
                if not isinstance(scope_spans, Mapping):
                    continue
                scope = scope_spans.get("scope", {})
                source_name = _source_name(resource_attrs, scope if isinstance(scope, Mapping) else {})
                for span in scope_spans.get("spans", []):
                    if not isinstance(span, Mapping):
                        continue
                    attrs = _attributes(span.get("attributes", []))
                    identity = {**resource_attrs, **attrs}
                    trace_id = _text(span.get("traceId"))
                    span_id = _text(span.get("spanId"))
                    name = str(span.get("name") or "otel.span")
                    event_id = f"otel:{trace_id}:{span_id}" if trace_id and span_id else stable_event_id({"span": span, "source": source_name})
                    provider = _text(_first_value(identity, "gen_ai.provider.name", "llm.observatory.provider", default="unknown"), "unknown")
                    model = _text(_first_value(identity, "gen_ai.response.model", "gen_ai.request.model", default="unknown"), "unknown")
                    operation_kind = str(_first_value(identity, "llm.observatory.operation.kind", "gen_ai.operation.name", default="")).casefold()
                    is_tool = operation_kind in {"tool", "tool.operation", "tool_call", "tool.call"} or name.casefold().startswith("tool.")
                    status = span.get("status") if isinstance(span.get("status"), Mapping) else {}
                    status_code = str(status.get("code") or "").upper()
                    reliability = "failed" if "ERROR" in status_code else "succeeded" if status_code else "unknown"
                    duration = _duration_ms(span.get("startTimeUnixNano"), span.get("endTimeUnixNano"))
                    source_evidence = str(_first_value(identity, "llm.observatory.evidence.source", default="unknown"))
                    agent_failure = _flag(identity.get("llm.observatory.agent.failure"))
                    aborted = _flag(identity.get("llm.observatory.aborted"))
                    timeout = str(identity.get("error.type") or "").casefold() in {"timeout", "deadline_exceeded"}
                    failed = "ERROR" in status_code or agent_failure or aborted or timeout
                    record = {
                        "schema_version": "1.0",
                        "event_id": event_id,
                        "event_type": "tool.operation" if is_tool else "model.operation" if "gen_ai" in name or provider != "unknown" else "otel.span",
                        "observed_at": _time(span.get("startTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name, "version": _text(scope.get("version")) if isinstance(scope, Mapping) else None},
                        "project": _project_identity(identity),
                        "execution": {
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "parent_event_id": _text(span.get("parentSpanId")),
                            "session_id": _text(_first_value(identity, "gen_ai.conversation.id", "llm.observatory.session.id", "session.id")),
                            "workflow_id": _text(identity.get("llm.observatory.workflow.id")),
                            "agent_id": _text(identity.get("llm.observatory.agent.id")),
                            "subagent_id": _text(identity.get("llm.observatory.subagent.id")),
                            "role": _text(identity.get("llm.observatory.role")),
                            "skill": _text(identity.get("llm.observatory.skill")),
                            "lane": _text(identity.get("llm.observatory.lane")),
                            "task_id": _text(identity.get("llm.observatory.task.id")),
                            "task_class": _text(identity.get("llm.observatory.task.class")),
                        },
                        "llm": {"provider": provider, "model": model, "model_family": _text(identity.get("llm.observatory.model.family")), "client": _text(_first_value(identity, "llm.observatory.client", default=source_name), source_name), "auth_mode": _text(identity.get("llm.observatory.auth.mode"), "unknown"), "route": _text(identity.get("llm.observatory.route.kind"), "unknown"), "reasoning_effort": _text(identity.get("gen_ai.request.effort"))},
                        "usage": {
                            "input_tokens": attrs.get("gen_ai.usage.input_tokens"),
                            "output_tokens": attrs.get("gen_ai.usage.output_tokens"),
                            "cached_tokens": attrs.get("gen_ai.usage.cache_read.input_tokens"),
                            "cache_creation_tokens": attrs.get("gen_ai.usage.cache_creation.input_tokens"),
                            "cache_read_tokens": attrs.get("gen_ai.usage.cache_read.input_tokens"),
                            "reasoning_tokens": attrs.get("gen_ai.usage.reasoning_tokens"),
                            "total_tokens": attrs.get("gen_ai.usage.total_tokens"),
                            "cost": attrs.get("gen_ai.usage.cost"),
                            "context_size": attrs.get("llm.observatory.context.size"),
                            "context_utilization": attrs.get("llm.observatory.context.utilization"),
                            "compaction_count": attrs.get("llm.observatory.compaction.count"),
                            "source": source_evidence,
                        },
                        "performance": {
                            "latency_ms": duration,
                            "duration_ms": duration,
                            "tool_duration_ms": attrs.get("llm.observatory.tool.duration_ms"),
                            "session_duration_ms": attrs.get("llm.observatory.session.duration_ms"),
                            "agent_duration_ms": attrs.get("llm.observatory.agent.duration_ms"),
                            "workflow_duration_ms": attrs.get("llm.observatory.workflow.duration_ms"),
                            "wall_clock_ms": attrs.get("llm.observatory.wall_clock_ms"),
                            "concurrency": attrs.get("llm.observatory.concurrency"),
                            "parallel_utilization": attrs.get("llm.observatory.parallel_utilization"),
                        },
                        "reliability": {
                            "status": "failed" if failed else reliability,
                            "error_kind": _text(identity.get("error.type")),
                            "retry_count": identity.get("llm.observatory.retry.count"),
                            "rate_limited": _rate_limited(identity.get("gen_ai.response.finish_reasons")),
                            "timeout": timeout,
                            "tool_failure": _flag(identity.get("llm.observatory.tool.failure")),
                            "aborted": aborted,
                        },
                        "provenance": {"fields": {"llm.provider": "client", "llm.model": "client", "usage": source_evidence}, "adapter": "otlp-json", "semantic_conventions": str(schema_url), "content_capture": "disabled"},
                        "attributes": _safe_attributes(resource_attrs, attrs),
                    }
                    yield record

    def _log_records(self, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        for resource_logs in payload.get("resourceLogs", []):
            if not isinstance(resource_logs, Mapping):
                continue
            resource_attrs = _resource_attributes(resource_logs.get("resource", {}))
            for scope_logs in resource_logs.get("scopeLogs", []):
                if not isinstance(scope_logs, Mapping):
                    continue
                for log_record in scope_logs.get("logRecords", []):
                    if not isinstance(log_record, Mapping):
                        continue
                    attrs = _attributes(log_record.get("attributes", []))
                    identity = {**resource_attrs, **attrs}
                    source_name = _source_name(resource_attrs, scope_logs.get("scope", {}))
                    event_id = stable_event_id({"log": log_record, "resource": resource_attrs, "source": source_name})
                    yield {
                        "schema_version": "1.0",
                        "event_id": event_id,
                        "event_type": "telemetry.log",
                        "observed_at": _time(log_record.get("timeUnixNano") or log_record.get("observedTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name},
                        "project": _project_identity(identity),
                        "execution": {"trace_id": _text(log_record.get("traceId")), "span_id": _text(log_record.get("spanId"))},
                        "reliability": {"status": "unknown"},
                        "provenance": {"fields": {"log": "client"}, "adapter": "otlp-json", "semantic_conventions": str(resource_logs.get("schemaUrl") or "otel"), "content_capture": "disabled"},
                        "attributes": {key: value for key, value in attrs.items() if key.startswith("llm.observatory.") or key in {"severity.text", "severity.number"}},
                    }

    def _metric_records(self, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        for resource_metrics in payload.get("resourceMetrics", []):
            if not isinstance(resource_metrics, Mapping):
                continue
            resource_attrs = _resource_attributes(resource_metrics.get("resource", {}))
            source_name = _source_name(resource_attrs, {})
            for scope_metrics in resource_metrics.get("scopeMetrics", []):
                if not isinstance(scope_metrics, Mapping):
                    continue
                for metric in scope_metrics.get("metrics", []):
                    if not isinstance(metric, Mapping):
                        continue
                    name = str(metric.get("name") or "unknown.metric")
                    yield {
                        "schema_version": "1.0",
                        "event_id": stable_event_id({"metric": metric, "resource": resource_attrs, "source": source_name}),
                        "event_type": "telemetry.metric",
                        "observed_at": utc_now().isoformat().replace("+00:00", "Z"),
                        "source": {"kind": "native_otel", "name": source_name},
                        "project": _project_identity(resource_attrs),
                        "reliability": {"status": "unknown"},
                        "provenance": {"fields": {"metric": "client"}, "adapter": "otlp-json", "semantic_conventions": str(resource_metrics.get("schemaUrl") or "otel"), "content_capture": "disabled"},
                        "attributes": {"metric.name": name, "metric.type": next((key for key in ("gauge", "sum", "histogram", "exponentialHistogram") if key in metric), "unknown")},
                    }
