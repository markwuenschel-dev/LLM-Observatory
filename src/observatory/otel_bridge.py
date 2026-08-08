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

from .contracts import canonical_json, stable_event_id
from .intake import Intake, IntakeResult
from .privacy import PrivacyPolicy, redact_mapping
from .project import sanitize_remote
from .store import EventStore


_MAX_ATTRIBUTES = 256
_MAX_ATTRIBUTE_ITEMS = 256
_MAX_ATTRIBUTE_STRING = 512
_UNKNOWN_METADATA_POLICY = PrivacyPolicy()


_METRIC_IDENTITY_ATTRIBUTES = frozenset({
    "gen_ai.conversation.id", "gen_ai.provider.name", "gen_ai.system", "gen_ai.request.model",
    "gen_ai.response.model", "gen_ai.request.model.version", "gen_ai.response.model.version",
    "gen_ai.model.version", "provider", "model", "client", "session.id", "session_id", "workflow.run_id",
    "workflow_id", "agent_id", "subagent_id", "parent_agent_id", "llm.observatory.project.id",
    "llm.observatory.project.root", "llm.observatory.project.path", "llm.observatory.project.repository",
    "llm.observatory.project.branch", "llm.observatory.project.commit", "llm.observatory.project.worktree",
    "llm.observatory.provider", "llm.observatory.client", "llm.observatory.model.family",
    "llm.observatory.model.variant", "llm.observatory.session.id", "llm.observatory.workflow.id",
    "llm.observatory.agent.id", "llm.observatory.subagent.id", "llm.observatory.parent.agent.id",
    "llm.observatory.role", "llm.observatory.skill", "llm.observatory.lane", "llm.observatory.task.id",
    "llm.observatory.task.class", "llm.observatory.auth.mode", "llm.observatory.route.kind",
})


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
            if isinstance(raw, str):
                return raw[:_MAX_ATTRIBUTE_STRING] + ("…" if len(raw) > _MAX_ATTRIBUTE_STRING else "")
            return raw
    if "arrayValue" in value and isinstance(value["arrayValue"], Mapping):
        values = value["arrayValue"].get("values", [])
        return [_attribute_value(item) for item in values[:_MAX_ATTRIBUTE_ITEMS]] if isinstance(values, list) else []
    if "kvlistValue" in value and isinstance(value["kvlistValue"], Mapping):
        values = value["kvlistValue"].get("values", [])
        return _attributes(values) if isinstance(values, list) else {}
    return None


def _attributes(values: Any) -> dict[str, Any]:
    if not isinstance(values, list):
        return {}
    result: dict[str, Any] = {}
    for item in values[:_MAX_ATTRIBUTES]:
        if not isinstance(item, Mapping) or not isinstance(item.get("key"), str):
            continue
        result[item["key"]] = _attribute_value(item.get("value"))
    return result


def _time(value: Any) -> str:
    if value is None or isinstance(value, bool):
        raise ValueError("OTLP timestamp is required")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError("OTLP timestamp must be an integer number of nanoseconds")
    try:
        nanos = int(value)
        return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError, OverflowError):
        raise ValueError("OTLP timestamp is invalid") from None


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


def _source_version(resource_attrs: Mapping[str, Any], scope: Mapping[str, Any]) -> str | None:
    return _text(scope.get("version")) or _text(resource_attrs.get("service.version"))


_KNOWN_SERVICE_PROVIDERS = {
    "claude-code": "anthropic",
    "claude-code-desktop": "anthropic",
}


def _provider(identity: Mapping[str, Any], source_name: str) -> str:
    explicit = _text(_first_value(identity, "gen_ai.provider.name", "gen_ai.system", "llm.observatory.provider", "provider"))
    if explicit is not None:
        return explicit
    return _KNOWN_SERVICE_PROVIDERS.get(source_name.casefold(), "unknown")


def _model(identity: Mapping[str, Any]) -> str:
    return _text(_first_value(identity, "gen_ai.response.model", "gen_ai.request.model", "model"), "unknown") or "unknown"


def _flag(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.casefold().strip()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _metric_number(value: Any) -> int | float | None:
    numeric = _number(value)
    if numeric is not None:
        return numeric
    if isinstance(value, str):
        text = value.strip()
        try:
            if text and text.lstrip("+-").isdigit():
                return int(text, 10)
            parsed = float(text)
            return parsed if math.isfinite(parsed) else None
        except ValueError:
            return None
    return None


def _retry_count(identity: Mapping[str, Any]) -> int | float | None:
    reported = _metric_number(identity.get("llm.observatory.retry.count"))
    if reported is not None:
        return reported
    attempts = _metric_number(identity.get("attempt"))
    if attempts is None:
        return None
    return max(attempts - 1, 0)


def _rate_limited(value: Any) -> bool:
    if isinstance(value, list):
        return any(_rate_limited(item) for item in value)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value) == 429
    text = str(value or "").casefold()
    if text.isdigit():
        return int(text) == 429
    return any(marker in text for marker in ("rate", "too_many", "resource_exhausted"))


_SAFE_OBSERVATORY_ATTRIBUTES = frozenset({
    "llm.observatory.evidence.source", "llm.observatory.project.id", "llm.observatory.project.repository",
    "llm.observatory.project.branch", "llm.observatory.project.commit", "llm.observatory.provider",
    "llm.observatory.client", "llm.observatory.model.family", "llm.observatory.model.variant",
    "llm.observatory.operation.kind", "llm.observatory.auth.mode", "llm.observatory.route.kind",
    "llm.observatory.session.id", "llm.observatory.workflow.id", "llm.observatory.agent.id",
    "llm.observatory.subagent.id", "llm.observatory.parent.agent.id", "llm.observatory.role", "llm.observatory.skill", "llm.observatory.lane",
    "llm.observatory.task.id", "llm.observatory.task.class", "llm.observatory.context.size",
    "llm.observatory.context.utilization", "llm.observatory.compaction.count", "llm.observatory.latency_ms",
    "llm.observatory.time_to_first_token_ms", "llm.observatory.duration_ms", "llm.observatory.tool.duration_ms",
    "llm.observatory.session.duration_ms", "llm.observatory.agent.duration_ms", "llm.observatory.workflow.duration_ms",
    "llm.observatory.wall_clock_ms", "llm.observatory.concurrency", "llm.observatory.parallel_utilization",
    "llm.observatory.retry.count", "llm.observatory.agent.failure", "llm.observatory.aborted",
    "llm.observatory.reassessment.count", "llm.observatory.rework.count",
    "llm.observatory.tool.failure",
    "llm.observatory.tool.call.count", "llm.observatory.tool.name", "llm.observatory.tool.names",
    "llm.observatory.extensions", "llm.observatory.error.kind", "llm.observatory.rate_limited",
    "llm.observatory.files.inspected.count", "llm.observatory.files.changed.count",
    "llm.observatory.commands.executed.count", "llm.observatory.tests.invoked.count",
})


_OTEL_ALLOWED_ATTRIBUTES = frozenset({
    "service.name", "service.version", "gen_ai.request.type", "gen_ai.response.finish_reasons",
    "gen_ai.provider.name", "gen_ai.system", "gen_ai.request.model", "gen_ai.response.model",
    "gen_ai.request.model.version", "gen_ai.response.model.version", "gen_ai.operation.name",
    "gen_ai.tool.name", "gen_ai.tool.type", "error.type", "error.code", "llm.observatory.evidence.source",
    "llm.observatory.acceptance.run_id", "event.name", "model", "provider", "client", "session_id",
    "workflow_id", "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "cost_usd",
    "cost_usd_micros", "duration_ms", "ttft_ms", "event.sequence", "success", "status_code", "attempt",
    "query_source", "agent_id", "parent_agent_id", "workflow.run_id",
})


def _safe_attributes(resource_attrs: Mapping[str, Any], attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Retain bounded diagnostic dimensions without copying arbitrary payloads."""

    merged = {**resource_attrs, **attrs}
    return {
        key: value
        for key, value in merged.items()
        if key in _OTEL_ALLOWED_ATTRIBUTES or key in _SAFE_OBSERVATORY_ATTRIBUTES
    }


def _unknown_attributes(resource_attrs: Mapping[str, Any], attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Retain future scalar dimensions after applying the metadata privacy policy."""

    merged = {**resource_attrs, **attrs}
    unknown = {
        key: value
        for key, value in merged.items()
        if key not in _OTEL_ALLOWED_ATTRIBUTES and key not in _SAFE_OBSERVATORY_ATTRIBUTES
    }
    if not unknown:
        return {}
    return redact_mapping(unknown, _UNKNOWN_METADATA_POLICY)


def _extensions(resource_attrs: Mapping[str, Any], attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Carry bounded client extension bags and unknown dimensions forward safely."""

    extensions: dict[str, Any] = {}
    unknown = _unknown_attributes(resource_attrs, attrs)
    if unknown:
        extensions["unknown_attributes"] = unknown
    extension_bag = _first_value({**resource_attrs, **attrs}, "llm.observatory.extensions")
    if isinstance(extension_bag, Mapping):
        extensions["client"] = redact_mapping(extension_bag, _UNKNOWN_METADATA_POLICY)
    return extensions


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


def _model_variant(identity: Mapping[str, Any]) -> str | None:
    return _text(_first_value(
        identity,
        "llm.observatory.model.variant",
        "gen_ai.response.model.version",
        "gen_ai.request.model.version",
        "gen_ai.model.version",
    ))


def _behavior(identity: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    count_keys = {
        "tool_call_count": "llm.observatory.tool.call.count",
        "files_inspected_count": "llm.observatory.files.inspected.count",
        "files_changed_count": "llm.observatory.files.changed.count",
        "commands_executed_count": "llm.observatory.commands.executed.count",
        "tests_invoked_count": "llm.observatory.tests.invoked.count",
    }
    for output_key, input_key in count_keys.items():
        number = _metric_number(identity.get(input_key))
        if number is not None:
            result[output_key] = number
    names = _first_value(identity, "llm.observatory.tool.names", "llm.observatory.tool.name", "gen_ai.tool.name")
    if names is not None:
        result["tool_names"] = names if isinstance(names, list) else [names]
        if "tool_call_count" not in result:
            result["tool_call_count"] = len(result["tool_names"])
    return result


def _reliability(identity: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    agent_failure = _flag(identity.get("llm.observatory.agent.failure"))
    if agent_failure is not None:
        result["agent_failure"] = agent_failure
    for output_key, input_key in {
        "reassessment_count": "llm.observatory.reassessment.count",
        "rework_count": "llm.observatory.rework.count",
    }.items():
        number = _metric_number(identity.get(input_key))
        if number is not None and number >= 0:
            result[output_key] = number
    return result


def _metric_point_group(point_attributes: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    """Return normalized identity, retained unknowns, and a stable group key."""

    safe_attributes = _safe_attributes({}, point_attributes)
    unknown_attributes = _unknown_attributes({}, point_attributes)
    identity = {
        key: value
        for key, value in safe_attributes.items()
        if key in _METRIC_IDENTITY_ATTRIBUTES
    }
    group_payload = {"safe": safe_attributes, "unknown": unknown_attributes}
    group_key = canonical_json(group_payload)
    digest = hashlib.sha256(group_key.encode("utf-8")).hexdigest()
    return identity, unknown_attributes, group_key, digest


def _metric_record(
    *,
    resource_attrs: Mapping[str, Any],
    scope: Mapping[str, Any],
    source_name: str,
    schema_url: Any,
    name: str,
    metric_type: str,
    unit: Any,
    point_summaries: list[dict[str, Any]],
    point_identity: Mapping[str, Any],
    point_unknown: Mapping[str, Any],
    point_context_sha256: str,
) -> dict[str, Any]:
    observed_at = _time(point_summaries[0]["time_unix_nano"])
    attributes: dict[str, Any] = {
        "metric.name": name,
        "metric.type": metric_type,
        "metric.points": point_summaries,
    }
    if unit is not None:
        attributes["metric.unit"] = str(unit)
    identity = {**resource_attrs, **point_identity}
    source = {"kind": "native_otel", "name": source_name, "version": _source_version(resource_attrs, scope)}
    project = _project_identity(identity)
    provider = _provider(identity, source_name)
    model = _model(identity)
    source_evidence = _text(_first_value(identity, "llm.observatory.evidence.source", default="unknown"), "unknown")
    execution = {
        "session_id": _text(_first_value(identity, "gen_ai.conversation.id", "llm.observatory.session.id", "session.id", "session_id")),
        "workflow_id": _text(_first_value(identity, "llm.observatory.workflow.id", "workflow.run_id", "workflow_id")),
        "agent_id": _text(_first_value(identity, "llm.observatory.agent.id", "agent_id")),
        "subagent_id": _text(_first_value(identity, "llm.observatory.subagent.id", "subagent_id")),
        "parent_agent_id": _text(_first_value(identity, "llm.observatory.parent.agent.id", "parent_agent_id")),
        "role": _text(identity.get("llm.observatory.role")),
        "skill": _text(identity.get("llm.observatory.skill")),
        "lane": _text(identity.get("llm.observatory.lane")),
        "task_id": _text(identity.get("llm.observatory.task.id")),
        "task_class": _text(identity.get("llm.observatory.task.class")),
    }
    llm = {
        "provider": provider,
        "model": model,
        "model_family": _text(identity.get("llm.observatory.model.family")),
        "model_variant": _model_variant(identity),
        "client": _text(_first_value(identity, "llm.observatory.client", "client", default=source_name), source_name),
        "auth_mode": _text(identity.get("llm.observatory.auth.mode"), "unknown"),
        "route": _text(identity.get("llm.observatory.route.kind"), "unknown"),
        "reasoning_effort": _text(identity.get("gen_ai.request.effort")),
    }
    usage = {
        "input_tokens": _number(_first_value(identity, "gen_ai.usage.input_tokens", "input_tokens")),
        "output_tokens": _number(_first_value(identity, "gen_ai.usage.output_tokens", "output_tokens")),
        "cached_tokens": _number(_first_value(identity, "gen_ai.usage.cache_read.input_tokens", "cache_read_tokens")),
        "cache_creation_tokens": _number(_first_value(identity, "gen_ai.usage.cache_creation.input_tokens", "cache_creation_tokens")),
        "total_tokens": _number(_first_value(identity, "gen_ai.usage.total_tokens", "total_tokens")),
        "cost": _number(_first_value(identity, "gen_ai.usage.cost", "cost_usd")),
        "source": source_evidence,
    }
    performance = {
        "latency_ms": _metric_number(_first_value(identity, "llm.observatory.latency_ms", "gen_ai.client.latency_ms")),
        "time_to_first_token_ms": _metric_number(_first_value(identity, "llm.observatory.time_to_first_token_ms", "gen_ai.time_to_first_token_ms", "ttft_ms")),
        "duration_ms": _metric_number(_first_value(identity, "llm.observatory.duration_ms", "gen_ai.client.operation.duration", "duration_ms")),
    }
    extensions = _extensions(resource_attrs, point_identity)
    if point_unknown:
        extensions["metric_point_attributes"] = dict(point_unknown)
    return {
        "schema_version": "1.0",
        "event_id": stable_event_id({
            "event_type": "telemetry.metric",
            "observed_at": observed_at,
            "source": source,
            "project": project,
            "execution": execution,
            "llm": llm,
            "attributes": {
                "metric_name": name,
                "metric_type": metric_type,
                "metric_unit": unit,
                "point_count": len(point_summaries),
                "first_time_unix_nano": point_summaries[0].get("time_unix_nano"),
                "last_time_unix_nano": point_summaries[-1].get("time_unix_nano"),
                "metric_context_sha256": point_context_sha256,
            },
        }),
        "event_type": "telemetry.metric",
        "observed_at": observed_at,
        "source": source,
        "project": project,
        "execution": execution,
        "llm": llm,
        "usage": usage,
        "performance": performance,
        "reliability": {"status": "unknown", **_reliability(identity)},
        "behavior": _behavior(identity),
        "provenance": {"fields": {"metric": "client", "metric.value": "client"}, "adapter": "otlp-json", "semantic_conventions": str(schema_url or "otel"), "content_capture": "disabled"},
        "attributes": attributes,
        "extensions": extensions,
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
        resource_spans_list = payload.get("resourceSpans")
        if not isinstance(resource_spans_list, list):
            # Emit a rejected marker so a malformed batch is never
            # acknowledged as an empty success, while keeping the bridge
            # generator fail-open for valid siblings.
            yield {}
            return
        saw_span = False
        for resource_spans in resource_spans_list:
            if not isinstance(resource_spans, Mapping):
                saw_span = True
                yield {}
                continue
            resource = resource_spans.get("resource", {})
            resource_attrs = _resource_attributes(resource)
            schema_url = resource_spans.get("schemaUrl") or "gen_ai.experimental"
            scope_spans_list = resource_spans.get("scopeSpans")
            if not isinstance(scope_spans_list, list):
                saw_span = True
                yield {}
                continue
            for scope_spans in scope_spans_list:
                if not isinstance(scope_spans, Mapping):
                    saw_span = True
                    yield {}
                    continue
                spans = scope_spans.get("spans")
                if not isinstance(spans, list):
                    saw_span = True
                    yield {}
                    continue
                scope = scope_spans.get("scope", {})
                source_name = _source_name(resource_attrs, scope if isinstance(scope, Mapping) else {})
                for span in spans:
                    if not isinstance(span, Mapping):
                        saw_span = True
                        yield {}
                        continue
                    saw_span = True
                    try:
                        _time(span.get("startTimeUnixNano"))
                    except ValueError:
                        # Reject only this span so valid siblings in the
                        # same OTLP batch remain ingestible.
                        yield {}
                        continue
                    attrs = _attributes(span.get("attributes", []))
                    identity = {**resource_attrs, **attrs}
                    trace_id = _text(span.get("traceId"))
                    span_id = _text(span.get("spanId"))
                    name = str(span.get("name") or "otel.span")
                    event_id = f"otel:{trace_id}:{span_id}" if trace_id and span_id else stable_event_id({
                        "event_type": "otel.span",
                        "observed_at": _time(span.get("startTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name},
                        "execution": {"trace_id": trace_id, "span_id": span_id},
                        "attributes": {"span_name": name},
                    })
                    provider = _provider(identity, source_name)
                    model = _model(identity)
                    operation_kind = str(_first_value(identity, "llm.observatory.operation.kind", "gen_ai.operation.name", default="")).casefold()
                    is_tool = operation_kind in {"tool", "tool.operation", "tool_call", "tool.call"} or name.casefold().startswith("tool.")
                    status = span.get("status") if isinstance(span.get("status"), Mapping) else {}
                    status_code = str(status.get("code") or "").upper()
                    status_failed = status_code in {"2", "ERROR", "STATUS_CODE_ERROR"} or status_code.endswith("_ERROR")
                    status_succeeded = status_code in {"1", "OK", "STATUS_CODE_OK"} or status_code.endswith("_OK")
                    reliability = "failed" if status_failed else "succeeded" if status_succeeded else "unknown"
                    duration = _duration_ms(span.get("startTimeUnixNano"), span.get("endTimeUnixNano"))
                    latency = _metric_number(_first_value(identity, "llm.observatory.latency_ms", "gen_ai.client.latency_ms"))
                    time_to_first_token = _metric_number(_first_value(identity, "llm.observatory.time_to_first_token_ms", "gen_ai.time_to_first_token_ms", "ttft_ms"))
                    source_evidence = str(_first_value(identity, "llm.observatory.evidence.source", default="unknown"))
                    agent_failure = _flag(identity.get("llm.observatory.agent.failure"))
                    aborted = _flag(identity.get("llm.observatory.aborted"))
                    reassessment_count = _metric_number(identity.get("llm.observatory.reassessment.count"))
                    rework_count = _metric_number(identity.get("llm.observatory.rework.count"))
                    error_type_signal = _first_value(identity, "error.type")
                    explicit_error_kind = _first_value(identity, "llm.observatory.error.kind", "error.kind", "error.type")
                    error_signal = _first_value(identity, "error.type", "error.code")
                    error_code = _metric_number(_first_value(identity, "error.code", "http.response.status_code", "http.status_code", "status_code"))
                    error_text = str(error_signal or "").casefold()
                    error_kind = _text(explicit_error_kind)
                    if error_kind is None:
                        error_kind = "rate_limited" if _rate_limited(error_signal) or error_code == 429 else "timeout" if error_text in {"timeout", "timed_out", "deadline_exceeded"} or error_code in {408, 504} else _text(error_signal)
                    explicit_rate_limited = _first_value(identity, "llm.observatory.rate_limited")
                    if explicit_rate_limited is not None:
                        rate_limited = _flag(explicit_rate_limited)
                        rate_limited_source = "client" if rate_limited is not None else "unknown"
                    else:
                        rate_limited_signal = _first_value(identity, "gen_ai.response.finish_reasons", "error.type")
                        rate_limited = _rate_limited(rate_limited_signal) if rate_limited_signal is not None else None
                        rate_limited = True if _rate_limited(error_signal) or error_code == 429 else rate_limited
                        rate_limited_source = "client" if rate_limited is True and rate_limited_signal is not None and _rate_limited(rate_limited_signal) else "derived" if rate_limited is True else "unknown"
                    timeout = True if error_kind == "timeout" else None
                    error_kind_source = "client" if error_kind is not None and (explicit_error_kind is not None or error_type_signal is not None) else "derived" if error_kind is not None else "unknown"
                    tool_failure_signal = identity.get("llm.observatory.tool.failure")
                    aborted_signal = identity.get("llm.observatory.aborted")
                    tool_failure = _flag(tool_failure_signal)
                    status_failed = status_failed or (error_code is not None and error_code >= 400)
                    success = _flag(identity.get("success"))
                    failed = status_failed or agent_failure is True or tool_failure is True or aborted is True or timeout is True or rate_limited is True or success is False
                    retry_signal = _first_value(identity, "llm.observatory.retry.count", "attempt")
                    retry_count = _retry_count(identity)
                    parent_span_id = _text(span.get("parentSpanId"))
                    parent_event_id = (
                        f"otel:{trace_id}:{parent_span_id}"
                        if trace_id and parent_span_id and not parent_span_id.startswith("otel:")
                        else parent_span_id
                    )
                    record = {
                        "schema_version": "1.0",
                        "event_id": event_id,
                        "event_type": "tool.operation" if is_tool else "model.operation" if "gen_ai" in name or provider != "unknown" else "otel.span",
                        "observed_at": _time(span.get("startTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name, "version": _source_version(resource_attrs, scope)},
                        "project": _project_identity(identity),
                        "execution": {
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "parent_event_id": parent_event_id,
                            "session_id": _text(_first_value(identity, "gen_ai.conversation.id", "llm.observatory.session.id", "session.id", "session_id")),
                            "workflow_id": _text(_first_value(identity, "llm.observatory.workflow.id", "workflow.run_id", "workflow_id")),
                            "agent_id": _text(_first_value(identity, "llm.observatory.agent.id", "agent_id")),
                            "subagent_id": _text(identity.get("llm.observatory.subagent.id")),
                            "parent_agent_id": _text(_first_value(identity, "llm.observatory.parent.agent.id", "parent_agent_id")),
                            "role": _text(identity.get("llm.observatory.role")),
                            "skill": _text(identity.get("llm.observatory.skill")),
                            "lane": _text(identity.get("llm.observatory.lane")),
                            "task_id": _text(identity.get("llm.observatory.task.id")),
                            "task_class": _text(identity.get("llm.observatory.task.class")),
                        },
                        "llm": {"provider": provider, "model": model, "model_family": _text(identity.get("llm.observatory.model.family")), "model_variant": _model_variant(identity), "client": _text(_first_value(identity, "llm.observatory.client", "client", default=source_name), source_name), "auth_mode": _text(identity.get("llm.observatory.auth.mode"), "unknown"), "route": _text(identity.get("llm.observatory.route.kind"), "unknown"), "reasoning_effort": _text(identity.get("gen_ai.request.effort"))},
                        "usage": {
                            "input_tokens": _first_value(attrs, "gen_ai.usage.input_tokens", "input_tokens"),
                            "output_tokens": _first_value(attrs, "gen_ai.usage.output_tokens", "output_tokens"),
                            "cached_tokens": _first_value(attrs, "gen_ai.usage.cached_tokens", "cache_read_tokens"),
                            "cache_creation_tokens": _first_value(attrs, "gen_ai.usage.cache_creation.input_tokens", "cache_creation_tokens"),
                            "cache_read_tokens": _first_value(attrs, "gen_ai.usage.cache_read.input_tokens", "cache_read_tokens"),
                            "reasoning_tokens": attrs.get("gen_ai.usage.reasoning_tokens"),
                            "total_tokens": attrs.get("gen_ai.usage.total_tokens"),
                            "cost": _first_value(attrs, "gen_ai.usage.cost", "cost_usd"),
                            "context_size": attrs.get("llm.observatory.context.size"),
                            "context_utilization": attrs.get("llm.observatory.context.utilization"),
                            "compaction_count": attrs.get("llm.observatory.compaction.count"),
                            "source": source_evidence,
                        },
                        "performance": {
                            "latency_ms": latency,
                            "duration_ms": duration,
                            "time_to_first_token_ms": time_to_first_token,
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
                            "error_kind": error_kind,
                            "retry_count": retry_count,
                            "rate_limited": rate_limited,
                            "timeout": timeout,
                            "tool_failure": tool_failure,
                            "agent_failure": agent_failure,
                            "aborted": aborted,
                            "reassessment_count": reassessment_count,
                            "rework_count": rework_count,
                        },
                        "behavior": _behavior(identity),
                        "provenance": {
                            "fields": {
                                "llm.provider": "client",
                                "llm.model": "client",
                                "llm.model_variant": "client" if _model_variant(identity) is not None else "unknown",
                                "usage": source_evidence,
                                "performance.latency_ms": "client" if latency is not None else "unknown",
                                "performance.duration_ms": "derived" if duration is not None else "unknown",
                                "performance.time_to_first_token_ms": "client" if time_to_first_token is not None else "unknown",
                                "reliability.error_kind": error_kind_source,
                                "reliability.retry_count": "client" if retry_signal is not None and retry_count is not None else "unknown",
                                "reliability.rate_limited": rate_limited_source,
                                "reliability.timeout": "client" if timeout is not None and error_signal is not None else "derived" if timeout is True else "unknown",
                                "reliability.tool_failure": "client" if tool_failure_signal is not None and tool_failure is not None else "unknown",
                                "reliability.agent_failure": "client" if identity.get("llm.observatory.agent.failure") is not None and agent_failure is not None else "unknown",
                                "reliability.aborted": "client" if aborted_signal is not None and aborted is not None else "unknown",
                                "reliability.reassessment_count": "client" if reassessment_count is not None else "unknown",
                                "reliability.rework_count": "client" if rework_count is not None else "unknown",
                                "performance.tool_duration_ms": "client" if attrs.get("llm.observatory.tool.duration_ms") is not None else "unknown",
                                "performance.session_duration_ms": "client" if attrs.get("llm.observatory.session.duration_ms") is not None else "unknown",
                                "performance.agent_duration_ms": "client" if attrs.get("llm.observatory.agent.duration_ms") is not None else "unknown",
                                "performance.workflow_duration_ms": "client" if attrs.get("llm.observatory.workflow.duration_ms") is not None else "unknown",
                                "performance.wall_clock_ms": "client" if attrs.get("llm.observatory.wall_clock_ms") is not None else "unknown",
                                "performance.concurrency": "client" if attrs.get("llm.observatory.concurrency") is not None else "unknown",
                                "performance.parallel_utilization": "client" if attrs.get("llm.observatory.parallel_utilization") is not None else "unknown",
                            },
                            "adapter": "otlp-json",
                            "semantic_conventions": str(schema_url),
                            "content_capture": "disabled",
                        },
                        "attributes": _safe_attributes(resource_attrs, attrs),
                        "extensions": _extensions(resource_attrs, attrs),
                    }
                    yield record
        if not saw_span:
            yield {}

    def _log_records(self, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        resource_logs_list = payload.get("resourceLogs")
        if not isinstance(resource_logs_list, list):
            yield {}
            return
        saw_log = False
        for resource_logs in resource_logs_list:
            if not isinstance(resource_logs, Mapping):
                saw_log = True
                yield {}
                continue
            resource_attrs = _resource_attributes(resource_logs.get("resource", {}))
            scope_logs_list = resource_logs.get("scopeLogs")
            if not isinstance(scope_logs_list, list):
                saw_log = True
                yield {}
                continue
            for scope_logs in scope_logs_list:
                if not isinstance(scope_logs, Mapping):
                    saw_log = True
                    yield {}
                    continue
                scope = scope_logs.get("scope", {})
                if not isinstance(scope, Mapping):
                    saw_log = True
                    yield {}
                    continue
                log_records = scope_logs.get("logRecords")
                if not isinstance(log_records, list):
                    saw_log = True
                    yield {}
                    continue
                for log_record in log_records:
                    if not isinstance(log_record, Mapping):
                        saw_log = True
                        yield {}
                        continue
                    saw_log = True
                    try:
                        _time(log_record.get("timeUnixNano") or log_record.get("observedTimeUnixNano"))
                    except ValueError:
                        yield {}
                        continue
                    attrs = _attributes(log_record.get("attributes", []))
                    identity = {**resource_attrs, **attrs}
                    source_name = _source_name(resource_attrs, scope)
                    event_id = stable_event_id({
                        "event_type": "telemetry.log",
                        "observed_at": _time(log_record.get("timeUnixNano") or log_record.get("observedTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name},
                        "execution": {"trace_id": log_record.get("traceId"), "span_id": log_record.get("spanId")},
                        "attributes": {
                            "provider": _provider(identity, source_name),
                            "model": _model(identity),
                            "severity": _first_value(identity, "severity.text"),
                        },
                    })
                    provider = _provider(identity, source_name)
                    model = _model(identity)
                    operation = _text(_first_value(identity, "llm.observatory.operation.kind", "gen_ai.operation.name", "event.name", default=""), "")
                    severity = str(_first_value(identity, "severity.text", default="")).casefold()
                    error_type_signal = _first_value(identity, "error.type")
                    explicit_error_kind = _first_value(identity, "llm.observatory.error.kind", "error.kind", "error.type")
                    error_signal = _first_value(identity, "error.type", "error.code", "http.response.status_code", "http.status_code")
                    error_code = _metric_number(_first_value(identity, "error.code", "http.response.status_code", "http.status_code"))
                    plain_status_code = _metric_number(identity.get("status_code"))
                    if error_code is None and plain_status_code is not None and plain_status_code >= 400:
                        error_code = plain_status_code
                        if error_signal is None:
                            error_signal = plain_status_code
                    error_text = str(error_signal or "").casefold()
                    explicit_rate_limited = _first_value(identity, "llm.observatory.rate_limited")
                    if explicit_rate_limited is not None:
                        rate_limited = _flag(explicit_rate_limited)
                        rate_limited_source = "client" if rate_limited is not None else "unknown"
                    else:
                        rate_limited_signal = _first_value(identity, "gen_ai.response.finish_reasons", "error.type")
                        rate_limited = _rate_limited(rate_limited_signal) if rate_limited_signal is not None else None
                        rate_limited = True if _rate_limited(error_signal) or error_code == 429 else rate_limited
                        rate_limited_source = "client" if rate_limited is True and rate_limited_signal is not None and _rate_limited(rate_limited_signal) else "derived" if rate_limited is True else "unknown"
                    timeout = True if error_text in {"timeout", "timed_out", "deadline_exceeded"} or error_code in {408, 504} else None
                    error_kind = _text(explicit_error_kind)
                    if error_kind is None:
                        error_kind = "rate_limited" if rate_limited else "timeout" if timeout else _text(_first_value(identity, "error.type", "error.code"))
                    error_kind_source = "client" if error_kind is not None and (explicit_error_kind is not None or error_type_signal is not None) else "derived" if error_kind is not None else "unknown"
                    agent_failure = _flag(identity.get("llm.observatory.agent.failure"))
                    tool_failure_signal = identity.get("llm.observatory.tool.failure")
                    aborted_signal = identity.get("llm.observatory.aborted")
                    tool_failure = _flag(tool_failure_signal)
                    aborted = _flag(aborted_signal)
                    reassessment_count = _metric_number(identity.get("llm.observatory.reassessment.count"))
                    rework_count = _metric_number(identity.get("llm.observatory.rework.count"))
                    success = _flag(identity.get("success"))
                    failed = bool(error_kind) or (error_code is not None and error_code >= 400) or severity in {"error", "fatal", "critical", "severe"} or timeout or agent_failure is True or tool_failure is True or aborted is True or success is False
                    source_evidence = _text(_first_value(identity, "llm.observatory.evidence.source", default="unknown"), "unknown")
                    is_tool = operation.casefold() in {"tool", "tool.operation", "tool_call", "tool.call", "tool_result", "tool_decision"}
                    event_type = "tool.operation" if is_tool else "model.operation" if provider != "unknown" or model != "unknown" else "telemetry.log"
                    duration = _number(_first_value(attrs, "llm.observatory.duration_ms", "gen_ai.client.operation.duration", "duration_ms"))
                    latency = _number(_first_value(attrs, "llm.observatory.latency_ms", "gen_ai.client.latency_ms"))
                    time_to_first_token = _number(_first_value(identity, "llm.observatory.time_to_first_token_ms", "gen_ai.time_to_first_token_ms", "ttft_ms"))
                    retry_signal = _first_value(identity, "llm.observatory.retry.count", "attempt")
                    retry_count = _retry_count(identity)
                    execution = {
                        "trace_id": _text(log_record.get("traceId")),
                        "span_id": _text(log_record.get("spanId")),
                        "session_id": _text(_first_value(identity, "gen_ai.conversation.id", "llm.observatory.session.id", "session.id", "session_id")),
                        "workflow_id": _text(_first_value(identity, "llm.observatory.workflow.id", "workflow.run_id", "workflow_id")),
                        "agent_id": _text(_first_value(identity, "llm.observatory.agent.id", "agent_id")),
                        "subagent_id": _text(identity.get("llm.observatory.subagent.id")),
                        "parent_agent_id": _text(_first_value(identity, "llm.observatory.parent.agent.id", "parent_agent_id")),
                        "role": _text(identity.get("llm.observatory.role")),
                        "skill": _text(identity.get("llm.observatory.skill")),
                        "lane": _text(identity.get("llm.observatory.lane")),
                        "task_id": _text(identity.get("llm.observatory.task.id")),
                        "task_class": _text(identity.get("llm.observatory.task.class")),
                    }
                    yield {
                        "schema_version": "1.0",
                        "event_id": event_id,
                        "event_type": event_type,
                        "observed_at": _time(log_record.get("timeUnixNano") or log_record.get("observedTimeUnixNano")),
                        "source": {"kind": "native_otel", "name": source_name, "version": _source_version(resource_attrs, scope)},
                        "project": _project_identity(identity),
                        "execution": execution,
                        "llm": {
                            "provider": provider,
                            "model": model,
                            "model_family": _text(identity.get("llm.observatory.model.family")),
                            "model_variant": _model_variant(identity),
                            "client": _text(_first_value(identity, "llm.observatory.client", "client", default=source_name), source_name),
                            "auth_mode": _text(identity.get("llm.observatory.auth.mode"), "unknown"),
                            "route": _text(identity.get("llm.observatory.route.kind"), "unknown"),
                            "reasoning_effort": _text(identity.get("gen_ai.request.effort")),
                        },
                        "usage": {
                            "input_tokens": _number(_first_value(attrs, "gen_ai.usage.input_tokens", "input_tokens")),
                            "output_tokens": _number(_first_value(attrs, "gen_ai.usage.output_tokens", "output_tokens")),
                            "cached_tokens": _number(_first_value(attrs, "gen_ai.usage.cached_tokens", "cache_read_tokens")),
                            "cache_creation_tokens": _number(_first_value(attrs, "gen_ai.usage.cache_creation.input_tokens", "cache_creation_tokens")),
                            "cache_read_tokens": _number(_first_value(attrs, "gen_ai.usage.cache_read.input_tokens", "cache_read_tokens")),
                            "reasoning_tokens": _number(attrs.get("gen_ai.usage.reasoning_tokens")),
                            "total_tokens": _number(attrs.get("gen_ai.usage.total_tokens")),
                            "cost": _number(_first_value(attrs, "gen_ai.usage.cost", "cost_usd")),
                            "source": source_evidence,
                        },
                        "performance": {
                            "latency_ms": latency,
                            "time_to_first_token_ms": time_to_first_token,
                            "duration_ms": duration,
                            "tool_duration_ms": _number(attrs.get("llm.observatory.tool.duration_ms")),
                            "session_duration_ms": _number(attrs.get("llm.observatory.session.duration_ms")),
                            "agent_duration_ms": _number(attrs.get("llm.observatory.agent.duration_ms")),
                            "workflow_duration_ms": _number(attrs.get("llm.observatory.workflow.duration_ms")),
                            "wall_clock_ms": _number(attrs.get("llm.observatory.wall_clock_ms")),
                            "concurrency": _number(attrs.get("llm.observatory.concurrency")),
                            "parallel_utilization": _number(attrs.get("llm.observatory.parallel_utilization")),
                        },
                        "reliability": {
                            "status": "failed" if failed else "succeeded" if provider != "unknown" else "unknown",
                            "error_kind": error_kind,
                            "retry_count": retry_count,
                            "rate_limited": rate_limited,
                            "timeout": timeout,
                            "tool_failure": tool_failure,
                            "agent_failure": agent_failure,
                            "aborted": aborted,
                            "reassessment_count": reassessment_count,
                            "rework_count": rework_count,
                        },
                        "behavior": _behavior(identity),
                        "provenance": {
                            "fields": {
                                "llm.provider": "client",
                                "llm.model": "client",
                                "llm.model_variant": "client" if _model_variant(identity) is not None else "unknown",
                                "usage": source_evidence,
                                "performance.latency_ms": "client" if latency is not None else "unknown",
                                "performance.time_to_first_token_ms": "client" if time_to_first_token is not None else "unknown",
                                "performance.duration_ms": "client" if duration is not None else "unknown",
                                "reliability.error_kind": error_kind_source,
                                "reliability.retry_count": "client" if retry_signal is not None and retry_count is not None else "unknown",
                                "reliability.rate_limited": rate_limited_source,
                                "reliability.timeout": "client" if timeout is not None and error_signal is not None else "derived" if timeout is True else "unknown",
                                "reliability.tool_failure": "client" if tool_failure_signal is not None and tool_failure is not None else "unknown",
                                "reliability.agent_failure": "client" if identity.get("llm.observatory.agent.failure") is not None and agent_failure is not None else "unknown",
                                "reliability.aborted": "client" if aborted_signal is not None and aborted is not None else "unknown",
                                "reliability.reassessment_count": "client" if reassessment_count is not None else "unknown",
                                "reliability.rework_count": "client" if rework_count is not None else "unknown",
                                "performance.tool_duration_ms": "client" if attrs.get("llm.observatory.tool.duration_ms") is not None else "unknown",
                                "performance.session_duration_ms": "client" if attrs.get("llm.observatory.session.duration_ms") is not None else "unknown",
                                "performance.agent_duration_ms": "client" if attrs.get("llm.observatory.agent.duration_ms") is not None else "unknown",
                                "performance.workflow_duration_ms": "client" if attrs.get("llm.observatory.workflow.duration_ms") is not None else "unknown",
                                "performance.wall_clock_ms": "client" if attrs.get("llm.observatory.wall_clock_ms") is not None else "unknown",
                                "performance.concurrency": "client" if attrs.get("llm.observatory.concurrency") is not None else "unknown",
                                "performance.parallel_utilization": "client" if attrs.get("llm.observatory.parallel_utilization") is not None else "unknown",
                            },
                            "adapter": "otlp-json",
                            "semantic_conventions": str(resource_logs.get("schemaUrl") or "otel"),
                            "content_capture": "disabled",
                        },
                        "attributes": _safe_attributes(resource_attrs, attrs),
                        "extensions": _extensions(resource_attrs, attrs),
                    }
        if not saw_log:
            yield {}

    def _metric_records(self, payload: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        resource_metrics_list = payload.get("resourceMetrics")
        if not isinstance(resource_metrics_list, list):
            yield {}
            return
        validated_metrics = 0
        invalid_metrics: set[tuple[int, int, int]] = set()
        saw_metric = False
        for resource_index, resource_metrics in enumerate(resource_metrics_list):
            if not isinstance(resource_metrics, Mapping):
                continue
            scope_metrics_list = resource_metrics.get("scopeMetrics")
            if not isinstance(scope_metrics_list, list):
                continue
            for scope_index, scope_metrics in enumerate(scope_metrics_list):
                if not isinstance(scope_metrics, Mapping):
                    continue
                metrics = scope_metrics.get("metrics")
                if not isinstance(metrics, list):
                    continue
                for metric_index, metric in enumerate(metrics):
                    saw_metric = True
                    try:
                        if not isinstance(metric, Mapping):
                            raise ValueError("metric must be an object")
                        metric_type = next((key for key in ("gauge", "sum", "histogram", "exponentialHistogram") if isinstance(metric.get(key), Mapping)), None)
                        if metric_type is None:
                            raise ValueError("metric has no supported data type")
                        data_points = metric[metric_type].get("dataPoints")
                        if not isinstance(data_points, list) or not data_points:
                            raise ValueError("metric contains no data points")
                        for point in data_points:
                            if not isinstance(point, Mapping):
                                raise ValueError("metric data point must be an object")
                            _time(point.get("timeUnixNano"))
                            value = _first_value(point, "asDouble", "asInt", "value", "sum", "count")
                            if value is not None and _metric_number(value) is None:
                                raise ValueError("metric data point value is not numeric")
                    except ValueError:
                        invalid_metrics.add((resource_index, scope_index, metric_index))
                        continue
                    validated_metrics += 1
        emitted_invalid_structure = False
        for resource_index, resource_metrics in enumerate(resource_metrics_list):
            if not isinstance(resource_metrics, Mapping):
                emitted_invalid_structure = True
                yield {}
                continue
            resource_attrs = _resource_attributes(resource_metrics.get("resource", {}))
            scope_metrics_list = resource_metrics.get("scopeMetrics")
            if not isinstance(scope_metrics_list, list):
                emitted_invalid_structure = True
                yield {}
                continue
            for scope_index, scope_metrics in enumerate(scope_metrics_list):
                if not isinstance(scope_metrics, Mapping):
                    emitted_invalid_structure = True
                    yield {}
                    continue
                scope = scope_metrics.get("scope", {})
                if not isinstance(scope, Mapping):
                    emitted_invalid_structure = True
                    yield {}
                    continue
                source_name = _source_name(resource_attrs, scope)
                metrics = scope_metrics.get("metrics")
                if not isinstance(metrics, list):
                    emitted_invalid_structure = True
                    yield {}
                    continue
                for metric_index, metric in enumerate(metrics):
                    if (resource_index, scope_index, metric_index) in invalid_metrics:
                        yield {}
                        continue
                    if not isinstance(metric, Mapping):
                        continue
                    name = str(metric.get("name") or "unknown.metric")
                    metric_type = next((key for key in ("gauge", "sum", "histogram", "exponentialHistogram") if isinstance(metric.get(key), Mapping)), "unknown")
                    metric_body = metric.get(metric_type, {}) if metric_type != "unknown" else {}
                    data_points = metric_body.get("dataPoints") if isinstance(metric_body, Mapping) else []
                    if not isinstance(data_points, list):
                        data_points = []
                    point_summaries: list[dict[str, Any]] = []
                    point_attribute_values: list[dict[str, Any]] = []
                    for point in data_points[:64]:
                        if not isinstance(point, Mapping):
                            continue
                        value = _first_value(point, "asDouble", "asInt", "value", "sum", "count")
                        point_value: dict[str, Any] = {}
                        if value is not None:
                            normalized_value = _metric_number(value)
                            if normalized_value is None:
                                point_summaries = []
                                break
                            point_value["value"] = normalized_value
                        if point.get("timeUnixNano") is not None:
                            point_value["time_unix_nano"] = point.get("timeUnixNano")
                        if point.get("startTimeUnixNano") is not None:
                            point_value["start_time_unix_nano"] = point.get("startTimeUnixNano")
                        point_attributes = _attributes(point.get("attributes", []))
                        point_attribute_values.append(point_attributes)
                        safe_point_attributes = _safe_attributes({}, point_attributes)
                        if safe_point_attributes:
                            point_value["attributes"] = safe_point_attributes
                        unknown_point_attributes = _unknown_attributes({}, point_attributes)
                        if unknown_point_attributes:
                            point_value["unknown_attributes"] = unknown_point_attributes
                        if point_value:
                            point_summaries.append(point_value)
                    if not point_summaries or point_summaries[0].get("time_unix_nano") is None:
                        yield {}
                        continue
                    unit = metric.get("unit")
                    point_groups: dict[str, dict[str, Any]] = {}
                    for point_summary, point_attributes in zip(point_summaries, point_attribute_values):
                        point_identity, point_unknown, group_key, point_context_sha256 = _metric_point_group(point_attributes)
                        group = point_groups.setdefault(group_key, {
                            "point_summaries": [],
                            "point_identity": point_identity,
                            "point_unknown": point_unknown,
                            "point_context_sha256": point_context_sha256,
                        })
                        group["point_summaries"].append(point_summary)
                    for group in point_groups.values():
                        yield _metric_record(
                            resource_attrs=resource_attrs,
                            scope=scope,
                            source_name=source_name,
                            schema_url=resource_metrics.get("schemaUrl"),
                            name=name,
                            metric_type=metric_type,
                            unit=unit,
                            point_summaries=group["point_summaries"],
                            point_identity=group["point_identity"],
                            point_unknown=group["point_unknown"],
                            point_context_sha256=group["point_context_sha256"],
                        )
        if not saw_metric and not emitted_invalid_structure:
            yield {}
