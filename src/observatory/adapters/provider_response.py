"""Caller-owned response adapters for direct APIs and API gateways.

These adapters observe a response that the application already received.  They
do not create a client, send a request, install a hook, or change a provider
endpoint.  That boundary is what keeps direct API and OpenRouter coverage
out-of-band from inference.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Iterator, Mapping

from .base import CapabilityRecord
from ..contracts import ProjectIdentity, stable_event_id
from ..privacy import PrivacyPolicy, redact_mapping
from ..project import resolve_project


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _optional_text(value: Any) -> str | None:
    return None if value is None else _text(value, "")


def _number(value: Any) -> int | float | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and (not isinstance(value, float) or math.isfinite(value)) else None


def _first_number(*values: Any) -> int | float | None:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None


def _first_count(*values: Any) -> int | float | None:
    for value in values:
        if isinstance(value, (list, tuple)):
            return len(value)
        number = _number(value)
        if number is not None and number >= 0:
            return number
    return None


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None


def _canonical_error_kind(value: Any) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.casefold().replace("-", "_").replace(" ", "_")
    if normalized in {"rate_limit", "rate_limited", "too_many_requests", "resource_exhausted", "429"}:
        return "rate_limited"
    if normalized in {"timeout", "timed_out", "deadline_exceeded", "408", "504"}:
        return "timeout"
    return text


def _status_code(*values: Any) -> int | None:
    for value in values:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, float) and math.isfinite(value) and value.is_integer():
            return int(value)
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
    return None


def _tool_call_summaries(response: Mapping[str, Any]) -> list[dict[str, str]]:
    """Keep tool identity while excluding arguments and results."""

    candidates: list[Any] = []
    for key in ("tool_calls", "content", "output"):
        value = response.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    for candidate in response.get("candidates", []) if isinstance(response.get("candidates"), list) else []:
        if isinstance(candidate, Mapping):
            content = candidate.get("content")
            if isinstance(content, Mapping) and isinstance(content.get("parts"), list):
                candidates.extend(content["parts"])

    summaries: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in candidates:
        if not isinstance(item, Mapping):
            continue
        function = _mapping(item.get("function"))
        function_call = _mapping(item.get("functionCall") or item.get("function_call"))
        tool_use = str(item.get("type") or "").casefold()
        name = function.get("name") or function_call.get("name") or item.get("name")
        call_id = item.get("id") or item.get("tool_call_id") or item.get("toolUseId") or item.get("call_id")
        if name is None and tool_use not in {"tool_use", "tool_result", "function_call", "function_call_output", "functionresponse", "function_response"}:
            continue
        kind = tool_use or "tool_call"
        summary = {
            "id": _text(call_id, "unknown"),
            "name": _text(name, "unknown"),
            "type": kind,
        }
        marker = (summary["id"], summary["name"], summary["type"])
        if marker not in seen:
            summaries.append(summary)
            seen.add(marker)
        if len(summaries) >= 64:
            break
    return summaries


def _bounded_provider_metadata(response: Mapping[str, Any], *, route: str) -> dict[str, Any]:
    """Preserve gateway routing facts without retaining a raw provider body."""

    metadata: dict[str, Any] = {}
    for output_key, input_keys in {
        "gateway.target_provider": ("target_provider", "targetProvider", "served_provider", "servedProvider", "provider_name", "providerName"),
        "gateway.served_model": ("served_model", "servedModel", "upstream_model", "upstreamModel"),
    }.items():
        value = next((response.get(key) for key in input_keys if response.get(key) not in (None, "")), None)
        if value is not None:
            metadata[output_key] = _text(value)
    if route == "openrouter" or metadata:
        requested_model = response.get("requested_model") or response.get("requestedModel") or response.get("model")
        if requested_model is not None:
            metadata["gateway.requested_model"] = _text(requested_model)
    attempts = response.get("provider_attempts") or response.get("providerAttempts")
    if isinstance(attempts, list):
        safe_attempts: list[dict[str, str]] = []
        for attempt in attempts[:32]:
            if not isinstance(attempt, Mapping):
                continue
            safe_attempts.append({
                "provider": _text(attempt.get("provider") or attempt.get("provider_name") or attempt.get("providerName")),
                "model": _text(attempt.get("model") or attempt.get("model_name") or attempt.get("modelName")),
                "status": _text(attempt.get("status") or attempt.get("status_code") or attempt.get("statusCode")),
            })
        if safe_attempts:
            metadata["gateway.provider_attempts"] = safe_attempts
    return metadata


_PROVIDER_RESPONSE_CORE_KEYS = frozenset({
    "id", "responseid", "response_id", "model", "modelversion", "model_version", "model_family", "modelfamily",
    "model_variant", "modelvariant", "variant", "usage", "usagemetadata", "cost", "status", "status_code",
    "error", "error_kind", "errorkind", "rate_limited", "ratelimited", "timeout", "retry_count", "retrycount",
    "time_to_first_token_ms", "timetofirsttokenms", "ttft_ms", "ttftms", "session_id", "sessionid", "workflow_id",
    "workflowid", "agent_id", "agentid", "subagent_id", "subagentid", "parent_agent_id", "parentagentid",
    "parent_agent", "role", "agent_role", "agentrole", "skill", "skill_name", "skillname", "lane", "trace_id",
    "traceid", "task_id", "taskid", "task_class", "taskclass", "tool_calls", "content", "output", "candidates",
    "provider_attempts", "providerattempts", "target_provider", "targetprovider", "served_provider", "servedprovider",
    "provider_name", "providername", "served_model", "servedmodel", "upstream_model", "upstreammodel",
    "requested_model", "requestedmodel", "files_inspected_count", "filesinspectedcount", "files_inspected",
    "files_changed_count", "fileschangedcount", "files_changed", "commands_executed_count", "commandsexecutedcount",
    "commands_executed", "tests_invoked_count", "testsinvokedcount", "tests_invoked", "agent_failure", "agentfailure",
    "reassessment_count", "reassessmentcount", "reassessments", "rework_count", "reworkcount", "rework", "rework_loops",
    "finish_reason", "finish_reasons", "stop_reason", "request_id", "requestid", "auth_mode", "authmode",
    "reasoning_effort", "reasoningeffort", "route",
})


def _bounded_provider_extensions(response: Mapping[str, Any]) -> dict[str, Any]:
    """Retain bounded future response dimensions after metadata redaction."""

    unknown: dict[str, Any] = {}
    for key, value in response.items():
        if not isinstance(key, str) or key.casefold().replace("-", "_") in _PROVIDER_RESPONSE_CORE_KEYS:
            continue
        if len(unknown) >= 64:
            break
        unknown[key] = value
    if not unknown:
        return {}
    return redact_mapping(unknown, PrivacyPolicy())


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value.strip():
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ProviderResponseAdapter:
    """Normalize one already-completed provider/gateway response."""

    def __init__(
        self,
        provider: str,
        client: str,
        response: Mapping[str, Any],
        *,
        route: str = "direct",
        auth_mode: str = "unknown",
        usage_source: str | None = None,
        observed_at: datetime | str | None = None,
        latency_ms: int | float | None = None,
        duration_ms: int | float | None = None,
        project: ProjectIdentity | None = None,
        project_path: str | Path | None = None,
    ) -> None:
        if not isinstance(response, Mapping):
            raise ValueError("provider response must be an object")
        if project is not None and project_path is not None:
            raise ValueError("pass project or project_path, not both")
        self.provider = _text(provider)
        self.client = _text(client)
        self.response = response
        self.route = _text(route, "direct")
        self.auth_mode = _text(auth_mode, "unknown")
        self.usage_source = _text(usage_source, "unknown") if usage_source is not None else "unknown"
        self.observed_at = _timestamp(observed_at)
        self.latency_ms = latency_ms
        self.duration_ms = duration_ms
        self.project = resolve_project(project_path) if project_path is not None else project or ProjectIdentity()
        self.name = f"response:{self.client}"

    def capabilities(self) -> CapabilityRecord:
        return CapabilityRecord(
            provider=self.provider,
            client=self.client,
            confidence="SUPPORTED_NOT_LOCALLY_VERIFIED",
            capabilities={
                "response_envelope": "SUPPORTED",
                "authoritative_usage": "SUPPORTED_IF_REPORTED",
                "model_identity": "SUPPORTED_IF_REPORTED",
                "tool_calls": "SUPPORTED_IF_REPORTED",
                "session_identity": "CALLER_SUPPLIED",
                "native_otel": "CALLER_OWNED",
                "inference_proxy": "MUST_NOT_BE_USED",
            },
            auth_modes=(self.auth_mode,),
            evidence=("caller-owned mapping adapter; it never sends inference requests",),
            last_verified=datetime.now(timezone.utc).date().isoformat(),
        )

    def _usage(self) -> tuple[dict[str, Any], str, str]:
        usage = _mapping(self.response.get("usage") or self.response.get("usageMetadata"))
        if self.provider == "google":
            input_tokens = _first_number(usage.get("promptTokenCount"), usage.get("prompt_token_count"))
            output_tokens = _first_number(usage.get("candidatesTokenCount"), usage.get("candidates_token_count"))
            reported_total = _first_number(usage.get("totalTokenCount"), usage.get("total_token_count"))
            total_tokens = reported_total if reported_total is not None else input_tokens + output_tokens if input_tokens is not None and output_tokens is not None else None
            usage_values = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cached_tokens": _first_number(usage.get("cached_tokens"), usage.get("cachedTokens")),
                    "cache_read_tokens": _first_number(usage.get("cachedContentTokenCount"), usage.get("cached_content_token_count")),
                    "reasoning_tokens": _first_number(usage.get("thoughtsTokenCount"), usage.get("thoughts_token_count")),
                    "total_tokens": total_tokens,
                    "cost": _first_number(usage.get("cost"), self.response.get("cost")),
                }
            return (
                usage_values,
                self.usage_source if any(value is not None for value in usage_values.values()) else "unknown",
                self.usage_source if reported_total is not None else "derived" if total_tokens is not None else "unknown",
            )
        if self.provider == "anthropic":
            input_tokens = _first_number(usage.get("input_tokens"))
            output_tokens = _first_number(usage.get("output_tokens"))
            reported_total = _first_number(usage.get("total_tokens"))
            total_tokens = reported_total if reported_total is not None else input_tokens + output_tokens if input_tokens is not None and output_tokens is not None else None
            usage_values = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cached_tokens": _first_number(usage.get("cached_tokens")),
                    "cache_creation_tokens": _first_number(usage.get("cache_creation_input_tokens")),
                    "cache_read_tokens": _first_number(usage.get("cache_read_input_tokens")),
                    "total_tokens": total_tokens,
                    "cost": _first_number(usage.get("cost"), self.response.get("cost")),
                }
            return (
                usage_values,
                self.usage_source if any(value is not None for value in usage_values.values()) else "unknown",
                self.usage_source if reported_total is not None else "derived" if total_tokens is not None else "unknown",
            )
        input_tokens = _first_number(usage.get("input_tokens"), usage.get("prompt_tokens"))
        output_tokens = _first_number(usage.get("output_tokens"), usage.get("completion_tokens"))
        reported_total = _first_number(usage.get("total_tokens"))
        total_tokens = reported_total if reported_total is not None else input_tokens + output_tokens if input_tokens is not None and output_tokens is not None else None
        usage_values = {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cached_tokens": _first_number(
                    usage.get("cached_tokens"),
                ),
                "cache_read_tokens": _first_number(
                    usage.get("cache_read_tokens"),
                    usage.get("cache_read_input_tokens"),
                    usage.get("prompt_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("prompt_tokens_details"), Mapping) else None,
                    usage.get("input_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("input_tokens_details"), Mapping) else None,
                ),
                "reasoning_tokens": _first_number(
                    usage.get("reasoning_tokens"),
                    usage.get("completion_tokens_details", {}).get("reasoning_tokens") if isinstance(usage.get("completion_tokens_details"), Mapping) else None,
                    usage.get("output_tokens_details", {}).get("reasoning_tokens") if isinstance(usage.get("output_tokens_details"), Mapping) else None,
                ),
                "total_tokens": total_tokens,
                "cost": _first_number(usage.get("cost"), self.response.get("cost")),
            }
        return (
            usage_values,
            self.usage_source if any(value is not None for value in usage_values.values()) else "unknown",
            self.usage_source if reported_total is not None else "derived" if total_tokens is not None else "unknown",
        )

    def iter_events(self) -> Iterator[Mapping[str, Any]]:
        usage, usage_source, total_tokens_source = self._usage()
        raw_error = self.response.get("error")
        error = _mapping(raw_error)
        if raw_error is not None and not error:
            error = {"type": "provider_error", "message": _text(raw_error, "provider request failed")}
        response_status = self.response.get("status")
        status_code = _status_code(
            self.response.get("status_code"),
            self.response.get("statusCode"),
            self.response.get("http_status"),
            self.response.get("httpStatus"),
            response_status,
        )
        if status_code is None:
            status_code = _status_code(
                error.get("status_code"),
                error.get("statusCode"),
                error.get("http_status"),
                error.get("httpStatus"),
                error.get("code"),
            )
        status_text = str(response_status).casefold() if response_status is not None else ""
        explicit_rate_limited = _first_bool(self.response.get("rate_limited"), self.response.get("rateLimited"))
        rate_limited = explicit_rate_limited
        explicit_timeout = _first_bool(
            self.response.get("timeout"),
            self.response.get("timed_out"),
            self.response.get("timedOut"),
        )
        timeout = explicit_timeout
        retry_count = _first_number(
            self.response.get("retry_count"),
            self.response.get("retryCount"),
            self.response.get("retries"),
        )
        tool_failure = _first_bool(self.response.get("tool_failure"), self.response.get("toolFailure"))
        agent_failure = _first_bool(
            self.response.get("agent_failure"),
            self.response.get("agentFailure"),
            self.response.get("agent_failed"),
        )
        aborted = _first_bool(self.response.get("aborted"), self.response.get("cancelled"), self.response.get("canceled"))
        reassessment_count = _first_count(
            self.response.get("reassessment_count"),
            self.response.get("reassessmentCount"),
            self.response.get("reassessments"),
        )
        rework_count = _first_count(
            self.response.get("rework_count"),
            self.response.get("reworkCount"),
            self.response.get("rework_loops"),
            self.response.get("rework"),
        )
        rate_limited_source = "reported" if explicit_rate_limited is not None else "unknown"
        timeout_source = "reported" if explicit_timeout is not None else "unknown"
        retry_count_source = "reported" if retry_count is not None else "unknown"
        agent_failure_source = "reported" if agent_failure is not None else "unknown"
        reassessment_source = "reported" if reassessment_count is not None else "unknown"
        rework_source = "reported" if rework_count is not None else "unknown"
        if status_code == 429 and rate_limited is None:
            rate_limited = True
            rate_limited_source = "derived"
        if status_code in {408, 504} and timeout is None:
            timeout = True
            timeout_source = "derived"
        failed = bool(error) or status_text in {"error", "failed", "failure", "timeout", "timed_out", "rate_limited"} or (status_code is not None and status_code >= 400) or agent_failure is True or tool_failure is True or aborted is True
        status = "failed" if failed else "succeeded"
        model = self.response.get("model") or self.response.get("modelVersion") or self.response.get("model_version") or "unknown"
        model_variant = _optional_text(
            self.response.get("model_variant")
            or self.response.get("modelVariant")
            or self.response.get("variant")
            or self.response.get("modelVersion")
            or self.response.get("model_version")
        )
        response_id = self.response.get("id") or self.response.get("responseId") or self.response.get("response_id")
        execution = {
            "parent_event_id": _optional_text(self.response.get("parent_event_id") or self.response.get("parentEventId")),
            "session_id": _optional_text(self.response.get("session_id") or self.response.get("sessionId")),
            "workflow_id": _optional_text(self.response.get("workflow_id") or self.response.get("workflowId")),
            "agent_id": _optional_text(self.response.get("agent_id") or self.response.get("agentId")),
            "subagent_id": _optional_text(self.response.get("subagent_id") or self.response.get("subagentId")),
            "parent_agent_id": _optional_text(
                self.response.get("parent_agent_id")
                or self.response.get("parentAgentId")
                or self.response.get("parent_agent")
            ),
            "role": _optional_text(self.response.get("role") or self.response.get("agent_role") or self.response.get("agentRole")),
            "skill": _optional_text(self.response.get("skill") or self.response.get("skill_name") or self.response.get("skillName")),
            "lane": _optional_text(self.response.get("lane")),
            "trace_id": _optional_text(self.response.get("trace_id") or self.response.get("traceId")),
            "task_id": _optional_text(self.response.get("task_id") or self.response.get("taskId")),
            "task_class": _optional_text(self.response.get("task_class") or self.response.get("taskClass")),
        }
        attributes: dict[str, Any] = _bounded_provider_metadata(self.response, route=self.route)
        provider_extensions = _bounded_provider_extensions(self.response)
        if response_id is not None:
            attributes["response_id"] = response_id
        tool_calls = _tool_call_summaries(self.response)
        behavior: dict[str, Any] = {}
        if tool_calls:
            attributes["tool_calls"] = tool_calls
            attributes["tool_call_count"] = len(tool_calls)
            behavior["tool_call_count"] = len(tool_calls)
            behavior["tool_names"] = [item["name"] for item in tool_calls]
        for output_key, input_keys in {
            "files_inspected_count": ("files_inspected_count", "filesInspectedCount", "files_inspected"),
            "files_changed_count": ("files_changed_count", "filesChangedCount", "files_changed"),
            "commands_executed_count": ("commands_executed_count", "commandsExecutedCount", "commands_executed"),
            "tests_invoked_count": ("tests_invoked_count", "testsInvokedCount", "tests_invoked"),
        }.items():
            candidate = next((self.response.get(key) for key in input_keys if self.response.get(key) is not None), None)
            if isinstance(candidate, (list, tuple)):
                candidate = len(candidate)
            number = _number(candidate)
            if number is not None and number >= 0:
                behavior[output_key] = number
        for key in ("finish_reason", "finish_reasons", "stop_reason", "status_code", "request_id"):
            if key in self.response:
                attributes[f"provider.{key}"] = self.response[key]
        if error:
            attributes["error.type"] = error.get("type") or "provider_error"
            attributes["error.message"] = "provider request failed"
        if status_code is not None:
            attributes["provider.status_code"] = status_code
        error_kind = _canonical_error_kind(
            error.get("type")
            or error.get("code")
            or self.response.get("error_kind")
            or self.response.get("errorKind")
        )
        error_kind_source = "reported" if error_kind is not None else "unknown"
        error_kind_text = error_kind.casefold() if error_kind is not None else ""
        if error_kind is not None and error_kind.casefold() in {"timeout", "timed_out", "deadline_exceeded"} and timeout is None:
            timeout = True
            timeout_source = "derived"
        if error_kind is not None and error_kind.casefold() in {"rate_limit", "rate_limited", "too_many_requests"} and rate_limited is None:
            rate_limited = True
            rate_limited_source = "derived"
        if rate_limited and (error_kind is None or error_kind_text in {"429", "too_many_requests", "rate_limit"}):
            error_kind = "rate_limited"
            error_kind_source = "derived"
        elif timeout and (error_kind is None or error_kind_text in {"408", "504", "timed_out", "deadline_exceeded"}):
            error_kind = "timeout"
            error_kind_source = "derived"
        elif error_kind is None and failed:
            error_kind = "provider_error"
            error_kind_source = "derived"
        stable_input = {
            "event_type": "model.operation",
            "observed_at": self.observed_at,
            "source": {"kind": "adapter", "name": self.name},
            "project": self.project.__dict__,
            "execution": execution,
            "llm": {
                "provider": self.provider,
                "client": self.client,
                "route": self.route,
                "model": str(model),
                "model_variant": model_variant,
            },
            "reliability": {
                "status": status,
                "status_code": status_code,
                "error_kind": error_kind,
                "agent_failure": agent_failure,
                "reassessment_count": reassessment_count,
                "rework_count": rework_count,
            },
            "behavior": behavior,
            "attributes": {
                "request_id": self.response.get("request_id") or self.response.get("requestId"),
                "response_id": response_id,
            },
        }
        if isinstance(response_id, str) and response_id.strip():
            event_id = stable_event_id({
                "event_type": "model.operation",
                "source": {"kind": "adapter", "name": self.name},
                "project": self.project.__dict__,
                "llm": {"provider": self.provider, "client": self.client, "route": self.route},
                "attributes": {"response_id": response_id.strip()},
            })
        else:
            event_id = stable_event_id(stable_input)
        time_to_first_token_ms = _first_number(
            self.response.get("time_to_first_token_ms"),
            self.response.get("timeToFirstTokenMs"),
            self.response.get("ttft_ms"),
            self.response.get("ttftMs"),
        )
        time_to_first_token_source = "reported" if time_to_first_token_ms is not None else "unknown"
        yield {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": "model.operation",
            "observed_at": self.observed_at,
            "source": {"kind": "adapter", "name": self.name},
            "project": self.project.__dict__,
            "execution": execution,
            "llm": {
                "provider": self.provider,
                "model": str(model),
                "model_family": _optional_text(self.response.get("model_family") or self.response.get("modelFamily")),
                "model_variant": model_variant,
                "client": self.client,
                "auth_mode": self.auth_mode,
                "route": self.route,
                "reasoning_effort": _optional_text(self.response.get("reasoning_effort") or self.response.get("reasoningEffort")),
            },
            "usage": {**usage, "source": usage_source},
            "performance": {
                "latency_ms": self.latency_ms,
                "time_to_first_token_ms": time_to_first_token_ms,
                "duration_ms": self.duration_ms,
            },
            "reliability": {
                "status": status,
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
            "behavior": behavior,
            "provenance": {
                "fields": {
                    "llm.model": "provider",
                    "llm.model_variant": "provider" if model_variant is not None else "unknown",
                    "usage": usage_source,
                    "usage.total_tokens": total_tokens_source,
                    "performance.latency_ms": "observed" if self.latency_ms is not None else "unknown",
                    "performance.time_to_first_token_ms": time_to_first_token_source,
                    "performance.duration_ms": "observed" if self.duration_ms is not None else "unknown",
                    "reliability.error_kind": error_kind_source,
                    "reliability.retry_count": retry_count_source,
                    "reliability.rate_limited": rate_limited_source,
                    "reliability.timeout": timeout_source,
                    "reliability.agent_failure": agent_failure_source,
                    "reliability.reassessment_count": reassessment_source,
                    "reliability.rework_count": rework_source,
                    "behavior": "derived" if behavior else "unknown",
                },
                "adapter": self.name,
                "semantic_conventions": "provider.response/v1",
                "content_capture": "disabled",
            },
            "attributes": attributes,
            "extensions": {"provider": provider_extensions} if provider_extensions else {},
        }
