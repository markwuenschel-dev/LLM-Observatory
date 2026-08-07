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
        auth_mode: str = "api",
        observed_at: datetime | str | None = None,
        latency_ms: int | float | None = None,
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
        self.auth_mode = _text(auth_mode, "api")
        self.observed_at = _timestamp(observed_at)
        self.latency_ms = latency_ms
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

    def _usage(self) -> tuple[dict[str, Any], str]:
        usage = _mapping(self.response.get("usage") or self.response.get("usageMetadata"))
        if self.provider == "google":
            return (
                {
                    "input_tokens": _first_number(usage.get("promptTokenCount"), usage.get("prompt_token_count")),
                    "output_tokens": _first_number(usage.get("candidatesTokenCount"), usage.get("candidates_token_count")),
                    "cached_tokens": _first_number(usage.get("cachedContentTokenCount"), usage.get("cached_content_token_count")),
                    "cache_read_tokens": _first_number(usage.get("cachedContentTokenCount"), usage.get("cached_content_token_count")),
                    "reasoning_tokens": _first_number(usage.get("thoughtsTokenCount"), usage.get("thoughts_token_count")),
                    "total_tokens": _first_number(usage.get("totalTokenCount"), usage.get("total_token_count")),
                    "cost": _first_number(usage.get("cost"), self.response.get("cost")),
                },
                "provider",
            )
        if self.provider == "anthropic":
            return (
                {
                    "input_tokens": _first_number(usage.get("input_tokens")),
                    "output_tokens": _first_number(usage.get("output_tokens")),
                    "cached_tokens": _first_number(usage.get("cache_read_input_tokens"), usage.get("cache_creation_input_tokens")),
                    "cache_creation_tokens": _first_number(usage.get("cache_creation_input_tokens")),
                    "cache_read_tokens": _first_number(usage.get("cache_read_input_tokens")),
                    "total_tokens": _first_number(usage.get("total_tokens")),
                    "cost": _first_number(usage.get("cost"), self.response.get("cost")),
                },
                "provider",
            )
        return (
            {
                "input_tokens": _first_number(usage.get("input_tokens"), usage.get("prompt_tokens")),
                "output_tokens": _first_number(usage.get("output_tokens"), usage.get("completion_tokens")),
                "cached_tokens": _first_number(usage.get("cached_tokens"), usage.get("prompt_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("prompt_tokens_details"), Mapping) else None),
                "cache_read_tokens": _first_number(usage.get("cached_tokens"), usage.get("prompt_tokens_details", {}).get("cached_tokens") if isinstance(usage.get("prompt_tokens_details"), Mapping) else None),
                "reasoning_tokens": _first_number(usage.get("reasoning_tokens"), usage.get("completion_tokens_details", {}).get("reasoning_tokens") if isinstance(usage.get("completion_tokens_details"), Mapping) else None),
                "total_tokens": _first_number(usage.get("total_tokens")),
                "cost": _first_number(usage.get("cost"), self.response.get("cost")),
            },
            "gateway" if self.route == "openrouter" or self.provider == "openrouter" else "provider",
        )

    def iter_events(self) -> Iterator[Mapping[str, Any]]:
        usage, usage_source = self._usage()
        error = _mapping(self.response.get("error"))
        response_status = self.response.get("status")
        status = "failed" if error or str(response_status).casefold() in {"error", "failed"} else "succeeded"
        model = self.response.get("model") or self.response.get("modelVersion") or self.response.get("model_version") or "unknown"
        response_id = self.response.get("id") or self.response.get("responseId") or self.response.get("response_id")
        execution = {
            "session_id": _optional_text(self.response.get("session_id") or self.response.get("sessionId")),
            "trace_id": _optional_text(self.response.get("trace_id") or self.response.get("traceId")),
            "task_id": _optional_text(self.response.get("task_id") or self.response.get("taskId")),
            "task_class": _optional_text(self.response.get("task_class") or self.response.get("taskClass")),
        }
        stable_input = {"provider": self.provider, "client": self.client, "route": self.route, "response": self.response}
        event_id = str(response_id) if isinstance(response_id, str) and response_id.strip() else stable_event_id(stable_input)
        attributes: dict[str, Any] = {}
        for key in ("finish_reason", "finish_reasons", "stop_reason", "status_code", "request_id"):
            if key in self.response:
                attributes[f"provider.{key}"] = self.response[key]
        if error:
            attributes["error.type"] = error.get("type") or "provider_error"
            attributes["error.message"] = error.get("message") or "provider request failed"
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
                "client": self.client,
                "auth_mode": self.auth_mode,
                "route": self.route,
                "reasoning_effort": _optional_text(self.response.get("reasoning_effort") or self.response.get("reasoningEffort")),
            },
            "usage": {**usage, "source": usage_source},
            "performance": {"latency_ms": self.latency_ms, "duration_ms": self.latency_ms},
            "reliability": {"status": status, "error_kind": _text(error.get("type"), "provider_error") if error else None},
            "provenance": {
                "fields": {"llm.model": "provider", "usage": usage_source},
                "adapter": self.name,
                "semantic_conventions": "provider.response/v1",
                "content_capture": "disabled",
            },
            "attributes": attributes,
        }
