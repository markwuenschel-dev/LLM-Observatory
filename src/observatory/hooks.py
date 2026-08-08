"""Metadata-only client hook normalization.

Client hook payloads are untrusted and provider-specific.  This module keeps
the boundary deliberately small: only bounded identity, lifecycle, usage, and
timing fields become a normalized event.  Prompt text, tool arguments, command
strings, paths, credentials, and the raw payload are never copied into the
event envelope.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
import re
from pathlib import Path
from typing import Any, Mapping

from .clients import CLIENT_SPECS, normalize_client_name
from .clock import utc_now
from .contracts import NormalizedEvent, stable_event_id
from .project import resolve_project


_MAX_LABEL_LENGTH = 160
_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:@/+-]+$")
_TOOL_EVENTS = frozenset({"pretooluse", "posttooluse", "tooluse", "tool_use", "tool"})
_LIFECYCLE_MARKERS = ("session", "notification", "interrupt", "compact", "stop", "start", "end")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sources(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    nested = []
    for key in ("data", "event", "params"):
        candidate = payload.get(key)
        if isinstance(candidate, Mapping):
            nested.append(candidate)
    return (payload, *nested)


def _first(payload: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for source in _sources(payload):
        for key in keys:
            if key in source and source[key] is not None:
                return source[key]
    return None


def _text(value: Any, *, max_length: int = _MAX_LABEL_LENGTH) -> str | None:
    if not isinstance(value, str):
        return None
    value = " ".join(value.split())
    if not value:
        return None
    return value[:max_length]


def _label(value: Any) -> str | None:
    value = _text(value)
    if value is None:
        return None
    # Labels are dimensions, not a second content channel.  Keep only the
    # compact identifier vocabulary used by client event names and tool names.
    return re.sub(r"[^A-Za-z0-9_.:@/+-]", "_", value)[:_MAX_LABEL_LENGTH]


def _identifier(value: Any) -> str | None:
    value = _text(value, max_length=256)
    if value is None or not _ID_PATTERN.fullmatch(value):
        return None
    return value


def _boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return value


def _number_from(payload: Mapping[str, Any], keys: tuple[str, ...]) -> int | float | None:
    value = _number(_first(payload, keys))
    if value is not None:
        return value
    for source in _sources(payload):
        usage = _mapping(source.get("usage"))
        value = _number(next((usage[key] for key in keys if key in usage), None))
        if value is not None:
            return value
    return None


def _timestamp(payload: Mapping[str, Any]) -> str:
    value = _text(_first(payload, ("observed_at", "timestamp", "time")), max_length=80)
    if value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
    return utc_now().isoformat().replace("+00:00", "Z")


def _event_type(event_name: str) -> str:
    normalized = event_name.casefold().replace("-", "").replace(" ", "")
    if normalized in _TOOL_EVENTS or "tool" in normalized:
        return "tool.operation"
    if any(marker in normalized for marker in _LIFECYCLE_MARKERS):
        return "agent.lifecycle"
    return "telemetry.log"


def _error_reported(payload: Mapping[str, Any]) -> bool:
    for key in ("error", "failed", "is_error", "isError", "tool_error", "toolError"):
        value = _first(payload, (key,))
        if value is not None and value is not False and value != "":
            return True
    return False


def build_hook_event(
    client: str,
    payload: Mapping[str, Any],
    *,
    project_path: str | Path,
) -> NormalizedEvent:
    """Convert one client hook payload into a privacy-safe event."""

    if not isinstance(payload, Mapping):
        raise ValueError("hook payload must be an object")
    normalized_client = normalize_client_name(client)
    spec = CLIENT_SPECS.get(normalized_client)
    provider = spec.provider if spec is not None else "unknown"
    client_name = spec.name if spec is not None else (_label(normalized_client) or "unknown")
    event_name = _label(_first(payload, ("hook_event_name", "hookEventName", "event_name", "event"))) or "unknown"
    session_id = _identifier(_first(payload, ("session_id", "sessionId", "session")))
    agent_id = _identifier(_first(payload, ("agent_id", "agentId")))
    subagent_id = _identifier(_first(payload, ("subagent_id", "subagentId")))
    model = _label(_first(payload, ("model", "model_name", "modelName"))) or "unknown"
    tool_name = _label(_first(payload, ("tool_name", "toolName", "tool")))
    notification_type = _label(_first(payload, ("notification_type", "notificationType")))
    auth_mode = _label(_first(payload, ("auth_mode", "authMode"))) or "unknown"
    route = _label(_first(payload, ("route", "gateway"))) or "unknown"
    error = _error_reported(payload)
    aborted = event_name.casefold() in {"interrupt", "aborted", "abort"}
    timeout = bool(_boolean(_first(payload, ("timeout", "timed_out", "timedOut"))))
    rate_limited = _boolean(_first(payload, ("rate_limited", "rateLimited")))
    reliability_status = "failed" if error else "aborted" if aborted else "unknown"

    hook_cwd = _text(_first(payload, ("cwd", "working_directory", "workingDirectory", "project_path")), max_length=1024)
    try:
        project = resolve_project(hook_cwd or project_path, git_timeout_seconds=0.25)
    except (OSError, RuntimeError, ValueError):
        # The hook must never make a client fail because Git or the working
        # directory is unavailable.  The privacy boundary still gets a valid
        # project identity fallback below.
        project = resolve_project(Path.cwd(), git_timeout_seconds=0.05)

    attributes: dict[str, Any] = {
        "hook_event": event_name,
        "hook_contract": "llm-observatory.hook/v1",
    }
    if notification_type:
        attributes["notification_type"] = notification_type
    if tool_name:
        attributes["tool_name"] = tool_name
    if error:
        attributes["error_reported"] = True

    value: dict[str, Any] = {
        "schema_version": "1.0",
        "event_type": _event_type(event_name),
        "observed_at": _timestamp(payload),
        "source": {"kind": "hook", "name": f"{client_name}-hook"},
        "project": project.__dict__,
        "execution": {
            "session_id": session_id,
            "agent_id": agent_id,
            "subagent_id": subagent_id,
        },
        "llm": {
            "provider": provider,
            "model": model,
            "client": client_name,
            "auth_mode": auth_mode,
            "route": route,
        },
        "usage": {
            "input_tokens": _number_from(payload, ("input_tokens", "inputTokens")),
            "output_tokens": _number_from(payload, ("output_tokens", "outputTokens")),
            "cached_tokens": _number_from(payload, ("cached_tokens", "cachedTokens")),
            "cache_creation_tokens": _number_from(payload, ("cache_creation_tokens", "cacheCreationTokens")),
            "cache_read_tokens": _number_from(payload, ("cache_read_tokens", "cacheReadTokens")),
            "reasoning_tokens": _number_from(payload, ("reasoning_tokens", "reasoningTokens")),
            "total_tokens": _number_from(payload, ("total_tokens", "totalTokens")),
            "cost": _number_from(payload, ("cost", "cost_usd", "costUsd")),
            "source": "client-hook",
        },
        "performance": {
            "latency_ms": _number_from(payload, ("latency_ms", "latencyMs")),
            "duration_ms": _number_from(payload, ("duration_ms", "durationMs")),
            "tool_duration_ms": _number_from(payload, ("tool_duration_ms", "toolDurationMs")),
        },
        "reliability": {
            "status": reliability_status,
            "error_kind": "client-hook-reported-error" if error else None,
            "rate_limited": rate_limited,
            "timeout": timeout,
            "tool_failure": error if tool_name else None,
            "aborted": aborted,
        },
        "behavior": {
            "tool_call_count": 1 if tool_name or _event_type(event_name) == "tool.operation" else None,
            "tool_names": [tool_name] if tool_name else [],
        },
        "outcome": {
            "kind": "client-hook",
            "status": event_name,
            "evidence_source": "client-hook",
        },
        "provenance": {
            "fields": {
                "llm.provider": "client-catalog",
                "execution.session_id": "client-hook" if session_id else "absent",
                "usage": "client-hook",
                "performance": "client-hook",
            },
            "adapter": "client-hook",
            "semantic_conventions": "llm-observatory.hook/v1",
            "content_capture": "disabled",
        },
        "attributes": attributes,
    }
    event_id = _identifier(_first(payload, ("event_id", "eventId", "id")))
    value["event_id"] = event_id or stable_event_id(value)
    return NormalizedEvent.from_mapping(value)
