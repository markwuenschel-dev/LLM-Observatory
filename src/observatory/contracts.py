"""Versioned, provider-neutral normalized telemetry contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any, Mapping

from .clock import utc_now


class ContractError(ValueError):
    """Raised when an event cannot safely enter the normalized model."""


def _as_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ContractError(f"{field_name} must be an object")
    return value


def _as_text(value: Any, field_name: str, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ContractError(f"{field_name} is required")
        return None
    if not isinstance(value, str):
        raise ContractError(f"{field_name} must be a string")
    value = value.strip()
    if required and not value:
        raise ContractError(f"{field_name} must not be empty")
    return value or None


def _as_number(value: Any, field_name: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"{field_name} must be numeric or null")
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError(f"{field_name} must be finite")
    return value


def _as_nonnegative_number(value: Any, field_name: str) -> int | float | None:
    number = _as_number(value, field_name)
    if number is not None and number < 0:
        raise ContractError(f"{field_name} must be non-negative")
    return number


def _as_optional_bool(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ContractError(f"{field_name} must be boolean or null")
    return value


def _behavior_count(value: Mapping[str, Any], count_key: str, list_key: str) -> int | float | None:
    candidate = value.get(count_key)
    if candidate is None:
        candidate = value.get(list_key)
        if isinstance(candidate, (list, tuple)):
            return len(candidate)
    return _as_nonnegative_number(candidate, f"behavior.{count_key}")


def _reliability_count(value: Mapping[str, Any], count_key: str, *list_keys: str) -> int | float | None:
    candidate = value.get(count_key)
    if candidate is None:
        for list_key in list_keys:
            candidate = value.get(list_key)
            if isinstance(candidate, (list, tuple)):
                candidate = len(candidate)
                break
            if candidate is not None:
                break
    return _as_nonnegative_number(candidate, f"reliability.{count_key}")


def _behavior_tool_names(value: Mapping[str, Any]) -> tuple[str, ...]:
    raw = value.get("tool_names")
    if raw is None:
        raw = value.get("tool_calls")
    if raw is None:
        return ()
    # ``tool_calls`` is accepted as either a bounded list of call metadata or
    # a numeric count.  A scalar count has no tool names to normalize.
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return ()
    if not isinstance(raw, (list, tuple)):
        raise ContractError("behavior.tool_names must be an array or null")
    names: list[str] = []
    seen: set[str] = set()
    for item in raw[:64]:
        candidate = item.get("name") if isinstance(item, Mapping) else item
        text = _as_text(candidate, "behavior.tool_names[]", required=True)
        if text is not None and text not in seen:
            names.append(text)
            seen.add(text)
    return tuple(names)


def ensure_utc(value: datetime | str, field_name: str) -> datetime:
    """Parse an aware ISO timestamp and normalize it to UTC."""

    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ContractError(f"{field_name} must be ISO-8601") from exc
    if not isinstance(value, datetime):
        raise ContractError(f"{field_name} must be a datetime or ISO-8601 string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible data deterministically for IDs and storage."""

    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"value is not canonical JSON: {exc}") from exc


_ID_SECTION_KEYS = frozenset({
    "source", "project", "execution", "llm", "usage", "performance", "reliability", "outcome",
    "behavior", "provenance", "attributes", "metric", "span", "log",
})
_ID_SAFE_KEYS = frozenset({
    "schema_version", "event_type", "observed_at", "source", "kind", "name", "version",
    "project", "project_id", "repository", "branch", "commit", "worktree", "execution",
    "trace_id", "span_id", "parent_event_id", "session_id", "workflow_id", "agent_id",
    "subagent_id", "parent_agent_id", "role", "skill", "lane", "task_id", "task_class", "llm", "provider",
    "model", "model_family", "model_variant", "client", "auth_mode", "route", "reasoning_effort",
    "usage", "source", "performance", "latency_ms", "duration_ms", "status", "status_code",
    "error_kind", "outcome", "correlation_id", "correlation_basis", "evidence_source", "provenance",
    "behavior", "tool_call_count", "tool_names", "files_inspected_count", "files_changed_count",
    "commands_executed_count", "tests_invoked_count",
    "agent_failure", "reassessment_count", "rework_count",
    "adapter", "semantic_conventions", "metric", "metric_name", "metric_type", "span_name", "severity",
    "request_id", "response_id", "id", "code", "finish_reason", "finish_reasons", "tool_name", "metric_context_sha256",
})
_ID_UNSAFE_KEY_PARTS = frozenset({
    "prompt", "completion", "content", "message", "body", "argument", "result", "secret", "password",
    "credential", "authorization", "cookie", "token", "path", "root", "command", "environment",
    "request", "response", "raw", "input", "output", "file", "email", "user", "private",
})
_ID_OMIT = object()


def _identity_key(value: Any) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(value)).casefold().replace("-", "_").split(".")[-1]


def _safe_identity(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
    """Project fallback-ID inputs onto bounded, non-content identity metadata."""

    if depth > 8:
        return _ID_OMIT
    normalized = _identity_key(key) if key is not None else ""
    if key is not None and (
        normalized not in _ID_SAFE_KEYS
        or normalized in _ID_UNSAFE_KEY_PARTS
        or any(part in normalized.split("_") for part in _ID_UNSAFE_KEY_PARTS)
    ) and normalized not in _ID_SECTION_KEYS:
        return _ID_OMIT
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for child_key, child_value in value.items():
            if not isinstance(child_key, str):
                continue
            safe = _safe_identity(child_value, key=child_key, depth=depth + 1)
            if safe is not _ID_OMIT:
                output[child_key] = safe
        return output
    if isinstance(value, (list, tuple)):
        values = []
        for item in value[:64]:
            safe = _safe_identity(item, key=key, depth=depth + 1)
            if safe is not _ID_OMIT:
                values.append(safe)
        return values
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return value if not isinstance(value, float) or math.isfinite(value) else _ID_OMIT
    if isinstance(value, str):
        text = value.strip()
        return text[:256] if text else _ID_OMIT
    return _ID_OMIT


def stable_event_id(value: Mapping[str, Any]) -> str:
    """Return an existing event ID or a deterministic content-derived ID."""

    existing = value.get("event_id")
    if isinstance(existing, str) and existing.strip():
        return existing.strip()
    # Receipt time is transport metadata, not event identity.  Excluding it
    # keeps a replay with a newly assigned receive timestamp idempotent.
    identity = _safe_identity(value)
    if not isinstance(identity, Mapping):
        identity = {}
    identity.pop("received_at", None)
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return f"evt_sha256_{digest}"


@dataclass(frozen=True)
class SourceInfo:
    kind: str = "unknown"
    name: str = "unknown"
    version: str | None = None


@dataclass(frozen=True)
class ProjectIdentity:
    project_id: str = "project:unknown"
    repository: str | None = None
    root: str | None = None
    remote: str | None = None
    branch: str | None = None
    commit: str | None = None
    worktree: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "repository": self.repository,
            "root": self.root,
            "remote": self.remote,
            "branch": self.branch,
            "commit": self.commit,
            "worktree": self.worktree,
        }


@dataclass(frozen=True)
class ExecutionContext:
    trace_id: str | None = None
    span_id: str | None = None
    parent_event_id: str | None = None
    session_id: str | None = None
    workflow_id: str | None = None
    agent_id: str | None = None
    subagent_id: str | None = None
    parent_agent_id: str | None = None
    role: str | None = None
    skill: str | None = None
    lane: str | None = None
    task_id: str | None = None
    task_class: str | None = None


@dataclass(frozen=True)
class LLMIdentity:
    provider: str = "unknown"
    model: str = "unknown"
    model_family: str | None = None
    model_variant: str | None = None
    client: str = "unknown"
    auth_mode: str = "unknown"
    route: str = "unknown"
    reasoning_effort: str | None = None


@dataclass(frozen=True)
class Usage:
    input_tokens: int | float | None = None
    output_tokens: int | float | None = None
    cached_tokens: int | float | None = None
    cache_creation_tokens: int | float | None = None
    cache_read_tokens: int | float | None = None
    reasoning_tokens: int | float | None = None
    total_tokens: int | float | None = None
    cost: int | float | None = None
    context_size: int | float | None = None
    context_utilization: int | float | None = None
    compaction_count: int | float | None = None
    source: str = "unknown"


@dataclass(frozen=True)
class Performance:
    latency_ms: int | float | None = None
    time_to_first_token_ms: int | float | None = None
    duration_ms: int | float | None = None
    tool_duration_ms: int | float | None = None
    session_duration_ms: int | float | None = None
    agent_duration_ms: int | float | None = None
    workflow_duration_ms: int | float | None = None
    wall_clock_ms: int | float | None = None
    concurrency: int | float | None = None
    parallel_utilization: int | float | None = None


@dataclass(frozen=True)
class Reliability:
    status: str = "unknown"
    error_kind: str | None = None
    retry_count: int | float | None = None
    rate_limited: bool | None = None
    timeout: bool | None = None
    tool_failure: bool | None = None
    agent_failure: bool | None = None
    aborted: bool | None = None
    reassessment_count: int | float | None = None
    rework_count: int | float | None = None


@dataclass(frozen=True)
class AgentBehavior:
    """Metadata-only agent activity counts; raw paths, commands, and payloads are excluded."""

    tool_call_count: int | float | None = None
    tool_names: tuple[str, ...] = ()
    files_inspected_count: int | float | None = None
    files_changed_count: int | float | None = None
    commands_executed_count: int | float | None = None
    tests_invoked_count: int | float | None = None


@dataclass(frozen=True)
class Outcome:
    kind: str | None = None
    status: str | None = None
    correlation_id: str | None = None
    correlation_basis: str | None = None
    evidence_source: str | None = None


@dataclass(frozen=True)
class Provenance:
    fields: Mapping[str, str] = field(default_factory=dict)
    adapter: str = "unknown"
    semantic_conventions: str = "gen_ai.experimental"
    content_capture: str = "disabled"


@dataclass(frozen=True)
class NormalizedEvent:
    schema_version: str
    event_id: str
    event_type: str
    observed_at: datetime
    received_at: datetime
    source: SourceInfo = field(default_factory=SourceInfo)
    project: ProjectIdentity = field(default_factory=ProjectIdentity)
    execution: ExecutionContext = field(default_factory=ExecutionContext)
    llm: LLMIdentity = field(default_factory=LLMIdentity)
    usage: Usage = field(default_factory=Usage)
    performance: Performance = field(default_factory=Performance)
    reliability: Reliability = field(default_factory=Reliability)
    behavior: AgentBehavior = field(default_factory=AgentBehavior)
    outcome: Outcome = field(default_factory=Outcome)
    provenance: Provenance = field(default_factory=Provenance)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    extensions: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
        source_kind: str | None = None,
        source_name: str | None = None,
    ) -> "NormalizedEvent":
        if not isinstance(value, Mapping):
            raise ContractError("event must be an object")
        schema_version = _as_text(value.get("schema_version"), "schema_version", required=True)
        event_id = _as_text(value.get("event_id"), "event_id", required=True)
        event_type = _as_text(value.get("event_type"), "event_type", required=True)
        observed_value = value.get("observed_at")
        if observed_value is None:
            raise ContractError("observed_at is required")
        observed_at = ensure_utc(observed_value, "observed_at")
        received_value = value.get("received_at", received_at or utc_now())
        received_at_utc = ensure_utc(received_value, "received_at")

        source = _as_mapping(value.get("source"), "source")
        project = _as_mapping(value.get("project"), "project")
        execution = _as_mapping(value.get("execution"), "execution")
        llm = _as_mapping(value.get("llm"), "llm")
        usage = _as_mapping(value.get("usage"), "usage")
        performance = _as_mapping(value.get("performance"), "performance")
        reliability = dict(_as_mapping(value.get("reliability"), "reliability"))
        behavior = dict(_as_mapping(value.get("behavior"), "behavior"))
        outcome = _as_mapping(value.get("outcome"), "outcome")
        provenance = _as_mapping(value.get("provenance"), "provenance")
        attributes = _as_mapping(value.get("attributes"), "attributes")
        extensions = dict(_as_mapping(value.get("extensions"), "extensions"))

        for alias, canonical_key in {
            "agent_failed": "agent_failure",
            "agentFailure": "agent_failure",
        }.items():
            if canonical_key not in reliability and alias in reliability:
                reliability[canonical_key] = reliability[alias]

        # Accept common adapter spellings without making callers duplicate a
        # metadata-only behavior section. Raw lists remain transient inputs;
        # only bounded counts and tool names enter the normalized envelope.
        for behavior_key in (
            "tool_call_count", "tool_names", "tool_calls",
            "files_inspected_count", "files_inspected",
            "files_changed_count", "files_changed",
            "commands_executed_count", "commands_executed",
                "tests_invoked_count", "tests_invoked",
        ):
            if behavior_key not in behavior and behavior_key in attributes:
                behavior[behavior_key] = attributes[behavior_key]

        for reliability_key in (
            "agent_failure", "agent_failed", "agentFailure",
            "reassessment_count", "reassessments", "reassessmentCount",
            "rework_count", "rework", "rework_loops", "reworkCount",
        ):
            canonical_key = {
                "agent_failed": "agent_failure",
                "agentFailure": "agent_failure",
                "reassessments": "reassessment_count",
                "reassessmentCount": "reassessment_count",
                "rework": "rework_count",
                "rework_loops": "rework_count",
                "reworkCount": "rework_count",
            }.get(reliability_key, reliability_key)
            if canonical_key not in reliability and reliability_key in attributes:
                reliability[canonical_key] = attributes[reliability_key]

        known = {
            "schema_version", "event_id", "event_type", "observed_at", "received_at",
            "source", "project", "execution", "llm", "usage", "performance",
            "reliability", "behavior", "outcome", "provenance", "attributes", "extensions",
        }
        unknown_top_level = {key: item for key, item in value.items() if key not in known}
        if unknown_top_level:
            existing_unknown = extensions.get("unknown_top_level")
            merged_unknown = dict(existing_unknown) if isinstance(existing_unknown, Mapping) else {}
            merged_unknown.update(unknown_top_level)
            extensions["unknown_top_level"] = merged_unknown

        section_specs = {
            "source": (source, {"kind", "name", "version"}),
            "project": (project, {"project_id", "repository", "root", "remote", "branch", "commit", "worktree"}),
            "execution": (execution, {"trace_id", "span_id", "parent_event_id", "session_id", "workflow_id", "agent_id", "subagent_id", "parent_agent_id", "parentAgentId", "parent_agent", "role", "skill", "lane", "task_id", "task_class"}),
            "llm": (llm, {"provider", "model", "model_family", "model_variant", "client", "auth_mode", "route", "reasoning_effort"}),
            "usage": (usage, {"input_tokens", "output_tokens", "cached_tokens", "cache_creation_tokens", "cache_read_tokens", "reasoning_tokens", "total_tokens", "cost", "context_size", "context_utilization", "compaction_count", "source"}),
            "performance": (performance, {"latency_ms", "time_to_first_token_ms", "duration_ms", "tool_duration_ms", "session_duration_ms", "agent_duration_ms", "workflow_duration_ms", "wall_clock_ms", "concurrency", "parallel_utilization"}),
            "reliability": (reliability, {"status", "error_kind", "retry_count", "rate_limited", "timeout", "tool_failure", "agent_failure", "aborted", "reassessment_count", "reassessments", "reassessmentCount", "rework_count", "rework", "rework_loops", "reworkCount"}),
            "behavior": (behavior, {"tool_call_count", "tool_names", "tool_calls", "files_inspected_count", "files_inspected", "files_changed_count", "files_changed", "commands_executed_count", "commands_executed", "tests_invoked_count", "tests_invoked"}),
            "outcome": (outcome, {"kind", "status", "correlation_id", "correlation_basis", "evidence_source"}),
            "provenance": (provenance, {"fields", "adapter", "semantic_conventions", "content_capture"}),
        }
        unknown_fields = dict(extensions.get("unknown_fields", {})) if isinstance(extensions.get("unknown_fields"), Mapping) else {}
        for section_name, (section_value, known_keys) in section_specs.items():
            extra = {key: item for key, item in section_value.items() if key not in known_keys}
            if extra:
                prior = unknown_fields.get(section_name)
                merged = dict(prior) if isinstance(prior, Mapping) else {}
                merged.update(extra)
                unknown_fields[section_name] = merged
        if unknown_fields:
            extensions["unknown_fields"] = unknown_fields

        source_obj = SourceInfo(
            kind=_as_text(source_kind if source_kind is not None else source.get("kind"), "source.kind") or "unknown",
            name=_as_text(source_name if source_name is not None else source.get("name"), "source.name") or "unknown",
            version=_as_text(source.get("version"), "source.version"),
        )
        project_obj = ProjectIdentity(
            project_id=_as_text(project.get("project_id"), "project.project_id") or "project:unknown",
            repository=_as_text(project.get("repository"), "project.repository"),
            root=_as_text(project.get("root"), "project.root"),
            remote=_as_text(project.get("remote"), "project.remote"),
            branch=_as_text(project.get("branch"), "project.branch"),
            commit=_as_text(project.get("commit"), "project.commit"),
            worktree=_as_text(project.get("worktree"), "project.worktree"),
        )
        execution_obj = ExecutionContext(
            trace_id=_as_text(execution.get("trace_id"), "execution.trace_id"),
            span_id=_as_text(execution.get("span_id"), "execution.span_id"),
            parent_event_id=_as_text(execution.get("parent_event_id"), "execution.parent_event_id"),
            session_id=_as_text(execution.get("session_id"), "execution.session_id"),
            workflow_id=_as_text(execution.get("workflow_id"), "execution.workflow_id"),
            agent_id=_as_text(execution.get("agent_id"), "execution.agent_id"),
            subagent_id=_as_text(execution.get("subagent_id"), "execution.subagent_id"),
            parent_agent_id=_as_text(
                execution.get("parent_agent_id")
                or execution.get("parentAgentId")
                or execution.get("parent_agent"),
                "execution.parent_agent_id",
            ),
            role=_as_text(execution.get("role"), "execution.role"),
            skill=_as_text(execution.get("skill"), "execution.skill"),
            lane=_as_text(execution.get("lane"), "execution.lane"),
            task_id=_as_text(execution.get("task_id"), "execution.task_id"),
            task_class=_as_text(execution.get("task_class"), "execution.task_class"),
        )
        llm_obj = LLMIdentity(
            provider=_as_text(llm.get("provider"), "llm.provider") or "unknown",
            model=_as_text(llm.get("model"), "llm.model") or "unknown",
            model_family=_as_text(llm.get("model_family"), "llm.model_family"),
            model_variant=_as_text(llm.get("model_variant"), "llm.model_variant"),
            client=_as_text(llm.get("client"), "llm.client") or "unknown",
            auth_mode=_as_text(llm.get("auth_mode"), "llm.auth_mode") or "unknown",
            route=_as_text(llm.get("route"), "llm.route") or "unknown",
            reasoning_effort=_as_text(llm.get("reasoning_effort"), "llm.reasoning_effort"),
        )
        usage_obj = Usage(
            input_tokens=_as_nonnegative_number(usage.get("input_tokens"), "usage.input_tokens"),
            output_tokens=_as_nonnegative_number(usage.get("output_tokens"), "usage.output_tokens"),
            cached_tokens=_as_nonnegative_number(usage.get("cached_tokens"), "usage.cached_tokens"),
            cache_creation_tokens=_as_nonnegative_number(usage.get("cache_creation_tokens"), "usage.cache_creation_tokens"),
            cache_read_tokens=_as_nonnegative_number(usage.get("cache_read_tokens"), "usage.cache_read_tokens"),
            reasoning_tokens=_as_nonnegative_number(usage.get("reasoning_tokens"), "usage.reasoning_tokens"),
            total_tokens=_as_nonnegative_number(usage.get("total_tokens"), "usage.total_tokens"),
            cost=_as_nonnegative_number(usage.get("cost"), "usage.cost"),
            context_size=_as_nonnegative_number(usage.get("context_size"), "usage.context_size"),
            context_utilization=_as_nonnegative_number(usage.get("context_utilization"), "usage.context_utilization"),
            compaction_count=_as_nonnegative_number(usage.get("compaction_count"), "usage.compaction_count"),
            source=_as_text(usage.get("source"), "usage.source") or "unknown",
        )
        performance_obj = Performance(
            latency_ms=_as_nonnegative_number(performance.get("latency_ms"), "performance.latency_ms"),
            time_to_first_token_ms=_as_nonnegative_number(performance.get("time_to_first_token_ms"), "performance.time_to_first_token_ms"),
            duration_ms=_as_nonnegative_number(performance.get("duration_ms"), "performance.duration_ms"),
            tool_duration_ms=_as_nonnegative_number(performance.get("tool_duration_ms"), "performance.tool_duration_ms"),
            session_duration_ms=_as_nonnegative_number(performance.get("session_duration_ms"), "performance.session_duration_ms"),
            agent_duration_ms=_as_nonnegative_number(performance.get("agent_duration_ms"), "performance.agent_duration_ms"),
            workflow_duration_ms=_as_nonnegative_number(performance.get("workflow_duration_ms"), "performance.workflow_duration_ms"),
            wall_clock_ms=_as_nonnegative_number(performance.get("wall_clock_ms"), "performance.wall_clock_ms"),
            concurrency=_as_nonnegative_number(performance.get("concurrency"), "performance.concurrency"),
            parallel_utilization=_as_nonnegative_number(performance.get("parallel_utilization"), "performance.parallel_utilization"),
        )
        reliability_obj = Reliability(
            status=_as_text(reliability.get("status"), "reliability.status") or "unknown",
            error_kind=_as_text(reliability.get("error_kind"), "reliability.error_kind"),
            retry_count=_as_nonnegative_number(reliability.get("retry_count"), "reliability.retry_count"),
            rate_limited=_as_optional_bool(reliability.get("rate_limited"), "reliability.rate_limited"),
            timeout=_as_optional_bool(reliability.get("timeout"), "reliability.timeout"),
            tool_failure=_as_optional_bool(reliability.get("tool_failure"), "reliability.tool_failure"),
            agent_failure=_as_optional_bool(reliability.get("agent_failure"), "reliability.agent_failure"),
            aborted=_as_optional_bool(reliability.get("aborted"), "reliability.aborted"),
            reassessment_count=_reliability_count(reliability, "reassessment_count", "reassessments", "reassessmentCount"),
            rework_count=_reliability_count(reliability, "rework_count", "rework", "rework_loops", "reworkCount"),
        )
        behavior_obj = AgentBehavior(
            tool_call_count=_behavior_count(behavior, "tool_call_count", "tool_calls"),
            tool_names=_behavior_tool_names(behavior),
            files_inspected_count=_behavior_count(behavior, "files_inspected_count", "files_inspected"),
            files_changed_count=_behavior_count(behavior, "files_changed_count", "files_changed"),
            commands_executed_count=_behavior_count(behavior, "commands_executed_count", "commands_executed"),
            tests_invoked_count=_behavior_count(behavior, "tests_invoked_count", "tests_invoked"),
        )
        outcome_obj = Outcome(
            kind=_as_text(outcome.get("kind"), "outcome.kind"),
            status=_as_text(outcome.get("status"), "outcome.status"),
            correlation_id=_as_text(outcome.get("correlation_id"), "outcome.correlation_id"),
            correlation_basis=_as_text(outcome.get("correlation_basis"), "outcome.correlation_basis"),
            evidence_source=_as_text(outcome.get("evidence_source"), "outcome.evidence_source"),
        )
        provenance_obj = Provenance(
            fields=dict(_as_mapping(provenance.get("fields"), "provenance.fields")),
            adapter=_as_text(provenance.get("adapter"), "provenance.adapter") or "unknown",
            semantic_conventions=_as_text(provenance.get("semantic_conventions"), "provenance.semantic_conventions") or "gen_ai.experimental",
            content_capture=_as_text(provenance.get("content_capture"), "provenance.content_capture") or "disabled",
        )
        return cls(
            schema_version=schema_version,
            event_id=event_id,
            event_type=event_type,
            observed_at=observed_at,
            received_at=received_at_utc,
            source=source_obj,
            project=project_obj,
            execution=execution_obj,
            llm=llm_obj,
            usage=usage_obj,
            performance=performance_obj,
            reliability=reliability_obj,
            behavior=behavior_obj,
            outcome=outcome_obj,
            provenance=provenance_obj,
            attributes=dict(attributes),
            extensions=extensions,
        )

    def to_mapping(self) -> dict[str, Any]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat().replace("+00:00", "Z")
        value["received_at"] = self.received_at.isoformat().replace("+00:00", "Z")
        return value

    def to_json(self) -> str:
        return canonical_json(self.to_mapping())
