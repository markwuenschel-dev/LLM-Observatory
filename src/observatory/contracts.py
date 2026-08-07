"""Versioned, provider-neutral normalized telemetry contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
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


def stable_event_id(value: Mapping[str, Any]) -> str:
    """Return an existing event ID or a deterministic content-derived ID."""

    existing = value.get("event_id")
    if isinstance(existing, str) and existing.strip():
        return existing.strip()
    digest = hashlib.sha256(canonical_json(dict(value)).encode("utf-8")).hexdigest()
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


@dataclass(frozen=True)
class ExecutionContext:
    trace_id: str | None = None
    span_id: str | None = None
    parent_event_id: str | None = None
    session_id: str | None = None
    workflow_id: str | None = None
    agent_id: str | None = None
    subagent_id: str | None = None
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
    aborted: bool | None = None


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
        reliability = _as_mapping(value.get("reliability"), "reliability")
        outcome = _as_mapping(value.get("outcome"), "outcome")
        provenance = _as_mapping(value.get("provenance"), "provenance")
        attributes = _as_mapping(value.get("attributes"), "attributes")
        extensions = dict(_as_mapping(value.get("extensions"), "extensions"))

        known = {
            "schema_version", "event_id", "event_type", "observed_at", "received_at",
            "source", "project", "execution", "llm", "usage", "performance",
            "reliability", "outcome", "provenance", "attributes", "extensions",
        }
        unknown_top_level = {key: item for key, item in value.items() if key not in known}
        if unknown_top_level:
            existing_unknown = extensions.get("unknown_top_level")
            merged_unknown = dict(existing_unknown) if isinstance(existing_unknown, Mapping) else {}
            merged_unknown.update(unknown_top_level)
            extensions["unknown_top_level"] = merged_unknown

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
            client=_as_text(llm.get("client"), "llm.client") or "unknown",
            auth_mode=_as_text(llm.get("auth_mode"), "llm.auth_mode") or "unknown",
            route=_as_text(llm.get("route"), "llm.route") or "unknown",
            reasoning_effort=_as_text(llm.get("reasoning_effort"), "llm.reasoning_effort"),
        )
        usage_obj = Usage(
            input_tokens=_as_number(usage.get("input_tokens"), "usage.input_tokens"),
            output_tokens=_as_number(usage.get("output_tokens"), "usage.output_tokens"),
            cached_tokens=_as_number(usage.get("cached_tokens"), "usage.cached_tokens"),
            cache_creation_tokens=_as_number(usage.get("cache_creation_tokens"), "usage.cache_creation_tokens"),
            cache_read_tokens=_as_number(usage.get("cache_read_tokens"), "usage.cache_read_tokens"),
            reasoning_tokens=_as_number(usage.get("reasoning_tokens"), "usage.reasoning_tokens"),
            total_tokens=_as_number(usage.get("total_tokens"), "usage.total_tokens"),
            cost=_as_number(usage.get("cost"), "usage.cost"),
            context_size=_as_number(usage.get("context_size"), "usage.context_size"),
            context_utilization=_as_number(usage.get("context_utilization"), "usage.context_utilization"),
            compaction_count=_as_number(usage.get("compaction_count"), "usage.compaction_count"),
            source=_as_text(usage.get("source"), "usage.source") or "unknown",
        )
        performance_obj = Performance(
            latency_ms=_as_number(performance.get("latency_ms"), "performance.latency_ms"),
            time_to_first_token_ms=_as_number(performance.get("time_to_first_token_ms"), "performance.time_to_first_token_ms"),
            duration_ms=_as_number(performance.get("duration_ms"), "performance.duration_ms"),
            tool_duration_ms=_as_number(performance.get("tool_duration_ms"), "performance.tool_duration_ms"),
            session_duration_ms=_as_number(performance.get("session_duration_ms"), "performance.session_duration_ms"),
            agent_duration_ms=_as_number(performance.get("agent_duration_ms"), "performance.agent_duration_ms"),
            workflow_duration_ms=_as_number(performance.get("workflow_duration_ms"), "performance.workflow_duration_ms"),
            wall_clock_ms=_as_number(performance.get("wall_clock_ms"), "performance.wall_clock_ms"),
            concurrency=_as_number(performance.get("concurrency"), "performance.concurrency"),
            parallel_utilization=_as_number(performance.get("parallel_utilization"), "performance.parallel_utilization"),
        )
        reliability_obj = Reliability(
            status=_as_text(reliability.get("status"), "reliability.status") or "unknown",
            error_kind=_as_text(reliability.get("error_kind"), "reliability.error_kind"),
            retry_count=_as_number(reliability.get("retry_count"), "reliability.retry_count"),
            rate_limited=reliability.get("rate_limited") if isinstance(reliability.get("rate_limited"), bool) else None,
            timeout=reliability.get("timeout") if isinstance(reliability.get("timeout"), bool) else None,
            tool_failure=reliability.get("tool_failure") if isinstance(reliability.get("tool_failure"), bool) else None,
            aborted=reliability.get("aborted") if isinstance(reliability.get("aborted"), bool) else None,
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
