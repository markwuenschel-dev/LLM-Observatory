"""Bounded Prometheus read API for event-time Observatory dashboards.

The normalized event store is not a Prometheus scrape target.  Its values are
facts about an event's ``observed_at`` time, so a current ``/metrics`` snapshot
cannot implement Grafana's time picker correctly.  This module exposes the
small, deliberately documented PromQL subset used by the provisioned
dashboards and evaluates it against SQLite at event time.  Collector
self-metrics remain on the real Prometheus service.

This is a read-only compatibility facade, not an inference or telemetry
proxy.  Queries are bounded by a maximum point count and only known metric
families, labels, and operators are accepted.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import re
import time
from typing import Any, Iterable, Mapping

from .store import EventStore


class PrometheusQueryError(ValueError):
    """A query is invalid or outside the supported bounded facade."""


@dataclass(frozen=True)
class _MetricDefinition:
    name: str
    labels: tuple[str, ...]
    label_sql: Mapping[str, str]
    from_sql: str
    observed_sql: str
    where_sql: str
    aggregate: str
    value_sql: str | None = None


@dataclass(frozen=True)
class _Matcher:
    label: str
    operator: str
    value: str


@dataclass(frozen=True)
class _Selector:
    metric: str
    matchers: tuple[_Matcher, ...]


@dataclass
class _Series:
    labels: dict[str, str]
    values: list[float]


_EVENT_LABEL_SQL: dict[str, str] = {
    "event_type": "COALESCE(e.event_type, 'unknown')",
    "project": "COALESCE(e.project_id, 'unknown')",
    "repository": "COALESCE(e.repository, 'unknown')",
    "branch": "COALESCE(e.branch, 'unknown')",
    "provider": "COALESCE(e.provider, 'unknown')",
    "model": "COALESCE(e.model, 'unknown')",
    "model_family": "COALESCE(e.model_family, 'unknown')",
    "model_variant": "COALESCE(e.model_variant, 'unknown')",
    "client": "COALESCE(e.client, 'unknown')",
    "auth_mode": "COALESCE(e.auth_mode, 'unknown')",
    "route": "COALESCE(e.route, 'unknown')",
    "usage_source": "COALESCE(e.usage_source, 'unknown')",
    "agent": "COALESCE(e.agent_id, 'unknown')",
    "subagent": "COALESCE(e.subagent_id, 'unknown')",
    "parent_agent": "COALESCE(e.parent_agent_id, 'unknown')",
    "role": "COALESCE(e.role, 'unknown')",
    "skill": "COALESCE(e.skill, 'unknown')",
    "lane": "COALESCE(e.lane, 'unknown')",
    "workflow": "COALESCE(e.workflow_id, 'unknown')",
    "task_class": "COALESCE(e.task_class, 'unknown')",
    "status": "COALESCE(e.status, 'unknown')",
}

_PROVIDER_LABELS = (
    "project",
    "provider",
    "model",
    "model_family",
    "model_variant",
    "client",
    "auth_mode",
    "route",
    "usage_source",
    "task_class",
)
_CONTEXT_LABELS = (
    "event_type",
    "project",
    "repository",
    "branch",
    "provider",
    "model",
    "model_family",
    "model_variant",
    "client",
    "auth_mode",
    "route",
    "usage_source",
    "agent",
    "subagent",
    "parent_agent",
    "role",
    "skill",
    "lane",
    "workflow",
    "task_class",
    "status",
)


def _event_definition(
    name: str,
    labels: Iterable[str],
    aggregate: str,
    *,
    where: str = "1 = 1",
    value_sql: str | None = None,
) -> _MetricDefinition:
    label_tuple = tuple(labels)
    return _MetricDefinition(
        name=name,
        labels=label_tuple,
        label_sql={label: _EVENT_LABEL_SQL[label] for label in label_tuple},
        from_sql="events e",
        observed_sql="e.observed_at",
        where_sql=where,
        aggregate=aggregate,
        value_sql=value_sql,
    )


def _definitions() -> dict[str, _MetricDefinition]:
    definitions: dict[str, _MetricDefinition] = {}

    global_counts = {
        "observatory_events_total": "1 = 1",
        "observatory_event_successes_total": "e.status IN ('ok', 'success', 'succeeded')",
        "observatory_event_failures_total": "e.status IN ('error', 'failed', 'failure')",
        "observatory_retries_total": "1 = 1",
        "observatory_rate_limited_total": "e.rate_limited = 1",
        "observatory_timeouts_total": "e.timeout = 1",
        "observatory_tool_failures_total": "e.tool_failure = 1",
        "observatory_agent_failures_total": "e.agent_failure = 1",
    }
    for name, where in global_counts.items():
        if name == "observatory_retries_total":
            definitions[name] = _event_definition(name, (), "sum", where=where, value_sql="e.retry_count")
        else:
            definitions[name] = _event_definition(name, (), "count", where=where)

    for name, column in {
        "observatory_input_tokens_total": "input_tokens",
        "observatory_output_tokens_total": "output_tokens",
        "observatory_cached_tokens_total": "cached_tokens",
        "observatory_cache_creation_tokens_total": "cache_creation_tokens",
        "observatory_cache_read_tokens_total": "cache_read_tokens",
        "observatory_reasoning_tokens_total": "reasoning_tokens",
        "observatory_compactions_total": "compaction_count",
        "observatory_cost_total": "cost",
        "observatory_tool_calls_total": "tool_call_count",
        "observatory_files_inspected_total": "files_inspected_count",
        "observatory_files_changed_total": "files_changed_count",
        "observatory_commands_executed_total": "commands_executed_count",
        "observatory_tests_invoked_total": "tests_invoked_count",
        "observatory_reassessments_total": "reassessment_count",
        "observatory_rework_loops_total": "rework_count",
    }.items():
        definitions[name] = _event_definition(name, (), "sum", value_sql=f"e.{column}")
    for name, column in {
        "observatory_latency_average_ms": "latency_ms",
        "observatory_time_to_first_token_average_ms": "time_to_first_token_ms",
        "observatory_duration_average_ms": "duration_ms",
        "observatory_context_size_average": "context_size",
        "observatory_context_utilization_average": "context_utilization",
        "observatory_concurrency_average": "concurrency",
        "observatory_parallel_utilization_average": "parallel_utilization",
    }.items():
        definitions[name] = _event_definition(name, (), "avg", value_sql=f"e.{column}")

    # Conflicts are attributed to the canonical event's observed time.  The
    # conflict itself is received later, but this keeps the dashboard's event
    # time contract stable and makes replay diagnosis filterable by the same
    # range as the original operation.
    definitions["observatory_event_conflicts_total"] = _MetricDefinition(
        name="observatory_event_conflicts_total",
        labels=(),
        label_sql={},
        from_sql="event_conflicts c JOIN events e ON e.event_id = c.event_id",
        observed_sql="e.observed_at",
        where_sql="1 = 1",
        aggregate="count",
    )

    provider_metrics = {
        "observatory_events_by_provider_model_total": ("count", None),
        "observatory_success_rate_by_provider_model": ("ratio", None),
        "observatory_tokens_by_provider_model_total": ("sum", "e.total_tokens"),
        "observatory_cost_by_provider_model": ("sum", "e.cost"),
        "observatory_latency_average_by_provider_model_ms": ("avg", "e.latency_ms"),
        "observatory_retries_by_provider_model_total": ("sum", "e.retry_count"),
        "observatory_rate_limited_by_provider_model_total": ("flag", "e.rate_limited"),
        "observatory_timeouts_by_provider_model_total": ("flag", "e.timeout"),
        "observatory_tool_failures_by_provider_model_total": ("flag", "e.tool_failure"),
        "observatory_agent_failures_by_provider_model_total": ("flag", "e.agent_failure"),
        "observatory_reassessments_by_provider_model_total": ("sum", "e.reassessment_count"),
        "observatory_rework_loops_by_provider_model_total": ("sum", "e.rework_count"),
        "observatory_tool_calls_by_provider_model_total": ("sum", "e.tool_call_count"),
        "observatory_files_changed_by_provider_model_total": ("sum", "e.files_changed_count"),
    }
    for name, (aggregate, value_sql) in provider_metrics.items():
        definitions[name] = _event_definition(
            name,
            _PROVIDER_LABELS,
            aggregate,
            where="e.event_type = 'model.operation'",
            value_sql=value_sql,
        )

    context_metrics = {
        "observatory_events_by_context_total": ("count", None),
        "observatory_input_tokens_by_context_total": ("sum", "e.input_tokens"),
        "observatory_output_tokens_by_context_total": ("sum", "e.output_tokens"),
        "observatory_cached_tokens_by_context_total": ("sum", "e.cached_tokens"),
        "observatory_reasoning_tokens_by_context_total": ("sum", "e.reasoning_tokens"),
        "observatory_tokens_by_context_total": ("sum", "e.total_tokens"),
        "observatory_cache_creation_tokens_by_context_total": ("sum", "e.cache_creation_tokens"),
        "observatory_cache_read_tokens_by_context_total": ("sum", "e.cache_read_tokens"),
        "observatory_compactions_by_context_total": ("sum", "e.compaction_count"),
        "observatory_cost_by_context": ("sum", "e.cost"),
        "observatory_latency_average_by_context_ms": ("avg", "e.latency_ms"),
        "observatory_time_to_first_token_average_by_context_ms": ("avg", "e.time_to_first_token_ms"),
        "observatory_duration_average_by_context_ms": ("avg", "e.duration_ms"),
        "observatory_context_size_average_by_context": ("avg", "e.context_size"),
        "observatory_context_utilization_average_by_context": ("avg", "e.context_utilization"),
        "observatory_concurrency_average_by_context": ("avg", "e.concurrency"),
        "observatory_parallel_utilization_average_by_context": ("avg", "e.parallel_utilization"),
        "observatory_retries_by_context_total": ("sum", "e.retry_count"),
        "observatory_rate_limited_by_context_total": ("flag", "e.rate_limited"),
        "observatory_timeouts_by_context_total": ("flag", "e.timeout"),
        "observatory_tool_failures_by_context_total": ("flag", "e.tool_failure"),
        "observatory_agent_failures_by_context_total": ("flag", "e.agent_failure"),
        "observatory_reassessments_by_context_total": ("sum", "e.reassessment_count"),
        "observatory_rework_loops_by_context_total": ("sum", "e.rework_count"),
        "observatory_tool_calls_by_context_total": ("sum", "e.tool_call_count"),
        "observatory_files_inspected_by_context_total": ("sum", "e.files_inspected_count"),
        "observatory_files_changed_by_context_total": ("sum", "e.files_changed_count"),
        "observatory_commands_executed_by_context_total": ("sum", "e.commands_executed_count"),
        "observatory_tests_invoked_by_context_total": ("sum", "e.tests_invoked_count"),
    }
    for name, (aggregate, value_sql) in context_metrics.items():
        definitions[name] = _event_definition(name, _CONTEXT_LABELS, aggregate, value_sql=value_sql)

    definitions["observatory_events_by_usage_source_total"] = _event_definition(
        "observatory_events_by_usage_source_total", ("usage_source",), "count"
    )
    definitions["observatory_events_by_project_total"] = _event_definition(
        "observatory_events_by_project_total", ("project",), "count"
    )
    definitions["observatory_events_by_client_route_total"] = _event_definition(
        "observatory_events_by_client_route_total", ("project", "client", "route", "auth_mode"), "count"
    )
    definitions["observatory_events_by_execution_total"] = _event_definition(
        "observatory_events_by_execution_total",
        ("event_type", "project", "repository", "branch", "role", "skill", "lane"),
        "count",
    )
    definitions["observatory_events_by_workflow_total"] = _event_definition(
        "observatory_events_by_workflow_total",
        ("event_type", "project", "repository", "branch", "workflow"),
        "count",
    )
    definitions["observatory_events_by_agent_total"] = _event_definition(
        "observatory_events_by_agent_total",
        ("event_type", "project", "repository", "branch", "agent", "subagent", "parent_agent"),
        "count",
    )

    outcome_labels = {
        "kind": "COALESCE(o.kind, 'unknown')",
        "status": "COALESCE(o.status, 'unknown')",
        "evidence_source": "COALESCE(o.evidence_source, 'unknown')",
        "project": "COALESCE(e.project_id, 'unknown')",
        "repository": "COALESCE(e.repository, 'unknown')",
        "branch": "COALESCE(e.branch, 'unknown')",
        "correlation_basis": "COALESCE(o.correlation_basis, 'uncorrelated')",
    }
    definitions["observatory_outcomes_by_kind_status_total"] = _MetricDefinition(
        name="observatory_outcomes_by_kind_status_total",
        labels=("kind", "status", "evidence_source", "project", "repository", "branch", "correlation_basis"),
        label_sql=outcome_labels,
        from_sql="outcome_events o JOIN events e ON e.event_id = o.event_id",
        observed_sql="e.observed_at",
        where_sql="1 = 1",
        aggregate="count",
    )
    return definitions


METRIC_DEFINITIONS = _definitions()
_LABEL_NAMES = tuple(sorted({"__name__", *(label for definition in METRIC_DEFINITIONS.values() for label in definition.labels)}))
_METRIC_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_MATCHER_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*(\"(?:\\.|[^\"])*\")$")
_MAX_POINTS = 10_000
_MAX_QUERY_LENGTH = 16 * 1024
_MAX_MATCHERS = 64
_MAX_REGEX_LENGTH = 256
_MAX_MATRIX_SERIES = 4_096
_MAX_MATRIX_VALUES = 2_000_000
_MAX_QUERY_ROWS = min(_MAX_MATRIX_VALUES, _MAX_MATRIX_SERIES * 64) + 1
# The facade intentionally evaluates event-time data from SQLite instead of
# pretending the normalized store is an unbounded Prometheus database.  Keep
# every selector family inside a finite lookback even when a caller omits
# start/end or asks for an instant cumulative value.
_MAX_LOOKBACK_SECONDS = 366.0 * 24.0 * 60.0 * 60.0


def _parse_time(value: str | None, *, default: float) -> float:
    if value is None or value == "":
        return default
    text = str(value).strip()
    try:
        parsed = float(text)
    except ValueError:
        try:
            iso = text[:-1] + "+00:00" if text.endswith("Z") else text
            datetime_value = datetime.fromisoformat(iso)
        except (ValueError, OSError, OverflowError) as exc:
            raise PrometheusQueryError(f"invalid Prometheus timestamp: {value}") from exc
        if datetime_value.tzinfo is None:
            raise PrometheusQueryError("Prometheus timestamps must include a timezone")
        parsed = datetime_value.astimezone(timezone.utc).timestamp()
    if not math.isfinite(parsed):
        raise PrometheusQueryError("Prometheus timestamp must be finite")
    if parsed < -62_135_596_800 or parsed > 253_402_300_799:
        raise PrometheusQueryError("Prometheus timestamp is outside the supported UTC range")
    return parsed


def _parse_step(value: str | None, start: float, end: float) -> float:
    if value is None or value == "":
        return max((end - start) / 240.0, 1.0)
    text = str(value).strip().lower()
    multipliers = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
    try:
        if text[-1:] in multipliers:
            step = float(text[:-1]) * multipliers[text[-1:]]
        else:
            step = float(text)
    except (ValueError, IndexError) as exc:
        raise PrometheusQueryError(f"invalid Prometheus step: {value}") from exc
    if not math.isfinite(step) or step <= 0:
        raise PrometheusQueryError("Prometheus step must be a positive finite number")
    return step


def _time_window(params: Mapping[str, list[str]], *, instant: bool) -> tuple[float, float, float, int]:
    def one(name: str) -> str | None:
        values = params.get(name, [])
        if len(values) > 1:
            raise PrometheusQueryError(f"Prometheus parameter {name} must occur at most once")
        return values[0] if values else None

    now = time.time()
    if instant:
        at = _parse_time(one("time"), default=now)
        start = max(at - _MAX_LOOKBACK_SECONDS, 0.0)
        return start, at, max(at - start, 1.0), 1
    start = _parse_time(one("start"), default=max(now - 86_400.0, 0.0))
    end = _parse_time(one("end"), default=now)
    if end < start:
        raise PrometheusQueryError("Prometheus end must be after or equal to start")
    if end - start > _MAX_LOOKBACK_SECONDS:
        raise PrometheusQueryError(
            f"Prometheus range cannot exceed {_MAX_LOOKBACK_SECONDS:.0f} seconds"
        )
    step = _parse_step(one("step"), start, end)
    points = int(math.floor((end - start) / step)) + 1
    if points < 1 or points > _MAX_POINTS:
        raise PrometheusQueryError(f"Prometheus range must contain between 1 and {_MAX_POINTS} points")
    return start, end, step, points


def _unescape_prometheus_string(value: str) -> str:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise PrometheusQueryError("invalid Prometheus label string") from exc
    if not isinstance(parsed, str):
        raise PrometheusQueryError("invalid Prometheus label string")
    return parsed


def _split_matchers(body: str) -> list[str]:
    result: list[str] = []
    start = 0
    escaped = False
    quoted = False
    for index, char in enumerate(body):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quoted:
            escaped = True
            continue
        if char == '"':
            quoted = not quoted
            continue
        if char == "," and not quoted:
            result.append(body[start:index].strip())
            start = index + 1
    if quoted or escaped:
        raise PrometheusQueryError("unterminated Prometheus label matcher")
    tail = body[start:].strip()
    if tail:
        result.append(tail)
    return result


def _parse_selector(expression: str) -> _Selector:
    text = expression.strip()
    if not text:
        raise PrometheusQueryError("Prometheus query is empty")
    if len(text) > _MAX_QUERY_LENGTH:
        raise PrometheusQueryError(f"Prometheus query exceeds {_MAX_QUERY_LENGTH} characters")
    if "{" in text:
        opening = text.find("{")
        if not text.endswith("}") or text.count("{") != 1:
            raise PrometheusQueryError("invalid Prometheus selector")
        metric = text[:opening].strip()
        body = text[opening + 1 : -1]
    else:
        metric = text
        body = ""
    if not _METRIC_RE.fullmatch(metric):
        raise PrometheusQueryError("Prometheus selector must name one known metric")
    matchers: list[_Matcher] = []
    raw_matchers = _split_matchers(body)
    if len(raw_matchers) > _MAX_MATCHERS:
        raise PrometheusQueryError(f"Prometheus selector allows at most {_MAX_MATCHERS} matchers")
    for raw in raw_matchers:
        match = _MATCHER_RE.fullmatch(raw)
        if not match:
            raise PrometheusQueryError(f"invalid Prometheus matcher: {raw}")
        value = _unescape_prometheus_string(match.group(3))
        if len(value) > 1024:
            raise PrometheusQueryError("Prometheus label matcher is too large")
        if match.group(2) in {"=~", "!~"} and len(value) > _MAX_REGEX_LENGTH:
            raise PrometheusQueryError(
                f"Prometheus regex matcher exceeds {_MAX_REGEX_LENGTH} characters"
            )
        matchers.append(_Matcher(match.group(1), match.group(2), value))
    return _Selector(metric, tuple(matchers))


def _matching_close(text: str, opening: int) -> int:
    depth = 0
    quoted = False
    escaped = False
    for index in range(opening, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\" and quoted:
            escaped = True
            continue
        if char == '"':
            quoted = not quoted
            continue
        if quoted:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    raise PrometheusQueryError("unbalanced Prometheus expression")


def _strip_outer_parentheses(expression: str) -> str:
    text = expression.strip()
    while text.startswith("("):
        close = _matching_close(text, 0)
        if close != len(text) - 1:
            break
        text = text[1:-1].strip()
    return text


def _split_top_level(expression: str, token: str) -> tuple[str, str] | None:
    depth = 0
    quoted = False
    escaped = False
    index = 0
    while index <= len(expression) - len(token):
        char = expression[index]
        if escaped:
            escaped = False
            index += 1
            continue
        if char == "\\" and quoted:
            escaped = True
            index += 1
            continue
        if char == '"':
            quoted = not quoted
        elif not quoted and char == "(":
            depth += 1
        elif not quoted and char == ")":
            depth -= 1
        if not quoted and depth == 0 and expression.startswith(token, index):
            return expression[:index].strip(), expression[index + len(token) :].strip()
        index += 1
    return None


def _format_value(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == 0:
        return "0"
    return format(value, ".15g")


def _format_timestamp(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return format(value, ".15g")


class PrometheusQueryEngine:
    """Evaluate the bounded event-time Prometheus compatibility contract."""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    @staticmethod
    def _matches(labels: Mapping[str, str], matchers: Iterable[_Matcher]) -> bool:
        for matcher in matchers:
            if matcher.label == "__name__":
                value = labels.get("__name__", "")
            elif matcher.label not in labels:
                return False
            else:
                value = labels[matcher.label]
            if matcher.operator == "=":
                matched = value == matcher.value
            elif matcher.operator == "!=":
                matched = value != matcher.value
            else:
                try:
                    matched = re.fullmatch(matcher.value, value) is not None
                except re.error as exc:
                    raise PrometheusQueryError(f"invalid Prometheus regex for {matcher.label}") from exc
                if matcher.operator == "!~":
                    matched = not matched
            if not matched:
                return False
        return True

    @staticmethod
    def _iso(value: float) -> str:
        return datetime.fromtimestamp(value, timezone.utc).isoformat()

    def _base_matrix(self, selector: _Selector, start: float, end: float, step: float, points: int) -> list[_Series]:
        definition = METRIC_DEFINITIONS.get(selector.metric)
        if definition is None:
            raise PrometheusQueryError(f"unsupported event-time metric: {selector.metric}")
        unknown_labels = {matcher.label for matcher in selector.matchers if matcher.label != "__name__"} - set(definition.labels)
        if unknown_labels:
            raise PrometheusQueryError(
                f"metric {selector.metric} does not expose label {sorted(unknown_labels)[0]}"
            )
        labels_sql = [f"{definition.label_sql[label]} AS {label}" for label in definition.labels]
        bucket_sql = (
            # A sample at t represents all events through t.  Ceil the
            # event's offset so an event at 14:09 appears at the 14:10 sample,
            # while an event exactly at a 5-minute boundary stays on that
            # boundary.  The epsilon absorbs SQLite julianday round-off.
            "CAST((((julianday({observed}) - julianday(?)) * 86400.0) + ? - 0.001) / ? AS INTEGER) AS bucket"
        ).format(observed=definition.observed_sql)
        select_parts = [bucket_sql, *labels_sql]
        if definition.aggregate in {"count", "flag"}:
            if definition.aggregate == "flag":
                expression = definition.value_sql or "0"
                select_parts.append(f"SUM(CASE WHEN {expression} = 1 THEN 1 ELSE 0 END) AS value")
            else:
                select_parts.append("COUNT(*) AS value")
        elif definition.aggregate == "ratio":
            select_parts.extend([
                "SUM(CASE WHEN e.status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS success_value",
                "COUNT(*) AS value_count",
            ])
        elif definition.aggregate in {"sum", "avg"}:
            if not definition.value_sql:
                raise PrometheusQueryError(f"metric {selector.metric} has no value expression")
            select_parts.extend([
                f"SUM({definition.value_sql}) AS sum_value",
                f"COUNT({definition.value_sql}) AS value_count",
            ])
        else:
            raise PrometheusQueryError(f"metric {selector.metric} has unsupported aggregate")

        group_parts = ["bucket", *(definition.label_sql[label] for label in definition.labels)]
        query = (
            f"SELECT {', '.join(select_parts)} FROM {definition.from_sql} "
            f"WHERE {definition.observed_sql} >= ? AND {definition.observed_sql} <= ? "
            f"AND {definition.where_sql} GROUP BY {', '.join(group_parts)} ORDER BY bucket LIMIT ?"
        )
        rows = self.store.connection.execute(
            query,
            (self._iso(start), step, step, self._iso(start), self._iso(end), _MAX_QUERY_ROWS),
        ).fetchall()
        if len(rows) >= _MAX_QUERY_ROWS:
            raise PrometheusQueryError(f"Prometheus query exceeds the {_MAX_QUERY_ROWS - 1}-row evaluation budget")

        contributions: dict[tuple[tuple[str, str], ...], dict[int, tuple[float, float]]] = {}
        labels_by_key: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}
        valid_by_key: dict[tuple[tuple[str, str], ...], set[int]] = {}
        for row in rows:
            labels = {label: str(row[label]) for label in definition.labels}
            labels["__name__"] = selector.metric
            if not self._matches(labels, selector.matchers):
                continue
            key = tuple(sorted(labels.items()))
            if key not in contributions and len(contributions) >= _MAX_MATRIX_SERIES:
                raise PrometheusQueryError(
                    f"Prometheus result exceeds {_MAX_MATRIX_SERIES} series"
                )
            bucket = max(0, min(points - 1, int(row["bucket"])))
            if definition.aggregate in {"count", "flag"}:
                contribution = float(row["value"] or 0)
                count = 1.0
            elif definition.aggregate == "ratio":
                contribution = float(row["success_value"] or 0)
                count = float(row["value_count"] or 0)
            else:
                count = float(row["value_count"] or 0)
                if count <= 0:
                    continue
                contribution = float(row["sum_value"] or 0)
            current = contributions.setdefault(key, {}).get(bucket, (0.0, 0.0))
            contributions[key][bucket] = (current[0] + contribution, current[1] + count)
            labels_by_key[key] = labels
            valid_by_key.setdefault(key, set()).add(bucket)

        if len(contributions) * points > _MAX_MATRIX_VALUES:
            raise PrometheusQueryError(
                f"Prometheus result exceeds {_MAX_MATRIX_VALUES} matrix values"
            )

        result: list[_Series] = []
        for key, by_bucket in contributions.items():
            values: list[float] = []
            running_value = 0.0
            running_count = 0.0
            for index in range(points):
                value, count = by_bucket.get(index, (0.0, 0.0))
                running_value += value
                running_count += count
                if definition.aggregate == "avg":
                    values.append(running_value / running_count if running_count else math.nan)
                elif definition.aggregate == "ratio":
                    values.append(running_value / running_count if running_count else math.nan)
                else:
                    values.append(running_value)
            if definition.aggregate in {"sum", "avg"} and not valid_by_key.get(key):
                continue
            result.append(_Series(labels_by_key[key], values))
        return result

    @staticmethod
    def _aggregate(series: list[_Series], function: str, by: tuple[str, ...], points: int) -> list[_Series]:
        grouped: dict[tuple[tuple[str, str], ...], list[_Series]] = {}
        labels_by_key: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}
        for item in series:
            labels = {label: item.labels[label] for label in by if label in item.labels}
            key = tuple(sorted(labels.items()))
            grouped.setdefault(key, []).append(item)
            labels_by_key[key] = labels
        result: list[_Series] = []
        for key, members in grouped.items():
            values: list[float] = []
            for index in range(points):
                present = [item.values[index] for item in members if not math.isnan(item.values[index])]
                if function == "sum":
                    values.append(sum(present) if present else math.nan)
                elif function == "avg":
                    values.append(sum(present) / len(present) if present else math.nan)
                elif function == "max":
                    values.append(max(present) if present else math.nan)
                elif function == "count":
                    values.append(float(len(present)))
                else:
                    raise PrometheusQueryError(f"unsupported Prometheus aggregate: {function}")
            result.append(_Series(labels_by_key[key], values))
        return result

    @staticmethod
    def _divide(left: list[_Series], right: list[_Series], points: int) -> list[_Series]:
        right_by_labels = {
            tuple(sorted((label, value) for label, value in item.labels.items() if label != "__name__")): item
            for item in right
        }
        result: list[_Series] = []
        for numerator in left:
            key = tuple(sorted((label, value) for label, value in numerator.labels.items() if label != "__name__"))
            denominator = right_by_labels.get(key)
            if denominator is None:
                continue
            values: list[float] = []
            for index in range(points):
                top = numerator.values[index]
                bottom = denominator.values[index]
                values.append(top / bottom if not math.isnan(top) and not math.isnan(bottom) and bottom != 0 else math.nan)
            result.append(_Series(dict(numerator.labels), values))
        return result

    def _evaluate(self, expression: str, start: float, end: float, step: float, points: int) -> list[_Series]:
        text = _strip_outer_parentheses(expression)
        split = _split_top_level(text, " or ")
        if split:
            left = self._evaluate(split[0], start, end, step, points)
            return left if left else self._evaluate(split[1], start, end, step, points)
        split = _split_top_level(text, " / ")
        if split:
            return self._divide(
                self._evaluate(split[0], start, end, step, points),
                self._evaluate(split[1], start, end, step, points),
                points,
            )
        if text == "vector(0)":
            return [_Series({}, [0.0] * points)]

        if text.startswith("topk("):
            close = _matching_close(text, text.find("("))
            if close != len(text) - 1:
                raise PrometheusQueryError("invalid topk expression")
            parts = _split_top_level(text[5:-1], ",")
            if not parts:
                raise PrometheusQueryError("topk requires a limit and expression")
            try:
                limit = int(parts[0].strip())
            except ValueError as exc:
                raise PrometheusQueryError("topk limit must be an integer") from exc
            if limit < 1 or limit > 500:
                raise PrometheusQueryError("topk limit must be between 1 and 500")
            inner = self._evaluate(parts[1], start, end, step, points)
            ranked = sorted(inner, key=lambda item: (-(item.values[-1] if not math.isnan(item.values[-1]) else -math.inf), tuple(sorted(item.labels.items()))))
            return ranked[:limit]

        for function in ("sum", "avg", "max", "count"):
            prefix = f"{function} by ("
            if text.startswith(prefix):
                label_end = text.find(")", len(prefix))
                if label_end < 0:
                    raise PrometheusQueryError("invalid grouped Prometheus aggregate")
                labels = tuple(label.strip() for label in text[len(prefix):label_end].split(",") if label.strip())
                rest = text[label_end + 1 :].strip()
                if not rest.startswith("(") or _matching_close(rest, 0) != len(rest) - 1:
                    raise PrometheusQueryError("invalid grouped Prometheus aggregate")
                inner = self._evaluate(rest[1:-1], start, end, step, points)
                return self._aggregate(inner, function, labels, points)
            prefix = f"{function}("
            if text.startswith(prefix):
                close = _matching_close(text, len(function))
                if close != len(text) - 1:
                    raise PrometheusQueryError(f"invalid {function} expression")
                inner = self._evaluate(text[len(prefix) : -1], start, end, step, points)
                return self._aggregate(inner, function, (), points)

        return self._base_matrix(_parse_selector(text), start, end, step, points)

    @staticmethod
    def _matrix_result(series: list[_Series], start: float, step: float) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for item in series:
            values = [
                [_format_timestamp(start + index * step), _format_value(value)]
                for index, value in enumerate(item.values)
            ]
            result.append({"metric": dict(item.labels), "values": values})
        return result

    @staticmethod
    def _vector_result(series: list[_Series], at: float) -> list[dict[str, Any]]:
        return [
            {"metric": dict(item.labels), "value": [_format_timestamp(at), _format_value(item.values[-1])]}
            for item in series
        ]

    def query(self, expression: str, params: Mapping[str, list[str]]) -> dict[str, Any]:
        if not isinstance(expression, str) or len(expression) > _MAX_QUERY_LENGTH:
            raise PrometheusQueryError(f"Prometheus query exceeds {_MAX_QUERY_LENGTH} characters")
        expression = expression.strip()
        start, end, step, points = _time_window(params, instant=True)
        scalar = re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", expression.strip())
        if scalar:
            return {
                "status": "success",
                "data": {"resultType": "scalar", "result": [_format_timestamp(end), _format_value(float(scalar.group(0)))]},
            }
        series = self._evaluate(expression, start, end, step, points)
        return {"status": "success", "data": {"resultType": "vector", "result": self._vector_result(series, end)}}

    def query_range(self, expression: str, params: Mapping[str, list[str]]) -> dict[str, Any]:
        if not isinstance(expression, str) or len(expression) > _MAX_QUERY_LENGTH:
            raise PrometheusQueryError(f"Prometheus query exceeds {_MAX_QUERY_LENGTH} characters")
        expression = expression.strip()
        start, _end, step, points = _time_window(params, instant=False)
        end = start + step * (points - 1)
        series = self._evaluate(expression, start, end, step, points)
        return {"status": "success", "data": {"resultType": "matrix", "result": self._matrix_result(series, start, step)}}

    def label_values(self, label: str, metric_names: Iterable[str] | None = None, params: Mapping[str, list[str]] | None = None) -> list[str]:
        if label == "__name__":
            return sorted(metric_names or METRIC_DEFINITIONS)
        if label not in _LABEL_NAMES:
            raise PrometheusQueryError(f"unsupported Prometheus label: {label}")
        params = params or {}
        now = time.time()
        start_values = params.get("start", [])
        end_values = params.get("end", [])
        if len(start_values) > 1 or len(end_values) > 1:
            raise PrometheusQueryError("Prometheus start/end must occur at most once")
        end = _parse_time(end_values[0] if end_values else None, default=now)
        start = _parse_time(
            start_values[0] if start_values else None,
            default=max(end - _MAX_LOOKBACK_SECONDS, 0.0),
        )
        if end < start:
            raise PrometheusQueryError("Prometheus end must be after or equal to start")
        if end - start > _MAX_LOOKBACK_SECONDS:
            raise PrometheusQueryError(
                f"Prometheus label lookup cannot exceed {_MAX_LOOKBACK_SECONDS:.0f} seconds"
            )
        names = set(metric_names or METRIC_DEFINITIONS)
        if len(names) > 64:
            raise PrometheusQueryError("label_values accepts at most 64 metric selectors")
        values: set[str] = set()
        for name in names:
            definition = METRIC_DEFINITIONS.get(name)
            if definition is None or label not in definition.labels:
                continue
            query = (
                f"SELECT DISTINCT {definition.label_sql[label]} AS value FROM {definition.from_sql} "
                f"WHERE {definition.observed_sql} >= ? AND {definition.observed_sql} <= ? AND {definition.where_sql} "
                "ORDER BY value LIMIT 1000"
            )
            rows = self.store.connection.execute(query, (self._iso(start), self._iso(end))).fetchall()
            values.update(str(row["value"]) for row in rows if row["value"] is not None)
        return sorted(values)[:1000]

    def series(self, selectors: Iterable[str], params: Mapping[str, list[str]]) -> list[dict[str, str]]:
        selectors = list(selectors)
        if len(selectors) > 64:
            raise PrometheusQueryError("series accepts at most 64 selectors")
        start_values = params.get("start", [])
        end_values = params.get("end", [])
        if len(start_values) > 1 or len(end_values) > 1:
            raise PrometheusQueryError("Prometheus start/end must occur at most once")
        now = _parse_time(end_values[0] if end_values else None, default=time.time())
        start = _parse_time(
            start_values[0] if start_values else None,
            default=max(now - _MAX_LOOKBACK_SECONDS, 0.0),
        )
        if now < start:
            raise PrometheusQueryError("Prometheus end must be after or equal to start")
        if now - start > _MAX_LOOKBACK_SECONDS:
            raise PrometheusQueryError(
                f"Prometheus series lookup cannot exceed {_MAX_LOOKBACK_SECONDS:.0f} seconds"
            )
        result: list[dict[str, str]] = []
        for expression in selectors:
            selector = _parse_selector(expression)
            for item in self._base_matrix(selector, start, now, max(now - start, 1.0), 1):
                result.append(dict(item.labels))
                if len(result) > _MAX_MATRIX_SERIES:
                    raise PrometheusQueryError(f"Prometheus series result exceeds {_MAX_MATRIX_SERIES} series")
        unique = {tuple(sorted(item.items())): item for item in result}
        return [unique[key] for key in sorted(unique)]

    def metadata(self) -> dict[str, Any]:
        return {
            "status": "success",
            "data": {
                name: [{"type": "gauge", "help": "Event-time Observatory aggregate", "unit": ""}]
                for name in sorted(METRIC_DEFINITIONS)
            },
        }

    def labels(self) -> list[str]:
        return list(_LABEL_NAMES)
