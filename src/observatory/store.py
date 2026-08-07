"""Durable idempotent event storage.

The domain depends on this small repository contract rather than on SQLite. The
initial local profile uses SQLite WAL mode; a PostgreSQL or analytical sink can
implement the same operations without changing normalized events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from .clock import utc_now
from .contracts import ContractError, NormalizedEvent, canonical_json
from .privacy import PrivacyPolicy, redact_event


@dataclass(frozen=True)
class AppendResult:
    status: str
    event_id: str
    conflict_digest: str | None = None


class EventStore:
    """SQLite-backed append-only normalized event store."""

    _FILTER_COLUMNS = {
        "project": "project_id",
        "project_id": "project_id",
        "provider": "provider",
        "model": "model",
        "client": "client",
        "event_type": "event_type",
        "status": "status",
        "evidence_source": "evidence_source",
        "model_family": "model_family",
        "auth_mode": "auth_mode",
        "route": "route",
        "trace_id": "trace_id",
        "span_id": "span_id",
        "branch": "branch",
        "commit": "commit_sha",
        "commit_sha": "commit_sha",
        "worktree": "worktree",
        "session_id": "session_id",
        "workflow_id": "workflow_id",
        "agent_id": "agent_id",
        "subagent_id": "subagent_id",
        "role": "role",
        "skill": "skill",
        "lane": "lane",
        "task_id": "task_id",
        "task_class": "task_class",
        "outcome_kind": "outcome_kind",
        "outcome_status": "outcome_status",
    }

    def __init__(self, path: str | Path, *, privacy_policy: PrivacyPolicy | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.connection = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.initialize()

    def initialize(self) -> None:
        migration_dir = Path(__file__).with_name("migrations")
        for migration_path in sorted(migration_dir.glob("*.sql")):
            version = migration_path.stem
            applied = self.connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?"
                if self._schema_migrations_exists()
                else "SELECT 0",
                (version,) if self._schema_migrations_exists() else (),
            ).fetchone()
            if applied and applied[0]:
                continue
            script = migration_path.read_text(encoding="utf-8")
            applied_at = utc_now().isoformat()
            sql_applied_at = applied_at.replace("'", "''")
            sql_version = version.replace("'", "''")
            try:
                self.connection.executescript(
                    "BEGIN;\n"
                    f"{script}\n"
                    f"INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES ('{sql_version}', '{sql_applied_at}');\n"
                    "COMMIT;\n"
                )
            except Exception:
                self.connection.rollback()
                raise
        self._backfill_projections()

    def _schema_migrations_exists(self) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone()
        return row is not None

    def _backfill_projections(self) -> None:
        """Repair projections for events created before evidence migrations.

        The event envelope remains the source of truth for a legacy row.  The
        backfill is idempotent and records a ledger decision when no prior
        ledger attempt exists, so a restart can resume safely.
        """

        rows = self.connection.execute("SELECT event_id, payload_digest, payload_json FROM events ORDER BY event_id").fetchall()
        if not rows:
            return
        with self.connection:
            for row in rows:
                event_id = str(row["event_id"])
                try:
                    event = NormalizedEvent.from_mapping(json.loads(row["payload_json"]))
                except (json.JSONDecodeError, ContractError) as exc:
                    raise RuntimeError(f"stored event {event_id} is corrupt during projection backfill: {exc}") from exc
                ledger = self.connection.execute(
                    "SELECT 1 FROM ingest_ledger WHERE event_id = ? LIMIT 1", (event_id,)
                ).fetchone()
                if ledger is None:
                    self._insert_ledger(
                        event,
                        str(row["payload_digest"]),
                        "backfill",
                        event.received_at.isoformat(),
                        "projection backfill during schema initialization",
                    )
                self._insert_projections(event)

    def append(self, event: NormalizedEvent) -> AppendResult:
        safe_event = redact_event(event, self.privacy_policy)
        payload_json = safe_event.to_json()
        payload_digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        arrival_at = utc_now().isoformat()
        with self.connection:
            existing = self.connection.execute(
                "SELECT payload_digest FROM events WHERE event_id = ?", (safe_event.event_id,)
            ).fetchone()
            if existing:
                if existing["payload_digest"] == payload_digest:
                    self._insert_ledger(safe_event, payload_digest, "duplicate", arrival_at)
                    return AppendResult("duplicate", safe_event.event_id)
                self.connection.execute(
                    "INSERT INTO event_conflicts(event_id, conflict_digest, conflict_payload_json, detected_at) VALUES (?, ?, ?, ?)",
                    (safe_event.event_id, payload_digest, payload_json, arrival_at),
                )
                self._insert_ledger(safe_event, payload_digest, "conflict", arrival_at, "event_id replay has a different payload")
                return AppendResult("conflict", safe_event.event_id, payload_digest)

            self.connection.execute(
                """
                INSERT INTO events (
                    event_id, schema_version, event_type, observed_at, received_at,
                    project_id, repository, provider, model, client, auth_mode, route,
                    status, usage_source, input_tokens, output_tokens, total_tokens,
                    trace_id, span_id, parent_event_id, session_id, workflow_id,
                    agent_id, subagent_id, evidence_source, payload_digest, payload_json,
                    inserted_at, model_family, reasoning_effort, branch, commit_sha,
                    worktree, role, skill, lane, outcome_kind, outcome_status,
                    task_id, task_class,
                    timeout, tool_failure, aborted,
                    cached_tokens, reasoning_tokens, cost, latency_ms,
                    time_to_first_token_ms, duration_ms, retry_count, rate_limited,
                    cache_creation_tokens, cache_read_tokens, context_size, context_utilization,
                    compaction_count, tool_duration_ms, session_duration_ms, agent_duration_ms,
                    workflow_duration_ms, wall_clock_ms, concurrency, parallel_utilization
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    safe_event.event_id,
                    safe_event.schema_version,
                    safe_event.event_type,
                    safe_event.observed_at.isoformat(),
                    safe_event.received_at.isoformat(),
                    safe_event.project.project_id,
                    safe_event.project.repository,
                    safe_event.llm.provider,
                    safe_event.llm.model,
                    safe_event.llm.client,
                    safe_event.llm.auth_mode,
                    safe_event.llm.route,
                    safe_event.reliability.status,
                    safe_event.usage.source,
                    safe_event.usage.input_tokens,
                    safe_event.usage.output_tokens,
                    safe_event.usage.total_tokens,
                    safe_event.execution.trace_id,
                    safe_event.execution.span_id,
                    safe_event.execution.parent_event_id,
                    safe_event.execution.session_id,
                    safe_event.execution.workflow_id,
                    safe_event.execution.agent_id,
                    safe_event.execution.subagent_id,
                    safe_event.outcome.evidence_source,
                    payload_digest,
                    payload_json,
                    arrival_at,
                    safe_event.llm.model_family,
                    safe_event.llm.reasoning_effort,
                    safe_event.project.branch,
                    safe_event.project.commit,
                    safe_event.project.worktree,
                    safe_event.execution.role,
                    safe_event.execution.skill,
                    safe_event.execution.lane,
                    safe_event.outcome.kind,
                    safe_event.outcome.status,
                    safe_event.execution.task_id,
                    safe_event.execution.task_class,
                    1 if safe_event.reliability.timeout else 0 if safe_event.reliability.timeout is not None else None,
                    1 if safe_event.reliability.tool_failure else 0 if safe_event.reliability.tool_failure is not None else None,
                    1 if safe_event.reliability.aborted else 0 if safe_event.reliability.aborted is not None else None,
                    safe_event.usage.cached_tokens,
                    safe_event.usage.reasoning_tokens,
                    safe_event.usage.cost,
                    safe_event.performance.latency_ms,
                    safe_event.performance.time_to_first_token_ms,
                    safe_event.performance.duration_ms,
                     safe_event.reliability.retry_count,
                     1 if safe_event.reliability.rate_limited else 0 if safe_event.reliability.rate_limited is not None else None,
                     safe_event.usage.cache_creation_tokens,
                     safe_event.usage.cache_read_tokens,
                     safe_event.usage.context_size,
                     safe_event.usage.context_utilization,
                     safe_event.usage.compaction_count,
                     safe_event.performance.tool_duration_ms,
                     safe_event.performance.session_duration_ms,
                     safe_event.performance.agent_duration_ms,
                     safe_event.performance.workflow_duration_ms,
                     safe_event.performance.wall_clock_ms,
                     safe_event.performance.concurrency,
                     safe_event.performance.parallel_utilization,
                 ),
            )
            self._insert_ledger(safe_event, payload_digest, "inserted", arrival_at)
            self._insert_projections(safe_event)
        return AppendResult("inserted", safe_event.event_id)

    def _insert_ledger(
        self,
        event: NormalizedEvent,
        payload_digest: str,
        decision: str,
        received_at: str,
        reason: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO ingest_ledger (
                event_id, observed_at, received_at, source_kind, source_name,
                payload_digest, payload_json, decision, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.observed_at.isoformat(),
                received_at,
                event.source.kind,
                event.source.name,
                payload_digest,
                event.to_json(),
                decision,
                reason,
            ),
        )

    @staticmethod
    def _evidence_quality(source: str) -> str:
        if source in {"observed", "reported", "inferred", "estimated", "derived"}:
            return source
        if source in {"provider", "client", "gateway"}:
            return "reported"
        return "unknown"

    def _insert_projections(self, event: NormalizedEvent) -> None:
        """Materialize bitemporal facts without changing the event envelope."""

        measurements = (
            ("usage.input_tokens", event.usage.input_tokens, "tokens", event.usage.source, "usage"),
            ("usage.output_tokens", event.usage.output_tokens, "tokens", event.usage.source, "usage"),
            ("usage.cached_tokens", event.usage.cached_tokens, "tokens", event.usage.source, "usage"),
            ("usage.cache_creation_tokens", event.usage.cache_creation_tokens, "tokens", event.usage.source, "usage"),
            ("usage.cache_read_tokens", event.usage.cache_read_tokens, "tokens", event.usage.source, "usage"),
            ("usage.reasoning_tokens", event.usage.reasoning_tokens, "tokens", event.usage.source, "usage"),
            ("usage.total_tokens", event.usage.total_tokens, "tokens", event.usage.source, "usage"),
            ("usage.cost", event.usage.cost, "cost", event.usage.source, "usage"),
            ("usage.context_size", event.usage.context_size, "tokens", event.usage.source, "usage"),
            ("usage.context_utilization", event.usage.context_utilization, "ratio", event.usage.source, "usage"),
            ("usage.compaction_count", event.usage.compaction_count, "count", event.usage.source, "usage"),
            (
                "performance.latency_ms",
                event.performance.latency_ms,
                "ms",
                str(event.provenance.fields.get("performance.latency_ms") or event.provenance.fields.get("performance") or "unknown"),
                "performance",
            ),
            (
                "performance.time_to_first_token_ms",
                event.performance.time_to_first_token_ms,
                "ms",
                str(event.provenance.fields.get("performance.time_to_first_token_ms") or event.provenance.fields.get("performance") or "unknown"),
                "performance",
            ),
            (
                "performance.duration_ms",
                event.performance.duration_ms,
                "ms",
                str(event.provenance.fields.get("performance.duration_ms") or event.provenance.fields.get("performance") or "unknown"),
                "performance",
            ),
            (
                "reliability.retry_count",
                event.reliability.retry_count,
                "attempts",
                str(event.provenance.fields.get("reliability.retry_count") or event.provenance.fields.get("reliability") or "unknown"),
                "reliability",
            ),
            ("performance.tool_duration_ms", event.performance.tool_duration_ms, "ms", "unknown", "performance"),
            ("performance.session_duration_ms", event.performance.session_duration_ms, "ms", "unknown", "performance"),
            ("performance.agent_duration_ms", event.performance.agent_duration_ms, "ms", "unknown", "performance"),
            ("performance.workflow_duration_ms", event.performance.workflow_duration_ms, "ms", "unknown", "performance"),
            ("performance.wall_clock_ms", event.performance.wall_clock_ms, "ms", "unknown", "performance"),
            ("performance.concurrency", event.performance.concurrency, "workers", "unknown", "performance"),
            ("performance.parallel_utilization", event.performance.parallel_utilization, "ratio", "unknown", "performance"),
        )
        for field_path, value, unit, source, _section in measurements:
            if value is None:
                continue
            evidence_id = f"evidence:{event.event_id}:{field_path}"
            self.connection.execute(
                """
                INSERT OR IGNORE INTO measurement_facts (
                    event_id, field_path, value_json, unit, evidence_id,
                    evidence_source, evidence_quality, observed_at, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    field_path,
                    canonical_json(value),
                    unit,
                    evidence_id,
                    source,
                    self._evidence_quality(source),
                    event.observed_at.isoformat(),
                    event.received_at.isoformat(),
                ),
            )

        outcome = event.outcome
        if any(value is not None for value in (outcome.kind, outcome.status, outcome.correlation_id, outcome.correlation_basis)):
            self.connection.execute(
                """
                INSERT OR IGNORE INTO outcome_events (
                    event_id, kind, status, correlation_id, correlation_basis, evidence_source,
                    observed_at, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    outcome.kind,
                    outcome.status,
                    outcome.correlation_id,
                    outcome.correlation_basis,
                    outcome.evidence_source or "unknown",
                    event.observed_at.isoformat(),
                    event.received_at.isoformat(),
                ),
            )

        edge_values: list[tuple[str, str | None, str]] = [
            ("project", None, event.project.project_id),
        ]
        execution = event.execution
        if execution.parent_event_id:
            edge_values.append(("parent_event", execution.parent_event_id, execution.parent_event_id))
        for relation, target_id in (
            ("session", execution.session_id),
            ("workflow", execution.workflow_id),
            ("agent", execution.agent_id),
            ("subagent", execution.subagent_id),
        ):
            if target_id:
                edge_values.append((relation, None, target_id))
        correlated_event_ids: list[str] = []
        if execution.task_id:
            if outcome.correlation_id:
                related_rows = self.connection.execute(
                    "SELECT event_id FROM events WHERE task_id = ? AND event_id <> ? ORDER BY observed_at, event_id",
                    (execution.task_id, event.event_id),
                ).fetchall()
            else:
                related_rows = self.connection.execute(
                    """
                    SELECT events.event_id
                    FROM events
                    JOIN outcome_events ON outcome_events.event_id = events.event_id
                    WHERE events.task_id = ?
                      AND events.event_id <> ?
                      AND outcome_events.correlation_id IS NOT NULL
                    ORDER BY events.observed_at, events.event_id
                    """,
                    (execution.task_id, event.event_id),
                ).fetchall()
            for related_row in related_rows:
                related_event_id = str(related_row["event_id"])
                correlated_event_ids.append(related_event_id)
                edge_values.append(("outcome_correlation", related_event_id, related_event_id))
        evidence_source = str(
            event.provenance.fields.get("execution")
            or "unknown"
        )
        for relation, parent_event_id, target_id in edge_values:
            relation_evidence_source = str(
                event.provenance.fields.get(f"execution.{relation}")
                or event.provenance.fields.get("execution")
                or "unknown"
            )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO attribution_edges (
                    child_event_id, parent_event_id, relation, target_id,
                    evidence_source, observed_at, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    parent_event_id,
                    relation,
                    target_id,
                    relation_evidence_source,
                    event.observed_at.isoformat(),
                    event.received_at.isoformat(),
                ),
            )
        for related_event_id in correlated_event_ids:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO attribution_edges (
                    child_event_id, parent_event_id, relation, target_id,
                    evidence_source, observed_at, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    related_event_id,
                    event.event_id,
                    "outcome_correlation",
                    event.event_id,
                    evidence_source,
                    event.observed_at.isoformat(),
                    event.received_at.isoformat(),
                ),
            )

    def get(self, event_id: str) -> NormalizedEvent | None:
        row = self.connection.execute("SELECT payload_json FROM events WHERE event_id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        try:
            return NormalizedEvent.from_mapping(json.loads(row["payload_json"]))
        except (json.JSONDecodeError, ContractError) as exc:
            raise RuntimeError(f"stored event {event_id} is corrupt: {exc}") from exc

    def list_events(self, filters: Mapping[str, str] | None = None, *, limit: int = 100) -> list[NormalizedEvent]:
        rows = self._select_rows(filters or {}, limit=limit)
        result: list[NormalizedEvent] = []
        for row in rows:
            try:
                result.append(NormalizedEvent.from_mapping(json.loads(row["payload_json"])))
            except (json.JSONDecodeError, ContractError) as exc:
                raise RuntimeError(f"stored event {row['event_id']} is corrupt: {exc}") from exc
        return result

    def summary(self, filters: Mapping[str, str] | None = None) -> dict[str, Any]:
        where, params = self._where_clause(filters or {})
        row = self.connection.execute(
            f"""
            SELECT
                COUNT(*) AS events,
                SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                COALESCE(SUM(input_tokens), 0) AS input_tokens,
                COALESCE(SUM(output_tokens), 0) AS output_tokens,
                COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
                COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
                COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                COALESCE(SUM(compaction_count), 0) AS compactions,
                COALESCE(SUM(cost), 0) AS cost,
                COALESCE(AVG(latency_ms), 0) AS average_latency_ms,
                COALESCE(AVG(time_to_first_token_ms), 0) AS average_time_to_first_token_ms,
                COALESCE(AVG(duration_ms), 0) AS average_duration_ms,
                COALESCE(AVG(context_size), 0) AS average_context_size,
                COALESCE(AVG(context_utilization), 0) AS average_context_utilization,
                COALESCE(AVG(concurrency), 0) AS average_concurrency,
                COALESCE(AVG(parallel_utilization), 0) AS average_parallel_utilization,
                COALESCE(SUM(retry_count), 0) AS retries,
                COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures,
                COALESCE(SUM(CASE WHEN aborted = 1 THEN 1 ELSE 0 END), 0) AS aborted,
                COUNT(DISTINCT project_id) AS projects,
                COUNT(DISTINCT provider || ':' || model) AS models
            FROM events {where}
            """,
            params,
        ).fetchone()
        provenance_rows = self.connection.execute(
            f"SELECT usage_source, COUNT(*) AS count FROM events {where} GROUP BY usage_source ORDER BY usage_source",
            params,
        ).fetchall()
        outcome_rows = self.connection.execute(
            f"""
            SELECT outcome_events.kind, outcome_events.status, COUNT(*) AS count
            FROM outcome_events
            WHERE outcome_events.event_id IN (SELECT event_id FROM events {where})
            GROUP BY outcome_events.kind, outcome_events.status
            ORDER BY outcome_events.kind, outcome_events.status
            """,
            params,
        ).fetchall()
        return {
            "events": int(row["events"] or 0),
            "successes": int(row["successes"] or 0),
            "failures": int(row["failures"] or 0),
            "input_tokens": row["input_tokens"] or 0,
            "output_tokens": row["output_tokens"] or 0,
            "cached_tokens": row["cached_tokens"] or 0,
            "cache_creation_tokens": row["cache_creation_tokens"] or 0,
            "cache_read_tokens": row["cache_read_tokens"] or 0,
            "reasoning_tokens": row["reasoning_tokens"] or 0,
            "compactions": row["compactions"] or 0,
            "cost": row["cost"] or 0,
            "average_latency_ms": row["average_latency_ms"] or 0,
            "average_time_to_first_token_ms": row["average_time_to_first_token_ms"] or 0,
            "average_duration_ms": row["average_duration_ms"] or 0,
            "average_context_size": row["average_context_size"] or 0,
            "average_context_utilization": row["average_context_utilization"] or 0,
            "average_concurrency": row["average_concurrency"] or 0,
            "average_parallel_utilization": row["average_parallel_utilization"] or 0,
            "retries": row["retries"] or 0,
            "rate_limited": int(row["rate_limited"] or 0),
            "timeouts": int(row["timeouts"] or 0),
            "tool_failures": int(row["tool_failures"] or 0),
            "aborted": int(row["aborted"] or 0),
            "projects": int(row["projects"] or 0),
            "models": int(row["models"] or 0),
            "usage_sources": {item["usage_source"]: item["count"] for item in provenance_rows},
            "outcomes": [dict(item) for item in outcome_rows],
        }

    def conflict_count(self, event_id: str | None = None) -> int:
        if event_id is None:
            row = self.connection.execute("SELECT COUNT(*) AS count FROM event_conflicts").fetchone()
        else:
            row = self.connection.execute("SELECT COUNT(*) AS count FROM event_conflicts WHERE event_id = ?", (event_id,)).fetchone()
        return int(row["count"] or 0)

    def ledger_entries(self, *, event_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        limit = self._bounded_limit(limit, maximum=1000)
        if event_id is None:
            rows = self.connection.execute(
                """
                SELECT ledger_id, event_id, observed_at, received_at, source_kind,
                       source_name, payload_digest, decision, reason
                FROM ingest_ledger ORDER BY ledger_id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT ledger_id, event_id, observed_at, received_at, source_kind,
                       source_name, payload_digest, decision, reason
                FROM ingest_ledger WHERE event_id = ? ORDER BY ledger_id DESC LIMIT ?
                """,
                (event_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def ledger_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) AS count FROM ingest_ledger").fetchone()
        return int(row["count"] or 0)

    def measurement_facts(self, *, event_id: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        limit = self._bounded_limit(limit, maximum=5000)
        if event_id is None:
            rows = self.connection.execute(
                "SELECT * FROM measurement_facts ORDER BY observed_at, fact_id LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM measurement_facts WHERE event_id = ? ORDER BY fact_id LIMIT ?",
                (event_id, limit),
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            try:
                value["value"] = json.loads(value.pop("value_json"))
            except (TypeError, json.JSONDecodeError):
                value["value"] = None
            result.append(value)
        return result

    def measurement_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) AS count FROM measurement_facts").fetchone()
        return int(row["count"] or 0)

    def outcomes(self, *, event_id: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        limit = self._bounded_limit(limit, maximum=5000)
        if event_id is None:
            rows = self.connection.execute(
                "SELECT * FROM outcome_events ORDER BY observed_at, outcome_id LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM outcome_events WHERE event_id = ? ORDER BY outcome_id LIMIT ?",
                (event_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def outcome_count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) AS count FROM outcome_events").fetchone()
        return int(row["count"] or 0)

    def attribution_edges(self, *, event_id: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        limit = self._bounded_limit(limit, maximum=5000)
        if event_id is None:
            rows = self.connection.execute(
                "SELECT * FROM attribution_edges ORDER BY observed_at, edge_id LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT * FROM attribution_edges
                WHERE child_event_id = ? OR parent_event_id = ?
                ORDER BY observed_at, edge_id LIMIT ?
                """,
                (event_id, event_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def event_detail(self, event_id: str) -> dict[str, Any] | None:
        event = self.get(event_id)
        if event is None:
            return None
        return {
            "event": event.to_mapping(),
            "ledger": self.ledger_entries(event_id=event_id),
            "measurements": self.measurement_facts(event_id=event_id),
            "outcomes": self.outcomes(event_id=event_id),
            "attribution": self.attribution_edges(event_id=event_id),
        }

    @staticmethod
    def _bounded_limit(limit: int, *, maximum: int) -> int:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > maximum:
            raise ValueError(f"limit must be an integer between 1 and {maximum}")
        return limit

    def metric_dimensions(self, *, limit: int = 100) -> dict[str, list[dict[str, Any]]]:
        """Return bounded aggregate dimensions safe for Prometheus labels."""

        if limit < 1 or limit > 500:
            raise ValueError("metric dimension limit must be between 1 and 500")
        provider_model = self.connection.execute(
            """
            SELECT provider, model, model_family, client, auth_mode, route, task_class, COUNT(*) AS count,
                   SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                   SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                   COALESCE(SUM(total_tokens), 0) AS total_tokens,
                   COALESCE(SUM(cost), 0) AS cost,
                   COALESCE(AVG(latency_ms), 0) AS average_latency_ms,
                   COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures
            FROM events
            GROUP BY provider, model, model_family, client, auth_mode, route, task_class
            ORDER BY count DESC, provider, model, model_family, client, auth_mode, route, task_class
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        usage_source = self.connection.execute(
            "SELECT usage_source AS source, COUNT(*) AS count FROM events GROUP BY usage_source ORDER BY count DESC, source LIMIT ?",
            (limit,),
        ).fetchall()
        project = self.connection.execute(
            "SELECT project_id AS project, COUNT(*) AS count FROM events GROUP BY project_id ORDER BY count DESC, project_id LIMIT ?",
            (limit,),
        ).fetchall()
        client_route = self.connection.execute(
            """
            SELECT project_id AS project, client, route, auth_mode, COUNT(*) AS count
            FROM events
            GROUP BY project_id, client, route, auth_mode
            ORDER BY count DESC, project, client, route, auth_mode
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        execution = self.connection.execute(
            """
            SELECT COALESCE(role, 'unknown') AS role,
                   COALESCE(skill, 'unknown') AS skill,
                   COALESCE(lane, 'unknown') AS lane,
                   COUNT(*) AS count
            FROM events
            GROUP BY role, skill, lane
            ORDER BY count DESC, role, skill, lane
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        workflow = self.connection.execute(
            """
            SELECT COALESCE(workflow_id, 'unknown') AS workflow, COUNT(*) AS count
            FROM events
            GROUP BY workflow_id
            ORDER BY count DESC, workflow
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        agent = self.connection.execute(
            """
            SELECT COALESCE(agent_id, 'unknown') AS agent,
                   COALESCE(subagent_id, 'unknown') AS subagent,
                   COUNT(*) AS count
            FROM events
            GROUP BY agent_id, subagent_id
            ORDER BY count DESC, agent, subagent
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        context = self.connection.execute(
            """
            SELECT project_id AS project,
                   COALESCE(repository, 'unknown') AS repository,
                   COALESCE(branch, 'unknown') AS branch,
                   provider,
                   model,
                   COALESCE(model_family, 'unknown') AS model_family,
                   client,
                   auth_mode,
                   route,
                   COALESCE(agent_id, 'unknown') AS agent,
                   COALESCE(subagent_id, 'unknown') AS subagent,
                   COALESCE(role, 'unknown') AS role,
                   COALESCE(skill, 'unknown') AS skill,
                   COALESCE(lane, 'unknown') AS lane,
                   COALESCE(workflow_id, 'unknown') AS workflow,
                   COALESCE(task_class, 'unknown') AS task_class,
                   COALESCE(status, 'unknown') AS status,
                    COUNT(*) AS count,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                    COALESCE(SUM(compaction_count), 0) AS compactions,
                    COALESCE(SUM(cost), 0) AS cost,
                    COALESCE(AVG(latency_ms), 0) AS average_latency_ms,
                    COALESCE(AVG(time_to_first_token_ms), 0) AS average_time_to_first_token_ms,
                    COALESCE(AVG(duration_ms), 0) AS average_duration_ms,
                    COALESCE(AVG(context_size), 0) AS average_context_size,
                    COALESCE(AVG(context_utilization), 0) AS average_context_utilization,
                    COALESCE(AVG(concurrency), 0) AS average_concurrency,
                    COALESCE(AVG(parallel_utilization), 0) AS average_parallel_utilization,
                    COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures
            FROM events
            GROUP BY project_id, repository, branch, provider, model, model_family,
                     client, auth_mode, route, agent_id, subagent_id, role, skill,
                     lane, workflow_id, task_class, status
            ORDER BY count DESC, project, repository, branch, provider, model, client
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        outcomes = self.connection.execute(
            """
            SELECT COALESCE(kind, 'unknown') AS kind,
                   COALESCE(status, 'unknown') AS status,
                   COUNT(*) AS count
            FROM outcome_events
            GROUP BY kind, status
            ORDER BY count DESC, kind, status
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return {
            "provider_model": [dict(row) for row in provider_model],
            "usage_source": [dict(row) for row in usage_source],
            "project": [dict(row) for row in project],
            "client_route": [dict(row) for row in client_route],
            "execution": [dict(row) for row in execution],
            "workflow": [dict(row) for row in workflow],
            "agent": [dict(row) for row in agent],
            "context": [dict(row) for row in context],
            "outcome": [dict(row) for row in outcomes],
        }

    def comparison(self, filters: Mapping[str, str] | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return bounded provider/model/client analytics for like-for-like comparison."""

        limit = self._bounded_limit(limit, maximum=500)
        where, params = self._where_clause(filters or {})
        rows = self.connection.execute(
            f"""
            SELECT provider, model, model_family, client, auth_mode, route,
                   COUNT(*) AS events,
                   SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                   SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
                   COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                   COALESCE(SUM(total_tokens), 0) AS total_tokens,
                   COALESCE(SUM(cost), 0) AS cost,
                   COALESCE(AVG(latency_ms), 0) AS average_latency_ms,
                   COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures
            FROM events {where}
            GROUP BY provider, model, model_family, client, auth_mode, route
            ORDER BY events DESC, provider, model, client
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def _select_rows(self, filters: Mapping[str, str], *, limit: int) -> list[sqlite3.Row]:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > 1000:
            raise ValueError("limit must be an integer between 1 and 1000")
        where, params = self._where_clause(filters)
        return self.connection.execute(
            f"SELECT event_id, payload_json FROM events {where} ORDER BY observed_at, event_id LIMIT ?",
            (*params, limit),
        ).fetchall()

    def _where_clause(self, filters: Mapping[str, str]) -> tuple[str, list[str]]:
        clauses: list[str] = []
        params: list[str] = []
        for key, value in filters.items():
            if key in ("start", "end"):
                operator = ">=" if key == "start" else "<="
                clauses.append(f"observed_at {operator} ?")
                params.append(value)
                continue
            column = self._FILTER_COLUMNS.get(key)
            if column is None:
                raise ValueError(f"unsupported filter: {key}")
            clauses.append(f"{column} = ?")
            params.append(value)
        return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
