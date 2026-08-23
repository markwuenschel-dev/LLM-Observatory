"""Durable idempotent event storage.

The domain depends on this small repository contract rather than on SQLite. The
initial local profile uses SQLite WAL mode; a PostgreSQL or analytical sink can
implement the same operations without changing normalized events.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta, timezone
import hashlib
import itertools
import json
from pathlib import Path
import shutil
import sqlite3
from typing import Any, Mapping, Sequence

from .clock import utc_now
from .contracts import ContractError, NormalizedEvent, canonical_json, ensure_utc
from .privacy import PrivacyPolicy, redact_event


DEFAULT_MAX_DATABASE_BYTES = 2 * 1024 * 1024 * 1024
# Truncate the WAL sidecar back to this size after a checkpoint.
WAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024
# Checkpoint roughly every 16 MiB of 4 KiB pages rather than SQLite's 1000.
WAL_AUTOCHECKPOINT_PAGES = 4000
# Intake fails fast so telemetry never blocks a client.
# Never let telemetry storage consume the last of the volume. A configured
# byte budget is a policy; this is the floor that keeps the host usable when
# the policy was set larger than the disk actually is.
MIN_FREE_DISK_BYTES = 2 * 1024 ** 3
DEFAULT_BUSY_TIMEOUT_MS = 250
# Maintenance writers wait instead of dropping what they collected.
MAINTENANCE_BUSY_TIMEOUT_MS = 10_000
# Statistics refresh on close is best-effort and must not delay shutdown.
CLOSE_OPTIMIZE_TIMEOUT_MS = 250
# Contract default for an event whose project could not be resolved.
UNKNOWN_PROJECT_ID = "project:unknown"
# Below this many associated outcomes a success rate is noise, not a finding.
MIN_RANKING_OUTCOMES = 5
# The only identifiers that may be interpolated into the ranking SQL.
# The dimensions the primary question names: "which models, agents, skills,
# workflows, and orchestration strategies deliver the most reliable engineering
# value". `workflow_id` and `task_class` were absent entirely, so workflows
# could not rank however much telemetry arrived -- and, worse, did not even
# appear in `dimension_coverage`, making the gap invisible rather than reported.
RANKING_DIMENSIONS = (
    "provider", "model", "client", "agent_id", "skill", "workflow_id", "task_class",
    # Orchestration: how the work was arranged, not what ran it. `parent_agent_id`
    # is the delegation edge (which agent spawned this work), `lane` the parallel
    # lane it ran in, `role` the part it played. Without these the surface could
    # describe every model and skill and still say nothing about whether fanning
    # work out was worth it.
    "parent_agent_id", "lane", "role",
)
# Dimensions no client stamps on the operation itself. A skill is invoked once
# and then governs the work that follows it, so it arrives on a separate hook
# event -- which is itself an outcome, and the ranking's effort side excludes
# outcomes. Left as a plain column these can never rank, however much telemetry
# accumulates.
#
# Resolved instead by the window the capability was active for: the most recent
# invocation in the SAME session at or before the operation. That is a bounded
# temporal association inside a shared session, which is the evidence standard
# this store already applies elsewhere -- not a session-wide join, which would
# charge a skill invoked once for an entire session's cost.
# A workflow, like a skill, is invoked once and governs what follows, so it
# arrives on the invocation event rather than on the operations it covers.
# `task_class` is a property of the operation itself and needs no window.
_WINDOWED_DIMENSIONS = frozenset({"skill", "workflow_id"})


def _coverage_predicate(name: str) -> str:
    """Is this dimension resolvable for an event? Must match the ranking's view.

    Reporting raw-column presence while the ranking resolves over a window would
    have the report say a dimension is unrankable at the same moment it ranks it.
    """

    if name not in _WINDOWED_DIMENSIONS:
        return f"e.{name} IS NOT NULL"
    return (
        f"(e.{name} IS NOT NULL OR EXISTS ("
        f"  SELECT 1 FROM events AS w"
        f"  WHERE w.session_id = e.session_id AND w.session_id IS NOT NULL"
        f"    AND w.{name} IS NOT NULL AND w.observed_at <= e.observed_at))"
    )


def _ranking_group_expression(name: str) -> str:
    """The bare expression for GROUP BY, without the output alias."""

    rendered = _ranking_expression(name)
    return rendered[: rendered.rindex(f" AS {name}")]


def _ranking_expression(name: str) -> str:
    """SQL for one ranking dimension, resolved over its active window if needed."""

    if name not in _WINDOWED_DIMENSIONS:
        return f"COALESCE(e.{name}, 'unknown') AS {name}"
    return (
        f"COALESCE(e.{name}, ("
        f"  SELECT w.{name} FROM events AS w"
        f"  WHERE w.session_id = e.session_id AND w.session_id IS NOT NULL"
        f"    AND w.{name} IS NOT NULL AND w.observed_at <= e.observed_at"
        f"  ORDER BY w.observed_at DESC, w.event_id DESC LIMIT 1"
        f"), 'unknown') AS {name}"
    )
# Basis for an outcome that carries no identifier of its own -- a commit,
# for example -- associated with work in the same project shortly before it.
PROJECT_WINDOW_BASIS = "project_window"
DEFAULT_PROJECT_WINDOW_SECONDS = 4 * 3600
MAX_PROJECT_WINDOW_SECONDS = 24 * 3600
# A temporal link is loose, so cap how much one outcome may sweep in.
PROJECT_WINDOW_MAX_LINKS = 500
# Drift needs a real sample behind each ratio, and a bounded scan.
DRIFT_MIN_SAMPLE_EVENTS = 30
DRIFT_SNAPSHOT_LIMIT = 5000
# The explicit-basis join runs on the append path. Client hooks now declare a
# session basis, so a long session would link every hook to every event in it
# -- quadratic edge growth inside the append transaction, against a 250 ms
# busy timeout. Bound it the same way the temporal branch already is.
CORRELATION_MAX_LINKS = 500
# Keep the writer lock short so intake is never starved by maintenance.
RECORRELATE_BATCH_SIZE = 20


def _project_window_seconds(attributes: Any) -> int:
    """Read the declared association window, bounded to a defensible range."""

    raw = None
    if isinstance(attributes, Mapping):
        raw = attributes.get("correlation_window_seconds")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PROJECT_WINDOW_SECONDS
    if value <= 0:
        return DEFAULT_PROJECT_WINDOW_SECONDS
    return min(value, MAX_PROJECT_WINDOW_SECONDS)


def _elapsed_seconds(start: Any, end: Any) -> float | None:
    try:
        return (ensure_utc(str(end), "end") - ensure_utc(str(start), "start")).total_seconds()
    except Exception:
        return None


def _coverage_ratio(reported: Any, observed: int) -> dict[str, Any]:
    """Report a field's completeness as a fraction, never as an assumed whole."""

    reported = int(reported or 0)
    return {
        "reported_events": reported,
        "observed_events": observed,
        "ratio": (reported / observed) if observed else None,
    }


class StorageCapacityError(RuntimeError):
    """Raised when accepting another event would exceed the store budget."""


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
        "repository": "repository",
        "provider": "provider",
        "model": "model",
        "model_variant": "model_variant",
        "client": "client",
        "event_type": "event_type",
        "status": "status",
        "evidence_source": "evidence_source",
        "usage_source": "usage_source",
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
        "parent_agent_id": "parent_agent_id",
        "parent_agent": "parent_agent_id",
        "role": "role",
        "skill": "skill",
        "lane": "lane",
        "task_id": "task_id",
        "task_class": "task_class",
        "outcome_kind": "outcome_kind",
        "outcome_status": "outcome_status",
    }

    def __init__(
        self,
        path: str | Path,
        *,
        privacy_policy: PrivacyPolicy | None = None,
        max_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
        read_only: bool = False,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        if max_bytes is not None and (isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1):
            raise ValueError("max_bytes must be a positive integer or None")
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.read_only = read_only
        self.closed = False
        if not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        if read_only:
            database_uri = f"{self.path.resolve().as_uri()}?mode=ro"
            self.connection = sqlite3.connect(database_uri, uri=True, timeout=0.25, check_same_thread=False)
        else:
            self.connection = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        # The PRAGMA overrides the connect timeout, so a single hard-coded value
        # applied to every writer, not just intake. Intake wants to fail fast so
        # it never blocks a client; a maintenance writer wants to wait rather
        # than silently drop what it collected.
        self.connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        if read_only:
            self.connection.execute("PRAGMA query_only = ON")
        else:
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            # Without a journal size limit SQLite never truncates the WAL file
            # back to disk after a checkpoint, so it grows without bound. The
            # WAL counts toward `storage_bytes()`, so an untruncated sidecar
            # silently consumes the store budget and stops intake even though
            # the real data volume is far smaller. Passive autocheckpoints also
            # cannot reset the WAL while the dashboard read plane holds
            # readers, which makes the unbounded case the normal one here.
            self.connection.execute(f"PRAGMA journal_size_limit = {WAL_SIZE_LIMIT_BYTES}")
            self.connection.execute(f"PRAGMA wal_autocheckpoint = {WAL_AUTOCHECKPOINT_PAGES}")
            self.initialize()

    def storage_bytes(self) -> int:
        """Return the SQLite database plus WAL sidecar size."""

        total = 0
        for candidate in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")):
            try:
                total += candidate.stat().st_size
            except OSError:
                pass
        return total

    def capacity(self) -> dict[str, Any]:
        """Report headroom against the smaller of the byte budget and the disk.

        A configured budget is a policy, not a guarantee the space exists. On
        this host the budget was 17.18 GB while the volume had 17.0 GB free, so
        honouring the budget literally would have filled the system disk and
        taken the machine down with it -- telemetry storage is never worth that.
        `doctor` does compare free space against `DEFAULT_MIN_FREE_BYTES`
        (`cli.py:915-918`), but that check is `"blocking": False` and runs only
        when an operator invokes it; nothing on the append path consulted the
        disk at all.

        Clamping here rather than at the call sites means the intake gate, the
        observation verdict, and `/healthz` all become honest at once, and the
        store degrades -- rejecting new telemetry and saying so -- instead of
        consuming the last free byte on the volume.
        """

        current = self.storage_bytes()
        maximum = self.max_bytes
        disk_limited = False
        try:
            free = shutil.disk_usage(self.path.parent).free
        except OSError:
            free = None
        if free is not None:
            # The most this store may reach without breaching the floor.
            disk_ceiling = max(current + free - MIN_FREE_DISK_BYTES, 0)
            if maximum is None or disk_ceiling < maximum:
                maximum = disk_ceiling
                disk_limited = True
        ratio = None if maximum is None else current / maximum if maximum else 1.0
        return {
            "bytes": current,
            "max_bytes": maximum,
            "configured_max_bytes": self.max_bytes,
            "disk_limited": disk_limited,
            "disk_free_bytes": free,
            "ratio": ratio,
            "exhausted": maximum is not None and current >= maximum,
        }

    def record_observation(self, report: Mapping[str, Any], *, snapshot_id: str | None = None) -> str:
        """Append one observation verdict to the longitudinal record.

        Kept separate from the events ledger: this is the deployment observing
        itself, not telemetry about an LLM. Snapshots are append-only so a
        recorded degradation can never be tidied away after the fact.
        """

        recorded_at = str(report.get("generated_at") or utc_now().isoformat())
        evidence = report.get("evidence") or {}
        snapshot_row = (
            recorded_at,
            str(report.get("verdict") or "UNKNOWN"),
            1 if report.get("observation_capable") else 0,
            int(evidence.get("clients_observing") or 0),
            int(evidence.get("clients_degraded") or 0),
            int(evidence.get("events_in_window") or 0),
            evidence.get("newest_event_age_seconds"),
            evidence.get("store_capacity_ratio"),
            json.dumps(list(report.get("blockers") or []), sort_keys=True),
        )
        client_rows = []
        for client in report.get("clients") or []:
            attribution = client.get("project_attribution") or {}
            client_rows.append((
                recorded_at,
                str(client.get("client")),
                str(client.get("state")),
                int(client.get("events_observed") or 0),
                client.get("last_event_at"),
                client.get("last_event_age_seconds"),
                attribution.get("ratio"),
                json.dumps(list(client.get("blockers") or []), sort_keys=True),
                json.dumps(
                    {
                        family: graded.get("ratio")
                        for family, graded in (client.get("signal_families") or {}).items()
                        if isinstance(graded, Mapping)
                    },
                    sort_keys=True,
                ),
            ))

        derived_id = snapshot_id is None
        if derived_id:
            # The id must cover everything the row records, not a subset of it.
            # An earlier version hashed only timestamp/verdict/blockers and then
            # "verified" the write by re-reading those same three fields -- a
            # tautology that could not fail on any collision it could produce,
            # so two observations differing only in evidence (all clients gone
            # degraded, say) silently kept the first. Hash the exact tuples that
            # get written, so identical content means an idempotent replay and
            # different content means a different id.
            digest = hashlib.sha256(
                json.dumps([snapshot_row, client_rows], sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:24]
            snapshot_id = f"obs:{digest}"

        # The check belongs INSIDE the transaction and BEFORE any write. Raising
        # after the commit left client rows from a rejected observation attached
        # to the accepted snapshot, in a table whose triggers make that
        # unrepairable through the application.
        with self.connection:
            stored = self.connection.execute(
                "SELECT recorded_at, verdict, observation_capable, clients_observing,"
                " clients_degraded, events_in_window, newest_event_age_seconds,"
                " store_capacity_ratio, blockers_json FROM observation_snapshots"
                " WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if stored is not None:
                if tuple(stored) != snapshot_row:
                    raise RuntimeError(
                        f"observation snapshot {snapshot_id} already holds a different observation "
                        f"({stored['verdict']}); the new verdict was not recorded"
                    )
                # Same id, same content: a replay. Already durable, nothing to do.
                return snapshot_id
            self.connection.execute(
                "INSERT INTO observation_snapshots(snapshot_id, recorded_at, verdict,"
                " observation_capable, clients_observing, clients_degraded, events_in_window,"
                " newest_event_age_seconds, store_capacity_ratio, blockers_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (snapshot_id, *snapshot_row),
            )
            self.connection.executemany(
                "INSERT INTO observation_client_snapshots(snapshot_id, recorded_at,"
                " client, state, events_observed, last_event_at, last_event_age_seconds,"
                " attribution_ratio, blockers_json, signal_families_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(snapshot_id, *row) for row in client_rows],
            )
        return snapshot_id

    def coverage_drift(
        self,
        *,
        since: str | None = None,
        baseline_snapshots: int = 5,
        minimum_drop: float = 0.25,
    ) -> list[dict[str, Any]]:
        """Report signal families whose coverage fell against their own history.

        Provider schema drift is quiet by nature: after a client upgrade a field
        simply stops arriving, nothing errors, and the aggregates keep returning
        numbers that are merely smaller. Comparing a family against its own
        recent baseline turns that into a visible change. The baseline is the
        median of prior snapshots so one bad sample cannot raise or hide a
        finding, and both values are reported so the operator judges the size of
        the drop rather than trusting a flag.
        """

        clauses, params = [], []
        if since:
            clauses.append("recorded_at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.connection.execute(
            f"SELECT client, recorded_at, events_observed, signal_families_json"
            f" FROM observation_client_snapshots {where}"
            f" ORDER BY client, recorded_at DESC LIMIT ?",
            (*params, DRIFT_SNAPSHOT_LIMIT),
        ).fetchall()

        history: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for row in rows:
            raw = row["signal_families_json"]
            if not raw:
                continue
            # A ratio over one or two events is noise: a quiet minute grades
            # COMPLETE at 1-of-1, and the next busy sample then looks like a
            # collapse. Drift is only meaningful against a real sample.
            if int(row["events_observed"] or 0) < DRIFT_MIN_SAMPLE_EVENTS:
                continue
            try:
                families = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(families, dict):
                history.setdefault(str(row["client"]), []).append((str(row["recorded_at"]), families))

        findings: list[dict[str, Any]] = []
        for client, snapshots in history.items():
            if len(snapshots) < 2:
                continue
            latest_at, latest = snapshots[0]
            baseline_rows = snapshots[1 : 1 + max(1, baseline_snapshots)]
            for family, current in latest.items():
                if not isinstance(current, (int, float)):
                    continue
                prior = [
                    value
                    for _, families in baseline_rows
                    for key, value in families.items()
                    if key == family and isinstance(value, (int, float))
                ]
                if not prior:
                    continue
                prior.sort()
                middle = len(prior) // 2
                baseline = prior[middle] if len(prior) % 2 else (prior[middle - 1] + prior[middle]) / 2
                if baseline - current >= minimum_drop:
                    findings.append(
                        {
                            "client": client,
                            "signal_family": family,
                            "baseline_ratio": baseline,
                            "current_ratio": current,
                            "drop": baseline - current,
                            "baseline_snapshots": len(prior),
                            "observed_at": latest_at,
                            "note": "coverage fell against this client's own recent history; "
                            "check for a client upgrade or a changed telemetry schema",
                        }
                    )
        findings.sort(key=lambda f: -f["drop"])
        return findings

    def observation_history(self, *, since: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        """Return recorded verdicts, newest first."""

        limit = self._bounded_limit(limit, maximum=2000)
        clauses = []
        params: list[Any] = []
        if since:
            clauses.append("recorded_at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.connection.execute(
            f"SELECT * FROM observation_snapshots {where} ORDER BY recorded_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        history = []
        for row in rows:
            record = {key: row[key] for key in row.keys()}
            record["blockers"] = json.loads(record.pop("blockers_json") or "[]")
            record["observation_capable"] = bool(record["observation_capable"])
            history.append(record)
        return history

    def observation_gaps(
        self,
        *,
        since: str | None = None,
        expected_interval_seconds: float = 3600.0,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Find intervals where the deployment was not observing, or not watched.

        Two different failures are separated deliberately. A *recorded* gap is a
        run of snapshots whose verdict was not capable -- the plane was known to
        be dark. An *unwatched* gap is a stretch longer than the expected
        recording interval with no snapshot at all, which means nothing was
        checking; the thirteen-day blackout was that second kind, and reporting
        it as "no problems recorded" would repeat the original mistake.
        """

        history = list(reversed(self.observation_history(since=since, limit=2000)))
        recorded_gaps: list[dict[str, Any]] = []
        unwatched_gaps: list[dict[str, Any]] = []
        open_gap: dict[str, Any] | None = None
        previous: dict[str, Any] | None = None
        for snapshot in history:
            if previous is not None:
                delta = _elapsed_seconds(previous["recorded_at"], snapshot["recorded_at"])
                if delta is not None and delta > expected_interval_seconds:
                    unwatched_gaps.append(
                        {"from": previous["recorded_at"], "to": snapshot["recorded_at"], "seconds": delta}
                    )
            if not snapshot["observation_capable"]:
                if open_gap is None:
                    open_gap = {"from": snapshot["recorded_at"], "to": snapshot["recorded_at"], "snapshots": 0, "blockers": []}
                open_gap["to"] = snapshot["recorded_at"]
                open_gap["snapshots"] += 1
                for blocker in snapshot["blockers"]:
                    if blocker not in open_gap["blockers"]:
                        open_gap["blockers"].append(blocker)
            elif open_gap is not None:
                open_gap["seconds"] = _elapsed_seconds(open_gap["from"], open_gap["to"])
                recorded_gaps.append(open_gap)
                open_gap = None
            previous = snapshot
        if open_gap is not None:
            open_gap["seconds"] = _elapsed_seconds(open_gap["from"], open_gap["to"])
            open_gap["ongoing"] = True
            recorded_gaps.append(open_gap)
        # An unwatched gap was only ever detected *between* two snapshots, so a
        # recorder that dies leaves no later snapshot and the silence reads as a
        # clean bill of health -- precisely the thirteen-day blackout this table
        # exists to catch. Compare the newest snapshot against now as well.
        current = now or utc_now().isoformat()
        newest_recorded = previous["recorded_at"] if previous is not None else None
        if newest_recorded is None:
            # Both live callers pass a 7-day window, so a recorder dead longer
            # than that returned an empty history and a clean bill of health --
            # the precise blackout this detection exists for. Look past the
            # window for the newest snapshot before concluding there is nothing.
            row = self.connection.execute(
                "SELECT recorded_at FROM observation_snapshots ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            newest_recorded = str(row["recorded_at"]) if row is not None else None
        if newest_recorded is not None:
            trailing = _elapsed_seconds(newest_recorded, current)
            if trailing is not None and trailing > expected_interval_seconds:
                unwatched_gaps.append(
                    {
                        "from": newest_recorded,
                        "to": current,
                        "seconds": trailing,
                        "ongoing": True,
                        "note": "no snapshot since; the recorder itself may have stopped",
                    }
                )
        return {
            "snapshots": len(history),
            "first_recorded_at": history[0]["recorded_at"] if history else None,
            "last_recorded_at": history[-1]["recorded_at"] if history else None,
            "expected_interval_seconds": expected_interval_seconds,
            "not_capable_intervals": recorded_gaps,
            "unwatched_intervals": unwatched_gaps,
        }

    def client_lifetime(self) -> dict[str, dict[str, Any]]:
        """Per-client lifetime totals, independent of any observation window.

        A windowed count cannot tell "never delivered anything" apart from
        "delivered, then stopped". Those need opposite responses -- reconfigure
        versus investigate a hook that went quiet -- so the distinction is kept
        rather than inferred.
        """

        rows = self.connection.execute(
            "SELECT COALESCE(client, 'unknown') AS client, COUNT(*) AS events,"
            " MAX(observed_at) AS last_observed, MAX(received_at) AS last_received"
            " FROM events GROUP BY COALESCE(client, 'unknown')"
        ).fetchall()
        return {str(row["client"]): {key: row[key] for key in row.keys()} for row in rows}

    def observation_samples(self, since: str | None = None) -> dict[str, dict[str, Any]]:
        """Per-client signal coverage for the observation verdict.

        One grouped pass over the window. Each signal family is counted as
        "events that actually carried it", so a caller can always report the
        numerator and denominator instead of treating an absent field as zero.
        """

        clauses = []
        params: list[Any] = []
        if since:
            clauses.append("received_at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.connection.execute(
            f"""
            SELECT
                COALESCE(client, 'unknown') AS client,
                COUNT(*) AS events,
                MAX(observed_at) AS last_observed,
                MAX(received_at) AS last_received,
                SUM(CASE WHEN provider IS NOT NULL AND provider <> 'unknown'
                          AND model IS NOT NULL AND model <> 'unknown' THEN 1 ELSE 0 END) AS model_identity,
                SUM(CASE WHEN session_id IS NOT NULL THEN 1 ELSE 0 END) AS session_identity,
                SUM(CASE WHEN agent_id IS NOT NULL OR subagent_id IS NOT NULL
                          OR parent_agent_id IS NOT NULL THEN 1 ELSE 0 END) AS agent_identity,
                SUM(CASE WHEN total_tokens IS NOT NULL OR input_tokens IS NOT NULL
                          OR output_tokens IS NOT NULL THEN 1 ELSE 0 END) AS token_usage,
                SUM(CASE WHEN cost IS NOT NULL THEN 1 ELSE 0 END) AS cost,
                SUM(CASE WHEN latency_ms IS NOT NULL OR duration_ms IS NOT NULL
                          OR time_to_first_token_ms IS NOT NULL THEN 1 ELSE 0 END) AS latency,
                SUM(CASE WHEN tool_call_count IS NOT NULL OR event_type = 'tool.operation' THEN 1 ELSE 0 END) AS tool_calls,
                SUM(CASE WHEN retry_count IS NOT NULL OR rate_limited IS NOT NULL OR timeout IS NOT NULL
                          OR tool_failure IS NOT NULL OR agent_failure IS NOT NULL
                          OR aborted IS NOT NULL THEN 1 ELSE 0 END) AS errors_retries,
                SUM(CASE WHEN project_id IS NOT NULL AND project_id <> ? THEN 1 ELSE 0 END) AS project_attribution,
                SUM(CASE WHEN outcome_kind IS NOT NULL THEN 1 ELSE 0 END) AS outcomes,
                -- The dimensions the primary question ranks by. Measured live at
                -- 4 of 42,577 cost-bearing events, which meant "which skills
                -- deliver value" was unanswerable at any duration -- and nothing
                -- said so, because no signal family covered it.
                SUM(CASE WHEN skill IS NOT NULL OR workflow_id IS NOT NULL
                          OR task_class IS NOT NULL OR lane IS NOT NULL THEN 1 ELSE 0 END) AS capability_identity
            FROM events
            {where}
            GROUP BY COALESCE(client, 'unknown')
            """,
            (UNKNOWN_PROJECT_ID, *params),
        ).fetchall()
        return {str(row["client"]): {key: row[key] for key in row.keys()} for row in rows}

    def _ensure_capacity(self, payload_bytes: int) -> None:
        if self.max_bytes is None:
            return
        current = self.storage_bytes()
        # SQLite transaction/index overhead can exceed the JSON payload. Keep
        # a conservative reserve so the cap remains meaningful under WAL.
        reserve = max(65_536, payload_bytes)
        if current + reserve > self.max_bytes:
            raise StorageCapacityError(
                f"normalized store capacity reached ({current} + {reserve} > {self.max_bytes} bytes); prune or back up before retrying"
            )

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

        # Only legacy rows need repair. Selecting every event and re-parsing its
        # payload made each store open O(total events) and load the entire
        # corpus into memory, so opening a multi-GB store -- for `doctor`, a CLI
        # command, or API startup -- took minutes and could exhaust RAM before
        # the process ever became ready. `_insert_projections` is INSERT OR
        # IGNORE, so an event that already has both a ledger row and its
        # always-written project attribution edge has nothing left to repair.
        # This anti-join returns no rows on a healthy store, and the payload
        # column is never read for events it excludes.
        cursor = self.connection.execute(
            """
            SELECT e.event_id, e.payload_digest, e.payload_json
            FROM events AS e
            WHERE NOT EXISTS (SELECT 1 FROM ingest_ledger AS l WHERE l.event_id = e.event_id)
               OR NOT EXISTS (SELECT 1 FROM attribution_edges AS a WHERE a.child_event_id = e.event_id)
            ORDER BY e.event_id
            """
        )
        first = cursor.fetchone()
        if first is None:
            return
        with self.connection:
            for row in itertools.chain((first,), cursor):
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

    @staticmethod
    def _semantic_payload_digest(event: NormalizedEvent) -> str:
        """Hash event content while ignoring transport-assigned receipt time."""

        identity = event.to_mapping()
        identity.pop("received_at", None)
        return hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()

    def _record_session_project(self, event: NormalizedEvent) -> None:
        """Remember which project a session belongs to.

        Only a resolved project is ever recorded, and the first one wins, so a
        session cannot be re-attributed by a later event that happens to carry
        weaker identity.
        """

        session_id = event.execution.session_id
        if not session_id or event.project.project_id == UNKNOWN_PROJECT_ID:
            return
        self.connection.execute(
            "INSERT OR IGNORE INTO session_projects(session_id, project_id, repository, branch,"
            " worktree, commit_sha, evidence_source, observed_at, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id,
                event.project.project_id,
                event.project.repository,
                event.project.branch,
                event.project.worktree,
                event.project.commit,
                event.source.name or "unknown",
                event.observed_at.isoformat(),
                event.received_at.isoformat(),
            ),
        )

    def recorrelate_outcomes(self, *, limit: int = 5000) -> dict[str, Any]:
        """Recompute associations for outcomes already stored.

        Correlation runs when an event is appended, so an outcome recorded
        before its counterpart existed -- or before a session binding made the
        counterpart reachable -- keeps whatever links it had at the time. Every
        projection insert is INSERT OR IGNORE, so replaying them adds the links
        that are now derivable without disturbing the ones already recorded, and
        never rewrites a stored event.
        """

        limit = self._bounded_limit(limit, maximum=20000)
        before = self.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE relation = 'outcome_correlation'"
        ).fetchone()[0]
        rows = self.connection.execute(
            "SELECT payload_json FROM events WHERE outcome_kind IS NOT NULL"
            " ORDER BY observed_at LIMIT ?",
            (limit,),
        ).fetchall()
        processed = 0
        # Commit in small batches. A single transaction over every outcome holds
        # the writer lock for minutes, and intake -- which uses a 250 ms busy
        # timeout so it never blocks a client -- reports unavailable for the
        # whole time. Maintenance must never starve the ingest path; releasing
        # the lock between batches lets appends interleave.
        batch: list[NormalizedEvent] = []
        for row in rows:
            try:
                batch.append(NormalizedEvent.from_mapping(json.loads(row["payload_json"])))
            except (json.JSONDecodeError, ContractError):
                continue
            if len(batch) >= RECORRELATE_BATCH_SIZE:
                processed += self._recorrelate_batch(batch)
                batch = []
        if batch:
            processed += self._recorrelate_batch(batch)
        after = self.connection.execute(
            "SELECT COUNT(*) FROM attribution_edges WHERE relation = 'outcome_correlation'"
        ).fetchone()[0]
        return {"outcomes_processed": processed, "edges_before": before, "edges_after": after, "edges_added": after - before}

    def _recorrelate_batch(self, batch: list[NormalizedEvent]) -> int:
        with self.connection:
            for event in batch:
                self._insert_projections(event)
        return len(batch)

    def bind_session_project(
        self,
        session_id: str,
        project: Any,
        *,
        evidence_source: str = "session-directory",
        observed_at: str | None = None,
    ) -> bool:
        """Record where a session was worked, from an external host source.

        Used when the binding comes from something other than a telemetry event
        -- for example the client's own per-project session directory. The first
        binding still wins, so discovery can be re-run safely and can never
        re-attribute a session that telemetry already established.
        """

        if not session_id:
            return False
        project_id = getattr(project, "project_id", None)
        if not project_id or project_id == UNKNOWN_PROJECT_ID:
            return False
        stamp = observed_at or utc_now().isoformat()
        with self.connection:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO session_projects(session_id, project_id, repository, branch,"
                " worktree, commit_sha, evidence_source, observed_at, received_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    project_id,
                    getattr(project, "repository", None),
                    getattr(project, "branch", None),
                    getattr(project, "worktree", None),
                    getattr(project, "commit", None),
                    evidence_source,
                    stamp,
                    stamp,
                ),
            )
        return cursor.rowcount > 0

    def _attribute_from_session(self, event: NormalizedEvent) -> NormalizedEvent:
        """Give an unattributed event the project its session is bound to.

        Native OTLP telemetry carries a session id but no working directory, so
        it arrives as project:unknown. A globally-configured client hook reports
        the same session id together with the resolved project. Enrichment runs
        before persistence, so this fills a blank rather than rewriting a stored
        fact, and the derivation is recorded in field provenance so no consumer
        mistakes it for something the client reported directly.
        """

        if event.project.project_id != UNKNOWN_PROJECT_ID:
            return event
        session_id = event.execution.session_id
        if not session_id:
            return event
        row = self.connection.execute(
            "SELECT project_id, repository, branch, worktree, commit_sha"
            " FROM session_projects WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            return event
        project = replace(
            event.project,
            project_id=str(row["project_id"]),
            repository=event.project.repository or row["repository"],
            branch=event.project.branch or row["branch"],
            worktree=event.project.worktree or row["worktree"],
            commit=event.project.commit or row["commit_sha"],
        )
        fields = dict(event.provenance.fields)
        fields["project.project_id"] = "derived:session_binding"
        return replace(event, project=project, provenance=replace(event.provenance, fields=fields))

    def append(self, event: NormalizedEvent) -> AppendResult:
        safe_event = redact_event(event, self.privacy_policy)
        safe_event = self._attribute_from_session(safe_event)
        payload_json = safe_event.to_json()
        payload_digest = self._semantic_payload_digest(safe_event)
        arrival_at = utc_now().isoformat()
        with self.connection:
            existing = self.connection.execute(
                "SELECT payload_digest, payload_json FROM events WHERE event_id = ?", (safe_event.event_id,)
            ).fetchone()
            if existing:
                existing_digest = str(existing["payload_digest"])
                stored_semantic_digest = None
                if existing_digest != payload_digest:
                    legacy_digest = hashlib.sha256(str(existing["payload_json"]).encode("utf-8")).hexdigest()
                    if existing_digest == legacy_digest:
                        try:
                            stored_mapping = json.loads(str(existing["payload_json"]))
                            if isinstance(stored_mapping, dict):
                                stored_mapping.pop("received_at", None)
                                stored_semantic_digest = hashlib.sha256(canonical_json(stored_mapping).encode("utf-8")).hexdigest()
                        except (json.JSONDecodeError, ContractError, TypeError, ValueError):
                            stored_semantic_digest = None
                if existing_digest == payload_digest or stored_semantic_digest == payload_digest:
                    self._insert_ledger(safe_event, payload_digest, "duplicate", arrival_at)
                    return AppendResult("duplicate", safe_event.event_id)
                self._ensure_capacity(len(payload_json.encode("utf-8")))
                self.connection.execute(
                    "INSERT INTO event_conflicts(event_id, conflict_digest, conflict_payload_json, detected_at) VALUES (?, ?, ?, ?)",
                    (safe_event.event_id, payload_digest, payload_json, arrival_at),
                )
                self._insert_ledger(safe_event, payload_digest, "conflict", arrival_at, "event_id replay has a different payload")
                return AppendResult("conflict", safe_event.event_id, payload_digest)

            self._ensure_capacity(len(payload_json.encode("utf-8")))
            self.connection.execute(
                """
                INSERT INTO events (
                    event_id, schema_version, event_type, observed_at, received_at,
                    project_id, repository, provider, model, client, auth_mode, route,
                    status, usage_source, input_tokens, output_tokens, total_tokens,
                    trace_id, span_id, parent_event_id, session_id, workflow_id,
                    agent_id, subagent_id, parent_agent_id, evidence_source, payload_digest, payload_json,
                    inserted_at, model_family, reasoning_effort, branch, commit_sha,
                    worktree, role, skill, lane, outcome_kind, outcome_status,
                    task_id, task_class,
                    timeout, tool_failure, agent_failure, aborted,
                    reassessment_count, rework_count,
                    cached_tokens, reasoning_tokens, cost, latency_ms,
                    time_to_first_token_ms, duration_ms, retry_count, rate_limited,
                    cache_creation_tokens, cache_read_tokens, context_size, context_utilization,
                    compaction_count, tool_duration_ms, session_duration_ms, agent_duration_ms,
                    workflow_duration_ms, wall_clock_ms, concurrency, parallel_utilization,
                    model_variant, tool_call_count, files_inspected_count, files_changed_count,
                    commands_executed_count, tests_invoked_count
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
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
                    safe_event.execution.parent_agent_id,
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
                    1 if safe_event.reliability.agent_failure else 0 if safe_event.reliability.agent_failure is not None else None,
                    1 if safe_event.reliability.aborted else 0 if safe_event.reliability.aborted is not None else None,
                    safe_event.reliability.reassessment_count,
                    safe_event.reliability.rework_count,
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
                    safe_event.llm.model_variant,
                    safe_event.behavior.tool_call_count,
                    safe_event.behavior.files_inspected_count,
                    safe_event.behavior.files_changed_count,
                    safe_event.behavior.commands_executed_count,
                    safe_event.behavior.tests_invoked_count,
                ),
            )
            self._insert_ledger(safe_event, payload_digest, "inserted", arrival_at)
            self._insert_projections(safe_event)
            self._record_session_project(safe_event)
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

    @staticmethod
    def _measurement_source(event: NormalizedEvent, field_path: str, section: str, default: str) -> str:
        source = event.provenance.fields.get(field_path) or event.provenance.fields.get(section) or default
        return str(source or "unknown")

    def _insert_projections(self, event: NormalizedEvent) -> None:
        """Materialize bitemporal facts without changing the event envelope."""

        measurements = (
            ("usage.input_tokens", event.usage.input_tokens, "tokens", self._measurement_source(event, "usage.input_tokens", "usage", event.usage.source), "usage"),
            ("usage.output_tokens", event.usage.output_tokens, "tokens", self._measurement_source(event, "usage.output_tokens", "usage", event.usage.source), "usage"),
            ("usage.cached_tokens", event.usage.cached_tokens, "tokens", self._measurement_source(event, "usage.cached_tokens", "usage", event.usage.source), "usage"),
            ("usage.cache_creation_tokens", event.usage.cache_creation_tokens, "tokens", self._measurement_source(event, "usage.cache_creation_tokens", "usage", event.usage.source), "usage"),
            ("usage.cache_read_tokens", event.usage.cache_read_tokens, "tokens", self._measurement_source(event, "usage.cache_read_tokens", "usage", event.usage.source), "usage"),
            ("usage.reasoning_tokens", event.usage.reasoning_tokens, "tokens", self._measurement_source(event, "usage.reasoning_tokens", "usage", event.usage.source), "usage"),
            ("usage.total_tokens", event.usage.total_tokens, "tokens", self._measurement_source(event, "usage.total_tokens", "usage", event.usage.source), "usage"),
            ("usage.cost", event.usage.cost, "cost", self._measurement_source(event, "usage.cost", "usage", event.usage.source), "usage"),
            ("usage.context_size", event.usage.context_size, "tokens", self._measurement_source(event, "usage.context_size", "usage", event.usage.source), "usage"),
            ("usage.context_utilization", event.usage.context_utilization, "ratio", self._measurement_source(event, "usage.context_utilization", "usage", event.usage.source), "usage"),
            ("usage.compaction_count", event.usage.compaction_count, "count", self._measurement_source(event, "usage.compaction_count", "usage", event.usage.source), "usage"),
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
            (
                "reliability.agent_failure",
                1 if event.reliability.agent_failure else 0 if event.reliability.agent_failure is not None else None,
                "flag",
                str(event.provenance.fields.get("reliability.agent_failure") or event.provenance.fields.get("reliability") or "unknown"),
                "reliability",
            ),
            (
                "reliability.reassessment_count",
                event.reliability.reassessment_count,
                "count",
                str(event.provenance.fields.get("reliability.reassessment_count") or event.provenance.fields.get("reliability") or "unknown"),
                "reliability",
            ),
            (
                "reliability.rework_count",
                event.reliability.rework_count,
                "count",
                str(event.provenance.fields.get("reliability.rework_count") or event.provenance.fields.get("reliability") or "unknown"),
                "reliability",
            ),
            ("performance.tool_duration_ms", event.performance.tool_duration_ms, "ms", str(event.provenance.fields.get("performance.tool_duration_ms") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.session_duration_ms", event.performance.session_duration_ms, "ms", str(event.provenance.fields.get("performance.session_duration_ms") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.agent_duration_ms", event.performance.agent_duration_ms, "ms", str(event.provenance.fields.get("performance.agent_duration_ms") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.workflow_duration_ms", event.performance.workflow_duration_ms, "ms", str(event.provenance.fields.get("performance.workflow_duration_ms") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.wall_clock_ms", event.performance.wall_clock_ms, "ms", str(event.provenance.fields.get("performance.wall_clock_ms") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.concurrency", event.performance.concurrency, "workers", str(event.provenance.fields.get("performance.concurrency") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("performance.parallel_utilization", event.performance.parallel_utilization, "ratio", str(event.provenance.fields.get("performance.parallel_utilization") or event.provenance.fields.get("performance") or "unknown"), "performance"),
            ("behavior.tool_call_count", event.behavior.tool_call_count, "count", self._measurement_source(event, "behavior.tool_call_count", "behavior", "unknown"), "behavior"),
            ("behavior.files_inspected_count", event.behavior.files_inspected_count, "count", self._measurement_source(event, "behavior.files_inspected_count", "behavior", "unknown"), "behavior"),
            ("behavior.files_changed_count", event.behavior.files_changed_count, "count", self._measurement_source(event, "behavior.files_changed_count", "behavior", "unknown"), "behavior"),
            ("behavior.commands_executed_count", event.behavior.commands_executed_count, "count", self._measurement_source(event, "behavior.commands_executed_count", "behavior", "unknown"), "behavior"),
            ("behavior.tests_invoked_count", event.behavior.tests_invoked_count, "count", self._measurement_source(event, "behavior.tests_invoked_count", "behavior", "unknown"), "behavior"),
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
            ("parent_agent", execution.parent_agent_id),
        ):
            if target_id:
                edge_values.append((relation, None, target_id))
        correlated_event_ids: list[str] = []
        current_is_outcome = bool(outcome.kind or outcome.status)
        correlation_columns = {
            "task_id": "events.task_id",
            "session_id": "events.session_id",
            "workflow_id": "events.workflow_id",
            "agent_id": "events.agent_id",
            "subagent_id": "events.subagent_id",
            "trace_id": "events.trace_id",
            "worktree": "events.worktree",
        }
        execution_values = {
            "task_id": execution.task_id,
            "session_id": execution.session_id,
            "workflow_id": execution.workflow_id,
            "agent_id": execution.agent_id,
            "subagent_id": execution.subagent_id,
            "trace_id": execution.trace_id,
            "worktree": event.project.worktree,
        }

        def add_correlations(rows: Iterable[Any]) -> None:
            for related_row in rows:
                related_event_id = str(related_row["event_id"])
                if related_event_id == event.event_id or related_event_id in correlated_event_ids:
                    continue
                correlated_event_ids.append(related_event_id)
                edge_values.append(("outcome_correlation", related_event_id, related_event_id))

        basis = outcome.correlation_basis
        value = outcome.correlation_id
        if basis in correlation_columns and value is None:
            value = execution_values.get(basis)
        if current_is_outcome and basis == "event_id" and value:
            add_correlations(self.connection.execute(
                "SELECT event_id FROM events WHERE event_id = ? AND event_id <> ?",
                (value, event.event_id),
            ).fetchall())
        elif current_is_outcome and basis in correlation_columns and value:
            add_correlations(self.connection.execute(
                f"""
                SELECT events.event_id
                FROM events
                WHERE {correlation_columns[basis]} = ?
                  AND events.event_id <> ?
                -- Newest first: when the cap bites, the work immediately before
                -- the outcome is the most probative association, and ascending
                -- order discarded exactly that.
                ORDER BY events.observed_at DESC, events.event_id DESC
                LIMIT ?
                """,
                (value, event.event_id, CORRELATION_MAX_LINKS),
            ).fetchall())
        elif current_is_outcome and basis == PROJECT_WINDOW_BASIS and value:
            # Bounded temporal association: the weakest evidence this store
            # accepts, and the only one available for an outcome like a commit
            # that carries no session or task of its own. It links the outcome
            # to work in the *same project* within a bounded window before it.
            # The window is finite and recorded on the outcome, so a reader can
            # see exactly how loose the association is; without a bound this
            # would silently attach every event a project ever produced.
            window = _project_window_seconds(event.attributes)
            add_correlations(self.connection.execute(
                """
                SELECT event_id FROM (
                    -- Two index-friendly branches instead of one OR: an OR
                    -- across different columns makes SQLite abandon both
                    -- composite indexes and scan the table.
                    SELECT events.event_id AS event_id, events.observed_at AS observed_at
                    FROM events
                    WHERE events.project_id = ?
                      AND events.observed_at <= ? AND events.observed_at >= ?
                      AND events.outcome_kind IS NULL AND events.event_id <> ?
                    UNION
                    -- Events stored before their session was bound keep
                    -- project:unknown, because the envelope is immutable. The
                    -- binding is recorded evidence of where that session worked,
                    -- so consult it rather than discarding history. Restricted
                    -- to unknown rows so a known, different project is never
                    -- overridden.
                    SELECT events.event_id AS event_id, events.observed_at AS observed_at
                    FROM session_projects
                    JOIN events ON events.session_id = session_projects.session_id
                    WHERE session_projects.project_id = ?
                      AND events.project_id = ?
                      AND events.observed_at <= ? AND events.observed_at >= ?
                      AND events.outcome_kind IS NULL AND events.event_id <> ?
                )
                ORDER BY observed_at DESC, event_id DESC
                LIMIT ?
                """,
                (
                    value,
                    event.observed_at.isoformat(),
                    (event.observed_at - timedelta(seconds=window)).isoformat(),
                    event.event_id,
                    value,
                    UNKNOWN_PROJECT_ID,
                    event.observed_at.isoformat(),
                    (event.observed_at - timedelta(seconds=window)).isoformat(),
                    event.event_id,
                    PROJECT_WINDOW_MAX_LINKS,
                ),
            ).fetchall())
        elif current_is_outcome and basis is None and execution.task_id:
            add_correlations(self.connection.execute(
                """
                SELECT events.event_id
                FROM events
                WHERE events.task_id = ? AND events.event_id <> ?
                ORDER BY events.observed_at DESC, events.event_id DESC
                LIMIT ?
                """,
                (execution.task_id, event.event_id, CORRELATION_MAX_LINKS),
            ).fetchall())
        elif not current_is_outcome and execution.task_id:
            # Preserve task-id correlation for outcomes that carry their task
            # identity in the execution block rather than explicit fields.
            add_correlations(self.connection.execute(
                """
                SELECT events.event_id
                FROM events
                JOIN outcome_events ON outcome_events.event_id = events.event_id
                WHERE events.task_id = ? AND events.event_id <> ?
                ORDER BY events.observed_at DESC, events.event_id DESC
                LIMIT ?
                """,
                (execution.task_id, event.event_id, CORRELATION_MAX_LINKS),
            ).fetchall())

        if not current_is_outcome:
            # Outcomes may be written before the operation they describe.
            # Match explicit basis/value pairs in the reverse direction so
            # insertion order does not decide whether attribution exists.
            #
            # Capped for the same reason as the forward direction, and it is the
            # same cap: this branch runs for EVERY non-outcome event on the
            # append path, so an uncapped reverse scan re-linked each new event
            # to every outcome the session had ever produced. Bounding only the
            # forward direction left total edge growth for a session unbounded
            # anyway -- one outcome linked to at most 500 events forward while
            # each of those linked to unlimited outcomes backward.
            for candidate_basis, candidate_value in execution_values.items():
                if not candidate_value:
                    continue
                add_correlations(self.connection.execute(
                    """
                    SELECT outcome_events.event_id
                    FROM outcome_events
                    WHERE outcome_events.correlation_basis = ?
                      AND outcome_events.correlation_id = ?
                      AND outcome_events.event_id <> ?
                    ORDER BY outcome_events.observed_at DESC, outcome_events.event_id DESC
                    LIMIT ?
                    """,
                    (candidate_basis, candidate_value, event.event_id, CORRELATION_MAX_LINKS),
                ).fetchall())
            add_correlations(self.connection.execute(
                """
                SELECT outcome_events.event_id
                FROM outcome_events
                WHERE outcome_events.correlation_basis = 'event_id'
                  AND outcome_events.correlation_id = ?
                  AND outcome_events.event_id <> ?
                ORDER BY outcome_events.observed_at DESC, outcome_events.event_id DESC
                LIMIT ?
                """,
                (event.event_id, event.event_id, CORRELATION_MAX_LINKS),
            ).fetchall())
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

    def _summary_statements(
        self, filters: Mapping[str, str] | None = None
    ) -> dict[str, tuple[str, list[str]]]:
        """The three reads behind `summary()`, as (sql, params) pairs.

        Split out so a regression test can plan the statements this method
        actually issues instead of a copy that drifts from them. Both reads over
        `events` are meant to be served entirely by `idx_events_summary_rollup`
        (migration 020); the events table itself is mostly `payload_json`, so
        touching a row rather than an index entry costs about a page each and
        turns a rollup into a multi-second read.
        """

        where, params = self._where_clause(filters or {})
        # Correlating on the event_id primary key lets SQLite drive from
        # outcome_events -- a few hundred rows -- and probe `events` by unique
        # index. The previous `event_id IN (SELECT event_id FROM events WHERE
        # ...)` form could not: a filtered summary made it materialize every
        # matching event id, which meant one random row fetch per matching
        # event (420k of them for a one-day window, 1.96 s) purely to build a
        # list that at most a few hundred outcomes would ever probe. `event_id`
        # is the primary key, so at most one event matches each outcome and the
        # counts are unchanged.
        correlated, correlated_params = self._filter_terms(filters or {}, alias="events")
        correlated_sql = "".join(f" AND {clause}" for clause in correlated)
        return {
            "aggregate": (self._SUMMARY_AGGREGATE_SQL.format(where=where), params),
            "usage_sources": (self._SUMMARY_USAGE_SOURCE_SQL.format(where=where), params),
            "outcomes": (
                self._SUMMARY_OUTCOME_SQL.format(correlated=correlated_sql),
                correlated_params,
            ),
        }

    _SUMMARY_AGGREGATE_SQL = """
            SELECT
                COUNT(*) AS events,
                SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                SUM(input_tokens) AS input_tokens,
                SUM(output_tokens) AS output_tokens,
                SUM(cached_tokens) AS cached_tokens,
                SUM(cache_creation_tokens) AS cache_creation_tokens,
                SUM(cache_read_tokens) AS cache_read_tokens,
                SUM(reasoning_tokens) AS reasoning_tokens,
                SUM(compaction_count) AS compactions,
                SUM(cost) AS cost,
                AVG(latency_ms) AS average_latency_ms,
                AVG(time_to_first_token_ms) AS average_time_to_first_token_ms,
                AVG(duration_ms) AS average_duration_ms,
                AVG(context_size) AS average_context_size,
                AVG(context_utilization) AS average_context_utilization,
                AVG(concurrency) AS average_concurrency,
                AVG(parallel_utilization) AS average_parallel_utilization,
                COALESCE(SUM(retry_count), 0) AS retries,
                COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures,
                COALESCE(SUM(CASE WHEN agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                COALESCE(SUM(reassessment_count), 0) AS reassessments,
                COALESCE(SUM(rework_count), 0) AS rework_loops,
                COALESCE(SUM(tool_call_count), 0) AS tool_calls,
                COALESCE(SUM(files_inspected_count), 0) AS files_inspected,
                COALESCE(SUM(files_changed_count), 0) AS files_changed,
                COALESCE(SUM(commands_executed_count), 0) AS commands_executed,
                COALESCE(SUM(tests_invoked_count), 0) AS tests_invoked,
                COALESCE(SUM(CASE WHEN aborted = 1 THEN 1 ELSE 0 END), 0) AS aborted,
                COUNT(DISTINCT project_id) AS projects,
                COUNT(DISTINCT provider || ':' || model) AS models
            FROM events {where}
            """

    _SUMMARY_USAGE_SOURCE_SQL = (
        "SELECT usage_source, COUNT(*) AS count FROM events {where}"
        " GROUP BY usage_source ORDER BY usage_source"
    )

    _SUMMARY_OUTCOME_SQL = """
            SELECT outcome_events.kind, outcome_events.status, COUNT(*) AS count
            FROM outcome_events
            WHERE EXISTS (
                SELECT 1 FROM events
                WHERE events.event_id = outcome_events.event_id{correlated}
            )
            GROUP BY outcome_events.kind, outcome_events.status
            ORDER BY outcome_events.kind, outcome_events.status
            """

    def summary(self, filters: Mapping[str, str] | None = None) -> dict[str, Any]:
        statements = self._summary_statements(filters)
        row = self.connection.execute(*statements["aggregate"]).fetchone()
        provenance_rows = self.connection.execute(*statements["usage_sources"]).fetchall()
        outcome_rows = self.connection.execute(*statements["outcomes"]).fetchall()
        return {
            "events": int(row["events"] or 0),
            "successes": int(row["successes"] or 0),
            "failures": int(row["failures"] or 0),
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "cached_tokens": row["cached_tokens"],
            "cache_creation_tokens": row["cache_creation_tokens"],
            "cache_read_tokens": row["cache_read_tokens"],
            "reasoning_tokens": row["reasoning_tokens"],
            "compactions": row["compactions"],
            "cost": row["cost"],
            "average_latency_ms": row["average_latency_ms"],
            "average_time_to_first_token_ms": row["average_time_to_first_token_ms"],
            "average_duration_ms": row["average_duration_ms"],
            "average_context_size": row["average_context_size"],
            "average_context_utilization": row["average_context_utilization"],
            "average_concurrency": row["average_concurrency"],
            "average_parallel_utilization": row["average_parallel_utilization"],
            "retries": row["retries"] or 0,
            "rate_limited": int(row["rate_limited"] or 0),
            "timeouts": int(row["timeouts"] or 0),
            "tool_failures": int(row["tool_failures"] or 0),
            "agent_failures": int(row["agent_failures"] or 0),
            "reassessments": row["reassessments"] or 0,
            "rework_loops": row["rework_loops"] or 0,
            "tool_calls": row["tool_calls"] or 0,
            "files_inspected": row["files_inspected"] or 0,
            "files_changed": row["files_changed"] or 0,
            "commands_executed": row["commands_executed"] or 0,
            "tests_invoked": row["tests_invoked"] or 0,
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
            SELECT project_id AS project, provider, model, model_family, model_variant, client, auth_mode, route, usage_source, task_class, COUNT(*) AS count,
                   SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                   SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                   SUM(total_tokens) AS total_tokens,
                   SUM(cost) AS cost,
                   AVG(latency_ms) AS average_latency_ms,
                   COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures,
                   COALESCE(SUM(CASE WHEN agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                   COALESCE(SUM(reassessment_count), 0) AS reassessments,
                   COALESCE(SUM(rework_count), 0) AS rework_loops,
                   COALESCE(SUM(tool_call_count), 0) AS tool_calls,
                   COALESCE(SUM(files_inspected_count), 0) AS files_inspected,
                   COALESCE(SUM(files_changed_count), 0) AS files_changed,
                   COALESCE(SUM(commands_executed_count), 0) AS commands_executed,
                   COALESCE(SUM(tests_invoked_count), 0) AS tests_invoked
             FROM events
             WHERE event_type = 'model.operation'
             GROUP BY project_id, provider, model, model_family, model_variant, client, auth_mode, route, usage_source, task_class
            ORDER BY count DESC, project, provider, model, model_family, model_variant, client, auth_mode, route, usage_source, task_class
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        usage_source = self.connection.execute(
            "SELECT usage_source AS source, COUNT(*) AS count FROM events WHERE event_type = 'model.operation' GROUP BY usage_source ORDER BY count DESC, source LIMIT ?",
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
            SELECT event_type,
                   COALESCE(project_id, 'unknown') AS project,
                   COALESCE(repository, 'unknown') AS repository,
                   COALESCE(branch, 'unknown') AS branch,
                   COALESCE(role, 'unknown') AS role,
                   COALESCE(skill, 'unknown') AS skill,
                   COALESCE(lane, 'unknown') AS lane,
                   COUNT(*) AS count
            FROM events
            GROUP BY event_type, project_id, repository, branch, role, skill, lane
            ORDER BY count DESC, event_type, project, repository, branch, role, skill, lane
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        workflow = self.connection.execute(
            """
            SELECT event_type,
                   COALESCE(project_id, 'unknown') AS project,
                   COALESCE(repository, 'unknown') AS repository,
                   COALESCE(branch, 'unknown') AS branch,
                   COALESCE(workflow_id, 'unknown') AS workflow,
                   COUNT(*) AS count
            FROM events
            GROUP BY event_type, project_id, repository, branch, workflow_id
            ORDER BY count DESC, event_type, project, repository, branch, workflow
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        agent = self.connection.execute(
            """
            SELECT event_type,
                   COALESCE(project_id, 'unknown') AS project,
                   COALESCE(repository, 'unknown') AS repository,
                   COALESCE(branch, 'unknown') AS branch,
                   COALESCE(agent_id, 'unknown') AS agent,
                   COALESCE(subagent_id, 'unknown') AS subagent,
                   COALESCE(parent_agent_id, 'unknown') AS parent_agent,
                   COUNT(*) AS count
            FROM events
            GROUP BY event_type, project_id, repository, branch, agent_id, subagent_id, parent_agent_id
            ORDER BY count DESC, event_type, project, repository, branch, agent, subagent, parent_agent
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        context = self.connection.execute(
            """
             SELECT event_type,
                    project_id AS project,
                   COALESCE(repository, 'unknown') AS repository,
                   COALESCE(branch, 'unknown') AS branch,
                   provider,
                   model,
                   COALESCE(model_family, 'unknown') AS model_family,
                   COALESCE(model_variant, 'unknown') AS model_variant,
                   client,
                   auth_mode,
                   route,
                   usage_source,
                   COALESCE(agent_id, 'unknown') AS agent,
                   COALESCE(subagent_id, 'unknown') AS subagent,
                   COALESCE(parent_agent_id, 'unknown') AS parent_agent,
                   COALESCE(role, 'unknown') AS role,
                   COALESCE(skill, 'unknown') AS skill,
                   COALESCE(lane, 'unknown') AS lane,
                   COALESCE(workflow_id, 'unknown') AS workflow,
                   COALESCE(task_class, 'unknown') AS task_class,
                   COALESCE(status, 'unknown') AS status,
                    COUNT(*) AS count,
                    SUM(input_tokens) AS input_tokens,
                    SUM(output_tokens) AS output_tokens,
                    SUM(cached_tokens) AS cached_tokens,
                    SUM(reasoning_tokens) AS reasoning_tokens,
                    SUM(total_tokens) AS total_tokens,
                    SUM(cache_creation_tokens) AS cache_creation_tokens,
                    SUM(cache_read_tokens) AS cache_read_tokens,
                    SUM(compaction_count) AS compactions,
                    SUM(cost) AS cost,
                    AVG(latency_ms) AS average_latency_ms,
                    AVG(time_to_first_token_ms) AS average_time_to_first_token_ms,
                    AVG(duration_ms) AS average_duration_ms,
                    AVG(context_size) AS average_context_size,
                    AVG(context_utilization) AS average_context_utilization,
                    AVG(concurrency) AS average_concurrency,
                    AVG(parallel_utilization) AS average_parallel_utilization,
                    COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures,
                   COALESCE(SUM(CASE WHEN agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                   COALESCE(SUM(reassessment_count), 0) AS reassessments,
                   COALESCE(SUM(rework_count), 0) AS rework_loops,
                   COALESCE(SUM(tool_call_count), 0) AS tool_calls,
                   COALESCE(SUM(files_inspected_count), 0) AS files_inspected,
                   COALESCE(SUM(files_changed_count), 0) AS files_changed,
                   COALESCE(SUM(commands_executed_count), 0) AS commands_executed,
                   COALESCE(SUM(tests_invoked_count), 0) AS tests_invoked
             FROM events
             GROUP BY event_type, project_id, repository, branch, provider, model, model_family, model_variant,
                     client, auth_mode, route, usage_source, agent_id, subagent_id, parent_agent_id, role, skill,
                     lane, workflow_id, task_class, status
            ORDER BY count DESC, project, repository, branch, provider, model, model_variant, client
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        outcomes = self.connection.execute(
            """
            SELECT COALESCE(outcome_events.kind, 'unknown') AS kind,
                   COALESCE(outcome_events.status, 'unknown') AS status,
                   COALESCE(outcome_events.correlation_basis, 'uncorrelated') AS correlation_basis,
                   COALESCE(outcome_events.evidence_source, 'unknown') AS evidence_source,
                   COALESCE(events.project_id, 'unknown') AS project,
                   COALESCE(events.repository, 'unknown') AS repository,
                   COALESCE(events.branch, 'unknown') AS branch,
                   COUNT(*) AS count
            FROM outcome_events
            JOIN events ON events.event_id = outcome_events.event_id
            GROUP BY outcome_events.kind, outcome_events.status, outcome_events.correlation_basis,
                     outcome_events.evidence_source, events.project_id, events.repository, events.branch
            ORDER BY count DESC, project, repository, branch, kind, status, correlation_basis, evidence_source
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

    def outcome_value(self, filters: Mapping[str, str] | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        """Cost and effort of LLM activity that is associated with an outcome.

        This is the only aggregation that crosses `attribution_edges`, so it is
        the only one that can relate spend to a validated engineering result.
        Two properties keep it honest:

        * association is not causation. Rows carry the `correlation_basis` that
          produced the link and never a `caused_by`; an outcome sharing a
          session with an operation is evidence of association only.
        * every money and token total is reported with the count of events that
          actually carried that field. A model whose client never reports cost
          must not appear cheaper than one that does, so the caller can always
          divide by a real denominator instead of assuming zero.
        """

        limit = self._bounded_limit(limit, maximum=500)
        predicate, params, outcome_predicate, outcome_params = self._correlation_predicates(filters)
        rows = self.connection.execute(
            f"""
            WITH linked AS (
                SELECT DISTINCT
                    CASE WHEN a.child_event_id = o.event_id
                         THEN a.parent_event_id ELSE a.child_event_id END AS llm_event_id,
                    o.event_id AS outcome_event_id,
                    -- NOT NULL: `outcome_events.kind`/`.status` are nullable and
                    -- a correlation-only outcome writes NULL for both. NULL = NULL
                    -- is false, so a `USING` join silently dropped that entire
                    -- group -- its spend vanished from the only aggregation that
                    -- relates cost to a result, with no error and no coverage flag.
                    COALESCE(o.kind, 'unknown') AS outcome_kind,
                    COALESCE(o.status, 'unknown') AS outcome_status,
                    COALESCE(o.correlation_basis, 'uncorrelated') AS correlation_basis
                FROM attribution_edges AS a
                JOIN outcome_events AS o
                  ON o.event_id IN (a.child_event_id, a.parent_event_id)
                WHERE a.relation = 'outcome_correlation'
            ),
            -- Effort must be summed over each operation ONCE per group. Joining
            -- `linked` straight to `events` repeated an operation for every
            -- outcome it was linked to, multiplying cost and tokens by the link
            -- count while coverage reported 1.0, because numerator and
            -- denominator inflated together.
            per_group AS (
                SELECT DISTINCT l.llm_event_id, l.outcome_kind, l.outcome_status, l.correlation_basis
                FROM linked AS l
                WHERE 1 = 1 {outcome_predicate}
            ),
            effort AS (
                SELECT
                    g.outcome_kind, g.outcome_status, g.correlation_basis,
                    COALESCE(e.provider, 'unknown') AS provider,
                    COALESCE(e.model, 'unknown') AS model,
                    COALESCE(e.client, 'unknown') AS client,
                    COALESCE(e.agent_id, 'unknown') AS agent_id,
                    COALESCE(e.skill, 'unknown') AS skill,
                    COALESCE(e.project_id, 'unknown') AS project_id,
                    COUNT(*) AS associated_events,
                    SUM(e.cost) AS cost,
                    SUM(CASE WHEN e.cost IS NOT NULL THEN 1 ELSE 0 END) AS cost_reported_events,
                    SUM(e.total_tokens) AS total_tokens,
                    SUM(CASE WHEN e.total_tokens IS NOT NULL THEN 1 ELSE 0 END) AS tokens_reported_events,
                    AVG(e.latency_ms) AS avg_latency_ms,
                    SUM(CASE WHEN e.latency_ms IS NOT NULL THEN 1 ELSE 0 END) AS latency_reported_events,
                    COALESCE(SUM(e.retry_count), 0) AS retries,
                    SUM(CASE WHEN e.retry_count IS NOT NULL THEN 1 ELSE 0 END) AS retries_reported_events,
                    COALESCE(SUM(e.rework_count), 0) AS rework_loops,
                    SUM(CASE WHEN e.rework_count IS NOT NULL THEN 1 ELSE 0 END) AS rework_reported_events,
                    COALESCE(SUM(e.reassessment_count), 0) AS reassessments,
                    SUM(CASE WHEN e.reassessment_count IS NOT NULL THEN 1 ELSE 0 END) AS reassessments_reported_events,
                    COALESCE(SUM(CASE WHEN e.agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                    SUM(CASE WHEN e.agent_failure IS NOT NULL THEN 1 ELSE 0 END) AS agent_failures_reported_events
                FROM per_group AS g
                JOIN events AS e ON e.event_id = g.llm_event_id
                WHERE e.outcome_kind IS NULL {predicate}
                GROUP BY g.outcome_kind, g.outcome_status, g.correlation_basis,
                         provider, model, client, agent_id, skill, project_id
            ),
            results AS (
                SELECT
                    l.outcome_kind, l.outcome_status, l.correlation_basis,
                    COALESCE(e.provider, 'unknown') AS provider,
                    COALESCE(e.model, 'unknown') AS model,
                    COALESCE(e.client, 'unknown') AS client,
                    COALESCE(e.agent_id, 'unknown') AS agent_id,
                    COALESCE(e.skill, 'unknown') AS skill,
                    COALESCE(e.project_id, 'unknown') AS project_id,
                    COUNT(DISTINCT l.outcome_event_id) AS associated_outcomes
                FROM linked AS l
                JOIN events AS e ON e.event_id = l.llm_event_id
                WHERE e.outcome_kind IS NULL {predicate} {outcome_predicate}
                GROUP BY l.outcome_kind, l.outcome_status, l.correlation_basis,
                         provider, model, client, agent_id, skill, project_id
            )
            SELECT effort.*, results.associated_outcomes
            FROM effort
            JOIN results USING (outcome_kind, outcome_status, correlation_basis,
                                provider, model, client, agent_id, skill, project_id)
            ORDER BY associated_events DESC
            LIMIT ?
            """,
            (*outcome_params, *params, *params, *outcome_params, limit),
        ).fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            record = {key: row[key] for key in row.keys()}
            observed = int(record["associated_events"] or 0)
            # Coverage travels with the number so a partial sample is never read
            # as a complete one.
            # A measure summed with COALESCE(..., 0) makes "never reported" look
            # identical to "reported zero", and a client that stays silent then
            # ranks better than one that admits its retries. Coverage travels
            # with every summed measure, not just the money ones.
            record["coverage"] = {
                "cost": _coverage_ratio(record.pop("cost_reported_events"), observed),
                "tokens": _coverage_ratio(record.pop("tokens_reported_events"), observed),
                "latency": _coverage_ratio(record.pop("latency_reported_events"), observed),
                "retries": _coverage_ratio(record.pop("retries_reported_events"), observed),
                "rework_loops": _coverage_ratio(record.pop("rework_reported_events"), observed),
                "reassessments": _coverage_ratio(record.pop("reassessments_reported_events"), observed),
                "agent_failures": _coverage_ratio(record.pop("agent_failures_reported_events"), observed),
            }
            record["association_only"] = True
            results.append(record)
        return results

    def engineering_value(
        self,
        filters: Mapping[str, str] | None = None,
        *,
        limit: int = 100,
        min_outcomes: int = MIN_RANKING_OUTCOMES,
        dimensions: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Rank AI configurations by validated outcome relative to effort.

        This is the question the whole store exists to answer, so it is also the
        easiest one to answer dishonestly. Three guards:

        * effort is summed over *distinct* associated events, so an operation
          linked to two outcomes is not counted twice;
        * a configuration with fewer than ``min_outcomes`` associated outcomes is
          returned with ``evidence: "insufficient"`` and is not ranked, because a
          success rate over one or two outcomes is noise, not a finding;
        * ``cost_per_success`` is None unless cost was actually reported for
          every associated event, since a partially-reported total would make a
          quiet client look cheap.

        Association is not causation. Rows say a configuration was *associated
        with* an outcome through the stated basis.
        """

        limit = self._bounded_limit(limit, maximum=500)
        predicate, params, outcome_predicate, outcome_params = self._correlation_predicates(filters)
        # Resolution is the operator's to choose. Grouping on all five dimensions
        # at once splits a modest corpus into many groups of one or two, so every
        # group falls under `min_outcomes` and nothing ranks -- which reads as
        # "not enough data" when the truth is "asked at too fine a grain".
        # Measured live: 339 correlated CI outcomes became 11 groups of 1-3,
        # because agent_id alone had 72 distinct values across 10.8% of events.
        # Coarsening does not invent evidence; it reports the same evidence at a
        # resolution it can actually support, and coverage travels either way.
        selected = tuple(dimensions) if dimensions is not None else RANKING_DIMENSIONS
        if not selected:
            raise ValueError("at least one ranking dimension is required")
        unknown = [name for name in selected if name not in RANKING_DIMENSIONS]
        if unknown:
            raise ValueError(
                f"unsupported ranking dimension(s): {', '.join(sorted(unknown))}; "
                f"choose from {', '.join(RANKING_DIMENSIONS)}"
            )
        # Deduplicate while preserving the canonical order, so the SQL below is
        # built only from allowlisted identifiers and never from caller text.
        selected = tuple(name for name in RANKING_DIMENSIONS if name in set(selected))
        config = ", ".join(_ranking_expression(name) for name in selected)
        # GROUP BY the EXPRESSION, never the output alias. `GROUP BY skill`
        # binds to the `events.skill` column rather than the alias, so every
        # window-resolved row collapsed into one NULL group and SQLite reported
        # an arbitrary row's value as the group's skill -- two different skills
        # counted as one. Harmless for a plain COALESCE alias, silently wrong
        # for a computed one.
        group = ", ".join(_ranking_group_expression(name) for name in selected)
        using = ", ".join(selected)
        rows = self.connection.execute(
            f"""
            WITH linked AS (
                SELECT DISTINCT
                    CASE WHEN a.child_event_id = o.event_id
                         THEN a.parent_event_id ELSE a.child_event_id END AS llm_event_id,
                    o.event_id AS outcome_event_id,
                    COALESCE(o.kind, 'unknown') AS outcome_kind,
                    COALESCE(o.status, 'unknown') AS outcome_status,
                    COALESCE(o.correlation_basis, 'uncorrelated') AS correlation_basis
                FROM attribution_edges AS a
                JOIN outcome_events AS o
                  ON o.event_id IN (a.child_event_id, a.parent_event_id)
                WHERE a.relation = 'outcome_correlation'
            ),
            associated AS (
                SELECT DISTINCT l.llm_event_id FROM linked AS l
                WHERE 1 = 1 {outcome_predicate}
            ),
            effort AS (
                SELECT {config},
                       COUNT(*) AS associated_events,
                       SUM(e.cost) AS cost,
                       SUM(CASE WHEN e.cost IS NOT NULL THEN 1 ELSE 0 END) AS cost_reported_events,
                       SUM(e.total_tokens) AS total_tokens,
                       SUM(CASE WHEN e.total_tokens IS NOT NULL THEN 1 ELSE 0 END) AS tokens_reported_events,
                       AVG(e.latency_ms) AS avg_latency_ms,
                       COALESCE(SUM(e.retry_count), 0) AS retries,
                       SUM(CASE WHEN e.retry_count IS NOT NULL THEN 1 ELSE 0 END) AS retries_reported_events,
                       COALESCE(SUM(e.rework_count), 0) AS rework_loops,
                       SUM(CASE WHEN e.rework_count IS NOT NULL THEN 1 ELSE 0 END) AS rework_reported_events,
                       COALESCE(SUM(e.reassessment_count), 0) AS reassessments,
                       SUM(CASE WHEN e.reassessment_count IS NOT NULL THEN 1 ELSE 0 END) AS reassessments_reported_events,
                       COALESCE(SUM(CASE WHEN e.agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                       SUM(CASE WHEN e.agent_failure IS NOT NULL THEN 1 ELSE 0 END) AS agent_failures_reported_events
                FROM associated AS s
                JOIN events AS e ON e.event_id = s.llm_event_id
                WHERE e.outcome_kind IS NULL {predicate}
                GROUP BY {group}
            ),
            results AS (
                SELECT {config},
                       COUNT(DISTINCT l.outcome_event_id) AS outcomes,
                       COUNT(DISTINCT CASE WHEN LOWER(l.outcome_status) IN
                            ('passed','pass','succeeded','success','ok','green')
                            THEN l.outcome_event_id END) AS outcomes_passed,
                       COUNT(DISTINCT CASE WHEN LOWER(l.outcome_status) IN
                            ('failed','fail','failure','error','red','timeout','aborted')
                            THEN l.outcome_event_id END) AS outcomes_failed,
                       COUNT(DISTINCT l.correlation_basis) AS bases
                FROM linked AS l
                JOIN events AS e ON e.event_id = l.llm_event_id
                WHERE e.outcome_kind IS NULL {predicate} {outcome_predicate}
                GROUP BY {group}
            )
            SELECT effort.*, results.outcomes, results.outcomes_passed, results.outcomes_failed, results.bases
            FROM effort
            JOIN results USING ({using})
            ORDER BY results.outcomes DESC, effort.associated_events DESC
            LIMIT ?
            """,
            (*outcome_params, *params, *params, *outcome_params, limit),
        ).fetchall()

        ranked: list[dict[str, Any]] = []
        withheld: list[dict[str, Any]] = []
        for row in rows:
            record = {key: row[key] for key in row.keys()}
            events = int(record["associated_events"] or 0)
            outcomes = int(record["outcomes"] or 0)
            passed = int(record["outcomes_passed"] or 0)
            cost_reported = int(record.pop("cost_reported_events") or 0)
            tokens_reported = int(record.pop("tokens_reported_events") or 0)
            failed = int(record.get("outcomes_failed") or 0)
            # Only outcomes that can fail carry a success rate. A landed commit
            # is neither a pass nor a failure, and counting it as "not passed"
            # would drag every configuration toward zero and invent a precise
            # number out of a category error.
            evaluated = passed + failed
            record["outcomes_evaluated"] = evaluated
            record["outcomes_non_binary"] = max(0, outcomes - evaluated)
            record["success_rate"] = (passed / evaluated) if evaluated else None
            complete_cost = events > 0 and cost_reported == events
            record["cost_per_success"] = (
                (record["cost"] / passed) if (complete_cost and passed and record["cost"] is not None) else None
            )
            record["coverage"] = {
                "cost": _coverage_ratio(cost_reported, events),
                "tokens": _coverage_ratio(tokens_reported, events),
                "retries": _coverage_ratio(record.pop("retries_reported_events", 0), events),
                "rework_loops": _coverage_ratio(record.pop("rework_reported_events", 0), events),
                "reassessments": _coverage_ratio(record.pop("reassessments_reported_events", 0), events),
                "agent_failures": _coverage_ratio(record.pop("agent_failures_reported_events", 0), events),
            }
            record["association_only"] = True
            if evaluated < min_outcomes:
                record["evidence"] = "insufficient"
                if evaluated == 0 and outcomes:
                    record["evidence_note"] = (
                        f"{outcomes} associated outcome(s), none with a pass/fail result "
                        f"({record['outcomes_non_binary']} such as landed commits); a success "
                        "rate cannot be computed from them"
                    )
                else:
                    record["evidence_note"] = (
                        f"{evaluated} pass/fail outcome(s); at least {min_outcomes} are required "
                        "before a success rate is treated as a finding"
                    )
                withheld.append(record)
            else:
                record["evidence"] = "sufficient"
                if not complete_cost:
                    record["evidence_note"] = (
                        f"cost reported for {cost_reported} of {events} associated events; "
                        "cost_per_success withheld rather than understated"
                    )
                ranked.append(record)
        ranked.sort(
            key=lambda r: (
                -(r["success_rate"] or 0),
                r["cost_per_success"] if r["cost_per_success"] is not None else float("inf"),
            )
        )
        # A dimension the telemetry never carries produces no rows at all, which
        # an operator reads as "no results yet" rather than "this cannot be
        # answered". Measured live: skill was present on 4 of 42,577 events and
        # workflow/task_class on none, so "which skills deliver value" would have
        # returned a confident silence forever. Report per-dimension capture
        # beside the ranking so an absent answer always states its own reason.
        # One pass for every dimension: run per-dimension against a live store
        # and the denominators disagree between rows of the same report, which
        # reads as a bug in the report itself.
        counts = self.connection.execute(
            f"""
            WITH linked AS (
                SELECT DISTINCT
                    CASE WHEN a.child_event_id = o.event_id
                         THEN a.parent_event_id ELSE a.child_event_id END AS llm_event_id
                FROM attribution_edges AS a
                JOIN outcome_events AS o
                  ON o.event_id IN (a.child_event_id, a.parent_event_id)
                WHERE a.relation = 'outcome_correlation'
            )
            SELECT COUNT(*) AS associated,
                   {', '.join(f"SUM(CASE WHEN {_coverage_predicate(name)} THEN 1 ELSE 0 END) AS {name}"
                              for name in RANKING_DIMENSIONS)}
            FROM linked AS l
            JOIN events AS e ON e.event_id = l.llm_event_id
            WHERE e.outcome_kind IS NULL
            """
        ).fetchone()
        associated = int(counts["associated"] or 0)
        dimension_coverage: dict[str, Any] = {}
        for name in RANKING_DIMENSIONS:
            carried = int(counts[name] or 0)
            dimension_coverage[name] = {
                "events_with_dimension": carried,
                "associated_events": associated,
                "ratio": (carried / associated) if associated else None,
                "rankable": carried > 0,
                "note": None if carried else (
                    f"no associated event carries `{name}`, so this dimension cannot be ranked "
                    "at any sample size until the client reports it"
                ),
            }
        return {
            "schema": "observatory.engineering-value/v1",
            "min_outcomes": min_outcomes,
            "association_only": True,
            "note": "configurations are associated with outcomes through the stated basis, not shown to cause them",
            "grouped_by": list(selected),
            "dimension_coverage": dimension_coverage,
            "ranked": ranked,
            "insufficient_evidence": withheld,
        }

    def comparison(self, filters: Mapping[str, str] | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return bounded provider/model/client analytics for like-for-like comparison."""

        limit = self._bounded_limit(limit, maximum=500)
        where, params = self._where_clause(filters or {})
        rows = self.connection.execute(
            f"""
            SELECT provider, model, model_family, model_variant, client, auth_mode, route, usage_source,
                   COUNT(*) AS events,
                   SUM(CASE WHEN status IN ('ok', 'success', 'succeeded') THEN 1 ELSE 0 END) AS successes,
                   SUM(CASE WHEN status IN ('error', 'failed', 'failure') THEN 1 ELSE 0 END) AS failures,
                   SUM(input_tokens) AS input_tokens,
                   SUM(output_tokens) AS output_tokens,
                   SUM(cached_tokens) AS cached_tokens,
                   SUM(reasoning_tokens) AS reasoning_tokens,
                   SUM(total_tokens) AS total_tokens,
                   SUM(cost) AS cost,
                   AVG(latency_ms) AS average_latency_ms,
                   COALESCE(SUM(retry_count), 0) AS retries,
                   COALESCE(SUM(CASE WHEN rate_limited = 1 THEN 1 ELSE 0 END), 0) AS rate_limited,
                   COALESCE(SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END), 0) AS timeouts,
                   COALESCE(SUM(CASE WHEN tool_failure = 1 THEN 1 ELSE 0 END), 0) AS tool_failures,
                   COALESCE(SUM(CASE WHEN agent_failure = 1 THEN 1 ELSE 0 END), 0) AS agent_failures,
                   COALESCE(SUM(reassessment_count), 0) AS reassessments,
                   COALESCE(SUM(rework_count), 0) AS rework_loops
            FROM events {where}
            GROUP BY provider, model, model_family, model_variant, client, auth_mode, route, usage_source
            ORDER BY events DESC, provider, model, model_variant, client, usage_source
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

    # `outcome_kind`/`outcome_status` describe the OUTCOME. In the correlation
    # analytics they must not be applied to the LLM event, whose own outcome_kind
    # is required to be NULL -- a guaranteed contradiction that returned an empty
    # result with no error.
    _OUTCOME_SIDE_FILTERS = {"outcome_kind": "outcome_kind", "outcome_status": "outcome_status"}

    def _correlation_predicates(
        self, filters: Mapping[str, str] | None
    ) -> tuple[str, list[str], str, list[str]]:
        """Split filters into an event-side and an outcome-side predicate.

        Both are returned already qualified. An unqualified predicate injected
        into `... FROM linked AS l JOIN events AS e ...` is ambiguous for any
        column both relations expose, which made two declared filters raise
        `ambiguous column name` and surface to the caller as HTTP 503.
        """

        filters = dict(filters or {})
        outcome_clauses: list[str] = []
        outcome_params: list[str] = []
        for key in list(filters):
            column = self._OUTCOME_SIDE_FILTERS.get(key)
            if column is None:
                continue
            outcome_clauses.append(f"l.{column} = ?")
            outcome_params.append(filters.pop(key))
        where, params = self._where_clause(filters, alias="e")
        event_predicate = f"AND {where.removeprefix('WHERE ').strip()}" if where else ""
        outcome_predicate = f"AND {' AND '.join(outcome_clauses)}" if outcome_clauses else ""
        return event_predicate, params, outcome_predicate, outcome_params

    def _where_clause(self, filters: Mapping[str, str], *, alias: str = "") -> tuple[str, list[str]]:
        clauses, params = self._filter_terms(filters, alias=alias)
        return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)

    def _filter_terms(self, filters: Mapping[str, str], *, alias: str = "") -> tuple[list[str], list[str]]:
        """Validate filters into individual SQL terms and their parameters.

        Returned unjoined so a caller can splice them into a clause that is
        already a `WHERE` -- a correlated subquery, for instance -- rather than
        string-surgering a leading keyword back off.
        """

        clauses: list[str] = []
        params: list[str] = []
        normalized_ranges: dict[str, str] = {}
        prefix = f"{alias}." if alias else ""
        for key, value in filters.items():
            if key in ("start", "end"):
                operator = ">=" if key == "start" else "<="
                try:
                    normalized = ensure_utc(value, key).isoformat()
                except ContractError as exc:
                    raise ValueError(str(exc)) from exc
                clauses.append(f"{prefix}observed_at {operator} ?")
                params.append(normalized)
                normalized_ranges[key] = normalized
                continue
            column = self._FILTER_COLUMNS.get(key)
            if column is None:
                raise ValueError(f"unsupported filter: {key}")
            clauses.append(f"{prefix}{column} = ?")
            params.append(value)
        if "start" in normalized_ranges and "end" in normalized_ranges and normalized_ranges["start"] > normalized_ranges["end"]:
            raise ValueError("start must be before or equal to end")
        return (clauses, params)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if not self.read_only:
            # Without statistics SQLite guesses at join order, and the guess is
            # unstable: the same analytics query took 0.15 s filtered by client
            # and 13.5 s filtered by provider -- past the read budget, so a
            # flagship route returned 503 for one filter and not another.
            # `optimize` runs ANALYZE only on tables that actually need it, so
            # it is safe to call on every writer close.
            try:
                # Bound the wait: refreshing statistics is an optimisation and
                # must never hold a close open for the full maintenance timeout.
                self.connection.execute(f"PRAGMA busy_timeout = {CLOSE_OPTIMIZE_TIMEOUT_MS}")
                self.connection.execute("PRAGMA optimize")
            except sqlite3.Error:
                # Statistics are an optimisation; failing to refresh them must
                # never prevent a clean close.
                pass
        self.connection.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
