"""Loopback-only HTTP intake and query API."""

from __future__ import annotations

from contextlib import contextmanager
from http import HTTPStatus
import ipaddress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import socket
from pathlib import Path
import sqlite3
from threading import BoundedSemaphore, Lock, Thread
import time
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlsplit

from .intake import Intake
from .otel_bridge import OTLPJsonBridge
from .prometheus import PrometheusQueryEngine, PrometheusQueryError
from .observation import (
    DEFAULT_FRESHNESS_SECONDS,
    DEFAULT_WINDOW_SECONDS,
    observation_report,
    window_start,
)
from .store import DEFAULT_MAX_DATABASE_BYTES, EventStore


MAX_QUERY_BYTES = 64 * 1024
MAX_QUERY_FIELDS = 128
DEFAULT_READ_REQUESTS = 12
DEFAULT_READ_TIMEOUT = 2.0
# The health verdict aggregates the whole store and is an operator request,
# not a dashboard panel; a panel-sized bound made a healthy deployment 503.
# This is the budget for the WHOLE verdict, shared by every query it issues --
# not a per-query bound that a multi-query handler can spend once per read.
OBSERVATION_READ_TIMEOUT = 15.0


def extended_request_cap(max_requests: int) -> int:
    """How many long-budget reads may hold pool slots at the same time.

    A quarter of the lane, and never the whole of it: the remainder is what
    ordinary panel reads are guaranteed while slow operator requests are in
    flight. A single-slot pool has nothing to reserve, so it degrades to one.
    """

    return min(max(1, max_requests // 4), max(1, max_requests - 1))


class DashboardReadUnavailable(RuntimeError):
    """A dashboard read cannot be admitted or completed within its budget."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class _LeasedRead:
    """Run reads against one leased connection under one shared deadline.

    Every read the lease serves is charged against the same absolute deadline,
    so a handler that issues several queries spends one budget rather than a
    fresh budget per query. Once the budget is gone no further read starts.
    """

    __slots__ = ("_store", "_deadline")

    def __init__(self, store: EventStore, deadline: float) -> None:
        self._store = store
        self._deadline = deadline

    @property
    def remaining(self) -> float:
        return max(self._deadline - time.monotonic(), 0.0)

    def _expired(self) -> DashboardReadUnavailable:
        return DashboardReadUnavailable(
            "read_deadline_exceeded", "dashboard query exceeded its execution budget"
        )

    def __call__(self, operation: Callable[[EventStore], Any]) -> Any:
        if time.monotonic() >= self._deadline:
            raise self._expired()
        try:
            value = operation(self._store)
        except sqlite3.OperationalError as exc:
            if time.monotonic() >= self._deadline or "interrupted" in str(exc).casefold():
                raise self._expired() from exc
            raise
        if time.monotonic() >= self._deadline:
            raise self._expired()
        return value


class DashboardReadPool:
    """Run bounded dashboard reads on independent read-only SQLite connections."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_requests: int = DEFAULT_READ_REQUESTS,
        timeout: float = DEFAULT_READ_TIMEOUT,
        max_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
        max_extended_requests: int | None = None,
    ) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be positive")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_extended_requests is None:
            max_extended_requests = extended_request_cap(max_requests)
        if not 1 <= max_extended_requests <= max_requests:
            raise ValueError("max_extended_requests must be between 1 and max_requests")
        self.path = Path(path)
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.max_requests = max_requests
        self.max_extended_requests = max_extended_requests
        # What ordinary panel reads keep no matter how many slow operator
        # requests arrive at once. Zero only for a degenerate one-slot pool.
        self.reserved_ordinary_requests = max_requests - max_extended_requests
        self._slots = BoundedSemaphore(max_requests)
        self._extended_slots = BoundedSemaphore(max_extended_requests)

    @contextmanager
    def lease(self, *, timeout: float | None = None, extended: bool = False):
        """Hold one slot and one connection for a sequence of reads on ONE budget.

        Two independent bounds are what make a read safe here, and a slot count
        is only the first of them:

        * one slot, so no request occupies more of the lane than any other;
        * one deadline for the entire lease, so a handler that issues several
          queries cannot spend a fresh deadline per query. This is the bound
          that makes a stated timeout the real time a slot can be held.

        Holding one slot is NOT on its own a reason a longer deadline is safe:
        lane starvation is duration x concurrency, so a longer duration only
        stays safe while the number of concurrent long holders is capped.
        ``extended`` leases -- the operator health verdict, which aggregates the
        whole store and legitimately needs longer than a panel -- are therefore
        admitted through a second, smaller gate of ``max_extended_requests``,
        strictly below ``max_requests`` for any pool with more than one slot.
        That leaves ``reserved_ordinary_requests`` slots for ordinary panel
        reads however many slow verdicts arrive together, and the excess slow
        requests are refused at the gate without ever taking a lane slot.
        """

        budget = timeout if timeout and timeout > 0 else self.timeout
        if extended and not self._extended_slots.acquire(blocking=False):
            raise DashboardReadUnavailable(
                "extended_read_lane_saturated", "extended dashboard read lane is full"
            )
        try:
            if not self._slots.acquire(blocking=False):
                raise DashboardReadUnavailable("read_lane_saturated", "dashboard read lane is full")
            try:
                deadline = time.monotonic() + budget
                store = EventStore(self.path, max_bytes=self.max_bytes, read_only=True)
                try:
                    store.connection.set_progress_handler(
                        lambda: 1 if time.monotonic() >= deadline else 0,
                        1_000,
                    )
                    yield _LeasedRead(store, deadline)
                finally:
                    try:
                        store.connection.set_progress_handler(None, 0)
                    finally:
                        store.close()
            finally:
                self._slots.release()
        finally:
            if extended:
                self._extended_slots.release()

    def run(
        self,
        operation: Callable[[EventStore], Any],
        *,
        timeout: float | None = None,
        extended: bool = False,
    ) -> Any:
        """Run a single read on its own slot under its own budget."""

        with self.lease(timeout=timeout, extended=extended) as read:
            return read(operation)

def _is_loopback_address(value: str) -> bool:
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _ingest_status(result: Any) -> HTTPStatus:
    """Make malformed records visible to exporters instead of acknowledging them."""

    if getattr(result, "unavailable", 0):
        return HTTPStatus.SERVICE_UNAVAILABLE
    return HTTPStatus.BAD_REQUEST if result.rejected else HTTPStatus.OK


def _ingest_outcome(result: Any) -> str:
    if getattr(result, "unavailable", 0):
        return "degraded"
    if not result.rejected:
        return "accepted"
    return "accepted_with_rejections" if result.inserted or result.duplicate or result.conflict else "rejected"


class ObservatoryApplication:
    def __init__(
        self,
        store: EventStore,
        *,
        max_request_bytes: int = 8_388_608,
        max_records: int = 256,
        max_read_requests: int = DEFAULT_READ_REQUESTS,
        max_extended_read_requests: int | None = None,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        config_path: str | Path | None = None,
    ) -> None:
        if max_request_bytes < 1024:
            raise ValueError("max_request_bytes must be at least 1024")
        self.store = store
        self.config_path = Path(config_path) if config_path else None
        self.intake = Intake(store, max_records=max_records)
        self.otel = OTLPJsonBridge(store, max_records=max_records)
        self.prometheus = PrometheusQueryEngine(store)
        self.read_pool = DashboardReadPool(
            store.path,
            max_requests=max_read_requests,
            max_extended_requests=max_extended_read_requests,
            timeout=read_timeout,
            max_bytes=store.max_bytes,
        )
        self.max_request_bytes = max_request_bytes
        self.intake_lock = Lock()
        self.stats_lock = Lock()
        self.started_at = store.connection.execute("SELECT datetime('now')").fetchone()[0]
        self.started_monotonic = time.monotonic()
        self.ingest_batches = 0
        self.ingest_records = 0
        self.ingest_rejected = 0
        self.ingest_unavailable = 0

    def ingest_json(self, value: Any) -> tuple[int, dict[str, Any]]:
        records = value if isinstance(value, list) else [value]
        with self.intake_lock:
            result = self.intake.ingest(records)
        with self.stats_lock:
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
            self.ingest_unavailable += result.unavailable
        return _ingest_status(result), {"schema": "observatory.intake/v1", "outcome": _ingest_outcome(result), **result.to_mapping()}

    def ingest_otlp(self, signal: str, value: Any) -> tuple[int, dict[str, Any]]:
        if not isinstance(value, Mapping):
            return HTTPStatus.BAD_REQUEST, {"error": "OTLP payload must be an object"}
        with self.intake_lock:
            result = self.otel.ingest(signal, value)
        with self.stats_lock:
            self.ingest_batches += 1
            self.ingest_records += result.inserted + result.duplicate + result.conflict + result.rejected
            self.ingest_rejected += result.rejected
            self.ingest_unavailable += result.unavailable
        return _ingest_status(result), {"schema": "observatory.otlp/v1", "signal": signal, "outcome": _ingest_outcome(result), **result.to_mapping()}

    def summary(self, filters: Mapping[str, str]) -> dict[str, Any]:
        return self._read(lambda store: {"schema": "observatory.summary/v1", "filters": dict(filters), "data": store.summary(filters)})

    def events(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        return self._read(
            lambda store: {
                "schema": "observatory.events/v1",
                "count": len(values := [event.to_mapping() for event in store.list_events(filters, limit=limit)]),
                "events": values,
            }
        )

    def event_detail(self, event_id: str) -> tuple[int, dict[str, Any]]:
        value = self._read(lambda store: store.event_detail(event_id))
        if value is None:
            return HTTPStatus.NOT_FOUND, {"error": "event_not_found", "event_id": event_id}
        return HTTPStatus.OK, {"schema": "observatory.event-detail/v1", **value}

    def measurements(self, event_id: str | None, limit: int) -> dict[str, Any]:
        return self._read(
            lambda store: {
                "schema": "observatory.measurements/v1",
                "count": len(values := store.measurement_facts(event_id=event_id, limit=limit)),
                "measurements": values,
            }
        )

    def outcomes(self, event_id: str | None, limit: int) -> dict[str, Any]:
        return self._read(
            lambda store: {
                "schema": "observatory.outcomes/v1",
                "count": len(values := store.outcomes(event_id=event_id, limit=limit)),
                "outcomes": values,
            }
        )

    def attribution(self, event_id: str | None, limit: int) -> dict[str, Any]:
        return self._read(
            lambda store: {
                "schema": "observatory.attribution/v1",
                "count": len(values := store.attribution_edges(event_id=event_id, limit=limit)),
                "edges": values,
            }
        )

    def comparison(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        return self._read(
            lambda store: {
                "schema": "observatory.analytics-comparison/v1",
                "count": len(values := store.comparison(filters, limit=limit)),
                "comparisons": values,
            }
        )

    def _read(
        self,
        operation: Callable[[EventStore], Any],
        *,
        timeout: float | None = None,
        extended: bool = False,
    ) -> Any:
        if self.store.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database")
        return self.read_pool.run(operation, timeout=timeout, extended=extended)

    def _read_session(self, *, timeout: float | None = None, extended: bool = False):
        """One read lease for a handler that needs several reads to agree on a budget."""

        if self.store.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database")
        return self.read_pool.lease(timeout=timeout, extended=extended)

    def _stats_snapshot(self) -> dict[str, Any]:
        with self.stats_lock:
            return {
                "batches": self.ingest_batches,
                "records": self.ingest_records,
                "rejected": self.ingest_rejected,
                "unavailable": self.ingest_unavailable,
                # Spans that arrived carrying nothing analysis can use. Reported
                # so a client that emits only empty spans is distinguishable
                # from one that emits nothing at all -- comparing those two as
                # the same thing is exactly what the coverage rules forbid.
                "dropped_empty_spans": self.otel.dropped_empty_spans,
                "dropped_empty_by_source": dict(self.otel.dropped_empty_by_source),
            }

    def _probe_store(self) -> None:
        if self.store.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database")
        connection = sqlite3.connect(
            f"{self.store.path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=0.25,
        )
        try:
            connection.execute("SELECT 1").fetchone()
        finally:
            connection.close()

    def health(self) -> dict[str, Any]:
        ingest = self._stats_snapshot()
        capacity = self.store.capacity()
        try:
            self._probe_store()
        except (OSError, sqlite3.Error):
            return {
                "schema": "observatory.health/v1",
                "status": "degraded",
                "store": "unavailable",
                "started_at": self.started_at,
                "process_id": os.getpid(),
                "uptime_seconds": round(max(time.monotonic() - self.started_monotonic, 0.0), 3),
                "ingest": ingest,
                "store_capacity": capacity,
                "inference_path": "unmanaged/no-proxy",
            }
        status = "degraded" if capacity["exhausted"] else "ok"
        return {
            "schema": "observatory.health/v1",
            "status": status,
            "store": "ready",
            "started_at": self.started_at,
            "process_id": os.getpid(),
            "uptime_seconds": round(max(time.monotonic() - self.started_monotonic, 0.0), 3),
            "ingest": ingest,
            "store_capacity": capacity,
            "inference_path": "unmanaged/no-proxy",
        }

    def outcome_value(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        """Spend and effort associated with observed engineering outcomes."""

        return self._read(
            lambda store: {
                "schema": "observatory.outcome-value/v1",
                "filters": dict(filters),
                "association_only": True,
                "note": "rows report association through the stated correlation basis, not causation",
                "data": store.outcome_value(filters, limit=limit),
            }
        )

    def engineering_value(self, filters: Mapping[str, str], limit: int) -> dict[str, Any]:
        """Ranked configurations by validated outcome relative to effort."""

        return self._read(lambda store: store.engineering_value(filters, limit=limit))

    def observation_reliability(self, *, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> dict[str, Any]:
        """Recorded verdict history plus the gaps in it.

        A gap in the record is itself a finding: an interval with no snapshot
        means nothing was checking, which is a different failure from a
        recorded degradation and must not be read as "no problems".
        """

        return self._read(
            lambda store: {
                "schema": "observatory.observation-history/v1",
                "history": store.observation_history(since=window_start(window_seconds), limit=500),
                "reliability": store.observation_gaps(since=window_start(window_seconds)),
                "coverage_drift": store.coverage_drift(since=window_start(window_seconds)),
            }
        )

    def observation(
        self,
        *,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        freshness_seconds: int = DEFAULT_FRESHNESS_SECONDS,
    ) -> dict[str, Any]:
        """Whether this deployment is actually observing, with evidence.

        Derived from what the store has received rather than from service
        health, because every service can report healthy while nothing at all is
        being persisted.
        """

        managed: dict[str, Any] = {}
        if self.config_path is not None:
            try:
                config = json.loads(self.config_path.read_text(encoding="utf-8"))
                if isinstance(config, Mapping):
                    owned = config.get("managed_clients")
                    if isinstance(owned, Mapping):
                        managed = dict(owned)
            except (OSError, json.JSONDecodeError):
                # Ownership is context, not a precondition; a missing manifest
                # must not stop the verdict from being reported.
                managed = {}
        try:
            # One lease, so all three queries share a single deadline and a
            # single slot: the verdict costs the lane one OBSERVATION_READ_TIMEOUT,
            # not one per query. The lease is extended, so concurrent verdicts
            # are capped well below the pool and cannot crowd out panel reads.
            with self._read_session(timeout=OBSERVATION_READ_TIMEOUT, extended=True) as read:
                samples = read(lambda store: store.observation_samples(window_start(window_seconds)))
                recent = read(lambda store: store.observation_samples(window_start(freshness_seconds)))
                lifetime = read(lambda store: store.client_lifetime())
            health: dict[str, Any] = {"reachable": True, **self.store.capacity()}
        except DashboardReadUnavailable as exc:
            # Read-lane saturation is not an unreachable store. Reporting it as
            # one produced the blocker "nothing can be persisted" while intake
            # was healthy -- a false alarm about the wrong subsystem.
            samples = {}
            recent = {}
            lifetime = {}
            health = {"reachable": True, "degraded": True, "read_lane": str(exc)}
        except Exception as exc:  # store unreachable is itself the finding
            samples = {}
            recent = {}
            lifetime = {}
            health = {"reachable": False, "error": str(exc)}
        return observation_report(
            samples,
            recent_samples=recent,
            lifetime_totals=lifetime,
            configured_clients=managed,
            store_health=health,
            window_seconds=window_seconds,
            freshness_seconds=freshness_seconds,
        )

    def metrics(self) -> str:
        stats = self._stats_snapshot()
        return self._read(lambda store: self._metrics_from_store(store, stats))

    def _metrics_from_store(self, store: EventStore, stats: Mapping[str, int]) -> str:
        summary = store.summary()
        conflicts = store.conflict_count()
        # Keep Prometheus bounded while making the default dashboard catalog
        # large enough that normal multi-project installations do not hide
        # dimensions behind the old top-100 cutoff.
        dimensions = store.metric_dimensions(limit=500)
        ingest_batches = stats["batches"]
        ingest_records = stats["records"]
        ingest_rejected = stats["rejected"]
        ingest_unavailable = stats["unavailable"]
        capacity = store.capacity()
        lines = [
            "# HELP observatory_process_ready Whether the normalizer process and store are ready.",
            "# TYPE observatory_process_ready gauge",
            f"observatory_process_ready {0 if capacity['exhausted'] else 1}",
            "# HELP observatory_process_uptime_seconds Normalizer process uptime in seconds.",
            "# TYPE observatory_process_uptime_seconds gauge",
            f"observatory_process_uptime_seconds {max(time.monotonic() - self.started_monotonic, 0.0)}",
            "# HELP observatory_ingest_batches_total Intake batches seen by this process.",
            "# TYPE observatory_ingest_batches_total counter",
            f"observatory_ingest_batches_total {ingest_batches}",
            "# HELP observatory_ingest_records_total Intake records seen by this process.",
            "# TYPE observatory_ingest_records_total counter",
            f"observatory_ingest_records_total {ingest_records}",
            "# HELP observatory_ingest_rejected_total Intake records rejected by this process.",
            "# TYPE observatory_ingest_rejected_total counter",
            f"observatory_ingest_rejected_total {ingest_rejected}",
            "# HELP observatory_ingest_unavailable_total Intake records rejected because the normalized store was unavailable or at capacity.",
            "# TYPE observatory_ingest_unavailable_total counter",
            f"observatory_ingest_unavailable_total {ingest_unavailable}",
            "# HELP observatory_store_bytes Current SQLite database and WAL sidecar bytes.",
            "# TYPE observatory_store_bytes gauge",
            f"observatory_store_bytes {capacity['bytes']}",
            "# HELP observatory_store_capacity_bytes Configured normalized store byte budget.",
            "# TYPE observatory_store_capacity_bytes gauge",
            f"observatory_store_capacity_bytes {_sample(capacity['max_bytes'])}",
            "# HELP observatory_store_capacity_ratio Current normalized store bytes divided by its configured budget.",
            "# TYPE observatory_store_capacity_ratio gauge",
            f"observatory_store_capacity_ratio {_sample(capacity['ratio'])}",
            "# HELP observatory_events_total Canonical normalized events accepted by the local store.",
            "# TYPE observatory_events_total gauge",
            f"observatory_events_total {summary['events']}",
            "# HELP observatory_event_conflicts_total Conflicting replays retained for diagnosis.",
            "# TYPE observatory_event_conflicts_total gauge",
            f"observatory_event_conflicts_total {conflicts}",
            "# HELP observatory_event_successes_total Successful normalized operations.",
            "# TYPE observatory_event_successes_total gauge",
            f"observatory_event_successes_total {summary['successes']}",
            "# HELP observatory_event_failures_total Failed normalized operations.",
            "# TYPE observatory_event_failures_total gauge",
            f"observatory_event_failures_total {summary['failures']}",
            "# HELP observatory_ingest_ledger_entries_total Append-only intake attempts retained for audit.",
            "# TYPE observatory_ingest_ledger_entries_total gauge",
            f"observatory_ingest_ledger_entries_total {store.ledger_count()}",
            "# HELP observatory_measurement_facts_total Field-level evidence facts retained.",
            "# TYPE observatory_measurement_facts_total gauge",
            f"observatory_measurement_facts_total {store.measurement_count()}",
            "# HELP observatory_outcomes_total Correlated outcome observations retained.",
            "# TYPE observatory_outcomes_total gauge",
            f"observatory_outcomes_total {store.outcome_count()}",
            "# HELP observatory_input_tokens_total Total input tokens reported in normalized events.",
            "# TYPE observatory_input_tokens_total gauge",
            f"observatory_input_tokens_total {_sample(summary['input_tokens'])}",
            "# HELP observatory_output_tokens_total Total output tokens reported in normalized events.",
            "# TYPE observatory_output_tokens_total gauge",
            f"observatory_output_tokens_total {_sample(summary['output_tokens'])}",
            "# HELP observatory_cached_tokens_total Total cached tokens reported in normalized events.",
            "# TYPE observatory_cached_tokens_total gauge",
            f"observatory_cached_tokens_total {_sample(summary['cached_tokens'])}",
            "# HELP observatory_cache_creation_tokens_total Total cache-creation tokens reported in normalized events.",
            "# TYPE observatory_cache_creation_tokens_total gauge",
            f"observatory_cache_creation_tokens_total {_sample(summary['cache_creation_tokens'])}",
            "# HELP observatory_cache_read_tokens_total Total cache-read tokens reported in normalized events.",
            "# TYPE observatory_cache_read_tokens_total gauge",
            f"observatory_cache_read_tokens_total {_sample(summary['cache_read_tokens'])}",
            "# HELP observatory_reasoning_tokens_total Total reasoning tokens reported in normalized events.",
            "# TYPE observatory_reasoning_tokens_total gauge",
            f"observatory_reasoning_tokens_total {_sample(summary['reasoning_tokens'])}",
            "# HELP observatory_compactions_total Total context compaction observations.",
            "# TYPE observatory_compactions_total gauge",
            f"observatory_compactions_total {_sample(summary['compactions'])}",
            "# HELP observatory_cost_total Total reported cost in the source currency or unit.",
            "# TYPE observatory_cost_total gauge",
            f"observatory_cost_total {_sample(summary['cost'])}",
            "# HELP observatory_latency_average_ms Average reported operation latency in milliseconds.",
            "# TYPE observatory_latency_average_ms gauge",
            f"observatory_latency_average_ms {_sample(summary['average_latency_ms'])}",
            "# HELP observatory_time_to_first_token_average_ms Average time to first token in milliseconds.",
            "# TYPE observatory_time_to_first_token_average_ms gauge",
            f"observatory_time_to_first_token_average_ms {_sample(summary['average_time_to_first_token_ms'])}",
            "# HELP observatory_duration_average_ms Average total generation duration in milliseconds.",
            "# TYPE observatory_duration_average_ms gauge",
            f"observatory_duration_average_ms {_sample(summary['average_duration_ms'])}",
            "# HELP observatory_context_size_average Average reported context size in tokens.",
            "# TYPE observatory_context_size_average gauge",
            f"observatory_context_size_average {_sample(summary['average_context_size'])}",
            "# HELP observatory_context_utilization_average Average reported context utilization ratio.",
            "# TYPE observatory_context_utilization_average gauge",
            f"observatory_context_utilization_average {_sample(summary['average_context_utilization'])}",
            "# HELP observatory_concurrency_average Average reported concurrency.",
            "# TYPE observatory_concurrency_average gauge",
            f"observatory_concurrency_average {_sample(summary['average_concurrency'])}",
            "# HELP observatory_parallel_utilization_average Average reported parallel utilization ratio.",
            "# TYPE observatory_parallel_utilization_average gauge",
            f"observatory_parallel_utilization_average {_sample(summary['average_parallel_utilization'])}",
            "# HELP observatory_retries_total Total retry attempts reported in normalized events.",
            "# TYPE observatory_retries_total gauge",
            f"observatory_retries_total {summary['retries']}",
            "# HELP observatory_rate_limited_total Total rate-limited operations reported in normalized events.",
            "# TYPE observatory_rate_limited_total gauge",
            f"observatory_rate_limited_total {summary['rate_limited']}",
            "# HELP observatory_timeouts_total Total timeout operations reported in normalized events.",
            "# TYPE observatory_timeouts_total gauge",
            f"observatory_timeouts_total {summary['timeouts']}",
            "# HELP observatory_tool_failures_total Total tool failures reported in normalized events.",
            "# TYPE observatory_tool_failures_total gauge",
            f"observatory_tool_failures_total {summary['tool_failures']}",
            "# HELP observatory_agent_failures_total Total agent failures reported in normalized events.",
            "# TYPE observatory_agent_failures_total gauge",
            f"observatory_agent_failures_total {summary['agent_failures']}",
            "# HELP observatory_reassessments_total Total reassessment-loop observations reported in normalized events.",
            "# TYPE observatory_reassessments_total gauge",
            f"observatory_reassessments_total {_sample(summary['reassessments'])}",
            "# HELP observatory_rework_loops_total Total rework-loop observations reported in normalized events.",
            "# TYPE observatory_rework_loops_total gauge",
            f"observatory_rework_loops_total {_sample(summary['rework_loops'])}",
            "# HELP observatory_tool_calls_total Total bounded tool-call observations.",
            "# TYPE observatory_tool_calls_total gauge",
            f"observatory_tool_calls_total {_sample(summary['tool_calls'])}",
            "# HELP observatory_files_inspected_total Total bounded file-inspection observations.",
            "# TYPE observatory_files_inspected_total gauge",
            f"observatory_files_inspected_total {_sample(summary['files_inspected'])}",
            "# HELP observatory_files_changed_total Total bounded file-change observations.",
            "# TYPE observatory_files_changed_total gauge",
            f"observatory_files_changed_total {_sample(summary['files_changed'])}",
            "# HELP observatory_commands_executed_total Total bounded command-execution observations.",
            "# TYPE observatory_commands_executed_total gauge",
            f"observatory_commands_executed_total {_sample(summary['commands_executed'])}",
            "# HELP observatory_tests_invoked_total Total bounded test-invocation observations.",
            "# TYPE observatory_tests_invoked_total gauge",
            f"observatory_tests_invoked_total {_sample(summary['tests_invoked'])}",
        ]
        lines.extend([
            "# HELP observatory_events_by_provider_model_total Model operations by bounded project, provider, model, family, variant, client, auth, route, and task dimensions.",
            "# TYPE observatory_events_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"project": item["project"], "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"], "task_class": item["task_class"]})
            lines.append(f"observatory_events_by_provider_model_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_success_rate_by_provider_model Operation success ratio by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_success_rate_by_provider_model gauge",
            "# HELP observatory_tokens_by_provider_model_total Total tokens by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_tokens_by_provider_model_total gauge",
            "# HELP observatory_cost_by_provider_model Reported cost by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_cost_by_provider_model gauge",
            "# HELP observatory_latency_average_by_provider_model_ms Average latency by bounded provider/model/variant/client dimensions.",
            "# TYPE observatory_latency_average_by_provider_model_ms gauge",
            "# HELP observatory_retries_by_provider_model_total Retry attempts by bounded provider/model/client dimensions.",
            "# TYPE observatory_retries_by_provider_model_total gauge",
            "# HELP observatory_rate_limited_by_provider_model_total Rate-limited operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_rate_limited_by_provider_model_total gauge",
            "# HELP observatory_timeouts_by_provider_model_total Timeout operations by bounded provider/model/client dimensions.",
            "# TYPE observatory_timeouts_by_provider_model_total gauge",
            "# HELP observatory_tool_failures_by_provider_model_total Tool failures by bounded provider/model/client dimensions.",
            "# TYPE observatory_tool_failures_by_provider_model_total gauge",
            "# HELP observatory_agent_failures_by_provider_model_total Agent failures by bounded provider/model/client dimensions.",
            "# TYPE observatory_agent_failures_by_provider_model_total gauge",
            "# HELP observatory_reassessments_by_provider_model_total Reassessment loops by bounded provider/model/client dimensions.",
            "# TYPE observatory_reassessments_by_provider_model_total gauge",
            "# HELP observatory_rework_loops_by_provider_model_total Rework loops by bounded provider/model/client dimensions.",
            "# TYPE observatory_rework_loops_by_provider_model_total gauge",
            "# HELP observatory_tool_calls_by_provider_model_total Tool calls by bounded provider/model/client dimensions.",
            "# TYPE observatory_tool_calls_by_provider_model_total gauge",
            "# HELP observatory_files_changed_by_provider_model_total File changes by bounded provider/model/client dimensions.",
            "# TYPE observatory_files_changed_by_provider_model_total gauge",
        ])
        for item in dimensions["provider_model"]:
            labels = _labels({"project": item["project"], "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"], "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"], "task_class": item["task_class"]})
            count = item["count"] or 0
            success_rate = (item["successes"] or 0) / count if count else 0
            lines.append(f"observatory_success_rate_by_provider_model{{{labels}}} {success_rate}")
            lines.append(f"observatory_tokens_by_provider_model_total{{{labels}}} {_sample(item['total_tokens'])}")
            lines.append(f"observatory_cost_by_provider_model{{{labels}}} {_sample(item['cost'])}")
            lines.append(f"observatory_latency_average_by_provider_model_ms{{{labels}}} {_sample(item['average_latency_ms'])}")
            lines.append(f"observatory_retries_by_provider_model_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_provider_model_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_provider_model_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_provider_model_total{{{labels}}} {item['tool_failures'] or 0}")
            lines.append(f"observatory_agent_failures_by_provider_model_total{{{labels}}} {item['agent_failures'] or 0}")
            lines.append(f"observatory_reassessments_by_provider_model_total{{{labels}}} {_sample(item['reassessments'])}")
            lines.append(f"observatory_rework_loops_by_provider_model_total{{{labels}}} {_sample(item['rework_loops'])}")
            lines.append(f"observatory_tool_calls_by_provider_model_total{{{labels}}} {_sample(item['tool_calls'])}")
            lines.append(f"observatory_files_changed_by_provider_model_total{{{labels}}} {_sample(item['files_changed'])}")
        lines.extend([
            "# HELP observatory_events_by_context_total Observed events by event type and bounded project, repository, branch, provider, model, variant, client, execution, workflow, task, and status context.",
            "# TYPE observatory_events_by_context_total gauge",
            "# HELP observatory_input_tokens_by_context_total Input tokens by bounded event context.",
            "# TYPE observatory_input_tokens_by_context_total gauge",
            "# HELP observatory_output_tokens_by_context_total Output tokens by bounded event context.",
            "# TYPE observatory_output_tokens_by_context_total gauge",
            "# HELP observatory_cached_tokens_by_context_total Cached tokens by bounded event context.",
            "# TYPE observatory_cached_tokens_by_context_total gauge",
            "# HELP observatory_reasoning_tokens_by_context_total Reasoning tokens by bounded event context.",
            "# TYPE observatory_reasoning_tokens_by_context_total gauge",
            "# HELP observatory_tokens_by_context_total Total tokens by bounded event context.",
            "# TYPE observatory_tokens_by_context_total gauge",
            "# HELP observatory_cache_creation_tokens_by_context_total Cache-creation tokens by bounded event context.",
            "# TYPE observatory_cache_creation_tokens_by_context_total gauge",
            "# HELP observatory_cache_read_tokens_by_context_total Cache-read tokens by bounded event context.",
            "# TYPE observatory_cache_read_tokens_by_context_total gauge",
            "# HELP observatory_compactions_by_context_total Context compactions by bounded event context.",
            "# TYPE observatory_compactions_by_context_total gauge",
            "# HELP observatory_cost_by_context Reported cost by bounded event context.",
            "# TYPE observatory_cost_by_context gauge",
            "# HELP observatory_latency_average_by_context_ms Average latency by bounded event context.",
            "# TYPE observatory_latency_average_by_context_ms gauge",
            "# HELP observatory_time_to_first_token_average_by_context_ms Average time to first token by bounded event context.",
            "# TYPE observatory_time_to_first_token_average_by_context_ms gauge",
            "# HELP observatory_duration_average_by_context_ms Average duration by bounded event context.",
            "# TYPE observatory_duration_average_by_context_ms gauge",
            "# HELP observatory_context_size_average_by_context Average context size by bounded event context.",
            "# TYPE observatory_context_size_average_by_context gauge",
            "# HELP observatory_context_utilization_average_by_context Average context utilization by bounded event context.",
            "# TYPE observatory_context_utilization_average_by_context gauge",
            "# HELP observatory_concurrency_average_by_context Average concurrency by bounded event context.",
            "# TYPE observatory_concurrency_average_by_context gauge",
            "# HELP observatory_parallel_utilization_average_by_context Average parallel utilization by bounded event context.",
            "# TYPE observatory_parallel_utilization_average_by_context gauge",
            "# HELP observatory_retries_by_context_total Retry attempts by bounded event context.",
            "# TYPE observatory_retries_by_context_total gauge",
            "# HELP observatory_rate_limited_by_context_total Rate-limited operations by bounded event context.",
            "# TYPE observatory_rate_limited_by_context_total gauge",
            "# HELP observatory_timeouts_by_context_total Timeout operations by bounded event context.",
            "# TYPE observatory_timeouts_by_context_total gauge",
            "# HELP observatory_tool_failures_by_context_total Tool failures by bounded event context.",
            "# TYPE observatory_tool_failures_by_context_total gauge",
            "# HELP observatory_agent_failures_by_context_total Agent failures by bounded event context.",
            "# TYPE observatory_agent_failures_by_context_total gauge",
            "# HELP observatory_reassessments_by_context_total Reassessment loops by bounded event context.",
            "# TYPE observatory_reassessments_by_context_total gauge",
            "# HELP observatory_rework_loops_by_context_total Rework loops by bounded event context.",
            "# TYPE observatory_rework_loops_by_context_total gauge",
            "# HELP observatory_tool_calls_by_context_total Tool calls by bounded event context.",
            "# TYPE observatory_tool_calls_by_context_total gauge",
            "# HELP observatory_files_inspected_by_context_total File inspections by bounded event context.",
            "# TYPE observatory_files_inspected_by_context_total gauge",
            "# HELP observatory_files_changed_by_context_total File changes by bounded event context.",
            "# TYPE observatory_files_changed_by_context_total gauge",
            "# HELP observatory_commands_executed_by_context_total Command executions by bounded event context.",
            "# TYPE observatory_commands_executed_by_context_total gauge",
            "# HELP observatory_tests_invoked_by_context_total Test invocations by bounded event context.",
            "# TYPE observatory_tests_invoked_by_context_total gauge",
        ])
        for item in dimensions["context"]:
            labels = _labels({
                "event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"],
                "provider": item["provider"], "model": item["model"], "model_family": item["model_family"], "model_variant": item["model_variant"],
                "client": item["client"], "auth_mode": item["auth_mode"], "route": item["route"], "usage_source": item["usage_source"],
                "agent": item["agent"], "subagent": item["subagent"], "parent_agent": item["parent_agent"], "role": item["role"],
                "skill": item["skill"], "lane": item["lane"], "workflow": item["workflow"],
                "task_class": item["task_class"], "status": item["status"],
            })
            lines.append(f"observatory_events_by_context_total{{{labels}}} {item['count'] or 0}")
            lines.append(f"observatory_input_tokens_by_context_total{{{labels}}} {_sample(item['input_tokens'])}")
            lines.append(f"observatory_output_tokens_by_context_total{{{labels}}} {_sample(item['output_tokens'])}")
            lines.append(f"observatory_cached_tokens_by_context_total{{{labels}}} {_sample(item['cached_tokens'])}")
            lines.append(f"observatory_reasoning_tokens_by_context_total{{{labels}}} {_sample(item['reasoning_tokens'])}")
            lines.append(f"observatory_tokens_by_context_total{{{labels}}} {_sample(item['total_tokens'])}")
            lines.append(f"observatory_cache_creation_tokens_by_context_total{{{labels}}} {_sample(item['cache_creation_tokens'])}")
            lines.append(f"observatory_cache_read_tokens_by_context_total{{{labels}}} {_sample(item['cache_read_tokens'])}")
            lines.append(f"observatory_compactions_by_context_total{{{labels}}} {_sample(item['compactions'])}")
            lines.append(f"observatory_cost_by_context{{{labels}}} {_sample(item['cost'])}")
            lines.append(f"observatory_latency_average_by_context_ms{{{labels}}} {_sample(item['average_latency_ms'])}")
            lines.append(f"observatory_time_to_first_token_average_by_context_ms{{{labels}}} {_sample(item['average_time_to_first_token_ms'])}")
            lines.append(f"observatory_duration_average_by_context_ms{{{labels}}} {_sample(item['average_duration_ms'])}")
            lines.append(f"observatory_context_size_average_by_context{{{labels}}} {_sample(item['average_context_size'])}")
            lines.append(f"observatory_context_utilization_average_by_context{{{labels}}} {_sample(item['average_context_utilization'])}")
            lines.append(f"observatory_concurrency_average_by_context{{{labels}}} {_sample(item['average_concurrency'])}")
            lines.append(f"observatory_parallel_utilization_average_by_context{{{labels}}} {_sample(item['average_parallel_utilization'])}")
            lines.append(f"observatory_retries_by_context_total{{{labels}}} {item['retries'] or 0}")
            lines.append(f"observatory_rate_limited_by_context_total{{{labels}}} {item['rate_limited'] or 0}")
            lines.append(f"observatory_timeouts_by_context_total{{{labels}}} {item['timeouts'] or 0}")
            lines.append(f"observatory_tool_failures_by_context_total{{{labels}}} {item['tool_failures'] or 0}")
            lines.append(f"observatory_agent_failures_by_context_total{{{labels}}} {item['agent_failures'] or 0}")
            lines.append(f"observatory_reassessments_by_context_total{{{labels}}} {_sample(item['reassessments'])}")
            lines.append(f"observatory_rework_loops_by_context_total{{{labels}}} {_sample(item['rework_loops'])}")
            lines.append(f"observatory_tool_calls_by_context_total{{{labels}}} {_sample(item['tool_calls'])}")
            lines.append(f"observatory_files_inspected_by_context_total{{{labels}}} {_sample(item['files_inspected'])}")
            lines.append(f"observatory_files_changed_by_context_total{{{labels}}} {_sample(item['files_changed'])}")
            lines.append(f"observatory_commands_executed_by_context_total{{{labels}}} {_sample(item['commands_executed'])}")
            lines.append(f"observatory_tests_invoked_by_context_total{{{labels}}} {_sample(item['tests_invoked'])}")
        lines.extend([
            "# HELP observatory_events_by_usage_source_total Observed events grouped by usage evidence source.",
            "# TYPE observatory_events_by_usage_source_total gauge",
        ])
        for item in dimensions["usage_source"]:
            lines.append(f"observatory_events_by_usage_source_total{{source=\"{_label(item['source'])}\"}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_project_total Observed events grouped by pseudonymous project identity.",
            "# TYPE observatory_events_by_project_total gauge",
        ])
        for item in dimensions["project"]:
            lines.append(f"observatory_events_by_project_total{{project=\"{_label(item['project'])}\"}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_client_route_total Observed events by client, route, and auth mode.",
            "# TYPE observatory_events_by_client_route_total gauge",
        ])
        for item in dimensions["client_route"]:
            labels = _labels({"project": item["project"], "client": item["client"], "route": item["route"], "auth_mode": item["auth_mode"]})
            lines.append(f"observatory_events_by_client_route_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_execution_total Observed events by event type and bounded project, repository, branch, role, skill, and lane dimensions.",
            "# TYPE observatory_events_by_execution_total gauge",
        ])
        for item in dimensions["execution"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "role": item["role"], "skill": item["skill"], "lane": item["lane"]})
            lines.append(f"observatory_events_by_execution_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_workflow_total Observed events by event type and bounded project, repository, branch, and workflow identity.",
            "# TYPE observatory_events_by_workflow_total gauge",
        ])
        for item in dimensions["workflow"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "workflow": item["workflow"]})
            lines.append(f"observatory_events_by_workflow_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_events_by_agent_total Observed events by event type and bounded project, repository, branch, agent, and subagent identity.",
            "# TYPE observatory_events_by_agent_total gauge",
        ])
        for item in dimensions["agent"]:
            labels = _labels({"event_type": item["event_type"], "project": item["project"], "repository": item["repository"], "branch": item["branch"], "agent": item["agent"], "subagent": item["subagent"], "parent_agent": item["parent_agent"]})
            lines.append(f"observatory_events_by_agent_total{{{labels}}} {item['count']}")
        lines.extend([
            "# HELP observatory_outcomes_by_kind_status_total Observed outcomes by kind, status, evidence source, project, and correlation basis.",
            "# TYPE observatory_outcomes_by_kind_status_total gauge",
        ])
        for item in dimensions["outcome"]:
            labels = _labels({
                "kind": item["kind"],
                "status": item["status"],
                "evidence_source": item["evidence_source"],
                "project": item["project"],
                "repository": item["repository"],
                "branch": item["branch"],
                "correlation_basis": item["correlation_basis"],
            })
            lines.append(f"observatory_outcomes_by_kind_status_total{{{labels}}} {item['count']}")
        return "\n".join(lines) + "\n"

    def prometheus_api(self, path: str, params: Mapping[str, list[str]]) -> dict[str, Any]:
        return self._read(lambda store: self._prometheus_api_unlocked(path, params, PrometheusQueryEngine(store)))

    def _prometheus_api_unlocked(
        self,
        path: str,
        params: Mapping[str, list[str]],
        prometheus: PrometheusQueryEngine | None = None,
    ) -> dict[str, Any]:
        """Serve the bounded event-time Prometheus compatibility surface."""

        prometheus = prometheus or self.prometheus
        if path == "/api/v1/query":
            query = _prometheus_param(params, "query")
            return prometheus.query(query, params)
        if path == "/api/v1/query_range":
            query = _prometheus_param(params, "query")
            return prometheus.query_range(query, params)
        if path == "/api/v1/labels":
            return {"status": "success", "data": prometheus.labels()}
        if path == "/api/v1/metadata":
            return prometheus.metadata()
        if path == "/api/v1/status/buildinfo":
            return {
                "status": "success",
                "data": {"version": "observatory-event-facade/v1", "revision": "local", "branch": "local"},
            }
        if path == "/api/v1/series":
            selectors = params.get("match[]", [])
            if not selectors:
                raise PrometheusQueryError("series requires at least one match[] selector")
            return {"status": "success", "data": prometheus.series(selectors, params)}
        label_prefix = "/api/v1/label/"
        if path.startswith(label_prefix) and path.endswith("/values"):
            label = path[len(label_prefix) : -len("/values")]
            if not label or "/" in label:
                raise PrometheusQueryError("invalid Prometheus label path")
            matchers = params.get("match[]", [])
            metric_names = []
            for selector in matchers:
                metric_names.append(selector.split("{", 1)[0].strip())
            return {
                "status": "success",
                "data": prometheus.label_values(label, metric_names or None, params),
            }
        raise PrometheusQueryError("unsupported Prometheus API path")


def _parse_query(query: str, *, keep_blank_values: bool = False) -> dict[str, list[str]]:
    if len(query.encode("utf-8")) > MAX_QUERY_BYTES:
        raise ValueError(f"query exceeds {MAX_QUERY_BYTES} bytes")
    try:
        return parse_qs(
            query,
            keep_blank_values=keep_blank_values,
            max_num_fields=MAX_QUERY_FIELDS,
        )
    except ValueError as exc:
        raise ValueError(f"query contains more than {MAX_QUERY_FIELDS} fields") from exc


def _query_filters(query: str) -> dict[str, str]:
    allowed = {
        "project", "project_id", "repository", "provider", "model", "model_family", "model_variant", "client", "auth_mode", "route", "trace_id", "span_id",
        "event_type", "status", "evidence_source", "branch", "commit", "worktree", "session_id",
        "workflow_id", "agent_id", "subagent_id", "parent_agent_id", "parent_agent", "role", "skill", "lane", "outcome_kind",
        "outcome_status", "task_id", "task_class", "usage_source", "start", "end",
    }
    parsed = _parse_query(query, keep_blank_values=False)
    filters: dict[str, str] = {}
    for key, values in parsed.items():
        if key == "limit":
            continue
        if key not in allowed:
            raise ValueError(f"unsupported filter: {key}")
        if len(values) != 1 or len(values[0]) > 256:
            raise ValueError(f"invalid filter: {key}")
        filters[key] = values[0]
    return filters


def _query_limit(query: str, *, default: int = 100, maximum: int = 1000) -> int:
    values = _parse_query(query, keep_blank_values=False).get("limit", [str(default)])
    if len(values) != 1:
        raise ValueError("limit must occur once")
    try:
        limit = int(values[0])
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    if limit < 1 or limit > maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return limit


def _query_event_id(query: str) -> str | None:
    values = _parse_query(query, keep_blank_values=False).get("event_id")
    if not values:
        return None
    if len(values) != 1 or not values[0] or len(values[0]) > 256:
        raise ValueError("event_id must occur once and be 1..256 characters")
    return values[0]


def _query_evidence_endpoint(query: str, *, maximum: int = 5000) -> tuple[str | None, int]:
    parsed = _parse_query(query, keep_blank_values=False)
    unsupported = set(parsed) - {"event_id", "limit"}
    if unsupported:
        raise ValueError(f"unsupported query parameter: {sorted(unsupported)[0]}")
    return _query_event_id(query), _query_limit(query, maximum=maximum)


def _label(value: Any) -> str:
    text = str(value if value is not None else "unknown")
    text = text[:128]
    return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


def _labels(values: Mapping[str, Any]) -> str:
    return ",".join(f'{key}="{_label(value)}"' for key, value in values.items())


def _sample(value: Any) -> str:
    """Render missing numeric observations as NaN rather than false zeroes."""

    return "NaN" if value is None else str(value)


def _prometheus_param(params: Mapping[str, list[str]], name: str) -> str:
    values = params.get(name, [])
    if len(values) != 1 or not values[0]:
        raise PrometheusQueryError(f"Prometheus parameter {name} must occur exactly once")
    if len(values[0]) > 64_000:
        raise PrometheusQueryError(f"Prometheus parameter {name} is too large")
    return values[0]


class _Handler(BaseHTTPRequestHandler):
    server: "ObservatoryHTTPServer"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(self.server.request_timeout)

    def _send_json(self, status: int, value: Mapping[str, Any], *, headers: Mapping[str, str] | None = None) -> None:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, value: str, content_type: str) -> None:
        body = value.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _require_authentication(self) -> bool:
        """Require the configured bearer token without exposing its value."""

        expected = self.server.auth_token
        if expected is None or (
            self.server.trust_loopback
            and self.client_address
            and _is_loopback_address(self.client_address[0])
        ):
            return True
        header = self.headers.get("Authorization", "")
        scheme, separator, presented = header.partition(" ")
        if separator and scheme.casefold() == "bearer" and presented and secrets.compare_digest(presented, expected):
            return True
        self.close_connection = True
        body = b'{"error":"authentication_required"}'
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        return False

    def _send_read_unavailable(self, error: DashboardReadUnavailable, *, prometheus: bool = False) -> None:
        if prometheus:
            value: Mapping[str, Any] = {
                "status": "error",
                "errorType": "unavailable",
                "error": "dashboard_query_unavailable",
                "reason": error.reason,
            }
        else:
            value = {"error": "dashboard_query_unavailable", "reason": error.reason}
        self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, value, headers={"Retry-After": "1"})

    def _plane_allows(self, path: str, method: str) -> bool:
        plane = self.server.plane
        if plane == "combined":
            return True
        if plane == "control":
            return path in ("/healthz", "/readyz") or (
                method == "POST" and path in ("/v1/events", "/v1/traces", "/v1/metrics", "/v1/logs")
            )
        if plane == "read":
            if method != "GET":
                return False
            return path == "/readz" or path == "/metrics" or path.startswith("/api/v1/") or path in {
                "/v1/summary",
                "/v1/events",
                "/v1/measurements",
                "/v1/outcomes",
                "/v1/attribution",
                "/v1/analytics/comparison",
                "/v1/analytics/engineering-value",
                "/v1/analytics/outcome-value",
                "/v1/observation",
                "/v1/observation/history",
            } or path.startswith("/v1/events/")
        return False

    def _handle_prometheus(self, path: str, params: Mapping[str, list[str]]) -> None:
        try:
            self._send_json(HTTPStatus.OK, self.server.application.prometheus_api(path, params))
        except DashboardReadUnavailable as exc:
            self._send_read_unavailable(exc, prometheus=True)
        except sqlite3.Error:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                "status": "error",
                "errorType": "unavailable",
                "error": "store_unavailable",
            })
        except (PrometheusQueryError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {
                "status": "error",
                "errorType": "bad_data",
                "error": str(exc),
            })

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if not self._plane_allows(parsed.path, "GET"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if parsed.path not in ("/healthz", "/readyz") and not self._require_authentication():
            return
        try:
            if parsed.path.startswith("/api/v1/"):
                try:
                    params = _parse_query(parsed.query, keep_blank_values=True)
                except ValueError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {
                        "status": "error",
                        "errorType": "bad_data",
                        "error": str(exc),
                    })
                    return
                self._handle_prometheus(parsed.path, params)
                return
            if parsed.path.startswith("/v1/events/"):
                event_id = parsed.path.removeprefix("/v1/events/")
                if not event_id or "/" in event_id or len(event_id) > 256:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_event_id"})
                    return
                status, value = self.server.application.event_detail(event_id)
                self._send_json(status, value)
                return
            if parsed.path in ("/healthz", "/readyz"):
                health = self.server.application.health()
                status = HTTPStatus.SERVICE_UNAVAILABLE if parsed.path == "/readyz" and health.get("status") != "ok" else HTTPStatus.OK
                self._send_json(status, health)
                return
            if parsed.path == "/readz":
                self._send_json(HTTPStatus.OK, {"schema": "observatory.readiness/v1", "status": "ok", "plane": "read"})
                return
            if parsed.path == "/metrics":
                self._send_text(HTTPStatus.OK, self.server.application.metrics(), "text/plain; version=0.0.4")
                return
            if parsed.path == "/v1/observation/history":
                self._send_json(HTTPStatus.OK, self.server.application.observation_reliability())
                return
            if parsed.path == "/v1/observation":
                report = self.server.application.observation()
                self._send_json(
                    HTTPStatus.OK if report["observation_capable"] else HTTPStatus.SERVICE_UNAVAILABLE,
                    report,
                )
                return
            if parsed.path == "/v1/summary":
                self._send_json(HTTPStatus.OK, self.server.application.summary(_query_filters(parsed.query)))
                return
            if parsed.path == "/v1/events":
                filters = _query_filters(parsed.query)
                limit = _query_limit(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.events(filters, limit))
                return
            if parsed.path == "/v1/measurements":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.measurements(event_id, limit))
                return
            if parsed.path == "/v1/outcomes":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.outcomes(event_id, limit))
                return
            if parsed.path == "/v1/attribution":
                event_id, limit = _query_evidence_endpoint(parsed.query)
                self._send_json(HTTPStatus.OK, self.server.application.attribution(event_id, limit))
                return
            if parsed.path == "/v1/analytics/comparison":
                self._send_json(HTTPStatus.OK, self.server.application.comparison(_query_filters(parsed.query), _query_limit(parsed.query, maximum=500)))
                return
            if parsed.path == "/v1/analytics/engineering-value":
                self._send_json(HTTPStatus.OK, self.server.application.engineering_value(_query_filters(parsed.query), _query_limit(parsed.query, maximum=500)))
                return
            if parsed.path == "/v1/analytics/outcome-value":
                self._send_json(HTTPStatus.OK, self.server.application.outcome_value(_query_filters(parsed.query), _query_limit(parsed.query, maximum=500)))
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except DashboardReadUnavailable as exc:
            self._send_read_unavailable(exc)
        except sqlite3.Error:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "store_unavailable"})
        except (ValueError, RuntimeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if not self._plane_allows(parsed.path, "POST"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if parsed.path.startswith("/api/v1/"):
            if not self._require_authentication():
                return
            length_text = self.headers.get("Content-Length")
            try:
                length = int(length_text or "0")
            except ValueError:
                length = -1
            if length < 0 or length > self.server.application.max_request_bytes:
                self.close_connection = length > self.server.application.max_request_bytes * 2
                self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": "request_too_large",
                })
                return
            try:
                body = self.rfile.read(length)
                form = _parse_query(body.decode("utf-8"), keep_blank_values=True)
            except (UnicodeDecodeError, ValueError) as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": f"invalid_form: {exc}",
                })
                return
            try:
                params = _parse_query(parsed.query, keep_blank_values=True)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": str(exc),
                })
                return
            for key, values in form.items():
                params.setdefault(key, []).extend(values)
            if sum(len(values) for values in params.values()) > MAX_QUERY_FIELDS:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "status": "error",
                    "errorType": "bad_data",
                    "error": f"query contains more than {MAX_QUERY_FIELDS} fields",
                })
                return
            self._handle_prometheus(parsed.path, params)
            return
        if parsed.path not in ("/v1/events", "/v1/traces", "/v1/metrics", "/v1/logs"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if not self._require_authentication():
            return
        length_text = self.headers.get("Content-Length")
        try:
            length = int(length_text or "-1")
        except ValueError:
            length = -1
        if length < 0 or length > self.server.application.max_request_bytes:
            if length > 0:
                drain_limit = self.server.application.max_request_bytes * 2
                remaining = min(length, drain_limit)
                while remaining > 0:
                    chunk = self.rfile.read(min(65_536, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                if length > drain_limit:
                    self.close_connection = True
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request_too_large"})
            return
        try:
            body = self.rfile.read(length)
        except (TimeoutError, socket.timeout):
            self.close_connection = True
            self._send_json(HTTPStatus.REQUEST_TIMEOUT, {"error": "request_read_timeout"})
            return
        try:
            value = json.loads(body.decode("utf-8"))
            if parsed.path == "/v1/events":
                status, result = self.server.application.ingest_json(value)
            else:
                signal = parsed.path.removeprefix("/v1/")
                status, result = self.server.application.ingest_otlp(signal, value)
            self._send_json(status, result)
        except DashboardReadUnavailable as exc:
            self._send_read_unavailable(exc)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": f"invalid_json: {exc}"})

    def log_message(self, format: str, *args: Any) -> None:
        # Never log request bodies, query credentials, or arbitrary client text.
        return


class ObservatoryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address: tuple[str, int],
        application: ObservatoryApplication,
        *,
        request_timeout: float = 10.0,
        max_concurrent_requests: int = 64,
        auth_token: str | None = None,
        plane: str = "combined",
        trust_loopback: bool = False,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be positive")
        if auth_token is not None and not auth_token:
            raise ValueError("auth_token must not be empty")
        if plane not in {"combined", "control", "read"}:
            raise ValueError("plane must be combined, control, or read")
        self.application = application
        self.request_timeout = request_timeout
        self._request_slots = BoundedSemaphore(max_concurrent_requests)
        self.auth_token = auth_token
        self.plane = plane
        self.trust_loopback = trust_loopback
        super().__init__(address, _Handler)

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self._request_slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def create_server(
    host: str,
    port: int,
    db_path: str | Path,
    *,
    max_database_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
    request_timeout: float = 10.0,
    max_concurrent_requests: int = 64,
    auth_token: str | None = None,
    plane: str = "combined",
    trust_loopback: bool = False,
) -> ObservatoryHTTPServer:
    return ObservatoryHTTPServer(
        (host, port),
        ObservatoryApplication(EventStore(db_path, max_bytes=max_database_bytes)),
        request_timeout=request_timeout,
        max_concurrent_requests=max_concurrent_requests,
        auth_token=auth_token,
        plane=plane,
        trust_loopback=trust_loopback,
    )


def serve(
    host: str = "127.0.0.1",
    port: int = 8787,
    db_path: str | Path = "observatory.sqlite3",
    *,
    max_database_bytes: int | None = DEFAULT_MAX_DATABASE_BYTES,
    request_timeout: float = 10.0,
    max_concurrent_requests: int = 64,
    auth_token: str | None = None,
    read_port: int | None = 8788,
    trust_loopback: bool = False,
    config_path: str | Path | None = None,
) -> None:
    if read_port is not None and read_port == port:
        raise ValueError("read_port must differ from port")
    application = ObservatoryApplication(
        EventStore(db_path, max_bytes=max_database_bytes),
        config_path=config_path,
    )
    servers = [
        ObservatoryHTTPServer(
            (host, port),
            application,
            request_timeout=request_timeout,
            max_concurrent_requests=max_concurrent_requests,
            auth_token=auth_token,
            plane="control" if read_port is not None else "combined",
            trust_loopback=trust_loopback,
        )
    ]
    if read_port is not None:
        servers.append(
            ObservatoryHTTPServer(
                (host, read_port),
                application,
                request_timeout=request_timeout,
                max_concurrent_requests=max_concurrent_requests,
                auth_token=auth_token,
                plane="read",
                trust_loopback=trust_loopback,
            )
        )
    threads = [Thread(target=server.serve_forever, daemon=True) for server in servers]
    try:
        for thread in threads:
            thread.start()
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.25)
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        application.store.close()
