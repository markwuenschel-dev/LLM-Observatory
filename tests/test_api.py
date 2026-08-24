from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from observatory.api import (
    DEFAULT_READ_TIMEOUT,
    OBSERVATION_READ_TIMEOUT,
    DashboardReadPool,
    ObservatoryApplication,
    ObservatoryHTTPServer,
    _parse_query,
)
from observatory.store import EventStore

from tests.test_contracts import event_mapping


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = EventStore(Path(self.temp.name) / "events.sqlite3")
        self.server = ObservatoryHTTPServer(("127.0.0.1", 0), ObservatoryApplication(self.store, max_request_bytes=4096))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.server_close()
        self.store.close()
        self.temp.cleanup()

    def request(self, method: str, path: str, value: object | None = None) -> tuple[int, dict | str]:
        data = None if value is None else json.dumps(value).encode("utf-8")
        request = Request(self.base + path, data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=3) as response:
                raw = response.read().decode("utf-8")
                content_type = response.headers.get("Content-Type", "")
                return response.status, json.loads(raw) if "json" in content_type else raw
        except HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()

    def test_health_ingest_duplicate_summary_and_metrics(self) -> None:
        status, health = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["inference_path"], "unmanaged/no-proxy")
        value = event_mapping()
        value["received_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        value["llm"]["model_variant"] = "2026-08-07"
        value["execution"] = {"agent_id": "agent-api", "subagent_id": "subagent-api", "parent_agent_id": "orchestrator-api"}
        value["usage"].update({
            "cached_tokens": 2,
            "cache_creation_tokens": 3,
            "cache_read_tokens": 4,
            "context_size": 100,
            "context_utilization": 0.5,
            "compaction_count": 1,
        })
        value["performance"] = {
            "latency_ms": 42,
            "time_to_first_token_ms": 12,
            "duration_ms": 100,
            "concurrency": 2,
            "parallel_utilization": 0.75,
        }
        status, first = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        self.assertEqual(first["inserted"], 1)
        status, second = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        self.assertEqual(second["duplicate"], 1)
        status, summary = self.request("GET", "/v1/summary?provider=unknown")
        self.assertEqual(status, 200)
        self.assertEqual(summary["data"]["events"], 1)
        status, metrics = self.request("GET", "/metrics")
        self.assertEqual(status, 200)
        self.assertIn("observatory_events_total 1", metrics)
        self.assertIn("observatory_input_tokens_total 10", metrics)
        self.assertIn("observatory_cache_creation_tokens_total 3", metrics)
        self.assertIn("observatory_cache_read_tokens_total 4", metrics)
        self.assertIn("observatory_compactions_total 1", metrics)
        self.assertIn("observatory_time_to_first_token_average_ms 12.0", metrics)
        self.assertIn("observatory_context_size_average 100.0", metrics)
        self.assertIn("observatory_parallel_utilization_average 0.75", metrics)
        self.assertIn("observatory_tool_calls_total 0", metrics)
        self.assertIn("observatory_files_inspected_total 0", metrics)
        self.assertIn("observatory_files_changed_total 0", metrics)
        self.assertIn("observatory_commands_executed_total 0", metrics)
        self.assertIn("observatory_tests_invoked_total 0", metrics)
        self.assertIn("observatory_ingest_batches_total 2", metrics)
        self.assertIn("observatory_ingest_unavailable_total 0", metrics)
        self.assertIn("observatory_process_ready 1", metrics)
        self.assertIn("observatory_store_capacity_bytes", metrics)
        self.assertIn("observatory_store_capacity_ratio", metrics)
        self.assertIn("observatory_events_by_context_total", metrics)
        self.assertIn("observatory_input_tokens_by_context_total", metrics)
        self.assertIn("observatory_output_tokens_by_context_total", metrics)
        self.assertIn("observatory_events_by_execution_total", metrics)
        self.assertIn("observatory_events_by_workflow_total", metrics)
        self.assertIn("observatory_events_by_agent_total", metrics)
        self.assertIn('parent_agent="orchestrator-api"', metrics)
        self.assertIn('repository="unknown"', metrics)
        status, comparison = self.request("GET", "/v1/analytics/comparison?provider=unknown")
        self.assertEqual(status, 200)
        self.assertEqual(comparison["count"], 1)
        self.assertEqual(comparison["comparisons"][0]["successes"], 0)
        self.assertEqual(comparison["comparisons"][0]["model_variant"], "2026-08-07")
        status, filtered = self.request("GET", "/v1/events?model_variant=2026-08-07")
        self.assertEqual(status, 200)
        self.assertEqual(filtered["count"], 1)
        status, parent_filtered = self.request("GET", "/v1/events?parent_agent=orchestrator-api")
        self.assertEqual(status, 200)
        self.assertEqual(parent_filtered["count"], 1)

    def test_configured_bearer_token_protects_telemetry_and_query_surfaces(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.server_close()
        self.store.close()
        self.store = EventStore(Path(self.temp.name) / "authenticated.sqlite3")
        self.server = ObservatoryHTTPServer(
            ("127.0.0.1", 0),
            ObservatoryApplication(self.store, max_request_bytes=4096),
            auth_token="test-token",
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

        status, health = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["inference_path"], "unmanaged/no-proxy")
        status, unauthorized = self.request("GET", "/v1/summary")
        self.assertEqual(status, 401)
        self.assertEqual(unauthorized["error"], "authentication_required")
        status, unauthorized = self.request("GET", "/api/v1/query?query=1")
        self.assertEqual(status, 401)
        self.assertEqual(unauthorized["error"], "authentication_required")
        status, unauthorized = self.request("POST", "/v1/events", event_mapping())
        self.assertEqual(status, 401)
        request = Request(self.base + "/v1/events", data=json.dumps(event_mapping()).encode("utf-8"), method="POST")
        request.add_header("Content-Type", "application/json")
        request.add_header("Authorization", "Bearer test-token")
        with urlopen(request, timeout=3) as response:
            self.assertEqual(response.status, 200)

    def test_raw_replay_without_received_at_is_a_duplicate(self) -> None:
        value = event_mapping()
        value["event_id"] = "raw-replay-without-receipt"
        value.pop("received_at", None)
        status, first = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        self.assertEqual(first["inserted"], 1)
        status, second = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        self.assertEqual(second["duplicate"], 1)
        self.assertEqual(second["conflict"], 0)

    def test_batch_rejects_bad_record_but_accepts_valid_sibling(self) -> None:
        bad = {"schema_version": "1.0", "event_type": "bad"}
        good = event_mapping()
        status, result = self.request("POST", "/v1/events", [bad, good])
        self.assertEqual(status, 400)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["outcome"], "accepted_with_rejections")

    def test_readiness_fails_closed_when_store_is_unavailable(self) -> None:
        self.store.close()
        status, health = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["status"], "degraded")
        status, health = self.request("GET", "/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(health["store"], "unavailable")
        status, result = self.request("GET", "/v1/summary")
        self.assertEqual(status, 503)
        self.assertEqual(result["error"], "store_unavailable")

    def test_dashboard_read_lane_cannot_starve_health_or_intake(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.server_close()
        self.store.close()

        self.store = EventStore(Path(self.temp.name) / "isolated.sqlite3")
        application = ObservatoryApplication(self.store, max_request_bytes=4096, max_read_requests=1)
        self.server = ObservatoryHTTPServer(("127.0.0.1", 0), application)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

        self.assertTrue(application.read_pool._slots.acquire(blocking=False))
        try:
            status, unavailable = self.request("GET", "/api/v1/query?query=1")
            self.assertEqual(status, 503)
            self.assertEqual(unavailable["error"], "dashboard_query_unavailable")
            self.assertEqual(unavailable["reason"], "read_lane_saturated")

            status, health = self.request("GET", "/readyz")
            self.assertEqual(status, 200)
            self.assertEqual(health["status"], "ok")

            status, intake = self.request("POST", "/v1/events", event_mapping())
            self.assertEqual(status, 200)
            self.assertEqual(intake["inserted"], 1)
        finally:
            application.read_pool._slots.release()

    def test_control_and_read_planes_expose_only_their_routes(self) -> None:
        application = ObservatoryApplication(self.store, max_request_bytes=4096)
        control = ObservatoryHTTPServer(("127.0.0.1", 0), application, plane="control")
        read = ObservatoryHTTPServer(("127.0.0.1", 0), application, plane="read")
        control_thread = threading.Thread(target=control.serve_forever, daemon=True)
        read_thread = threading.Thread(target=read.serve_forever, daemon=True)
        control_thread.start()
        read_thread.start()

        def get(port: int, path: str, *, method: str = "GET", value: object | None = None) -> int:
            data = None if value is None else json.dumps(value).encode("utf-8")
            request = Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
            try:
                with urlopen(request, timeout=3) as response:
                    response.read()
                    return response.status
            except HTTPError as exc:
                try:
                    exc.read()
                    return exc.code
                finally:
                    exc.close()

        try:
            self.assertEqual(get(control.server_port, "/readyz"), 200)
            self.assertEqual(get(control.server_port, "/v1/summary"), 404)
            self.assertEqual(get(read.server_port, "/readz"), 200)
            self.assertEqual(get(read.server_port, "/v1/summary"), 200)
            self.assertEqual(get(read.server_port, "/v1/events", method="POST", value=event_mapping()), 404)
            form = urlencode({"query": "1"}).encode("utf-8")
            request = Request(
                f"http://127.0.0.1:{read.server_port}/api/v1/query",
                data=form,
                method="POST",
            )
            request.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urlopen(request, timeout=3) as response:
                self.assertEqual(response.status, 200)
                posted = json.loads(response.read().decode("utf-8"))
            self.assertEqual(posted["status"], "success")
        finally:
            for server, thread in ((control, control_thread), (read, read_thread)):
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()

    def test_ingest_reports_store_unavailable_without_resetting_the_connection(self) -> None:
        self.store.close()
        status, result = self.request("POST", "/v1/events", event_mapping())
        self.assertEqual(status, 503)
        self.assertEqual(result["outcome"], "degraded")
        self.assertEqual(result["unavailable"], 1)
        self.assertEqual(result["rejected"], 1)
        self.assertIn("store unavailable", result["errors"][0])

    def test_store_capacity_degrades_readiness_and_rejects_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = EventStore(Path(temp) / "events.sqlite3", max_bytes=1)
            try:
                application = ObservatoryApplication(store)
                health = application.health()
                self.assertEqual(health["status"], "degraded")
                self.assertIn("observatory_process_ready 0", application.metrics())
                status, result = application.ingest_json(event_mapping())
                self.assertEqual(status, 503)
                self.assertEqual(result["outcome"], "degraded")
                self.assertEqual(result["rejected"], 1)
                self.assertEqual(result["unavailable"], 1)
                self.assertIn("capacity", result["errors"][0])
            finally:
                store.close()

    def test_oversized_request_is_rejected(self) -> None:
        value = {"payload": "x" * 5000}
        status, result = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 413)
        self.assertEqual(result["error"], "request_too_large")

    def test_event_query_accepts_bounded_limit_parameter(self) -> None:
        value = event_mapping()
        value["execution"] = {"trace_id": "api-trace-1", "span_id": "api-span-1"}
        value["project"] = {"repository": "api-repository"}
        status, result = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        status, result = self.request("GET", "/v1/events?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(result["count"], 1)
        status, result = self.request("GET", "/v1/events?trace_id=api-trace-1")
        self.assertEqual(status, 200)
        self.assertEqual(result["events"][0]["execution"]["trace_id"], "api-trace-1")
        status, result = self.request("GET", "/v1/events?repository=api-repository")
        self.assertEqual(status, 200)
        self.assertEqual(result["count"], 1)

    def test_time_query_rejects_malformed_timestamp(self) -> None:
        status, result = self.request("GET", "/v1/summary?start=not-a-timestamp")
        self.assertEqual(status, 400)
        self.assertIn("ISO-8601", result["error"])

    def test_prometheus_compatibility_facade_uses_event_time_and_supports_form_posts(self) -> None:
        for index, observed_at in enumerate((
            "2026-08-07T14:00:00Z",
            "2026-08-07T14:05:00Z",
            "2026-08-07T14:09:00Z",
        ), start=1):
            value = event_mapping()
            value["event_id"] = f"prometheus-event-{index}"
            value["observed_at"] = observed_at
            value["received_at"] = "2026-08-07T15:00:00Z" if index == 3 else observed_at
            value["project"] = {
                "project_id": f"repo:prometheus-repo-{index % 2}",
                "repository": f"prometheus-repo-{index % 2}",
            }
            if index == 1:
                value["behavior"] = {
                    "tool_call_count": 2,
                    "files_inspected_count": 3,
                    "files_changed_count": 1,
                    "commands_executed_count": 4,
                    "tests_invoked_count": 1,
                }
                value["reliability"] = {
                    "agent_failure": True,
                    "reassessment_count": 2,
                    "rework_count": 1,
                }
            self.assertEqual(self.request("POST", "/v1/events", value)[0], 200)

        query = "sum(observatory_events_by_context_total)"
        status, result = self.request(
            "GET",
            "/api/v1/query_range?query=" + query.replace(" ", "%20")
            + "&start=2026-08-07T14:00:00Z&end=2026-08-07T14:10:00Z&step=300",
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["data"]["resultType"], "matrix")
        self.assertEqual([float(point[1]) for point in result["data"]["result"][0]["values"]], [1.0, 2.0, 3.0])

        status, labels = self.request(
            "GET",
            "/api/v1/label/project/values?match%5B%5D=observatory_events_by_context_total"
            "&start=2026-08-07T14:00:00Z&end=2026-08-07T14:10:00Z",
        )
        self.assertEqual(status, 200)
        self.assertEqual(labels["status"], "success")
        self.assertEqual(sorted(labels["data"]), ["repo:prometheus-repo-0", "repo:prometheus-repo-1"])

        form = urlencode({
            "query": "sum(observatory_events_by_context_total)",
            "time": "2026-08-07T14:10:00Z",
        }).encode("utf-8")
        request = Request(self.base + "/api/v1/query", data=form, method="POST")
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urlopen(request, timeout=3) as response:
            self.assertEqual(response.status, 200)
            posted = json.loads(response.read().decode("utf-8"))
        self.assertEqual(posted["data"]["resultType"], "vector")
        self.assertEqual(float(posted["data"]["result"][0]["value"][1]), 3.0)

        status, behavior = self.request(
            "GET",
            "/api/v1/query?" + urlencode({
                "query": "observatory_tool_calls_by_context_total{project=~\"repo:prometheus-repo-.*\"}",
                "time": "2026-08-07T14:10:00Z",
            }),
        )
        self.assertEqual(status, 200)
        self.assertEqual(behavior["status"], "success")
        self.assertTrue(behavior["data"]["result"])

        status, reliability = self.request(
            "GET",
            "/api/v1/query?" + urlencode({
                "query": "observatory_rework_loops_by_context_total{project=~\"repo:prometheus-repo-.*\"}",
                "time": "2026-08-07T14:10:00Z",
            }),
        )
        self.assertEqual(status, 200)
        self.assertEqual(reliability["status"], "success")
        self.assertTrue(any(float(item["value"][1]) == 1.0 for item in reliability["data"]["result"]))

    def test_prometheus_facade_rejects_unbounded_query_inputs(self) -> None:
        oversized = "observatory_events_total" + (" " * (16 * 1024))
        status, result = self.request("GET", "/api/v1/query?" + urlencode({"query": oversized}))
        self.assertEqual(status, 400)
        self.assertIn("exceeds", result["error"])

        regex = 'observatory_events_by_context_total{project=~"' + ("a" * 257) + '"}'
        status, result = self.request("GET", "/api/v1/query?" + urlencode({"query": regex}))
        self.assertEqual(status, 400)
        self.assertIn("regex matcher", result["error"])

        too_many_fields = "&".join(f"field{index}=x" for index in range(129))
        status, result = self.request("GET", "/v1/summary?" + too_many_fields)
        self.assertEqual(status, 400)
        self.assertIn("fields", result["error"])

        with self.assertRaisesRegex(ValueError, "bytes"):
            _parse_query("x" * (64 * 1024 + 1))

        old_range = (
            "/api/v1/query_range?query=observatory_events_total"
            "&start=2020-01-01T00:00:00Z&end=2026-08-08T00:00:00Z&step=86400"
        )
        status, result = self.request("GET", old_range)
        self.assertEqual(status, 400)
        self.assertIn("cannot exceed", result["error"])

    def test_otlp_http_signals_are_normalized_through_the_live_api_surface(self) -> None:
        traces = {
            "resourceSpans": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api-otlp-test"}}]},
                "scopeSpans": [{
                    "scope": {"name": "api-test", "version": "1"},
                    "spans": [{
                        "traceId": "api-otlp-trace",
                        "spanId": "api-otlp-span",
                        "name": "gen_ai.chat",
                        "startTimeUnixNano": "1786111200000000000",
                        "endTimeUnixNano": "1786111201000000000",
                        "attributes": [
                            {"key": "gen_ai.provider.name", "value": {"stringValue": "future-provider"}},
                            {"key": "gen_ai.request.model", "value": {"stringValue": "future-model"}},
                            {"key": "gen_ai.request.model.version", "value": {"stringValue": "future-variant"}},
                            {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "4"}},
                        ],
                    }],
                }],
            }],
        }
        status, result = self.request("POST", "/v1/traces", traces)
        self.assertEqual(status, 200)
        self.assertEqual(result["inserted"], 1)
        status, events = self.request("GET", "/v1/events?trace_id=api-otlp-trace")
        self.assertEqual(status, 200)
        self.assertEqual(events["events"][0]["llm"]["provider"], "future-provider")
        self.assertEqual(events["events"][0]["llm"]["model_variant"], "future-variant")

        logs = {"resourceLogs": [{"resource": {}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "1786111200000000000", "attributes": [{"key": "severity.text", "value": {"stringValue": "INFO"}}]}]}]}]}
        metrics = {"resourceMetrics": [{"resource": {}, "scopeMetrics": [{"metrics": [{"name": "llm.events", "gauge": {"dataPoints": [{"asInt": "1", "timeUnixNano": "1786111200000000000"}]}}]}]}]}
        self.assertEqual(self.request("POST", "/v1/logs", logs)[0], 200)
        self.assertEqual(self.request("POST", "/v1/metrics", metrics)[0], 200)

    def test_evidence_and_event_detail_endpoints_expose_projections(self) -> None:
        value = event_mapping()
        value["event_id"] = "api-detail-1"
        value["execution"] = {"session_id": "session-api"}
        value["outcome"] = {"kind": "build", "status": "passed", "correlation_id": "build-1", "evidence_source": "ci"}
        status, result = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        self.assertEqual(result["inserted"], 1)
        status, detail = self.request("GET", "/v1/events/api-detail-1")
        self.assertEqual(status, 200)
        self.assertEqual(detail["event"]["event_id"], "api-detail-1")
        self.assertTrue(detail["measurements"])
        self.assertEqual(detail["outcomes"][0]["kind"], "build")
        status, measurements = self.request("GET", "/v1/measurements?event_id=api-detail-1")
        self.assertEqual(status, 200)
        self.assertEqual(measurements["count"], 2)
        status, outcomes = self.request("GET", "/v1/outcomes?event_id=api-detail-1")
        self.assertEqual(status, 200)
        self.assertEqual(outcomes["count"], 1)
        status, edges = self.request("GET", "/v1/attribution?event_id=api-detail-1")
        self.assertEqual(status, 200)
        self.assertEqual(edges["edges"][0]["relation"], "project")
        status, metrics = self.request("GET", "/metrics")
        self.assertEqual(status, 200)
        self.assertIn('observatory_outcomes_by_kind_status_total{', metrics)
        self.assertIn('evidence_source="ci"', metrics)
        self.assertIn('kind="build"', metrics)

    def test_evidence_endpoints_reject_unknown_query_parameters(self) -> None:
        for path in ("/v1/measurements", "/v1/outcomes", "/v1/attribution"):
            status, result = self.request("GET", f"{path}?unexpected=value")
            self.assertEqual(status, 400)
            self.assertIn("unsupported query parameter", result["error"])


class _BudgetRecordingPool(DashboardReadPool):
    """A read pool that records every execution budget it hands out.

    The unit that matters for lane occupancy is the budget granted per pool
    admission, so recording it is what separates "one request, one deadline"
    from "one request, one deadline per query".
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.granted: list[float] = []
        # `run` may be implemented on top of `lease`, so count the outermost
        # entry point only: one record per pool admission, not per call layer.
        self._depth = threading.local()

    def _record(self, timeout: float | None) -> None:
        self.granted.append(timeout if timeout and timeout > 0 else self.timeout)

    def run(self, operation, *, timeout=None, **kwargs):
        self._record(timeout)
        depth = getattr(self._depth, "value", 0)
        self._depth.value = depth + 1
        try:
            return super().run(operation, timeout=timeout, **kwargs)
        finally:
            self._depth.value = depth

    def lease(self, *, timeout=None, **kwargs):
        if getattr(self._depth, "value", 0) == 0:
            self._record(timeout)
        return super().lease(timeout=timeout, **kwargs)


class ReadLaneBudgetTests(unittest.TestCase):
    """A read bound is only honest if it bounds the whole request.

    `/v1/observation` is served on the same read plane, and out of the same
    pool, as every Grafana panel. It issues three store reads, so a per-query
    deadline let one request hold a read slot for three deadlines, and nothing
    capped how many such requests could hold slots at once. Starvation is
    duration x concurrency; both have to be bounded, and these tests pin both.
    """

    def _serve(self, **application_kwargs):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        store = EventStore(Path(temp.name) / "events.sqlite3")
        self.addCleanup(store.close)
        application = ObservatoryApplication(store, max_request_bytes=4096, **application_kwargs)
        server = ObservatoryHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.shutdown)
        return application, f"http://127.0.0.1:{server.server_port}"

    @staticmethod
    def _get(base: str, path: str, *, timeout: float = 30) -> tuple[int, dict]:
        try:
            with urlopen(Request(base + path), timeout=timeout) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()

    def test_observation_is_granted_one_read_budget_not_one_per_query(self) -> None:
        application, base = self._serve()
        pool = _BudgetRecordingPool(application.store.path, max_bytes=application.store.max_bytes)
        application.read_pool = pool

        status, _report = self._get(base, "/v1/observation")

        self.assertIn(status, (200, 503))
        self.assertTrue(pool.granted, "the observation route did not use the dashboard read pool")
        self.assertLessEqual(
            sum(pool.granted),
            OBSERVATION_READ_TIMEOUT,
            f"one /v1/observation request was granted {pool.granted} read budgets "
            f"({sum(pool.granted)}s in total), so it may hold a read slot far "
            f"longer than the single stated {OBSERVATION_READ_TIMEOUT}s deadline",
        )

    def test_ordinary_dashboard_reads_keep_the_short_deadline(self) -> None:
        application, base = self._serve()
        pool = _BudgetRecordingPool(application.store.path, max_bytes=application.store.max_bytes)
        application.read_pool = pool

        status, _summary = self._get(base, "/v1/summary")

        self.assertEqual(status, 200)
        self.assertEqual(pool.granted, [DEFAULT_READ_TIMEOUT])

    def test_the_shared_budget_stops_the_remaining_reads_once_it_is_spent(self) -> None:
        _application, base = self._serve()
        sample_calls: list[float] = []
        lifetime_calls: list[float] = []
        original_samples = EventStore.observation_samples
        original_lifetime = EventStore.client_lifetime

        def slow_samples(store, *args, **kwargs):
            sample_calls.append(time.monotonic())
            time.sleep(0.3)
            return original_samples(store, *args, **kwargs)

        def counted_lifetime(store, *args, **kwargs):
            lifetime_calls.append(time.monotonic())
            return original_lifetime(store, *args, **kwargs)

        with patch.object(EventStore, "observation_samples", slow_samples), patch.object(
            EventStore, "client_lifetime", counted_lifetime
        ), patch("observatory.api.OBSERVATION_READ_TIMEOUT", 0.4):
            status, _report = self._get(base, "/v1/observation")

        self.assertIn(status, (200, 503))
        self.assertEqual(
            lifetime_calls,
            [],
            "the third observation read started even though the 0.4s budget was "
            f"already spent by {len(sample_calls)} earlier reads: the handler is "
            "getting a fresh deadline per query instead of one for the request",
        )

    def test_saturating_slow_observation_requests_cannot_starve_ordinary_reads(self) -> None:
        application, base = self._serve(max_read_requests=4)
        release = threading.Event()
        self.addCleanup(release.set)
        lock = threading.Lock()
        state = {"blocked": 0, "returned": 0}
        original_samples = EventStore.observation_samples

        def blocking_samples(store, *args, **kwargs):
            with lock:
                state["blocked"] += 1
            release.wait(30)
            return original_samples(store, *args, **kwargs)

        patcher = patch.object(EventStore, "observation_samples", blocking_samples)
        patcher.start()
        self.addCleanup(patcher.stop)

        def observe() -> None:
            try:
                self._get(base, "/v1/observation", timeout=60)
            finally:
                with lock:
                    state["returned"] += 1

        threads = [threading.Thread(target=observe, daemon=True) for _ in range(4)]
        for thread in threads:
            thread.start()

        settled = time.monotonic() + 20
        while time.monotonic() < settled:
            with lock:
                if state["blocked"] + state["returned"] >= len(threads):
                    break
            time.sleep(0.02)
        else:  # pragma: no cover - only reached on a wedged run
            self.fail("the observation requests never reached the read lane")

        try:
            status, body = self._get(base, "/v1/summary", timeout=10)
            self.assertEqual(
                status,
                200,
                "an ordinary dashboard read was starved by concurrent slow "
                f"/v1/observation requests: {body}",
            )
            self.assertLess(
                application.read_pool.max_extended_requests,
                application.read_pool.max_requests,
                "long-budget reads must be capped strictly below the pool so that "
                "ordinary panel reads always keep capacity",
            )
        finally:
            release.set()
            for thread in threads:
                thread.join(timeout=30)


if __name__ == "__main__":
    unittest.main()
