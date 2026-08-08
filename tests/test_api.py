from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from observatory.api import ObservatoryApplication, ObservatoryHTTPServer, _parse_query
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

        form = urlencode({"query": "sum(observatory_events_by_context_total)"}).encode("utf-8")
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


if __name__ == "__main__":
    unittest.main()
