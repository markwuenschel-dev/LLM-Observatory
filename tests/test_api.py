from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from observatory.api import ObservatoryApplication, ObservatoryHTTPServer
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
        self.assertIn("observatory_ingest_batches_total 2", metrics)
        self.assertIn("observatory_process_ready 1", metrics)
        self.assertIn("observatory_events_by_context_total", metrics)
        self.assertIn('repository="unknown"', metrics)
        status, comparison = self.request("GET", "/v1/analytics/comparison?provider=unknown")
        self.assertEqual(status, 200)
        self.assertEqual(comparison["count"], 1)
        self.assertEqual(comparison["comparisons"][0]["successes"], 0)

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

    def test_oversized_request_is_rejected(self) -> None:
        value = {"payload": "x" * 5000}
        status, result = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 413)
        self.assertEqual(result["error"], "request_too_large")

    def test_event_query_accepts_bounded_limit_parameter(self) -> None:
        value = event_mapping()
        value["execution"] = {"trace_id": "api-trace-1", "span_id": "api-span-1"}
        status, result = self.request("POST", "/v1/events", value)
        self.assertEqual(status, 200)
        status, result = self.request("GET", "/v1/events?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(result["count"], 1)
        status, result = self.request("GET", "/v1/events?trace_id=api-trace-1")
        self.assertEqual(status, 200)
        self.assertEqual(result["events"][0]["execution"]["trace_id"], "api-trace-1")

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

        logs = {"resourceLogs": [{"resource": {}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "1786111200000000000", "attributes": [{"key": "severity.text", "value": {"stringValue": "INFO"}}]}]}]}]}
        metrics = {"resourceMetrics": [{"resource": {}, "scopeMetrics": [{"metrics": [{"name": "llm.events", "gauge": {}}]}]}]}
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

    def test_evidence_endpoints_reject_unknown_query_parameters(self) -> None:
        for path in ("/v1/measurements", "/v1/outcomes", "/v1/attribution"):
            status, result = self.request("GET", f"{path}?unexpected=value")
            self.assertEqual(status, 400)
            self.assertIn("unsupported query parameter", result["error"])


if __name__ == "__main__":
    unittest.main()
