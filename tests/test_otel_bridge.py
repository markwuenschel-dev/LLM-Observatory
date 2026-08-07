import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from observatory.otel_bridge import OTLPJsonBridge
from observatory.store import EventStore


TRACE_PAYLOAD = {
    "resourceSpans": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "codex"}},
            {"key": "llm.observatory.auth.mode", "value": {"stringValue": "subscription"}},
        ]},
        "schemaUrl": "https://opentelemetry.io/schemas/1.44.0",
        "scopeSpans": [{
            "scope": {"name": "codex-otel", "version": "1.0"},
            "spans": [{
                "traceId": "0123456789abcdef0123456789abcdef",
                "spanId": "0123456789abcdef",
                "name": "gen_ai.chat",
                "startTimeUnixNano": "1786111200000000000",
                "endTimeUnixNano": "1786111201500000000",
                "attributes": [
                    {"key": "gen_ai.provider.name", "value": {"stringValue": "openai"}},
                    {"key": "gen_ai.request.model", "value": {"stringValue": "gpt-example"}},
                    {"key": "gen_ai.conversation.id", "value": {"stringValue": "session-otel"}},
                    {"key": "llm.observatory.workflow.id", "value": {"stringValue": "workflow-otel"}},
                    {"key": "llm.observatory.agent.id", "value": {"stringValue": "agent-otel"}},
                    {"key": "llm.observatory.skill", "value": {"stringValue": "skill-otel"}},
                    {"key": "llm.observatory.client", "value": {"stringValue": "codex"}},
                    {"key": "llm.observatory.project.root", "value": {"stringValue": "C:\\private\\repo"}},
                    {"key": "llm.observatory.project.repository", "value": {"stringValue": "repo"}},
                    {"key": "llm.observatory.project.branch", "value": {"stringValue": "main"}},
                    {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "12"}},
                    {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "8"}},
                    {"key": "gen_ai.prompt", "value": {"stringValue": "OTLP_CANARY"}},
                    {"key": "llm.observatory.evidence.source", "value": {"stringValue": "provider"}},
                ],
                "status": {"code": "STATUS_CODE_OK"},
            }],
        }],
    }]
}


class OTLPBridgeTests(unittest.TestCase):
    def test_trace_is_normalized_and_sensitive_attributes_are_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", TRACE_PAYLOAD)
                self.assertEqual(result.inserted, 1)
                event = store.get("otel:0123456789abcdef0123456789abcdef:0123456789abcdef")
                self.assertIsNotNone(event)
                self.assertEqual(event.llm.provider, "openai")
                self.assertEqual(event.llm.model, "gpt-example")
                self.assertEqual(event.usage.input_tokens, 12)
                self.assertEqual(event.usage.source, "provider")
                self.assertEqual(event.execution.session_id, "session-otel")
                self.assertEqual(event.execution.workflow_id, "workflow-otel")
                self.assertEqual(event.execution.agent_id, "agent-otel")
                self.assertEqual(event.execution.skill, "skill-otel")
                self.assertEqual(event.llm.client, "codex")
                self.assertEqual(event.project.repository, "repo")
                self.assertEqual(event.project.branch, "main")
                self.assertIsNone(event.project.root)
                self.assertTrue(event.project.project_id.startswith("local_sha256:"))
                self.assertNotIn("OTLP_CANARY", event.to_json())
                self.assertEqual(event.attributes["gen_ai.request.model"], "gpt-example")
                self.assertEqual(event.attributes["llm.observatory.evidence.source"], "provider")
                self.assertEqual(event.provenance.adapter, "otlp-json")

    def test_metrics_and_logs_are_retained_as_metadata_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                bridge = OTLPJsonBridge(store)
                logs = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "fixture"}}]}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "1786111200000000000", "traceId": "t", "spanId": "s", "attributes": [{"key": "severity.text", "value": {"stringValue": "INFO"}}]}]}]}]}
                metrics = {"resourceMetrics": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "fixture"}}]}, "scopeMetrics": [{"metrics": [{"name": "llm.events", "gauge": {}}]}]}]}
                self.assertEqual(bridge.ingest("logs", logs).inserted, 1)
                self.assertEqual(bridge.ingest("metrics", metrics).inserted, 1)
                self.assertEqual(store.summary()["events"], 2)

    def test_otlp_batch_is_streamed_into_the_record_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = deepcopy(TRACE_PAYLOAD)
            spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
            duplicate_span = deepcopy(spans[0])
            duplicate_span["spanId"] = "fedcba9876543210"
            spans.append(duplicate_span)
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store, max_records=1).ingest("traces", payload)
                self.assertEqual(result.inserted, 1)
                self.assertEqual(result.rejected, 1)

    def test_otlp_numeric_identity_values_are_normalized_without_rejecting_the_batch(self) -> None:
        payload = {
            "resourceSpans": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "numeric-client"}}]},
                "scopeSpans": [{
                    "scope": {"name": "scope", "version": 1},
                    "spans": [{
                        "traceId": 42,
                        "spanId": 7,
                        "name": "model.call",
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "2000000000",
                        "attributes": [
                            {"key": "gen_ai.provider.name", "value": {"intValue": "99"}},
                            {"key": "gen_ai.request.model", "value": {"intValue": "100"}},
                            {"key": "llm.observatory.client", "value": {"intValue": "101"}},
                        ],
                    }],
                }],
            }],
        }
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", payload)
                self.assertEqual(result.inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.execution.trace_id, "42")
                self.assertEqual(event.llm.provider, "99")
                self.assertEqual(event.llm.model, "100")
                self.assertEqual(event.llm.client, "101")


if __name__ == "__main__":
    unittest.main()
