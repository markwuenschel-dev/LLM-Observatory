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
                    {"key": "gen_ai.request.model.version", "value": {"stringValue": "gpt-example-2026"}},
                    {"key": "gen_ai.conversation.id", "value": {"stringValue": "session-otel"}},
                    {"key": "llm.observatory.workflow.id", "value": {"stringValue": "workflow-otel"}},
                    {"key": "llm.observatory.agent.id", "value": {"stringValue": "agent-otel"}},
                    {"key": "llm.observatory.parent.agent.id", "value": {"stringValue": "orchestrator-otel"}},
                    {"key": "llm.observatory.skill", "value": {"stringValue": "skill-otel"}},
                    {"key": "llm.observatory.client", "value": {"stringValue": "codex"}},
                    {"key": "llm.observatory.project.root", "value": {"stringValue": "C:\\private\\repo"}},
                    {"key": "llm.observatory.project.repository", "value": {"stringValue": "repo"}},
                    {"key": "llm.observatory.project.branch", "value": {"stringValue": "main"}},
                    {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "12"}},
                    {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "8"}},
                    {"key": "llm.observatory.tool.call.count", "value": {"intValue": "2"}},
                    {"key": "llm.observatory.tool.names", "value": {"arrayValue": {"values": [{"stringValue": "shell"}, {"stringValue": "grep"}]}}},
                    {"key": "llm.observatory.files.inspected.count", "value": {"intValue": "3"}},
                    {"key": "llm.observatory.files.changed.count", "value": {"intValue": "1"}},
                    {"key": "llm.observatory.commands.executed.count", "value": {"intValue": "4"}},
                    {"key": "llm.observatory.tests.invoked.count", "value": {"intValue": "1"}},
                    {"key": "llm.observatory.agent.failure", "value": {"boolValue": True}},
                    {"key": "llm.observatory.reassessment.count", "value": {"intValue": "2"}},
                    {"key": "llm.observatory.rework.count", "value": {"intValue": "1"}},
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
                self.assertEqual(event.llm.model_variant, "gpt-example-2026")
                self.assertEqual(event.usage.input_tokens, 12)
                self.assertEqual(event.usage.source, "provider")
                self.assertEqual(event.execution.session_id, "session-otel")
                self.assertEqual(event.execution.workflow_id, "workflow-otel")
                self.assertEqual(event.execution.agent_id, "agent-otel")
                self.assertEqual(event.execution.parent_agent_id, "orchestrator-otel")
                self.assertEqual(event.execution.skill, "skill-otel")
                self.assertEqual(event.llm.client, "codex")
                self.assertEqual(event.project.repository, "repo")
                self.assertEqual(event.project.branch, "main")
                self.assertEqual(event.behavior.tool_call_count, 2)
                self.assertEqual(event.behavior.tool_names, ("shell", "grep"))
                self.assertEqual(event.behavior.files_inspected_count, 3)
                self.assertEqual(event.behavior.files_changed_count, 1)
                self.assertEqual(event.behavior.commands_executed_count, 4)
                self.assertEqual(event.behavior.tests_invoked_count, 1)
                self.assertTrue(event.reliability.agent_failure)
                self.assertEqual(event.reliability.reassessment_count, 2)
                self.assertEqual(event.reliability.rework_count, 1)
                self.assertIsNone(event.project.root)
                self.assertTrue(event.project.project_id.startswith("local_sha256:"))
                self.assertNotIn("OTLP_CANARY", event.to_json())
                self.assertEqual(event.attributes["gen_ai.request.model"], "gpt-example")
                self.assertEqual(event.attributes["llm.observatory.evidence.source"], "provider")
                self.assertEqual(event.provenance.adapter, "otlp-json")

    def test_parent_span_is_addressable_by_the_canonical_otel_event_id(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        span["parentSpanId"] = "fedcba9876543210"
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self.assertEqual(OTLPJsonBridge(store).ingest("traces", payload).inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.execution.parent_event_id, "otel:0123456789abcdef0123456789abcdef:fedcba9876543210")
                edge = next(item for item in store.attribution_edges(event_id=event.event_id) if item["relation"] == "parent_event")
                self.assertEqual(edge["parent_event_id"], event.execution.parent_event_id)

    def test_trace_status_and_ttft_attributes_are_mapped_with_provenance(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        span["status"] = {"code": 2, "message": "provider failed"}
        span["attributes"].append({"key": "llm.observatory.time_to_first_token_ms", "value": {"intValue": "23"}})
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self.assertEqual(OTLPJsonBridge(store).ingest("traces", payload).inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.reliability.status, "failed")
                self.assertIsNone(event.performance.latency_ms)
                self.assertAlmostEqual(event.performance.duration_ms, 1500)
                self.assertEqual(event.performance.time_to_first_token_ms, 23)
                self.assertEqual(event.provenance.fields["performance.time_to_first_token_ms"], "client")

    def test_trace_numeric_rate_limit_error_is_normalized_as_failed_rate_limited(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        span["status"] = {"code": 2}
        span["attributes"].append({"key": "error.code", "value": {"intValue": "429"}})
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self.assertEqual(OTLPJsonBridge(store).ingest("traces", payload).inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.reliability.status, "failed")
                self.assertTrue(event.reliability.rate_limited)
                self.assertEqual(event.reliability.error_kind, "rate_limited")
                self.assertEqual(event.provenance.fields["reliability.error_kind"], "derived")
                self.assertEqual(event.provenance.fields["reliability.rate_limited"], "derived")

    def test_trace_explicit_rate_limit_boolean_remains_client_reported(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        span["attributes"].append({"key": "llm.observatory.rate_limited", "value": {"boolValue": True}})
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self.assertEqual(OTLPJsonBridge(store).ingest("traces", payload).inserted, 1)
                event = store.list_events()[0]
                self.assertTrue(event.reliability.rate_limited)
                self.assertEqual(event.provenance.fields["reliability.rate_limited"], "client")

    def test_metrics_and_logs_are_retained_as_metadata_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                bridge = OTLPJsonBridge(store)
                logs = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "fixture"}}]}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "1786111200000000000", "traceId": "t", "spanId": "s", "attributes": [{"key": "severity.text", "value": {"stringValue": "INFO"}}]}]}]}]}
                metrics = {"resourceMetrics": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "fixture"}}]}, "scopeMetrics": [{"metrics": [{"name": "llm.events", "unit": "events", "gauge": {"dataPoints": [{"asDouble": 3.5, "timeUnixNano": "1786111200000000000"}, {"asInt": "4", "timeUnixNano": "1786111201000000000"}]}}]}]}]}
                self.assertEqual(bridge.ingest("logs", logs).inserted, 1)
                self.assertEqual(bridge.ingest("metrics", metrics).inserted, 1)
                self.assertEqual(store.summary()["events"], 2)
                metric = store.list_events({"event_type": "telemetry.metric"})[0]
                self.assertEqual(metric.attributes["metric.unit"], "events")
                self.assertEqual(metric.attributes["metric.points"][0]["value"], 3.5)
                self.assertEqual(metric.attributes["metric.points"][1]["value"], 4)

    def test_metrics_are_project_scoped_and_preserve_context_and_unknown_metadata(self) -> None:
        def payload(root: str, provider: str, workflow: str) -> dict:
            return {"resourceMetrics": [{"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "metric-client"}},
                {"key": "service.version", "value": {"stringValue": "1"}},
                {"key": "llm.observatory.project.root", "value": {"stringValue": root}},
                {"key": "provider", "value": {"stringValue": provider}},
                {"key": "model", "value": {"stringValue": "future-model"}},
                {"key": "future.dimension", "value": {"stringValue": "keep-me"}},
                {"key": "api_key", "value": {"stringValue": "DO_NOT_RETAIN"}},
            ]}, "scopeMetrics": [{"scope": {"name": "metric-scope", "version": "1"}, "metrics": [{
                "name": "llm.events", "unit": "events", "gauge": {"dataPoints": [{"asInt": "1", "timeUnixNano": "1786111200000000000", "attributes": [{"key": "workflow_id", "value": {"stringValue": workflow}}]}]},
            }]}]}]}

        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                bridge = OTLPJsonBridge(store)
                self.assertEqual(bridge.ingest("metrics", payload("C:/repo/alpha", "future-a", "workflow-a")).inserted, 1)
                self.assertEqual(bridge.ingest("metrics", payload("C:/repo/alpha", "future-a", "workflow-b")).inserted, 1)
                self.assertEqual(bridge.ingest("metrics", payload("C:/repo/beta", "future-b", "workflow-a")).inserted, 1)
                events = store.list_events({"event_type": "telemetry.metric"})
                self.assertEqual(len(events), 3)
                self.assertEqual(len({event.event_id for event in events}), 3)
                self.assertNotEqual(events[0].project.project_id, events[2].project.project_id)
                self.assertEqual(events[0].llm.model, "future-model")
                self.assertEqual({event.execution.workflow_id for event in events}, {"workflow-a", "workflow-b"})
                self.assertEqual(events[0].extensions["unknown_attributes"]["future.dimension"], "keep-me")
                self.assertNotIn("DO_NOT_RETAIN", events[0].to_json())

    def test_metric_datapoints_with_different_contexts_become_distinct_normalized_events(self) -> None:
        payload = {"resourceMetrics": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "metric-client"}},
            {"key": "llm.observatory.project.root", "value": {"stringValue": "C:/repo/alpha"}},
        ]}, "scopeMetrics": [{"metrics": [{"name": "llm.events", "gauge": {"dataPoints": [
            {"asInt": "1", "timeUnixNano": "1786111200000000000", "attributes": [{"key": "workflow_id", "value": {"stringValue": "workflow-a"}}]},
            {"asInt": "2", "timeUnixNano": "1786111201000000000", "attributes": [{"key": "workflow_id", "value": {"stringValue": "workflow-b"}}]},
        ]}}]}]}]}
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("metrics", payload)
                self.assertEqual(result.inserted, 2)
                events = store.list_events({"event_type": "telemetry.metric"})
                self.assertEqual({event.execution.workflow_id for event in events}, {"workflow-a", "workflow-b"})
                self.assertEqual(len({event.event_id for event in events}), 2)

    def test_log_signal_preserves_known_model_usage_and_failure_dimensions(self) -> None:
        logs = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "log-client"}}]}, "scopeLogs": [{"scope": {"name": "log-scope", "version": "1"}, "logRecords": [{"timeUnixNano": "1786111200000000000", "attributes": [
            {"key": "gen_ai.provider.name", "value": {"stringValue": "openai"}},
            {"key": "gen_ai.request.model", "value": {"stringValue": "gpt-log"}},
            {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "9"}},
            {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "4"}},
            {"key": "llm.observatory.time_to_first_token_ms", "value": {"intValue": "17"}},
            {"key": "error.type", "value": {"stringValue": "timeout"}},
            {"key": "llm.observatory.evidence.source", "value": {"stringValue": "client"}},
            {"key": "severity.text", "value": {"stringValue": "ERROR"}},
        ]}]}]}]}
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("logs", logs)
                self.assertEqual(result.inserted, 1)
                event = store.list_events({"provider": "openai"})[0]
                self.assertEqual(event.event_type, "model.operation")
                self.assertEqual(event.llm.model, "gpt-log")
                self.assertEqual(event.usage.input_tokens, 9)
                self.assertEqual(event.performance.time_to_first_token_ms, 17)
                self.assertEqual(event.provenance.fields["performance.time_to_first_token_ms"], "client")
                self.assertTrue(event.reliability.timeout)
                self.assertEqual(event.reliability.status, "failed")

    def test_claude_code_event_attributes_are_normalized_from_documented_plain_keys(self) -> None:
        logs = {"resourceLogs": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "claude-code"}},
            {"key": "service.version", "value": {"stringValue": "2.1.226"}},
        ]}, "scopeLogs": [{"scope": {"name": "com.anthropic.claude_code", "version": "1"}, "logRecords": [{
            "timeUnixNano": "1786111200000000000",
            "attributes": [
                {"key": "event.name", "value": {"stringValue": "api_request"}},
                {"key": "session.id", "value": {"stringValue": "claude-session"}},
                {"key": "model", "value": {"stringValue": "claude-sonnet-4"}},
                {"key": "input_tokens", "value": {"intValue": "11"}},
                {"key": "output_tokens", "value": {"intValue": "7"}},
                {"key": "cache_read_tokens", "value": {"intValue": "2"}},
                {"key": "cache_creation_tokens", "value": {"intValue": "3"}},
                {"key": "cost_usd", "value": {"doubleValue": 0.001}},
                {"key": "duration_ms", "value": {"intValue": "19"}},
                {"key": "ttft_ms", "value": {"intValue": "5"}},
                {"key": "success", "value": {"boolValue": True}},
                {"key": "status_code", "value": {"intValue": "200"}},
                {"key": "attempt", "value": {"intValue": "3"}},
                {"key": "workflow.run_id", "value": {"stringValue": "claude-workflow"}},
                {"key": "agent_id", "value": {"stringValue": "claude-agent"}},
                {"key": "parent_agent_id", "value": {"stringValue": "claude-parent"}},
                {"key": "llm.observatory.acceptance.run_id", "value": {"stringValue": "claude-acceptance"}},
            ],
        }]}]}]}
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("logs", logs)
                self.assertEqual(result.inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.event_type, "model.operation")
                self.assertEqual(event.source.name, "claude-code")
                self.assertEqual(event.source.version, "1")
                self.assertEqual(event.llm.provider, "anthropic")
                self.assertEqual(event.llm.model, "claude-sonnet-4")
                self.assertEqual(event.llm.client, "claude-code")
                self.assertEqual(event.execution.session_id, "claude-session")
                self.assertEqual(event.usage.input_tokens, 11)
                self.assertEqual(event.usage.output_tokens, 7)
                self.assertEqual(event.usage.cached_tokens, 2)
                self.assertEqual(event.usage.cache_creation_tokens, 3)
                self.assertEqual(event.usage.cost, 0.001)
                self.assertEqual(event.performance.duration_ms, 19)
                self.assertEqual(event.performance.time_to_first_token_ms, 5)
                self.assertEqual(event.execution.workflow_id, "claude-workflow")
                self.assertEqual(event.execution.agent_id, "claude-agent")
                self.assertEqual(event.execution.parent_agent_id, "claude-parent")
                self.assertEqual(event.reliability.retry_count, 2)
                self.assertEqual(event.reliability.status, "succeeded")

    def test_log_numeric_rate_limit_error_is_normalized_as_failed_rate_limited(self) -> None:
        logs = {"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "log-client"}}]}, "scopeLogs": [{"logRecords": [{"timeUnixNano": "1786111200000000000", "attributes": [
            {"key": "gen_ai.provider.name", "value": {"stringValue": "openai"}},
            {"key": "gen_ai.request.model", "value": {"stringValue": "gpt-log"}},
            {"key": "http.response.status_code", "value": {"intValue": "429"}},
            {"key": "severity.text", "value": {"stringValue": "ERROR"}},
        ]}]}]}]}
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("logs", logs)
                self.assertEqual(result.inserted, 1)
                event = store.list_events({"provider": "openai"})[0]
                self.assertEqual(event.reliability.status, "failed")
                self.assertTrue(event.reliability.rate_limited)
                self.assertEqual(event.reliability.error_kind, "rate_limited")
                self.assertEqual(event.provenance.fields["reliability.error_kind"], "derived")
                self.assertEqual(event.provenance.fields["reliability.rate_limited"], "derived")

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

    def test_otlp_missing_or_invalid_timestamps_are_rejected_without_synthesizing_now(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0].pop("startTimeUnixNano")
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", payload)
                self.assertEqual(result.rejected, 1)
                self.assertEqual(store.summary()["events"], 0)

        invalid = deepcopy(TRACE_PAYLOAD)
        invalid["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["startTimeUnixNano"] = "not-a-time"
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", invalid)
                self.assertEqual(result.rejected, 1)
                self.assertEqual(store.summary()["events"], 0)

    def test_otlp_non_numeric_metric_values_are_rejected_without_persistence(self) -> None:
        payload = {
            "resourceMetrics": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "invalid-metric"}}]},
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "llm.invalid",
                        "gauge": {"dataPoints": [{"asDouble": "not-a-number", "timeUnixNano": "1786111200000000000"}]},
                    }],
                }],
            }],
        }
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("metrics", payload)
                self.assertEqual(result.rejected, 1)
                self.assertEqual(store.summary()["events"], 0)

    def test_otlp_acceptance_resource_marker_is_retained_without_content_capture(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        payload["resourceSpans"][0]["resource"]["attributes"].append({
            "key": "llm.observatory.acceptance.run_id",
            "value": {"stringValue": "provider-acceptance-test"},
        })
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", payload)
                self.assertEqual(result.inserted, 1)
                event = store.list_events()[0]
                self.assertEqual(event.attributes["llm.observatory.acceptance.run_id"], "provider-acceptance-test")
                self.assertEqual(event.provenance.content_capture, "disabled")

    def test_otlp_invalid_sibling_does_not_drop_valid_trace(self) -> None:
        payload = deepcopy(TRACE_PAYLOAD)
        spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        invalid = deepcopy(spans[0])
        invalid["spanId"] = "invalid-span"
        invalid.pop("startTimeUnixNano")
        spans.extend([invalid, deepcopy(spans[0])])
        spans[-1]["spanId"] = "valid-sibling"
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                result = OTLPJsonBridge(store).ingest("traces", payload)
                self.assertEqual(result.inserted, 2)
                self.assertEqual(result.rejected, 1)
                self.assertIsNotNone(store.get("otel:0123456789abcdef0123456789abcdef:valid-sibling"))

    def test_otlp_empty_or_structurally_malformed_containers_are_rejected(self) -> None:
        payloads = (
            ("traces", {"resourceSpans": [{"scopeSpans": {}}]}),
            ("logs", {"resourceLogs": [{"scopeLogs": [{"logRecords": {}}]}]}),
            ("logs", {"resourceLogs": [{"scopeLogs": [{"scope": [], "logRecords": []}]}]}),
            ("metrics", {"resourceMetrics": [{"scopeMetrics": {}}]}),
            ("traces", {"resourceSpans": []}),
            ("logs", {"resourceLogs": []}),
            ("metrics", {"resourceMetrics": []}),
        )
        for signal, payload in payloads:
            with self.subTest(signal=signal, payload=payload):
                with tempfile.TemporaryDirectory() as temp:
                    with EventStore(Path(temp) / "events.sqlite3") as store:
                        result = OTLPJsonBridge(store).ingest(signal, payload)
                        self.assertEqual(result.inserted, 0)
                        self.assertEqual(result.rejected, 1)
                        self.assertEqual(store.summary()["events"], 0)


if __name__ == "__main__":
    unittest.main()
