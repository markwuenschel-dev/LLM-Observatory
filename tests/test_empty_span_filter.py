"""Residual spans that carry nothing must not consume the byte budget.

This is a capture-coverage control, not a volume hack. The store has a finite
byte budget, and longitudinal analysis needs weeks of history inside it. On this
host, `codex-app-server` emitted 565,205 residual spans of which 564,679
(99.91%) carried a bare model label and nothing else -- no usage, no latency, no
session, no project, no span name -- at roughly 3.1 KB each. Storing them buys
no analysis and evicts the attributable history that does.

The Collector has a filter for exactly this, and it stopped matching when codex
began stamping `model` on every internal span (schema drift, client version
0.149.0-alpha.4.1): the filter's condition requires `attributes["model"] == nil`.
The Collector filter also sees pre-redaction attributes, so its input is not
reproducible from stored data. This predicate runs where the shape IS knowable
-- on the normalized record -- and is verified against the real corpus.

Nothing is silently discarded: the drops are counted per source so coverage can
say "N spans arrived carrying nothing" rather than report the client as silent.
Raw spans remain queryable in Tempo, which is fed by a separate pipeline with no
filter.
"""

import tempfile
import unittest
from pathlib import Path

from observatory.otel_bridge import OTLPJsonBridge, _is_analytically_empty_span
from observatory.store import EventStore


def _record(**overrides):
    """The shape the bridge builds for a codex internal span."""
    record = {
        "event_type": "otel.span",
        "execution": {"trace_id": "t" * 32, "span_id": "s" * 16, "parent_event_id": "otel:x:y",
                      "session_id": None, "workflow_id": None, "agent_id": None,
                      "subagent_id": None, "task_id": None},
        "project": {"project_id": "project:unknown", "repository": None, "root": None,
                    "remote": None, "worktree": None, "branch": None, "commit": None},
        "usage": {"input_tokens": None, "output_tokens": None, "total_tokens": None,
                  "cost": None, "source": "unknown"},
        # Every span has a derived duration; it is not evidence of anything.
        "performance": {"duration_ms": 0.0048, "latency_ms": None, "time_to_first_token_ms": None},
        "reliability": {"status": "unknown", "error_kind": None, "retry_count": None,
                        "rate_limited": None, "timeout": False, "tool_failure": False,
                        "agent_failure": None, "aborted": False,
                        "reassessment_count": None, "rework_count": None},
        "attributes": {"service.name": "codex-app-server", "service.version": "0.149.0"},
    }
    for section, value in overrides.items():
        if isinstance(value, dict) and isinstance(record.get(section), dict):
            record[section] = {**record[section], **value}
        else:
            record[section] = value
    return record


class EmptySpanPredicateTests(unittest.TestCase):
    def test_the_live_empty_shape_is_dropped(self):
        self.assertTrue(_is_analytically_empty_span(_record()))

    def test_a_derived_duration_is_not_evidence(self):
        # Derived from the span's own start/end stamps, so every span has one.
        # Treating it as evidence made the predicate unable to drop anything.
        self.assertTrue(_is_analytically_empty_span(_record(performance={"duration_ms": 99999.0})))

    def test_false_reliability_flags_are_not_evidence(self):
        # `aborted`/`timeout`/`tool_failure` are stamped False on every ordinary
        # span. Counting them kept 110,890 empty spans alive.
        self.assertTrue(_is_analytically_empty_span(
            _record(reliability={"aborted": False, "timeout": False, "tool_failure": False})))

    def test_a_zero_retry_count_is_not_evidence(self):
        self.assertTrue(_is_analytically_empty_span(_record(reliability={"retry_count": 0})))

    # --- everything below must be KEPT -------------------------------------

    def test_usage_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(usage={"input_tokens": 122516})))

    def test_a_session_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(execution={"session_id": "S"})))

    def test_a_resolved_project_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(
            _record(project={"project_id": "local_sha256:abc"})))

    def test_a_client_reported_latency_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(performance={"latency_ms": 900})))

    def test_a_named_span_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(attributes={"span_name": "chat"})))

    def test_a_real_status_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(reliability={"status": "failed"})))

    def test_a_true_failure_flag_is_kept(self):
        self.assertFalse(_is_analytically_empty_span(_record(reliability={"tool_failure": True})))
        self.assertFalse(_is_analytically_empty_span(_record(reliability={"retry_count": 2})))

    def test_only_the_residual_bucket_is_eligible(self):
        # A model or tool operation is never dropped, however sparse: those
        # event types are the analysis itself.
        for event_type in ("model.operation", "tool.operation", "telemetry.metric"):
            with self.subTest(event_type=event_type):
                self.assertFalse(_is_analytically_empty_span(_record(event_type=event_type)))


class BridgeDropAccountingTests(unittest.TestCase):
    """Dropping is only defensible if it is visible."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = EventStore(Path(self._dir.name) / "events.sqlite3")
        self.addCleanup(self.store.close)
        self.bridge = OTLPJsonBridge(self.store)

    def _payload(self, spans):
        return {
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "codex-app-server"}},
                ]},
                "scopeSpans": [{"scope": {"name": "codex"}, "spans": spans}],
            }]
        }

    def _span(self, span_id, *, attributes=None, name=None):
        span = {
            "traceId": "a" * 32,
            "spanId": span_id,
            "startTimeUnixNano": "1755000000000000000",
            "endTimeUnixNano": "1755000000004800000",
            "attributes": attributes or [{"key": "model", "value": {"stringValue": "gpt-5.6-terra"}}],
        }
        if name is not None:
            span["name"] = name
        return span

    def test_empty_spans_are_dropped_and_counted_while_useful_ones_persist(self):
        payload = self._payload([
            self._span("1" * 16),
            self._span("2" * 16),
            self._span("3" * 16, attributes=[
                {"key": "model", "value": {"stringValue": "gpt-5.6-terra"}},
                {"key": "session.id", "value": {"stringValue": "S-real"}},
            ]),
        ])
        records = [r for r in self.bridge.iter_records("traces", payload) if r]
        self.assertEqual(len(records), 1, "the span carrying a session should be the only one kept")
        self.assertEqual(records[0]["execution"]["session_id"], "S-real")
        self.assertEqual(self.bridge.dropped_empty_spans, 2)
        self.assertEqual(self.bridge.dropped_empty_by_source, {"codex-app-server": 2})

    def test_a_client_that_sent_only_empty_spans_is_not_indistinguishable_from_silent(self):
        # The objective forbids comparing a client as though missing telemetry
        # were zero telemetry. A count is what separates the two cases.
        self.bridge.ingest("traces", self._payload([self._span("1" * 16), self._span("2" * 16)]))
        self.assertEqual(self.bridge.dropped_empty_by_source.get("codex-app-server"), 2)
        stored = self.store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(stored, 0)

    def test_a_named_span_survives_the_bridge(self):
        payload = self._payload([self._span("4" * 16, name="gen_ai.chat")])
        records = [r for r in self.bridge.iter_records("traces", payload) if r]
        self.assertEqual(len(records), 1)
        self.assertEqual(self.bridge.dropped_empty_spans, 0)


if __name__ == "__main__":
    unittest.main()
