from datetime import datetime, timezone
import json
import math
import unittest

from observatory.contracts import ContractError, NormalizedEvent, stable_event_id


def event_mapping() -> dict:
    return {
        "schema_version": "1.0",
        "event_id": "evt-1",
        "event_type": "model.operation",
        "observed_at": "2026-08-07T14:00:00Z",
        "source": {"kind": "client", "name": "fixture", "version": "1"},
        "llm": {"provider": "unknown", "model": "unknown", "client": "fixture"},
        "usage": {"input_tokens": 10, "output_tokens": 5, "source": "client"},
        "extensions": {"future_field": {"value": True}},
        "future_top_level": "retain-me",
    }


class NormalizedEventTests(unittest.TestCase):
    def test_normalizes_timestamps_and_retains_unknown_fields(self) -> None:
        event = NormalizedEvent.from_mapping(
            event_mapping(),
            received_at=datetime(2026, 8, 7, 14, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(event.observed_at.tzinfo, timezone.utc)
        self.assertEqual(event.received_at.isoformat(), "2026-08-07T14:00:01+00:00")
        self.assertEqual(event.extensions["future_field"], {"value": True})
        self.assertEqual(event.extensions["unknown_top_level"], {"future_top_level": "retain-me"})
        encoded = json.loads(event.to_json())
        self.assertEqual(encoded["observed_at"], "2026-08-07T14:00:00Z")

    def test_deterministic_event_id_for_idless_adapter_payload(self) -> None:
        value = {"schema_version": "1.0", "event_type": "tool.operation", "observed_at": "2026-08-07T14:00:00Z"}
        self.assertEqual(stable_event_id(value), stable_event_id(dict(value)))
        with_receipt = {**value, "received_at": "2026-08-07T14:00:01Z"}
        self.assertEqual(stable_event_id(value), stable_event_id(with_receipt))
        self.assertTrue(stable_event_id(value).startswith("evt_sha256_"))

    def test_fallback_event_id_ignores_raw_content_and_credentials(self) -> None:
        first = {
            "schema_version": "1.0",
            "event_type": "model.operation",
            "observed_at": "2026-08-07T14:00:00Z",
            "llm": {"provider": "openai", "model": "gpt-test", "client": "fixture"},
            "attributes": {"prompt": "FIRST_PROMPT", "authorization": "Bearer first-secret", "request_id": "req-1"},
        }
        second = {
            **first,
            "attributes": {"prompt": "SECOND_PROMPT", "authorization": "Bearer second-secret", "request_id": "req-1"},
        }
        self.assertEqual(stable_event_id(first), stable_event_id(second))
        self.assertNotIn("FIRST_PROMPT", stable_event_id(first))

    def test_reliability_flags_reject_non_boolean_values(self) -> None:
        value = event_mapping()
        value["reliability"] = {"timeout": "sometimes"}
        with self.assertRaisesRegex(ContractError, "boolean"):
            NormalizedEvent.from_mapping(value)

    def test_rejects_missing_required_identity(self) -> None:
        for key in ("schema_version", "event_id", "event_type", "observed_at"):
            value = event_mapping()
            value.pop(key)
            with self.subTest(key=key):
                with self.assertRaises(ContractError):
                    NormalizedEvent.from_mapping(value)

    def test_rejects_naive_timestamp(self) -> None:
        value = event_mapping()
        value["observed_at"] = "2026-08-07T14:00:00"
        with self.assertRaisesRegex(ContractError, "timezone"):
            NormalizedEvent.from_mapping(value)

    def test_rejects_non_finite_measurements(self) -> None:
        value = event_mapping()
        value["usage"] = {"input_tokens": math.nan}
        with self.assertRaisesRegex(ContractError, "finite"):
            NormalizedEvent.from_mapping(value)

    def test_rejects_negative_usage_and_performance_measurements(self) -> None:
        for section, field in (("usage", "input_tokens"), ("performance", "latency_ms"), ("reliability", "retry_count")):
            value = event_mapping()
            value[section] = {field: -1}
            with self.subTest(section=section, field=field):
                with self.assertRaisesRegex(ContractError, "non-negative"):
                    NormalizedEvent.from_mapping(value)

    def test_source_override_is_explicit(self) -> None:
        event = NormalizedEvent.from_mapping(event_mapping(), source_kind="adapter", source_name="jsonl")
        self.assertEqual(event.source.kind, "adapter")
        self.assertEqual(event.source.name, "jsonl")

    def test_nested_unknown_fields_are_retained_as_extensions(self) -> None:
        value = event_mapping()
        value["usage"]["future_metric"] = 7
        value["execution"] = {"future_execution_id": "exec-1"}
        event = NormalizedEvent.from_mapping(value)
        self.assertEqual(event.extensions["unknown_fields"]["usage"]["future_metric"], 7)
        self.assertEqual(event.extensions["unknown_fields"]["execution"]["future_execution_id"], "exec-1")

    def test_model_variant_is_a_first_class_llm_dimension(self) -> None:
        value = event_mapping()
        value["llm"]["model_variant"] = "2026-08-07"
        event = NormalizedEvent.from_mapping(value)
        self.assertEqual(event.llm.model_variant, "2026-08-07")
        self.assertEqual(json.loads(event.to_json())["llm"]["model_variant"], "2026-08-07")

    def test_parent_agent_is_a_first_class_execution_dimension(self) -> None:
        value = event_mapping()
        value["execution"] = {"agent_id": "worker-1", "subagent_id": "worker-1a", "parent_agent_id": "orchestrator-1"}
        event = NormalizedEvent.from_mapping(value)
        self.assertEqual(event.execution.parent_agent_id, "orchestrator-1")
        self.assertEqual(json.loads(event.to_json())["execution"]["parent_agent_id"], "orchestrator-1")

    def test_agent_behavior_normalizes_bounded_metadata_only(self) -> None:
        value = event_mapping()
        value["behavior"] = {
            "tool_calls": [
                {"name": "shell", "arguments": "DO_NOT_RETAIN"},
                {"name": "grep"},
                {"name": "shell"},
            ],
            "files_inspected": ["C:\\private\\repo\\README.md"],
            "files_changed": ["C:\\private\\repo\\src\\app.py"],
            "commands_executed": ["git status --secret"],
            "tests_invoked": ["python -m pytest"],
        }
        value["reliability"] = {
            "agent_failure": True,
            "reassessment_count": 2,
            "rework_count": 1,
        }
        event = NormalizedEvent.from_mapping(value)

        self.assertEqual(event.behavior.tool_call_count, 3)
        self.assertEqual(event.behavior.tool_names, ("shell", "grep"))
        self.assertEqual(event.behavior.files_inspected_count, 1)
        self.assertEqual(event.behavior.files_changed_count, 1)
        self.assertEqual(event.behavior.commands_executed_count, 1)
        self.assertEqual(event.behavior.tests_invoked_count, 1)
        self.assertTrue(event.reliability.agent_failure)
        self.assertEqual(event.reliability.reassessment_count, 2)
        self.assertEqual(event.reliability.rework_count, 1)
        encoded = event.to_json()
        self.assertNotIn("DO_NOT_RETAIN", encoded)
        self.assertNotIn("C:\\private\\repo", encoded)
        self.assertNotIn("git status --secret", encoded)

    def test_agent_behavior_rejects_negative_counts(self) -> None:
        value = event_mapping()
        value["behavior"] = {"commands_executed_count": -1}
        with self.assertRaisesRegex(ContractError, "non-negative"):
            NormalizedEvent.from_mapping(value)

    def test_agent_behavior_accepts_numeric_tool_call_count_without_tool_names(self) -> None:
        value = event_mapping()
        value["behavior"] = {"tool_calls": 3}
        event = NormalizedEvent.from_mapping(value)
        self.assertEqual(event.behavior.tool_call_count, 3)
        self.assertEqual(event.behavior.tool_names, ())

    def test_reliability_dimensions_reject_negative_loop_counts(self) -> None:
        value = event_mapping()
        value["reliability"] = {"reassessment_count": -1}
        with self.assertRaisesRegex(ContractError, "non-negative"):
            NormalizedEvent.from_mapping(value)

    def test_reliability_aliases_are_canonicalized(self) -> None:
        value = event_mapping()
        value["reliability"] = {"agentFailure": True, "reassessments": ["first", "second"], "rework": ["loop"]}
        event = NormalizedEvent.from_mapping(value)
        self.assertTrue(event.reliability.agent_failure)
        self.assertEqual(event.reliability.reassessment_count, 2)
        self.assertEqual(event.reliability.rework_count, 1)


if __name__ == "__main__":
    unittest.main()
