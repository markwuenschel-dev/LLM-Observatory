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
        self.assertTrue(stable_event_id(value).startswith("evt_sha256_"))

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

    def test_source_override_is_explicit(self) -> None:
        event = NormalizedEvent.from_mapping(event_mapping(), source_kind="adapter", source_name="jsonl")
        self.assertEqual(event.source.kind, "adapter")
        self.assertEqual(event.source.name, "jsonl")


if __name__ == "__main__":
    unittest.main()
