import json
from pathlib import Path
import tempfile
import unittest

from observatory.adapters import AdapterError, AdapterRegistry, JsonlAdapter, ProviderResponseAdapter
from observatory.contracts import ProjectIdentity


class AdapterTests(unittest.TestCase):
    def test_jsonl_adapter_assigns_stable_ids_and_isolates_bad_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            path.write_text(
                json.dumps({"observed_at": "2026-08-07T14:00:00Z"}) + "\n"
                "not json\n"
                + json.dumps({"event_id": "evt-2", "observed_at": "2026-08-07T14:00:01Z"}) + "\n",
                encoding="utf-8",
            )
            adapter = JsonlAdapter(path)
            values = list(adapter.iter_events())
            self.assertEqual(len(values), 2)
            self.assertTrue(values[0]["event_id"].startswith("evt_sha256_"))
            self.assertEqual(values[1]["event_id"], "evt-2")
            self.assertEqual(len(adapter.errors), 1)
            self.assertEqual(adapter.capabilities().confidence, "VERIFIED_LOCALLY")

    def test_registry_rejects_duplicate_and_unknown_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            adapter = JsonlAdapter(Path(temp) / "events.jsonl")
            registry = AdapterRegistry([adapter])
            with self.assertRaises(AdapterError):
                registry.register(adapter)
            with self.assertRaises(AdapterError):
                registry.get("missing")
            self.assertEqual(registry.names(), ("jsonl",))

    def test_provider_response_adapter_preserves_provider_usage_without_proxying(self) -> None:
        adapter = ProviderResponseAdapter(
            "openai",
            "direct-openai-api",
            {"id": "resp-1", "model": "gpt-test", "usage": {"input_tokens": 12, "output_tokens": 7, "total_tokens": 19}},
            route="direct",
            latency_ms=33,
        )
        event = next(adapter.iter_events())
        self.assertEqual(event["event_id"], "resp-1")
        self.assertEqual(event["usage"]["total_tokens"], 19)
        self.assertEqual(event["usage"]["source"], "provider")
        self.assertFalse(adapter.capabilities().to_mapping()["capabilities"]["inference_proxy"] == "SUPPORTED")

    def test_openrouter_usage_is_gateway_evidence(self) -> None:
        adapter = ProviderResponseAdapter(
            "openrouter",
            "openrouter-api",
            {"id": "gen-1", "model": "provider/model", "usage": {"prompt_tokens": 4, "completion_tokens": 5, "total_tokens": 9}},
            route="openrouter",
        )
        event = next(adapter.iter_events())
        self.assertEqual(event["usage"]["source"], "gateway")
        self.assertEqual(event["llm"]["route"], "openrouter")

    def test_provider_response_adapter_accepts_explicit_project_identity(self) -> None:
        adapter = ProviderResponseAdapter(
            "openai",
            "direct-openai-api",
            {"id": "resp-project", "model": "gpt-test"},
            project=ProjectIdentity(project_id="repo:test", repository="repo"),
        )
        event = next(adapter.iter_events())
        self.assertEqual(event["project"]["project_id"], "repo:test")
        self.assertEqual(event["project"]["repository"], "repo")


if __name__ == "__main__":
    unittest.main()
