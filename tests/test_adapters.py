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

    def test_jsonl_adapter_can_fill_missing_project_identity_without_overwriting_explicit_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            path.write_text(json.dumps({"observed_at": "2026-08-07T14:00:00Z"}) + "\n", encoding="utf-8")
            event = next(JsonlAdapter(path, project_path=temp).iter_events())
            self.assertIn("project_id", event["project"])
            self.assertNotEqual(event["project"]["project_id"], "project:unknown")

    def test_jsonl_fallback_ids_include_project_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            path.write_text(json.dumps({"observed_at": "2026-08-07T14:00:00Z"}) + "\n", encoding="utf-8")
            alpha = next(JsonlAdapter(path, project_path=Path(temp) / "alpha").iter_events())
            beta = next(JsonlAdapter(path, project_path=Path(temp) / "beta").iter_events())
            self.assertNotEqual(alpha["event_id"], beta["event_id"])

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
            {"id": "resp-1", "model": "gpt-test", "model_variant": "2026-08-07", "usage": {"input_tokens": 12, "output_tokens": 7, "total_tokens": 19}},
            route="direct",
            usage_source="provider",
            latency_ms=33,
            duration_ms=41,
        )
        event = next(adapter.iter_events())
        self.assertTrue(event["event_id"].startswith("evt_sha256_"))
        self.assertEqual(event["attributes"]["response_id"], "resp-1")
        self.assertEqual(event["usage"]["total_tokens"], 19)
        self.assertEqual(event["usage"]["source"], "provider")
        self.assertEqual(event["llm"]["model_variant"], "2026-08-07")
        self.assertEqual(event["performance"]["duration_ms"], 41)
        self.assertFalse(adapter.capabilities().to_mapping()["capabilities"]["inference_proxy"] == "SUPPORTED")

    def test_openrouter_usage_is_gateway_evidence(self) -> None:
        adapter = ProviderResponseAdapter(
            "openrouter",
            "openrouter-api",
            {"id": "gen-1", "model": "provider/model", "usage": {"prompt_tokens": 4, "completion_tokens": 5, "total_tokens": 9}},
            route="openrouter",
            usage_source="gateway",
        )
        event = next(adapter.iter_events())
        self.assertEqual(event["usage"]["source"], "gateway")
        self.assertEqual(event["llm"]["route"], "openrouter")

    def test_first_party_response_shapes_preserve_provider_usage_and_variants(self) -> None:
        cases = (
            (
                "anthropic",
                "direct-anthropic-api",
                {"id": "msg-1", "model": "claude-test", "usage": {"input_tokens": 12, "output_tokens": 8}},
                "provider",
                20,
            ),
            (
                "google",
                "direct-google-api",
                {"responseId": "resp-1", "model": "gemini-test", "modelVersion": "002", "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 6, "totalTokenCount": 16}},
                "provider",
                16,
            ),
            (
                "xai",
                "direct-xai-api",
                {"id": "xai-1", "model": "grok-test", "usage": {"prompt_tokens": 9, "completion_tokens": 4, "total_tokens": 13}, "cost": 0.02},
                "provider",
                13,
            ),
        )
        for provider, client, response, source, total_tokens in cases:
            with self.subTest(provider=provider):
                event = next(ProviderResponseAdapter(provider, client, response, usage_source=source).iter_events())
                self.assertEqual(event["usage"]["source"], source)
                self.assertEqual(event["usage"]["total_tokens"], total_tokens)
                self.assertFalse(event["provenance"]["fields"]["usage"] == "estimated")
                self.assertFalse(event["llm"]["route"] == "openrouter")
        google_event = next(ProviderResponseAdapter("google", "direct-google-api", cases[1][2], usage_source="provider").iter_events())
        self.assertEqual(google_event["llm"]["model_variant"], "002")

    def test_provider_response_adapter_preserves_reliability_signals_without_inference(self) -> None:
        event = next(
            ProviderResponseAdapter(
                "openai",
                "direct-openai-api",
                {
                    "id": "rate-limit-1",
                    "model": "gpt-test",
                    "status_code": 429,
                    "retry_count": 2,
                    "time_to_first_token_ms": 17,
                },
            ).iter_events()
        )
        self.assertEqual(event["reliability"]["status"], "failed")
        self.assertEqual(event["reliability"]["retry_count"], 2)
        self.assertTrue(event["reliability"]["rate_limited"])
        self.assertEqual(event["reliability"]["error_kind"], "rate_limited")
        self.assertEqual(event["provenance"]["fields"]["reliability.rate_limited"], "derived")
        self.assertEqual(event["performance"]["time_to_first_token_ms"], 17)
        self.assertEqual(event["attributes"]["provider.status_code"], 429)

    def test_provider_response_adapter_rejects_scalar_errors_and_marks_missing_usage_unknown(self) -> None:
        event = next(ProviderResponseAdapter("openai", "direct-openai-api", {"id": "error-1", "model": "gpt-test", "error": "boom"}).iter_events())
        self.assertEqual(event["reliability"]["status"], "failed")
        self.assertEqual(event["reliability"]["error_kind"], "provider_error")
        self.assertEqual(event["usage"]["source"], "unknown")
        self.assertEqual(event["provenance"]["fields"]["usage.total_tokens"], "unknown")

    def test_provider_response_adapter_derives_timeout_from_error_type(self) -> None:
        event = next(ProviderResponseAdapter("openai", "direct-openai-api", {"id": "timeout-1", "model": "gpt-test", "error": {"type": "timeout"}}).iter_events())
        self.assertTrue(event["reliability"]["timeout"])
        self.assertEqual(event["provenance"]["fields"]["reliability.timeout"], "derived")

    def test_provider_response_adapter_normalizes_nested_numeric_error_codes(self) -> None:
        response = {"id": "nested-rate-limit-1", "model": "gpt-test", "error": {"code": 429, "message": "rate limited"}}
        event = next(ProviderResponseAdapter("openai", "direct-openai-api", response).iter_events())
        self.assertEqual(event["reliability"]["status"], "failed")
        self.assertTrue(event["reliability"]["rate_limited"])
        self.assertEqual(event["reliability"]["error_kind"], "rate_limited")
        self.assertEqual(event["attributes"]["provider.status_code"], 429)
        self.assertEqual(event["provenance"]["fields"]["reliability.rate_limited"], "derived")

    def test_provider_response_adapter_does_not_overstate_reported_reliability_or_ttft_origin(self) -> None:
        event = next(
            ProviderResponseAdapter(
                "openai",
                "direct-openai-api",
                {
                    "id": "reported-signals-1",
                    "model": "gpt-test",
                    "rate_limited": False,
                    "timeout": True,
                    "retry_count": 2,
                    "time_to_first_token_ms": 17,
                },
            ).iter_events()
        )
        fields = event["provenance"]["fields"]
        self.assertEqual(fields["reliability.rate_limited"], "reported")
        self.assertEqual(fields["reliability.timeout"], "reported")
        self.assertEqual(fields["reliability.retry_count"], "reported")
        self.assertEqual(fields["performance.time_to_first_token_ms"], "reported")

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

    def test_provider_response_ids_are_scoped_and_future_metadata_is_retained_safely(self) -> None:
        openai_event = next(ProviderResponseAdapter(
            "openai",
            "direct-openai-api",
            {"id": "same-response", "model": "gpt-test", "future_dimension": "keep-me", "api_key": "DO_NOT_RETAIN"},
            project=ProjectIdentity(project_id="repo:test"),
        ).iter_events())
        anthropic_event = next(ProviderResponseAdapter(
            "anthropic",
            "direct-anthropic-api",
            {"id": "same-response", "model": "claude-test"},
            project=ProjectIdentity(project_id="repo:test"),
        ).iter_events())
        self.assertNotEqual(openai_event["event_id"], anthropic_event["event_id"])
        self.assertEqual(openai_event["attributes"]["response_id"], "same-response")
        self.assertEqual(openai_event["extensions"]["provider"]["future_dimension"], "keep-me")
        self.assertNotIn("DO_NOT_RETAIN", json.dumps(openai_event))

    def test_provider_response_adapter_defaults_usage_and_auth_provenance_to_unknown(self) -> None:
        event = next(ProviderResponseAdapter(
            "openai",
            "direct-openai-api",
            {"id": "unknown-provenance", "model": "gpt-test", "usage": {"input_tokens": 1}},
        ).iter_events())
        self.assertEqual(event["usage"]["source"], "unknown")
        self.assertEqual(event["llm"]["auth_mode"], "unknown")

    def test_provider_response_adapter_does_not_alias_cache_read_as_cached_total(self) -> None:
        event = next(ProviderResponseAdapter(
            "anthropic",
            "direct-anthropic-api",
            {"id": "cache-provenance", "model": "claude-test", "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_read_input_tokens": 4,
                "cache_creation_input_tokens": 2,
            }},
            usage_source="provider",
        ).iter_events())
        self.assertIsNone(event["usage"]["cached_tokens"])
        self.assertEqual(event["usage"]["cache_read_tokens"], 4)
        self.assertEqual(event["usage"]["cache_creation_tokens"], 2)

    def test_provider_response_adapter_preserves_bounded_tool_agent_and_gateway_metadata(self) -> None:
        adapter = ProviderResponseAdapter(
            "openrouter",
            "openrouter-api",
            {
                "id": "gen-tools",
                "model": "openai/gpt-test",
                "provider_name": "openai",
                "served_model": "gpt-test-2026",
                "provider_attempts": [{"provider": "openai", "model": "gpt-test-2026", "status": "200", "body": "DO_NOT_RETAIN"}],
                "agent_id": "agent-1",
                "subagent_id": "subagent-1",
                "parent_agent_id": "orchestrator-1",
                "tool_calls": [{"id": "call-1", "type": "function", "function": {"name": "shell", "arguments": "SECRET_TOOL_CONTENT"}}],
                "files_inspected": ["C:\\private\\repo\\README.md"],
                "files_changed": ["C:\\private\\repo\\src\\app.py"],
                "commands_executed": ["git status --secret"],
                "tests_invoked": ["python -m pytest"],
                "agent_failure": True,
                "reassessment_count": 2,
                "rework_count": 1,
                "usage": {"prompt_tokens": 4, "completion_tokens": 5, "total_tokens": 9},
            },
            route="openrouter",
            latency_ms=33,
        )
        event = next(adapter.iter_events())
        self.assertEqual(event["execution"]["agent_id"], "agent-1")
        self.assertEqual(event["execution"]["subagent_id"], "subagent-1")
        self.assertEqual(event["execution"]["parent_agent_id"], "orchestrator-1")
        self.assertEqual(event["behavior"]["tool_call_count"], 1)
        self.assertEqual(event["behavior"]["tool_names"], ["shell"])
        self.assertEqual(event["behavior"]["files_inspected_count"], 1)
        self.assertEqual(event["behavior"]["files_changed_count"], 1)
        self.assertEqual(event["behavior"]["commands_executed_count"], 1)
        self.assertEqual(event["behavior"]["tests_invoked_count"], 1)
        self.assertTrue(event["reliability"]["agent_failure"])
        self.assertEqual(event["reliability"]["reassessment_count"], 2)
        self.assertEqual(event["reliability"]["rework_count"], 1)
        self.assertEqual(event["attributes"]["tool_calls"][0], {"id": "call-1", "name": "shell", "type": "function"})
        self.assertEqual(event["attributes"]["gateway.target_provider"], "openai")
        self.assertEqual(event["attributes"]["gateway.served_model"], "gpt-test-2026")
        self.assertNotIn("DO_NOT_RETAIN", json.dumps(event))
        self.assertEqual(event["provenance"]["fields"]["performance.latency_ms"], "observed")
        self.assertEqual(event["provenance"]["fields"]["performance.duration_ms"], "unknown")


if __name__ == "__main__":
    unittest.main()
