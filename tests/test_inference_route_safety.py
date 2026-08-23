"""The written client configuration must never touch an inference route.

`inference_proxy: False` is a self-declaration. These tests check the artifact
actually written to a user's client configuration instead, because that is what
would redirect a real request. A future change could add a provider base URL
while the declared flag stayed False, and nothing would have noticed.
"""

import re
import unittest

from observatory.clients import (
    CLIENT_SPECS,
    _claude_values,
    _codex_block,
    _gemini_values,
    _hook_block,
    client_spec,
)

# Variables that would move inference somewhere else if Observatory set them.
PROVIDER_ROUTE = re.compile(
    r"(ANTHROPIC_BASE_URL|ANTHROPIC_API_URL|OPENAI_BASE_URL|OPENAI_API_BASE|"
    r"AZURE_OPENAI_ENDPOINT|GOOGLE_API_BASE|GEMINI_BASE_URL|XAI_BASE_URL|"
    r"MOONSHOT_BASE_URL|OPENROUTER_BASE_URL|BEDROCK_ENDPOINT|VERTEX_ENDPOINT|"
    r"HTTPS?_PROXY|ALL_PROXY|NO_PROXY|api_base|base_url|proxy_url)",
    re.I,
)
CREDENTIAL = re.compile(
    r"(API_KEY|ACCESS_TOKEN|SECRET|PASSWORD|BEARER|sk-[A-Za-z0-9]{8,}|"
    r"xai-[A-Za-z0-9]{8,}|AIza[A-Za-z0-9_\-]{10,})",
    re.I,
)
# Telemetry destinations are legitimate; they must stay on the loopback host.
LOOPBACK = re.compile(r"^https?://(127\.0\.0\.1|localhost|\[::1\])(:\d+)?(/|$)")


def _written_artifacts():
    """Every string a user's configuration could receive from `configure`."""

    yield "claude/env", "\n".join(f"{k}={v}" for k, v in _claude_values(enable_traces=True).items())
    yield "gemini/telemetry", "\n".join(f"{k}={v}" for k, v in _gemini_values(enable_traces=True).items())
    yield "codex/otel", _codex_block(enable_traces=True)
    for name in ("kimi", "grok"):
        yield f"{name}/hooks", _hook_block(client_spec(name))


class WrittenConfigurationSafetyTests(unittest.TestCase):
    def test_no_client_configuration_sets_a_provider_route(self):
        for label, text in _written_artifacts():
            with self.subTest(artifact=label):
                hit = PROVIDER_ROUTE.search(text)
                self.assertIsNone(
                    hit,
                    f"{label} writes a provider route or proxy variable: {hit.group(0) if hit else ''}",
                )

    def test_no_client_configuration_carries_a_credential(self):
        for label, text in _written_artifacts():
            with self.subTest(artifact=label):
                hit = CREDENTIAL.search(text)
                self.assertIsNone(hit, f"{label} carries a credential-shaped value")

    def test_every_endpoint_written_stays_on_loopback(self):
        # Telemetry may be redirected; it may not be sent off the machine by default.
        urls = []
        for label, text in _written_artifacts():
            urls.extend((label, url) for url in re.findall(r"https?://[^\s\"',]+", text))
        self.assertTrue(urls, "expected at least one telemetry endpoint to be written")
        for label, url in urls:
            with self.subTest(artifact=label, url=url):
                self.assertRegex(url, LOOPBACK, f"{label} points telemetry off-host: {url}")

    def test_content_capture_is_disabled_in_every_written_configuration(self):
        claude = _claude_values(enable_traces=True)
        for key in ("OTEL_LOG_USER_PROMPTS", "OTEL_LOG_TOOL_CONTENT", "OTEL_LOG_RAW_API_BODIES"):
            self.assertEqual(str(claude[key]), "0", f"{key} must disable content capture")
        self.assertIs(_gemini_values(enable_traces=True)["logPrompts"], False)
        self.assertIn("log_user_prompt = false", _codex_block(enable_traces=True))

    def test_every_managed_hook_declares_a_timeout(self):
        # An unbounded hook lets telemetry delay the client that invoked it.
        for name in ("kimi", "grok"):
            with self.subTest(client=name):
                self.assertRegex(_hook_block(client_spec(name)), r"timeout\s*=\s*\d+")

    def test_no_spec_declares_itself_an_inference_proxy(self):
        for key, spec in CLIENT_SPECS.items():
            with self.subTest(client=key):
                self.assertNotEqual(spec.capabilities.get("inference_proxy"), "SUPPORTED")


if __name__ == "__main__":
    unittest.main()
