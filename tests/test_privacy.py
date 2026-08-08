import unittest

from observatory.contracts import NormalizedEvent
from observatory.privacy import PrivacyPolicy, redact_event, redact_mapping

from tests.test_contracts import event_mapping


class PrivacyTests(unittest.TestCase):
    def test_default_policy_redacts_content_credentials_and_paths(self) -> None:
        value = {
            "prompt": "PROMPT_CANARY",
            "completion": "COMPLETION_CANARY",
            "tool": {"arguments": "ARGUMENT_CANARY", "result": "RESULT_CANARY"},
            "prompt_text": "PROMPT_TEXT_CANARY",
            "response_body": "RESPONSE_BODY_CANARY",
            "raw_api_body": "RAW_API_BODY_CANARY",
            "authorization": "Bearer SECRET_CANARY",
            "api_key": "API_KEY_CANARY",
            "api_token": "API_TOKEN_CANARY",
            "session_token": "SESSION_TOKEN_CANARY",
            "OPENAI_API_KEY": "ENV_API_KEY_CANARY",
            "provider_secret": "PROVIDER_SECRET_CANARY",
            "token": "GENERIC_TOKEN_CANARY",
            "x-auth-token": "HEADER_TOKEN_CANARY",
            "authToken": "AUTH_TOKEN_CAMEL_CANARY",
            "clientSecret": "CLIENT_SECRET_CAMEL_CANARY",
            "promptText": "PROMPT_TEXT_CAMEL_CANARY",
            "toolArguments": "TOOL_ARGUMENTS_CAMEL_CANARY",
            "completionText": "COMPLETION_TEXT_CAMEL_CANARY",
            "responseText": "RESPONSE_TEXT_CAMEL_CANARY",
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "user": {"email": "person@example.test", "userId": "user-123"},
            "unknownSensitive": "UNKNOWN_SENSITIVE_CANARY",
            "error_message": "provider returned Bearer SECRET_VALUE_CANARY",
            "diagnostic": "sk-proj-SECRET_VALUE_CANARY",
            "project": {"root": "C:\\private\\repo"},
            "repository": "C:\\private\\repository-name",
            "attributes": {"command": "git status --secret"},
        }

        redacted = redact_mapping(value, PrivacyPolicy())
        encoded = repr(redacted)
        for canary in ("PROMPT_CANARY", "COMPLETION_CANARY", "ARGUMENT_CANARY", "RESULT_CANARY", "PROMPT_TEXT_CANARY", "RESPONSE_BODY_CANARY", "RAW_API_BODY_CANARY", "SECRET_CANARY", "API_KEY_CANARY", "API_TOKEN_CANARY", "SESSION_TOKEN_CANARY", "ENV_API_KEY_CANARY", "PROVIDER_SECRET_CANARY", "GENERIC_TOKEN_CANARY", "HEADER_TOKEN_CANARY", "AUTH_TOKEN_CAMEL_CANARY", "CLIENT_SECRET_CAMEL_CANARY", "PROMPT_TEXT_CAMEL_CANARY", "TOOL_ARGUMENTS_CAMEL_CANARY", "COMPLETION_TEXT_CAMEL_CANARY", "RESPONSE_TEXT_CAMEL_CANARY", "AKIAIOSFODNN7EXAMPLE", "person@example.test", "user-123", "UNKNOWN_SENSITIVE_CANARY", "SECRET_VALUE_CANARY", "C:\\private\\repo", "C:\\private\\repository-name"):
            self.assertNotIn(canary, encoded)
        self.assertEqual(redacted["prompt"], "[CONTENT_REDACTED]")
        self.assertEqual(redacted["project"]["root"], "[PATH_REDACTED]")

    def test_redacted_event_marks_capture_policy(self) -> None:
        value = event_mapping()
        value["prompt"] = "do not persist"
        value["project"] = {
            "root": "C:\\repo",
            "project_id": "C:\\private\\repo",
            "remote": "https://alice:secret@example.com/acme/demo.git?token=bad",
            "worktree": "C:\\private\\repo\\.git",
        }
        event = NormalizedEvent.from_mapping(value)
        redacted = redact_event(event)

        self.assertEqual(redacted.provenance.content_capture, "disabled")
        self.assertIsNone(redacted.project.root)
        self.assertTrue(redacted.project.project_id.startswith("project_sha256:"))
        self.assertTrue(redacted.project.worktree.startswith("worktree_sha256:"))
        self.assertEqual(redacted.project.remote, "https://example.com/acme/demo.git")
        encoded = redacted.to_json()
        for canary in ("C:\\private\\repo", "alice", "secret", "token=bad"):
            self.assertNotIn(canary, encoded)
        self.assertEqual(redacted.extensions["unknown_top_level"]["prompt"], "[CONTENT_REDACTED]")

    def test_default_policy_keeps_behavior_counts_and_tool_names_but_drops_raw_activity(self) -> None:
        value = event_mapping()
        value["behavior"] = {
            "tool_call_count": 2,
            "tool_names": ["shell", "grep"],
            "files_inspected_count": 3,
            "files_changed_count": 1,
            "commands_executed_count": 2,
            "tests_invoked_count": 1,
        }
        value["reliability"] = {
            "agent_failure": True,
            "reassessment_count": 2,
            "rework_count": 1,
        }
        value["attributes"] = {
            "tool_calls": [{"name": "shell", "arguments": "SECRET_BEHAVIOR_ARGUMENT"}],
            "files_inspected": ["C:\\private\\repo\\README.md"],
            "files_changed": ["C:\\private\\repo\\src\\app.py"],
            "commands_executed": ["git status --secret"],
            "tests_invoked": ["python -m pytest --secret"],
        }
        redacted = redact_event(NormalizedEvent.from_mapping(value))

        self.assertEqual(redacted.behavior.tool_call_count, 2)
        self.assertEqual(redacted.behavior.tool_names, ("shell", "grep"))
        self.assertEqual(redacted.behavior.files_changed_count, 1)
        self.assertTrue(redacted.reliability.agent_failure)
        self.assertEqual(redacted.reliability.reassessment_count, 2)
        self.assertEqual(redacted.reliability.rework_count, 1)
        encoded = redacted.to_json()
        for canary in ("SECRET_BEHAVIOR_ARGUMENT", "C:\\private\\repo", "git status --secret", "python -m pytest --secret"):
            self.assertNotIn(canary, encoded)
        self.assertEqual(redacted.attributes["tool_calls"], "[CONTENT_REDACTED]")

    def test_opt_in_content_is_bounded_but_secrets_remain_redacted(self) -> None:
        policy = PrivacyPolicy(content_capture=True, max_string_length=32)
        value = {"prompt": "x" * 100, "api_key": "secret"}
        redacted = redact_mapping(value, policy)
        self.assertEqual(len(redacted["prompt"]), 33)
        self.assertNotEqual(redacted["api_key"], "secret")

    def test_opt_in_content_still_redacts_embedded_credentials(self) -> None:
        redacted = redact_mapping(
            {"prompt": "use Bearer embedded-secret-value for this request"},
            PrivacyPolicy(content_capture=True),
        )
        self.assertNotIn("embedded-secret-value", repr(redacted))
        self.assertEqual(redacted["prompt"], "[REDACTED]")

    def test_nested_collections_are_bounded_before_persistence(self) -> None:
        redacted = redact_mapping(
            {"attributes": [{"value": index} for index in range(300)]},
            PrivacyPolicy(),
        )
        self.assertEqual(len(redacted["attributes"]), 257)
        self.assertEqual(redacted["attributes"][-1], "[COLLECTION_TRUNCATED]")

    def test_deep_unknown_values_are_dropped(self) -> None:
        value: dict[str, object] = {"value": "safe"}
        for _ in range(40):
            value = {"nested": value}
        redacted = redact_mapping(value, PrivacyPolicy())
        self.assertNotIn("safe", repr(redacted))


if __name__ == "__main__":
    unittest.main()
