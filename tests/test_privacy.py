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
            "error_message": "provider returned Bearer SECRET_VALUE_CANARY",
            "diagnostic": "sk-proj-SECRET_VALUE_CANARY",
            "project": {"root": "C:\\private\\repo"},
            "attributes": {"command": "git status --secret"},
        }

        redacted = redact_mapping(value, PrivacyPolicy())
        encoded = repr(redacted)
        for canary in ("PROMPT_CANARY", "COMPLETION_CANARY", "ARGUMENT_CANARY", "RESULT_CANARY", "PROMPT_TEXT_CANARY", "RESPONSE_BODY_CANARY", "RAW_API_BODY_CANARY", "SECRET_CANARY", "API_KEY_CANARY", "API_TOKEN_CANARY", "SESSION_TOKEN_CANARY", "ENV_API_KEY_CANARY", "PROVIDER_SECRET_CANARY", "SECRET_VALUE_CANARY", "C:\\private\\repo"):
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

    def test_opt_in_content_is_bounded_but_secrets_remain_redacted(self) -> None:
        policy = PrivacyPolicy(content_capture=True, max_string_length=32)
        value = {"prompt": "x" * 100, "api_key": "secret"}
        redacted = redact_mapping(value, policy)
        self.assertEqual(len(redacted["prompt"]), 33)
        self.assertNotEqual(redacted["api_key"], "secret")


if __name__ == "__main__":
    unittest.main()
