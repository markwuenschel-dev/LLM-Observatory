from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError

from observatory.cli import main
from observatory.hooks import build_hook_event
from observatory.privacy import redact_event


class HookCaptureTests(unittest.TestCase):
    def run_cli(self, arguments: list[str], payload: object) -> tuple[int, dict]:
        output = io.StringIO()
        errors = io.StringIO()
        with patch("sys.stdin", io.StringIO(json.dumps(payload))), redirect_stdout(output), redirect_stderr(errors):
            code = main(["--json", *arguments])
        return code, json.loads(output.getvalue()) if output.getvalue() else {}

    def test_hook_normalization_is_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            event = redact_event(
                build_hook_event(
                    "kimi",
                    {
                        "hook_event_name": "PostToolUse",
                        "session_id": "session-1",
                        "cwd": str(Path(temp) / "private-repo"),
                        "tool_name": "shell",
                        "prompt": "HOOK_PROMPT_MUST_NOT_PERSIST",
                        "tool_input": "git status --secret-token=never-store",
                        "model": "moonshot-v1",
                        "usage": {"input_tokens": 4, "output_tokens": 2},
                    },
                    project_path=temp,
                )
            )
            encoded = event.to_json()
            self.assertEqual(event.event_type, "tool.operation")
            self.assertEqual(event.llm.client, "kimi")
            self.assertEqual(event.execution.session_id, "session-1")
            self.assertEqual(event.usage.input_tokens, 4)
            self.assertNotIn("HOOK_PROMPT_MUST_NOT_PERSIST", encoded)
            self.assertNotIn("secret-token", encoded)
            self.assertNotIn(str(Path(temp) / "private-repo"), encoded)
            self.assertEqual(event.provenance.content_capture, "disabled")

    def test_hook_spools_fail_open_and_attributes_unseen_repositories(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            self.run_cli(["--state-dir", str(state), "install"], {})
            repos = []
            for name in ("alpha", "beta"):
                repo = Path(temp) / name
                subprocess.run(["git", "init", str(repo)], check=True, capture_output=True, text=True)
                repos.append(repo)

            with patch("observatory.cli._post_events", side_effect=URLError("collector offline")):
                for index, repo in enumerate(repos):
                    code, result = self.run_cli(
                        [
                            "--state-dir", str(state),
                            "hook",
                            "--client", "kimi",
                            "--project-path", str(repo),
                            "--quiet",
                        ],
                        {
                            "hook_event_name": "SessionStart",
                            "session_id": f"session-{index}",
                            "cwd": str(repo),
                            "prompt": "MUST_NOT_BE_STORED",
                        },
                    )
                    self.assertEqual(code, 0)
                    self.assertEqual(result, {})

            files = sorted((state / "spool").glob("*.jsonl"))
            self.assertEqual(len(files), 2)
            records = [json.loads(path.read_text(encoding="utf-8")) for path in files]
            project_ids = {record["project"]["project_id"] for record in records}
            self.assertEqual(len(project_ids), 2)
            encoded = "\n".join(json.dumps(record) for record in records)
            self.assertNotIn("MUST_NOT_BE_STORED", encoded)
            self.assertNotIn(str(repos[0]), encoded)
            self.assertNotIn(str(repos[1]), encoded)

    def test_hook_api_delivery_is_bounded_and_fail_open(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            self.run_cli(["--state-dir", str(state), "install"], {})
            with patch("observatory.cli._post_events", return_value={"outcome": "accepted", "rejected": 0, "unavailable": 0}) as post:
                code, result = self.run_cli(
                    ["--state-dir", str(state), "hook", "--client", "grok", "--quiet"],
                    {"hook_event_name": "SessionEnd", "session_id": "session-accepted"},
                )
            self.assertEqual(code, 0)
            self.assertEqual(result, {})
            post.assert_called_once()
            self.assertLessEqual(post.call_args.kwargs["timeout"], 0.75)
            self.assertEqual(list((state / "spool").glob("*.jsonl")), [])


if __name__ == "__main__":
    unittest.main()
