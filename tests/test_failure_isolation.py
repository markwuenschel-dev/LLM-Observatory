from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from observatory.cli import main
import observatory.cli as cli_module
from observatory.contracts import NormalizedEvent
from observatory.store import EventStore

from tests.test_contracts import event_mapping


def baseline_inference(payload: str) -> dict[str, str]:
    """A stand-in for the normal provider path; it has no Observatory dependency."""

    return {"destination": "normal-provider", "payload_hash": str(hash(payload))}


class FailureIsolationTests(unittest.TestCase):
    def run_cli(self, arguments: list[str]) -> tuple[int, dict]:
        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["--json", *arguments])
        return code, json.loads(output.getvalue())

    def test_unavailable_intake_spools_without_touching_inference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "events.jsonl"
            value = event_mapping()
            value["event_id"] = "isolation-1"
            fixture.write_text(json.dumps(value) + "\n", encoding="utf-8")

            before = baseline_inference("same request")
            code, result = self.run_cli([
                "--state-dir", temp,
                "ingest",
                "--file", str(fixture),
                "--url", "http://127.0.0.1:1/v1/events",
            ])
            after = baseline_inference("same request")

            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["mode"], "spooled")
            self.assertEqual(before, after)
            self.assertEqual(len(list(Path(temp, "spool").glob("*.jsonl"))), 1)

    def test_partial_api_acceptance_is_degraded_instead_of_reported_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "events.jsonl"
            fixture.write_text(json.dumps(event_mapping()) + "\n", encoding="utf-8")
            original_post = cli_module._post_events
            try:
                cli_module._post_events = lambda url, records: {"outcome": "accepted_with_rejections", "inserted": 1, "rejected": 1, "unavailable": 0}
                code, result = self.run_cli(["--state-dir", temp, "ingest", "--file", str(fixture), "--url", "http://observatory.test/v1/events"])
            finally:
                cli_module._post_events = original_post
            self.assertEqual(code, 5)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["data"]["mode"], "api")
            self.assertEqual(result["data"]["rejected"], 1)
            self.assertFalse(list(Path(temp, "spool").glob("*.jsonl")))

    def test_outcome_commands_do_not_acknowledge_partial_api_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            original_post = cli_module._post_events
            try:
                cli_module._post_events = lambda url, records: {"outcome": "accepted_with_rejections", "inserted": 1, "rejected": 1, "unavailable": 0}
                code, result = self.run_cli([
                    "--state-dir", temp,
                    "record-outcome",
                    "--kind", "tests",
                    "--status", "passed",
                    "--url", "http://observatory.test/v1/events",
                ])
            finally:
                cli_module._post_events = original_post
            self.assertEqual(code, 5)
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(result["data"]["mode"], "spooled")
            self.assertEqual(len(list(Path(temp, "spool").glob("*.jsonl"))), 1)

    def test_default_deployment_queue_is_bounded_and_nonblocking(self) -> None:
        config = Path(__file__).resolve().parents[1] / "deployment" / "otel-collector" / "config.yaml"
        text = config.read_text(encoding="utf-8")
        self.assertIn("queue_size: 1024", text)
        self.assertIn("queue_size: 512", text)
        self.assertIn("block_on_overflow: false", text)
        self.assertNotIn("block_on_overflow: true", text)

    def test_unknown_provider_and_duplicate_replay_remain_visible_without_causal_claims(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                value = event_mapping()
                value["event_id"] = "unknown-1"
                value["llm"] = {"provider": "future-provider", "model": "future-model", "client": "unknown-client"}
                value["outcome"] = {"kind": "test", "status": "passed", "correlation_id": "test-1", "evidence_source": "ci"}
                event = NormalizedEvent.from_mapping(value)
                self.assertEqual(store.append(event).status, "inserted")
                self.assertEqual(store.append(event).status, "duplicate")
                stored = store.get("unknown-1")
                self.assertEqual(stored.llm.provider, "future-provider")
                self.assertNotIn("caused_by", stored.to_mapping())

    def test_configuration_removal_does_not_touch_provider_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            self.run_cli(["--state-dir", temp, "configure", "claude"])
            code, result = self.run_cli(["--state-dir", temp, "configure", "claude", "--remove"])
            self.assertEqual(code, 0)
            self.assertFalse(result["data"]["inference_proxy"])
            config = json.loads(Path(temp, "config.json").read_text(encoding="utf-8"))
            self.assertEqual(config["managed_clients"], {})

    def test_saturated_spool_drops_telemetry_explicitly_and_stays_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "events.jsonl"
            value = event_mapping()
            value["event_id"] = "spool-limit-1"
            fixture.write_text(json.dumps(value) + "\n", encoding="utf-8")
            original = cli_module.MAX_SPOOL_BYTES
            try:
                cli_module.MAX_SPOOL_BYTES = 1
                code, result = self.run_cli([
                    "--state-dir", temp, "ingest", "--file", str(fixture),
                    "--url", "http://127.0.0.1:1/v1/events",
                ])
            finally:
                cli_module.MAX_SPOOL_BYTES = original
            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["mode"], "dropped")
            self.assertTrue(result["data"]["telemetry_lost"])
            self.assertEqual(list(Path(temp, "spool").glob("*.jsonl")), [])

    def test_spool_flush_replays_and_removes_only_delivered_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "events.jsonl"
            value = event_mapping()
            value["event_id"] = "flush-1"
            fixture.write_text(json.dumps(value) + "\n", encoding="utf-8")
            self.run_cli([
                "--state-dir", temp,
                "ingest",
                "--file", str(fixture),
                "--url", "http://127.0.0.1:1/v1/events",
            ])
            self.assertEqual(len(list(Path(temp, "spool").glob("*.jsonl"))), 1)
            code, result = self.run_cli(["--state-dir", temp, "flush", "--offline"])
            self.assertEqual(code, 0)
            self.assertEqual(result["data"]["removed"], 1)
            self.assertEqual(list(Path(temp, "spool").glob("*.jsonl")), [])
            with EventStore(Path(temp, "data", "events.sqlite3")) as store:
                self.assertIsNotNone(store.get("flush-1"))

    def test_offline_flush_retains_spool_when_store_capacity_rejects_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self.run_cli(["--state-dir", temp, "install"])
            fixture = Path(temp) / "events.jsonl"
            value = event_mapping()
            value["event_id"] = "flush-capacity-1"
            fixture.write_text(json.dumps(value) + "\n", encoding="utf-8")
            self.run_cli([
                "--state-dir", temp,
                "ingest",
                "--file", str(fixture),
                "--url", "http://127.0.0.1:1/v1/events",
            ])
            # The normal CLI uses its configured default; shrink the database
            # guard through the process environment for this bounded replay.
            import os
            previous = os.environ.get("OBSERVATORY_MAX_DATABASE_BYTES")
            os.environ["OBSERVATORY_MAX_DATABASE_BYTES"] = "1"
            try:
                code, result = self.run_cli(["--state-dir", temp, "flush", "--offline"])
            finally:
                if previous is None:
                    os.environ.pop("OBSERVATORY_MAX_DATABASE_BYTES", None)
                else:
                    os.environ["OBSERVATORY_MAX_DATABASE_BYTES"] = previous
            self.assertEqual(code, 5)
            self.assertEqual(result["data"]["removed"], 0)
            self.assertEqual(len(list(Path(temp, "spool").glob("*.jsonl"))), 1)


if __name__ == "__main__":
    unittest.main()
