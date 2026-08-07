from datetime import datetime, timezone
import sqlite3
import tempfile
import unittest

from observatory.contracts import NormalizedEvent
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore

from tests.test_contracts import event_mapping


def make_event(event_id: str, *, provider: str = "openai", status: str = "success") -> NormalizedEvent:
    value = event_mapping()
    value["event_id"] = event_id
    value["llm"] = {"provider": provider, "model": "model-a", "client": "fixture"}
    value["reliability"] = {"status": status}
    value["observed_at"] = f"2026-08-07T14:00:{int(event_id[-1]):02d}Z"
    return NormalizedEvent.from_mapping(value, received_at=datetime.now(timezone.utc))


class EventStoreTests(unittest.TestCase):
    def test_insert_duplicate_and_conflict_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                event = make_event("evt-1")
                self.assertEqual(store.append(event).status, "inserted")
                self.assertEqual(store.append(event).status, "duplicate")
                conflicting = make_event("evt-1", provider="anthropic")
                result = store.append(conflicting)
                self.assertEqual(result.status, "conflict")
                self.assertEqual(store.conflict_count("evt-1"), 1)
                self.assertEqual(store.get("evt-1").llm.provider, "openai")

    def test_summary_and_filters_are_dimensioned(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(make_event("evt-1", provider="openai", status="success"))
                store.append(make_event("evt-2", provider="anthropic", status="failed"))
                summary = store.summary()
                self.assertEqual(summary["events"], 2)
                self.assertEqual(summary["successes"], 1)
                self.assertEqual(summary["failures"], 1)
                self.assertEqual(summary["usage_sources"], {"client": 2})
                filtered = store.list_events({"provider": "anthropic"})
                self.assertEqual([item.event_id for item in filtered], ["evt-2"])

    def test_summary_and_context_dimensions_include_optional_usage_and_performance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = event_mapping()
                value["event_id"] = "analytics-1"
                value["usage"] = {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "cached_tokens": 2,
                    "cache_creation_tokens": 3,
                    "cache_read_tokens": 4,
                    "context_size": 100,
                    "context_utilization": 0.5,
                    "compaction_count": 2,
                    "source": "provider",
                }
                value["performance"] = {
                    "latency_ms": 42,
                    "time_to_first_token_ms": 12,
                    "duration_ms": 100,
                    "concurrency": 2,
                    "parallel_utilization": 0.75,
                }
                event = NormalizedEvent.from_mapping(value)
                self.assertEqual(store.append(event).status, "inserted")
                summary = store.summary()
                self.assertEqual(summary["cache_creation_tokens"], 3)
                self.assertEqual(summary["cache_read_tokens"], 4)
                self.assertEqual(summary["compactions"], 2)
                self.assertEqual(summary["average_time_to_first_token_ms"], 12)
                self.assertEqual(summary["average_duration_ms"], 100)
                self.assertEqual(summary["average_context_size"], 100)
                self.assertEqual(summary["average_context_utilization"], 0.5)
                self.assertEqual(summary["average_concurrency"], 2)
                self.assertEqual(summary["average_parallel_utilization"], 0.75)
                context = store.metric_dimensions()["context"][0]
                self.assertEqual(context["cache_creation_tokens"], 3)
                self.assertEqual(context["cache_read_tokens"], 4)
                self.assertEqual(context["compactions"], 2)
                self.assertEqual(context["average_time_to_first_token_ms"], 12)
                self.assertEqual(context["average_duration_ms"], 100)
                self.assertEqual(context["average_context_size"], 100)
                self.assertEqual(context["average_context_utilization"], 0.5)
                self.assertEqual(context["average_concurrency"], 2)
                self.assertEqual(context["average_parallel_utilization"], 0.75)

    def test_summary_filters_correlated_outcomes_with_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                first = make_event("evt-1", provider="anthropic")
                first_value = first.to_mapping()
                first_value["outcome"] = {
                    "kind": "tests",
                    "status": "passed",
                    "correlation_id": "run-1",
                    "evidence_source": "ci",
                }
                store.append(NormalizedEvent.from_mapping(first_value))
                second = make_event("evt-2", provider="openai")
                second_value = second.to_mapping()
                second_value["outcome"] = first_value["outcome"]
                store.append(NormalizedEvent.from_mapping(second_value))
                self.assertEqual(
                    store.summary({"provider": "anthropic"})["outcomes"],
                    [{"kind": "tests", "status": "passed", "count": 1}],
                )

    def test_store_redacts_before_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = event_mapping()
                value["event_id"] = "evt-3"
                value["prompt"] = "STORE_CANARY"
                event = NormalizedEvent.from_mapping(value)
                store.append(event)
                raw = store.connection.execute("SELECT payload_json FROM events").fetchone()[0]
                self.assertNotIn("STORE_CANARY", raw)

    def test_unknown_filter_and_bad_limit_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                with self.assertRaises(ValueError):
                    store.summary({"arbitrary_sql": "1"})
                trace_value = make_event("trace-filter-1")
                trace_mapping = trace_value.to_mapping()
                trace_mapping["execution"] = {"trace_id": "trace-filter-1", "span_id": "span-filter-1"}
                self.assertEqual(store.append(NormalizedEvent.from_mapping(trace_mapping)).status, "inserted")
                self.assertEqual(store.list_events({"trace_id": "trace-filter-1"})[0].event_id, "trace-filter-1")
                self.assertEqual(store.list_events({"span_id": "span-filter-1"})[0].event_id, "trace-filter-1")
                with self.assertRaises(ValueError):
                    store.list_events(limit=0)

    def test_append_projects_immutable_facts_outcomes_and_bitemporal_edges(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = event_mapping()
                value["event_id"] = "ledger-1"
                value["project"] = {"project_id": "repo:example", "repository": "example", "branch": "main"}
                value["execution"] = {
                    "parent_event_id": "parent-1",
                    "session_id": "session-1",
                    "workflow_id": "workflow-1",
                    "agent_id": "agent-1",
                }
                value["performance"] = {"latency_ms": 42}
                value["provenance"] = {"fields": {"performance.latency_ms": "derived"}, "adapter": "fixture"}
                value["outcome"] = {"kind": "test", "status": "passed", "correlation_id": "run-1", "correlation_basis": "task_id", "evidence_source": "ci"}
                event = NormalizedEvent.from_mapping(value)

                self.assertEqual(store.append(event).status, "inserted")
                self.assertEqual(store.ledger_count(), 1)
                self.assertEqual(store.measurement_count(), 3)
                self.assertEqual(store.outcome_count(), 1)
                self.assertEqual(store.outcomes()[0]["correlation_basis"], "task_id")
                edges = store.attribution_edges(event_id="ledger-1")
                self.assertEqual({item["relation"] for item in edges}, {"project", "parent_event", "session", "workflow", "agent"})
                latency = next(item for item in store.measurement_facts(event_id="ledger-1") if item["field_path"] == "performance.latency_ms")
                self.assertEqual(latency["evidence_quality"], "derived")
                self.assertEqual(store.append(event).status, "duplicate")
                self.assertEqual(store.ledger_count(), 2)
                self.assertEqual(store.measurement_count(), 3)

                with self.assertRaises(sqlite3.IntegrityError):
                    store.connection.execute("UPDATE measurement_facts SET unit = 'bad' WHERE event_id = 'ledger-1'")
                with self.assertRaises(sqlite3.IntegrityError):
                    store.connection.execute("DELETE FROM attribution_edges WHERE child_event_id = 'ledger-1'")

    def test_same_task_id_creates_explicit_outcome_correlation_edges(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                model_value = make_event("task-model-1").to_mapping()
                model_value["execution"] = {"task_id": "task-1"}
                model = NormalizedEvent.from_mapping(model_value)
                outcome = make_outcome_event("tests", "passed", correlation_id="run-1", task_id="task-1", evidence_source="ci")
                self.assertEqual(store.append(model).status, "inserted")
                self.assertEqual(store.append(outcome).status, "inserted")
                edges = store.attribution_edges(event_id=outcome.event_id)
                self.assertTrue(any(edge["relation"] == "outcome_correlation" and edge["parent_event_id"] == model.event_id for edge in edges))
                model_edges = store.attribution_edges(event_id=model.event_id)
                self.assertTrue(any(edge["relation"] == "outcome_correlation" and edge["parent_event_id"] == outcome.event_id for edge in model_edges))

    def test_startup_backfills_missing_evidence_projections(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = f"{temp}/events.sqlite3"
            with EventStore(database) as store:
                event = make_event("legacy-1")
                self.assertEqual(store.append(event).status, "inserted")
                for trigger in (
                    "prevent_ingest_ledger_delete",
                    "prevent_measurement_facts_delete",
                    "prevent_outcome_events_delete",
                    "prevent_attribution_edges_delete",
                ):
                    store.connection.execute(f"DROP TRIGGER {trigger}")
                store.connection.execute("DELETE FROM attribution_edges WHERE child_event_id = ?", (event.event_id,))
                store.connection.execute("DELETE FROM measurement_facts WHERE event_id = ?", (event.event_id,))
                store.connection.execute("DELETE FROM outcome_events WHERE event_id = ?", (event.event_id,))
                store.connection.execute("DELETE FROM ingest_ledger WHERE event_id = ?", (event.event_id,))
                store.connection.commit()
            with EventStore(database) as restored:
                self.assertGreater(restored.measurement_count(), 0)
                self.assertEqual(restored.ledger_count(), 1)
                self.assertEqual(restored.connection.execute("SELECT decision FROM ingest_ledger").fetchone()[0], "backfill")
                self.assertTrue(restored.attribution_edges(event_id="legacy-1"))


if __name__ == "__main__":
    unittest.main()
