from datetime import datetime, timezone
import sqlite3
import tempfile
import unittest

from observatory.contracts import NormalizedEvent
from observatory.outcomes import make_outcome_event
from observatory.store import EventStore, StorageCapacityError

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

    def test_store_capacity_rejects_before_persisting_an_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3", max_bytes=1) as store:
                self.assertTrue(store.capacity()["exhausted"])
                with self.assertRaises(StorageCapacityError):
                    store.append(make_event("capacity-1"))

    def test_duplicate_replay_is_idempotent_even_when_capacity_is_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                event = make_event("capacity-duplicate-1")
                self.assertEqual(store.append(event).status, "inserted")
                store.max_bytes = 1
                self.assertEqual(store.append(event).status, "duplicate")

    def test_measurement_projection_preserves_field_level_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = make_event("provenance-usage-1").to_mapping()
                value["usage"] = {"input_tokens": 4, "output_tokens": 3, "total_tokens": 7, "source": "provider"}
                value["provenance"] = {"fields": {"usage": "provider", "usage.total_tokens": "derived"}}
                event = NormalizedEvent.from_mapping(value)
                self.assertEqual(store.append(event).status, "inserted")
                facts = {item["field_path"]: item for item in store.measurement_facts(event_id=event.event_id)}
                self.assertEqual(facts["usage.input_tokens"]["evidence_source"], "provider")
                self.assertEqual(facts["usage.input_tokens"]["evidence_quality"], "reported")
                self.assertEqual(facts["usage.total_tokens"]["evidence_source"], "derived")
                self.assertEqual(facts["usage.total_tokens"]["evidence_quality"], "derived")

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
                self.assertIsNone(summary["average_context_size"])
                self.assertIsNone(summary["cost"])
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

    def test_agent_behavior_is_persisted_aggregated_and_projected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = event_mapping()
                value["event_id"] = "behavior-1"
                value["behavior"] = {
                    "tool_call_count": 4,
                    "tool_names": ["shell", "grep"],
                    "files_inspected_count": 8,
                    "files_changed_count": 2,
                    "commands_executed_count": 3,
                    "tests_invoked_count": 1,
                }
                value["reliability"] = {
                    "agent_failure": True,
                    "reassessment_count": 2,
                    "rework_count": 1,
                }
                value["provenance"] = {"fields": {
                    "behavior.files_changed_count": "client",
                    "reliability.agent_failure": "client",
                    "reliability.reassessment_count": "client",
                    "reliability.rework_count": "client",
                }}
                event = NormalizedEvent.from_mapping(value)
                self.assertEqual(store.append(event).status, "inserted")

                stored = store.get("behavior-1")
                self.assertIsNotNone(stored)
                self.assertEqual(stored.behavior.tool_names, ("shell", "grep"))
                summary = store.summary()
                self.assertEqual(summary["tool_calls"], 4)
                self.assertEqual(summary["files_inspected"], 8)
                self.assertEqual(summary["files_changed"], 2)
                self.assertEqual(summary["commands_executed"], 3)
                self.assertEqual(summary["tests_invoked"], 1)
                self.assertEqual(summary["agent_failures"], 1)
                self.assertEqual(summary["reassessments"], 2)
                self.assertEqual(summary["rework_loops"], 1)
                provider = store.metric_dimensions()["provider_model"][0]
                context = store.metric_dimensions()["context"][0]
                for row in (provider, context):
                    self.assertEqual(row["tool_calls"], 4)
                    self.assertEqual(row["files_inspected"], 8)
                    self.assertEqual(row["files_changed"], 2)
                    self.assertEqual(row["commands_executed"], 3)
                    self.assertEqual(row["tests_invoked"], 1)
                    self.assertEqual(row["agent_failures"], 1)
                    self.assertEqual(row["reassessments"], 2)
                    self.assertEqual(row["rework_loops"], 1)
                facts = {item["field_path"]: item for item in store.measurement_facts(event_id="behavior-1")}
                self.assertEqual(facts["behavior.files_changed_count"]["evidence_source"], "client")
                self.assertEqual(facts["reliability.reassessment_count"]["evidence_source"], "client")

    def test_model_variant_is_persisted_filtered_and_aggregated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                value = make_event("variant-1").to_mapping()
                value["llm"]["model_variant"] = "2026-08-07"
                event = NormalizedEvent.from_mapping(value)
                self.assertEqual(store.append(event).status, "inserted")
                self.assertEqual(store.get("variant-1").llm.model_variant, "2026-08-07")
                self.assertEqual(store.list_events({"model_variant": "2026-08-07"})[0].event_id, "variant-1")
                self.assertEqual(store.metric_dimensions()["provider_model"][0]["model_variant"], "2026-08-07")
                self.assertEqual(store.comparison({"model_variant": "2026-08-07"})[0]["model_variant"], "2026-08-07")

    def test_prometheus_model_dimensions_exclude_non_model_events_and_context_keeps_type(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                model = make_event("metric-model-1")
                tool_value = make_event("metric-tool-1").to_mapping()
                tool_value["event_type"] = "tool.operation"
                outcome_value = make_outcome_event("tests", "passed", correlation_id="metric-run-1").to_mapping()
                self.assertEqual(store.append(model).status, "inserted")
                self.assertEqual(store.append(NormalizedEvent.from_mapping(tool_value)).status, "inserted")
                self.assertEqual(store.append(NormalizedEvent.from_mapping(outcome_value)).status, "inserted")
                dimensions = store.metric_dimensions()
                self.assertEqual(dimensions["provider_model"][0]["count"], 1)
                self.assertEqual({item["event_type"] for item in dimensions["context"]}, {"model.operation", "tool.operation", "outcome.tests"})
                outcome_dimension = dimensions["outcome"][0]
                self.assertEqual(outcome_dimension["kind"], "tests")
                self.assertEqual(outcome_dimension["evidence_source"], "unknown")
                self.assertIn("project", outcome_dimension)

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

    def test_task_outcome_correlation_uses_explicit_basis_without_correlation_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                model_value = make_event("task-model-2").to_mapping()
                model_value["execution"] = {"task_id": "task-without-run-id"}
                outcome = make_outcome_event(
                    "tests",
                    "passed",
                    correlation_basis="task_id",
                    task_id="task-without-run-id",
                    evidence_source="ci",
                )
                model = NormalizedEvent.from_mapping(model_value)
                self.assertEqual(store.append(model).status, "inserted")
                self.assertEqual(store.append(outcome).status, "inserted")
                self.assertTrue(any(edge["relation"] == "outcome_correlation" for edge in store.attribution_edges(event_id=outcome.event_id)))

    def test_session_and_event_id_outcome_correlation_are_order_independent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                outcome = make_outcome_event(
                    "tests",
                    "passed",
                    correlation_id="session-correlation",
                    correlation_basis="session_id",
                    evidence_source="ci",
                )
                self.assertEqual(store.append(outcome).status, "inserted")
                model_value = make_event("session-model-1").to_mapping()
                model_value["execution"] = {"session_id": "session-correlation"}
                model = NormalizedEvent.from_mapping(model_value)
                self.assertEqual(store.append(model).status, "inserted")
                self.assertTrue(any(edge["relation"] == "outcome_correlation" for edge in store.attribution_edges(event_id=model.event_id)))

                event_target = make_event("event-target-1")
                self.assertEqual(store.append(event_target).status, "inserted")
                event_outcome = make_outcome_event(
                    "review",
                    "accepted",
                    correlation_id=event_target.event_id,
                    correlation_basis="event_id",
                    evidence_source="operator",
                )
                self.assertEqual(store.append(event_outcome).status, "inserted")
                self.assertTrue(any(edge["relation"] == "outcome_correlation" for edge in store.attribution_edges(event_id=event_outcome.event_id)))

    def test_time_filters_normalize_utc_offsets_and_comparison_keeps_usage_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                first_value = make_event("range-1").to_mapping()
                first_value["usage"]["source"] = "provider"
                second_value = make_event("range-2").to_mapping()
                second_value["usage"]["source"] = "client"
                store.append(NormalizedEvent.from_mapping(first_value))
                store.append(NormalizedEvent.from_mapping(second_value))
                self.assertEqual(len(store.list_events({"start": "2026-08-07T10:00:00-04:00", "end": "2026-08-07T14:01:00Z"})), 2)
                self.assertEqual(len(store.list_events({"start": "2026-08-07T14:00:01Z", "end": "2026-08-07T14:00:02Z"})), 2)
                with self.assertRaises(ValueError):
                    store.list_events({"start": "not-a-timestamp"})
                with self.assertRaises(ValueError):
                    store.list_events({"start": "2026-08-07T14:00:03Z", "end": "2026-08-07T14:00:02Z"})
                rows = store.comparison({"provider": "openai"})
                self.assertEqual({row["usage_source"] for row in rows}, {"provider", "client"})

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
                    "subagent_id": "subagent-1",
                    "parent_agent_id": "orchestrator-1",
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
                self.assertEqual({item["relation"] for item in edges}, {"project", "parent_event", "session", "workflow", "agent", "subagent", "parent_agent"})
                parent_agent_edge = next(item for item in edges if item["relation"] == "parent_agent")
                self.assertEqual(parent_agent_edge["target_id"], "orchestrator-1")
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


class SummaryReadBudgetTests(unittest.TestCase):
    """Pin the plan `summary()` gets, not the wall-clock it happens to take.

    `/v1/summary` is the primary dashboard route and it runs against a 2.0 s
    read budget (`api.DEFAULT_READ_TIMEOUT`). The events table is mostly
    `payload_json`, so a row costs about a page to touch and any plan that
    reaches the table rather than an index is linear in bytes stored: on the
    live store that was 2.78 s unfiltered and 5.89 s filtered by project, both
    served to the operator as a 503. Timing that on a fixture proves nothing --
    a hundred rows are fast however they are read -- so these tests assert the
    property that stops being true first: that every read `summary()` issues is
    answered entirely out of the migration 020 indexes.
    """

    # Shapes the dashboard actually issues, and the ones measured broken: no
    # filter, a project, and a time window. A selective dimension such as
    # `provider` is deliberately absent -- its own index is the right plan
    # there, and asserting coverage would forbid a correct choice.
    FILTERS = (
        {},
        {"project": "project:fixture"},
        {"start": "2026-08-07T00:00:00Z"},
        {"start": "2026-08-07T00:00:00Z", "end": "2026-08-08T00:00:00Z"},
        {"project": "project:fixture", "start": "2026-08-07T00:00:00Z", "end": "2026-08-08T00:00:00Z"},
    )

    LIVE_EVENTS = 618_219

    def _seeded(self, temp: str) -> EventStore:
        store = EventStore(f"{temp}/events.sqlite3")
        for index in range(120):
            store.append(make_event(f"summary-plan-{index}"))
        store.append(make_outcome_event("ci", "passed", correlation_id="summary-plan-run", evidence_source="ci"))
        return store

    def _describe_a_large_store(self, store: EventStore) -> None:
        """Tell the planner the table is live-sized without building one.

        The bad plans only appear on a large table, so a fixture cannot
        reproduce them by being seeded harder -- a hundred rows are cheap to
        read any way at all, and SQLite picks accordingly. Statistics are what
        the planner actually consults, and `sqlite_stat1` is an ordinary table,
        so describing the live store's shape there reproduces the live store's
        plans exactly: 618,219 events, `project_id` skewed across 13 values,
        `usage_source` across 5. Checked against the real database -- every plan
        this asserts is the plan the live store produces.
        """

        rows = [("events", "sqlite_autoindex_events_1", f"{self.LIVE_EVENTS} 1")]
        distinct = {"project_id": 13, "observed_at": self.LIVE_EVENTS // 2, "usage_source": 5, "received_at": self.LIVE_EVENTS}
        indexes = [
            str(row["name"])
            for row in store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
                " AND tbl_name = 'events' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        for index in indexes:
            prefix, stat = 1, [str(self.LIVE_EVENTS)]
            for column in store.connection.execute(f"PRAGMA index_info({index})"):
                prefix *= max(1, distinct.get(str(column["name"]), 300))
                stat.append(str(max(1, self.LIVE_EVENTS // min(prefix, self.LIVE_EVENTS))))
            rows.append(("events", index, " ".join(stat)))
        store.connection.execute("ANALYZE")
        store.connection.execute("DELETE FROM sqlite_stat1 WHERE tbl = 'events'")
        store.connection.executemany("INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES (?, ?, ?)", rows)
        store.connection.commit()
        # Statistics are cached at prepare time; this reloads them.
        store.connection.execute("ANALYZE sqlite_master")

    @staticmethod
    def _plan(store: EventStore, sql: str, params) -> list[str]:
        return [str(row["detail"]) for row in store.connection.execute(f"EXPLAIN QUERY PLAN {sql}", params)]

    def test_summary_reads_are_covered_by_the_rollup_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with self._seeded(temp) as store:
                self._describe_a_large_store(store)
                for filters in self.FILTERS:
                    statements = store._summary_statements(filters)
                    for name in ("aggregate", "usage_sources"):
                        sql, params = statements[name]
                        steps = self._plan(store, sql, params)
                        touching = [step for step in steps if "events" in step and "outcome_events" not in step]
                        self.assertTrue(touching, f"{name} {filters}: no step reads events: {steps}")
                        for step in touching:
                            self.assertTrue(
                                any(
                                    f"COVERING INDEX {index}" in step
                                    for index in ("idx_events_summary_rollup", "idx_events_usage_source_window")
                                ),
                                f"{name} {filters} left the covering indexes: {steps}",
                            )

    def test_summary_outcomes_probe_events_by_identity_not_by_listing_them(self) -> None:
        """A filtered summary must not materialize every matching event id.

        `event_id IN (SELECT event_id FROM events WHERE ...)` did exactly that:
        one random row fetch per matching event to build a list only a few
        hundred outcomes ever probe. On the live store a one-day window made
        that single sub-query cost 1.96 s of the 2.0 s budget on its own.
        """

        with tempfile.TemporaryDirectory() as temp:
            with self._seeded(temp) as store:
                for filters in self.FILTERS:
                    sql, params = store._summary_statements(filters)["outcomes"]
                    steps = self._plan(store, sql, params)
                    joined = " ; ".join(steps)
                    self.assertNotIn("LIST SUBQUERY", joined, f"{filters}: outcomes listed event ids: {steps}")
                    self.assertTrue(
                        any("SEARCH events" in step and "event_id=?" in step for step in steps),
                        f"{filters}: outcomes did not probe events by event_id: {steps}",
                    )

    def test_rollup_index_lists_every_column_summary_reads(self) -> None:
        """The covering property, asserted without asking the query planner.

        A fixture is small enough that SQLite might pick a covering scan for
        reasons unrelated to coverage, so check the containment directly: every
        `events` column the summary statements mention has to appear in the
        index, or the plan degrades to a table scan on a store large enough to
        matter.
        """

        import re

        with tempfile.TemporaryDirectory() as temp:
            with self._seeded(temp) as store:
                columns = {row["name"] for row in store.connection.execute("PRAGMA table_info(events)")}
                indexed = {
                    row["name"]
                    for index in ("idx_events_summary_rollup", "idx_events_usage_source_window")
                    for row in store.connection.execute(f"PRAGMA index_info({index})")
                }
                self.assertTrue(indexed, "migration 020 did not create the summary rollup indexes")
                referenced: set[str] = set()
                for filters in self.FILTERS:
                    statements = store._summary_statements(filters)
                    for name in ("aggregate", "usage_sources"):
                        sql = statements[name][0]
                        referenced |= {token for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql) if token in columns}
                self.assertLessEqual(
                    referenced,
                    indexed,
                    f"summary() reads columns missing from the rollup indexes: {sorted(referenced - indexed)}",
                )

    def test_rollup_index_changes_no_reported_value(self) -> None:
        """The index and the rewritten outcome join are pure performance work."""

        with tempfile.TemporaryDirectory() as temp:
            with self._seeded(temp) as store:
                with_index = {repr(f): store.summary(f) for f in self.FILTERS}
                store.connection.execute("DROP INDEX idx_events_summary_rollup")
                store.connection.execute("DROP INDEX idx_events_usage_source_window")
                without_index = {repr(f): store.summary(f) for f in self.FILTERS}
                self.assertEqual(with_index, without_index)
                self.assertGreater(with_index["{}"]["events"], 0)
                self.assertTrue(any(value["outcomes"] for value in with_index.values()))
