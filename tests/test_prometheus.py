from datetime import datetime, timezone
import tempfile
import unittest
from unittest.mock import patch

from observatory.contracts import NormalizedEvent
from observatory.prometheus import (
    PrometheusQueryEngine,
    PrometheusQueryError,
)
from observatory.store import EventStore

from tests.test_contracts import event_mapping


def _event(*, event_id: str, observed_at: str, **overrides) -> NormalizedEvent:
    value = event_mapping()
    value["event_id"] = event_id
    value["observed_at"] = observed_at
    value.update(overrides)
    return NormalizedEvent.from_mapping(value, received_at=datetime(2026, 8, 7, 15, tzinfo=timezone.utc))


def _engine(store: EventStore) -> PrometheusQueryEngine:
    return PrometheusQueryEngine(store)


class PrometheusFacadeTests(unittest.TestCase):
    def test_sum_collapses_series_instead_of_hitting_the_matrix_cap(self) -> None:
        """Grafana's Observed-events panel is sum() over a 21-label metric.

        Materializing one series per label set trips _MAX_MATRIX_SERIES before
        the sum can run. The facade must evaluate the aggregate in SQL.
        """

        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                for index in range(4):
                    store.append(
                        _event(
                            event_id=f"sum-cap-{index}",
                            observed_at="2026-08-07T14:00:00Z",
                            llm={"provider": "openai", "model": f"model-{index}", "client": "fixture"},
                        )
                    )
                engine = _engine(store)
                params = {
                    "start": ["2026-08-07T14:00:00Z"],
                    "end": ["2026-08-07T14:10:00Z"],
                    "step": ["300"],
                }
                with patch("observatory.prometheus._MAX_MATRIX_SERIES", 3):
                    result = engine.query_range("sum(observatory_events_by_context_total)", params)
                self.assertEqual(result["status"], "success")
                values = [float(point[1]) for point in result["data"]["result"][0]["values"]]
                self.assertEqual(values[-1], 4.0)

    def test_matrix_timestamps_are_json_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(_event(event_id="ts-1", observed_at="2026-08-07T14:00:00Z"))
                engine = _engine(store)
                result = engine.query_range(
                    "sum(observatory_events_by_context_total)",
                    {
                        "start": ["2026-08-07T14:00:00Z"],
                        "end": ["2026-08-07T14:10:00Z"],
                        "step": ["300"],
                    },
                )
                point = result["data"]["result"][0]["values"][0]
                self.assertIsInstance(point[0], (int, float))
                self.assertNotIsInstance(point[0], bool)
                self.assertIsInstance(point[1], str)

    def test_sum_treats_grafana_all_token_as_unbounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(_event(event_id="all-1", observed_at="2026-08-07T14:00:00Z"))
                engine = _engine(store)
                params = {
                    "start": ["2026-08-07T14:00:00Z"],
                    "end": ["2026-08-07T14:10:00Z"],
                    "step": ["300"],
                }
                result = engine.query_range(
                    'sum(observatory_events_by_context_total{project=~"$__all",event_type=~"$__all"})',
                    params,
                )
                self.assertEqual(float(result["data"]["result"][0]["values"][-1][1]), 1.0)

    def test_sum_applies_grafana_all_and_alternation_matchers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(_event(event_id="m1", observed_at="2026-08-07T14:00:00Z", event_type="model.operation"))
                store.append(_event(event_id="t1", observed_at="2026-08-07T14:05:00Z", event_type="tool.operation"))
                store.append(_event(event_id="l1", observed_at="2026-08-07T14:09:00Z", event_type="telemetry.log"))
                engine = _engine(store)
                params = {
                    "query": ['sum(observatory_events_by_context_total{event_type=~".*",project=~".*"})'],
                    "start": ["2026-08-07T14:00:00Z"],
                    "end": ["2026-08-07T14:10:00Z"],
                    "step": ["300"],
                }
                all_events = engine.query_range(params["query"][0], params)
                self.assertEqual(float(all_events["data"]["result"][0]["values"][-1][1]), 3.0)
                ops = engine.query_range(
                    'sum(observatory_events_by_context_total{event_type=~"model.operation|tool.operation"})',
                    params,
                )
                self.assertEqual(float(ops["data"]["result"][0]["values"][-1][1]), 2.0)

    def test_sum_by_usage_source_keeps_one_series_per_group(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                first = event_mapping()
                first["event_id"] = "u1"
                first["observed_at"] = "2026-08-07T14:00:00Z"
                first["usage"] = {"input_tokens": 1, "output_tokens": 1, "source": "provider"}
                store.append(NormalizedEvent.from_mapping(first))
                second = event_mapping()
                second["event_id"] = "u2"
                second["observed_at"] = "2026-08-07T14:05:00Z"
                second["usage"] = {"input_tokens": 1, "output_tokens": 1, "source": "client"}
                store.append(NormalizedEvent.from_mapping(second))
                engine = _engine(store)
                result = engine.query_range(
                    "sum by (usage_source) (observatory_events_by_context_total)",
                    {
                        "start": ["2026-08-07T14:00:00Z"],
                        "end": ["2026-08-07T14:10:00Z"],
                        "step": ["300"],
                    },
                )
                sources = sorted(item["metric"].get("usage_source") for item in result["data"]["result"])
                self.assertEqual(sources, ["client", "provider"])

    def test_instant_query_defaults_to_a_day_not_a_year(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(_event(event_id="old", observed_at="2026-08-05T14:00:00Z"))
                store.append(_event(event_id="new", observed_at="2026-08-07T14:00:00Z"))
                engine = _engine(store)
                result = engine.query(
                    "sum(observatory_events_by_context_total)",
                    {"time": ["2026-08-07T14:10:00Z"]},
                )
                self.assertEqual(float(result["data"]["result"][0]["value"][1]), 1.0)

    def test_unaggregated_selector_still_enforces_the_series_cap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                for index in range(4):
                    store.append(
                        _event(
                            event_id=f"cap-{index}",
                            observed_at="2026-08-07T14:00:00Z",
                            llm={"provider": "openai", "model": f"model-{index}", "client": "fixture"},
                        )
                    )
                engine = _engine(store)
                with patch("observatory.prometheus._MAX_MATRIX_SERIES", 3):
                    with self.assertRaisesRegex(PrometheusQueryError, "4096|3 series"):
                        engine.query_range(
                            "observatory_events_by_context_total",
                            {
                                "start": ["2026-08-07T14:00:00Z"],
                                "end": ["2026-08-07T14:10:00Z"],
                                "step": ["300"],
                            },
                        )

    def test_promql_window_index_covers_the_pushed_down_sum(self) -> None:
        import re as _re

        with tempfile.TemporaryDirectory() as temp:
            with EventStore(f"{temp}/events.sqlite3") as store:
                store.append(_event(event_id="idx-1", observed_at="2026-08-07T14:00:00Z"))
                columns = [row["name"] for row in store.connection.execute("PRAGMA index_info(idx_events_promql_window)")]
                self.assertEqual(columns[0], "observed_at")
                self.assertIn("event_type", columns)
                table_columns = {row["name"] for row in store.connection.execute("PRAGMA table_info(events)")}
                engine = _engine(store)
                sql, _params = engine.matrix_sql(
                    "sum(observatory_events_by_context_total)",
                    start="2026-08-07T14:00:00Z",
                    end="2026-08-07T14:10:00Z",
                    step=300,
                )
                referenced = {token for token in _re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql) if token in table_columns}
                indexed = set(columns)
                self.assertLessEqual(
                    referenced,
                    indexed,
                    f"pushed sum reads columns missing from idx_events_promql_window: {sorted(referenced - indexed)}",
                )
