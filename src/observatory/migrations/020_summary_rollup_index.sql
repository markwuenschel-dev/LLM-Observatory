-- Cover the /v1/summary rollup so it stops scanning the events table.
--
-- `events` stores the whole event envelope in `payload_json` (mean 3037 bytes
-- against a 4096-byte page), so nearly every row fills a leaf page of its own
-- and the table b-tree is gigabytes wide. `summary()` reads only narrow
-- analytic columns, but with no index covering them SQLite had to walk that
-- whole b-tree -- twice, once for the aggregate row and once for the
-- usage_source histogram. Measured on the live store (618,219 events, 6.7 GB):
-- aggregate 1.79 s + histogram 1.21 s, so `store.summary()` took 2.78 s against
-- the 2.0 s dashboard read budget and the primary dashboard route returned 503
-- with no concurrent load at all. The cost tracks bytes stored rather than
-- events summarized, so it worsens with every append.
--
-- Every column `summary()` reads is listed here, which is what makes the scan a
-- COVERING one. Adding an aggregate to `summary()` without adding its column
-- here silently drops the plan back to a full table scan, so
-- test_store.py::test_summary_reads_are_covered_by_the_rollup_index plans the
-- statements `summary()` actually issues and fails when that happens.
--
-- The leading pair is chosen, not incidental. `project_id` first because an
-- equality filter on it must be a seek: sqlite_stat1 records only an average
-- (572 rows per project on the live store) and this deployment is 97.2%
-- `project:unknown` -- 601,104 of 618,219 rows -- so without a covering index
-- to seek into, the planner picked `idx_events_project_observed` and paid
-- 601,104 random row fetches, 5.89 s. SQLite here is built without STAT4, so no
-- amount of ANALYZE can teach it that skew; the index has to make the good plan
-- the cheap one. `observed_at` second so a windowed summary stays proportional
-- to its window rather than to the store: a one-hour window costs 0.021 s and a
-- full-store scan 0.481 s. Ordering the pair the other way left the
-- `project_id` filter at 3.02 s, still over budget.
CREATE INDEX IF NOT EXISTS idx_events_summary_rollup ON events (
    project_id,
    observed_at,
    status,
    usage_source,
    provider,
    model,
    input_tokens,
    output_tokens,
    cached_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    reasoning_tokens,
    compaction_count,
    cost,
    latency_ms,
    time_to_first_token_ms,
    duration_ms,
    context_size,
    context_utilization,
    concurrency,
    parallel_utilization,
    retry_count,
    rate_limited,
    timeout,
    tool_failure,
    agent_failure,
    aborted,
    reassessment_count,
    rework_count,
    tool_call_count,
    files_inspected_count,
    files_changed_count,
    commands_executed_count,
    tests_invoked_count
);
