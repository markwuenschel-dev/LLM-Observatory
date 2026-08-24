-- Cover the Grafana PromQL facade's time-window aggregates.
--
-- Dashboard panels are `sum(observatory_*_by_context_total{...})` evaluated
-- at event time. Before pushdown that query grouped by 21 labels and scanned
-- the `events` b-tree (payload_json ~3 KiB/row), so a 24h window of ~500k
-- rows exceeded the 2.0 s dashboard read budget and Grafana painted No data
-- against a full store. After sum()/avg() collapse to GROUP BY time bucket
-- (and optional `by` labels), the remaining cost is the time-range scan.
--
-- `idx_events_observed_at` is only `observed_at`. A matcher on event_type
-- or a SUM of tokens/cost/duration still fetched the table row, which is
-- the payload_json page. Lead with `observed_at` so a window seeks; keep
-- every identity column the 21-label context metric groups by, plus the
-- measure columns sum()/avg() read, so both the pushed-down stats and the
-- remaining topk charts can stay covering. Agent/skill/workflow belong
-- here for that reason: omitting them left topk walking payload_json at
-- ~21s for a 24h window against a 2.0s budget.
--
-- Split from 020. That index leads with `project_id` for `/v1/summary`
-- equality filters. PromQL All-projects is a pure time window, and 020 was
-- already recorded on the live store, so a new version is the only way it
-- reaches both new and existing databases.
CREATE INDEX IF NOT EXISTS idx_events_promql_window ON events (
    observed_at,
    event_type,
    project_id,
    repository,
    branch,
    provider,
    model,
    model_family,
    model_variant,
    client,
    auth_mode,
    route,
    usage_source,
    agent_id,
    subagent_id,
    parent_agent_id,
    role,
    skill,
    lane,
    workflow_id,
    task_class,
    status,
    input_tokens,
    output_tokens,
    total_tokens,
    cached_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    reasoning_tokens,
    cost,
    latency_ms,
    time_to_first_token_ms,
    duration_ms
);
