-- Preserve bounded agent-activity counts as queryable normalized dimensions.
ALTER TABLE events ADD COLUMN tool_call_count REAL;
ALTER TABLE events ADD COLUMN files_inspected_count REAL;
ALTER TABLE events ADD COLUMN files_changed_count REAL;
ALTER TABLE events ADD COLUMN commands_executed_count REAL;
ALTER TABLE events ADD COLUMN tests_invoked_count REAL;

CREATE INDEX IF NOT EXISTS idx_events_behavior_counts ON events (
    tool_call_count,
    files_inspected_count,
    files_changed_count,
    commands_executed_count,
    tests_invoked_count
);
