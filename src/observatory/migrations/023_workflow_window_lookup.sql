-- Companion to 022 for the workflow window.
--
-- `engineering_value` resolves a workflow the same way it resolves a skill:
-- the most recent invocation in the same session at or before the operation.
-- That is a correlated lookup per associated event, and unindexed it cost 48 s
-- for the skill column alone against a 2 s read budget. The predicate is highly
-- selective, so a partial index keeps it proportional to invocations.
CREATE INDEX IF NOT EXISTS idx_events_workflow_window
    ON events (session_id, observed_at, workflow_id)
    WHERE workflow_id IS NOT NULL;
