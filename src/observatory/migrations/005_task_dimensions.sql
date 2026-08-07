-- Task dimensions remain optional and source-reported; they are not inferred.
ALTER TABLE events ADD COLUMN task_id TEXT;
ALTER TABLE events ADD COLUMN task_class TEXT;
ALTER TABLE events ADD COLUMN timeout INTEGER;
ALTER TABLE events ADD COLUMN tool_failure INTEGER;
ALTER TABLE events ADD COLUMN aborted INTEGER;

CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_task_class ON events(task_class);
CREATE INDEX IF NOT EXISTS idx_events_reliability_flags ON events(timeout, tool_failure, aborted);
