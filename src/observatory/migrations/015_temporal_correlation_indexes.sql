-- Indexes for bounded temporal association.
--
-- The association query selects events in one project within a time window.
-- Without a composite index SQLite fell back to the outcome_kind index, which
-- is useless here because almost every row has outcome_kind NULL: each lookup
-- scanned the whole events table and sorted in a temp B-tree. Correlation runs
-- on the append path, so that cost was paid on every ingested commit and it
-- starved intake, which uses a short busy timeout precisely so it never blocks
-- a client.
CREATE INDEX IF NOT EXISTS idx_events_project_observed ON events(project_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_events_session_observed ON events(session_id, observed_at);
