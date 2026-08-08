-- Preserve agent failures and reassessment/rework loop counts as normalized reliability dimensions.
ALTER TABLE events ADD COLUMN agent_failure INTEGER;
ALTER TABLE events ADD COLUMN reassessment_count REAL;
ALTER TABLE events ADD COLUMN rework_count REAL;

CREATE INDEX IF NOT EXISTS idx_events_reliability_dimensions ON events (
    agent_failure,
    reassessment_count,
    rework_count
);
