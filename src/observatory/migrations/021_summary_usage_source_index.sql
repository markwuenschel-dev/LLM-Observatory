-- Cover the second read /v1/summary issues: the usage_source histogram.
--
-- Split from 020 rather than appended to it. 020 was already recorded in
-- `schema_migrations` on the live store before this index was written, and
-- `initialize()` skips any version it finds recorded (store.py, the
-- `SELECT 1 FROM schema_migrations WHERE version = ?` guard), so a statement
-- added to an applied file runs on new stores and never on the one that
-- needs it. A new version is the only thing that reaches both.
--
-- The histogram is narrow enough that the planner will not always take the
-- wide rollup index of 020 for it: given a one-sided `observed_at >= ?` it
-- preferred a range over `idx_events_observed_at` plus one row fetch per
-- match, which on the live store cost 1.25 s for a 30-day window while the
-- aggregate beside it, needing 33 columns, stayed on the covering index at
-- 0.30 s. Two columns is the whole of what this read touches, so a cover its
-- own size makes the choice stop mattering: the same window then costs
-- 0.05 s, and the worst summary shape measured across every supported filter
-- falls from 1.70 s to 0.62 s against the 2.0 s read budget.
CREATE INDEX IF NOT EXISTS idx_events_usage_source_window
    ON events (observed_at, usage_source);
