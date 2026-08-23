-- Index for the capability-window resolution in `engineering_value`.
--
-- A skill is invoked once and then governs the work that follows, so it lands
-- on a separate hook event rather than on the operations it covers. The ranking
-- therefore resolves it by "the most recent invocation in the same session at or
-- before this operation", which is a correlated lookup per associated event.
--
-- Unindexed that measured 48 s against a 2 s read budget on 620k events -- it
-- scanned the table once per linked event. The predicate is highly selective
-- (19 skill-bearing rows out of 620,000 here), so a partial index keeps the
-- lookup proportional to invocations rather than to the store.
CREATE INDEX IF NOT EXISTS idx_events_capability_window
    ON events (session_id, observed_at, skill)
    WHERE skill IS NOT NULL;
