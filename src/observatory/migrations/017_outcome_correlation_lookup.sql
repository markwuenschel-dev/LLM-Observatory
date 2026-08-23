-- Indexes for the outcome-correlation join used by the analytics surfaces.
--
-- `linked` selects edges by relation and then joins outcome_events on either
-- endpoint. The only relation index was (relation, target_id), which that join
-- cannot use, so a filtered engineering-value query degraded to ~13 s against a
-- 2 s read budget -- a 503 on a flagship route for a common filter.
CREATE INDEX IF NOT EXISTS idx_attribution_relation_child
    ON attribution_edges(relation, child_event_id);
CREATE INDEX IF NOT EXISTS idx_attribution_relation_parent
    ON attribution_edges(relation, parent_event_id);
