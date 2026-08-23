-- Make the outcome_events uniqueness constraint actually fire.
--
-- `UNIQUE (event_id, kind, status, correlation_id)` in 002 covers three
-- NULLABLE columns, and SQLite treats NULLs as distinct in a UNIQUE. Most
-- projected outcomes carry a NULL correlation_id, so the constraint never
-- matched and the `INSERT OR IGNORE` in `_record_outcome_projection` inserted
-- a fresh duplicate every time. `_backfill_projections` runs on EVERY store
-- open and `recorrelate_outcomes` on every `--recorrelate`, so the duplicates
-- accumulate without bound. Measured on the live store before this migration:
-- 1383 rows for 859 distinct source events (1.6x), with single identity groups
-- repeated up to 10 times. Those counts feed /v1/summary and the Prometheus
-- exporter, and the append-only triggers mean nothing could remove them.
--
-- correlation_basis is part of the identity here even though 002 predates it
-- (006 added the column): two projections of one event that disagree about the
-- basis of their correlation are different claims and must not collide.

DROP TRIGGER IF EXISTS prevent_outcome_events_update;
DROP TRIGGER IF EXISTS prevent_outcome_events_delete;

-- Keep the earliest row of each identity group; it is the one every existing
-- attribution edge and analytics query already resolved against.
DELETE FROM outcome_events
WHERE outcome_id NOT IN (
    SELECT MIN(outcome_id) FROM outcome_events
    GROUP BY
        event_id,
        COALESCE(kind, ''),
        COALESCE(status, ''),
        COALESCE(correlation_id, ''),
        COALESCE(correlation_basis, '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_outcome_events_identity
    ON outcome_events (
        event_id,
        COALESCE(kind, ''),
        COALESCE(status, ''),
        COALESCE(correlation_id, ''),
        COALESCE(correlation_basis, '')
    );

CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_update
BEFORE UPDATE ON outcome_events
BEGIN
    SELECT RAISE(ABORT, 'outcome_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_delete
BEFORE DELETE ON outcome_events
BEGIN
    SELECT RAISE(ABORT, 'outcome_events is append-only');
END;
