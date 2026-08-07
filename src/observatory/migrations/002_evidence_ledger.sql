-- Append-only evidence projections.  The normalized event remains the
-- compatibility envelope; these tables make bitemporal and field-level
-- analysis queryable without parsing every payload.

CREATE TABLE IF NOT EXISTS ingest_ledger (
    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    observed_at TEXT,
    received_at TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_name TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_ledger_event ON ingest_ledger (event_id, ledger_id);
CREATE INDEX IF NOT EXISTS idx_ingest_ledger_received ON ingest_ledger (received_at);

CREATE TABLE IF NOT EXISTS measurement_facts (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    field_path TEXT NOT NULL,
    value_json TEXT NOT NULL,
    unit TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    evidence_quality TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    UNIQUE (event_id, field_path)
);

CREATE INDEX IF NOT EXISTS idx_measurement_facts_event ON measurement_facts (event_id);
CREATE INDEX IF NOT EXISTS idx_measurement_facts_field ON measurement_facts (field_path, evidence_source);
CREATE INDEX IF NOT EXISTS idx_measurement_facts_observed ON measurement_facts (observed_at);

CREATE TABLE IF NOT EXISTS outcome_events (
    outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    kind TEXT,
    status TEXT,
    correlation_id TEXT,
    evidence_source TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    UNIQUE (event_id, kind, status, correlation_id)
);

CREATE INDEX IF NOT EXISTS idx_outcome_events_correlation ON outcome_events (correlation_id);
CREATE INDEX IF NOT EXISTS idx_outcome_events_kind_status ON outcome_events (kind, status);

CREATE TABLE IF NOT EXISTS attribution_edges (
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    child_event_id TEXT NOT NULL,
    parent_event_id TEXT,
    relation TEXT NOT NULL,
    target_id TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    FOREIGN KEY (child_event_id) REFERENCES events(event_id),
    UNIQUE (child_event_id, relation, target_id)
);

CREATE INDEX IF NOT EXISTS idx_attribution_child ON attribution_edges (child_event_id);
CREATE INDEX IF NOT EXISTS idx_attribution_parent ON attribution_edges (parent_event_id);
CREATE INDEX IF NOT EXISTS idx_attribution_target ON attribution_edges (relation, target_id);

-- These projections are evidence ledgers.  Retention/deletion is an explicit
-- operator operation, not an accidental UPDATE/DELETE through the app path.
CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_update
BEFORE UPDATE ON ingest_ledger
BEGIN
    SELECT RAISE(ABORT, 'ingest_ledger is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_delete
BEFORE DELETE ON ingest_ledger
BEGIN
    SELECT RAISE(ABORT, 'ingest_ledger is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_update
BEFORE UPDATE ON measurement_facts
BEGIN
    SELECT RAISE(ABORT, 'measurement_facts is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_delete
BEFORE DELETE ON measurement_facts
BEGIN
    SELECT RAISE(ABORT, 'measurement_facts is append-only');
END;

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

CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_update
BEFORE UPDATE ON attribution_edges
BEGIN
    SELECT RAISE(ABORT, 'attribution_edges is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_delete
BEFORE DELETE ON attribution_edges
BEGIN
    SELECT RAISE(ABORT, 'attribution_edges is append-only');
END;
