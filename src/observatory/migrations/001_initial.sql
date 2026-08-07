CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    project_id TEXT NOT NULL,
    repository TEXT,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    client TEXT NOT NULL,
    auth_mode TEXT NOT NULL,
    route TEXT NOT NULL,
    status TEXT NOT NULL,
    usage_source TEXT NOT NULL,
    input_tokens REAL,
    output_tokens REAL,
    total_tokens REAL,
    trace_id TEXT,
    span_id TEXT,
    parent_event_id TEXT,
    session_id TEXT,
    workflow_id TEXT,
    agent_id TEXT,
    subagent_id TEXT,
    evidence_source TEXT,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    inserted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_observed_at ON events (observed_at);
CREATE INDEX IF NOT EXISTS idx_events_project ON events (project_id);
CREATE INDEX IF NOT EXISTS idx_events_provider_model ON events (provider, model);
CREATE INDEX IF NOT EXISTS idx_events_client ON events (client);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);

CREATE TABLE IF NOT EXISTS event_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    conflict_digest TEXT NOT NULL,
    conflict_payload_json TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    FOREIGN KEY (event_id) REFERENCES events(event_id)
);

CREATE INDEX IF NOT EXISTS idx_event_conflicts_event_id ON event_conflicts (event_id);

