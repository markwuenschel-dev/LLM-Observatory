CREATE TABLE IF NOT EXISTS maintenance_actions (
    action_id TEXT PRIMARY KEY,
    action_type TEXT NOT NULL,
    selector_json TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT NOT NULL,
    affected_events INTEGER NOT NULL DEFAULT 0,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_maintenance_actions_requested ON maintenance_actions (requested_at);

CREATE TRIGGER IF NOT EXISTS prevent_maintenance_actions_update
BEFORE UPDATE ON maintenance_actions
BEGIN
    SELECT RAISE(ABORT, 'maintenance_actions is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_maintenance_actions_delete
BEFORE DELETE ON maintenance_actions
BEGIN
    SELECT RAISE(ABORT, 'maintenance_actions is append-only');
END;
