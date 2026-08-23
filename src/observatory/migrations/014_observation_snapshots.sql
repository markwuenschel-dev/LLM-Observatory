-- Longitudinal record of the observation verdict.
--
-- A point-in-time verdict can say the plane is dark right now, but it keeps no
-- trace of a gap that opens and closes. The thirteen-day blackout on this host
-- was invisible for exactly that reason: nothing recorded that telemetry had
-- stopped, so nothing could show how long it had been stopped for.
--
-- Snapshots are append-only observations of the deployment's own health, taken
-- whenever `observe --record` runs. Gaps between snapshots are themselves
-- evidence: a missing interval means the recorder was not running, which is a
-- different failure from a recorded DEGRADED interval.
CREATE TABLE IF NOT EXISTS observation_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    recorded_at TEXT NOT NULL,
    verdict TEXT NOT NULL,
    observation_capable INTEGER NOT NULL,
    clients_observing INTEGER NOT NULL,
    clients_degraded INTEGER NOT NULL,
    events_in_window INTEGER NOT NULL,
    newest_event_age_seconds REAL,
    store_capacity_ratio REAL,
    blockers_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observation_snapshots_recorded
    ON observation_snapshots(recorded_at);

CREATE TABLE IF NOT EXISTS observation_client_snapshots (
    snapshot_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    client TEXT NOT NULL,
    state TEXT NOT NULL,
    events_observed INTEGER NOT NULL,
    last_event_at TEXT,
    last_event_age_seconds REAL,
    attribution_ratio REAL,
    blockers_json TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, client)
);

CREATE INDEX IF NOT EXISTS idx_observation_client_snapshots_client
    ON observation_client_snapshots(client, recorded_at);

CREATE TRIGGER IF NOT EXISTS observation_snapshots_no_update
BEFORE UPDATE ON observation_snapshots
BEGIN
    SELECT RAISE(ABORT, 'observation_snapshots is append-only');
END;

CREATE TRIGGER IF NOT EXISTS observation_client_snapshots_no_update
BEFORE UPDATE ON observation_client_snapshots
BEGIN
    SELECT RAISE(ABORT, 'observation_client_snapshots is append-only');
END;
