-- Preserve explicit parent-agent identity for swarm and subagent drill-down.
ALTER TABLE events ADD COLUMN parent_agent_id TEXT;

CREATE INDEX IF NOT EXISTS idx_events_parent_agent ON events (parent_agent_id);
