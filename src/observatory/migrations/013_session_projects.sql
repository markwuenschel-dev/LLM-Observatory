-- Session-to-project bindings.
--
-- Native OTLP clients (Claude Code, codex) emit a session identifier but no
-- working-directory or repository attribute, so every native event normalizes
-- to project:unknown. A globally-configured client hook does know the working
-- directory, and reports the SAME session identifier. Binding the two lets a
-- later native event inherit the project identity that the hook established,
-- without requiring any file inside the observed repository.
--
-- The binding is evidence, not a mutation: stored events remain immutable, and
-- enrichment happens before an event is persisted.
CREATE TABLE IF NOT EXISTS session_projects (
    session_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    repository TEXT,
    branch TEXT,
    worktree TEXT,
    commit_sha TEXT,
    evidence_source TEXT,
    observed_at TEXT NOT NULL,
    received_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_projects_project ON session_projects(project_id);

-- First known binding wins, so a session cannot be silently re-attributed.
CREATE TRIGGER IF NOT EXISTS session_projects_no_update
BEFORE UPDATE ON session_projects
BEGIN
    SELECT RAISE(ABORT, 'session_projects is append-only');
END;
