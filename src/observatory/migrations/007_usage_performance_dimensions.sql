-- Preserve optional usage and performance fields in indexed aggregate columns.
ALTER TABLE events ADD COLUMN cache_creation_tokens REAL;
ALTER TABLE events ADD COLUMN cache_read_tokens REAL;
ALTER TABLE events ADD COLUMN context_size REAL;
ALTER TABLE events ADD COLUMN context_utilization REAL;
ALTER TABLE events ADD COLUMN compaction_count REAL;
ALTER TABLE events ADD COLUMN tool_duration_ms REAL;
ALTER TABLE events ADD COLUMN session_duration_ms REAL;
ALTER TABLE events ADD COLUMN agent_duration_ms REAL;
ALTER TABLE events ADD COLUMN workflow_duration_ms REAL;
ALTER TABLE events ADD COLUMN wall_clock_ms REAL;
ALTER TABLE events ADD COLUMN concurrency REAL;
ALTER TABLE events ADD COLUMN parallel_utilization REAL;
