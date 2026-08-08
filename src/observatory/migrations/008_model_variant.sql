-- Preserve provider/client model variant identity as a separate comparison dimension.
ALTER TABLE events ADD COLUMN model_variant TEXT;

CREATE INDEX IF NOT EXISTS idx_events_model_variant ON events (model_variant);
