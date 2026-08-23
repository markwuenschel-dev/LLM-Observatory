-- Index the receipt time used by the observation samples.
--
-- `observation_samples` groups by client over a `received_at` window, and
-- `received_at` was indexed only on `ingest_ledger`. The verdict route
-- therefore scanned the whole events table three times per request, ran past
-- the two-second dashboard read budget, and returned 503 -- reported to the
-- operator as "the store is not reachable" while intake was healthy.
CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_events_client_received ON events(client, received_at);
