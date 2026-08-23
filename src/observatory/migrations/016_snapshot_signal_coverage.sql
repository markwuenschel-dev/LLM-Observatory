-- Persist per-signal-family coverage with each observation snapshot.
--
-- Coverage was computed live but never recorded, so provider schema drift --
-- a client quietly ceasing to report cost, tokens, or session identity after
-- an upgrade -- was invisible. Nothing degraded, nothing alerted; the numbers
-- simply got quieter. Recording the ratios makes the change comparable against
-- the client's own history rather than against an assumption.
ALTER TABLE observation_client_snapshots ADD COLUMN signal_families_json TEXT;
