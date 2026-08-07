-- Preserve the explicit basis for any outcome correlation without changing
-- the append-only projection contract.
ALTER TABLE outcome_events ADD COLUMN correlation_basis TEXT;
