-- Idempotent upgrade for existing V6 local PostgreSQL volumes.
-- Run ONLY on the lab_local database. The role never gains write access.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='monitor_reader') THEN
    CREATE ROLE monitor_reader LOGIN PASSWORD 'monitor-reader';
  END IF;
END $$;
GRANT CONNECT ON DATABASE lab_local TO monitor_reader;
GRANT USAGE ON SCHEMA replica TO monitor_reader;
GRANT SELECT ON replica.active_state, replica.transitions, replica.sync_runs,
  replica.schema_state TO monitor_reader;

-- Upgrade initialized V7 database; repeatable and non-destructive.
ALTER TABLE replica.sync_runs ADD COLUMN IF NOT EXISTS source_engine TEXT;
ALTER TABLE replica.sync_runs ADD COLUMN IF NOT EXISTS sweep_target_max_id BIGINT;
CREATE TABLE IF NOT EXISTS replica.range_checkpoints (
    run_id UUID PRIMARY KEY REFERENCES replica.sync_runs(run_id),
    sweep_low BIGINT NOT NULL,
    sweep_high BIGINT NOT NULL CHECK(sweep_high >= sweep_low),
    target_max_id BIGINT,
    source_engine TEXT NOT NULL,
    version BIGINT NOT NULL REFERENCES replica.generations(version),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS range_checkpoints_recent ON replica.range_checkpoints(checked_at DESC);
GRANT SELECT ON replica.active_state, replica.transitions, replica.sync_runs,
    replica.schema_state, replica.slot_state, replica.range_checkpoints,
    replica.schema_versions, replica.schema_changes, replica.schema_backfill_runs,
    replica.schema_backfill_intervals, replica.source_catalog_observations
    TO monitor_reader;

-- Keeps the V8 20s polling inexpensive even after many runs.
CREATE INDEX IF NOT EXISTS sync_runs_started_idx ON replica.sync_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_status_finished_idx ON replica.sync_runs(status,finished_at DESC);
