-- V4: blue/green com auditoria incremental de faixas e hashes.
-- Destino de leitura: public.contracts (view transacional).
CREATE SCHEMA replica;
CREATE TABLE replica.blue_contracts (
    id BIGINT PRIMARY KEY,
    contract_code TEXT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    status TEXT NOT NULL,
    description TEXT
);
CREATE TABLE replica.green_contracts (LIKE replica.blue_contracts INCLUDING ALL);
CREATE TABLE replica.generations (
    version BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_version BIGINT REFERENCES replica.generations(version),
    run_id UUID UNIQUE NOT NULL,
    cutoff TIMESTAMPTZ NOT NULL,
    row_count BIGINT NOT NULL,
    next_sweep_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE replica.slot_state (
    slot TEXT PRIMARY KEY CHECK (slot IN ('blue','green')),
    version BIGINT REFERENCES replica.generations(version)
);
INSERT INTO replica.slot_state(slot) VALUES ('blue'), ('green');
CREATE TABLE replica.active_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    slot TEXT CHECK (slot IN ('blue','green')),
    version BIGINT REFERENCES replica.generations(version),
    cutoff TIMESTAMPTZ,
    next_sweep_id BIGINT NOT NULL DEFAULT 1,
    row_count BIGINT NOT NULL DEFAULT 0
);
INSERT INTO replica.active_state(singleton) VALUES (TRUE);
CREATE TABLE replica.sync_runs (
    run_id UUID PRIMARY KEY,
    mode TEXT NOT NULL CHECK(mode IN ('bootstrap','incremental')),
    status TEXT NOT NULL CHECK(status IN ('extracting','loading','failed','aborted','promoted','rejected')),
    base_version BIGINT,
    window_start TIMESTAMPTZ,
    cutoff TIMESTAMPTZ,
    sweep_low BIGINT,
    sweep_high BIGINT,
    delta_count BIGINT,
    actual_count BIGINT,
    sweep_count BIGINT,
    inserted_count BIGINT,
    updated_count BIGINT,
    deleted_count BIGINT,
    result_version BIGINT,
    logical_kafka_bytes BIGINT,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ
);
CREATE TABLE replica.ingest_delta (
    run_id UUID NOT NULL REFERENCES replica.sync_runs(run_id),
    id BIGINT NOT NULL,
    op TEXT NOT NULL CHECK(op IN ('upsert','delete')),
    payload JSONB,
    PRIMARY KEY(run_id,id)
);
CREATE TABLE replica.ingest_deep (
    run_id UUID NOT NULL REFERENCES replica.sync_runs(run_id),
    id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY(run_id,id)
);
CREATE TABLE replica.ingest_keys (
    run_id UUID NOT NULL REFERENCES replica.sync_runs(run_id),
    id BIGINT NOT NULL,
    PRIMARY KEY(run_id,id)
);
-- Journal para avançar/reverter APENAS os registros modificados entre versões.
-- A relação parent_version representa também ramificações após rollback.
CREATE TABLE replica.change_journal (
    version BIGINT NOT NULL REFERENCES replica.generations(version),
    id BIGINT NOT NULL,
    before_row JSONB,
    after_row JSONB,
    PRIMARY KEY(version,id)
);
CREATE TABLE replica.transitions (
    id BIGSERIAL PRIMARY KEY,
    action TEXT NOT NULL CHECK(action IN ('promote','rollback')),
    from_slot TEXT,
    to_slot TEXT,
    from_version BIGINT,
    to_version BIGINT,
    transitioned_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- A VIEW permanente não sofre DDL por ciclo. Cada SELECT vê um único slot
-- coerente pelo MVCC. Verificar no EXPLAIN: filtros de execução por ramificação.
CREATE VIEW public.contracts AS
  SELECT b.id,b.contract_code,b.amount,b.status,b.description
  FROM replica.blue_contracts b
  WHERE EXISTS (SELECT 1 FROM replica.active_state s WHERE s.singleton AND s.slot='blue')
  UNION ALL
  SELECT g.id,g.contract_code,g.amount,g.status,g.description
  FROM replica.green_contracts g
  WHERE EXISTS (SELECT 1 FROM replica.active_state s WHERE s.singleton AND s.slot='green');

-- V5: contracts of record and operator-approved schema evolution.
-- Only an explicitly approved additive nullable TEXT column can be migrated.
CREATE TABLE replica.schema_versions (
    schema_hash TEXT PRIMARY KEY,
    manifest JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','observed','prepared','retired','rejected')),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    activated_at TIMESTAMPTZ
);
CREATE TABLE replica.schema_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    active_hash TEXT REFERENCES replica.schema_versions(schema_hash),
    pending_hash TEXT REFERENCES replica.schema_versions(schema_hash),
    backfill_next BIGINT,
    backfill_max BIGINT,
    CONSTRAINT schema_state_hash_check CHECK (
        pending_hash IS NULL
        OR (
            active_hash IS NOT NULL
            AND active_hash <> pending_hash
        )
    )
);
INSERT INTO replica.schema_state(singleton) VALUES (TRUE);
CREATE TABLE replica.schema_changes (
    id BIGSERIAL PRIMARY KEY,
    from_hash TEXT,
    to_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    diff JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    acted_at TIMESTAMPTZ
);
CREATE TABLE replica.schema_backfill_runs (
    run_id UUID PRIMARY KEY,
    schema_hash TEXT NOT NULL REFERENCES replica.schema_versions(schema_hash),
    first_id BIGINT NOT NULL,
    last_id BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('loading','applied','rejected','aborted')),
    n_rows BIGINT,
    digest TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE replica.schema_backfill_values (
    run_id UUID NOT NULL REFERENCES replica.schema_backfill_runs(run_id),
    id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id,id)
);
CREATE TABLE replica.schema_backfill_intervals (
    schema_hash TEXT NOT NULL REFERENCES replica.schema_versions(schema_hash),
    first_id BIGINT NOT NULL,
    last_id BIGINT NOT NULL,
    run_id UUID UNIQUE NOT NULL REFERENCES replica.schema_backfill_runs(run_id),
    rows_checked BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(schema_hash,first_id)
);
ALTER TABLE replica.sync_runs ADD COLUMN data_schema_hash TEXT;
ALTER TABLE replica.generations ADD COLUMN data_schema_hash TEXT;

-- Engine-neutral source dictionary history. No remote DDL is ever executed.
-- Native and logical manifests are immutable evidence of observed states.
CREATE TABLE replica.source_catalog_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    source_engine TEXT NOT NULL CHECK (source_engine IN ('postgresql','sqlserver')),
    object_name TEXT NOT NULL,
    native_manifest JSONB NOT NULL,
    logical_manifest JSONB NOT NULL,
    native_hash CHAR(64) NOT NULL,
    logical_hash CHAR(64) NOT NULL,
    changes JSONB NOT NULL,
    native_changes JSONB NOT NULL,
    classification TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX source_catalog_latest ON replica.source_catalog_observations
  (source_key, observation_id DESC);
CREATE INDEX source_catalog_columns ON replica.source_catalog_observations
  USING GIN (logical_manifest jsonb_path_ops);


-- Monitor has read-only access to local audit tables only. No remote credentials.
CREATE ROLE monitor_reader LOGIN PASSWORD 'monitor-reader';
GRANT CONNECT ON DATABASE lab_local TO monitor_reader;
GRANT USAGE ON SCHEMA replica TO monitor_reader;
GRANT SELECT ON replica.active_state, replica.transitions, replica.sync_runs,
  replica.schema_state TO monitor_reader;

-- Audit of completed PK intervals, stored transactionally with promotion.
ALTER TABLE replica.sync_runs ADD COLUMN source_engine TEXT;
ALTER TABLE replica.sync_runs ADD COLUMN sweep_target_max_id BIGINT;
CREATE TABLE replica.range_checkpoints (
    run_id UUID PRIMARY KEY REFERENCES replica.sync_runs(run_id),
    sweep_low BIGINT NOT NULL,
    sweep_high BIGINT NOT NULL CHECK(sweep_high >= sweep_low),
    target_max_id BIGINT,
    source_engine TEXT NOT NULL,
    version BIGINT NOT NULL REFERENCES replica.generations(version),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX range_checkpoints_recent ON replica.range_checkpoints(checked_at DESC);
GRANT SELECT ON replica.active_state, replica.transitions, replica.sync_runs,
    replica.schema_state, replica.slot_state, replica.generations, replica.range_checkpoints,
    replica.schema_versions, replica.schema_changes, replica.schema_backfill_runs,
    replica.schema_backfill_intervals, replica.source_catalog_observations
    TO monitor_reader;

-- Keeps the monitor 20s polling inexpensive even after many runs.
CREATE INDEX IF NOT EXISTS sync_runs_started_idx ON replica.sync_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_status_finished_idx ON replica.sync_runs(status,finished_at DESC);
