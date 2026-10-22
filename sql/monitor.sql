-- Independent projection database. No business rows or replication checkpoints.
CREATE ROLE monitor_writer LOGIN PASSWORD 'monitor-writer';
CREATE ROLE monitor_reader LOGIN PASSWORD 'monitor-reader';
REVOKE ALL ON DATABASE lab_monitor FROM PUBLIC;
GRANT CONNECT ON DATABASE lab_monitor TO monitor_writer, monitor_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA monitoring AUTHORIZATION monitor_writer;
SET ROLE monitor_writer;
CREATE TABLE monitoring.collection_state (
    mode TEXT PRIMARY KEY CHECK(mode IN ('catalog','legacy')),
    snapshot JSONB,
    attempted_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ,
    source_status JSONB NOT NULL
);
CREATE TABLE monitoring.details (
    mode TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('run','table_key','source_key')),
    detail_key TEXT NOT NULL,
    document JSONB NOT NULL,
    PRIMARY KEY(mode,kind,detail_key)
);
RESET ROLE;
GRANT USAGE ON SCHEMA monitoring TO monitor_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO monitor_reader;
ALTER ROLE monitor_reader SET default_transaction_read_only = on;
