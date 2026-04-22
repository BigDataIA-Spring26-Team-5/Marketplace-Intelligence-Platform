-- UC2 Observability — Postgres schema init
-- Run once after `docker-compose -p mip up -d`:
--   docker exec -i mip_postgres psql -U mip -d uc2 < src/uc2_observability/init_pg.sql

CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    status      TEXT,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload     JSONB,
    UNIQUE (run_id, event_type)
);

CREATE TABLE IF NOT EXISTS block_trace (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    block_name  TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    rows_in     BIGINT,
    rows_out    BIGINT,
    null_rates  JSONB,
    duration_ms FLOAT,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, block_name, event_type)
);

CREATE TABLE IF NOT EXISTS quarantine_rows (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    row_hash    TEXT        NOT NULL,
    reason      TEXT,
    row_data    JSONB,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, row_hash)
);

CREATE TABLE IF NOT EXISTS dedup_clusters (
    id               BIGSERIAL PRIMARY KEY,
    run_id           TEXT        NOT NULL,
    source           TEXT        NOT NULL,
    cluster_id       TEXT        NOT NULL,
    canonical        TEXT,
    members          JSONB,
    merge_decisions  JSONB,
    ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, cluster_id)
);

CREATE TABLE IF NOT EXISTS anomaly_reports (
    id      BIGSERIAL PRIMARY KEY,
    run_id  TEXT        NOT NULL,
    source  TEXT        NOT NULL,
    signal  TEXT,
    score   FLOAT,
    details JSONB,
    ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, signal)
);

CREATE INDEX IF NOT EXISTS idx_audit_events_run_id  ON audit_events  (run_id);
CREATE INDEX IF NOT EXISTS idx_block_trace_run_id   ON block_trace   (run_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_source       ON anomaly_reports (source);
