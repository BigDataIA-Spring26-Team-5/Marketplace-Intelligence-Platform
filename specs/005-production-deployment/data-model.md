# Data Model: Production Deployment

**Feature**: 005-production-deployment  
**Date**: 2026-04-18  
**Storage**: PostgreSQL (primary), S3/MinIO (artifacts)

---

## PostgreSQL Tables

### `run_state`

Tracks lifecycle of each pipeline execution. Replaces the in-memory dict used in Phase 1 and the SQLite LangGraph checkpoint for run-level status.

```sql
CREATE TABLE run_state (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status          VARCHAR(32) NOT NULL DEFAULT 'queued',
                    -- queued | running | awaiting_approval | approved | failed | completed
    source_path     TEXT NOT NULL,
    domain          VARCHAR(64) NOT NULL,
    chunk_size      INTEGER NOT NULL DEFAULT 10000,
    hitl_mode       BOOLEAN NOT NULL DEFAULT TRUE,
    celery_task_id  TEXT,
    dag_run_id      TEXT,                            -- Airflow DAG run ID (Phase 3+)
    langgraph_thread_id TEXT,                        -- LangGraph checkpoint thread
    error_message   TEXT,
    dq_score_pre    FLOAT,
    dq_score_post   FLOAT,
    enrichment_stats JSONB,                          -- {"deterministic":N,"embedding":N,"llm":N,"unresolved":N}
    output_s3_key   TEXT,                            -- s3://bucket/runs/{run_id}/output.csv
    row_count_in    INTEGER,
    row_count_out   INTEGER,
    row_count_quarantined INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX run_state_status_idx ON run_state (status);
CREATE INDEX run_state_created_at_idx ON run_state (created_at DESC);
```

**State transitions**:
```
queued → running → awaiting_approval → approved → running → completed
                                                           → failed
       → running → failed
```

**Notes**:
- `hitl_mode=false` skips HITL gates; status goes `queued → running → completed` without `awaiting_approval`
- `langgraph_thread_id` maps to the LangGraph `PostgresSaver` thread used for checkpoint/resume
- `enrichment_stats` stores the same dict emitted by `LLMEnrichBlock`

---

### `audit_log`

Stores per-block execution records for each run. Maps directly to the `audit_log` list returned by `PipelineRunner.run()`.

```sql
CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES run_state(run_id) ON DELETE CASCADE,
    chunk_index     INTEGER NOT NULL DEFAULT 0,
    block_name      TEXT NOT NULL,
    rows_in         INTEGER,
    rows_out        INTEGER,
    rows_delta      INTEGER GENERATED ALWAYS AS (rows_out - rows_in) STORED,
    columns_renamed JSONB,                           -- {old_name: new_name, ...}
    extra_meta      JSONB,                           -- block-specific metadata
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms     INTEGER
);

CREATE INDEX audit_log_run_id_idx ON audit_log (run_id, chunk_index);
```

**Notes**:
- `chunk_index` is 0 for single-chunk runs; increments per Celery task for large datasets
- `columns_renamed` populated only for the `column_mapping` block entry
- `extra_meta` captures block-specific fields (e.g. `{"duplicate_clusters": 12}` for `fuzzy_deduplicate`)

---

### `block_registry`

Production-durable version of the in-memory `BlockRegistry` singleton. Stores YAML mapping hits and block metadata across restarts.

```sql
CREATE TABLE block_registry (
    id              BIGSERIAL PRIMARY KEY,
    domain          VARCHAR(64) NOT NULL,
    dataset_name    TEXT NOT NULL,
    block_key       TEXT NOT NULL,                   -- e.g. "DYNAMIC_MAPPING_usda_fooddata_sample"
    yaml_s3_key     TEXT,                            -- s3://bucket/generated/{domain}/DYNAMIC_MAPPING_{dataset}.yaml
    yaml_content    TEXT,                            -- full YAML text cached locally
    hit_count       INTEGER NOT NULL DEFAULT 0,
    last_hit_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (domain, dataset_name, block_key)
);

CREATE INDEX block_registry_domain_idx ON block_registry (domain, dataset_name);
```

**Notes**:
- `yaml_s3_key` is the authoritative source; `yaml_content` is a cache for offline/fast access
- `hit_count` increments each time `check_registry_node` finds an existing mapping — tracks "pipeline remembers" behavior
- Local `src/blocks/generated/<domain>/` files still written for backward compat with non-deployed pipeline runs

---

### `hitl_decision`

Records human approval/rejection decisions at HITL gates. Provides audit trail for schema mapping reviews and quarantine overrides.

```sql
CREATE TABLE hitl_decision (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES run_state(run_id) ON DELETE CASCADE,
    gate            VARCHAR(32) NOT NULL,            -- 'schema_mapping' | 'quarantine'
    decision        VARCHAR(16) NOT NULL,            -- 'approved' | 'rejected' | 'modified'
    payload         JSONB NOT NULL,                  -- the operations list or quarantine rows reviewed
    operator        TEXT,                            -- user identity (future: auth)
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX hitl_decision_run_id_idx ON hitl_decision (run_id);
```

---

## S3/MinIO Object Layout

```text
s3://{BUCKET}/
├── runs/
│   └── {run_id}/
│       ├── source.csv                      # uploaded input file (if user uploads)
│       ├── chunks/
│       │   ├── chunk_0000.parquet          # chunk 0 after DynamicMappingBlock
│       │   ├── chunk_0001.parquet
│       │   └── ...
│       └── output.csv                      # final merged output (run_state.output_s3_key)
├── generated/
│   └── {domain}/
│       └── DYNAMIC_MAPPING_{dataset}.yaml  # mirrors src/blocks/generated/{domain}/
└── corpus/
    ├── faiss_index.bin                     # KNN embedding index
    └── corpus_metadata.json                # corpus metadata
```

**Notes**:
- `runs/{run_id}/chunks/` populated by modified `runner.py` (Phase 2)
- `generated/{domain}/` populated by `check_registry_node` via `S3Storage.upload_yaml()`
- `corpus/` synced from local `corpus/` on startup; updated after each enrichment run

---

## Entity Relationships

```
run_state ──< audit_log         (1 run → many block execution records)
run_state ──< hitl_decision     (1 run → 0–2 HITL decisions)
block_registry                  (independent; keyed by domain+dataset+block_key)
```

---

## Validation Rules

| Entity | Rule |
|--------|------|
| `run_state.status` | Must be one of: `queued`, `running`, `awaiting_approval`, `approved`, `failed`, `completed` |
| `run_state.domain` | Must match a key in `config/unified_schema.json` domain list |
| `run_state.chunk_size` | 1000–100000 (enforced at API layer via `PipelineRunRequest`) |
| `audit_log.block_name` | Must match a registered block key from `BlockRegistry` |
| `block_registry` (domain, dataset_name, block_key) | UNIQUE constraint enforced at DB |

---

## Alembic Migration Order

1. `001_create_run_state` — `run_state` table
2. `002_create_audit_log` — `audit_log` table + FK
3. `003_create_block_registry` — `block_registry` table
4. `004_create_hitl_decision` — `hitl_decision` table + FK
