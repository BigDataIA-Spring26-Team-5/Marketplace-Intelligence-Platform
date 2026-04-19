# Implementation Plan: Production Deployment

**Branch**: `005-production-deployment` | **Date**: 2026-04-18 | **Spec**: `specs/005-production-deployment/plan.md`
**Input**: Architecture design from session on 2026-04-18 — converting local ETL pipeline to deployed, API-accessible production system.

## Summary

Convert the schema-driven ETL pipeline from a local Streamlit + Poetry environment to a containerized, API-accessible production system. Replace SQLite checkpoints with PostgreSQL, expose pipeline execution via FastAPI endpoints, orchestrate multi-step runs via Airflow DAGs, add Celery workers for parallel chunk processing, replace local FAISS with Qdrant for scalable vector search, and store all data artifacts in S3/MinIO. Deploy via Docker Compose to Railway or Fly.io for a public URL.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: FastAPI, Celery, Redis, Airflow 2.x, Qdrant-client, boto3, tenacity, pydantic v2, uvicorn, litellm, langgraph  
**Storage**: PostgreSQL (state/audit/registry), S3/MinIO (CSVs, parquet chunks, FAISS/Qdrant index), Redis (Celery broker + result backend)  
**Testing**: pytest (existing), add pytest-asyncio for async LLM tests, testcontainers for PostgreSQL integration tests  
**Target Platform**: Linux server (Docker), deployed to Railway or Fly.io  
**Project Type**: Web service + background worker + orchestration DAGs  
**Performance Goals**: Single OFf run (3.4M rows) completes in <2 hrs (vs 90 min killed locally); S3 LLM enrichment parallel batches reduce per-chunk S3 time from ~40 min to <10 min  
**Constraints**: DeepSeek API rate limits (~60 RPM); SentenceTransformer CPU latency ~200ms/batch; FAISS replace only if corpus >1M vectors  
**Scale/Scope**: Single-tenant school project; 1–5 concurrent pipeline runs; datasets up to 10M rows

## Constitution Check

- [x] Unified-schema impact: `config/unified_schema.json` becomes `api/models/schema.py` (Pydantic `UnifiedSchema`); JSON file retained as source-of-truth serialization format for backward compat
- [x] Agent responsibilities unchanged: `load_source_node`, `analyze_schema_node`, `critique_schema_node`, `check_registry_node`, `plan_sequence_node`, `run_pipeline_node`, `save_output_node` become Airflow tasks — same logic, new execution context
- [x] Transformations remain declarative YAML: `DynamicMappingBlock` and generated YAML files unchanged; no runtime Python generation added
- [x] HITL approval points: current Streamlit HITL gates (YAML review, sequence approval) move to FastAPI endpoints returning `status: awaiting_approval`; caller polls and PATCH-approves
- [x] Enrichment safety fields preserved: `llm_enrich.py` safety snapshot logic unchanged; `allergens/dietary_tags/is_organic` remain deterministic-only
- [x] DQ scoring, generated mapping persistence: `DQScorePreBlock`/`DQScorePostBlock` unchanged; YAML mappings written to `s3://{bucket}/generated/{domain}/DYNAMIC_MAPPING_{dataset}.yaml` mirroring current local path

## Project Structure

### Documentation (this feature)

```text
specs/005-production-deployment/
├── plan.md              # This file
├── research.md          # Architecture options evaluated
├── data-model.md        # PostgreSQL schema for run_state, audit_log, block_registry
├── quickstart.md        # Docker Compose up + first API call walkthrough
└── tasks.md             # Phase-by-phase task breakdown (created separately)
```

### Source Code (repository root)

```text
# Web service + background worker structure

api/
├── main.py                  # FastAPI app factory, mounts routers
├── routes/
│   ├── pipeline.py          # POST /pipeline/run, GET /pipeline/{id}/status, PATCH /pipeline/{id}/approve
│   └── output.py            # GET /pipeline/{id}/output, GET /pipeline/{id}/audit
├── models/
│   ├── request.py           # PipelineRunRequest (Pydantic): source_path, domain, chunk_size, hitl_mode
│   ├── response.py          # RunStatus, AuditEntry, EnrichmentStats (Pydantic)
│   └── schema.py            # UnifiedSchema, ColumnSpec (Pydantic v2) — replaces unified_schema.json parsing
├── tasks/
│   └── pipeline_task.py     # Celery task: run_pipeline_task(run_id, req) → calls PipelineRunner
└── db/
    ├── models.py             # SQLAlchemy ORM: RunState, AuditLog, BlockRegistryEntry
    └── migrations/           # Alembic migrations

dags/
└── etl_pipeline.py          # Airflow DAG: load_source >> analyze_schema >> critique >>
                             #   check_registry >> run_pipeline_chunks >> merge_output >> save_output

src/
├── agents/                  # Unchanged (orchestrator.py, critic.py, graph.py, state.py)
├── blocks/                  # Unchanged block implementations
├── enrichment/
│   ├── embedding.py         # MODIFIED: batch model.encode() per chunk, not per row
│   └── corpus.py            # MODIFIED: Qdrant client replaces FAISS when env QDRANT_URL set
├── models/
│   └── llm.py               # MODIFIED: tenacity @retry on call_llm()
├── pipeline/
│   └── runner.py            # MODIFIED: write chunks to S3 instead of local disk
├── storage/
│   └── s3.py                # NEW: S3Storage class (upload_parquet, download_parquet, upload_yaml)
└── utils/
    └── csv_stream.py        # Unchanged (low_memory=False already added)

tests/
├── contract/                # Pydantic model validation tests
├── integration/             # testcontainers: PostgreSQL, MinIO, Redis
└── unit/                    # Existing + new async LLM mock tests

docker-compose.yml           # NEW: api, worker, postgres, redis, minio, airflow-scheduler, airflow-webserver
Dockerfile                   # NEW: python:3.11-slim + poetry install
.env.example                 # NEW: all required env vars documented
```

**Structure Decision**: Web service + worker. The existing `src/` tree stays intact — new `api/`, `dags/`, and `src/storage/` directories wrap it. No rewrites to block logic or agent nodes; only execution context changes (from Streamlit calls to Celery tasks and Airflow DAG tasks).

---

## Implementation Phases

### Phase 1 — Pydantic Models + FastAPI (No infra change)

**Goal**: API layer runs locally, all business logic unchanged.

| Task | File | Notes |
|------|------|-------|
| Define `UnifiedSchema`, `ColumnSpec` Pydantic models | `api/models/schema.py` | Load from `config/unified_schema.json` at startup; validate on import |
| Define `PipelineRunRequest`, `RunStatus`, `AuditEntry` | `api/models/request.py`, `response.py` | |
| Implement `POST /pipeline/run` (background task, no Celery yet) | `api/routes/pipeline.py` | `BackgroundTasks` for now; swap to Celery in Phase 2 |
| Implement `GET /pipeline/{id}/status` and `/output` | `api/routes/output.py` | Read from in-memory dict initially |
| Wire `api/main.py` | `api/main.py` | `uvicorn api.main:app` |

**Deliverable**: `uvicorn api.main:app` works locally; curl triggers a pipeline run.

---

### Phase 2 — Docker Compose + PostgreSQL + Celery

**Goal**: Full stack runs in containers; public deploy ready.

| Task | File | Notes |
|------|------|-------|
| Write `Dockerfile` | `Dockerfile` | `python:3.11-slim`, `poetry install --no-root` |
| Write `docker-compose.yml` | `docker-compose.yml` | api, worker, postgres, redis, minio |
| SQLAlchemy `RunState` + `AuditLog` models | `api/db/models.py` | |
| Alembic migration: create tables | `api/db/migrations/` | |
| Switch LangGraph checkpointer to PostgreSQL | `src/agents/graph.py` | `PostgresSaver.from_conn_string(DATABASE_URL)` |
| Wrap `run_pipeline_node` in Celery task | `api/tasks/pipeline_task.py` | |
| Switch API route from `BackgroundTasks` to `celery.delay()` | `api/routes/pipeline.py` | |
| Write `S3Storage` class | `src/storage/s3.py` | Works with MinIO locally via `S3_ENDPOINT_URL` |
| Modify `runner.py` to write chunks to S3 | `src/pipeline/runner.py` | Path: `runs/{run_id}/chunks/chunk_{i:04d}.parquet` |

**Deliverable**: `docker-compose up` → `curl localhost:8000/pipeline/run` → run completes, parquets in MinIO.

**Deploy**: Push to Railway or Fly.io with `railway up` / `flyctl deploy`.

---

### Phase 3 — Airflow DAGs

**Goal**: Replace manual `run_step()` sequencing with Airflow; gain retry, backfill, monitoring.

| Task | File | Notes |
|------|------|-------|
| Add Airflow to `docker-compose.yml` | `docker-compose.yml` | `apache/airflow:2.9` scheduler + webserver + triggerer |
| Write `etl_pipeline` DAG | `dags/etl_pipeline.py` | One `@task` per LangGraph node; state passed via XCom or PostgreSQL |
| `POST /pipeline/run` triggers DAG via Airflow REST API | `api/routes/pipeline.py` | `PATCH /api/v1/dags/etl_pipeline/dagRuns` |
| Dynamic task mapping for chunks | `dags/etl_pipeline.py` | `run_pipeline_chunks.expand(chunk_index=chunk_indices)` |

**Deliverable**: Airflow UI at `localhost:8080`; each pipeline run is a DAG run with per-task logs and retry.

---

### Phase 4 — Performance: LLM Retry + Async Batching + Qdrant

**Goal**: Eliminate the remaining runtime bottlenecks.

| Task | File | Change |
|------|------|--------|
| Add `tenacity` to `call_llm()` | `src/models/llm.py` | `@retry(wait=wait_exponential(min=2, max=60), stop=stop_after_attempt(3))` |
| Async LLM batch in `llm_tier.py` | `src/enrichment/llm_tier.py` | `asyncio.gather` over `litellm.acompletion` calls; replaces sequential batch loop |
| Batch `model.encode()` per chunk | `src/enrichment/embedding.py` | Collect all query vectors, single `encode()` call, then search |
| Save corpus once per chunk | `src/enrichment/llm_tier.py` | Accumulate new entries; call `save_corpus()` after chunk completes |
| Qdrant client (optional, when corpus >500K) | `src/enrichment/corpus.py` | Feature-flagged: `if os.environ.get("QDRANT_URL"): use Qdrant else FAISS` |

**Deliverable**: OFf chunk 1 enrichment time drops from ~40 min to <10 min.

---

## Complexity Tracking

| Decision | Why Needed | Simpler Alternative Rejected Because |
|----------|------------|-------------------------------------|
| Celery + Redis alongside Airflow | Celery handles sub-chunk parallelism (concurrent LLM batches); Airflow handles inter-node sequencing | Airflow alone can't efficiently parallelize within a running chunk without external worker pool |
| S3Storage abstraction layer | Must work with local MinIO (dev) and real S3 (prod) without code changes | Direct boto3 calls would require env-conditional branching in runner.py |
| PostgreSQL for LangGraph checkpoint | LangGraph `PostgresSaver` is production-supported; SQLite has no concurrent writer support | SQLite blocks concurrent chunk workers |
