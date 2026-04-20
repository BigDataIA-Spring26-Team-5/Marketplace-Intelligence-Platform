# Research: Production Deployment Architecture

**Feature**: 006-production-deployment  
**Date**: 2026-04-18

## Summary

All architectural decisions resolved. No open NEEDS CLARIFICATION items. This document records rationale and alternatives evaluated for each major decision in `plan.md`.

---

## Decision 1: PostgreSQL for LangGraph Checkpointing

**Decision**: Replace SQLite checkpoint databases (`checkpoint/checkpoint.db`, `checkpoints.db`) with PostgreSQL via `langgraph-checkpoint-postgres`.

**Rationale**:
- SQLite has no concurrent write support — Celery workers processing parallel chunks would serialize on a single file lock, defeating parallelism
- `PostgresSaver.from_conn_string(DATABASE_URL)` is the LangGraph-recommended production checkpointer
- Same PostgreSQL instance serves checkpoint, audit log, and block registry → one infra component, not three

**Alternatives Considered**:
- Redis as checkpoint store: LangGraph Redis checkpointer exists but adds a second stateful dependency; PostgreSQL already required for audit/registry
- Keep SQLite, use WAL mode: WAL mode helps concurrent reads but concurrent writers still serialize; won't scale past ~2 parallel chunks
- MongoDB: document store is natural for PipelineState dicts but adds operational complexity with no benefit over JSONB columns in PostgreSQL

---

## Decision 2: Celery + Redis for Sub-Chunk Parallelism

**Decision**: Celery with Redis broker + result backend, wrapping `run_pipeline_node` as a Celery task.

**Rationale**:
- DeepSeek rate limit (~60 RPM) means LLM enrichment batches must be parallelized across chunks to stay within the limit without serializing all chunk work
- Celery `asyncio.gather` over `litellm.acompletion` per chunk enables concurrent LLM calls up to the RPM cap
- Redis is already required for Celery — reusing it for pub/sub pipeline status events costs nothing

**Alternatives Considered**:
- FastAPI `BackgroundTasks` only: no retry, no distributed workers, single-process bottleneck — acceptable for Phase 1 only
- Airflow TaskFlow with inline parallelism: Airflow handles inter-node sequencing well but is not designed for sub-task parallelism within a single worker; requires an external pool
- Dask or Ray: heavy-weight distributed compute frameworks; overkill for 1–5 concurrent runs, adds operator burden

---

## Decision 3: Airflow 2.x for Pipeline Orchestration

**Decision**: Apache Airflow 2.9 DAG with one `@task` per LangGraph node; dynamic task mapping for chunk processing.

**Rationale**:
- Per-node retry: each LangGraph node becomes a retryable Airflow task — `analyze_schema_node` LLM timeouts retry without restarting `load_source_node`
- Visibility: Airflow UI surfaces per-task logs, duration, and status without custom instrumentation
- Dynamic task mapping (`expand(chunk_index=chunk_indices)`) enables Airflow to fan out chunk processing without pre-declaring task count

**Alternatives Considered**:
- Prefect: comparable feature set but smaller community; PostgreSQL backend support is newer
- Temporal: excellent retry/durability story but Go-primary SDK; Python SDK is secondary-class
- Keep LangGraph as sole orchestrator: LangGraph does not provide cross-run retry, scheduling, or fan-out visibility at the run level
- Celery chains only: sequential Celery chains with no visibility into partial completion; harder to debug partial failures

---

## Decision 4: S3/MinIO for Artifact Storage

**Decision**: `S3Storage` abstraction class (`src/storage/s3.py`) using boto3; dev uses MinIO at `S3_ENDPOINT_URL`, prod uses real S3.

**Rationale**:
- Chunk parquet files (~50–500 MB each for 3.4M-row dataset) cannot live on API container ephemeral disk
- MinIO is S3-API-compatible → same `S3Storage` code in dev and prod; no conditional branching
- Path pattern `runs/{run_id}/chunks/chunk_{i:04d}.parquet` makes per-run cleanup trivial (`s3.delete_prefix(f"runs/{run_id}/")`)

**Alternatives Considered**:
- PostgreSQL large object (BYTEA/lo): BYTEA columns hit memory pressure at large parquet sizes; not designed for file-scale blobs
- Local volume mount shared between containers: works in Docker Compose but breaks in Fly.io/Railway multi-node deployments
- GCS or Azure Blob: S3-compatible clients exist but boto3 is the defacto standard; switching endpoint URL covers both

---

## Decision 5: Qdrant (Optional) vs FAISS for Vector Search

**Decision**: Feature-flag Qdrant behind `QDRANT_URL` env var; FAISS remains default until corpus exceeds 500K vectors.

**Rationale**:
- Current corpus is small (<<100K vectors); FAISS in-memory is faster and has no network overhead
- Qdrant adds a network hop per batch search — measurable latency cost for small corpus
- When corpus exceeds 500K, FAISS index no longer fits in API container memory; Qdrant's disk-backed store becomes necessary
- Feature flag avoids forced migration; school-project scale unlikely to hit threshold during course

**Alternatives Considered**:
- Weaviate: schema-heavy, heavier container footprint, no clear advantage at this scale
- pgvector extension: natural fit alongside PostgreSQL but requires PostgreSQL 15+ with pgvector installed; `corpus.py` rewrite is larger than Qdrant client swap
- Pinecone: managed SaaS, no self-hosted option for local dev

---

## Decision 6: Railway vs Fly.io for Deployment

**Decision**: Both are viable; Railway preferred for initial deploy due to simpler Docker Compose import.

**Rationale**:
- Railway supports `railway up` from a `docker-compose.yml` with minimal reconfiguration
- Fly.io requires per-service `fly.toml` files; multi-service deploy is more manual
- Both offer free-tier compute sufficient for a school project demo
- Either can be swapped without code changes — the `docker-compose.yml` is the deployment artifact

**Alternatives Considered**:
- Render: Docker Compose support is limited; PostgreSQL and Redis must be provisioned separately
- Heroku: deprecated Docker Compose support; container registry approach is more manual
- AWS ECS/GCP Cloud Run: production-grade but substantial setup overhead for a school demo

---

## Open Questions / Deferred

| Item | Status | Notes |
|------|--------|-------|
| LangGraph `PostgresSaver` connection pooling | Deferred to Phase 2 | Use `psycopg2` pool size 5 initially; tune if contention observed |
| Airflow XCom vs PostgreSQL for inter-task state | Deferred to Phase 3 | `PipelineState` dict serialized to JSONB column; XCom for small values only |
| `config/limits.yaml` schema | Deferred to Phase 4 | Must satisfy Constitution §VIII: `max_llm_calls_per_run`, `llm_batch_size`, `confidence_threshold` |
