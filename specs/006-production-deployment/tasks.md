---
description: "Task list for 006-production-deployment"
---

# Tasks: Production Deployment

**Input**: `specs/006-production-deployment/plan.md`, `data-model.md`, `contracts/api.yaml`, `research.md`, `quickstart.md`
**Branch**: `006-production-deployment`
**Date**: 2026-04-18

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: User story label (US1–US4)
- Exact file paths in all descriptions

## User Story Map

| Story | Phase | Goal | Priority |
|-------|-------|------|----------|
| US1 | Phase 1 — FastAPI Layer | `uvicorn api.main:app` works locally; curl triggers pipeline run | P1 🎯 MVP |
| US2 | Phase 2 — Docker + Postgres + Celery | `docker compose up` → full containerized stack; parquets in MinIO | P2 |
| US3 | Phase 3 — Airflow DAGs | Airflow UI at :8080; each run is a DAG run with per-task logs | P3 |
| US4 | Phase 4 — Performance | OFf chunk enrichment: ~40 min → <10 min; DeepSeek retries; async batching | P4 |

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Directory structure, dependency declarations, env config scaffold

- [ ] T001 Create `api/`, `api/routes/`, `api/models/`, `api/tasks/`, `api/db/`, `api/db/migrations/` directories
- [ ] T002 Create `src/storage/` directory
- [ ] T003 Add FastAPI, uvicorn, pydantic>=2, celery, redis, sqlalchemy, alembic, psycopg2-binary, tenacity, boto3, qdrant-client to `pyproject.toml` via `poetry add`
- [ ] T004 [P] Create `.env.example` at repo root with all required vars: `DEEPSEEK_API_KEY`, `DATABASE_URL`, `REDIS_URL`, `S3_ENDPOINT_URL`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `QDRANT_URL` (optional)
- [ ] T005 [P] Create `config/limits.yaml` with `max_llm_calls_per_run: 500`, `llm_batch_size: 20`, `confidence_threshold: 0.85` (Constitution §VIII: limits must be configurable, not hardcoded)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: SQLAlchemy models, Alembic scaffold, S3Storage class — required by all user stories

**⚠️ CRITICAL**: Complete before any US1–US4 work

- [ ] T006 Create SQLAlchemy ORM models `RunState`, `AuditLog`, `BlockRegistryEntry`, `HitlDecision` in `api/db/models.py` per `data-model.md` schema (UUIDs, JSONB fields, FK constraints, indexes)
- [ ] T007 Initialize Alembic in `api/db/migrations/`: `alembic init api/db/migrations`, configure `env.py` to use `DATABASE_URL` from env, set `target_metadata = Base.metadata`
- [ ] T008 Generate migration `001_create_run_state.py` covering all 4 tables in dependency order (run_state → audit_log → block_registry → hitl_decision)
- [ ] T009 [P] Create `src/storage/s3.py` with `S3Storage` class: `upload_parquet(run_id, chunk_index, df)`, `download_parquet(run_id, chunk_index)`, `upload_yaml(domain, dataset_name, content)`, `download_yaml(domain, dataset_name)`, `get_output_url(run_id, ttl=900)` — uses `S3_ENDPOINT_URL` env for MinIO/real-S3 switch
- [ ] T010 [P] Create `api/db/session.py` with SQLAlchemy `SessionLocal` factory and `get_db()` FastAPI dependency; connection pool size 5

**Checkpoint**: DB models + S3 abstraction complete — US1–US4 can begin

---

## Phase 3: User Story 1 — FastAPI Layer (P1) 🎯 MVP

**Goal**: Local API wrapping existing pipeline. No Docker/Celery yet — uses `BackgroundTasks` and in-memory run dict.

**Independent Test**:
```bash
uvicorn api.main:app --reload
curl -X POST localhost:8000/pipeline/run -d '{"source_path":"data/usda_fooddata_sample.csv","domain":"nutrition","hitl_mode":false}'
# poll /pipeline/{id}/status until "completed"
# GET /pipeline/{id}/output returns download_url
```

### Implementation for User Story 1

- [ ] T011 [P] [US1] Create `api/models/schema.py` with `UnifiedSchema` and `ColumnSpec` Pydantic v2 models; load `config/unified_schema.json` at import time and expose as `UNIFIED_SCHEMA` module constant
- [ ] T012 [P] [US1] Create `api/models/request.py` with `PipelineRunRequest` Pydantic model: `source_path: str`, `domain: Literal["nutrition","recalls","pricing"]`, `chunk_size: int = Field(10000, ge=1000, le=100000)`, `hitl_mode: bool = True`
- [ ] T013 [P] [US1] Create `api/models/response.py` with `RunAccepted`, `RunStatus`, `ApprovalRequest`, `ApprovalResponse`, `OutputResponse`, `AuditEntry` Pydantic models matching `contracts/api.yaml` schemas exactly
- [ ] T014 [US1] Create in-memory run store `api/store.py`: `dict[str, dict]` keyed by `run_id`; thread-safe via `threading.Lock`; functions `create_run(req) -> run_id`, `get_run(run_id) -> dict`, `update_run(run_id, **fields)`
- [ ] T015 [US1] Implement `api/routes/pipeline.py`: `POST /pipeline/run` (creates run, enqueues via `BackgroundTasks`, returns `RunAccepted`); background function calls `run_step()` sequence from `src/agents/graph.py` and writes result back to store
- [ ] T016 [US1] Implement `api/routes/pipeline.py`: `GET /pipeline/{run_id}/status` reads from store, returns `RunStatus`; `PATCH /pipeline/{run_id}/approve` updates store `status` to `approved`/`failed` and unblocks background task via `threading.Event`
- [ ] T017 [US1] Implement `api/routes/output.py`: `GET /pipeline/{run_id}/output` reads `output_path` from store, reads CSV bytes, returns as `FileResponse`; `GET /pipeline/{run_id}/audit` reads `audit_log` list from store
- [ ] T018 [US1] Create `api/main.py`: FastAPI app factory, mount `pipeline` and `output` routers, `GET /health`, lifespan context that loads `UNIFIED_SCHEMA` on startup
- [ ] T019 [US1] Wire HITL blocking in background task: when `hitl_mode=True` and `check_registry_node` completes, set `status=awaiting_approval` + `gate=schema_mapping` in store, then block on `threading.Event` until `PATCH /approve` fires; resume pipeline after event; repeat for quarantine gate after `run_pipeline_node`

**Checkpoint**: `uvicorn api.main:app` triggers real pipeline run; status polling and approval work end-to-end

---

## Phase 4: User Story 2 — Docker + PostgreSQL + Celery (P2)

**Goal**: Full stack in containers. Replace in-memory store with PostgreSQL. Replace `BackgroundTasks` with Celery. Parquet chunks in MinIO. Deploy to Railway/Fly.io.

**Independent Test**:
```bash
docker compose up --build -d
alembic upgrade head  # inside api container
curl -X POST localhost:8000/pipeline/run ...
# poll until completed
# check MinIO console (localhost:9001) for runs/{run_id}/chunks/
```

### Implementation for User Story 2

- [ ] T020 [US2] Create `Dockerfile`: `FROM python:3.11-slim`, `WORKDIR /app`, `COPY pyproject.toml poetry.lock ./`, `RUN pip install poetry && poetry install --no-root --only main`, `COPY . .`, `CMD ["uvicorn","api.main:app","--host","0.0.0.0","--port","8000"]`
- [ ] T021 [US2] Create `docker-compose.yml` with services: `api` (build `.`, port 8000, env from `.env`, depends_on postgres+redis+minio), `worker` (same image, `CMD ["celery","-A","api.tasks.celery_app","worker","--loglevel=info"]`), `postgres` (postgres:16, healthcheck), `redis` (redis:7-alpine), `minio` (minio/minio, ports 9000+9001, healthcheck)
- [ ] T022 [US2] Create `api/tasks/celery_app.py`: configure Celery with `REDIS_URL` broker and backend; create `run_pipeline_task` task that accepts `(run_id, req_dict)` and executes the full `run_step()` sequence (same logic as US1 background function)
- [ ] T023 [US2] Replace in-memory store in `api/routes/pipeline.py` with PostgreSQL: `POST /pipeline/run` creates `RunState` row; background/Celery task updates row status; `GET /status` reads from DB via `get_db()` dependency; remove `api/store.py`
- [ ] T024 [US2] Switch `POST /pipeline/run` from `BackgroundTasks` to `celery.delay()`: call `run_pipeline_task.delay(run_id, req.model_dump())`; store `celery_task_id` in `RunState`
- [ ] T025 [US2] Switch LangGraph checkpointer in `src/agents/graph.py`: import `langgraph.checkpoint.postgres.PostgresSaver`; when `DATABASE_URL` env set, use `PostgresSaver.from_conn_string(DATABASE_URL)` instead of `SqliteSaver`; keep SQLite as fallback for local non-Docker runs
- [ ] T026 [US2] Modify `src/pipeline/runner.py` `_write_chunk_output()` (or equivalent): after each chunk completes, upload DataFrame as parquet to S3 via `S3Storage.upload_parquet(run_id, chunk_index, df)` instead of accumulating in memory; update `RunState.output_s3_key` after merge
- [ ] T027 [US2] Replace `GET /pipeline/{run_id}/output` file response with presigned URL: call `S3Storage.get_output_url(run_id, ttl=900)`, return `OutputResponse` with `download_url` and `expires_at`
- [ ] T028 [US2] Write `AuditLog` rows to PostgreSQL at end of each chunk in Celery task; `GET /pipeline/{run_id}/audit` queries `AuditLog` table (filtered by `chunk_index` if query param set)
- [ ] T029 [US2] Add Railway/Fly.io deploy config: create `railway.json` or `fly.toml` pointing at `docker-compose.yml`; document deploy step in `quickstart.md` under "Deploy to Railway"

**Checkpoint**: `docker compose up` → curl → parquets in MinIO → output downloadable via presigned URL

---

## Phase 5: User Story 3 — Airflow DAGs (P3)

**Goal**: Replace manual `run_step()` sequencing with Airflow DAG. Per-task retry and monitoring. Dynamic task mapping for chunks.

**Independent Test**:
```bash
# Add airflow to docker-compose.yml, docker compose up
# Trigger via API: POST /pipeline/run → triggers DAG run
# Check Airflow UI at localhost:8080 — see DAG run with per-task status
```

### Implementation for User Story 3

- [ ] T030 [US3] Add Airflow to `docker-compose.yml`: services `airflow-scheduler` and `airflow-webserver` using `apache/airflow:2.9`, shared `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointing at postgres, mount `dags/` volume
- [ ] T031 [US3] Create `dags/etl_pipeline.py`: define `@dag` with `dag_id="etl_pipeline"`, `schedule=None` (triggered via API); define `@task` functions `load_source_task`, `analyze_schema_task`, `critique_schema_task`, `check_registry_task`, `plan_sequence_task`, `save_output_task` — each wraps corresponding `*_node()` function; pass `PipelineState` dict via XCom or `run_id` → DB lookup
- [ ] T032 [US3] Add `run_pipeline_chunks` dynamic task mapping in `dags/etl_pipeline.py`: `@task` `compute_chunk_indices(run_id)` returns list of ints; `run_pipeline_chunks.expand(chunk_index=chunk_indices)` fans out Celery `run_pipeline_task` calls per chunk; `merge_chunks_task` collects S3 parquets and writes final output
- [ ] T033 [US3] Modify `POST /pipeline/run` in `api/routes/pipeline.py`: when `AIRFLOW_URL` env set, trigger DAG via Airflow REST API `POST /api/v1/dags/etl_pipeline/dagRuns` with `conf={"run_id": run_id, "req": req_dict}`; store `dag_run_id` in `RunState`; keep Celery path as fallback when `AIRFLOW_URL` not set
- [ ] T034 [US3] Store `PipelineState` dicts in PostgreSQL JSONB: add `state_snapshot JSONB` column to `run_state` table (new Alembic migration `002_add_state_snapshot`); Airflow tasks read/write state via `run_id` DB lookup instead of XCom for large DataFrames

**Checkpoint**: Airflow UI shows DAG run; per-task logs accessible; failed task retries without restarting earlier tasks

---

## Phase 6: User Story 4 — Performance (P4)

**Goal**: Eliminate LLM enrichment bottleneck. Add tenacity retries to `call_llm()`. Async batching in `llm_tier.py`. Batch `model.encode()`. Optional Qdrant.

**Independent Test**:
```bash
# Run OFf chunk (3.4M rows) with Phase 4 changes
# Verify chunk 1 enrichment completes in <10 min (vs ~40 min baseline)
# Induce DeepSeek 429 error — verify tenacity retries with backoff and recovers
```

### Implementation for User Story 4

- [ ] T035 [P] [US4] Add `@retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))` decorator to `call_llm()` in `src/models/llm.py`; log each retry attempt at WARNING level
- [ ] T036 [P] [US4] Replace sequential `for batch in batches: litellm.completion(...)` loop in `src/enrichment/llm_tier.py` with `asyncio.gather(*[litellm.acompletion(...) for batch in batches])`; add `asyncio.Semaphore(10)` to cap concurrent requests at DeepSeek RPM limit; call from sync context via `asyncio.run()`
- [ ] T037 [P] [US4] Replace per-row `model.encode(query)` in `src/enrichment/embedding.py` with single `model.encode(all_queries)` call over all rows in chunk; then vectorized FAISS search over result matrix; eliminates N individual encode calls per chunk
- [ ] T038 [US4] Accumulate corpus additions in `src/enrichment/llm_tier.py` per-chunk: collect new `(text, vector, metadata)` tuples during chunk processing, call `save_corpus()` once after chunk completes (not after each row); reduces FAISS serialization from N to 1 per chunk
- [ ] T039 [US4] Add Qdrant feature flag to `src/enrichment/corpus.py`: `if os.environ.get("QDRANT_URL"): use QdrantClient for knn_search() and add_to_corpus() else: use FAISS`; keep existing FAISS code path intact; add `qdrant-client` to optional deps in `pyproject.toml`
- [ ] T040 [US4] Load `config/limits.yaml` in `src/enrichment/llm_tier.py` at import: read `max_llm_calls_per_run`, `llm_batch_size`, `confidence_threshold`; replace hardcoded `LLM_ENRICH_BATCH_SIZE` env var with `limits.yaml` value (env var still overrides if set for backward compat)
- [ ] T041 [US4] Add auto-approval to HITL gates: in background task / Celery task, after critic review, compute confidence score from `critique_notes`; if all ops score >= `confidence_threshold` from `config/limits.yaml`, set `hitl_decision.decision="approved"` automatically and continue without blocking on `threading.Event`

**Checkpoint**: OFf enrichment benchmark confirms <10 min per chunk; DeepSeek 429s recovered via retry; `config/limits.yaml` controls LLM budget

---

## Phase 7: Polish & Cross-Cutting

- [ ] T042 Drop `_knn_neighbors` column before `save_output_node` in `src/agents/graph.py` (known gotcha: column leak into final CSV output)
- [ ] T043 [P] Add `GET /health` endpoint to `api/main.py` that checks DB connectivity and returns `{"status":"ok","db":"ok","celery":"ok"}` (or degraded)
- [ ] T044 [P] Write contract tests in `tests/contract/test_api_models.py`: validate `PipelineRunRequest`, `RunStatus`, `AuditEntry` Pydantic models against `contracts/api.yaml` schemas using `pydantic.validate_call`
- [ ] T045 [P] Write integration test in `tests/integration/test_pipeline_run.py` using `testcontainers` (PostgreSQL + MinIO + Redis): submit run via `TestClient`, poll until completed, verify `RunState.status=="completed"` in DB and parquet in MinIO
- [ ] T046 Update `quickstart.md` "Deploy to Railway" section with actual `railway up` output and verified env var list after Phase 2 deploy succeeds
- [ ] T047 Update `CLAUDE.md` commands section with `uvicorn api.main:app`, `docker compose up --build -d`, `celery -A api.tasks.celery_app worker` startup commands
- [ ] T048 Verify YAML mapping replay still works end-to-end: run `demo.py` Run 3 (FDA Recalls replay) against deployed API; confirm `block_registry_hits` populated from PostgreSQL `block_registry` table

---

## Dependencies (Story Completion Order)

```
Phase 1 (Setup)
    └── Phase 2 (Foundational: DB models + S3Storage)
            ├── Phase 3 (US1: FastAPI local) ← MVP deliverable
            │       └── Phase 4 (US2: Docker + Celery)  ← requires running API
            │               ├── Phase 5 (US3: Airflow)  ← requires Celery worker
            │               └── Phase 6 (US4: Perf)     ← independent of Airflow
            └── Phase 7 (Polish) ← after US1 complete
```

US3 and US4 are **independent** of each other — can proceed in parallel after US2.

---

## Parallel Execution Opportunities

### Within US1 (T011–T019):
- T011, T012, T013 in parallel (3 separate model files, no cross-deps)
- T014 after models complete
- T015, T016, T017 after T014 (different route files)
- T018 after routes complete
- T019 last (wires all together)

### Within US2 (T020–T029):
- T020 (Dockerfile) independent
- T021 (docker-compose) independent of T020 but authored together
- T022 (Celery app) independent of T020–T021
- T023, T025, T026 in parallel after T022 (different files)
- T024 after T022 + T023
- T027 after T009 (S3Storage)
- T028 after T006 (DB models)

### Within US4 (T035–T041):
- T035, T036, T037, T038 in parallel (different files, no cross-deps)
- T039 after T037 (extends embedding path)
- T040 before T041 (limits.yaml must exist)

---

## Implementation Strategy

**MVP** = Phase 1 + Phase 2 + Phase 3 (US1) = local API, no infra

Deliver in this order:
1. **T001–T005** (Setup): ~30 min
2. **T006–T010** (Foundational): ~1 hr
3. **T011–T019** (US1): ~3 hrs — first curl-to-completion demo
4. **T020–T029** (US2): ~4 hrs — docker compose demo, deploy
5. **T030–T034** (US3): ~3 hrs — Airflow UI demo
6. **T035–T041** (US4): ~2 hrs — perf validation
7. **T042–T048** (Polish): ~1 hr

---

## Task Count Summary

| Phase | Tasks | Notes |
|-------|-------|-------|
| Setup | T001–T005 | 5 tasks |
| Foundational | T006–T010 | 5 tasks |
| US1 FastAPI | T011–T019 | 9 tasks |
| US2 Docker+Celery | T020–T029 | 10 tasks |
| US3 Airflow | T030–T034 | 5 tasks |
| US4 Performance | T035–T041 | 7 tasks |
| Polish | T042–T048 | 7 tasks |
| **Total** | **T001–T048** | **48 tasks** |
