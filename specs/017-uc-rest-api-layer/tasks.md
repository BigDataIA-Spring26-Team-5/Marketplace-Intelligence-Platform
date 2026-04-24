# Tasks: UC REST API Layer

**Input**: Design documents from `specs/017-uc-rest-api-layer/`
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/openapi.md ✓, quickstart.md ✓
**Clarifications applied**: chunk_index (not %) for progress; `/v1/` prefix on all routers; concurrent runs capped at `MAX_CONCURRENT_RUNS`; startup orphan cleanup; per-IP rate limit on run submission.

**Tests**: Included per plan.md (`tests/api/` explicitly in project structure).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no blocking dependencies)
- **[Story]**: User story label (US1–US5)
- Exact file paths in all tasks

---

## Phase 1: Setup

**Purpose**: Create `src/api/` package skeleton and verify dependencies.

- [x] T001 Create `src/api/` package with subdirectories `routers/` and `models/` — add `__init__.py` to each; create `tests/api/__init__.py`
- [x] T002 Verify FastAPI, Uvicorn, and `slowapi` are declared in `pyproject.toml`; add any missing deps and run `poetry install`

**Checkpoint**: `src/api/`, `tests/api/` exist; `poetry install` succeeds.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core app wiring, all Pydantic models, and shared infrastructure. Must complete before any router work.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T003 Create `src/api/main.py` — `FastAPI(root_path="/v1")` app; register `slowapi` `RateLimiter` as state; add `@app.on_event("startup")` handler that calls `CheckpointManager` to transition any `"running"` records to `"failed"` with `error="server_restart"`; add `GET /health` endpoint checking Redis, Postgres, Prometheus, UC3/UC4 `is_ready()`; mount MCP sub-app at `/mcp` via `app.mount("/mcp", mcp_app)`; add CORS middleware
- [x] T004 [P] Create `src/api/dependencies.py` — module-level singletons for `CacheClient`, `CheckpointManager`, `HybridSearch`, `ProductRecommender`; expose as FastAPI `Depends` callables; add `_run_semaphore: asyncio.Semaphore` initialized from `int(os.getenv("MAX_CONCURRENT_RUNS", "2"))`
- [x] T005 [P] Create `src/api/models/pipeline.py` — `RunRequest`, `RunStatus` (with `chunk_index: int | None`, no `progress_percent`), `RunResult`, `BlockAuditEntry`, `ResumeRequest` per data-model.md
- [x] T006 [P] Create `src/api/models/observability.py` — `RunSummary`, `RunListResponse`, `BlockTrace`, `BlockTraceEntry`, `AnomalyRecord`, `QuarantineRecord`, `CostReport`, `SourceCost`, `DedupStats` per data-model.md
- [x] T007 [P] Create `src/api/models/search.py` — `SearchRequest`, `SearchResult`, `SearchHit` per data-model.md
- [x] T008 [P] Create `src/api/models/recommendations.py` — `RecommendationResult`, `RecHit` per data-model.md
- [x] T009 [P] Create `src/api/models/ops.py` — `CacheStats`, `CacheFlushRequest`, `CacheFlushResult`, `SchemaResponse`, `ColumnDef` per data-model.md

**Checkpoint**: `uvicorn src.api.main:app --port 8002` starts; `GET /health` returns 200; `GET /mcp/tools` returns existing MCP tool list; startup log shows orphan-cleanup ran.

---

## Phase 3: User Story 1 — Trigger and Monitor a Pipeline Run (Priority: P1) 🎯 MVP

**Goal**: Operators can submit, poll, and retrieve pipeline runs via HTTP without CLI or Streamlit.

**Independent Test**: `POST /v1/pipeline/runs` with `data/usda_fooddata_sample.csv` + domain `nutrition` → 202 with `run_id`. Poll `GET /v1/pipeline/runs/{run_id}/status` until `status="completed"`. Verify `GET /v1/pipeline/runs/{run_id}/result` returns non-zero `rows_out`, `dq_score_post`, and `block_audit` list. Submitting 3rd concurrent run while 2 are running → 429 with `Retry-After`.

- [x] T010 [US1] Create `src/api/routers/pipeline.py` — `POST /v1/pipeline/runs`: validate `RunRequest`; acquire `_run_semaphore` (return 429 + `Retry-After: 60` if cap exceeded); check for in-progress run on same source (return 409 with existing `run_id`); call `CheckpointManager.create()`; fire `graph.invoke(state)` via `BackgroundTasks`; return 202 `RunStatus`. Decorate with `@limiter.limit(os.getenv("PIPELINE_RATE_LIMIT", "10/minute"))` from `slowapi`; release semaphore in background task finally block.
- [x] T011 [US1] Add `GET /v1/pipeline/runs/{run_id}/status` to `src/api/routers/pipeline.py` — call `CheckpointManager.load_checkpoint(run_id)`; map checkpoint fields to `RunStatus` (`chunk_index` from `get_chunk_resume_index`); return 404 for unknown `run_id`
- [x] T012 [US1] Add `GET /v1/pipeline/runs/{run_id}/result` to `src/api/routers/pipeline.py` — load checkpoint; return 409 with `{"error": "run_not_complete", "detail": "...", "run_id": run_id}` if `status != "completed"`; build `RunResult` with `block_audit` from stored audit log in checkpoint
- [x] T013 [US1] Add `POST /v1/pipeline/runs/{run_id}/resume` to `src/api/routers/pipeline.py` — call `CheckpointManager.load_checkpoint(run_id)`; return 409 if status is not `"failed"`; call `get_resume_state()`; re-fire `graph.invoke` with checkpoint state via `BackgroundTasks`; acquire semaphore with same cap logic; return 202 `RunStatus`
- [x] T014 [US1] Add `POST /v1/pipeline/runs/{run_id}/cancel` stub to `src/api/routers/pipeline.py` — always return 501 `{"error": "not_implemented", "detail": "Run cancellation not supported in this version"}`
- [x] T015 [US1] Register `/v1/pipeline` router in `src/api/main.py` via `app.include_router(pipeline_router, prefix="/v1/pipeline")`
- [ ] T016 [US1] Create `tests/api/test_pipeline.py` — test: submit returns 202, status poll reflects `chunk_index`, result returns DQ fields, resume returns 409 for non-failed run, cancel returns 501, unknown run_id returns 404, 11th submission/minute returns 429, 3rd concurrent run returns 429

**Checkpoint**: Full submit→poll→result cycle with small local CSV. `MAX_CONCURRENT_RUNS` semaphore enforced. Rate limit fires on excess submissions. Startup orphan cleanup transitions stale `"running"` records.

---

## Phase 4: User Story 2 — Query Observability Data (Priority: P2)

**Goal**: Engineers can query run history, block traces, anomalies, quarantine, cost, and dedup via HTTP.

**Independent Test**: Against existing run data — `GET /v1/observability/runs?domain=nutrition&page_size=5` returns paginated `RunSummary` list. `GET /v1/observability/anomalies` returns 200 (empty list acceptable). All endpoints return 200, not 500, when underlying stores are empty.

- [x] T017 [P] [US2] Create `src/api/routers/observability.py` — `GET /v1/observability/runs`: query `RunLogStore.load_all()`, apply `source`/`domain`/`status`/`from_date`/`to_date` filters, paginate via `page`/`page_size` (max 100), return `RunListResponse`
- [x] T018 [P] [US2] Add `GET /v1/observability/runs/{run_id}/trace` to `src/api/routers/observability.py` — call `_pg_query("SELECT * FROM block_trace WHERE run_id = %s ORDER BY started_at", (run_id,))` using helpers imported from `mcp_server`; return `BlockTrace`; 404 if no rows
- [x] T019 [P] [US2] Add `GET /v1/observability/anomalies` to `src/api/routers/observability.py` — query `anomaly_reports` Postgres table with optional `source` filter and `limit` (default 20); return list of `AnomalyRecord`
- [x] T020 [P] [US2] Add `GET /v1/observability/quarantine` to `src/api/routers/observability.py` — query `quarantine_rows` Postgres table with optional `run_id`/`source` filter and `limit` (default 50); return list of `QuarantineRecord`
- [x] T021 [P] [US2] Add `GET /v1/observability/cost` to `src/api/routers/observability.py` — query Prometheus `etl_llm_tokens_total` via `_prom_flat()` helper imported from `mcp_server`; group by `source` + `model_tier` labels; return `CostReport`; return empty report (not 500) if Prometheus unavailable
- [x] T022 [P] [US2] Add `GET /v1/observability/dedup` to `src/api/routers/observability.py` — query `dedup_clusters` Postgres table with optional `run_id`/`source`; compute `dedup_rate = merged_rows / total_rows`; return `DedupStats`
- [x] T023 [US2] Register `/v1/observability` router in `src/api/main.py` via `app.include_router(obs_router, prefix="/v1/observability")`
- [ ] T024 [US2] Create `tests/api/test_observability.py` — test: runs list 200 + pagination fields present, trace 404 for unknown run_id, anomalies 200 with empty list, quarantine 200, cost 200 (mock Prometheus unavailable → still 200 empty), dedup 200

**Checkpoint**: All 6 observability endpoints return 200 against existing run data. MCP `/tools/*` still returns identical responses to pre-feature baseline.

---

## Phase 5: User Story 3 — Search the Product Catalog (Priority: P3)

**Goal**: Callers get ranked search results; 503 (not crash) when index not built.

**Independent Test**: With unbuilt corpus → `POST /v1/search/query` returns 503 with corpus build instruction in `detail`. Empty query string → 422. `GET /v1/search/status` returns `{"ready": false}`.

- [x] T025 [US3] Create `src/api/routers/search.py` — `POST /v1/search/query`: validate `query` non-empty (422 if blank); call `HybridSearch.is_ready()` — if false return 503 `{"error": "service_unavailable", "detail": "Search index not ready. Run: poetry run python scripts/build_corpus.py"}`; call `hybrid_search.search(query, top_k, mode)`; apply `domain`/`category` post-filters; return `SearchResult`
- [x] T026 [US3] Add `GET /v1/search/status` to `src/api/routers/search.py` — return `{"ready": bool, "backend": "hybrid"}`
- [x] T027 [US3] Register `/v1/search` router in `src/api/main.py` via `app.include_router(search_router, prefix="/v1/search")`
- [ ] T028 [US3] Create `tests/api/test_search.py` — test: 503 when `is_ready()` False (mock), 422 on empty query, 200 with `SearchHit` list when ready (mock), status endpoint returns `ready` bool

**Checkpoint**: 503 for missing index; 422 for empty query; no unhandled exceptions. Existing corpus files unmodified.

---

## Phase 6: User Story 4 — Get Product Recommendations (Priority: P4)

**Goal**: Callers get `also-bought` / `you-might-like` recs; 503 when graph not built; 404 for unknown product.

**Independent Test**: `GET /v1/recommendations/any-id/also-bought` → 503 when graph not built. `GET /v1/recommendations/status` → `{"ready": false}`. No 500s.

- [x] T029 [US4] Create `src/api/routers/recommendations.py` — `GET /v1/recommendations/{product_id}/also-bought`: call `Recommender.is_ready()` — 503 if false; call `recommender.also_bought(product_id, top_k)`; if result empty return 404 `{"error": "not_found", "detail": "product_id not in recommendation graph"}`; return `RecommendationResult`
- [x] T030 [US4] Add `GET /v1/recommendations/{product_id}/you-might-like` to `src/api/routers/recommendations.py` — same guard pattern; call `recommender.you_might_like(product_id, top_k)`; 404 on empty result
- [x] T031 [US4] Add `GET /v1/recommendations/status` to `src/api/routers/recommendations.py` — if `is_ready()` return `{"ready": true, **recommender.stats()}`; else return `{"ready": false, "products": 0, "rules": 0, "graph_edges": 0}`
- [x] T032 [US4] Register `/v1/recommendations` router in `src/api/main.py` via `app.include_router(rec_router, prefix="/v1/recommendations")`
- [ ] T033 [US4] Create `tests/api/test_recommendations.py` — test: 503 when not ready (mock), 404 for unknown product_id (mock empty result), 200 with `RecHit` list when ready (mock), status endpoint both states

**Checkpoint**: 503 for unbuilt graph; 404 for unknown product; no crashes. Graph build state unmodified.

---

## Phase 7: User Story 5 — Cache and Schema Management (Priority: P4)

**Goal**: Operators inspect and flush cache, retrieve domain schemas, via HTTP.

**Independent Test**: `GET /v1/ops/cache/stats` returns dict with `by_prefix` keys `["yaml","llm","emb","dedup"]`. `POST /v1/ops/cache/flush` with `confirm: false` → 422. `GET /v1/ops/schema/invalid_domain` → 404.

- [x] T034 [P] [US5] Create `src/api/routers/ops.py` — `GET /v1/ops/cache/stats`: call `CacheClient.get_stats()`; return `CacheStats`
- [x] T035 [P] [US5] Add `POST /v1/ops/cache/flush` to `src/api/routers/ops.py` — validate `confirm: true` (422 if false); if `prefix` given call `cache_client.delete(prefix, ...)`; if null call `flush_all_prefixes()`; domain filter removes matching `yaml` keys; return `CacheFlushResult` with `deleted_count`
- [x] T036 [P] [US5] Add `GET /v1/ops/schema/{domain}` to `src/api/routers/ops.py` — resolve `config/schemas/{domain}_schema.json`; return 404 `{"error": "not_found", "detail": "No schema file for domain '{domain}'"}` if absent; parse JSON; annotate each column as `enrichment=True` for `["allergens","primary_category","dietary_tags","is_organic"]` and `computed=True` for `["dq_score_pre","dq_score_post","dq_delta"]`; return `SchemaResponse`
- [x] T037 [US5] Register `/v1/ops` router in `src/api/main.py` via `app.include_router(ops_router, prefix="/v1/ops")`
- [ ] T038 [US5] Create `tests/api/test_ops.py` — test: cache stats 200 with expected prefix keys, flush `confirm=false` → 422, flush `confirm=true` → 200 with `deleted_count`, schema known domain → 200 with annotated columns, schema unknown domain → 404

**Checkpoint**: All ops endpoints functional. `confirm: false` rejected. Schema 404 for absent files. Safety field annotations correct.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Contract updates for `/v1/` paths, auth stub, docs, backward-compat verification.

- [x] T039 Add `X-API-Key` auth middleware stub in `src/api/main.py` — reads `API_KEY_ENABLED` env var (default `"false"`); when `"true"`, validate `X-API-Key` header on all routes except `GET /health`; return 401 if missing/invalid; when disabled, pass through silently; document `API_KEY` env var
- [x] T040 [P] Update `specs/017-uc-rest-api-layer/contracts/openapi.md` — add `/v1/` prefix to all endpoint paths; add `POST /v1/pipeline/runs` 429 response; add startup orphan-cleanup note to `/health` description; add `FR-015` rate limit note
- [x] T041 [P] Update `specs/017-uc-rest-api-layer/quickstart.md` — update all curl examples to use `/v1/` prefix; add `MAX_CONCURRENT_RUNS`, `PIPELINE_RATE_LIMIT`, `API_KEY_ENABLED`, `API_KEY` to env vars table
- [x] T042 [P] Update `ENDPOINTS.md` — add port 8002 section listing all `/v1/*` routers; note MCP accessible at both `localhost:8001` and `localhost:8002/mcp`; add `uvicorn src.api.main:app --port 8002` to startup commands
- [x] T043 [P] Update `CLAUDE.md` Common commands block — add `uvicorn src.api.main:app --host 0.0.0.0 --port 8002` entry with note on `/v1/` prefix and Swagger at `:8002/docs`
- [x] T044 Smoke test: start both servers; verify `GET localhost:8001/tools` and `GET localhost:8002/mcp/tools` return identical JSON; confirm no `import src.api` leaks into `src/agents/graph.py`, `src/pipeline/cli.py`, or `app.py`

**Checkpoint**: All 44 tasks complete. Both servers start independently. `/v1/` prefix consistent across all artifacts. MCP 8001 unaffected.

---

## Dependencies (Story Completion Order)

```
Phase 1 (T001–T002)
    ↓
Phase 2 (T003–T009)   ← T004–T009 fully parallel after T003 exists
    ↓
Phase 3 (T010–T016)   ← US1 (P1) MVP — must ship before Phases 4–7
    ↓
┌──────────┬──────────┬──────────┬──────────┐
Phase 4    Phase 5    Phase 6    Phase 7    ← all independent, run in parallel
(US2)      (US3)      (US4)      (US5)
└──────────┴──────────┴──────────┴──────────┘
    ↓ (all complete)
Phase 8 (T039–T044)
```

---

## Parallel Execution Examples

**Within Phase 2** — all model files independent:
```
T004 dependencies.py  |  T005 models/pipeline.py  |  T006 models/observability.py
T007 models/search.py |  T008 models/recommendations.py  |  T009 models/ops.py
```

**After Phase 3 ships** — 4 parallel streams:
```
Stream 1: T017–T024 (US2 observability)
Stream 2: T025–T028 (US3 search)
Stream 3: T029–T033 (US4 recommendations)
Stream 4: T034–T038 (US5 ops)
```

---

## Implementation Strategy

**MVP** = Phases 1–3 (T001–T016, 16 tasks).

Delivers: versioned pipeline submission, status polling (`chunk_index`), result retrieval, resume, concurrent run cap, rate limiting, startup orphan cleanup. Everything else layers on.

**Suggested delivery order after MVP**:
1. US5 ops (5 tasks, no external dependencies) — high operator utility
2. US2 observability (8 tasks, backed by existing Postgres/Prometheus)
3. US3 search (4 tasks, requires corpus build first)
4. US4 recommendations (5 tasks, requires graph build first)

---

## Summary

| Phase | Scope | Tasks |
|---|---|---|
| Phase 1 | Setup | 2 |
| Phase 2 | Foundation | 7 |
| Phase 3 | US1 Pipeline | 7 |
| Phase 4 | US2 Observability | 8 |
| Phase 5 | US3 Search | 4 |
| Phase 6 | US4 Recommendations | 5 |
| Phase 7 | US5 Ops | 5 |
| Phase 8 | Polish | 6 |
| **Total** | | **44** |

Parallelizable [P] tasks: 22
MVP scope (Phases 1–3): 16 tasks
