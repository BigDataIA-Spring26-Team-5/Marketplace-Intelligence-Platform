# Research: UC REST API Layer

**Phase**: 0 — Unknowns resolved before design
**Feature**: specs/017-uc-rest-api-layer

---

## Decision 1: Async execution model for long-running pipeline runs

**Decision**: Submit-and-poll pattern. `POST /pipeline/runs` returns a `run_id` immediately; background task executes `graph.invoke()`. Client polls `GET /pipeline/runs/{run_id}/status`.

**Rationale**: Pipeline runs are chunked (10k rows/chunk), may process 50k+ rows. FastAPI `BackgroundTasks` supports fire-and-forget without a separate worker queue. `CheckpointManager` already persists run state in SQLite — the status endpoint reads from that, not from in-memory state. No new queue infrastructure needed for this phase.

**Alternatives considered**:
- Synchronous response: rejected — will timeout for large sources; Uvicorn default timeout is 60s.
- Celery/RQ task queue: rejected — adds Redis queue dependency and operational overhead not justified for single-server deployment.
- WebSocket streaming: deferred to future phase; adds client complexity not required by spec.

---

## Decision 2: API structure — new `src/api/` vs. extending `mcp_server.py`

**Decision**: New `src/api/` package with a `main.py` FastAPI app that mounts routers. The existing `mcp_server.py` is imported as a sub-application mounted at `/mcp` to preserve backward compatibility.

**Rationale**: `mcp_server.py` is a flat 400-line file with no router structure. Adding 4 new domains (pipeline, observability, search, ops) to it would make it unmanageable. Mounting it as a sub-app (`app.mount("/mcp", mcp_app)`) keeps all 7 existing tool endpoints working at their current paths while the new layer gets proper router organization.

**Alternatives considered**:
- Refactor `mcp_server.py` in-place: rejected — spec FR-013 requires existing endpoints unmodified.
- Separate port for new API: rejected — single API surface is simpler to document and secure.

---

## Decision 3: State persistence for background runs

**Decision**: Reuse `CheckpointManager` (SQLite at `checkpoint/checkpoint.db`) as the run state store. `POST /pipeline/runs` calls `CheckpointManager.create()` and returns the `run_id`. Status endpoint calls `CheckpointManager.load_checkpoint(run_id)`.

**Rationale**: `CheckpointManager` already tracks stage, chunk index, DQ scores, and plan YAML per run. No new storage layer needed. Run result (output path, row counts, audit log) is added as a field in the checkpoint record after `save_output_node` completes.

**Alternatives considered**:
- In-memory dict keyed by run_id: rejected — lost on server restart; no resume support.
- Separate Postgres table: rejected — over-engineering for current scale; SQLite checkpoint is already authoritative.

---

## Decision 4: UC3 / UC4 readiness handling

**Decision**: `HybridSearch.is_ready()` and `ProductRecommender.is_ready()` are checked at request time. If false, return HTTP 503 with `{"error": "service_unavailable", "detail": "index not ready — run scripts/build_corpus.py first"}`. No eager initialization at startup.

**Rationale**: Both classes already expose `is_ready()`. Lazy checking avoids blocking server startup when FAISS/ChromaDB are unavailable. 503 is the correct HTTP semantics for a temporarily unavailable service.

**Alternatives considered**:
- Raise 501 Not Implemented: rejected — the feature exists, the data just hasn't been loaded. 503 is more accurate and retryable.
- Block startup until ready: rejected — would prevent the API from starting in dev/test environments without a built corpus.

---

## Decision 5: Run cancellation

**Decision**: Out of scope for this phase. `POST /pipeline/runs/{run_id}/cancel` is reserved as a path but returns 501. Background tasks cannot be cancelled via `BackgroundTasks`; this requires a separate signal mechanism (e.g., a SQLite `cancelled` flag checked inside `run_pipeline_node`) that is deferred.

**Rationale**: Spec does not require cancellation. Adding partial cancellation would require modifying graph internals (adding checkpoint polling inside `run_pipeline_node`). Too much scope for this phase.

---

## Decision 6: Authentication

**Decision**: No authentication in this phase. All endpoints are unauthenticated. An `X-API-Key` header check middleware stub is included but disabled by default via `API_KEY_ENABLED=false` env var, so it can be enabled without code changes.

**Rationale**: Spec assumption explicitly defers auth. Stub allows future enablement without a code release.

---

## Decision 7: Observability router — wrap or re-expose MCP tools

**Decision**: The `/observability` router calls the same backing functions (`_pg_query`, `_prom_query`, `_prom_flat`) directly rather than proxying through the MCP tool endpoints. The MCP tool functions are already importable from `mcp_server.py`.

**Rationale**: Avoids HTTP-over-HTTP overhead. The MCP tools are thin wrappers around `_pg_query` and `_prom_query` — the new router shares the same helpers. Response shape for `/observability/*` endpoints uses standard REST pagination and filter params rather than the flat `ToolInput` shape.

---

## Decision 8: Project layout

**Decision**: New `src/api/` package:

```
src/api/
├── __init__.py
├── main.py                  # FastAPI app, mounts all routers + mcp sub-app
├── dependencies.py          # Shared: CacheClient, CheckpointManager, HybridSearch, Recommender singletons
├── routers/
│   ├── __init__.py
│   ├── pipeline.py          # /pipeline/*
│   ├── observability.py     # /observability/*
│   ├── search.py            # /search/*
│   ├── recommendations.py   # /recommendations/*
│   └── ops.py               # /ops/cache, /ops/schema
└── models/
    ├── __init__.py
    ├── pipeline.py          # RunRequest, RunStatus, RunResult Pydantic models
    ├── observability.py     # ObsQuery, RunSummary, BlockTrace, etc.
    ├── search.py            # SearchRequest, SearchResult
    ├── recommendations.py   # RecRequest, RecResult
    └── ops.py               # CacheStats, CacheFlushRequest, SchemaResponse
```

Entry point: `uvicorn src.api.main:app --host 0.0.0.0 --port 8002` (8001 stays for MCP server).
