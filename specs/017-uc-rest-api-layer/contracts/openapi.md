# API Contract: UC REST API Layer

**Base URL**: `http://localhost:8002`
**MCP sub-app** (existing, preserved): `http://localhost:8002/mcp/tools/*`
**Format**: JSON
**Auth**: None (phase 1). Opt-in via `API_KEY_ENABLED=true` + `X-API-Key` header.

---

## Standard Error Envelope

All error responses use this shape:

```json
{
  "error": "error_code",
  "detail": "Human-readable description",
  "run_id": "optional-if-applicable"
}
```

Common error codes: `validation_error`, `not_found`, `service_unavailable`, `conflict`, `not_implemented`

---

## /pipeline

### POST /v1/pipeline/runs
Submit a new pipeline run.

**Request body:**
```json
{
  "source_path": "data/usda_fooddata_sample.csv",
  "domain": "nutrition",
  "pipeline_mode": "full",
  "with_critic": false,
  "force_fresh": false,
  "no_cache": false,
  "chunk_size": 10000,
  "source_name": null
}
```

**Response 202:**
```json
{
  "run_id": "a3f2c1d0-...",
  "status": "pending",
  "stage": null,
  "chunk_index": null,
  "started_at": "2026-04-24T10:00:00Z",
  "updated_at": "2026-04-24T10:00:00Z",
  "error": null
}
```

**Response 409** — run already in progress for same source:
```json
{ "error": "conflict", "detail": "Run abc123 already running for this source", "run_id": "abc123" }
```

---

### GET /v1/pipeline/runs/{run_id}/status
Poll run status.

**Response 200:**
```json
{
  "run_id": "a3f2c1d0-...",
  "status": "running",
  "stage": "run_pipeline",
  "chunk_index": 3,
  "started_at": "2026-04-24T10:00:00Z",
  "updated_at": "2026-04-24T10:00:42Z",
  "error": null
}
```

**Response 404** — unknown run_id.

---

### GET /v1/pipeline/runs/{run_id}/result
Fetch completed run output.

**Response 200:**
```json
{
  "run_id": "a3f2c1d0-...",
  "status": "completed",
  "output_path": "output/silver/nutrition/usda_fooddata_sample.parquet",
  "rows_in": 50000,
  "rows_out": 49823,
  "rows_quarantined": 177,
  "dq_score_pre": 0.71,
  "dq_score_post": 0.89,
  "dq_delta": 0.18,
  "block_audit": [
    { "block": "dq_score_pre", "rows_in": 50000, "rows_out": 50000, "duration_ms": 340, "extra": {} },
    { "block": "__generated__", "rows_in": 50000, "rows_out": 50000, "duration_ms": 210, "extra": {} }
  ],
  "completed_at": "2026-04-24T10:05:12Z"
}
```

**Response 404** — unknown run_id.
**Response 409** — run not yet completed (`status != "completed"`).

---

### POST /v1/pipeline/runs/{run_id}/resume
Resume from last checkpoint.

**Request body:** `{}` (empty)

**Response 202:** Same shape as POST /v1/pipeline/runs response.

**Response 404** — unknown run_id.
**Response 409** — run not in `failed` state.

---

### POST /v1/pipeline/runs/{run_id}/cancel
Cancel an in-progress run.

**Response 501:**
```json
{ "error": "not_implemented", "detail": "Run cancellation is not supported in this version" }
```

---

## /observability

### GET /v1/observability/runs
List run history with optional filters.

**Query params:**
- `source` (str, optional)
- `domain` (str, optional)
- `status` (str, optional)
- `from_date` (ISO8601, optional)
- `to_date` (ISO8601, optional)
- `page` (int, default 1)
- `page_size` (int, default 20, max 100)

**Response 200:**
```json
{
  "runs": [
    {
      "run_id": "...",
      "source": "usda_fooddata_sample",
      "domain": "nutrition",
      "status": "completed",
      "dq_score_pre": 0.71,
      "dq_score_post": 0.89,
      "started_at": "2026-04-24T10:00:00Z",
      "completed_at": "2026-04-24T10:05:12Z",
      "rows_in": 50000,
      "rows_out": 49823
    }
  ],
  "total": 42,
  "page": 1,
  "page_size": 20
}
```

---

### GET /v1/observability/runs/{run_id}/trace
Block-level execution trace for a run.

**Response 200:**
```json
{
  "run_id": "...",
  "blocks": [
    { "block": "dq_score_pre", "rows_in": 50000, "rows_out": 50000, "started_at": "...", "duration_ms": 340 }
  ]
}
```

---

### GET /v1/observability/anomalies
Recent anomaly flags from Isolation Forest.

**Query params:** `source` (optional), `limit` (default 20)

**Response 200:**
```json
{
  "anomalies": [
    { "source": "fda_recalls", "anomaly_score": -0.23, "flagged_at": "...", "metrics": { "etl_dq_score_post": 0.41 } }
  ]
}
```

---

### GET /v1/observability/quarantine
Quarantined rows.

**Query params:** `run_id` (optional), `source` (optional), `limit` (default 50)

**Response 200:**
```json
{
  "records": [
    { "run_id": "...", "row_index": 412, "reason": "product_name null after enrichment", "fields": { "brand_name": "Acme" } }
  ]
}
```

---

### GET /v1/observability/cost
LLM cost report.

**Query params:** `from_date`, `to_date` (ISO8601, optional)

**Response 200:**
```json
{
  "period_start": "2026-04-01T00:00:00Z",
  "period_end": "2026-04-24T23:59:59Z",
  "by_source": [
    { "source": "usda_fooddata_sample", "model_tier": "groq", "tokens_used": 840000, "requests": 420 }
  ],
  "total_tokens": 840000,
  "estimated_usd": null
}
```

---

### GET /v1/observability/dedup
Dedup cluster statistics.

**Query params:** `run_id` (optional), `source` (optional)

**Response 200:**
```json
{
  "run_id": null,
  "source": "usda_fooddata_sample",
  "clusters": 312,
  "merged_rows": 891,
  "dedup_rate": 0.018
}
```

---

## /search

### POST /v1/search/query
Hybrid product search.

**Request body:**
```json
{
  "query": "organic almond milk",
  "domain": "nutrition",
  "category": null,
  "top_k": 10,
  "mode": "hybrid"
}
```

**Response 200:**
```json
{
  "query": "organic almond milk",
  "mode": "hybrid",
  "total": 10,
  "index_ready": true,
  "results": [
    {
      "product_name": "Silk Organic Unsweetened Almond Milk",
      "brand_name": "Silk",
      "primary_category": "plant-based milk",
      "data_source": "usda_fooddata_sample",
      "is_recalled": false,
      "recall_class": null,
      "score": 0.87,
      "rank": 1
    }
  ]
}
```

**Response 503** — index not ready:
```json
{ "error": "service_unavailable", "detail": "Search index not ready. Run: poetry run python scripts/build_corpus.py" }
```

---

### GET /v1/search/status
Search index readiness check.

**Response 200:**
```json
{ "ready": true, "backend": "hybrid" }
```

---

## /recommendations

### GET /v1/recommendations/{product_id}/also-bought
Association-rule based recommendations.

**Query params:** `top_k` (int, default 5, max 20)

**Response 200:**
```json
{
  "product_id": "usda-12345",
  "rec_type": "also_bought",
  "top_k": 5,
  "graph_ready": true,
  "results": [
    { "product_id": "usda-67890", "product_name": "...", "primary_category": "...", "score": 0.72, "rank": 1, "extra": { "lift": 2.4 } }
  ]
}
```

**Response 404** — unknown product_id.
**Response 503** — graph not ready.

---

### GET /v1/recommendations/{product_id}/you-might-like
Graph-traversal based recommendations.

**Query params:** `top_k` (int, default 5, max 20)

**Response 200:** Same shape as `also-bought`, `extra` contains `{ "hops": 2 }`.

---

### GET /v1/recommendations/status
Recommendation graph readiness check.

**Response 200:**
```json
{ "ready": false, "products": 0, "rules": 0, "graph_edges": 0 }
```

---

## /ops

### GET /v1/ops/cache/stats
Cache statistics.

**Response 200:**
```json
{
  "redis_connected": true,
  "total_keys": 1240,
  "by_prefix": { "yaml": 18, "llm": 402, "emb": 800, "dedup": 20 },
  "sqlite_fallback": false,
  "sqlite_key_count": null
}
```

---

### POST /v1/ops/cache/flush
Flush cache keys.

**Request body:**
```json
{
  "prefix": "llm",
  "domain": null,
  "confirm": true
}
```

**Response 200:**
```json
{ "deleted_count": 402, "prefix": "llm", "domain": null }
```

**Response 422** — `confirm` is false.

---

### GET /v1/ops/schema/{domain}
Domain schema definition.

**Path params:** `domain` = "nutrition" | "safety" | "pricing"

**Response 200:**
```json
{
  "domain": "nutrition",
  "source_file": "config/schemas/nutrition_schema.json",
  "columns": [
    { "name": "product_name", "dtype": "str", "required": true, "enrichment": false, "computed": false },
    { "name": "primary_category", "dtype": "str", "required": false, "enrichment": true, "computed": false },
    { "name": "dq_score_post", "dtype": "float", "required": false, "enrichment": false, "computed": true }
  ]
}
```

**Response 404** — schema file does not exist for domain.

---

## /health

### GET /health
Service health and dependency status.

**Response 200:**
```json
{
  "status": "ok",
  "dependencies": {
    "redis": "ok",
    "postgres": "ok",
    "prometheus": "degraded",
    "search_index": "not_ready",
    "rec_graph": "not_ready"
  }
}
```

`status` = "ok" if all critical deps ok; "degraded" if non-critical deps down.
Critical: none (API continues running regardless). Degraded states are informational.

---

## /mcp (sub-app, preserved)

All existing endpoints at `http://localhost:8001/tools/*` are also accessible at `http://localhost:8002/mcp/tools/*`. The MCP server continues to run independently on port 8001. The sub-app mount is additive only.
