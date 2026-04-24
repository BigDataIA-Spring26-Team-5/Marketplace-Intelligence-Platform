# Quickstart: UC REST API Layer

## Start the API server

```bash
# New API server (port 8002)
uvicorn src.api.main:app --host 0.0.0.0 --port 8002 --reload

# Existing MCP server still runs on 8001 (unchanged)
uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001
```

## Submit a pipeline run

```bash
curl -X POST http://localhost:8002/v1/pipeline/runs \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "data/usda_fooddata_sample.csv",
    "domain": "nutrition",
    "pipeline_mode": "full"
  }'
# → {"run_id": "a3f2c1d0-...", "status": "pending", ...}
```

## Poll run status

```bash
RUN_ID="a3f2c1d0-..."
curl http://localhost:8002/v1/pipeline/runs/$RUN_ID/status
# → {"status": "running", "stage": "run_pipeline", "chunk_index": 3, ...}
```

## Get run result

```bash
curl http://localhost:8002/v1/pipeline/runs/$RUN_ID/result
# → {"status": "completed", "rows_out": 49823, "dq_score_post": 0.89, ...}
```

## Query run history

```bash
curl "http://localhost:8002/v1/observability/runs?domain=nutrition&page_size=5"
```

## Search the product catalog

```bash
# Requires: poetry run python scripts/build_corpus.py (run once)
curl -X POST http://localhost:8002/v1/search/query \
  -H "Content-Type: application/json" \
  -d '{"query": "organic almond milk", "top_k": 5}'
```

## Get recommendations

```bash
# Requires: ProductRecommender.build() to have run first
curl http://localhost:8002/v1/recommendations/usda-12345/also-bought?top_k=5
```

## Health check

```bash
curl http://localhost:8002/health
```

## Flush LLM cache

```bash
curl -X POST http://localhost:8002/v1/ops/cache/flush \
  -H "Content-Type: application/json" \
  -d '{"prefix": "llm", "confirm": true}'
```

## Interactive docs

FastAPI auto-generates Swagger UI and ReDoc:
- Swagger: http://localhost:8002/docs
- ReDoc:   http://localhost:8002/redoc

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `API_PORT` | `8002` | API server port |
| `API_KEY_ENABLED` | `false` | Enable `X-API-Key` header check |
| `API_KEY` | — | Required if `API_KEY_ENABLED=true` |
