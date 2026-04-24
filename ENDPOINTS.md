# MIP Platform — Service Endpoints

**VM IP:** `35.239.47.242`

## UC2 Observability

| Service | URL | Credentials |
|---------|-----|-------------|
| UC2 Observability (Streamlit) | http://35.239.47.242:8502 | — |
| Grafana | http://35.239.47.242:3000 | admin / mip_admin |
| MCP Server (FastAPI, 8 tools) | http://35.239.47.242:8001/tools | — |
| MCP Server — Swagger Docs | http://35.239.47.242:8001/docs | — |
| Prometheus | http://35.239.47.242:9090 | — |
| Pushgateway | http://35.239.47.242:9091 | — |
| ChromaDB | http://35.239.47.242:8000 | — |
| Redis (MCP cache) | 35.239.47.242:6379 | no auth |
| MLflow | http://35.239.47.242:5000 | — |

## REST API (port 8002) — UC1/UC2/UC3/UC4

All endpoints versioned under `/v1/`. MCP tools also accessible at `/mcp/tools/*`.

| Router | Base path | Description |
|--------|-----------|-------------|
| Pipeline | `/v1/pipeline/runs` | Submit/poll/resume pipeline runs |
| Observability | `/v1/observability/` | Run history, traces, anomalies, cost, dedup |
| Search | `/v1/search/query` | Hybrid product search (UC3) |
| Recommendations | `/v1/recommendations/` | Also-bought / you-might-like (UC4) |
| Ops | `/v1/ops/` | Cache stats/flush, domain schema |

| Service | URL | Credentials |
|---------|-----|-------------|
| REST API | http://35.239.47.242:8002 | — |
| REST API Swagger | http://35.239.47.242:8002/docs | — |
| REST API Health | http://35.239.47.242:8002/health | — |
| MCP via REST API | http://35.239.47.242:8002/mcp/tools | — |

Start: `uvicorn src.api.main:app --host 0.0.0.0 --port 8002`

Env vars: `MAX_CONCURRENT_RUNS` (default 2), `PIPELINE_RATE_LIMIT` (default 10/minute), `API_KEY_ENABLED` (default false), `API_KEY`

## UC1 Pipeline

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://35.239.47.242:8080 | admin / admin |

## GCS Buckets (Bronze Layer)

| Bucket | Contents |
|--------|----------|
| gs://mip-bronze-2024/usda/bulk/2026/04/21/ | USDA 467k records |
| gs://mip-bronze-2024/off/2026/04/21/ | OFF 1M records (complete, 100 files) |
| gs://mip-bronze-2024/openfda/2026/04/20/ | openFDA 25,100 records |
| gs://mip-bronze-2024/esci/2024/01/01/ | ESCI 2M records |

## BigQuery Tables

**Project:** `mip-platform-2024` · **Dataset:** `bronze_raw`

| Table | Records |
|-------|---------|
| `bronze_raw.usda_branded` | 432,706 |
| `bronze_raw.usda_foundation` | 365 |
| `bronze_raw.usda_sr_legacy` | 7,793 |
| `bronze_raw.usda_survey` | 5,432 |
| `bronze_raw.esci` | 2,027,874 |
| `bronze_raw.openfda` | 25,100 |
| `bronze_raw.off` | 1,000,000 |

## Background Processes (running on VM)

| Process | Log |
|---------|-----|
| OFF producer (→ GCS) | DONE — `docker exec mip_airflow_1 cat /tmp/off_out.txt \| tail -5` |
| kafka_to_pg consumer | `tail -f /tmp/kafka_to_pg.log` |
| MCP Server | `tail -f /tmp/mcp_server.log` |
| UC2 Streamlit | `tail -f /tmp/streamlit.log` |
| Chunker (audit → ChromaDB) | `tail -f /tmp/chunker.log` |
| Anomaly Detector | `tail -f /tmp/anomaly_detector.log` |
| USDA Airflow DAG | http://35.239.47.242:8080 |
