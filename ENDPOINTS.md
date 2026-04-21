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
