# Cloud Deployment — Marketplace Intelligence Platform

Deployed on Google Cloud Platform.

- **Project:** `mip-platform-2024`
- **VM:** `mip-vm` (`us-central1-a`, `e2-standard-8`)
- **Static IP:** `35.239.47.242` (reserved as `mip-static-ip`)
- **OS:** Ubuntu (Linux 6.8.0-1053-gcp)
- **Runtime:** Docker Compose stack, project name `mip`

All containers set `restart: unless-stopped`. Docker daemon is systemd-enabled, so the stack auto-recovers from VM reboot.

## Public endpoints

| Service | URL | Credentials | Health check |
|---|---|---|---|
| Streamlit app (UC2 Observability) | http://35.239.47.242:8502/ | — | `GET /` |
| Grafana dashboards | http://35.239.47.242:3000/ | `admin` / `admin` | `GET /login` |
| Prometheus | http://35.239.47.242:9090/ | — | `GET /-/ready` |
| Pushgateway | http://35.239.47.242:9091/ | — | `GET /-/healthy` |
| ChromaDB (UC3 vector store) | http://35.239.47.242:8000/api/v2/heartbeat | — | `GET /api/v2/heartbeat` |
| MCP observability API | http://35.239.47.242:8001/docs | — | `GET /docs` (Swagger UI) |
| Airflow webserver | http://35.239.47.242:8080/ | `admin` / `admin` | `GET /health` |

Internal-only (firewalled to VPC):
- Postgres 5432 (db `uc2`, user `mip`/`mip_pass`) — UC2 audit tables, UC4 build log
- Redis 6379 — pipeline cache
- Kafka 9092 — event bus

## Verify all endpoints

```bash
IP=35.239.47.242
for url in \
  http://$IP:8502/ \
  http://$IP:3000/login \
  http://$IP:9090/-/ready \
  http://$IP:9091/-/healthy \
  http://$IP:8000/api/v2/heartbeat \
  http://$IP:8001/docs \
  http://$IP:8080/health ; do
  printf "%-55s %s\n" "$url" "$(curl -sL -o /dev/null -w '%{http_code}' --max-time 10 $url)"
done
```

All should return `200`.

## What each link demonstrates

- **Streamlit (8502)** — UC2 observability UI: run history, DQ trends, natural-language chatbot over run logs
- **Grafana (3000)** — pipeline dashboards (ingest lag, row counts, DQ scores, anomaly flags)
- **Prometheus (9090)** — raw metrics and alerts
- **Pushgateway (9091)** — batch-job metric ingestion point
- **ChromaDB (8000)** — UC3 semantic search collection (99,666 products indexed; BM25 + embedding hybrid)
- **MCP API (8001)** — 7 MCP tool endpoints for programmatic observability queries
- **Airflow (8080)** — daily DAG chain: Bronze → Silver → Gold, plus hourly UC2 anomaly detection and 5-min ChromaDB chunker

## Data layer (GCP)

- Bronze: `gs://mip-bronze-2024/` (JSONL, partitioned by source + date)
- Silver: `gs://mip-silver-2024/` (Parquet, partitioned by domain + source)
- Gold: BigQuery `mip_gold.products`
