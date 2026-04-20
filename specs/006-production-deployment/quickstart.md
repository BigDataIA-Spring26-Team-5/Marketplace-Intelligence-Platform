# Quickstart: Production Deployment

**Feature**: 006-production-deployment  
**Date**: 2026-04-18  
**Prerequisite**: Docker + Docker Compose installed; `.env` file created (see `.env.example`)

---

## 0. Prerequisites

```bash
# Verify Docker
docker --version       # >= 24.x
docker compose version # >= 2.x

# Clone and enter repo
cd /path/to/etl-pipeline

# Create .env from example
cp .env.example .env
# Edit .env: set DEEPSEEK_API_KEY at minimum
```

---

## 1. Build and Start All Services

```bash
docker compose up --build -d
```

Services started:
| Service | Port | Purpose |
|---------|------|---------|
| `api` | 8000 | FastAPI app |
| `worker` | — | Celery worker |
| `postgres` | 5432 | RunState, AuditLog, BlockRegistry |
| `redis` | 6379 | Celery broker + result backend |
| `minio` | 9000/9001 | S3-compatible artifact store (9001 = console) |

Wait for all services healthy:
```bash
docker compose ps          # all should show "healthy" or "running"
docker compose logs api    # watch for "Application startup complete"
```

---

## 2. Run Alembic Migrations

```bash
docker compose exec api alembic upgrade head
```

Expected output:
```
INFO  [alembic.runtime.migration] Running upgrade  -> 001, create run_state
INFO  [alembic.runtime.migration] Running upgrade 001 -> 002, create audit_log
INFO  [alembic.runtime.migration] Running upgrade 002 -> 003, create block_registry
INFO  [alembic.runtime.migration] Running upgrade 003 -> 004, create hitl_decision
```

---

## 3. Create MinIO Bucket

```bash
# Open MinIO console at http://localhost:9001
# Login: minioadmin / minioadmin (default)
# Create bucket: etl-pipeline-dev

# OR via mc CLI:
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc mb local/etl-pipeline-dev
```

---

## 4. Verify API is Running

```bash
curl http://localhost:8000/health
# {"status": "ok", "version": "1.0.0"}

curl http://localhost:8000/docs
# Opens Swagger UI in browser
```

---

## 5. Submit a Pipeline Run (HITL mode off for quick test)

```bash
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "data/usda_fooddata_sample.csv",
    "domain": "nutrition",
    "chunk_size": 5000,
    "hitl_mode": false
  }'
```

Response:
```json
{
  "run_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "queued"
}
```

---

## 6. Poll Status Until Complete

```bash
RUN_ID="f47ac10b-58cc-4372-a567-0e02b2c3d479"

watch -n 5 "curl -s http://localhost:8000/pipeline/$RUN_ID/status | python3 -m json.tool"
```

Status progression (hitl_mode=false):
```
queued → running → completed
```

Completed response:
```json
{
  "run_id": "f47ac10b-...",
  "status": "completed",
  "dq_score_pre": 0.68,
  "dq_score_post": 0.84,
  "row_count_in": 50000,
  "row_count_out": 49200,
  "row_count_quarantined": 800,
  "completed_at": "2026-04-18T10:12:00Z"
}
```

---

## 7. Download Output

```bash
curl -s http://localhost:8000/pipeline/$RUN_ID/output | python3 -m json.tool
# Returns: {"download_url": "http://localhost:9000/etl-pipeline-dev/runs/.../output.csv?...", ...}

# Download the CSV
curl -o output.csv "$(curl -s http://localhost:8000/pipeline/$RUN_ID/output | python3 -c "import sys,json; print(json.load(sys.stdin)['download_url'])")"
```

---

## 8. Inspect Audit Log

```bash
curl -s http://localhost:8000/pipeline/$RUN_ID/audit | python3 -m json.tool
```

Sample output:
```json
[
  {"block_name": "dq_score_pre",  "rows_in": 5000, "rows_out": 5000, "duration_ms": 95},
  {"block_name": "DYNAMIC_MAPPING_nutrition", "rows_in": 5000, "rows_out": 5000, "duration_ms": 280},
  {"block_name": "strip_whitespace", "rows_in": 5000, "rows_out": 5000, "duration_ms": 12},
  {"block_name": "fuzzy_deduplicate", "rows_in": 5000, "rows_out": 4912, "duration_ms": 3200},
  {"block_name": "llm_enrich", "rows_in": 4912, "rows_out": 4912, "duration_ms": 18400},
  {"block_name": "dq_score_post", "rows_in": 4912, "rows_out": 4912, "duration_ms": 88}
]
```

---

## 9. HITL Mode Walk-Through

For the full HITL flow:

```bash
# Submit with hitl_mode=true (default)
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"source_path": "data/fda_recalls_sample.csv", "domain": "recalls"}'

RUN_ID="..."  # from response

# Poll until awaiting_approval
curl http://localhost:8000/pipeline/$RUN_ID/status
# {"status": "awaiting_approval", "gate": "schema_mapping", ...}

# Approve schema mapping
curl -X PATCH http://localhost:8000/pipeline/$RUN_ID/approve \
  -H "Content-Type: application/json" \
  -d '{"gate": "schema_mapping", "decision": "approved"}'

# Poll again — run continues
# If quarantined rows exist, gate becomes "quarantine"
# Approve quarantine or reject to stop
```

---

## 10. Tear Down

```bash
docker compose down        # stop containers, preserve volumes
docker compose down -v     # stop + delete volumes (wipes postgres + minio data)
```

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| `api` container exits immediately | `docker compose logs api` — likely missing `DATABASE_URL` or `DEEPSEEK_API_KEY` in `.env` |
| Run stuck at `queued` | `docker compose logs worker` — Celery worker may not have started or Redis unreachable |
| `alembic upgrade head` fails | Postgres not ready; retry after `docker compose ps` shows postgres healthy |
| MinIO bucket not found | Run step 3 (bucket creation) before submitting first run |
| LLM enrichment very slow | DeepSeek rate limit — normal for large datasets; reduce `LLM_ENRICH_BATCH_SIZE` in `.env` |

---

## Environment Variables Reference

See `.env.example` for the full list. Minimum required to start:

```bash
DEEPSEEK_API_KEY=sk-...
DATABASE_URL=postgresql://etl:etl@postgres:5432/etl
REDIS_URL=redis://redis:6379/0
S3_ENDPOINT_URL=http://minio:9000
S3_BUCKET=etl-pipeline-dev
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```
