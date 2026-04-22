# Quickstart: Gold Layer Pipeline

**Created**: 2026-04-21

---

## Prerequisites

1. Silver pipeline complete — Parquet files at `gs://mip-silver-2024/{source}/{date}/`
2. Python 3.11 + Poetry
3. GCS credentials configured

## Install

```bash
cd /home/aq/work/NEU/SPRING_26/Big\ Data/ETL
poetry install

# Check optional dependencies
poetry run python -c "import faiss; print('FAISS ok')"
poetry run python -c "from sentence_transformers import SentenceTransformer; print('ST ok')"
```

## Run

### Basic run

```bash
poetry run python -m src.pipeline.gold \
  --run-date 2026-04-21 \
  --silver-bucket gs://mip-silver-2024 \
  --gold-bucket gs://mip-gold-2024
```

### Dry run (validate only)

```bash
poetry run python -m src.pipeline.gold \
  --run-date 2026-04-21 \
  --dry-run
```

### With options

```bash
# Custom threshold
GOLD_DEDUP_THRESHOLD=90 poetry run python -m src.pipeline.gold --run-date 2026-04-21

# Limit LLM calls
GOLD_MAX_LLM_CALLS=100 poetry run python -m src.pipeline.gold --run-date 2026-04-21

# Skip enrichment
poetry run python -m src.pipeline.gold --run-date 2026-04-21 --skip-enrichment

# Load to BigQuery
poetry run python -m src.pipeline.gold --run-date 2026-04-21 --load-bq
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOLD_DEDUP_THRESHOLD` | 85 | Fuzzy match threshold |
| `GOLD_MAX_LLM_CALLS` | 500 | Max S3 LLM calls |
| `GOLD_KNN_THRESHOLD` | 0.85 | Min S2 similarity |
| `GOLD_CACHE_BACKEND` | sqlite | Cache type (sqlite/redis) |
| `REDIS_URL` | redis://localhost:6379 | Redis URL if using redis backend |

## Output

- Gold Parquet: `gs://mip-gold-2024/{run_date}/catalog.parquet`
- Run log: `gs://mip-gold-2024/run-logs/run_{timestamp}_{uuid}.json`

## Verify

```bash
# Check row count
poetry run python -c "
import pyarrow.parquet as pq
t = pq.read_table('gs://mip-gold-2024/2026/04/21/catalog.parquet')
print(f'Rows: {t.num_rows}')
"

# Check run log
gsutil cat gs://mip-gold-2024/run-logs/run_*.json | jq '.dq_scores'
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Schema validation fails | Fix Silver pipeline to match contract |
| OOM at dedup | Set `GOLD_LAZY_BLOCKING=1` for lazy mode |
| S3 rate limited | Reduce `GOLD_S3_BATCH_SIZE` or `GOLD_MAX_LLM_CALLS` |
| Missing FAISS | `poetry add faiss-cpu` |
