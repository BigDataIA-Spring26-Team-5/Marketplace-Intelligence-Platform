# Quickstart: Gold Layer Revamp

**Created**: 2026-04-22

## Prerequisites

- Python 3.11 + Poetry installed
- GCS access (`gcloud auth application-default login`)
- ChromaDB running: `docker run -p 8000:8000 chromadb/chroma`
- Redis running: `redis-server` or `docker run -p 6379:6379 redis`
- `.env` with `GCP_PROJECT`, `SILVER_BUCKET`, `BQ_GOLD_DATASET` set

## Validate the fix (1000-row sample, <10 min)

```bash
cd "/home/aq/work/NEU/SPRING_26/Big Data/ETL"

# Sample run — uses gs://mip-silver-2024/off/2026/04/21/sample.parquet
poetry run python -m src.pipeline.gold_pipeline \
  --source off \
  --date 2026/04/21 \
  --domain nutrition

# Expected log lines:
# [gold_pipeline] INFO: Cast N StringDtype columns to object: [...]   ← Change 1
# [embedding]     INFO: S2 KNN: corpus too sparse ... Augmenting ...  ← Change 2
# [corpus]        INFO: S2 KNN: queried chunk 1/2 (500/1000 rows)     ← Change 3
# [embedding]     INFO: S2 KNN: resolved N rows                        ← S2 working
# No TypeError anywhere in output
```

## Full run (783k rows, target <3h)

```bash
poetry run python -m src.pipeline.gold_pipeline \
  --source off \
  --date 2026/04/21 \
  --domain nutrition

# Monitor dedup (should complete in <1h):
# [fuzzy_deduplicate] WARNING: Block size N >= OOM threshold 2000  ← should be <20 occurrences
# [fuzzy_deduplicate] INFO: Dedup: 783225 rows → N clusters (N% duplicate rate)

# Monitor S2 (should resolve >100k rows):
# [corpus] INFO: S2 KNN: queried chunk 10/820 (5000/409276 rows)
# [embedding] INFO: S2 KNN: resolved N rows

# Verify BQ output:
bq query --nouse_legacy_sql \
  "SELECT COUNT(*) FROM mip_gold.products WHERE _PARTITIONDATE = '2026-04-22'"
```

## Run other sources

```bash
# branded
poetry run python -m src.pipeline.gold_pipeline --source branded --date 2026/04/21 --domain nutrition

# foundation
poetry run python -m src.pipeline.gold_pipeline --source foundation --date 2026/04/21 --domain nutrition

# openfda (safety domain)
poetry run python -m src.pipeline.gold_pipeline --source openfda --date 2026/04/20 --domain safety
```

## Tune performance

```bash
# Increase ChromaDB query chunk size (if network is fast)
CHROMA_QUERY_CHUNK_SIZE=1000 poetry run python -m src.pipeline.gold_pipeline ...

# Lower corpus augmentation ratio (skip augmentation more often on repeat runs)
CORPUS_AUGMENT_RATIO=0.10 poetry run python -m src.pipeline.gold_pipeline ...

# Raise dedup OOM threshold if RAM allows
DEDUP_BLOCK_OOM_THRESHOLD=5000 poetry run python -m src.pipeline.gold_pipeline ...
```

## Skip enrichment (dedup-only, fastest validation)

```bash
poetry run python -m src.pipeline.gold_pipeline \
  --source off \
  --date 2026/04/21 \
  --domain nutrition \
  --skip-enrichment
```
