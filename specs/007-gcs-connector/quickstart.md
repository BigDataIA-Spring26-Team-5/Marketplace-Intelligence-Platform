# Quickstart: GCS Bronze Layer Connector

## Prerequisites

```bash
# Auth (ADC)
gcloud auth application-default login

# Env
cp .env.example .env
# Set GOOGLE_CLOUD_PROJECT=mip-platform-2024 in .env
```

## Run pipeline from GCS

```bash
# Schema analysis + full pipeline (single partition)
python -m src.pipeline.cli \
  --source gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl \
  --domain nutrition

# Full dataset (glob)
python -m src.pipeline.cli \
  --source "gs://mip-bronze-2024/usda/2026/04/20/*.jsonl" \
  --domain nutrition

# Resume from checkpoint
python -m src.pipeline.cli \
  --source "gs://mip-bronze-2024/usda/2026/04/20/*.jsonl" \
  --domain nutrition \
  --resume
```

## Use loader directly

```python
from src.pipeline.loaders.gcs_loader import GCSSourceLoader

loader = GCSSourceLoader("gs://mip-bronze-2024/usda/2026/04/20/*.jsonl")

# Schema analysis (first partition, up to 5K rows)
sample_df = loader.load_sample(n_rows=5000)

# Full run (10K-row chunks)
for chunk in loader.iter_chunks(chunk_size=10000):
    print(chunk.shape)
```

## Run tests

```bash
cd src && pytest ../tests/test_gcs_loader.py -v

# Integration test (requires real GCS access + GOOGLE_CLOUD_PROJECT set)
pytest tests/test_gcs_loader.py -v -m integration
```
