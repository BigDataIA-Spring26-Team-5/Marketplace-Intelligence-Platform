# Spec 007 — GCS Bronze Layer Connector

## Status: Ready for `/speckit.plan`

## Problem

The ETL pipeline currently reads from local CSV files via `--source data/file.csv`. The bronze data now lives in GCS (`gs://mip-bronze-2024/`) as JSONL partitioned by source and date. The pipeline needs to read from GCS without rewriting the agent graph.

## Scope

Wire GCS as a source input to the existing `load_source` node. Everything downstream (analyze_schema → critique → HITL → plan → run → save) stays untouched.

## Requirements

### R1 — GCS Source Loader
- New `GCSSourceLoader` class in `src/pipeline/loaders/gcs_loader.py`
- Accepts a GCS URI pattern: `gs://mip-bronze-2024/usda/2026/04/20/*.jsonl`
- Downloads matching JSONL files, concatenates into a single DataFrame
- Handles nested JSON fields: nested dicts/lists serialized to JSON strings in the DataFrame cell (explosion is a Silver concern)
- Raises a descriptive error immediately if the URI pattern matches zero files (fail-fast before agent graph entry)

### R2 — CLI Extension
- Extend `src/pipeline/cli.py` to accept `--source gs://bucket/path/*.jsonl`
- Auto-detect GCS vs local path based on `gs://` prefix
- Example: `python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/*.jsonl --domain nutrition`

### R3 — Authentication
- Use Application Default Credentials (ADC) — same as the explorer script
- Add `GOOGLE_CLOUD_PROJECT` to `.env.example`
- No service account key files in the repo

### R4 — Sampling Compatibility
- Current `load_source` does adaptive sampling (~5K rows) for schema analysis
- GCS loader must support the same sampling: download only enough partitions to get ~5K rows, not the full 468K
- For schema analysis: read first partition only
- For full pipeline run: stream all partitions in 10K-row chunks (compatible with existing `PipelineRunner`)
- Large partition files (>10K rows): stream line-by-line, yielding 10K-row chunks — do not materialize full file in memory

### R5 — Checkpoint Integration
- Checkpoint key should include the GCS URI so resume works across runs
- `checkpoints.db` stores `gs://...` as the source identifier

### R6 — Error Handling & Retries
- GCS API failures (network errors, permission errors, blob-not-found mid-stream): retry up to 3×, exponential backoff, then raise
- Zero-file match: fail immediately with descriptive error (see R1)

## Out of Scope
- Writing output to GCS (stays local for now — Spec 008)
- BigQuery reads (bronze BQ table is incomplete, missing nutrients)
- Kafka consumer integration
- Any changes to the agent graph nodes 2-7

## Clarifications

### Session 2026-04-20
- Q: How should the GCS loader handle a JSONL partition file that exceeds 10K rows? → A: Stream line-by-line, yield 10K-row chunks (no full-file materialization)
- Q: What should the pipeline do when a GCS URI pattern matches zero files? → A: Raise descriptive error immediately (fail-fast before agent graph entry)
- Q: How should nested JSON fields (dicts/lists) be stored in the DataFrame? → A: Serialize to JSON string
- Q: Should the loader retry on GCS API failures? → A: Retry up to 3×, exponential backoff, then raise

## Dependencies
- `google-cloud-storage` package
- GCP project: `mip-platform-2024`
- Bucket: `mip-bronze-2024`

## Test Plan
- Unit: Mock GCS client, verify JSONL concat and sampling
- Integration: Read `usda/2026/04/20/part_0000.jsonl` from real GCS, verify DataFrame schema matches what the Orchestrator expects
- CLI: `python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl --domain nutrition` completes schema analysis