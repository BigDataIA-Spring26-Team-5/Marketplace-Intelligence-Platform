# Implementation Plan: GCS Bronze Layer Connector

**Branch**: `aqeel` | **Date**: 2026-04-20 | **Spec**: [spec.md](spec.md)

## Summary

Wire GCS JSONL partitions as a first-class source input via `GCSSourceLoader`. Core loader, CLI integration, checkpoint support, and the orchestrator's `load_source` hook are already implemented. Three gaps remain from post-clarification spec: (1) fail-fast on zero-file match must be in the loader itself, (2) large-partition streaming must be line-by-line (no full-blob materialization), (3) GCS API calls need exponential-backoff retry (3×).

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: `google-cloud-storage ^3.10.1`, `pandas ^2.2`, `langgraph ^0.4`, `pytest`  
**Storage**: GCS bucket `mip-bronze-2024` (read-only); `checkpoints.db` SQLite (checkpoint keys)  
**Testing**: pytest + unittest.mock  
**Target Platform**: Linux server (same as existing pipeline)  
**Project Type**: CLI / LangGraph agent pipeline  
**Performance Goals**: Schema analysis ≤5K rows from first partition; full run streams 10K-row chunks without OOM  
**Constraints**: No full-blob materialization for large partitions; no service-account key files in repo  
**Scale/Scope**: 468K+ rows across partitioned JSONL; single-node execution

## Constitution Check

| Gate | Status | Notes |
|------|--------|-------|
| Unified-schema impact identified | ✅ Pass | GCS loader feeds `load_source` node — schema analysis path unchanged |
| Agent responsibilities unchanged | ✅ Pass | Only `load_source` entry point changes; nodes 2–7 untouched |
| Declarative YAML / no runtime codegen | ✅ Pass | Loader is pure I/O; no transformation logic |
| HITL approval points preserved | ✅ Pass | `check_registry` gate unaffected |
| Safety-field enrichment unchanged | ✅ Pass | No enrichment changes |
| DQ scoring / mapping persistence / docs covered | ✅ Pass | Tasks include test + README updates |

No violations. No Complexity Tracking entry required.

## Implementation Status

### Already Implemented

| Req | File | Status |
|-----|------|--------|
| R1 `GCSSourceLoader` (basic) | `src/pipeline/loaders/gcs_loader.py` | ✅ Done |
| R2 CLI `--source gs://` auto-detect | `src/pipeline/cli.py` | ✅ Done |
| R3 ADC auth + `GOOGLE_CLOUD_PROJECT` | `.env.example` | ✅ Done |
| R4 `load_sample()` schema path | `src/pipeline/loaders/gcs_loader.py` | ✅ Done |
| R4 `iter_chunks()` full-run path | `src/pipeline/loaders/gcs_loader.py` | ⚠️ Gap: full-blob download |
| R5 Checkpoint w/ GCS URI key | `src/pipeline/cli.py` | ✅ Done |
| Orchestrator `load_source` integration | `src/agents/orchestrator.py` | ✅ Done |
| Unit tests (happy path) | `tests/test_gcs_loader.py` | ✅ Done |

### Gaps to Close

| Gap | Where | Change Needed |
|-----|-------|--------------|
| Zero-file → raise immediately (R1/R6) | `gcs_loader._list_blobs()` | Raise `FileNotFoundError` instead of returning `[]` |
| Line-by-line streaming (R4/Q1) | `gcs_loader._blob_to_df()` + `iter_chunks()` | Use `blob.open()` + `pd.read_json(chunksize=...)` or manual line iteration |
| Retry w/ backoff (R6/Q4) | `gcs_loader._blob_to_df()` + `_list_blobs()` | Wrap GCS calls with `tenacity` or manual 3× retry loop |
| Nested JSON → string (R1/Q3) | `gcs_loader._blob_to_df()` | Post-process dict/list columns with `json.dumps` |
| Test: zero-file raises error | `tests/test_gcs_loader.py` | Update `test_load_sample_empty_bucket_returns_empty_df` → expect `FileNotFoundError` |
| Test: retry behavior | `tests/test_gcs_loader.py` | Add retry tests with side-effect mocks |
| Integration test | `tests/test_gcs_loader.py` | Real GCS read of `usda/2026/04/20/part_0000.jsonl` |

## Project Structure

### Documentation (this feature)

```text
specs/007-gcs-connector/
├── plan.md              ← this file
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── gcs-loader-interface.md
└── tasks.md             ← /speckit.tasks output
```

### Source Code

```text
src/pipeline/loaders/
├── __init__.py
└── gcs_loader.py        ← GCSSourceLoader (gaps to close)

src/pipeline/
├── cli.py               ← GCS checkpoint path (done)
└── runner.py            ← run_chunked() GCS branch (done)

src/agents/
└── orchestrator.py      ← load_source GCS branch (done)

tests/
└── test_gcs_loader.py   ← unit + integration tests
```

## Phase 0: Research

See [research.md](research.md).

## Phase 1: Design

See [data-model.md](data-model.md) and [contracts/gcs-loader-interface.md](contracts/gcs-loader-interface.md).
