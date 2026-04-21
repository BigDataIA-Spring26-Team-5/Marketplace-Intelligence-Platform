# Spec 008 — Silver Layer: Unified Output + Global Dedup

## Status: Ready for `/speckit.plan` (depends on Spec 007)

## Problem

Currently each pipeline run deduplicates internally, producing one clean output per source. When multiple sources are processed in parallel, duplicates across sources (e.g., "Cheerios" in USDA and OFF) survive. A second global dedup pass is needed but doesn't exist.

The Bronze → Silver → Gold medallion architecture solves this:
- **Bronze**: Raw data as-is (GCS, owned by teammate's Airflow DAGs)
- **Silver**: Each source transformed to unified 14-column schema (this spec)
- **Gold**: Global dedup + enrichment across all Silver tables (this spec)

## Architecture

```
Bronze (GCS/JSONL per source)
    ↓ per-source pipeline (existing agentic flow)
Silver (unified schema, one table per source, GCS or BigQuery)
    ↓ single execution
Gold (global dedup → enrichment → final catalog)
```

## Requirements

### Phase 1 — Silver Output Layer

#### R1 — Silver Write Target
- After `run_pipeline` (node 6), write unified output to Silver location
- Target: `gs://mip-silver-2024/{source}/{run_date}/output.parquet`
- Alternative: BigQuery table `silver_catalog.{source}_unified`
- Decision: Start with GCS Parquet (simpler, no BQ schema management)

#### R2 — Source Metadata Tagging
- Every Silver row gets three metadata columns appended:
  - `_source`: source identifier (e.g., `usda`, `off`, `openfda`)
  - `_bronze_file`: original GCS path of the bronze file this row came from
  - `_pipeline_run_id`: checkpoint run ID for lineage
- These columns are NOT part of the unified 14-column schema — they're Silver-layer-only

#### R3 — Remove Per-Source Dedup
- Current pipeline runs `FuzzyDeduplicateBlock` within each source
- In Silver mode: SKIP the dedup block entirely
- Dedup moves to Gold layer (R5)
- Controlled by a flag: `--skip-local-dedup` or auto-detected when output target is Silver

#### R4 — Per-Source Pipeline Parallelism
- Each source's Bronze → Silver pipeline is independent
- Can run in parallel via:
  - Multiple CLI invocations (simplest)
  - Airflow DAG with one task per source (production path)
  - No shared state between source pipelines except the unified schema

### Phase 2 — Gold Layer (Global Dedup + Enrichment)

#### R5 — Silver Reader
- New `GoldPipelineRunner` that reads ALL Silver tables
- Concatenates into one DataFrame with `_source` column preserved
- Input: `gs://mip-silver-2024/*/latest/output.parquet` (glob across sources)

#### R6 — Global Dedup (single pass)
- Runs the existing dedup sequence ONCE across the full concatenated Silver dataset:
  1. `FuzzyDeduplicateBlock` — blocking on lowercase `description[:3]`, Union-Find clustering
  2. `ColumnWiseMergeBlock` — merges duplicate groups, tracks source provenance via `_source` column
  3. `GoldenRecordSelectBlock` — scores on column coverage, freshness, ingredient richness
- Cross-source duplicates are now caught (USDA Cheerios + OFF Cheerios → one golden record)
- Golden record retains `_source` of the winning record, plus `_merged_sources` listing all contributing sources

#### R7 — Enrichment (unchanged)
- Runs AFTER global dedup on the deduplicated golden records
- Three tiers unchanged: S1 Deterministic → S2 KNN → S3 RAG-LLM
- Safety boundary unchanged: allergens, dietary_tags, is_organic are S1-only
- Input size is now much smaller (deduplicated set vs raw)

#### R8 — Gold Output
- Final catalog written to: `gs://mip-gold-2024/{run_date}/catalog.parquet`
- Also loadable to BigQuery: `gold_catalog.products`
- DQ scores computed on the final Gold output

#### R9 — Gold Pipeline CLI
- `python -m src.pipeline.gold --run-date 2026-04-20`
- Reads all available Silver outputs, runs global dedup + enrichment
- Separate from per-source Silver CLI — this is a distinct execution

## Data Flow Example

```
USDA Bronze (468K rows, JSONL)
    → Agentic pipeline (Orchestrator maps 8 cols + nested nutrients)
    → Silver: 468K rows, 14 unified columns + 3 metadata cols
    → NO dedup at this stage

OFF Bronze (4.48M rows, JSONL)  [when landed]
    → Agentic pipeline (Orchestrator maps ~200 cols to 14)
    → Silver: 4.48M rows, same 14 columns + 3 metadata cols
    → NO dedup at this stage

Gold Pipeline reads both Silver tables:
    → Concatenate: ~4.95M rows
    → Global FuzzyDedup: ~1.8M unique products (estimate)
    → Enrichment: fill missing categories, dietary tags
    → Gold catalog: ~1.8M enriched golden records
```

## What Changes In Existing Code

| Component | Change |
|---|---|
| `src/pipeline/cli.py` | Add `--output-layer silver` flag, add `--skip-local-dedup` |
| `save_output` (node 7) | Write to GCS Silver path when Silver mode active |
| `FuzzyDeduplicateBlock` | No code change — just skipped in Silver mode |
| `PipelineRunner` | Add Silver/Gold mode awareness |
| NEW: `src/pipeline/gold.py` | Gold pipeline runner (read Silver → dedup → enrich → write) |
| NEW: `src/pipeline/loaders/silver_reader.py` | Read + concat all Silver parquet files |

## Out of Scope
- Airflow DAG for orchestrating Silver + Gold (manual CLI for now)
- Semantic fact model (future optimization, not needed for Option 3)
- OFF and ESCI pipeline runs (blocked on data landing)
- Real-time / streaming Silver updates

## Dependencies
- Spec 007 (GCS connector) must be complete
- `pyarrow` for Parquet read/write
- Silver GCS bucket: `gs://mip-silver-2024/` (needs creation)
- Gold GCS bucket: `gs://mip-gold-2024/` (needs creation)

## Test Plan
- Unit: Verify Silver metadata columns appended correctly
- Unit: Verify dedup is skipped in Silver mode
- Integration: Run USDA through Silver, verify 14-col schema + metadata
- Integration: Duplicate two Silver outputs, run Gold, verify cross-source dedup works
- End-to-end: Bronze USDA → Silver → Gold with DQ scores