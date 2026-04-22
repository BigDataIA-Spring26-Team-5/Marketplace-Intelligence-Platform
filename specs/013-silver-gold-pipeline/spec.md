# Spec 012 — Gold Layer: Unification, Deduplication & 3-Tier Enrichment

**Status:** Draft — ready for `/speckit.clarify`  
**Depends on:** Silver pipeline producing clean, unified-schema outputs for all sources  
**Spec Kit branch:** `spec-012-gold-layer`

---

## 1. Problem

The Silver layer contains 4+ source-specific Parquet files (OFF, USDA branded, USDA foundation, openFDA), each independently transformed to the unified schema. These sources overlap — the same physical product (e.g., "Cheerios Original 8.9oz by General Mills") may appear in OFF, USDA branded, and openFDA with slightly different names, different completeness, and different freshness. There is no single deduplicated, enriched catalog today.

The Gold layer must produce exactly one golden record per real-world product, with the most complete and freshest data merged from all available sources, and enrichment applied to fill remaining gaps.

---

## 2. Hard Rules

1. **No schema change from Silver to Gold.** The Gold output has the same columns as Silver, same types. No columns added, no columns removed, no types changed.
2. **Dedup happens before enrichment.** Enriching duplicates wastes LLM calls and risks inconsistency. Deduplicate first, enrich the golden records only.
3. **No per-record LLM calls during dedup.** Dedup must be deterministic + fuzzy. LLM is only used in Tier 3 enrichment, post-dedup.
4. **Enrichment must not fabricate data.** If a field cannot be confidently filled, it stays null. Leaving a field blank is safer than hallucinating a value. Allergens, dietary tags, and safety-critical fields are S1-only (deterministic) — never LLM-enriched.
5. **Gold must be reproducible.** Same Silver inputs + same config = same Gold output. No non-deterministic steps in dedup. Enrichment results are cached so re-runs are stable.

---

## 3. Silver Schema Contract

Gold expects every Silver Parquet file to have exactly these columns and types. If a source deviates, it is a Silver bug and must be fixed upstream before Gold runs.

### Core Columns

| Column | Type | Description |
|---|---|---|
| `product_name` | `string` | Product display name |
| `brand_owner` | `string` | Manufacturing company / brand owner |
| `brand_name` | `string` | Consumer-facing brand (may be null if source lacks it) |
| `ingredients` | `string` | Ingredients list as text |
| `serving_size` | `float64` | Numeric serving size |
| `serving_size_unit` | `string` | Unit for serving size (g, ml, oz, etc.) |
| `published_date` | `timestamp[ns]` | Publication or last-modified date |
| `allergens` | `string` | Allergen declarations (may be null) |
| `sizes` | `string` | Package size info |
| `data_source` | `string` | Source identifier (e.g., "off", "usda_branded", "openfda") |

### Metadata Columns (carried through, not used in dedup/enrichment logic)

| Column | Type | Description |
|---|---|---|
| `_bronze_file` | `string` | Original Bronze file path |
| `_source` | `string` | Pipeline source identifier |
| `_pipeline_run_id` | `string` | Run ID that produced this Silver record |
| `dq_score_pre` | `float64` | Pre-enrichment data quality score |

### Validation Rule

On Gold pipeline startup, read the schema of every Silver Parquet file. If any file has extra columns, missing columns, or type mismatches against this contract, the pipeline halts with a clear error message listing every violation. Gold never silently drops or adds columns.

---

## 4. Architecture

```
┌─────────────────────────────────────────────────────┐
│                   GOLD PIPELINE                     │
│                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────┐ │
│  │  STAGE 1 │──▶│  STAGE 2│──▶│     STAGE 3     │ │
│  │  Unify   │   │  Dedup   │   │   Enrichment     │ │
│  │          │   │          │   │                  │ │
│  │ Read all │   │ Blocking │   │ Tier 1: Determ.  │ │
│  │ Silver   │   │ Fuzzy    │   │ Tier 2: KNN      │ │
│  │ Validate │   │ Cluster  │   │ Tier 3: RAG-LLM  │ │
│  │ Concat   │   │ Merge    │   │                  │ │
│  │          │   │ Select   │   │ DQ Score Final   │ │
│  └──────────┘   └──────────┘   └──────────────────┘ │
│                                          │          │
│                                          ▼          │
│                               ┌──────────────────┐  │
│                               │    STAGE 4       │  │
│                               │    Write Gold    │  │
│                               │    + Run Log     │  │
│                               └──────────────────┘  │
└─────────────────────────────────────────────────────┘
```

---

## 5. Stage 1 — Unification

### R1.1 — Silver Reader

Read all Parquet files from `gs://mip-silver-2024/{source}/{date}/part_*.parquet`. The reader accepts a run date and discovers all available sources automatically by listing top-level folders in the Silver bucket.

New file: `src/pipeline/gold/silver_reader.py`

Input: `gs://mip-silver-2024/` + run date (or "latest" per source)  
Output: Single concatenated DataFrame with all Silver records

### R1.2 — Schema Validation Gate

Before concatenation, validate every source's schema against the contract in Section 3. This is a hard gate — any mismatch aborts the pipeline. The validator reports all mismatches across all sources in a single error, not one-at-a-time.

Checks performed per source:
- Extra columns present that are not in the contract → ERROR (list them)
- Missing columns that should exist → ERROR (list them)
- Type mismatch (e.g., `published_date` is `string` instead of `timestamp[ns]`) → ERROR (show expected vs actual)
- Zero rows → WARNING (source is empty but schema is valid)

### R1.3 — Source Tagging

After concatenation, ensure every row has `data_source` populated. If Silver already set this correctly, no action needed. If `data_source` is null for any rows, derive it from `_source` metadata column as a fallback.

---

## 6. Stage 2 — Deduplication

Dedup uses the existing three-block pipeline (`FuzzyDeduplicateBlock` → `ColumnWiseMergeBlock` → `GoldenRecordSelectBlock`) operating on the unified cross-source DataFrame. No code changes to the block logic — only the input changes (all Silver sources concatenated vs a single source).

### R2.1 — Blocking

Group rows by the first 3 lowercase characters of `product_name`. This reduces pairwise comparisons from O(n²) to O(n × block_size). Cross-source duplicates land in the same block because blocking is on product name prefix, not on `data_source`.

Expected input: ~1.25M rows (OFF 783K + branded 447K + openFDA 25K + foundation 365)  
Expected blocks: ~17,000 blocks (26³ possible prefixes, but distribution is skewed)

### R2.2 — Fuzzy Scoring

Within each block, compute weighted similarity using `rapidfuzz.fuzz.token_sort_ratio`:

| Signal | Weight | Source |
|---|---|---|
| `product_name` | 0.5 | Token sort ratio |
| `brand_owner` | 0.2 | Token sort ratio (null = 0 score, not penalty) |
| Combined text (`product_name + brand_owner + brand_name`) | 0.3 | Token sort ratio |

Threshold: **85** (configurable via `GOLD_DEDUP_THRESHOLD` env var)

Pairs scoring ≥ threshold are considered duplicates.

### R2.3 — Transitive Clustering (Union-Find)

Use Union-Find with path compression and union by rank to form transitive closure clusters. If A matches B and B matches C, then {A, B, C} form one cluster even if A and C score below threshold directly.

Assign `duplicate_group_id` to every row. Mark the first row per group as `canonical` (temporary — overridden by golden record selection).

### R2.4 — Column-Wise Merge

Group by `duplicate_group_id` and merge:
- String columns: pick the **longest non-null value** (most complete)
- Numeric columns: pick the **first non-null value**
- `data_source`: concatenate all unique sources into comma-separated string (e.g., `"off,usda_branded"`)
- `_bronze_file`, `_source`, `_pipeline_run_id`: take from the winning golden record (not merged)

### R2.5 — Golden Record Selection

Score each row in a duplicate cluster using a weighted composite:

| Factor | Weight | Calculation |
|---|---|---|
| Completeness | 0.4 | Fraction of non-null core columns (10 columns) |
| Freshness | 0.35 | Normalized `published_date` (most recent = 1.0) |
| Ingredient richness | 0.25 | Normalized length of `ingredients` text |

The row with the highest composite score per `duplicate_group_id` is selected as the golden record. All other rows in the cluster are discarded.

### R2.6 — Dedup Metrics

After dedup completes, log:
- Total input rows
- Total duplicate clusters found
- Total rows after dedup (golden records)
- Dedup ratio (1 - output/input)
- Top 10 largest clusters (for sanity check — clusters > 50 rows likely indicate a blocking or threshold problem)
- Per-source breakdown: how many rows from each `data_source` survived as golden records

---

## 7. Stage 3 — Three-Tier Enrichment

Enrichment runs on the deduplicated golden records only. The goal is to fill null values in core columns where possible, using increasingly expensive methods.

### Tier 1 — Deterministic (S1)

Zero-cost, rule-based fills. Runs on every golden record.

| Rule | Target Column | Logic |
|---|---|---|
| Brand from brand_owner | `brand_name` | If `brand_name` is null and `brand_owner` is not null, copy `brand_owner` as `brand_name` |
| Serving size unit default | `serving_size_unit` | If `serving_size` is not null and `serving_size_unit` is null, default to `"g"` |
| Allergen extraction | `allergens` | If `allergens` is null and `ingredients` contains known allergen keywords (wheat, milk, soy, egg, peanut, tree nut, fish, shellfish, sesame), extract and populate |
| Data source normalization | `data_source` | Normalize to lowercase enum: `off`, `usda_branded`, `usda_foundation`, `usda_sr_legacy`, `usda_survey`, `openfda` |

Safety boundary: `allergens` is S1-only. If keyword extraction finds nothing, allergens stays null. Never LLM-infer allergens.

New file: `src/pipeline/gold/enrichment/tier1_deterministic.py`

### Tier 2 — KNN Embedding Similarity (S2)

For golden records where core columns are still null after Tier 1, attempt to fill from similar records that have the data.

Method: Batch FAISS similarity search. Embed `product_name + brand_owner` using `all-MiniLM-L6-v2`. For each record with null fields, find the top-5 nearest neighbors that have those fields populated. If the nearest neighbor similarity score ≥ 0.85 and the neighbor's value is non-null, propagate the value.

Columns eligible for S2 fill: `brand_name`, `serving_size`, `serving_size_unit`, `sizes`  
Columns NOT eligible for S2 (safety boundary): `allergens`, `ingredients`

Batch operation: embed all golden records in one pass, build FAISS index once, query in batch. No per-record loops.

New file: `src/pipeline/gold/enrichment/tier2_knn.py`

### Tier 3 — RAG-LLM (S3)

For remaining nulls after Tier 2, use LLM with context from the product's own fields plus retrieved similar products.

Method: Build a prompt with the product's known fields + top-3 KNN neighbors as context. Ask the LLM to infer the missing field values. Validate LLM output against Pydantic schema before accepting.

Columns eligible for S3 fill: `brand_name`, `category` (if added to schema later), `serving_size`, `serving_size_unit`  
Columns NEVER eligible for S3: `allergens`, `ingredients`, `published_date` (these are factual and must come from source data)

Batching: Group records by missing-field pattern (e.g., all records missing only `brand_name` go in one batch). Send 10-20 records per LLM call. Max LLM calls per Gold run: configurable via `GOLD_MAX_LLM_CALLS` env var (default: 500).

Caching: Cache LLM responses in Redis keyed by `hash(product_name + brand_owner + missing_fields)`. Re-runs with the same inputs skip LLM calls entirely.

New file: `src/pipeline/gold/enrichment/tier3_rag_llm.py`

### R3.1 — Enrichment Sequencing

Tiers run strictly in order: S1 → S2 → S3. Each tier only processes records that still have nulls after the previous tier. This minimizes cost — most records are resolved by S1 and S2, leaving a small tail for S3.

### R3.2 — Enrichment Provenance

For every field filled by enrichment, track the method used. Add a `_enrichment_log` column (JSON string) that records which fields were filled and by which tier:

```json
{
  "brand_name": {"tier": "S1", "method": "copy_from_brand_owner"},
  "serving_size_unit": {"tier": "S2", "method": "knn_neighbor", "neighbor_similarity": 0.92}
}
```

This column is metadata — it does not violate the "no schema change" rule because it is a metadata column in the same category as `_bronze_file`, `_source`, `_pipeline_run_id`.

### R3.3 — DQ Score Post-Enrichment

After all 3 tiers complete, compute `dq_score_post` for every golden record using the same scoring formula as `dq_score_pre`. This enables delta measurement: `dq_delta = dq_score_post - dq_score_pre`.

The `dq_score_post` column replaces the existing `dq_score_pre` value in the Gold output. Both values are logged in the run log for comparison, but the Gold Parquet file carries only the final post-enrichment score renamed to `dq_score`.

Wait — this would be a schema change. Correction: keep `dq_score_pre` as-is (from Silver) and add `dq_score_post` as a new metadata column alongside `_enrichment_log`. Both are metadata columns, consistent with the pattern.

---

## 8. Stage 4 — Gold Output

### R4.1 — Write Gold Parquet

Write the final deduplicated, enriched catalog to:

```
gs://mip-gold-2024/{run_date}/catalog.parquet
```

Single file, not partitioned. At ~1M golden records the file will be ~200-500MB which is manageable as a single Parquet file.

If the file would exceed 1GB, split into `catalog_0000.parquet`, `catalog_0001.parquet` etc. (future concern — unlikely at current scale).

### R4.2 — Run Log

Write a JSON run log to `gs://mip-gold-2024/run-logs/run_{timestamp}_{uuid}.json` with:

```json
{
  "run_id": "uuid",
  "run_date": "2026-04-21",
  "timestamp": "2026-04-21T20:00:00Z",
  "silver_sources": {
    "off": {"rows": 783225, "path": "gs://mip-silver-2024/off/2026/04/21/part_0000.parquet"},
    "branded": {"rows": 447444, "path": "gs://mip-silver-2024/branded/2026/04/21/part_0000.parquet"},
    "foundation": {"rows": 365, "path": "gs://mip-silver-2024/foundation/2026/04/21/part_0000.parquet"},
    "openfda": {"rows": 25100, "path": "gs://mip-silver-2024/openfda/2026/04/20/part_0000.parquet"}
  },
  "total_input_rows": 1256134,
  "dedup": {
    "clusters_found": 82400,
    "golden_records": 1043200,
    "dedup_ratio": 0.17,
    "threshold": 85
  },
  "enrichment": {
    "s1_fills": {"brand_name": 320000, "allergens": 45000, "serving_size_unit": 12000},
    "s2_fills": {"brand_name": 8500, "serving_size": 3200},
    "s3_fills": {"brand_name": 1200},
    "s3_llm_calls": 120,
    "s3_cache_hits": 340
  },
  "dq_scores": {
    "mean_pre": 0.72,
    "mean_post": 0.89,
    "mean_delta": 0.17
  },
  "output_path": "gs://mip-gold-2024/2026/04/21/catalog.parquet",
  "output_rows": 1043200,
  "duration_seconds": 1840
}
```

### R4.3 — BigQuery Load (Optional)

If `--load-bq` flag is passed, also load the Gold Parquet into BigQuery:

```
project: mip-platform-2024
dataset: gold_catalog
table: products
```

Load method: `WRITE_TRUNCATE` (full replace per run). Partitioning by `data_source` for query efficiency.

This is optional and not required for the initial Gold pipeline to be functional.

---

## 9. CLI Interface

```bash
# Full Gold run — read all Silver, dedup, enrich, write Gold
python -m src.pipeline.gold \
  --run-date 2026-04-21 \
  --silver-bucket gs://mip-silver-2024 \
  --gold-bucket gs://mip-gold-2024

# With custom dedup threshold
GOLD_DEDUP_THRESHOLD=90 python -m src.pipeline.gold --run-date 2026-04-21

# With BigQuery load
python -m src.pipeline.gold --run-date 2026-04-21 --load-bq

# Dry run — validate Silver schemas, report dedup estimates, do not write
python -m src.pipeline.gold --run-date 2026-04-21 --dry-run

# Skip enrichment (dedup only)
python -m src.pipeline.gold --run-date 2026-04-21 --skip-enrichment

# Limit LLM calls
GOLD_MAX_LLM_CALLS=100 python -m src.pipeline.gold --run-date 2026-04-21
```

New file: `src/pipeline/gold/__main__.py` (entry point)  
New file: `src/pipeline/gold/cli.py` (argument parsing)

---

## 10. New Files

| File | Purpose |
|---|---|
| `src/pipeline/gold/__init__.py` | Package init |
| `src/pipeline/gold/__main__.py` | Entry point for `python -m src.pipeline.gold` |
| `src/pipeline/gold/cli.py` | CLI argument parsing and orchestration |
| `src/pipeline/gold/silver_reader.py` | Read + validate + concat Silver Parquet files from GCS |
| `src/pipeline/gold/schema_contract.py` | Silver schema contract definition + validator |
| `src/pipeline/gold/dedup.py` | Orchestrates the 3 dedup blocks on cross-source data |
| `src/pipeline/gold/enrichment/__init__.py` | Enrichment package init |
| `src/pipeline/gold/enrichment/tier1_deterministic.py` | S1 rule-based enrichment |
| `src/pipeline/gold/enrichment/tier2_knn.py` | S2 FAISS batch similarity enrichment |
| `src/pipeline/gold/enrichment/tier3_rag_llm.py` | S3 LLM enrichment with batching + caching |
| `src/pipeline/gold/enrichment/provenance.py` | Enrichment log tracking |
| `src/pipeline/gold/writer.py` | Write Gold Parquet to GCS + optional BQ load |
| `src/pipeline/gold/run_log.py` | Generate and write JSON run log |

---

## 11. Existing Files Modified

| File | Change |
|---|---|
| `src/blocks/fuzzy_deduplicate.py` | No code change. Reused as-is by Gold dedup orchestrator. |
| `src/blocks/column_wise_merge.py` | No code change. Reused as-is. |
| `src/blocks/golden_record_select.py` | No code change. Reused as-is. |
| `src/enrichment/embedding.py` | Refactor to support batch mode (embed all records in one call, build FAISS index once). Currently does per-record loops. |
| `src/enrichment/llm_tier.py` | Refactor to support grouped batching (10-20 records per LLM call). Add Redis cache integration. |
| `src/models/llm.py` | Add 3-attempt exponential backoff on 429 rate limit errors. |
| `pyproject.toml` | Add `redis`, `faiss-cpu` dependencies if not already present. |

---

## 12. Configuration

All configurable via environment variables (no hardcoded values):

| Variable | Default | Description |
|---|---|---|
| `GOLD_DEDUP_THRESHOLD` | `85` | Fuzzy match threshold (0-100) |
| `GOLD_MAX_LLM_CALLS` | `500` | Max Tier 3 LLM calls per run |
| `GOLD_KNN_THRESHOLD` | `0.85` | Min cosine similarity for S2 fill |
| `GOLD_KNN_TOP_K` | `5` | Number of neighbors to retrieve |
| `GOLD_S3_BATCH_SIZE` | `15` | Records per LLM call in S3 |
| `GOLD_SILVER_BUCKET` | `gs://mip-silver-2024` | Silver input bucket |
| `GOLD_OUTPUT_BUCKET` | `gs://mip-gold-2024` | Gold output bucket |
| `REDIS_URL` | `redis://localhost:6379` | Redis for S3 LLM cache |
| `SCHEMA_SAMPLE_ROWS` | `500` | Sample size for large sources |

---

## 13. Dependencies

| Dependency | Purpose | Already installed? |
|---|---|---|
| `rapidfuzz` | Fuzzy string matching in dedup | Yes (used by FuzzyDeduplicateBlock) |
| `faiss-cpu` | Batch vector similarity for S2 | Check — may need install |
| `sentence-transformers` | Embedding model for S2 | Check — may need install |
| `redis` | LLM response cache for S3 | No — install or mock (see Spec 009) |
| `google-cloud-storage` | GCS read/write | Yes (used by Silver pipeline) |
| `google-cloud-bigquery` | Optional BQ load | Check — may need install |
| `pyarrow` | Parquet read/write + schema validation | Yes |

---

## 14. Test Plan

### Unit Tests

| Test | Validates |
|---|---|
| Schema validator catches extra columns | R1.2 — extra column in Silver triggers error |
| Schema validator catches type mismatch | R1.2 — `string` vs `timestamp[ns]` detected |
| Schema validator passes clean Silver | R1.2 — valid Silver files pass validation |
| Blocking groups cross-source products | R2.1 — "Cheerios" from OFF and USDA land in same block |
| Union-Find transitive closure | R2.3 — A↔B + B↔C → {A,B,C} cluster |
| Column-wise merge picks longest string | R2.4 — "Cheerios Original Cereal" wins over "Cheerios" |
| Golden record scores correctly | R2.5 — most complete + freshest record wins |
| S1 allergen extraction from ingredients | Tier 1 — "contains wheat and soy" → "wheat, soy" |
| S1 does not fill allergens from LLM | Tier 1 — safety boundary enforced |
| S2 batch FAISS returns correct neighbors | Tier 2 — known similar products matched |
| S2 respects similarity threshold | Tier 2 — low-similarity neighbors rejected |
| S3 batching groups by missing-field pattern | Tier 3 — records with same null pattern batched |
| S3 Redis cache hit skips LLM call | Tier 3 — cached response returned on re-run |
| S3 max call limit enforced | Tier 3 — pipeline stops calling LLM after limit hit |
| Enrichment provenance log is correct | R3.2 — `_enrichment_log` tracks tier + method per field |
| DQ score post > DQ score pre after enrichment | R3.3 — enrichment improves quality score |

### Integration Tests

| Test | Validates |
|---|---|
| End-to-end: 4 Silver sources → Gold catalog | Full pipeline produces valid output |
| Dedup removes known cross-source duplicates | OFF + USDA branded duplicate collapsed to 1 record |
| Gold output schema matches Silver contract | No accidental schema drift |
| Re-run with same inputs = same output | Reproducibility (with cached S3 responses) |
| Dry run reports stats without writing | `--dry-run` flag works correctly |
| Run log JSON is valid and complete | All fields populated, row counts match |

### Performance Benchmarks

| Metric | Target |
|---|---|
| Schema validation | < 5 seconds for all Silver sources |
| Dedup (1.25M rows) | < 10 minutes |
| S1 enrichment | < 30 seconds (pure Python rules) |
| S2 enrichment (FAISS build + query) | < 5 minutes |
| S3 enrichment (≤500 LLM calls) | < 15 minutes |
| Total Gold pipeline | < 35 minutes end-to-end |
| Memory peak | < 8GB (chunked if necessary) |

---

## 15. Out of Scope

- Airflow/Prefect DAG orchestration (manual CLI for now)
- Real-time/streaming Gold updates (batch only)
- ML classifier for dedup (fuzzy + rules sufficient at current scale)
- Neo4j knowledge graph writes (future spec)
- Product image dedup or matching
- ESCI dataset integration (structure not yet investigated)
- Gold → downstream API serving layer

---

## 16. Open Questions for Clarify Phase

1. **sr_legacy and survey sources** — ✅ RESOLVED: Run Gold without sr_legacy/survey; re-run when sources become available. Pipeline proceeds with OFF, USDA branded, USDA foundation, openFDA only.
2. **Dedup threshold tuning** — ✅ RESOLVED: Run 10K sample first to validate threshold 85 for cross-source matching before full run. Measure precision/recall on sample.
3. **Redis availability** — ✅ RESOLVED: Use SQLite file as local fallback cache when Redis unavailable. Path: `cache/s3_llm_cache.db`. Migrate to Redis when Spec 009 implemented.
4. **`dq_score_post` computation** — ✅ RESOLVED: Add dq_score_post block after Stage 3 (all enrichment complete), before Stage 4 output. Captures full enrichment effect for delta measurement.
5. **Memory at 1.25M rows** — ✅ RESOLVED: Eager load full DataFrame initially. Add lazy block-streaming fallback only if OOM observed. 8GB target provides sufficient headroom for 2-4GB working set.

---

## 17. Clarifications

### Session 2026-04-21

- Q: Should Gold pipeline run without sr_legacy/survey sources or wait? → A: Run without; re-run when available.
- Q: Should we validate dedup threshold 85 before full run? → A: Yes, run 10K sample first to measure precision/recall.
- Q: What cache fallback when Redis unavailable? → A: SQLite file as local fallback.
- Q: Where to add dq_score_post block? → A: After Stage 3 (all enrichment complete), before Stage 4.
- Q: Memory strategy at 1.25M rows? → A: Eager load; add lazy fallback only if OOM.