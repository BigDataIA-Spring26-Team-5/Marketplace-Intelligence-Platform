# Feature Specification: Gold Layer Revamp — Reliability & Performance

**Feature Branch**: `aqeel`
**Created**: 2026-04-22
**Status**: Draft
**Depends on**: 013-silver-gold-pipeline (base Gold architecture)
**Input**: Post-mortem of OFF/2026-04-21 Gold run — 4h 16min crash, 0 rows written to BigQuery

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Pipeline Completes Without Crashing (Priority: P1)

A data engineer runs the Gold pipeline on a full Silver partition (780k+ rows from the OFF
food database). The pipeline reads Parquet files, deduplicates, enriches, and writes results
to BigQuery without crashing. Currently the pipeline crashes after 4+ hours because nullable
text values from Parquet files cause a runtime error deep in the enrichment stage.

**Why this priority**: Zero rows reach BigQuery on a crash. The entire run is wasted compute.
This is the blocker for all other value.

**Independent Test**: Run `gold_pipeline --source off --date 2026/04/21` on the existing
`sample.parquet` (1000-row sample). Run must complete and write rows to BigQuery without
any `TypeError`.

**Acceptance Scenarios**:

1. **Given** a Silver Parquet file where text columns contain null values (Arrow `string` dtype), **When** the Gold pipeline reads the file, **Then** all null values are handled as empty strings — no `TypeError: boolean value of NA is ambiguous` is raised at any pipeline stage.
2. **Given** a Silver Parquet file where a column is entirely null (`null` Arrow dtype, e.g., `brand_name` in branded source), **When** the pipeline processes that column, **Then** the run continues and treats those values as empty — it does not fail or skip the source.
3. **Given** a valid Silver partition with mixed null/non-null text columns, **When** the full 6-block Gold sequence runs to completion, **Then** BigQuery receives the enriched rows and the run log records `status: success`.

---

### User Story 2 — S2 Enrichment Resolves Rows (Priority: P2)

After the pipeline deduplicates 783k rows to 599k unique products, the enrichment stage
assigns a `primary_category` to as many rows as possible. Strategy 2 (KNN corpus search)
is supposed to handle ~35–40% of total rows using similarity matching against known products.
Currently S2 resolves 0 rows because: (a) the corpus is too sparse relative to the query
volume, and (b) the ChromaDB query call sends all vectors in one HTTP request, which is
rejected as too large.

**Why this priority**: Without S2, all 387k unresolved rows fall to S3 (LLM API calls),
which is slower, more expensive, and rate-limited. S2's value is cost and speed reduction.

**Independent Test**: Run the pipeline on the sample partition. After S1 resolves its share,
the S2 augmentation log must appear and S2 must resolve at least 1 row (corpus populated
from S1 results).

**Acceptance Scenarios**:

1. **Given** a corpus that has fewer vectors than 25% of the unresolved row count, **When** S2 starts, **Then** it first augments the corpus from S1-resolved rows in the current run before querying.
2. **Given** a corpus augmented to 189k+ vectors, **When** S2 queries for neighbors, **Then** the query is sent in chunks of 500 vectors — no single HTTP call exceeds the ChromaDB payload limit.
3. **Given** a corpus with fewer than 1000 vectors even after augmentation, **When** S2 would start encoding 400k+ rows, **Then** S2 is skipped entirely with a clear log message — no CPU time wasted.
4. **Given** a healthy corpus (>1000 vectors), **When** S2 runs on 409k unresolved rows, **Then** it resolves more than 0 rows and the count is logged.

---

### User Story 3 — Deduplication Completes in Under 1 Hour (Priority: P3)

The fuzzy deduplication block groups similar products and merges duplicates. Currently it
takes 2h 15min for 783k rows because a 3-character blocking key groups thousands of
unrelated products (e.g., all "chocolate" products share the key "cho"), creating blocks
of 12,000+ rows that require 72 million pairwise comparisons.

**Why this priority**: Dedup is the single largest time sink. Under the current scheme,
even a perfectly fixed enrichment stage still produces a 3h+ total runtime.

**Independent Test**: Run dedup on the full 783k-row OFF partition. Count of blocks exceeding
the OOM threshold (2000 rows) should be dramatically reduced vs. the 87 observed in the
crash run.

**Acceptance Scenarios**:

1. **Given** a DataFrame of 783k product rows, **When** the dedup block runs with the new blocking key, **Then** the number of blocks exceeding the OOM threshold (2000 rows) is fewer than 20 (vs. 87 previously).
2. **Given** product rows where `brand_name` is null, **When** the composite blocking key is computed, **Then** those rows use only the 4-char name prefix and are not grouped into a null-brand mega-cluster.
3. **Given** the full OFF partition, **When** dedup completes, **Then** the duplicate rate remains within ±5% of the 23.5% baseline (confirming the key change doesn't break clustering logic).

---

### Edge Cases

- Silver file where ALL rows have null `ingredients` — S3 cache hash uses empty string, no crash.
- Corpus augmentation called when ChromaDB is unreachable — logs WARNING, returns 0, pipeline continues to S3.
- ChromaDB query chunk partially fails mid-batch — failed chunk positions filled with `(None, 0.0, [])`, rest of batch continues.
- Product name exactly 3 chars long — 4-char prefix truncates to 3 chars, brand prefix still appended.
- Product name is empty string — excluded from blocking by existing `valid_name_mask` guard (unchanged).
- `source` = `branded` where `brand_name` is Arrow `null` dtype (all nulls) — cast to `object`, dedup uses empty string for brand prefix, no crash.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Pipeline MUST cast all Arrow `StringDtype` columns to `object` dtype immediately after reading Silver Parquet, before any block runs.
- **FR-002**: Pipeline MUST accept Arrow `null`-typed columns (all-null columns) without failing — they are cast to `object` dtype alongside string columns.
- **FR-003**: S2 enrichment MUST check if `corpus_size / unresolved_row_count < 0.25` before querying, and if so, augment the corpus from S1-resolved rows in the current run.
- **FR-004**: Corpus augmentation MUST use upsert semantics — rows already present in the corpus are not re-encoded.
- **FR-005**: Corpus augmentation MUST be best-effort — ChromaDB failure during augmentation logs a WARNING and allows the pipeline to continue to S3.
- **FR-006**: S2 `knn_search_batch` MUST send ChromaDB queries in chunks of at most 500 embeddings per HTTP call (configurable via `CHROMA_QUERY_CHUNK_SIZE`).
- **FR-007**: A chunk-level ChromaDB query failure MUST NOT abort the full batch — failed chunk positions return `(None, 0.0, [])`.
- **FR-008**: If corpus size is below 1000 vectors after augmentation, S2 MUST be skipped entirely and the pipeline falls through to S3.
- **FR-009**: `llm_tier.py` MUST handle `pd.NA`, `None`, and `float NaN` values in any text field using a `_safe_text` helper that returns an empty string for all three.
- **FR-010**: Dedup blocking key MUST use 4 characters of product name + 2 characters of brand name (composite key), replacing the current 3-character name-only key.
- **FR-011**: Null brand names MUST produce an empty brand prefix (not a null-key collision) — existing `valid_name_mask` guard for null product names is unchanged.
- **FR-012**: The ChromaDB corpus MUST implement a two-tier eviction strategy, run once per pipeline invocation at the start of S2 (before encoding):
  1. **TTL eviction**: Delete all vectors whose `last_seen` metadata field is older than `CORPUS_TTL_DAYS` (default 90 days). These represent products not seen in any recent run and may reflect stale or deprecated category taxonomies.
  2. **Size cap**: If the corpus still exceeds `MAX_CORPUS_SIZE` (default 500,000) after TTL eviction, evict by oldest `last_seen` until under cap.
  Eviction runs in batches (same 500-chunk pattern). If ChromaDB is unreachable, eviction is skipped silently (best-effort).
- **FR-012a**: Every vector upserted to the corpus MUST include a `last_seen` ISO-8601 timestamp in its metadata. When an existing vector (same `_make_vector_id` key) is re-upserted, its `last_seen` MUST be updated to the current run timestamp. This keeps frequently-recurring products fresh and allows one-off batch products to age out naturally.
- **FR-013**: The Gold run log `enrichment_stats` dict MUST include two new keys after each run: `corpus_augmented` (integer count of vectors upserted by `augment_from_df`, 0 if augmentation was skipped) and `corpus_size_after` (integer total corpus vector count after augmentation, 0 if ChromaDB unreachable).

### Pipeline Governance Constraints

- Gold output schema is **unchanged** — no new columns, no removed columns, no type changes in BigQuery.
- Safety fields (`allergens`, `is_organic`, `dietary_tags`) remain S1-extraction-only. `augment_from_df` only augments `primary_category` vectors — safety fields are never added to the KNN corpus.
- `_validate_silver_schema` behavior is unchanged: missing required columns still raise `ValueError`; missing expected columns still log `WARNING`. The dtype cast is additive, not a substitute for validation.
- Redis dedup cache keys are SHA-256 hashes of normalized product names — blocking key change does not affect cache consistency across partitions.

### Key Entities

- **Silver Parquet**: Source input. Arrow `string` and `null` dtype columns must be normalized at read time.
- **ChromaDB corpus**: Persistent vector store for KNN enrichment. Grows via upsert from S1-resolved rows. Minimum 1000 vectors required for batch querying. Each vector carries a `last_seen` timestamp updated on re-upsert. Eviction at S2 start: TTL (default 90 days) first, then size cap (default 500,000) by oldest `last_seen`.
- **Dedup block**: Groups rows by composite 4-char name + 2-char brand prefix before fuzzy scoring.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Full OFF Gold run (783k rows) completes without crashing and writes rows to BigQuery.
- **SC-002**: Total Gold pipeline runtime for the full OFF partition is under 3 hours (vs. 4h 16min crash run).
- **SC-003**: Dedup block runtime for 783k rows is under 1 hour (vs. 2h 15min).
- **SC-004**: S2 resolves more than 0 rows on the full OFF partition (vs. 0 on crash run).
- **SC-005**: 1000-row sample run (`sample.parquet`) completes in under 10 minutes end-to-end.
- **SC-006**: Number of dedup blocks exceeding the OOM threshold (2000 rows) drops below 20 (vs. 87).

---

## Assumptions

- Silver Parquet files will continue to be written by the teammate-owned Silver ETL. The `brand_name: null` Arrow dtype defect in branded/foundation/openfda Silver files is being fixed separately; Gold must tolerate it until that fix ships.
- ChromaDB is running and reachable at `localhost:8000` for the duration of a Gold run. If unreachable, S2 is skipped and S3 handles all enrichment (existing fallback behavior).
- Redis is running and reachable at `localhost:6379`. If unreachable, cache is bypassed (existing fallback behavior).
- The `all-MiniLM-L6-v2` SentenceTransformer model runs on CPU. GPU acceleration is out of scope for this revamp.
- The 0.25 corpus-to-query ratio threshold and 1000-vector minimum are starting values; they may be tuned after observing the first successful full run.
- The `aqeel` branch is the working branch; no merge to main is expected until the full run validates successfully.

---

## Technical Specification

*The sections below are implementation-level detail for engineers. The requirements and
success criteria above are the authoritative acceptance standard.*

### Problem Root Causes

| # | Bug | Symptom | Stage | Time lost |
|---|-----|---------|-------|-----------|
| 1 | Arrow `string` → pandas `StringDtype` → `pd.NA.__bool__()` raises | `TypeError: boolean value of NA is ambiguous` at `llm_tier.py:190` | S3 | 4h 16min (entire run) |
| 2 | Corpus bootstrap deadlock: 4,084 vectors vs 409k queries, `index.count() < 10` gate skips re-seed | S2 resolves 0 rows | S2 | ~1h 31min wasted |
| 3 | `knn_search_batch` sends ~600 MB JSON payload in one call | ChromaDB `Payload too large` | S2 | included above |
| 4 | 3-char blocking key → "cho" block = 12,038 rows = 72M comparisons | 87 OOM blocks, lazy pairwise | Dedup | 2h 15min |

### Hard Rules

| # | Rule |
|---|------|
| H1 | Safety fields (`allergens`, `is_organic`, `dietary_tags`) remain S1-extraction-only. |
| H2 | dtype cast happens at GCS read boundary — not inside individual blocks. |
| H3 | Corpus augmentation uses upsert only — no re-encoding of existing vectors. |
| H4 | `knn_search_batch` chunking is internal — `embedding.py` call site unchanged. |
| H5 | Blocking key change preserves existing `valid_name_mask` null guard. |
| H6 | Backward-compatible with all 4 sources: off, branded, foundation, openfda. |

### Architecture (unchanged block sequence)

```
Silver GCS Parquet
       │
       ▼
  _read_silver_parquet()          ← Change 1: StringDtype → object cast
       │
       ▼
  fuzzy_deduplicate               ← Change 6: blocking key name[:4]_brand[:2]
       │
       ▼
  column_wise_merge
       │
       ▼
  golden_record_select
       │
       ▼
  extract_allergens
       │
       ▼
  llm_enrich
    ├─ S1 deterministic           (unchanged)
    ├─ S2 KNN corpus              ← Change 2: ratio-aware augmentation
    │    └─ knn_search_batch      ← Change 3: chunked query loop (500/call)
    │    └─ short-circuit gate    ← Change 4: MIN_ENRICHMENT_CORPUS=1000
    └─ S3 RAG-LLM                 ← Change 5: _safe_text helper
       │
       ▼
  dq_score_post
       │
       ▼
  BigQuery mip_gold.products
```

### Detailed Requirements

#### Change 1 — dtype cast (`src/pipeline/gold_pipeline.py`)

After `pd.concat` in `_read_silver_parquet`:
```python
string_cols = [c for c in df.columns if str(df[c].dtype) == "string"]
if string_cols:
    df[string_cols] = df[string_cols].astype(object)
    logger.debug("Cast %d StringDtype columns to object: %s", len(string_cols), string_cols)
```

#### Change 2 — Ratio-aware augmentation (`src/enrichment/corpus.py`)

New function `augment_from_df(df, collection, force_ratio_threshold=0.25)`:
- If `collection.count() / len(unresolved_rows) < force_ratio_threshold`, encode S1-resolved rows and upsert in chunks of 500.
- Called from `embedding_enrich` in `src/enrichment/embedding.py` before `knn_search_batch`.

#### Change 3 — Chunked query (`src/enrichment/corpus.py`)

Replace single `index.query(query_embeddings=embeddings.tolist(), ...)` with loop:
```python
CHROMA_QUERY_CHUNK_SIZE = int(os.environ.get("CHROMA_QUERY_CHUNK_SIZE", "500"))
```
Collect `all_metadatas` + `all_distances` per chunk, reconstruct `batch_results` dict.
Per-chunk exception → fill positions with `([], [])`, log WARNING, continue.

#### Change 4 — Short-circuit gate (`src/enrichment/embedding.py`)

```python
MIN_ENRICHMENT_CORPUS = int(os.environ.get("MIN_ENRICHMENT_CORPUS", "1000"))
```
After augmentation, if `collection.count() < MIN_ENRICHMENT_CORPUS` → skip S2, return
`{"resolved": 0, "skipped": "corpus_too_small"}`. Existing `MIN_CORPUS_SIZE=10` unchanged
(used only in single-row `knn_search` path).

#### Change 5 — `_safe_text` (`src/enrichment/llm_tier.py`)

```python
def _safe_text(v) -> str:
    try:
        return "" if pd.isna(v) else str(v)
    except (TypeError, ValueError):
        return str(v) if v is not None else ""
```

Replace at lines 189-190 and 282-283:
```python
product_name = _safe_text(row.get("product_name"))
description  = _safe_text(row.get("ingredients")) or _safe_text(row.get("description"))
```

#### Change 7 — Run log observability (`src/pipeline/gold_pipeline.py`)

In `_build_gold_run_log`, extend `enrichment_stats` dict:
```python
"enrichment_stats": {
    "deterministic":      es.get("deterministic",      0),
    "embedding":          es.get("embedding",          0),
    "llm":                es.get("llm",                0),
    "unresolved":         es.get("unresolved",         0),
    "corpus_augmented":   es.get("corpus_augmented",   0),  # new
    "corpus_size_after":  es.get("corpus_size_after",  0),  # new
},
```

`LLMEnrichBlock.last_enrichment_stats` must be updated to populate both keys from
the return value of `augment_from_df`. If augmentation was skipped (ratio met or
ChromaDB down), both keys default to 0.

#### Change 6 — Blocking key (`src/blocks/fuzzy_deduplicate.py`)

```python
# Before (line 119):
key = names.iloc[idx][:3].strip()

# After:
name_prefix  = names.iloc[idx][:4].strip()
brand_prefix = brands.iloc[idx][:2].strip()
key = f"{name_prefix}_{brand_prefix}" if name_prefix else ""
```

### Modified Files

| File | Change |
|------|--------|
| `src/pipeline/gold_pipeline.py` | Change 1: dtype cast after concat |
| `src/enrichment/corpus.py` | Change 2: `augment_from_df`; Change 3: chunked query |
| `src/enrichment/embedding.py` | Change 2: call augment; Change 4: short-circuit gate |
| `src/enrichment/llm_tier.py` | Change 5: `_safe_text` helper + 2 call sites |
| `src/blocks/fuzzy_deduplicate.py` | Change 6: 4-char composite blocking key |
| `src/pipeline/gold_pipeline.py` | Change 7: `enrichment_stats` gains `corpus_augmented` + `corpus_size_after` keys in `_build_gold_run_log` |

### Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `CHROMA_QUERY_CHUNK_SIZE` | `500` | Max embeddings per ChromaDB `query()` call |
| `MIN_ENRICHMENT_CORPUS` | `1000` | Min corpus vectors before S2 batch runs |
| `CORPUS_AUGMENT_RATIO` | `0.25` | Augmentation trigger threshold |
| `MAX_CORPUS_SIZE` | `500000` | Max ChromaDB corpus vectors; oldest `last_seen` evicted after TTL pass |
| `CORPUS_TTL_DAYS` | `90` | Vectors not re-seen within this many days are evicted at S2 start |
| `DEDUP_BLOCK_OOM_THRESHOLD` | `2000` | Existing — unchanged |

### Out of Scope

- Silver ETL dtype fixes (`brand_name: null`, `published_date: timestamp`) — teammate
- `source_name` absent from Silver — Silver ETL concern
- GPU acceleration — post-fix evaluation
- ChromaDB infra/scaling — infra concern
- New enrichment strategies or Gold output columns

---

## Clarifications

### Session 2026-04-22

- Q: Should the ChromaDB corpus be bounded in size across repeated runs? → A: Two-tier eviction — TTL-first (default 90 days, `CORPUS_TTL_DAYS`) then size cap (default 500,000, `MAX_CORPUS_SIZE`) by oldest `last_seen`. Every vector carries a `last_seen` timestamp updated on re-upsert. Eviction runs once per pipeline invocation at S2 start, best-effort.
- Q: Should corpus augmentation be observable in the run log? → A: Add both `corpus_augmented` (vectors upserted) and `corpus_size_after` (total post-augmentation) to `enrichment_stats` in the Gold run log.
