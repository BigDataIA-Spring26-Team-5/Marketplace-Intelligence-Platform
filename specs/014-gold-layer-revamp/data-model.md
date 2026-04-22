# Data Model: Gold Layer Revamp

**Created**: 2026-04-22
**Feature**: [spec.md](spec.md)

No new entities, tables, or schemas are introduced by this feature. The Gold output
schema written to BigQuery is unchanged. This document describes the data structures
that are *modified in behavior* by the 6 changes.

---

## 1. Silver Parquet — dtype contract at Gold read boundary

### Before (broken)

| Column | Arrow dtype | pandas dtype after read | Null representation |
|--------|------------|------------------------|---------------------|
| `product_name` | `string` | `StringDtype` | `pd.NA` |
| `brand_name` | `null` | `object` | `pd.NA` |
| `ingredients` | `string` | `StringDtype` | `pd.NA` |
| `brand_owner` | `string` | `StringDtype` | `pd.NA` |
| `data_source` | `string` | `StringDtype` | `pd.NA` |
| `allergens` | `null` | `object` | `pd.NA` |

### After (Change 1)

| Column | Arrow dtype | pandas dtype after cast | Null representation |
|--------|------------|------------------------|---------------------|
| `product_name` | `string` | `object` | `None` |
| `brand_name` | `null` | `object` | `None` |
| `ingredients` | `string` | `object` | `None` |
| `brand_owner` | `string` | `object` | `None` |
| `data_source` | `string` | `object` | `None` |
| `allergens` | `null` | `object` | `None` |

Cast rule: `string_cols = [c for c in df.columns if str(df[c].dtype) == "string"]`

Note: Arrow `null` dtype reads as pandas `object` with all-`pd.NA` values. The cast
loop covers these because `str(df[c].dtype) == "string"` is `False` for `null`-typed
columns — however, since null-typed columns already produce `object` dtype in pandas,
they naturally contain `None` after any `fillna(None)` or implicit object coercion.
The primary cast targets explicitly are the `string`→`StringDtype` columns.

---

## 2. ChromaDB Corpus Collection

**Collection name**: `product_corpus`
**Space**: cosine (HNSW)
**Host**: `localhost:8000` (configurable via `CHROMA_HOST`, `CHROMA_PORT`)

### Vector document schema (unchanged)

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` (SHA-256-16) | Stable upsert key: `hash(text + category)[:16]` |
| `embedding` | `float32[384]` | `all-MiniLM-L6-v2` embedding of `_build_row_text(row)` |
| `metadata.category` | `str` | `primary_category` value |
| `metadata.product_name` | `str` | Product name for debugging |
| `metadata.last_seen` | `str` (ISO-8601) | Timestamp of most recent upsert. Updated on re-upsert. Used for TTL eviction. |

### Augmentation input (Change 2)

`augment_from_df` encodes rows where `primary_category` is not null.
`_build_row_text` concatenates available non-null values from:
`["product_name", "brand_name", "ingredients", "category"]`

Safety fields (`allergens`, `is_organic`, `dietary_tags`) are **never** included
in `_build_row_text` and **never** stored in corpus metadata.

### Growth model

| Run | Corpus before | S1 resolved | Augmented | Corpus after |
|-----|--------------|------------|-----------|-------------|
| Sample run (cold start) | 0 | ~300 | ~300 | ~300 |
| Full OFF run (pre-fix, crash) | ~300 | 189,812 | 0 (bootstrap deadlock) | ~300 |
| Full OFF run (post-fix, first) | ~300 | 189,812 | ~189,812 | ~193,896 |
| Full OFF run (post-fix, repeat) | ~193,896 | varies | ~0 (upsert dedup) | ~193,896 |
| After 90-day TTL eviction | ~193,896 | varies | low delta | ≤193,896 |
| After size-cap eviction (if >500k) | >500,000 | varies | varies | ≤500,000 |

---

## 3. Dedup Blocking Key

### Before (3-char name prefix)

```
key = names.iloc[idx][:3].strip()
```

| Key | Example products | Block size risk |
|-----|-----------------|----------------|
| `"cho"` | All chocolate products | 12,038 rows observed |
| `"gre"` | green tea, greek yogurt, green beans… | 3,000–5,000 rows |
| `"ora"` | orange juice, oranges, oranta snacks… | 2,000–4,000 rows |

### After (4-char name + 2-char brand prefix)

```
name_prefix  = names.iloc[idx][:4].strip()
brand_prefix = brands.iloc[idx][:2].strip()
key = f"{name_prefix}_{brand_prefix}" if name_prefix else ""
```

| Key | Example products | Expected block size |
|-----|-----------------|---------------------|
| `"choc_ne"` | Nestlé chocolate products | <500 rows |
| `"choc_he"` | Hershey chocolate products | <500 rows |
| `"choc_"` | Chocolate products with no brand | <1,000 rows |
| `"gree_li"` | Lipton green tea | <200 rows |

### Null handling

| Case | Before | After |
|------|--------|-------|
| `product_name` = null | Excluded by `valid_name_mask` | Unchanged — excluded |
| `brand_name` = null | N/A (key used name only) | `brand_prefix = ""` → key = `"name_"` |
| `brand_name` = Arrow `null` dtype | N/A | `fillna("").astype(str)` at line 88 → `""` |

---

## 4. Config/Environment Variables

| Variable | Default | Type | Used by |
|----------|---------|------|---------|
| `CHROMA_QUERY_CHUNK_SIZE` | `500` | `int` | `corpus.py:knn_search_batch` |
| `MIN_ENRICHMENT_CORPUS` | `1000` | `int` | `embedding.py:embedding_enrich` |
| `CORPUS_AUGMENT_RATIO` | `0.25` | `float` | `corpus.py:augment_from_df` |
| `MAX_CORPUS_SIZE` | `500000` | `int` | `corpus.py:evict_corpus` (new) |
| `CORPUS_TTL_DAYS` | `90` | `int` | `corpus.py:evict_corpus` (new) |
| `DEDUP_BLOCK_OOM_THRESHOLD` | `2000` | `int` | `fuzzy_deduplicate.py` (existing, unchanged) |
| `CHROMA_HOST` | `localhost` | `str` | `corpus.py:_get_collection` (existing) |
| `CHROMA_PORT` | `8000` | `int` | `corpus.py:_get_collection` (existing) |

## 5. Corpus Eviction Logic

Run once per pipeline invocation, at the start of S2, before augmentation and before encoding.

```
evict_corpus(collection):
    cutoff = now - CORPUS_TTL_DAYS
    1. Query for vectors where metadata.last_seen < cutoff
    2. Delete in batches of 500
    3. If collection.count() > MAX_CORPUS_SIZE:
       Query all, sort by last_seen ascending
       Delete oldest until count <= MAX_CORPUS_SIZE (batches of 500)
```

Both passes are best-effort — ChromaDB failure logs WARNING and continues. Total eviction
runtime expected: 10–30 seconds (negligible vs. encoding cost).
