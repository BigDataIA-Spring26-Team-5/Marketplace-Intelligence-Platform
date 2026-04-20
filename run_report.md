# Run Report — `en.openfoodfacts.org.products.csv`

**Run ID:** `d58700c0-1a95-47dd-9753-57180a83de55`
**Date:** 2026-04-18
**Killed:** ~75–90 min in, mid-chunk 10

---

## Checkpoint DB Status

File: `checkpoint/checkpoint.db`

4 runs recorded:

| run_id (short) | File | Started |
|---|---|---|
| `70007fcf` | en.openfoodfacts | Apr 18 08:26 |
| `5e2eaa96` | en.openfoodfacts | Apr 18 18:56 |
| `345604c0` | usda_fooddata_sample | Apr 18 19:58 |
| `d58700c0` ← **this run** | en.openfoodfacts | Apr 18 20:04 |

`chunk_states` table: **0 rows.** Schema exists, no data written. Checkpoint manager created run record but never persisted per-chunk progress. No resume capability was active — killing process lost all processed data.

---

## Transformation Blocks Generated

**Auto-generated YAML block** (Agent 1 + Agent 2 critic):
`src/blocks/generated/nutrition/DYNAMIC_MAPPING_en.openfoodfacts.org.products.yaml`

| Operation | Action | Detail |
|---|---|---|
| `serving_size` | `regex_extract` | Pattern `([0-9]+\.?[0-9]*)` — parse numeric value |
| `serving_size_unit` | `regex_extract` | Unit token pattern (30+ unit words) |
| `serving_size_unit` | `value_map` | 30+ aliases → canonical (g, ml, oz, etc.) |
| `published_date` | `parse_date` | Standardize date format |
| `data_source` | `set_default` | Static label `"OpenFoodFacts"` |

**Known bug — fires every chunk:**
Both `serving_size` and `serving_size_unit` source from same column `serving_size`. `regex_extract` returns multi-column DataFrame, then assignment fails:
```
Columns must be same length as key
Cannot set a DataFrame with multiple columns to single column
```
Result: `serving_size` and `serving_size_unit` stayed null throughout entire run.

**Block sequence per chunk:**
```
dq_score_pre
→ DYNAMIC_MAPPING_en.openfoodfacts
→ strip_whitespace → lowercase_brand → remove_noise_words → strip_punctuation
→ extract_quantity_column
→ fuzzy_deduplicate → column_wise_merge → golden_record_select
→ extract_allergens → llm_enrich (S1 → S2 → S3)
→ dq_score_post
```

---

## Rows Processed

| Metric | Value |
|---|---|
| Chunk size | 10,000 rows |
| Chunks completed | ~9–10 (killed mid-chunk 10) |
| Rows read from CSV | ~90,000–100,000 |
| Rows after dedup (est.) | ~75,000–80,000 |
| Output CSV written | **None** — killed before `pd.concat` + `save_output_node` |

---

## Deduplication Statistics

Config: 3-char prefix blocking, weighted score (name 0.5 / brand 0.2 / combined 0.3), threshold 85.

| Chunk | Observation |
|---|---|
| Chunk 1 | Oversized cluster: ~1,420 rows → 1 group. Null/empty `product_name` shares key `""` |
| Chunks 2–9 | Normal distribution, largest cluster 2–5 rows |
| Estimated dedup rate | 15–18% |
| Cross-chunk dedup | **Not performed** — each chunk deduped independently |

`column_wise_merge` bottleneck: ~4 min/chunk regardless of cluster size. Root cause: ~180 OFf columns processed individually per cluster even for trivially small clusters.

---

## Enrichment Statistics

### Strategy 1 — Deterministic (regex/keyword)
- `categories_en` and `main_category_en` present in OFf source → high hit rate
- Estimated **40–55%** of rows resolved by S1

### Strategy 2 — KNN FAISS
- Corpus grew via flywheel each chunk
- Start: ~0 vectors → End: **194,098 vectors**
- Confidence threshold: 0.60 cosine similarity
- Estimated **25–35%** of S1-remaining rows resolved by S2

### Strategy 3 — RAG-LLM (DeepSeek)
- Batch size: 20 rows/call
- Est. calls per chunk: 15–30
- Total across 9 chunks: **~135–270 batch LLM calls**

### Corpus Category Distribution (post-run, 194,098 total vectors)

| Category | Vectors | Share |
|---|---|---|
| Snacks | 40,473 | 20.8% |
| Bakery | 38,108 | 19.6% |
| Confectionery | 36,845 | 19.0% |
| Dairy | 32,734 | 16.9% |
| Condiments | 14,598 | 7.5% |
| Beverages | 8,558 | 4.4% |
| Frozen Foods | 6,766 | 3.5% |
| Canned Foods | 5,141 | 2.6% |
| Pasta & Grains | 3,058 | 1.6% |
| Deli | 1,845 | 0.9% |
| Other | ~6,072 | 3.1% |

---

## Cost Estimate

**API:** DeepSeek via LiteLLM (`deepseek-chat` + `deepseek-reasoner`)
Pricing: $0.27/M input tokens, $1.10/M output tokens (deepseek-chat)

| Call Type | Count | Est. Cost |
|---|---|---|
| Schema analysis | 1 (~3K in, 1K out) | ~$0.001 |
| Critic (deepseek-reasoner) | 1 (~4K tokens) | ~$0.004 |
| Sequence planner | 1 | ~$0.001 |
| S3 enrichment batches | ~200 × ~1.5K tokens avg | ~$0.09 |
| **Total** | | **~$0.10–0.15** |

---

## Time Breakdown

Total wall time: **~75–90 min**

| Phase | Time |
|---|---|
| Schema analysis (5K sample) | ~2 min |
| Chunk 1 (corpus seed + oversized dedup cluster) | ~12–15 min |
| Chunks 2–9 average | ~7–9 min each |
| `column_wise_merge` per chunk | ~4 min (bottleneck) |
| S3 LLM per chunk | ~1–2 min |
| S2 KNN per chunk | ~20–40 sec |

---

## Artifacts on Disk

| Artifact | Size | Status |
|---|---|---|
| `corpus/faiss_index.bin` | 284 MB | Saved — 194K vectors |
| `corpus/corpus_metadata.json` | 16 MB | Saved |
| `src/blocks/generated/nutrition/DYNAMIC_MAPPING_en.openfoodfacts.org.products.yaml` | ~5 KB | Saved |
| `checkpoint/checkpoint.db` | — | Schema only, 0 chunk rows |
| `output/en.openfoodfacts.org.products_unified.csv` | — | **Not written** |

---

## Pending Fixes

| # | Issue | Impact |
|---|---|---|
| 1 | `column_wise_merge` bottleneck (~4 min/chunk) | Skip merge for clusters > N rows or limit to key columns |
| 2 | `regex_extract` YAML bug — serving_size/unit fail every chunk | Fix: split into two source columns or use single capture group |
| 3 | `primary_category` DERIVE interception via `_BLOCK_COLUMN_PROVIDERS` | Sends rows to full S1/S2/S3 when `coalesce` from `categories_en` would suffice |
| 4 | Chunk state never written to `chunk_states` table | Kill = total data loss; needs per-chunk persistence |
| 5 | `low_memory=False` missing in `CsvStreamReader.__iter__` | DtypeWarning on mixed-type OFf columns |
| 6 | Empty `product_name` dedup guard | 3-char prefix `""` collapses all null-name rows into one cluster (1,420 rows in chunk 1) |
