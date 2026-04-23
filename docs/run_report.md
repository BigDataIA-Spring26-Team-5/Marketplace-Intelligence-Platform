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

### Strategy 3 — LLM (DeepSeek)
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


## Last run
aq@mip-vm:~/work/Marketplace-Intelligence-Platform$ poetry run python -m src.pipeline.cli --source gs://mip-bronze-2024/off/2026/04/21/part_0000.jsonl --domain nutrition
08:20:22 [__main__] INFO: Created GCS checkpoint: run_id=e300a716-40a6-4a69-8441-1af2d68b8c07, source=gs://mip-bronze-2024/off/2026/04/21/part_0000.jsonl
08:20:22 [__main__] INFO: Created new checkpoint: run_id=e300a716-40a6-4a69-8441-1af2d68b8c07
08:20:22 [src.agents.orchestrator] INFO: Loading source: gs://mip-bronze-2024/off/2026/04/21/part_0000.jsonl
08:20:23 [src.pipeline.loaders.gcs_loader] INFO: GCS: found 1 blobs matching gs://mip-bronze-2024/off/2026/04/21/part_0000.jsonl
08:20:23 [src.pipeline.loaders.gcs_loader] INFO: GCS schema sample: streaming off/2026/04/21/part_0000.jsonl
08:20:29 [src.pipeline.loaders.gcs_loader] INFO: GCS schema sample: 5000 rows from off/2026/04/21/part_0000.jsonl
08:20:29 [src.agents.orchestrator] INFO: GCS schema sample: 5000 rows loaded for schema analysis
08:20:29 [src.agents.orchestrator] INFO: Sampling: method=full_scan, sample_size=5000, fallback=True
08:20:29 [src.agents.orchestrator] INFO: Unified schema found — diffing against source
08:21:08 [src.agents.orchestrator] INFO: Schema analysis: 5 mappings, 20 derivable gaps, 2 missing columns
08:21:08 [src.agents.orchestrator] INFO: Enrichment columns absent from source (will be generated by blocks): ['primary_category', 'dietary_tags', 'is_organic']
08:21:08 [src.agents.orchestrator] INFO: Enrichment aliases: [('category', '←', 'primary_category')]
08:21:08 [src.agents.orchestrator] INFO: Agent 1 unresolved columns (final — Critic disabled): ['brand_owner', 'data_source']
08:21:08 [src.agents.graph] INFO: Agent 2 (Critic) skipped — use --with-critic to enable
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_safety (domain: safety, file: DYNAMIC_MAPPING_fda_recalls_sample.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_synthetic_dataset_3_camelcase.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_part_0000.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_synthetic_dataset_5_kv_pairs.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_usda_sample_raw.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_en.openfoodfacts.org.products.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_nutrition (domain: nutrition, file: DYNAMIC_MAPPING_usda_fooddata_sample.yaml)
08:21:08 [src.registry.block_registry] INFO: Loaded YAML mapping block: DYNAMIC_MAPPING_test (domain: test, file: DYNAMIC_MAPPING_test_split.yaml)
08:21:08 [src.registry.block_registry] INFO: BlockRegistry initialized with 16 blocks (3 generated)
08:21:08 [src.agents.orchestrator] INFO: ADD op for 'brand_owner' → YAML set_null
08:21:08 [src.agents.orchestrator] INFO: ADD op for 'data_source' → YAML set_default
08:21:08 [src.agents.orchestrator] INFO: FORMAT gap 'product_name' → YAML format_transform
08:21:08 [src.agents.orchestrator] INFO: FORMAT gap 'ingredients' → YAML format_transform
08:21:08 [src.agents.orchestrator] INFO: FORMAT gap 'serving_size' → YAML regex_extract
08:21:08 [src.agents.orchestrator] INFO: FORMAT gap 'serving_size_unit' → YAML regex_extract
08:21:08 [src.agents.orchestrator] INFO: FORMAT gap 'published_date' → YAML parse_date
08:21:08 [src.agents.orchestrator] INFO: DELETE 'pnns_groups_1' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'pnns_groups_2' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'allergens' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'traces' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'countries' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'energy_100g' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'fat_100g' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'carbohydrates_100g' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'proteins_100g' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'salt_100g' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'nova_group' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'nutriscore_grade' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: DELETE 'data_quality_tags' → YAML drop_column
08:21:08 [src.agents.orchestrator] INFO: Block registry hit for gap 'dietary_tags': llm_enrich
08:21:08 [src.agents.orchestrator] INFO: Block registry hit for gap 'is_organic': llm_enrich
08:21:08 [src.blocks.mapping_io] INFO: Wrote mapping YAML: /home/aq/work/Marketplace-Intelligence-Platform/src/blocks/generated/nutrition/DYNAMIC_MAPPING_part_0000.yaml (20 operations)
08:21:08 [src.registry.block_registry] INFO: Registered block: DYNAMIC_MAPPING_nutrition
08:21:08 [src.agents.orchestrator] INFO: Registered DynamicMappingBlock: DYNAMIC_MAPPING_nutrition
08:21:08 [src.agents.orchestrator] INFO: All missing columns have coverage (alias, block, or YAML)
08:21:15 [src.agents.graph] INFO: Agent 3 planned sequence (13 blocks): ['dq_score_pre', '__generated__', 'strip_whitespace', 'lowercase_brand', 'remove_noise_words', 'strip_punctuation', 'extract_quantity_column', 'fuzzy_deduplicate', 'column_wise_merge', 'golden_record_select', 'extract_allergens', 'llm_enrich', 'dq_score_post']
08:21:15 [src.agents.graph] INFO: Agent 3 reasoning: The order follows the rules by placing schema transformation after initial DQ scoring, normalization before deduplication, allergen extraction before LLM enrichment, and final DQ scoring last.
08:21:15 [src.pipeline.loaders.gcs_loader] INFO: GCS: found 1 blobs matching gs://mip-bronze-2024/off/2026/04/21/part_0000.jsonl
08:21:15 [src.pipeline.loaders.gcs_loader] INFO: GCS: streaming off/2026/04/21/part_0000.jsonl
08:21:20 [src.pipeline.runner] INFO: Processing chunk 1 (10000 rows)
08:21:20 [src.pipeline.runner] INFO: Expanded sequence: ['dq_score_pre', 'DYNAMIC_MAPPING_nutrition', 'strip_whitespace', 'lowercase_brand', 'remove_noise_words', 'strip_punctuation', 'extract_quantity_column', 'fuzzy_deduplicate', 'column_wise_merge', 'golden_record_select', 'extract_allergens', 'llm_enrich', 'dq_score_post']
08:21:20 [src.blocks.dq_score] INFO: DQ Score (pre): mean=43.5%, min=11.4%, max=54.0%
08:21:20 [src.pipeline.runner] INFO: Block 'dq_score_pre': 10000 -> 10000 rows
08:21:20 [src.pipeline.runner] INFO: Block 'DYNAMIC_MAPPING_nutrition': 10000 -> 10000 rows
08:21:20 [src.pipeline.runner] INFO: Block 'strip_whitespace': 10000 -> 10000 rows
08:21:20 [src.pipeline.runner] INFO: Block 'lowercase_brand': 10000 -> 10000 rows
08:21:20 [src.pipeline.runner] INFO: Block 'remove_noise_words': 10000 -> 10000 rows
08:21:21 [src.pipeline.runner] INFO: Block 'strip_punctuation': 10000 -> 10000 rows
08:21:21 [src.pipeline.runner] INFO: Block 'extract_quantity_column': 10000 -> 10000 rows
08:21:46 [src.blocks.fuzzy_deduplicate] INFO: Dedup: 10000 rows → 5488 clusters (45.1% duplicate rate)
08:21:46 [src.blocks.fuzzy_deduplicate] INFO:   Largest cluster #330: 709 rows
08:21:46 [src.blocks.fuzzy_deduplicate] INFO:   Largest cluster #1155: 577 rows
08:21:46 [src.blocks.fuzzy_deduplicate] INFO:   Largest cluster #439: 136 rows
08:21:46 [src.pipeline.runner] INFO: Block 'fuzzy_deduplicate': 10000 -> 10000 rows
08:21:56 [src.blocks.column_wise_merge] INFO: Column-wise merge: 10000 rows → 5488 merged rows
08:21:56 [src.pipeline.runner] INFO: Block 'column_wise_merge': 10000 -> 5488 rows
08:21:56 [src.pipeline.runner] INFO: Block 'golden_record_select': 5488 -> 5488 rows
08:21:57 [src.blocks.extract_allergens] INFO: Allergens: detected in 5488/5488 rows (100.0%)
08:21:57 [src.pipeline.runner] INFO: Block 'extract_allergens': 5488 -> 5488 rows
08:21:57 [src.blocks.llm_enrich] INFO: Enrichment: 5488/5488 rows need enrichment
08:21:58 [src.blocks.llm_enrich] INFO:   S1 (deterministic extraction): resolved 4732 rows
08:21:58 [faiss.loader] INFO: Loading faiss with AVX2 support.
08:21:59 [faiss.loader] INFO: Successfully loaded faiss with AVX2 support.
08:21:59 [src.enrichment.corpus] INFO: Loaded corpus: 6711 vectors
08:22:25 [sentence_transformers.SentenceTransformer] INFO: Use pytorch device_name: cpu
08:22:25 [sentence_transformers.SentenceTransformer] INFO: Load pretrained SentenceTransformer: all-MiniLM-L6-v2
08:22:45 [src.enrichment.corpus] INFO: Saved corpus: 6719 vectors
08:22:45 [src.enrichment.embedding] INFO: S2 KNN: resolved 8 rows
08:22:45 [src.blocks.llm_enrich] INFO:   S2 (KNN corpus): resolved 8 rows
08:22:45 [src.enrichment.llm_tier] INFO: S3 LLM: 748 rows need primary_category (batch_size=20)
08:22:45 [src.enrichment.corpus] INFO: Loaded corpus: 6719 vectors
08:32:01 [src.enrichment.corpus] INFO: Saved corpus: 7453 vectors
08:32:01 [src.blocks.llm_enrich] INFO:   S3 (LLM): resolved 734 rows
08:32:01 [src.blocks.llm_enrich] INFO:   Unresolved: 14 rows
/home/aq/work/Marketplace-Intelligence-Platform/src/blocks/llm_enrich.py:92: FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
  after.loc[s3_llm_rows].fillna("__null__")
/home/aq/work/Marketplace-Intelligence-Platform/src/blocks/llm_enrich.py:93: FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
  != before.loc[s3_llm_rows].fillna("__null__")
08:32:01 [src.pipeline.runner] INFO: Block 'llm_enrich': 5488 -> 5488 rows
08:32:01 [src.blocks.dq_score] INFO: DQ Score (post): mean=59.5%, delta=+16.2%
08:32:01 [src.pipeline.runner] INFO: Block 'dq_score_post': 5488 -> 5488 rows
08:32:02 [src.agents.graph] INFO: Quarantine: 5488 rows failed post-enrichment validation
08:32:02 [src.agents.graph] INFO: Output saved to /home/aq/work/Marketplace-Intelligence-Platform/output/part_0000_unified.csv (0 rows)
08:32:02 [src.pipeline.checkpoint.manager] INFO: Saved checkpoint state for chunk 0
08:32:02 [__main__] INFO: Checkpoint saved
08:32:02 [__main__] INFO: Pipeline complete: 0 rows, DQ: 43.3% -> 59.5%
aq@mip-vm:~/work/Marketplace-Intelligence-Platform$