# Tasks: Gold Layer Revamp — Reliability & Performance

**Input**: Design documents from `specs/014-gold-layer-revamp/`
**Branch**: `aqeel`
**Prerequisites**: plan.md ✅ | spec.md ✅ | research.md ✅ | data-model.md ✅ | quickstart.md ✅

**Tests**: Not requested — no test tasks generated.

**Organization**: Tasks grouped by user story for independent implementation and delivery.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no shared dependencies)
- **[Story]**: User story this task delivers
- All file paths are relative to repo root

---

## Phase 1: Foundational (Blocking Prerequisite for US2)

**Purpose**: Add `last_seen` ISO-8601 timestamp to every existing ChromaDB upsert path.
This is a prerequisite for US2 because `evict_corpus` and `augment_from_df` both read and write
`last_seen` metadata. Without it, the eviction query has nothing to filter on.

**⚠️ CRITICAL**: US2 implementation tasks T004–T009 cannot begin until T001 is complete.
US1 (T002–T003) and US3 (T010) are fully independent and can proceed immediately.

- [ ] T001 Add `last_seen` ISO-8601 timestamp to ChromaDB metadata in all existing upsert paths in `src/enrichment/corpus.py`: set `"last_seen": datetime.utcnow().isoformat()` in `add_to_corpus` (line ~363) and `build_seed_corpus` (line ~169 metadatas list). Import `datetime` if not already imported.

**Checkpoint**: T001 complete — US2 corpus tasks can now begin.

---

## Phase 2: User Story 1 — Pipeline Completes Without Crashing (Priority: P1) 🎯 MVP

**Goal**: Pipeline reads Silver Parquet and runs to completion — no `TypeError` at any stage.

**Independent Test**: Run `poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21 --domain nutrition` with `sample.parquet`. Expect no `TypeError: boolean value of NA is ambiguous`, run log `status: success`, rows written to BigQuery.

- [ ] T002 [P] [US1] In `_read_silver_parquet` in `src/pipeline/gold_pipeline.py`, after `df = pd.concat(frames, ignore_index=True)` (line ~100), add: `string_cols = [c for c in df.columns if str(df[c].dtype) == "string"]` then `df[string_cols] = df[string_cols].astype(object)` then `logger.debug("Cast %d StringDtype columns to object: %s", len(string_cols), string_cols)`. This converts Arrow `StringDtype` nulls (`pd.NA`) to `None` before any block runs.

- [ ] T003 [P] [US1] In `src/enrichment/llm_tier.py`, add module-level helper after imports: `def _safe_text(v) -> str:` that returns `""` for `pd.NA`/`None`/`NaN` (use `pd.isna` inside `try/except TypeError`). Replace both `or`-chain crash sites: line ~189 `str(row.get("product_name") or "")` → `_safe_text(row.get("product_name"))` and line ~190 `str(row.get("ingredients") or row.get("description") or "")` → `_safe_text(row.get("ingredients")) or _safe_text(row.get("description"))`. Apply same replacement at lines ~282–283.

**Checkpoint**: T002 + T003 complete — full pipeline run should complete without crashing on any Silver source.

---

## Phase 3: User Story 2 — S2 Enrichment Resolves Rows (Priority: P2)

**Goal**: S2 KNN corpus resolves >0 rows. Corpus is augmented before querying, queries are chunked, corpus grows and evicts predictably across runs.

**Independent Test**: Run sample pipeline. Verify log line `S2 KNN: corpus too sparse ... Augmenting` appears, `S2 KNN: resolved N rows` shows N > 0, run log `corpus_augmented` > 0 and `corpus_size_after` > 1000.

**Depends on**: T001 (foundational `last_seen` schema).

### Corpus changes (sequential — all in `src/enrichment/corpus.py`)

- [ ] T004 [US2] Add `evict_corpus(collection)` function to `src/enrichment/corpus.py`. Read `CORPUS_TTL_DAYS = int(os.environ.get("CORPUS_TTL_DAYS", "90"))` and `MAX_CORPUS_SIZE = int(os.environ.get("MAX_CORPUS_SIZE", "500000"))` as module constants. Function logic: (1) compute `cutoff = (datetime.utcnow() - timedelta(days=CORPUS_TTL_DAYS)).isoformat()`; (2) query ChromaDB for vectors where `last_seen < cutoff`, delete in batches of 500; (3) if `collection.count() > MAX_CORPUS_SIZE`, query all IDs + `last_seen`, sort ascending, delete oldest in batches of 500 until under cap. Wrap entire function in `try/except` — log WARNING on any ChromaDB failure and return without raising.

- [ ] T005 [US2] Add `augment_from_df(df, collection, unresolved_count, force_ratio_threshold=0.25)` function to `src/enrichment/corpus.py`. Logic: (1) if `collection.count() / unresolved_count >= force_ratio_threshold`, log DEBUG and return 0; (2) filter `labeled = df[df["primary_category"].notna()]`; (3) if empty, log WARNING and return 0; (4) encode texts via `_get_model()`, upsert in chunks of 500 with `last_seen = datetime.utcnow().isoformat()` in metadata; (5) log augmentation count before and after; (6) return count of vectors upserted. Wrap ChromaDB calls in `try/except` — log WARNING on failure, return 0.

- [ ] T006 [US2] In `knn_search_batch` in `src/enrichment/corpus.py`, add module constant `CHROMA_QUERY_CHUNK_SIZE = int(os.environ.get("CHROMA_QUERY_CHUNK_SIZE", "500"))`. Replace the single `batch_results = index.query(query_embeddings=embeddings.tolist(), n_results=k_actual)` call with a loop: iterate `embeddings` in slices of `CHROMA_QUERY_CHUNK_SIZE`, collect `all_metadatas` and `all_distances` lists, reconstruct `batch_results = {"metadatas": all_metadatas, "distances": all_distances}`. Per-chunk exception → fill `n` empty lists and log WARNING. Log progress every 10 chunks at INFO level.

### Downstream wiring (parallel — different files)

- [ ] T007 [P] [US2] Update `embedding_enrich` in `src/enrichment/embedding.py`: (1) import `evict_corpus`, `augment_from_df` from `src.enrichment.corpus`; (2) add `MIN_ENRICHMENT_CORPUS = int(os.environ.get("MIN_ENRICHMENT_CORPUS", "1000"))` constant; (3) after `index, metadata = load_corpus()`, call `evict_corpus(index)`; (4) call `augmented = augment_from_df(df, index, len(unresolved_indices))`; (5) after augmentation, if `index.count() < MIN_ENRICHMENT_CORPUS`, log INFO `"S2 KNN: corpus too small after augmentation (%d vectors), skipping to S3"` and return `(df, needs_enrichment, {"resolved": 0, "skipped": "corpus_too_small", "corpus_augmented": augmented, "corpus_size_after": index.count()})`.

- [ ] T008 [P] [US2] Update `LLMEnrichBlock` in `src/blocks/llm_enrich.py`: add `corpus_augmented: int = 0` and `corpus_size_after: int = 0` to `last_enrichment_stats` class-level dict. Capture the return value of the S2 call in `run()` and merge `corpus_augmented` and `corpus_size_after` keys from S2 stats into `last_enrichment_stats`.

- [ ] T009 [P] [US2] Extend `enrichment_stats` dict in `_build_gold_run_log` in `src/pipeline/gold_pipeline.py` with two new keys: `"corpus_augmented": es.get("corpus_augmented", 0)` and `"corpus_size_after": es.get("corpus_size_after", 0)`.

**Checkpoint**: T004–T009 complete — S2 should resolve >0 rows on next full run, corpus evicts stale vectors, run log tracks augmentation.

---

## Phase 4: User Story 3 — Deduplication Completes in Under 1 Hour (Priority: P3)

**Goal**: Dedup blocking key splits large same-prefix blocks across brands, reducing OOM-threshold blocks from 87 to <20.

**Independent Test**: Run dedup on full OFF partition (783k rows). Count of log lines `Block size N >= OOM threshold 2000` must be fewer than 20.

- [ ] T010 [US3] In `src/blocks/fuzzy_deduplicate.py` at line ~119, replace `key = names.iloc[idx][:3].strip()` with:
  ```python
  name_prefix  = names.iloc[idx][:4].strip()
  brand_prefix = brands.iloc[idx][:2].strip()
  key = f"{name_prefix}_{brand_prefix}" if name_prefix else ""
  ```
  Verify `brands` series is already available at this point (computed at line ~88 via `fillna("").astype(str).str.lower()`). The `valid_name_mask` guard at line ~111 is unchanged — rows with empty `product_name` remain excluded from blocking.

**Checkpoint**: T010 complete — run dedup on full partition and verify large-block count drops.

---

## Phase 5: Polish & Cross-Cutting

- [ ] T011 [P] Update `specs/014-gold-layer-revamp/data-model.md` Section 2 (ChromaDB corpus growth model table) to reflect expected post-fix corpus sizes: first full OFF run seeds ~193,896 vectors; subsequent daily runs add small deltas; TTL eviction keeps corpus fresh after 90 days.

- [ ] T012 Run end-to-end validation per `specs/014-gold-layer-revamp/quickstart.md` sample-run scenario: `poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21 --domain nutrition` against `sample.parquet`. Verify all SC-001–SC-006 success criteria are met, all 6 expected log lines appear, and run log JSON contains `corpus_augmented` and `corpus_size_after` keys.

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1 (Foundational T001)
    │
    ├── [unblocks] Phase 3 (US2): T004 → T005 → T006 → T007/T008/T009 (parallel)
    │
Phase 2 (US1): T002/T003 (parallel) ← no dependency on T001
Phase 4 (US3): T010               ← no dependency on T001
    │
    └── Phase 5 (Polish): T011/T012 — after all stories complete
```

### User Story Dependencies

- **US1 (P1)**: Independent — start immediately, no blockers.
- **US2 (P2)**: Blocked on T001 (foundational `last_seen`). Within US2: T004 → T005 → T006 must be sequential (same file); T007, T008, T009 are parallel (different files) and can start after T004 is drafted (interfaces are known).
- **US3 (P3)**: Independent — start immediately, single task.

### Parallel Opportunities

```
Immediately (no blockers):
  T001                      ← foundational, start first
  T002 ‖ T003               ← US1, parallel (different files)
  T010                      ← US3, parallel with everything

After T001:
  T004 → T005 → T006        ← US2 corpus.py, sequential (same file)
  T007 ‖ T008 ‖ T009        ← US2 wiring, parallel once T004 interface known
```

---

## Parallel Example: User Story 2

```bash
# corpus.py changes (sequential — same file):
Task: "T004 — evict_corpus function in src/enrichment/corpus.py"
Task: "T005 — augment_from_df function in src/enrichment/corpus.py"
Task: "T006 — knn_search_batch chunked loop in src/enrichment/corpus.py"

# Wiring changes (parallel — different files, start after T004 interface is clear):
Task: "T007 — embedding_enrich wiring in src/enrichment/embedding.py"
Task: "T008 — LLMEnrichBlock stats in src/blocks/llm_enrich.py"
Task: "T009 — run log enrichment_stats in src/pipeline/gold_pipeline.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete T001 (foundational — quick, ~10 lines)
2. Complete T002 + T003 (US1 — different files, ~20 lines total)
3. **STOP and VALIDATE**: run sample pipeline, confirm no `TypeError`, rows in BigQuery
4. Deploy / share for teammate review

### Incremental Delivery

1. T001 → T002 ‖ T003 → validate US1 → **MVP milestone**
2. T004 → T005 → T006 → T007 ‖ T008 ‖ T009 → validate US2
3. T010 → validate US3 with full partition timing benchmark
4. T011 ‖ T012 → polish

### Single-developer order (recommended)

```
T001 → T002 → T003 → T010 → T004 → T005 → T006 → T007 → T008 → T009 → T011 → T012
```

T010 (blocking key) early because it's a 3-line change with instant validateability.

---

## Notes

- All changes are in-place edits to existing functions — no new files, no new dependencies.
- `corpus.py` receives the most changes (T001, T004, T005, T006). Edit sequentially to avoid conflicts.
- `gold_pipeline.py` receives two independent changes (T002 in `_read_silver_parquet`, T009 in `_build_gold_run_log`) — they touch different functions, can be done in one edit pass.
- After each task, run `poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21 --domain nutrition` on the sample to catch regressions early.
