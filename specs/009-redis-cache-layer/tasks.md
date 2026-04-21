# Tasks: Redis Cache Layer

**Input**: Design documents from `specs/009-redis-cache-layer/`
**Prerequisites**: plan.md ✅ spec.md ✅ research.md ✅ data-model.md ✅ quickstart.md ✅

**Organization**: Tasks grouped by user story — each story independently implementable and testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no blocking dependencies)
- **[Story]**: User story this task belongs to (US1–US4)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Dependency installation and Redis operational configuration.

- [x] T001 Add `redis` to project dependencies via `poetry add redis` in `pyproject.toml`
- [x] T002 Configure Redis RDB snapshot persistence: add `save 3600 1` to `/etc/redis/redis.conf` (or `~/.redis.conf`); update `specs/009-redis-cache-layer/quickstart.md` with `redis-cli config get save` verification step
- [x] T003 [P] Create `src/cache/` module skeleton: `src/cache/__init__.py` (empty), `src/cache/client.py` (empty), `src/cache/stats.py` (empty)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: `CacheClient` and `CacheStats` infrastructure + `PipelineState` additions that every user story depends on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T004 Implement `CacheStats` in `src/cache/stats.py`: `record_hit(prefix)`, `record_miss(prefix)`, `summary() -> dict`, `log_all()` emitting one `INFO` line per prefix (`{prefix}: {hits} hits, {misses} misses ({hit_rate:.1f}%)`)
- [x] T005 Implement `CacheClient` in `src/cache/client.py`: `ConnectionPool(max_connections=10, socket_connect_timeout=1, socket_timeout=1)`; `__init__` pings Redis and sets `_available=False` on failure; `get(prefix, key_input) -> Optional[bytes]`; `set(prefix, key_input, value, ttl) -> bool`; `delete(prefix, key_input) -> bool`; `flush_all_prefixes() -> int` (SCAN+DEL for all known prefixes); `_make_key` hashing to `{prefix}:{sha256_16}`; all ops no-op when `_available=False` or `_no_cache=True`; all ops wrapped in `try/except` logging warnings on `redis.RedisError`
- [x] T006 [P] Export `CacheClient` and `CacheStats` from `src/cache/__init__.py`
- [x] T007 Add two optional fields to `PipelineState` TypedDict in `src/agents/state.py`: `cache_client: Optional[CacheClient]` (import from `src/cache`) and `cache_yaml_hit: bool`

**Checkpoint**: `CacheClient` and `CacheStats` ready; `PipelineState` extended — user story phases can now begin.

---

## Phase 3: User Story 1 — YAML Mapping Cache (Priority: P1) 🎯 MVP

**Goal**: Partitions 2–13 skip Orchestrator + Critic LLM calls entirely, saving ≥2 minutes per partition. Full YAML text cached and re-materialized from Redis on hit.

**Independent Test**: Run pipeline on `part_0000.jsonl`, confirm `yaml:*` key exists in Redis. Run on `part_0001.jsonl`, confirm logs show `Cache HIT: loading YAML mapping from Redis`, zero LLM calls for schema analysis, and output schema matches `part_0000.jsonl` output.

- [x] T008 Add `_compute_schema_fingerprint(source_schema: dict, domain: str, schema_version: str) -> str` helper to `src/agents/orchestrator.py`: hash input = `json.dumps({"cols": sorted(source_schema.keys()), "domain": domain, "schema_version": schema_version})`; return first 16 hex chars of `hashlib.sha256(...).hexdigest()`
- [x] T009 Modify `analyze_schema_node()` in `src/agents/orchestrator.py`: at function start, if `state.get("cache_client")` is not None, compute fingerprint and call `cache_client.get("yaml", fingerprint)`; on hit: JSON-deserialize cached dict, restore `column_mapping`, `operations`, `revised_operations`, `mapping_yaml_path`, `block_sequence`, `enrichment_columns_to_generate`, `enrich_alias_ops` into return dict, re-materialize YAML file to `mapping_yaml_path` if file is missing (write cached YAML text to disk), set `cache_yaml_hit=True`, return early; on miss: run LLM normally, then after `mapping_yaml_path` is written read its text content and call `cache_client.set("yaml", fingerprint, json.dumps(state_subset).encode(), ttl=CACHE_TTL_YAML)`
- [x] T010 Add `route_after_analyze_schema(state: PipelineState) -> str` conditional routing function to `src/agents/graph.py`; register it as the conditional edge after `analyze_schema` node: return `"check_registry"` if `state.get("cache_yaml_hit")` else `"critique_schema"`
- [x] T011 In `src/agents/graph.py:run_pipeline_node()`, extend the local `config` dict to include `"cache_client": state.get("cache_client")` so all blocks have cache access
- [x] T012 Add `argparse` to `demo.py`: `--no-cache` (`store_true`) and `--flush-cache` (`store_true`); construct `cache_client = None if args.no_cache else CacheClient()`; call `cache_client.flush_all_prefixes()` when `args.flush_cache` and `cache_client is not None`; pass `cache_client` in each `graph.invoke({...})` state dict
- [x] T013 [P] Add cache controls to `app.py` Streamlit sidebar: `no_cache = st.checkbox("Bypass cache", key="no_cache")` and `flush = st.button("Flush cache", key="flush_cache")`; initialize `CacheClient` in session state on first load; call `flush_all_prefixes()` when button pressed; pass `None` or client into `graph.invoke` state dict based on checkbox

**Checkpoint**: YAML mapping cache fully functional. Partition 1 warms cache; partitions 2–13 load from Redis.

---

## Phase 4: User Story 2 — LLM Response Cache (Priority: P1)

**Goal**: S3 RAG-LLM enrichment hits Redis before batching; only cache-miss rows sent to LLM. Cache hit rate >50% by partition 3.

**Independent Test**: Run on `part_0000.jsonl`, note S3 row count. Run on `part_0001.jsonl`, confirm logs show `llm: {n} hits, {m} misses` where n > 0. Verify enrichment values for cache-hit rows match original LLM output.

- [x] T014 Add `_compute_content_hash(product_name: str, description: str, enrich_cols: list[str]) -> str` helper to `src/enrichment/llm_tier.py`: hash input = `json.dumps({"name": product_name.strip().lower(), "desc": description.strip(), "cols": sorted(enrich_cols)})`; return first 16 hex chars of SHA-256
- [x] T015 Modify `llm_enrich()` in `src/enrichment/llm_tier.py`: before assembling each 20-row batch, for each row compute content hash and call `cache_client.get("llm", hash)` if `cache_client` is in `config`; separate rows into cache-hits (restore JSON-deserialized `{col: value}` dict directly) and cache-misses (send to LLM as before); after LLM returns results for misses, call `cache_client.set("llm", hash, json.dumps(row_result).encode(), ttl=CACHE_TTL_LLM)` for each miss; merge hit and miss results before returning full batch

**Checkpoint**: LLM enrichment cache active. Cost and latency reduce with each successive partition.

---

## Phase 5: User Story 3 — KNN Embedding Cache (Priority: P2)

**Goal**: `model.encode()` skipped for ≥70% of rows on partition 2+. Cached vectors deserialized from Redis and merged with fresh vectors before FAISS index build.

**Independent Test**: Run on `part_0000.jsonl`, confirm `emb:*` keys in Redis (count ≈ row count). Run on `part_0001.jsonl`, confirm logs show `emb: {n} hits, {m} new encodings` where n ≥ 70% of batch size.

- [x] T016 Add `_compute_embedding_key(model_name: str, text: str) -> str` helper to `src/enrichment/corpus.py`: hash input = `json.dumps({"model": model_name, "text": text})`; return first 16 hex chars of SHA-256
- [x] T017 Modify `batch_search()` in `src/enrichment/corpus.py` (line ~240): before `model.encode(valid_texts, batch_size=64, ...)`, split `valid_texts` into `cached_texts` and `uncached_texts` by checking `cache_client.get("emb", _compute_embedding_key(model_name, text))` for each; for cached: deserialize with `np.frombuffer(raw_bytes, dtype=np.float32).reshape(EMBEDDING_DIM)`; for uncached: encode normally then `cache_client.set("emb", key, vec.tobytes(), ttl=CACHE_TTL_EMB)` for each result; combine cached + fresh arrays to form the full embedding matrix; retrieve `cache_client` from the `EmbeddingCorpus` constructor argument or a module-level accessor (inject via `corpus.py:EmbeddingCorpus.set_cache_client(client)` called from `run_pipeline_node`)

**Checkpoint**: Embedding cache active. CPU encode time drops ≥70% on partition 2+.

---

## Phase 6: User Story 4 — Dedup Signature Cache (Priority: P3)

**Goal**: Products previously assigned to a cluster are pre-assigned from Redis. Cross-partition cluster ID consistency guaranteed. Fully-cached partitions complete dedup in <1 second.

**Independent Test**: Run on `part_0000.jsonl`, confirm `dedup:*` keys in Redis. Run on `part_0001.jsonl`, confirm products appearing in both partitions have identical cluster IDs. Confirm logs show `dedup: {n} hits`.

- [x] T018 Add `_normalize_name(name: str) -> str` (lowercase + strip + remove noise words) and `_compute_dedup_key(normalized_name: str) -> str` (SHA-256-16 of normalized name) helpers to `src/blocks/dedup/fuzzy_deduplicate.py`
- [x] T019 Modify fuzzy dedup block in `src/blocks/dedup/fuzzy_deduplicate.py`: before running similarity computation, for each row compute dedup key and check `cache_client.get("dedup", key)` (get `cache_client` from `config` dict); pre-assign cached cluster IDs; run TF-IDF / minhash similarity only on uncached rows; after cluster assignment `cache_client.set("dedup", key, json.dumps({"cluster_id": cluster_id}).encode(), ttl=CACHE_TTL_DEDUP)` for each new assignment; if all rows are cache hits, skip similarity computation entirely

**Checkpoint**: All four cache layers active. Full 13-partition USDA ingest now uses cache throughout.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [x] T020 [P] Add `CacheStats.log_all()` call at pipeline run completion in `src/agents/graph.py:save_output_node` (or at end of `run_pipeline_node`): read `state.get("cache_client")` and call `.get_stats().log_all()` to satisfy FR-005
- [x] T021 [P] Write integration test in `tests/integration/test_cache_pipeline.py` covering SC-004: two sequential calls to `graph.invoke()` with the same source; assert second call produces zero `analyze_schema` LLM calls (mock `call_llm_json`, assert not called on second invoke); assert output DataFrames are identical
- [x] T022 [P] Write unit tests in `tests/unit/test_cache_client.py`: `CacheClient` graceful degradation on `ConnectionError`; no-op behavior when `_no_cache=True`; `flush_all_prefixes()` returns correct delete count; `_make_key` produces consistent `{prefix}:{sha256_16}` format
- [x] T023 Update `specs/009-redis-cache-layer/quickstart.md`: add YAML text re-materialization behavior explanation to "Verifying cache behavior" section; add `redis-cli config get save` step to Prerequisites section
- [ ] T024 Validate SC-004 manually: run `demo.py`, run `demo.py --no-cache`, diff output CSVs; assert identical row counts, enrichment values, and cluster IDs

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 (T001–T003) — **BLOCKS all user stories**
- **Phase 3 (US1 YAML)**: Depends on Phase 2 completion — `CacheClient`, `CacheStats`, `PipelineState` must exist
- **Phase 4 (US2 LLM)**: Depends on Phase 2 + T011 (cache_client in block config dict)
- **Phase 5 (US3 Embedding)**: Depends on Phase 2 + T011
- **Phase 6 (US4 Dedup)**: Depends on Phase 2 + T011
- **Phase 7 (Polish)**: Depends on all desired user story phases complete

### User Story Dependencies

- **US1 (P1)**: Start after Phase 2 — no dependency on US2/3/4
- **US2 (P1)**: Start after Phase 2 + T011 — no dependency on US1/3/4
- **US3 (P2)**: Start after Phase 2 + T011 — no dependency on US1/2/4
- **US4 (P3)**: Start after Phase 2 + T011 — no dependency on US1/2/3

### Within Each User Story

- Phase 3: T008 → T009 (fingerprint helper before cache check/set) → T010, T011 (parallel graph changes) → T012, T013 (parallel CLI/UI)
- Phase 4: T014 → T015 (hash helper before enrichment modification)
- Phase 5: T016 → T017 (key helper before corpus modification)
- Phase 6: T018 → T019 (normalize helper before dedup modification)

---

## Parallel Example: US1 (after T007 and T011 complete)

```bash
# After T008 + T009 (YAML cache in orchestrator.py):
Task: "T010 — route_after_analyze_schema edge in graph.py"
Task: "T012 — argparse in demo.py"     # different file, parallel
Task: "T013 — sidebar toggle in app.py"  # different file, parallel
```

## Parallel Example: US2 + US3 (after Phase 2 + T011 complete)

```bash
# US2 and US3 touch different files — fully parallel:
Task: "T014 + T015 — llm_tier.py LLM cache"
Task: "T016 + T017 — corpus.py embedding cache"
```

---

## Implementation Strategy

### MVP First (US1 only — highest ROI)

1. Phase 1: Setup (T001–T003)
2. Phase 2: Foundational (T004–T007)
3. Phase 3: US1 YAML Cache (T008–T013)
4. **STOP and VALIDATE**: Run partitions 0 and 1, confirm YAML cache hit, log output
5. Delivers ~30 minutes saved per 13-partition USDA ingest

### Incremental Delivery

1. Setup + Foundational → `CacheClient` ready
2. US1 → YAML cache active → MVP (biggest time saving)
3. US2 → LLM enrichment cache → cost + latency reduction
4. US3 → Embedding cache → CPU reduction
5. US4 → Dedup cache → cross-partition consistency
6. Polish → validation, stats logging, tests

---

## Notes

- `[P]` tasks touch different files — safe to run concurrently
- `[US#]` traces each task to its acceptance criteria in spec.md
- `cache_client=None` is the no-cache sentinel — no other conditional needed
- T011 is the critical unlock for US2, US3, US4 (cache_client flows into block config)
- T017 requires knowing embedding dim at runtime — read from `model.get_sentence_embedding_dimension()` rather than hardcoding 384
