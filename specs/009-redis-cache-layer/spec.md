# Feature Specification: Redis Cache Layer for Pipeline Optimization

**Feature Branch**: `009-redis-cache-layer`  
**Created**: 2026-04-21  
**Status**: Draft  
**Input**: User description: "Implement Redis-backed caching across 4 pipeline layers — LLM response cache, KNN embedding cache, YAML mapping cache, and dedup signature cache — to eliminate redundant compute across partitions and pipeline runs."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — YAML Mapping Cache (Priority: P1)

When the Orchestrator + Critic produce a YAML mapping for a source schema (e.g., USDA `part_0000.jsonl`), that mapping is cached in Redis keyed by a fingerprint of the source schema's sorted column names + domain. All subsequent partitions (`part_0001` through `part_0012`) that share the same schema skip both the Orchestrator LLM call and the 2m24s Critic (`deepseek-reasoner`) call entirely, loading the cached YAML instead.

**Why this priority**: The Critic is the single most expensive per-run fixed cost — 2m24s of `deepseek-reasoner` time. All 13 USDA partitions share the same schema, so partitions 2–13 save ~2.5 minutes each. This is ~30 minutes saved per full USDA ingest with zero quality risk since the schema-to-mapping relationship is deterministic.

**Independent Test**: Run the pipeline against `part_0000.jsonl`, confirm YAML is written to Redis. Run against `part_0001.jsonl`, confirm Orchestrator and Critic are skipped (no LLM calls in logs), and the cached YAML is loaded. Verify the output schema matches between both runs.

**Acceptance Scenarios**:

1. **Given** a USDA partition has never been processed and no cache entry exists for its schema fingerprint, **When** the pipeline runs, **Then** Orchestrator + Critic execute normally, produce YAML, and the full YAML text is stored in Redis under key `yaml:{sha256(sorted_column_names + domain)}` with a configurable TTL (default: 30 days).
2. **Given** a cache entry exists for the schema fingerprint, **When** a new partition with the same schema is processed, **Then** the pipeline logs `Cache HIT: loading YAML mapping from Redis`, skips Agent 1 (Orchestrator) schema analysis and Agent 2 (Critic) correction, loads the cached YAML text from Redis, materializes it to disk if the generated file is missing, and loads it into the BlockRegistry.
3. **Given** a cached YAML exists but the source schema has changed (e.g., USDA adds a new column), **When** the pipeline runs, **Then** the fingerprint mismatches, cache is missed, and the Orchestrator + Critic run normally producing a fresh YAML.

---

### User Story 2 — LLM Response Cache (Priority: P1)

Individual product rows that pass through S3 RAG-LLM enrichment have their enrichment results cached in Redis, keyed by a hash of the product's identifying text (product name + description). Subsequent partitions or re-runs that encounter the same product retrieve cached enrichment results instead of making LLM calls.

**Why this priority**: S3 RAG-LLM enrichment is the throughput bottleneck — 922 rows at batch_size=20 means ~46 sequential LLM round-trips per partition. Across 13 partitions with significant product overlap, caching resolves the majority of S3 rows after the first 2–3 partitions. This directly reduces both wall-clock time and LLM API cost.

**Independent Test**: Run the pipeline on `part_0000.jsonl` and note the S3 row count. Run on `part_0001.jsonl` and confirm logs show `S3 cache HIT: {n} rows resolved from Redis, {m} rows sent to LLM`. Verify the cached enrichment values (e.g., `primary_category`, `dietary_tags`) match the original LLM output.

**Acceptance Scenarios**:

1. **Given** a product row reaches S3 enrichment and no cache entry exists for its content hash, **When** the LLM returns enrichment results, **Then** the results are stored in Redis under key `llm:enrich:{sha256(product_name + description)}` with a configurable TTL (default: 7 days), and the row is processed normally.
2. **Given** a cache entry exists for a product's content hash, **When** that product appears in a subsequent partition or re-run, **Then** the cached enrichment values are applied directly, the row is excluded from the LLM batch, and the log reports it as a cache hit.
3. **Given** a batch of 20 rows is assembled for S3, **When** 14 rows have cache hits and 6 are misses, **Then** only the 6 misses are sent to the LLM in a single batch call, and all 20 rows are returned with correct enrichment values.

---

### User Story 3 — KNN Embedding Cache (Priority: P2)

Sentence-transformer embeddings computed for product text during S2 KNN enrichment are cached in Redis. On subsequent runs, embeddings are loaded from cache instead of re-encoding on CPU, and only genuinely new product text triggers `model.encode()`.

**Why this priority**: Embedding 4,305 rows on CPU takes ~20–25 seconds per partition. Caching eliminates redundant encoding across partitions and re-runs. Lower priority than P1 stories because the absolute time savings per partition is smaller (~20s vs minutes), but it compounds across all 13 partitions and reduces CPU load on the VM.

**Independent Test**: Run the pipeline on `part_0000.jsonl`, confirm embeddings are written to Redis. Run on `part_0001.jsonl`, confirm logs show `Embedding cache: {n} hits, {m} new encodings`. Verify the FAISS index built from cached embeddings produces identical KNN matches to a fresh encoding.

**Acceptance Scenarios**:

1. **Given** a product text has not been embedded before, **When** `model.encode()` produces a 384-dim vector, **Then** the vector is stored in Redis under key `emb:all-MiniLM-L6-v2:{sha256(text)}` as a serialized byte array with a configurable TTL (default: 30 days).
2. **Given** a batch of 4,305 rows needs embedding, **When** 3,100 have cache hits, **Then** only 1,205 rows are passed to `model.encode()`, cached vectors are deserialized and combined with fresh vectors, and the full set is used to build the FAISS index.
3. **Given** the embedding model is changed (e.g., from `all-MiniLM-L6-v2` to `bge-small-en`), **When** the pipeline runs, **Then** all cache keys miss (model name is part of the key prefix), and fresh embeddings are computed and cached under the new model prefix.

---

### User Story 4 — Dedup Signature Cache (Priority: P3)

Fuzzy deduplication signatures (TF-IDF vectors or minhash signatures) and cluster assignments computed during `fuzzy_deduplicate` are cached in Redis. Products already assigned to a cluster in a prior partition can be pre-assigned without recomputing similarity.

**Why this priority**: Dedup currently takes ~4 seconds per 10K-row partition, which is relatively fast. The value here is cross-partition consistency (same product always lands in the same cluster) rather than raw speed. Lowest priority because the time savings is marginal compared to the LLM and embedding caches.

**Independent Test**: Run on `part_0000.jsonl`, confirm cluster assignments are cached. Run on `part_0001.jsonl`, confirm products that appeared in both partitions are assigned to the same cluster IDs. Verify the dedup rate is consistent.

**Acceptance Scenarios**:

1. **Given** a product's normalized name has been processed before and assigned to cluster #586, **When** the same product appears in a new partition, **Then** it is pre-assigned to cluster #586 from cache, and the fuzzy matching only runs on uncached rows.
2. **Given** all rows in a partition have cache hits, **When** dedup runs, **Then** no similarity computation occurs, cluster assignments are loaded entirely from Redis, and the block completes in <1 second.

---

### Edge Cases

- What happens when Redis is unavailable (connection refused, timeout)? Pipeline must fall back to full computation with a warning log, never fail hard on cache miss.
- What happens when a cached LLM response has a different schema than the current enrichment config (e.g., new enrichment columns added)? Cache miss — the key should incorporate the list of enrichment target columns.
- What happens when Redis memory is exhausted? Rely on Redis's own eviction policy (LRU recommended). Pipeline should handle `OOM` errors from `SET` gracefully — log a warning, continue without caching.
- What happens when the unified schema changes (e.g., a new column is added)? YAML mapping cache must invalidate — include the unified schema hash in the cache key.
- What happens during concurrent pipeline runs on different partitions? Redis is single-threaded and atomic for individual GET/SET — no race conditions. Last-write-wins is acceptable since identical inputs produce identical outputs.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a `CacheClient` wrapper class with `get(prefix, key_input)`, `set(prefix, key_input, value, ttl)`, and `delete(prefix, key_input)` methods that abstract Redis operations behind a consistent interface.
- **FR-002**: System MUST use SHA-256 hashing for all cache keys and enforce the key format `{prefix}:{hash}` (e.g., `llm:enrich:a3f2...`, `yaml:b7c1...`).
- **FR-003**: System MUST support configurable TTLs per cache layer — YAML (default 30 days), LLM responses (default 7 days), embeddings (default 30 days), dedup signatures (default 14 days).
- **FR-004**: System MUST gracefully degrade when Redis is unavailable — all cache operations wrapped in try/except, pipeline continues with full computation on any Redis error, warning logged.
- **FR-005**: System MUST log cache hit/miss statistics per block per run — `{prefix}: {hits} hits, {misses} misses, {hit_rate}% hit rate`.
- **FR-006**: System MUST serialize embedding vectors as msgpack byte arrays (not JSON) to minimize storage and deserialization overhead.
- **FR-007**: System MUST include the enrichment column list in the LLM cache key hash to auto-invalidate when enrichment targets change.
- **FR-008**: System MUST include the unified schema version/hash in the YAML mapping cache key to auto-invalidate when the schema evolves.
- **FR-009**: System MUST support a `--no-cache` CLI flag that bypasses all Redis lookups and writes, forcing full recomputation for debugging/validation.
- **FR-010**: System MUST support a `--flush-cache` CLI flag that clears all pipeline cache keys from Redis before execution.
- **FR-011**: The `CacheClient` MUST be injectable via the pipeline's existing config/context mechanism, not imported as a global singleton.
- **FR-012**: System MUST cache the full YAML mapping text (not merely the file path) so that a YAML cache hit remains valid even if the generated mapping file has been deleted from disk. On a cache hit, the system MUST re-materialize the YAML file to its expected path before loading it into the BlockRegistry.

### Pipeline Governance Constraints

- This feature does NOT change `config/unified_schema.json` or downstream required columns.
- This feature does NOT change YAML mapping behavior — it only caches the output of existing YAML generation.
- No HITL review points are affected — caching is transparent to the operator. The `--no-cache` flag serves as the operator override.
- Enrichment behavior is unchanged — cached LLM responses return the same values the LLM would have produced. `primary_category` and safety fields are cached identically; no probabilistic inference is introduced.
- Cache invalidation is deterministic — schema changes, enrichment config changes, and model changes all produce different cache keys automatically.

### Key Entities

- **CacheClient**: Wrapper around `redis.Redis` providing typed get/set/delete with prefix-based namespacing, SHA-256 key hashing, TTL management, and graceful degradation.
- **CacheStats**: Per-prefix hit/miss counters accumulated during a pipeline run, logged at run completion.
- **Schema Fingerprint**: SHA-256 hash of sorted source column names + domain string + unified schema version, used as the YAML cache key. The cache value is the full YAML mapping text (not a file path).
- **Content Hash**: SHA-256 hash of product identifying text (name + description) + enrichment column list, used as the LLM response cache key.
- **Embedding Key**: SHA-256 hash of model name + input text, used as the embedding vector cache key.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Partitions 2–13 of USDA ingest skip Orchestrator + Critic entirely (0 LLM calls for schema analysis), saving ≥2 minutes per partition.
- **SC-002**: S3 RAG-LLM enrichment cache hit rate exceeds 50% by partition 3 and 75% by partition 6, reported in per-run cache statistics logs.
- **SC-003**: Total wall-clock time for a full 13-partition USDA ingest is reduced by ≥60% compared to the uncached baseline.
- **SC-004**: Pipeline produces identical output (same row counts, same enrichment values, same dedup clusters) with and without cache — verified by running `--no-cache` and diffing output DataFrames.
- **SC-005**: Redis failure (simulated by stopping the Redis service mid-run) does not crash the pipeline — run completes with full computation and warning logs.
- **SC-006**: Embedding computation is skipped for ≥70% of rows on partition 2+, compared to partition 1 where all rows are freshly computed.

## Assumptions

- Redis is installed and running on the GCP VM (localhost:6379), or can be installed as part of this feature's setup.
- Redis is configured with RDB snapshot persistence (`save 3600 1`) so cache entries survive VM restarts. Memory-only mode is not relied upon — cross-run cache availability depends on persistence being active.
- The VM has sufficient memory for Redis to hold the cache (~500MB estimated for 13 partitions × 10K rows of embeddings + LLM responses).
- `redis-py` is compatible with the project's Python 3.11 + Poetry environment.
- Product overlap across USDA partitions is ≥30% (validated by the 37.8% duplicate rate observed in bronze data exploration).
- The existing `litellm` LLM calls in `llm_tier.py` are deterministic for the same input (temperature=0 or near-zero) — if not, the LLM cache will return the first-seen result, which is acceptable for classification tasks.
- `msgpack` or `pickle` is available for embedding vector serialization. `msgpack` is preferred for portability; `pickle` is acceptable as fallback.
- The pipeline's existing `PipelineConfig` or run context can carry a `CacheClient` instance without architectural changes to the runner or graph.

## Clarifications

### Session 2026-04-21

- Q: Should Redis be configured with persistence so cache survives VM restarts? → A: RDB snapshot (`save 3600 1`) — hourly snapshot, survives restarts, minimal overhead.
- Q: Should the YAML mapping cache store full YAML text or just the file path? → A: Full YAML text — cache hit valid even if generated file is deleted; re-materialize to disk on hit.