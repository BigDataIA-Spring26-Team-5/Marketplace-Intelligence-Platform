# Data Model: Redis Cache Layer

**Branch**: `009-redis-cache-layer` | **Date**: 2026-04-21

## Entities

### CacheClient (`src/cache/client.py`)

Central access object for all cache operations. One instance per pipeline run, injected via `PipelineState`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `_pool` | `redis.ConnectionPool` | Shared connection pool, `max_connections=10` |
| `_available` | `bool` | Set `False` on first connection failure; short-circuits all ops |
| `_stats` | `CacheStats` | Hit/miss accumulator for this run |
| `_no_cache` | `bool` | When `True`, all `get()` return `None`, all `set()` are no-ops |

**Methods**:
- `get(prefix: str, key_input: str | list) -> Optional[bytes]` — compute key, GET from Redis, return raw bytes or None
- `set(prefix: str, key_input: str | list, value: bytes, ttl: int) -> bool` — SET with EX; swallow errors
- `delete(prefix: str, key_input: str | list) -> bool`
- `flush_all_prefixes() -> int` — SCAN+DEL for all known prefixes; returns count deleted
- `get_stats() -> CacheStats` — return accumulated stats for this run
- `_make_key(prefix: str, key_input: str | list) -> str` — `f"{prefix}:{sha256_16(key_input)}"`

**State transitions**:
- `_available: True` → `False` on `redis.ConnectionError` or `redis.TimeoutError`; stays `False` for remainder of run (no retry)
- Never transitions back to `True` within a run

---

### CacheStats (`src/cache/stats.py`)

Immutable per-prefix counters accumulated during one pipeline run. Logged at run completion.

| Attribute | Type | Description |
|-----------|------|-------------|
| `_counts` | `dict[str, dict]` | `{prefix: {hits: int, misses: int}}` |

**Methods**:
- `record_hit(prefix: str)` — increment `_counts[prefix]["hits"]`
- `record_miss(prefix: str)` — increment `_counts[prefix]["misses"]`
- `summary() -> dict` — return `{prefix: {hits, misses, hit_rate_pct}}` for all prefixes
- `log_all()` — emit one `INFO` log line per prefix: `{prefix}: {hits} hits, {misses} misses ({hit_rate:.1f}%)`

---

## Value Objects (Cache Keys)

These are not stored objects — they describe the hashing contracts for each prefix.

### SchemaFingerprint (prefix: `yaml`)

Used as YAML mapping cache key in `analyze_schema_node`.

| Input | Type | Notes |
|-------|------|-------|
| `source_columns` | `list[str]` | sorted alphabetically before hashing |
| `domain` | `str` | `"nutrition"`, `"safety"`, `"pricing"` |
| `schema_version` | `str` | from `config/unified_schema.json` metadata field |

**Hash input**: `json.dumps({"cols": sorted(source_columns), "domain": domain, "schema_version": schema_version})`

**Cache value**: JSON-serialized dict of state fields to restore (see research.md Decision 5).
**TTL**: 30 days (configurable)

---

### ContentHash (prefix: `llm`)

Used as LLM enrichment cache key in `llm_tier.py`.

| Input | Type | Notes |
|-------|------|-------|
| `product_name` | `str` | stripped, lowercased before hashing |
| `description` | `str` | stripped |
| `enrich_cols` | `list[str]` | sorted alphabetically |

**Hash input**: `json.dumps({"name": product_name, "desc": description, "cols": sorted(enrich_cols)})`

**Cache value**: JSON-serialized dict `{col: value}` for all `enrich_cols`.
**TTL**: 7 days (configurable)

---

### EmbeddingKey (prefix: `emb`)

Used as embedding vector cache key in `corpus.py`.

| Input | Type | Notes |
|-------|------|-------|
| `model_name` | `str` | e.g., `"all-MiniLM-L6-v2"` |
| `text` | `str` | raw product text before encoding |

**Hash input**: `json.dumps({"model": model_name, "text": text})`

**Cache value**: raw bytes from `numpy.ndarray.tobytes()` (float32, shape `(384,)`).
**TTL**: 30 days (configurable)

---

### DedupKey (prefix: `dedup`)

Used as cluster assignment cache key in `fuzzy_deduplicate.py`.

| Input | Type | Notes |
|-------|------|-------|
| `normalized_name` | `str` | product name after lowercase + strip + noise removal |

**Hash input**: `normalized_name`

**Cache value**: JSON-serialized `{"cluster_id": int}`.
**TTL**: 14 days (configurable)

---

## PipelineState additions (`src/agents/state.py`)

Two new optional fields added to the existing `PipelineState` TypedDict:

| Field | Type | Set by | Read by |
|-------|------|--------|---------|
| `cache_client` | `Optional[CacheClient]` | `demo.py` / `app.py` | `analyze_schema_node`, `run_pipeline_node` (→ block config), direct node access |
| `cache_yaml_hit` | `bool` | `analyze_schema_node` | `route_after_analyze_schema` conditional edge |

---

## TTL Configuration

Default TTLs (in seconds) defined as module-level constants in `src/cache/client.py`. Can be overridden via environment variables.

| Prefix | Default TTL | Env Var Override |
|--------|-------------|-----------------|
| `yaml` | 2,592,000 (30 days) | `CACHE_TTL_YAML` |
| `llm` | 604,800 (7 days) | `CACHE_TTL_LLM` |
| `emb` | 2,592,000 (30 days) | `CACHE_TTL_EMB` |
| `dedup` | 1,209,600 (14 days) | `CACHE_TTL_DEDUP` |
