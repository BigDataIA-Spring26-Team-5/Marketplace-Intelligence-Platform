# Research: Redis Cache Layer

**Branch**: `009-redis-cache-layer` | **Date**: 2026-04-21

## Decision 1: Redis Connection Strategy

**Decision**: Use `redis.ConnectionPool` with 1-second connect + read timeouts. Set `max_connections=10`. `CacheClient` holds the pool; `Redis.from_pool()` for each operation.

**Rationale**: Connection pools avoid repeated TCP handshakes across block runs. 1-second timeouts allow fast failure — if Redis is down, the pipeline notices within 1s and sets `_available = False`, short-circuiting all subsequent ops without re-attempting.

**Alternatives considered**:
- Single `redis.Redis()` per operation — creates new connection each time, defeats purpose
- No timeout — hangs pipeline indefinitely if Redis is unresponsive
- `fakeredis` for testing — will use in unit tests, not production

**Implementation note**: `CacheClient.__init__` pings Redis on construction. If ping fails, `_available = False` immediately. All `get()`/`set()` methods are no-ops when `_available = False`.

---

## Decision 2: Embedding Vector Serialization

**Decision**: `numpy.ndarray.tobytes()` for serialization; `numpy.frombuffer(..., dtype=np.float32)` for deserialization. No external dependency.

**Rationale**: The embeddings are `float32` numpy arrays of shape `(384,)` (all-MiniLM-L6-v2). `tobytes()` produces exactly 1,536 bytes per vector — the most compact possible representation. Deserialization is a single `frombuffer` call, ~10µs per vector. No msgpack install needed; no dtype ambiguity.

**Alternatives considered**:
- `msgpack` + `msgpack-numpy` — adds install dependency; `tobytes()` is equivalent size and faster
- `pickle` — larger overhead, version-sensitive, security concern for untrusted data
- JSON of float list — 10× larger, slow to parse

**Implementation note**: Store raw bytes under key `emb:{model_name_slug}:{sha256_16}`. On read, call `np.frombuffer(value, dtype=np.float32).reshape(384)` (or whatever the model's dim is — read from model at runtime).

---

## Decision 3: Cache Key Format and Truncation

**Decision**: `{prefix}:{sha256_16}` where `sha256_16` is the first 16 hex chars (64 bits) of SHA-256. Keys never exceed ~40 characters.

**Rationale**: 64-bit collision probability at 13K items is ~1 in 10¹⁴. For a 13-partition ingest of 10K rows, this is negligible. Short keys reduce Redis memory overhead and improve scan performance. Using a consistent prefix enables `SCAN MATCH {prefix}:*` for `--flush-cache`.

**Alternatives considered**:
- Full 64-char SHA-256 — collision-proof but wastes memory in key namespace
- MD5 — faster but cryptographically weaker; SHA-256 is fine since we use hashlib (no perf difference at Python level)

**Key map**:
| Cache Layer | Prefix | Hash Input |
|-------------|--------|------------|
| YAML mapping | `yaml` | sorted(source_cols) + domain + schema_version |
| LLM enrichment | `llm` | product_name + description + sorted(enrich_cols) |
| Embedding | `emb` | model_name_slug + text |
| Dedup cluster | `dedup` | normalized_product_name |

---

## Decision 4: CacheClient Injection Pattern

**Decision**: Add `cache_client: Optional[CacheClient]` to `PipelineState` (TypedDict, `total=False`). In `graph.py:run_pipeline_node`, pass `state.get("cache_client")` into the block `config` dict. `analyze_schema_node` reads it directly from state.

**Rationale**: `PipelineState` already flows through every node — it's the natural carrier. Adding one optional field is minimal friction. The block `config` dict is already passed to all blocks; adding `cache_client` there makes it available to `llm_enrich`, `fuzzy_deduplicate`, etc. without changing block signatures.

**Alternatives considered**:
- Global singleton `CacheClient` — spec explicitly prohibits; harder to test; not compatible with `--no-cache`
- New `PipelineConfig` dataclass — overcomplicated; `PipelineState` already serves this purpose
- Thread-local / context var — appropriate for async frameworks, not LangGraph's sync pattern

**`--no-cache` implementation**: Pass `cache_client=None` in state. All cache check sites guard with `if cache_client is None: return None`. No other changes needed — `None` behaves identically to a cold cache.

**`--flush-cache` implementation**: After `CacheClient` construction in `demo.py`/`app.py`, call `cache_client.flush_all_prefixes()` before `graph.invoke()`. This runs `SCAN MATCH {p}:* + DEL` for each prefix.

---

## Decision 5: YAML Cache Bypass in the Graph

**Decision**: Modify `analyze_schema_node` to check YAML cache at the start. On hit, populate all state fields from cache and set `cache_yaml_hit = True`. Add new conditional edge `route_after_analyze_schema` that routes to `check_registry` (skipping `critique_schema`) when `cache_yaml_hit` is True.

**Rationale**: This is the minimum graph change that correctly skips both Agent 1 LLM call AND Agent 2 (Critic). The state fields normally produced by both nodes (`column_mapping`, `operations`, `revised_operations`, `mapping_yaml_path`, etc.) are restored from cache, so `check_registry` and `plan_sequence` see a complete state identical to a fresh run.

**What gets cached** (JSON-serialized dict):
- `column_mapping` (dict)
- `operations` (list of dicts — Agent 1 output)
- `revised_operations` (list of dicts — Agent 2 output)
- `mapping_yaml_path` (str)
- `block_sequence` (list of str — Agent 3 output, also cached since sequence is deterministic for a given schema)
- `enrichment_columns_to_generate` (list of str)
- `enrich_alias_ops` (list of dicts)

**What does NOT get cached**: Row-level state (`working_df`, `source_df`, DQ scores) — these are per-run data, not reusable.

**Graph change**:
```
Before: analyze_schema → critique_schema → check_registry → plan_sequence → run_pipeline
After:  analyze_schema → [route_after_analyze_schema] → critique_schema (cache miss path)
                                                       → check_registry  (cache hit path)
```

---

## Decision 6: LLM Enrichment Cache Integration

**Decision**: Wrap the per-batch LLM call in `llm_tier.py:llm_enrich()`. Before assembling each batch of 20 rows, check each row's content hash. Separate into cache-hits (restore directly) and cache-misses (send to LLM). After LLM returns, cache each miss result. Return merged 20-row result.

**Key insight**: The content hash must include `sorted(enrich_cols)` to auto-invalidate when enrichment targets change (FR-007). This means adding the `enrich_cols` list to the hash input — `llm_enrich()` already receives `enrich_cols` as a parameter.

**Cache value**: JSON-serialized dict `{col: value for col in enrich_cols}` for one row.

---

## Decision 7: Embedding Cache Integration

**Decision**: Integrate in `corpus.py:batch_search()` (the most-used path, line 240). Before calling `model.encode(valid_texts, ...)`, split texts into cached (load bytes from Redis) and uncached (encode). Combine arrays before FAISS search.

**Where to NOT cache**: `seed_corpus()` (line 122) — this runs once from USDA FoodData Central, not per-partition. Caching its embeddings would just duplicate the FAISS index in Redis.

---

## Decision 8: demo.py and app.py Integration

**`demo.py`**: Add `argparse` with `--no-cache` (store_true) and `--flush-cache` (store_true). Initialize `CacheClient` at top of `main()`. Pass via state dict.

```python
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--no-cache", action="store_true")
parser.add_argument("--flush-cache", action="store_true")
args = parser.parse_args()
cache_client = None if args.no_cache else CacheClient()
if args.flush_cache and cache_client:
    cache_client.flush_all_prefixes()
```

**`app.py`**: Add sidebar checkbox `"Bypass cache (--no-cache)"` and button `"Flush cache"`. Store in `st.session_state`. Pass to state dict at graph invocation.

## All NEEDS CLARIFICATION Items

None — all decisions above resolve every ambiguity in the spec without requiring user input.
