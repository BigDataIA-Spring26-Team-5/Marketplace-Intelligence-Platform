# Research: Gold Layer Pipeline

**Created**: 2026-04-21

---

## 1. Existing Dedup Block Interfaces

### Decision
Reuse existing blocks without modification.

### Findings

**FuzzyDeduplicateBlock** (`src/blocks/fuzzy_deduplicate.py`):
- Input: DataFrame with `product_name`, `brand_owner`, `brand_name`
- Output: DataFrame with `duplicate_group_id`, `canonical` columns added
- Config: `threshold` (default 85), `blocking_key_fn` (default first 3 chars)
- Already uses rapidfuzz token_sort_ratio
- Already implements Union-Find clustering

**ColumnWiseMergeBlock** (`src/blocks/column_wise_merge.py`):
- Input: DataFrame with `duplicate_group_id`
- Output: Merged DataFrame (one row per group)
- String: longest non-null
- Numeric: first non-null
- `data_source`: comma-joined unique values

**GoldenRecordSelectBlock** (`src/blocks/golden_record_select.py`):
- Input: Merged DataFrame with clusters
- Output: Single golden record per cluster
- Uses weighted composite score (completeness, freshness, richness)

### Rationale
Blocks already implement spec requirements. Only orchestration layer needed.

---

## 2. Batch FAISS Patterns

### Decision
Build FAISS IndexFlatIP once, batch query.

### Findings

Pattern from existing `src/enrichment/embedding.py`:
```python
# Current: per-record loop
for idx, row in df.iterrows():
    embedding = model.encode(text)
    neighbors = index.search(embedding, k)
```

Required change for Gold:
```python
# Batch: embed all, query in batch
texts = df['product_name'] + ' ' + df['brand_owner'].fillna('')
embeddings = model.encode(texts.tolist(), batch_size=256)
distances, indices = index.search(embeddings, k)
```

Embedding model: `all-MiniLM-L6-v2` (384-dim, fast)
Index type: `IndexFlatIP` (inner product = cosine on L2-normalized)
Expected index size: ~1M vectors × 384 dims × 4 bytes = ~1.5GB

### Rationale
Batch embedding 10-100x faster than per-record. FAISS batch query also vectorized.

---

## 3. SQLite Cache Schema

### Decision
Simple key-value with hash key.

### Schema

```sql
CREATE TABLE llm_cache (
    cache_key TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_created ON llm_cache(created_at);
```

Cache key: `sha256(product_name + brand_owner + missing_fields_json)`

### Interface

```python
class SQLiteCache:
    def get(self, key: str) -> dict | None
    def set(self, key: str, response: dict) -> None
    def stats(self) -> dict  # hits, misses, size
```

### Migration path
When Redis available (Spec 009), swap `SQLiteCache` for `RedisCache` with same interface. Environment variable `GOLD_CACHE_BACKEND=sqlite|redis`.

### Rationale
SQLite is zero-dependency, file-based, concurrent-read safe. Good fallback.

---

## 4. Existing Enrichment Integration

### Decision
Gold enrichment tiers are separate from existing UC1 `LLMEnrichBlock`.

### Findings

Existing `src/blocks/llm_enrich.py`:
- Designed for single-source row-by-row enrichment
- Embedded in UC1 block sequence
- Uses corpus for S2

Gold enrichment:
- Operates on post-dedup golden records only
- Batch operations
- Separate files under `src/pipeline/gold/enrichment/`
- May share corpus with UC1 (read-only)

### Rationale
Gold is a distinct pipeline. Shared corpus is ok, but enrichment logic is Gold-specific.

---

## Alternatives Considered

| Area | Alternative | Why Rejected |
|------|-------------|--------------|
| Dedup blocks | Write new Gold-specific blocks | Existing blocks already work; unnecessary code |
| S2 embeddings | OpenAI embeddings | Cost, latency; MiniLM is free and fast enough |
| Cache | shelve | Corruption-prone, no concurrent access |
| Cache | In-memory dict | Lost on restart, no reproducibility |
