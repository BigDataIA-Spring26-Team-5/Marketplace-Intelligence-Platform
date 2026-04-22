# Research: Gold Layer Revamp — Reliability & Performance

**Created**: 2026-04-22
**Feature**: [spec.md](spec.md) | **Plan**: [impl-plan.md](impl-plan.md)

All decisions resolved from post-mortem analysis of the OFF/2026-04-21 crash run
(live log analysis + code review of 5 source files). No external research required.

---

## Decision 1: Where to fix the dtype boundary

**Question**: Should the `pd.NA` / `StringDtype` fix live in the Silver ETL (write side),
the Gold loader (read side), or each downstream block?

**Decision**: Gold loader (`_read_silver_parquet`), after `pd.concat`.

**Rationale**:
- Silver ETL is owned by a teammate; the `brand_name: null` Arrow dtype defect is being
  fixed there separately. The Gold pipeline must tolerate it independently.
- Fixing at the loader boundary means all 5 downstream blocks (`fuzzy_deduplicate`,
  `extract_allergens`, `llm_enrich`, `corpus.py`, `llm_tier.py`) see clean `object` dtypes
  with no further changes.
- Per-block fixes would require patching 5+ locations and would be brittle as new blocks
  are added.

**Alternatives considered**:
- Silver ETL fix only → unacceptable; teammate dependency, timing risk.
- Per-block `pd.isna` guards everywhere → too broad; doesn't address root cause.
- `pd.read_parquet(..., dtype_backend="numpy_nullable")` → still produces `pd.NA`; wrong direction.

---

## Decision 2: Corpus augmentation trigger — ratio vs absolute threshold

**Question**: What condition triggers corpus re-seeding before S2 queries?

**Decision**: Ratio threshold: `corpus_size / unresolved_count < 0.25`.

**Rationale**:
- An absolute threshold (e.g., "corpus < 10k vectors") is fragile — it passes when
  corpus is 10k but queries are 400k, giving 1 vector per 40 queries (too sparse).
- A ratio threshold scales with run size. 0.25 means corpus has at least 1 vector per
  4 queries, providing meaningful neighborhood coverage for KNN voting.
- Derived from observed S1 resolution rate (31.7%): if S1 resolves 30% of rows, the
  seeded corpus will have ~190k vectors against ~409k queries = 0.46 ratio, comfortably
  above 0.25.
- 0.25 is a starting value; tunable via `CORPUS_AUGMENT_RATIO` env var.

**Alternatives considered**:
- Absolute count (e.g., 10k): doesn't scale with run size.
- Always re-seed: wasteful; ignores case where corpus is already well-populated from
  previous runs.
- Skip augmentation entirely: leaves S2 broken for first-run scenarios.

---

## Decision 3: ChromaDB query chunk size

**Question**: What chunk size for `knn_search_batch` query loop?

**Decision**: 500 (default), configurable via `CHROMA_QUERY_CHUNK_SIZE`.

**Rationale**:
- `build_seed_corpus` already chunks upserts at 500 — using the same value gives
  consistent behavior and avoids introducing a second tuning knob without justification.
- 500 embeddings × 384 dimensions × 4 bytes = ~768 KB per chunk — well within ChromaDB's
  HTTP payload limit.
- Actual bottleneck is encode time (1h31min on CPU for 409k rows), not query round-trips.
  Chunking fixes the payload error; chunk size doesn't significantly affect total time.

**Alternatives considered**:
- 1000: could still hit payload limits on large embedding dimensions; less headroom.
- 100: over-chunked; 820 round-trips for 409k rows adds network overhead.
- Dynamic (auto-size based on embedding dim): over-engineering for a configurable constant.

---

## Decision 4: MIN_ENRICHMENT_CORPUS threshold

**Question**: What minimum corpus size warrants running S2 at all after augmentation?

**Decision**: 1000 vectors (configurable via `MIN_ENRICHMENT_CORPUS`).

**Rationale**:
- The existing `MIN_CORPUS_SIZE = 10` was designed for the single-row `knn_search` path.
  For batch KNN on 400k+ rows, 10 vectors means 5 category labels at most — useless for voting.
- 1000 vectors provides at minimum ~10–20 category labels with sufficient density for majority
  voting to be meaningful.
- If S1 resolves any rows (it always does — 189k in the crash run), augmentation will
  produce orders of magnitude more than 1000. The threshold is a guard against edge cases
  where S1 resolves nothing (e.g., source with no parseable ingredient text).

**Alternatives considered**:
- 100: still too low for meaningful KNN voting over 400k queries.
- 10k: could skip S2 unnecessarily if S1 resolution rate is low on small sources.
- Same as `MIN_CORPUS_SIZE=10`: doesn't address the batch-vs-single-row distinction.

---

## Decision 5: `_safe_text` implementation

**Question**: What is the safest way to convert a `pd.Series` cell value to a string,
handling `pd.NA`, `None`, and `float NaN`?

**Decision**:
```python
def _safe_text(v) -> str:
    try:
        return "" if pd.isna(v) else str(v)
    except (TypeError, ValueError):
        return str(v) if v is not None else ""
```

**Rationale**:
- `pd.isna(v)` returns `True` for `pd.NA`, `None`, `float('nan')` — covers all three cases.
- The `try/except` handles the edge case where `v` is an iterable or custom object where
  `pd.isna` itself raises (e.g., arrays). Belt-and-suspenders.
- `str(v) if v is not None else ""` in the fallback avoids calling `str(None)` = `"None"`.

**Alternatives considered**:
- `str(v or "")`: `or` calls `__bool__` → crashes on `pd.NA`. This is the current broken code.
- `str(v) if v is not None and not pd.isnull(v) else ""`: `pd.isnull` raises on `pd.NA` — same bug.
- `"" if pd.isna(v) else str(v)` (no try/except): safe for scalar values but not for array-type cells.

---

## Decision 6: Blocking key change

**Question**: What blocking key reduces large blocks without breaking dedup correctness?

**Decision**: `f"{name[:4]}_{brand[:2]}"` (composite 4-char name prefix + 2-char brand prefix).

**Rationale**:
- Root cause of the mega-block: common 3-char prefixes ("cho" = chocolate, "gre" = green, etc.)
  group thousands of unrelated products together. 72M comparisons for a 12k-row block.
- Adding 1 char to the name prefix (3→4) directly splits "cho" into "choc"/"choo"/"chol"/etc.
- Adding brand prefix further splits same-name products across brands (e.g., "choc_ne" =
  Nestlé chocolate vs. "choc_he" = Hershey chocolate).
- Composite key does not break dedup: two truly duplicate products have the same name AND
  the same brand → same key → still compared.
- The `valid_name_mask` guard (line 111) is preserved — rows with empty product_name are
  still excluded from blocking.

**Alternatives considered**:
- 4-char name only (no brand): reduces large blocks but doesn't split same-name/diff-brand rows.
- 5-char name: diminishing returns; rare products with short names (3-4 chars) would be excluded from blocking.
- Bigram blocking: more sophisticated but overkill; the simple prefix fix addresses the observed bottleneck.
- Separate blocking for null-brand rows: not needed — brand is `.fillna("")`, so null brand → empty prefix → key `"name_"`.
