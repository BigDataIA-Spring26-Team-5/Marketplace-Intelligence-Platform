# Implementation Plan: Gold Layer Revamp — Reliability & Performance

**Branch**: `aqeel` | **Date**: 2026-04-22 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/014-gold-layer-revamp/spec.md`

---

## Summary

Six targeted fixes to the Silver→Gold pipeline that eliminate a crash caused by
Arrow `StringDtype` nullable columns, fix an S2 corpus bootstrap deadlock, chunk
ChromaDB batch queries to avoid payload limits, raise the corpus-size gate before
S2 runs, add a `_safe_text` defense in `llm_tier.py`, and replace the 3-char dedup
blocking key with a 4-char composite key. No schema changes. No new dependencies.
No new files. All changes are in-place edits to 5 existing source modules.

---

## Technical Context

**Language/Version**: Python 3.11
**Primary Dependencies**: pandas 2.x, chromadb (HTTP client), sentence-transformers (`all-MiniLM-L6-v2`), rapidfuzz, faiss-cpu, redis-py, pyarrow
**Storage**: GCS (Silver Parquet input), BigQuery (Gold output), ChromaDB at localhost:8000 (corpus), Redis at localhost:6379 (embedding + dedup cache)
**Testing**: pytest (`poetry run pytest`)
**Target Platform**: Linux server (mip-vm, GCP)
**Project Type**: CLI data pipeline (`python -m src.pipeline.gold_pipeline`)
**Performance Goals**: Full OFF run (783k rows) completes in <3h; dedup <1h; 1k-row sample <10min
**Constraints**: Backward-compatible with all 4 Silver sources; no BigQuery schema change; no new pip dependencies; Redis/ChromaDB unavailability must be graceful degradation not crash
**Scale/Scope**: 783k rows per run (OFF source); up to 409k rows through S2/S3 enrichment

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Schema-First Gap Analysis | ✅ Pass | `config/unified_schema.json` is unchanged. Gold output schema unchanged. |
| II. Three-Agent Pipeline | ✅ Pass | Changes are in Gold pipeline blocks only — not in LangGraph nodes, not in Agent 1/2/3 responsibilities. |
| III. Declarative YAML Execution Only | ✅ Pass | No new YAML mapping blocks. No runtime Python code generation. All 6 changes are fixes to existing Python execution logic. |
| IV. Human Approval Gates | ✅ Pass | HITL gates (schema mapping review, quarantine review) are not affected by any of the 6 changes. |
| V. Cascading Enrichment with Safety Boundaries | ✅ Pass | `augment_from_df` adds `primary_category` vectors only. Safety fields (`allergens`, `is_organic`, `dietary_tags`) are never upserted to corpus, never inferred by S2 or S3. Principle V invariant is preserved. |
| VI. Self-Extending Mapping Memory | ✅ Pass | Not affected. Generated YAML mappings are unchanged. |
| VII. DQ and Quarantine Enforcement | ✅ Pass | `dq_score_pre`/`dq_score_post` unchanged. Quarantine logic unchanged. |
| VIII. Production Scale | ✅ Pass | This feature IS the production-scale fix. Batched ChromaDB queries (FR-006), corpus augmentation before batch KNN (FR-003), no per-record LLM calls (existing S3 batch is unchanged). |

**GATE: PASSED. No violations.**

---

## Project Structure

### Documentation (this feature)

```text
specs/014-gold-layer-revamp/
├── impl-plan.md         # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── spec.md              # Feature specification
└── checklists/
    └── requirements.md  # Quality checklist
```

### Source Code (modified files only)

```text
src/
├── pipeline/
│   └── gold_pipeline.py         # Change 1: StringDtype cast after read_parquet
├── enrichment/
│   ├── corpus.py                # Change 2: augment_from_df fn
│   │                            # Change 3: chunked knn_search_batch query
│   ├── embedding.py             # Change 2: call augment_from_df
│   │                            # Change 4: MIN_ENRICHMENT_CORPUS short-circuit
│   └── llm_tier.py              # Change 5: _safe_text helper + 2 call sites
└── blocks/
    └── fuzzy_deduplicate.py     # Change 6: 4-char composite blocking key
```

**Structure Decision**: Single-project CLI pipeline. All modified files are already
within `src/`. No new directories. No new files.

---

## Phase 0: Research

*See [research.md](research.md) for full findings.*

All technical decisions were resolved during the post-mortem analysis session
(live run logs + code review). No unknowns remain. Summary:

| Decision | Chosen | Rationale |
|----------|--------|-----------|
| dtype cast location | `_read_silver_parquet` after concat | One fix covers all 5 blocks; blocks should not need dtype awareness |
| Cast method | `df[cols].astype(object)` | Simplest; converts `pd.NA` → `None`; no new dependencies |
| Augment trigger | corpus/queries < 0.25 | Derived from S1 resolution rate (31.7%); ensures corpus ≥ 25% of query volume before KNN |
| Augment chunk size | 500 | Matches existing `build_seed_corpus` upsert chunk size for consistency |
| Query chunk size | 500 (env: `CHROMA_QUERY_CHUNK_SIZE`) | Same as upsert; tunable |
| MIN_ENRICHMENT_CORPUS | 1000 | Below 1k vectors, KNN on 400k+ rows produces noise not signal |
| Blocking key | `name[:4]_brand[:2]` | 4-char name reduces large blocks; brand prefix splits same-name products across brands |
| `_safe_text` impl | `pd.isna(v)` guard with `try/except` | Handles `pd.NA`, `None`, `float NaN`; no `or` chain that triggers `__bool__` |

---

## Phase 1: Design

*See [data-model.md](data-model.md) for entity detail.*

### Key design decisions

**Change 1 — Cast placement**: After `pd.concat`, not inside `read_parquet`. Reason:
`pd.read_parquet(engine="pyarrow")` always returns `StringDtype` for Arrow `string` columns
in pandas 2.x; cast must be explicit. Doing it once at the read boundary is cleaner than
patching each of the 5 downstream blocks.

**Change 2 — `augment_from_df` signature**:
```python
def augment_from_df(
    df: pd.DataFrame,
    collection,
    unresolved_count: int,
    force_ratio_threshold: float = 0.25,
) -> int
```
Takes `unresolved_count` as explicit parameter rather than computing from `df` — the caller
(`embedding_enrich`) already has this value and passing it avoids a redundant mask computation.

**Change 3 — Chunk loop pattern**:
```python
all_metadatas, all_distances = [], []
total = len(valid_texts)
for chunk_start in range(0, total, CHROMA_QUERY_CHUNK_SIZE):
    chunk_embs = embeddings[chunk_start:chunk_start + CHROMA_QUERY_CHUNK_SIZE]
    try:
        r = index.query(query_embeddings=chunk_embs.tolist(), n_results=k_actual)
        all_metadatas.extend(r["metadatas"])
        all_distances.extend(r["distances"])
    except Exception as e:
        n = len(chunk_embs)
        logger.warning("ChromaDB chunk query failed [%d:%d]: %s", chunk_start, chunk_start+n, e)
        all_metadatas.extend([[] for _ in range(n)])
        all_distances.extend([[] for _ in range(n)])
    if (chunk_start // CHROMA_QUERY_CHUNK_SIZE + 1) % 10 == 0:
        logger.info("S2 KNN: queried chunk %d/%d (%d/%d rows)",
                    chunk_start // CHROMA_QUERY_CHUNK_SIZE + 1,
                    (total + CHROMA_QUERY_CHUNK_SIZE - 1) // CHROMA_QUERY_CHUNK_SIZE,
                    min(chunk_start + CHROMA_QUERY_CHUNK_SIZE, total), total)
batch_results = {"metadatas": all_metadatas, "distances": all_distances}
```

**Change 6 — Blocking key**: Composite key breaks the "cho" mega-block. Off-brand products with the
same 4-char name prefix but different brands get different keys, reducing block sizes.
The `valid_name_mask` guard (line 111) excludes empty-name rows from blocking — unchanged.

### Contracts

No external interface contracts. This is a CLI pipeline (`python -m src.pipeline.gold_pipeline`).
The CLI signature is unchanged. No new env vars affect external callers beyond what's documented
in the spec's Configuration table.

### Agent context update

Run after plan is complete:
```bash
cd "/home/aq/work/NEU/SPRING_26/Big Data/ETL" && bash .specify/scripts/bash/update-agent-context.sh claude
```

---

## Post-Design Constitution Re-check

All 8 principles: ✅ Pass (identical to pre-design check — design confirms no violations).

Specific re-check for V (enrichment safety boundary):
- `augment_from_df` iterates `df[df["primary_category"].notna()]` — only rows with resolved
  `primary_category` are encoded and upserted. The function never reads or upserts `allergens`,
  `is_organic`, or `dietary_tags` columns.
- `_build_row_text` (used by `augment_from_df` internally) concatenates `product_name`,
  `brand_name`, `ingredients`, `category` — none of the safety fields.

Specific re-check for VIII (production scale):
- ChromaDB queries now chunked at 500 — no single HTTP call exceeds ~2MB payload.
- Corpus augmentation runs in chunks of 500 upserts — consistent with existing behavior.
- S2 still uses batched `model.encode()` (not per-record) — Principle VIII batch requirement met.
- S3 async batch with `_LLM_CONCURRENCY=5` is unchanged — Principle VIII met.
