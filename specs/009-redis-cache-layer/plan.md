# Implementation Plan: Redis Cache Layer

**Branch**: `009-redis-cache-layer` | **Date**: 2026-04-21 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/009-redis-cache-layer/spec.md`

## Summary

Add a Redis-backed cache layer across four pipeline stages — YAML mapping, LLM S3 enrichment, KNN embedding, and fuzzy dedup — to eliminate redundant compute across partitions. A thin `CacheClient` wrapper is injected into `PipelineState` and the block `config` dict so each stage can hit/miss independently with graceful fallback on Redis unavailability. `demo.py` gains `--no-cache` and `--flush-cache` CLI flags; `app.py` gains a sidebar toggle. The most impactful change is short-circuiting `analyze_schema_node` → `critique_schema_node` on YAML cache hits, saving the full `deepseek-reasoner` cost (≥2m24s) on partitions 2–13.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: `redis-py` (new), `numpy` (existing, for embedding serialization), `hashlib` (stdlib), `argparse` (stdlib)  
**Storage**: Redis at `localhost:6379` (new); FAISS index (existing, unaffected)  
**Testing**: pytest (existing)  
**Target Platform**: GCP VM, Linux  
**Project Type**: Pipeline extension (library-style, no external API)  
**Performance Goals**: ≥60% wall-clock reduction for 13-partition USDA ingest; 0 LLM calls for schema analysis on partitions 2–13; ≥70% embedding cache hits on partition 2+  
**Constraints**: Redis failure must not crash pipeline; no per-row LLM calls (constitution VIII); all Redis ops wrapped in `try/except` with 1s connect/read timeout for fast failure  
**Scale/Scope**: 13 partitions × ~10K rows; ~500MB estimated Redis footprint  

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| **I. Schema-First Gap Analysis** | ✅ Pass | YAML cache key includes `unified_schema` hash — schema changes auto-invalidate and force re-analysis. Cache only activates when schema is provably identical to a prior analyzed run. |
| **II. Three-Agent Pipeline** | ✅ Pass | Agent 1 + 2 bypassed on YAML cache hit, but only when their output already exists and is valid (per Principle VI below). Cache miss restores full three-agent flow. |
| **III. Declarative YAML Only** | ✅ Pass | Cache stores and retrieves YAML file paths and column mappings. No code generation introduced. |
| **IV. Human Approval Gates** | ✅ Pass | Cache is transparent to operator. `--no-cache` flag provides full override. Gate 1 (schema review) triggers normally on first partition; Gate 2 (quarantine) unaffected. |
| **V. Enrichment Safety Boundaries** | ✅ Pass | Cached LLM responses contain the same values as original LLM output. Safety fields (`allergens`, `dietary_tags`, `is_organic`) cached with same deterministic-extraction values — no probabilistic inference added. |
| **VI. Self-Extending Mapping Memory** | ✅ Pass | YAML mapping cache is the distributed-store expression of this principle. Complements, does not replace, the file-based registry. |
| **VII. DQ and Quarantine** | ✅ Pass | `dq_score_pre`, `dq_score_post`, and quarantine logic run per-chunk as before. Cache operates on enrichment values, not DQ scoring. |
| **VIII. Production Scale** | ✅ Pass | Directly addresses scale: eliminates redundant LLM calls, enables <1s dedup for cached partitions, reduces S3 API cost across 13+ partitions. Batched operations preserved. |

**Constitution Violation Count**: 0 — proceed to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/009-redis-cache-layer/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── checklists/
│   └── requirements.md  # Spec quality checklist
└── tasks.md             # Phase 2 output (/speckit.tasks — not created here)
```

### Source Code (repository root)

```text
src/
├── cache/                          # NEW module
│   ├── __init__.py                 # exports CacheClient, CacheStats
│   ├── client.py                   # CacheClient wrapper class
│   └── stats.py                    # CacheStats per-run accumulator
├── agents/
│   ├── state.py                    # ADD: cache_client, cache_yaml_hit fields
│   ├── graph.py                    # ADD: cache_client in run_pipeline config dict;
│   │                               #      ADD: route_after_analyze_schema conditional edge
│   └── orchestrator.py             # MODIFY: analyze_schema_node — YAML cache check/set
├── enrichment/
│   ├── llm_tier.py                 # MODIFY: cache check/set per-row before/after LLM batch
│   └── corpus.py                   # MODIFY: embedding cache check/set in batch_search
└── blocks/
    └── dedup/
        └── fuzzy_deduplicate.py    # MODIFY: cluster assignment cache check/set

demo.py                             # MODIFY: add argparse --no-cache, --flush-cache
app.py                              # MODIFY: sidebar cache toggle + --flush-cache button

tests/
├── unit/
│   └── test_cache_client.py        # unit tests for CacheClient
└── integration/
    └── test_cache_pipeline.py      # integration test: cache hit/miss across two runs
```

**Structure Decision**: Single-project layout. New `src/cache/` module is self-contained and injected into the existing pipeline at well-defined call sites. No new graph nodes required — YAML cache bypass uses a new conditional edge (`route_after_analyze_schema`) added to the existing graph.

## Complexity Tracking

No constitution violations requiring justification.
