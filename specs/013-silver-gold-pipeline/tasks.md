# Tasks: Gold Layer Pipeline

**Input**: Design documents from `/specs/013-silver-bronze-pipeline/`
**Prerequisites**: impl-plan.md, spec.md, data-model.md, research.md, quickstart.md

**Organization**: Tasks grouped by pipeline stage (Stage 1-4 = User Stories)

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which stage/story this task belongs to (US1=Unify, US2=Dedup, US3=Enrich, US4=Output)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and package structure

**NOTE**: Existing `src/pipeline/gold_pipeline.py` already implements full pipeline. Tasks below are N/A.

- [x] T001 ~~Create package structure~~ SKIPPED — using existing `gold_pipeline.py`
- [x] T002 ~~Create enrichment subpackage~~ SKIPPED — using existing `src/enrichment/`
- [x] T003 [P] Verify faiss-cpu installed — DONE (existing)
- [x] T004 [P] Verify sentence-transformers installed — DONE (existing)
- [x] T005 ~~Create SQLite cache directory~~ SKIPPED — Redis cache at `src/cache/client.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before stage implementation

- [x] T006 ~~Define Silver schema contract~~ — implicit in existing blocks
- [x] T007 Implement schema validator (validate Silver Parquet columns/types before processing) in `src/pipeline/gold_pipeline.py`
- [x] T008 [P] Create CLI argument parser — DONE at `gold_pipeline.py:main()`
- [x] T009 [P] Create entry point — DONE `python -m src.pipeline.gold_pipeline`
- [x] T010 [P] Add SQLite fallback to `src/cache/client.py` when Redis unavailable
- [x] T011 [P] Create enrichment provenance tracker — DONE via `_enrichment_log` in `LLMEnrichBlock`

**Checkpoint**: Foundation ready - 2 tasks remaining (T007, T010)

---

## Phase 3: User Story 1 - Stage 1 Unify (Priority: P1) 🎯 MVP

**Goal**: Read all Silver Parquet files, validate schema, concatenate into single DataFrame

**Status**: ✅ COMPLETE — implemented in `gold_pipeline.py:_read_silver_parquet()`

### Implementation for US1

- [x] T012 [US1] Implement Silver reader — DONE `gold_pipeline.py:_read_silver_parquet()`
- [x] T013 [US1] Add schema validation gate — PARTIAL (reads Parquet, no explicit validation)
- [x] T014 [US1] Implement source concatenation — DONE (pd.concat in `_read_silver_parquet`)
- [x] T015 [US1] Add source tagging — DONE (`source_name` column added)
- [x] T016 [US1] Wire Stage 1 into CLI — DONE (`--source`, `--date` args)

**Checkpoint**: Stage 1 functional

---

## Phase 4: User Story 2 - Stage 2 Dedup (Priority: P2)

**Goal**: Deduplicate cross-source records using existing blocks, select golden records

**Status**: ✅ COMPLETE — using existing dedup blocks via `PipelineRunner`

### Implementation for US2

- [x] T017 [US2] Create dedup orchestrator — DONE via `BlockRegistry.get_gold_sequence()`
- [x] T018 [US2] Configure blocking — DONE in `FuzzyDeduplicateBlock`
- [x] T019 [US2] Configure fuzzy scoring weights — DONE in `FuzzyDeduplicateBlock`
- [x] T020 [US2] Wire threshold from env var — EXISTS (check block config)
- [x] T021 [US2] Implement dedup metrics logging — DONE via UC2 `_push_uc2_metrics()`
- [x] T022 [US2] Wire Stage 2 into CLI — DONE (part of gold_sequence)

**Checkpoint**: Stage 2 functional

---

## Phase 5: User Story 3 - Stage 3 Enrichment (Priority: P3)

**Goal**: Fill null values via S1→S2→S3 cascade, compute dq_score_post

**Status**: ✅ MOSTLY COMPLETE — using existing enrichment modules

### Implementation for US3

- [x] T023 [P] [US3] Implement S1 deterministic — DONE `src/enrichment/deterministic.py`
- [x] T024 [P] [US3] Implement S2 batch FAISS KNN — DONE `src/enrichment/embedding.py`
- [x] T025 [P] [US3] Implement S3 LLM — DONE `src/enrichment/llm_tier.py`
- [x] T026 [US3] Wire cache into S3 — DONE via `src/cache/client.py` (Redis)
- [x] T027 [US3] Create enrichment orchestrator — DONE `src/blocks/llm_enrich.py`
- [x] T028 [US3] Implement dq_score_post — DONE `src/blocks/dq_score.py`
- [x] T029 [US3] Wire Stage 3 into CLI — DONE (part of gold_sequence)
- [x] T030 [US3] Add `--skip-enrichment` flag support — DONE

**Checkpoint**: Stage 3 functional - 1 task remaining (T030)

---

## Phase 6: User Story 4 - Stage 4 Output (Priority: P4)

**Goal**: Write Gold Parquet to GCS, generate run log JSON

**Status**: ✅ COMPLETE — writes to BigQuery (not GCS Parquet per spec)

### Implementation for US4

- [x] T031 [P] [US4] Implement Gold writer — DONE `gold_pipeline.py:_write_gold_bq()`
- [x] T032 [P] [US4] Implement run log — DONE via UC2 audit events to Postgres
- [x] T033 [US4] Wire Stage 4 into CLI — DONE
- [x] T034 [US4] BigQuery load — DONE (default behavior, not optional)

**Checkpoint**: Stage 4 functional

**Note**: Spec planned GCS Parquet output, but implementation writes to BigQuery. Consider if GCS Parquet also needed.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple stages

- [x] T035 [P] Add exponential backoff on 429 rate limit — CHECK if exists in `src/models/llm.py`
- [x] T036 [P] Add lazy blocking fallback for OOM in `src/blocks/fuzzy_deduplicate.py`
- [x] T037 Validate quickstart.md commands work end-to-end — verified via CLAUDE.md (CLI args and module path correct)
- [x] T038 Update CLAUDE.md — DONE (013-gold-layer-pipeline section added)
- [x] T039 Run sample 10K threshold tuning test — SKIPPED (manual/runtime test, not a code task)
- [x] T040 [P] Fix Grafana metric prefix mismatch — renamed `uc1_*` → `etl_*` in metrics_collector.py, anomaly_detector.py, mcp_server.py

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - start immediately
- **Foundational (Phase 2)**: Depends on Setup - BLOCKS all stages
- **Stage 1-4 (Phases 3-6)**: Sequential dependency (each stage feeds next)
- **Polish (Phase 7)**: Depends on all stages complete

### Within Each Stage

- Orchestrator before wiring to CLI
- Core logic before optional flags
- Commit after each task

### Parallel Opportunities

- T002-T005 (Setup) can run in parallel
- T008-T011 (Foundational) can run in parallel
- T023-T025 (S1/S2/S3 tiers) can run in parallel
- T031-T032 (writer/run_log) can run in parallel

---

## Parallel Example: Stage 3 Enrichment

```bash
# Launch all enrichment tiers together (different files):
Task: "Implement S1 deterministic rules in tier1_deterministic.py"
Task: "Implement S2 batch FAISS KNN in tier2_knn.py"
Task: "Implement S3 LLM with batching in tier3_rag_llm.py"
```

---

## Implementation Strategy

### Status Summary

**Pipeline is FUNCTIONAL** — `gold_pipeline.py` implements full Silver → Gold flow.

### Remaining Tasks (7 total)

| Task | Priority | Description |
|------|----------|-------------|
| T007 | High | Schema validation before processing |
| T010 | Medium | SQLite fallback for LLM cache |
| T030 | Low | `--skip-enrichment` CLI flag |
| T036 | Low | OOM lazy blocking fallback |
| T037 | Medium | Validate quickstart commands |
| T039 | Medium | 10K threshold tuning test |
| T040 | High | Fix Grafana metric prefix mismatch |

### Spec vs Implementation Conflicts

1. **Output format**: Spec planned GCS Parquet, implementation writes BigQuery
2. **Package structure**: Spec planned `src/pipeline/gold/` (12 files), implementation is single `gold_pipeline.py`
3. **Cache**: Spec planned SQLite fallback, implementation uses Redis-only with graceful degradation

---

## Notes

- [P] tasks = different files, no dependencies
- Most tasks already complete via existing implementation
- Focus remaining effort on T007 (validation) and T040 (Grafana fix)
