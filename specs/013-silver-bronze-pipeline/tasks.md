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

- [ ] T001 Create package structure `src/pipeline/gold/__init__.py`
- [ ] T002 [P] Create enrichment subpackage `src/pipeline/gold/enrichment/__init__.py`
- [ ] T003 [P] Verify faiss-cpu installed (`poetry add faiss-cpu` if missing)
- [ ] T004 [P] Verify sentence-transformers installed (`poetry add sentence-transformers` if missing)
- [ ] T005 Create SQLite cache directory `cache/` and add to .gitignore

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before stage implementation

**⚠️ CRITICAL**: No stage work can begin until this phase is complete

- [ ] T006 Define Silver schema contract in `src/pipeline/gold/schema_contract.py`
- [ ] T007 Implement schema validator (check columns, types, report all mismatches) in `src/pipeline/gold/schema_contract.py`
- [ ] T008 [P] Create CLI argument parser in `src/pipeline/gold/cli.py`
- [ ] T009 [P] Create entry point `src/pipeline/gold/__main__.py`
- [ ] T010 [P] Create SQLite cache class in `src/pipeline/gold/enrichment/cache.py`
- [ ] T011 [P] Create enrichment provenance tracker in `src/pipeline/gold/enrichment/provenance.py`

**Checkpoint**: Foundation ready - stage implementation can begin

---

## Phase 3: User Story 1 - Stage 1 Unify (Priority: P1) 🎯 MVP

**Goal**: Read all Silver Parquet files, validate schema, concatenate into single DataFrame

**Independent Test**: `python -m src.pipeline.gold --run-date 2026-04-21 --dry-run` validates schemas without processing

### Implementation for US1

- [ ] T012 [US1] Implement Silver reader (GCS list + read Parquet) in `src/pipeline/gold/silver_reader.py`
- [ ] T013 [US1] Add schema validation gate (call validator, abort on mismatch) in `src/pipeline/gold/silver_reader.py`
- [ ] T014 [US1] Implement source concatenation in `src/pipeline/gold/silver_reader.py`
- [ ] T015 [US1] Add source tagging fallback (`data_source` from `_source` if null) in `src/pipeline/gold/silver_reader.py`
- [ ] T016 [US1] Wire Stage 1 into CLI `--dry-run` path in `src/pipeline/gold/cli.py`

**Checkpoint**: Stage 1 functional - can read and validate Silver files

---

## Phase 4: User Story 2 - Stage 2 Dedup (Priority: P2)

**Goal**: Deduplicate cross-source records using existing blocks, select golden records

**Independent Test**: Run on 10K sample, verify dedup metrics logged

### Implementation for US2

- [ ] T017 [US2] Create dedup orchestrator (import existing blocks) in `src/pipeline/gold/dedup.py`
- [ ] T018 [US2] Configure blocking (first 3 chars of product_name) in `src/pipeline/gold/dedup.py`
- [ ] T019 [US2] Configure fuzzy scoring weights (0.5/0.2/0.3) in `src/pipeline/gold/dedup.py`
- [ ] T020 [US2] Wire threshold from `GOLD_DEDUP_THRESHOLD` env var in `src/pipeline/gold/dedup.py`
- [ ] T021 [US2] Implement dedup metrics logging (clusters, ratio, top-10 largest) in `src/pipeline/gold/dedup.py`
- [ ] T022 [US2] Wire Stage 2 into CLI in `src/pipeline/gold/cli.py`

**Checkpoint**: Stage 2 functional - dedup produces golden records

---

## Phase 5: User Story 3 - Stage 3 Enrichment (Priority: P3)

**Goal**: Fill null values via S1→S2→S3 cascade, compute dq_score_post

**Independent Test**: Run enrichment on golden records, verify _enrichment_log populated

### Implementation for US3

- [ ] T023 [P] [US3] Implement S1 deterministic rules in `src/pipeline/gold/enrichment/tier1_deterministic.py`
- [ ] T024 [P] [US3] Implement S2 batch FAISS KNN in `src/pipeline/gold/enrichment/tier2_knn.py`
- [ ] T025 [P] [US3] Implement S3 RAG-LLM with batching in `src/pipeline/gold/enrichment/tier3_rag_llm.py`
- [ ] T026 [US3] Wire cache (SQLite/Redis) into S3 in `src/pipeline/gold/enrichment/tier3_rag_llm.py`
- [ ] T027 [US3] Create enrichment orchestrator (S1→S2→S3 sequence) in `src/pipeline/gold/enrichment/__init__.py`
- [ ] T028 [US3] Implement dq_score_post computation in `src/pipeline/gold/dq_score.py`
- [ ] T029 [US3] Wire Stage 3 into CLI in `src/pipeline/gold/cli.py`
- [ ] T030 [US3] Add `--skip-enrichment` flag support in `src/pipeline/gold/cli.py`

**Checkpoint**: Stage 3 functional - enrichment fills nulls, DQ scores computed

---

## Phase 6: User Story 4 - Stage 4 Output (Priority: P4)

**Goal**: Write Gold Parquet to GCS, generate run log JSON

**Independent Test**: Verify output file exists at expected GCS path, run log valid JSON

### Implementation for US4

- [ ] T031 [P] [US4] Implement Gold Parquet writer in `src/pipeline/gold/writer.py`
- [ ] T032 [P] [US4] Implement run log generator in `src/pipeline/gold/run_log.py`
- [ ] T033 [US4] Wire Stage 4 into CLI in `src/pipeline/gold/cli.py`
- [ ] T034 [US4] Add optional BigQuery load (`--load-bq` flag) in `src/pipeline/gold/writer.py`

**Checkpoint**: Full pipeline functional - end-to-end Gold run works

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple stages

- [ ] T035 [P] Add exponential backoff on 429 rate limit in `src/models/llm.py`
- [ ] T036 [P] Add lazy blocking fallback for OOM in `src/pipeline/gold/dedup.py`
- [ ] T037 Validate quickstart.md commands work end-to-end
- [ ] T038 Update CLAUDE.md with final implementation notes
- [ ] T039 Run sample 10K threshold tuning test per clarification decision

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
Task: "Implement S3 RAG-LLM with batching in tier3_rag_llm.py"
```

---

## Implementation Strategy

### MVP First (Stage 1 + Stage 2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: Stage 1 (Unify)
4. Complete Phase 4: Stage 2 (Dedup)
5. **STOP and VALIDATE**: Run on 10K sample, verify dedup works
6. Can produce deduplicated catalog without enrichment

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add Stage 1 → Can validate Silver schemas
3. Add Stage 2 → Can deduplicate (MVP!)
4. Add Stage 3 → Can enrich
5. Add Stage 4 → Full pipeline with output

---

## Notes

- [P] tasks = different files, no dependencies
- Stages are sequential (output of one feeds next)
- Enrichment tiers (S1/S2/S3) are parallel within Stage 3
- Commit after each task
- Validate at each checkpoint before proceeding
