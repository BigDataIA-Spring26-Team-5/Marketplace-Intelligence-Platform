# Tasks: Chunked CSV Processing

**Feature**: UC1 Data Enrichment Pipeline - Chunked Processing Enhancement  
**Spec**: specs/001-data-enrichment-pipeline/spec.md  
**Plan**: specs/001-data-enrichment-pipeline/plan.md

## Overview

| Metric | Value |
|--------|-------|
| Total Tasks | 8 |
| User Stories | 5 |
| Parallelizable | 2 |

## Phase 1: Setup

- [ ] T001 Create streaming CSV loader utility in src/utils/csv_stream.py

## Phase 2: Foundational

- [ ] T002 [P] Add chunk state tracking methods to CheckpointManager in src/pipeline/checkpoint/manager.py

## Phase 3: Core Implementation

- [ ] T003 [P] [US1] Modify schema analyzer to accept chunk input in src/schema/analyzer.py
- [ ] T004 [US1] Update schema __init__ to export streaming loader in src/schema/__init__.py
- [X] T005 [US3] Modify pipeline runner to iterate over chunks in src/pipeline/runner.py
- [X] T006 [US3] Update orchestrator to handle chunk iteration state in src/agents/orchestrator.py
- [X] T007 [P] Add --chunk-size CLI argument in src/pipeline/cli.py

## Phase 4: Polish

- [ ] T008 Verify full pipeline works with large file (use usda_fooddata_sample.csv)

## Dependencies

```
T001 → T003 → T005 → T006 → T007
T002 → T005
T004 → T005
```

## Independent Test Criteria

| User Story | Test |
|-----------|------|
| US1 (Data Source Ingestion) | Verify schema analysis works on partial file (first 10k rows) |
| US3 (Pipeline Execution) | Verify chunked processing completes all chunks |

## MVP Scope

User Story 1 (Data Source Ingestion) + chunked processing for large files.

---

## Task Details

### Phase 1: Setup

- [X] T001 Create streaming CSV loader utility in src/utils/csv_stream.py
  - Implement CsvStreamReader class with __iter__ method
  - Add get_total_rows() for counting without loading full file
  - Support configurable chunk_size (default: 10000)
  - Handle encoding and delimiter options

### Phase 2: Foundational

- [X] T002 [P] Add chunk state tracking methods to CheckpointManager in src/pipeline/checkpoint/manager.py
  - Add save_chunk_state(run_id, chunk_index, stage, state)
  - Add get_chunk_resume_state(run_id, chunk_index)
  - Update schema to include stage tracking

### Phase 3: Core Implementation

- [X] T003 [P] [US1] Modify schema analyzer to accept chunk input in src/schema/analyzer.py
- [X] T004 [US1] Update schema __init__ to export streaming loader in src/schema/__init__.py
  - Export CsvStreamReader from csv_stream module

- [ ] T005 [US3] Modify pipeline runner to iterate over chunks in src/pipeline/runner.py
  - Use CsvStreamReader for input
  - Process each chunk through transform → enrich → DQ
  - Save checkpoint after each chunk
  - Aggregate results at end

- [ ] T006 [US3] Update orchestrator to handle chunk iteration state in src/agents/orchestrator.py
  - Track current chunk index
  - Handle resume from checkpoint

- [ ] T007 [P] Add --chunk-size CLI argument in src/pipeline/cli.py
  - Add --chunk-size argument
  - Read from CHUNK_SIZE env var as default
  - Pass to CheckpointManager

### Phase 4: Polish

- [X] T008 Verify full pipeline works with large file (use usda_fooddata_sample.csv)
  - Run pipeline with --chunk-size 5000
  - Verify checkpoint correctly tracks chunks
  - Verify output is correct