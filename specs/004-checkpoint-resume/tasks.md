# Tasks: Checkpoint and Resume Capability

**Feature**: Checkpoint and Resume Capability  
**Branch**: `004-checkpoint-resume`  
**Input**: spec.md, plan.md, data-model.md, research.md

## Phase 1: Setup

- [X] T001 Create checkpoint directory `checkpoint/` at project root
- [X] T002 [P] Add schema_version to `.specify/requiredlimits.yaml` (e.g., `checkpoint_schema_version: 1`)

## Phase 2: Foundational

- [X] T003 [P] Create SQLite database schema in `src/pipeline/checkpoint/schema.sql` with tables: checkpoints, chunk_states, transformation_plans, corpus_snapshots
- [X] T004 Create checkpoint database module `src/pipeline/checkpoint/__init__.py`
- [X] T005 Create CheckpointManager class in `src/pipeline/checkpoint/manager.py` with methods: create, load, validate, atomic_write

## Phase 3: User Story 1 - Pipeline Resumes After Crash (P1)

Goal: Pipeline automatically resumes from last completed chunk after crash

**Independent Test**: Simulate crash mid-run, restart, verify only unprocessed chunks are handled

- [X] T006 [US1] Implement save_checkpoint method in `src/pipeline/checkpoint/manager.py`
- [X] T007 [US1] Implement load_checkpoint method in `src/pipeline/checkpoint/manager.py`
- [X] T008 [US1] Implement get_resume_state method in `src/pipeline/checkpoint/manager.py`
- [X] T009 [US1] Add chunk boundary detection in `src/pipeline/runner.py` (call checkpoint.save after each chunk)
- [X] T010 [US1] Add checkpoint detection on startup in `src/pipeline/runner.py` (call checkpoint.load before run)
- [X] T011 [US1] Integrate FAISS corpus serialization with checkpoint in `src/enrichment/corpus.py`

## Phase 4: User Story 2 - Checkpoint Integrity Validation (P2)

Goal: Verify checkpoint is valid before resuming

**Independent Test**: Corrupt checkpoint, verify system detects and offers fresh start

- [X] T012 [US2] Implement validate_checkpoint method in `src/pipeline/checkpoint/manager.py`
- [X] T013 [US2] Add schema version comparison on resume
- [X] T014 [US2] Add source file SHA256 comparison on resume
- [X] T015 [US2] Add corruption detection (missing required fields)

## Phase 5: User Story 3 - Manual Resume Control (P3)

Goal: Operator can explicitly control resume behavior

**Independent Test**: Test --resume, --force-fresh flags

- [X] T016 [US3] Add CLI argument parser for --resume and --force-fresh flags in `src/pipeline/cli.py`
- [X] T017 [US3] Implement force_fresh method in `src/pipeline/checkpoint/manager.py`
- [X] T018 [US3] Implement clear_checkpoint method in `src/pipeline/checkpoint/manager.py`

## Phase 6: Poland & Cross-Cutting

- [X] T019 [P] Add integration test in `tests/integration/test_checkpoint_resume.py`
- [X] T020 Add performance validation (verify checkpoint write <5s, resume detection <2s)
- [X] T021 Update quickstart.md with new CLI flags

---

## Summary

| Metric | Value |
|--------|-------|
| Total Tasks | 21 |
| Phase 1 (Setup) | 2 |
| Phase 2 (Foundational) | 3 |
| Phase 3 (US1) | 6 |
| Phase 4 (US2) | 4 |
| Phase 5 (US3) | 3 |
| Phase 6 (Polish) | 3 |

### Parallel Opportunities

- T002 can run parallel to T003
- T004 depends on T003
- T006, T007, T008 can be parallelized within US1 phase
- T012, T013, T014, T015 can be parallelized within US2 phase
- T016, T017, T018 can be parallelized within US3 phase

### Independent Test Criteria by User Story

- **US1**: Given 5 chunks, crash at chunk 4, restart → resumes from chunk 4
- **US2**: Given corrupted checkpoint, start pipeline → warns and offers fresh start
- **US3**: Given checkpoint exists, run with --force-fresh → starts from scratch

### MVP Scope

User Story 1 (US1) is the MVP. It provides the core checkpoint/resume functionality. US2 and US3 are enhancements for operational control.

---

## Dependencies

```
Phase 1 (Setup)
    │
    └── Phase 2 (Foundational)
            │
            ├── Phase 3 (US1 - Pipeline Resumes)
            │       │
            │       └── Phase 6 (Polish)
            │
            ├── Phase 4 (US2 - Integrity Validation)
            │       │
            │       └── Phase 6 (Polish)
            │
            └── Phase 5 (US3 - Manual Control)
                    │
                    └── Phase 6 (Polish)
```

## Implementation Strategy

1. **MVP First**: Complete Phases 1-3 (US1) to get checkpoint/resume working
2. **Incremental Delivery**: Add US2 for validation, US3 for control
3. **Polish**: Tests and performance validation last