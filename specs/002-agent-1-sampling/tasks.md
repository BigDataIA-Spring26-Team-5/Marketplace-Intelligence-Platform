---

description: "Task list for Agent 1 Representative Sampling feature"
---

# Tasks: Agent 1 Representative Sampling

**Input**: Design documents from `/specs/002-agent-1-sampling/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md

**Tests**: Not requested in feature specification - tests will be manual verification via quickstart.md scenarios

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Review existing codebase and prepare for enhancement

- [x] T001 Review existing src/schema/analyzer.py to understand current profiling logic
- [x] T002 Review existing src/agents/state.py to understand GapItem structure
- [x] T003 Review existing src/agents/orchestrator.py to understand analyze_schema_node

---

## Phase 2: Foundational (State Schema Extension)

**Purpose**: Add confidence scoring field to state schema - required by all user stories

- [x] T004 Add confidence_score (float) field to GapItem in src/agents/state.py
- [x] T005 Add confidence_factors (list) field to GapItem in src/agents/state.py
- [x] T006 Add sampling_evidence (dict) field to ColumnProfile in src/agents/state.py

---

## Phase 3: User Story 1 - Representative Row Sampling (Priority: P1) 🎯 MVP

**Goal**: Implement representative row sampling to prevent false gap detections from sparse values

**Independent Test**: Run pipeline on CSV where column has values in rows 100-500 but null in rows 1-99 - verify column detected as mappable, not missing

### Implementation for User Story 1

- [x] T007 [P] [US1] Implement calculate_sample_size() function in src/schema/sampling.py (new file)
- [x] T008 [P] [US1] Implement random_sample() function with seed support in src/schema/sampling.py
- [x] T009 [P] [US1] Implement full_scan() fallback for high null rate in src/schema/sampling.py
- [x] T010 [US1] Integrate sampling into load_source_node in src/agents/orchestrator.py
- [x] T011 [US1] Modify profile_dataframe() in src/schema/analyzer.py to accept sample parameter
- [x] T012 [US1] Add sampling metadata to source_schema in PipelineState (sample_size, sample_method, sample_seed)
- [x] T013 [US1] Add sampling audit logging in load_source_node

---

## Phase 4: User Story 2 - Adaptive Sampling Strategy (Priority: P1)

**Goal**: Auto-adjust sample size based on dataset characteristics

**Independent Test**: Run pipeline on datasets of varying sizes (100, 1K, 100K rows) and verify sample size scales appropriately

### Implementation for User Story 2

- [x] T014 [P] [US2] Add adaptive_sample_size() logic in src/schema/sampling.py based on research.md formula
- [x] T015 [P] [US2] Implement detect_sparse_columns() to identify columns needing larger sample
- [x] T016 [US2] Add fallback_triggered flag to SamplingStrategy in src/agents/state.py
- [x] T017 [US2] Add fallback_reason string to track why larger sample was needed

---

## Phase 5: User Story 3 - Confidence Scoring for Gap Detection (Priority: P2)

**Goal**: Provide confidence scores for gap classifications to help HITL prioritize review

**Independent Test**: Compare confidence scores for: (a) truly missing column, (b) sparse column, (c) fully populated column - verify differentiation

### Implementation for User Story 3

- [x] T018 [P] [US3] Implement calculate_confidence() heuristic in src/agents/confidence.py (new file)
- [x] T019 [P] [US3] Add confidence_score to each GapItem after analyze_schema_node completes
- [x] T020 [US3] Integrate confidence scoring in analyze_schema_node in src/agents/orchestrator.py
- [x] T021 [US3] Modify HITL display in src/ui/components.py to show confidence indicators
- [x] T022 [US3] Add confidence-based sorting in schema mapping review UI

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Validation, documentation, and edge case handling

- [x] T023 [P] Run quickstart.md validation scenarios - verify sparse column detection works
- [x] T024 [P] Test edge case: dataset with 80%+ null rate triggers full scan fallback
- [x] T025 Verify token usage stays within 2x baseline for large datasets
- [x] T026 Run pytest existing tests to ensure no regressions

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 (Phase 3) is MVP - complete first
  - US2 (Phase 4) can start after US1 core logic
  - US3 (Phase 5) can start after US1 baseline
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational - core sampling logic
- **User Story 2 (P1)**: Can start after US1 T007-T009 complete - builds on sampling core
- **User Story 3 (P2)**: Can start after US1 T010-T013 complete - needs sampling output

### Within Each User Story

- Core functions (sampling.py) before integration (orchestrator.py)
- State schema before confidence calculation
- Backend logic before UI changes

### Parallel Opportunities

- Phase 1: T001, T002, T003 can run in parallel (code review)
- Phase 2: T004, T005, T006 can run in parallel (state additions)
- Phase 3: T007, T008, T009 can run in parallel (sampling.py functions)
- Phase 4: T014, T015 can run in parallel (adaptive logic)
- Phase 5: T018, T019 can run in parallel (confidence calculation)

---

## Parallel Example: Phase 3 (User Story 1)

```bash
# Launch all core sampling functions in parallel:
Task: "Implement calculate_sample_size() in src/schema/sampling.py"
Task: "Implement random_sample() in src/schema/sampling.py"
Task: "Implement full_scan() fallback in src/schema/sampling.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 core sampling
4. **STOP and VALIDATE**: Test sparse column detection works
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 sampling core → Test → MVP!
3. Add US2 adaptive sizing → Test → Better scaling
4. Add US3 confidence scoring → Test → Better UX

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (sampling core)
   - Developer B: User Story 2 (adaptive sizing)
3. User Story 3 depends on US1 output - sequential after US1

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- No explicit tests in tasks - using manual verification via quickstart.md scenarios