# Tasks: UI Architecture Rebuild

**Feature**: UI Architecture Rebuild  
**Branch**: `003-ui-architecture-rebuild`  
**Generated**: 2026-04-17  
**Spec**: `specs/003-ui-architecture-rebuild/spec.md`

## Task Summary

| Metric | Count |
|--------|-------|
| Total Tasks | 12 |
| Setup Phase | 1 |
| Foundational Phase | 2 |
| User Story Phases | 8 |
| Parallelizable | 3 |

## Phase 1: Setup

- [X] T001 Verify existing project dependencies (Streamlit, pandas, langgraph, LiteLLM)

## Phase 2: Foundational

- [X] T002 [P] Create render_agent_header() function in src/ui/components.py for Agent 1/2/3 display
- [X] T003 [P] Create render_extraction_only_flag() function in src/ui/components.py for safety constraint columns

## Phase 3: User Story 1 - Pipeline Wizard Navigation (P1)

**Goal**: Implement 5-step wizard that maps to 7-node pipeline
**Independent Test**: Launch app.py, verify all 5 steps navigable, pipeline executes end-to-end

- [X] T004 [US1] Create app.py with 5-step wizard structure in root directory
- [X] T005 [US1] Implement step_select_source() with CSV file selection and domain picker
- [X] T006 [US1] Implement step_schema_analysis() calling load_source and analyze_schema nodes
- [X] T007 [US1] Implement step_critique_and_plan() calling critique_schema, check_registry, plan_sequence nodes
- [X] T008 [US1] Implement step_execution() calling run_pipeline and save_output nodes
- [X] T009 [US1] Implement step_results() displaying DQ scores, enrichment stats, quarantine table

## Phase 4: User Story 2 - Agent Activity Visibility (P1)

**Goal**: Display Agent 1/2/3 activity with distinct labels
**Independent Test**: Run pipeline, verify each agent's output is labeled

- [X] T010 [P] [US2] Add Agent 1 (Orchestrator) header with role label in schema analysis step
- [X] T011 [P] [US2] Add Agent 2 (Critic) header with role label in critique step
- [X] T012 [P] [US2] Add Agent 3 (Sequence Planner) header with role label in plan sequence step

## Phase 5: User Story 3 - Sampling & Confidence Display (P2)

**Goal**: Show Agent 1's sampling strategy and confidence scores
**Independent Test**: Verify sampling stats and confidence badges in schema delta

- [X] T013 [US3] Create render_sampling_stats() displaying method, sample_size, fallback_triggered
- [X] T014 [US3] Add confidence badges (High/Medium/Low) to schema delta table rendering

## Phase 6: User Story 4 - HITL Gate Compliance (P1)

**Goal**: Position HITL gates correctly
**Independent Test**: Count HITL interactions - should be exactly 2 explicit (Gate 1, Gate 3)

- [X] T015 [US4] Implement HITL Gate 1: Schema mapping approval with approve/exclude/abort options
- [X] T016 [US4] Implement HITL Gate 3: Quarantine acceptance with accept/override options
- [X] T017 [US4] Ensure no explicit code review gate (Gate 2 is implicit per constitution)

## Phase 7: User Story 5 - Safety Constraint Flagging (P2)

**Goal**: Flag enrichment-only columns as extraction-only
**Independent Test**: Verify allergens, is_organic, dietary_tags show EXTRACTION-ONLY badge

- [X] T018 [US5] Add extraction-only badge rendering for safety columns in schema delta

## Phase 8: Polish

- [X] T019 Add sidebar navigation between completed steps
- [X] T020 Update step bar to show agent names where applicable

---

## Dependency Graph

```
T001 (Setup)
   └─→ T002, T003 (Foundational)
          ├─→ T004-T009 (US1 - Wizard Navigation)
          │      ├─→ T010-T012 (US2 - Agent Visibility)
          │      │      ├─→ T013-T014 (US3 - Sampling & Confidence)
          │      │      │      └─→ T015-T017 (US4 - HITL Gates)
          │      │      │             └─→ T018 (US5 - Safety Flags)
          │      │      └─→ T015-T017 (US4 - HITL Gates)
          │      └─→ T013-T014 (US3 - Sampling & Confidence)
          └─→ T019-T020 (Polish)
```

## Independent Test Criteria

| User Story | Test Criteria |
|------------|---------------|
| US1 | Launch app.py, all 5 steps navigable, pipeline executes end-to-end |
| US2 | Each agent output labeled with correct name and role |
| US3 | Sampling stats and confidence badges visible in schema analysis |
| US4 | Gate 1 and Gate 3 appear at correct pipeline points |
| US5 | allergens, is_organic, dietary_tags show EXTRACTION-ONLY badge |

## MVP Scope

**Recommended MVP**: User Story 1 + User Story 2 (Tasks T001-T012)
- Basic wizard structure with agent labels
- Core pipeline execution flow
- Can be tested independently