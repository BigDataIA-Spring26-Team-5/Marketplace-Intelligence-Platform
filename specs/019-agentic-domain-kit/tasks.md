# Tasks: Agentic Domain Kit Builder

**Input**: Design documents from `specs/019-agentic-domain-kit/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/ ✅, quickstart.md ✅

**Tests**: Unit tests included per story. Integration tests in Polish phase.

**Organization**: Grouped by user story — each story independently implementable and testable.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no shared state)
- **[Story]**: Maps to US1/US2/US3 from spec.md
- Paths are relative to repo root

---

## Phase 1: Setup (New Module Skeletons)

**Purpose**: Create the two new files so downstream tasks have concrete targets.

- [X] T001 Create `src/agents/domain_kit_graph.py` with module docstring, all required imports (LangGraph, TypedDict, call_llm_json), and empty section comments for state types / node functions / graph builders
- [X] T002 [P] Create `src/agents/domain_kit_prompts.py` with module docstring and empty section comments for kit prompts and scaffold prompts

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared types and the validator pure function that all three user stories depend on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T002 Define `DomainKitState(TypedDict, total=False)`, `ScaffoldState(TypedDict, total=False)`, and `ValidationIssue(TypedDict)` in `src/agents/domain_kit_graph.py` per data-model.md field tables
- [X] T002 Implement `validate_enrichment_rules_yaml(yaml_dict: dict, csv_headers: list[str]) -> list[ValidationIssue]` in `src/agents/domain_kit_graph.py` with all 5 deterministic checks: (1) missing `__generated__` sentinel → error, (2) `dq_score_pre` not first or `dq_score_post` not last → warning, (3) custom block in sequence with no matching `.py` file → error, (4) enrichment field name matches a CSV header → warning, (5) enrichment field name matches a custom block name in sequence → warning
- [X] T002 Write kit generation prompt constants in `src/agents/domain_kit_prompts.py`: `CSV_ANALYSIS_PROMPT`, `ENRICHMENT_RULES_PROMPT`, `ENRICHMENT_RULES_FIX_PROMPT`, `PROMPT_EXAMPLES_PROMPT`, `BLOCK_SEQUENCE_PROMPT` — all domain-agnostic (no hardcoded field names); include explicit rule: "structured CSV columns are RENAME candidates in prompt_examples, NOT extraction fields in enrichment_rules"
- [X] T002 Write scaffold prompt constants in `src/agents/domain_kit_prompts.py`: `SCAFFOLD_GENERATE_PROMPT`, `SCAFFOLD_FIX_PROMPT` — include Block base class contract, naming convention `<domain>__<block_name>`, and syntax-error injection slot for fix prompt

**Checkpoint**: State types, validator, and all prompts ready — user story implementation can begin.

---

## Phase 3: User Story 1 — Generate Domain Pack via AI Agent (Priority: P1) 🎯 MVP

**Goal**: Multi-step LangGraph agent produces 3 YAML files with auto-retry on validation failure, diff view on overwrite, and single atomic HITL gate before file write.

**Independent Test**: Provide `pharma_sample.csv` + description → observe step-by-step generation → verify 3 YAML text areas appear → approve → confirm files written to `domain_packs/pharma/` with audit entry in `.audit.jsonl`.

### Implementation for User Story 1

- [X] T002 [US1] Implement `analyze_csv` node in `src/agents/domain_kit_graph.py`: parse `csv_content` → set `csv_headers` (list[str]) and `csv_sample_table` (markdown, first 5 rows)
- [X] T002 [US1] Implement `generate_enrichment_rules` node in `src/agents/domain_kit_graph.py`: call `ENRICHMENT_RULES_PROMPT` (or `ENRICHMENT_RULES_FIX_PROMPT` when `validation_errors` present) via `call_llm_json`, serialize result to YAML string, set `enrichment_rules_yaml`
- [X] T002 [US1] Implement `validate_enrichment_rules` node in `src/agents/domain_kit_graph.py`: parse `enrichment_rules_yaml`, call `validate_enrichment_rules_yaml()`, set `enrichment_fields` (field names list), `validation_errors`, increment `retry_count`
- [X] T0XX [US1] Implement `revise_enrichment_rules` node in `src/agents/domain_kit_graph.py`: re-call LLM with `ENRICHMENT_RULES_FIX_PROMPT` injecting current `validation_errors` and previous `enrichment_rules_yaml`; update `enrichment_rules_yaml`
- [X] T0XX [US1] Implement `generate_prompt_examples` node in `src/agents/domain_kit_graph.py`: call `PROMPT_EXAMPLES_PROMPT` with `csv_headers` and `enrichment_fields`; set `prompt_examples_yaml`
- [X] T0XX [US1] Implement `generate_block_sequence` node in `src/agents/domain_kit_graph.py`: call `BLOCK_SEQUENCE_PROMPT` passing `enrichment_fields` to prevent phantom custom blocks; set `block_sequence_yaml`
- [X] T0XX [US1] Implement `hitl_review` node in `src/agents/domain_kit_graph.py`: no-op node that sets `pending_review=True`; does not call LLM or write files
- [X] T0XX [US1] Implement `commit_to_disk` node in `src/agents/domain_kit_graph.py`: (a) detect existing files in `domain_packs/<domain>/`, write `.bak` copies if present; (b) write `user_edits` content (or raw yaml fields if no edits) for all 3 YAML files; (c) append to `domain_packs/<domain>/.audit.jsonl` with action `generate` (fresh) or `overwrite` (existing); (d) set `committed=True`
- [X] T0XX [US1] Build `DomainKitGraph` in `src/agents/domain_kit_graph.py`: wire all 8 nodes, add conditional edge from `validate_enrichment_rules` → `revise_enrichment_rules` when `validation_errors` non-empty and `retry_count < 2`, else → `generate_prompt_examples`; implement `build_kit_graph() -> StateGraph` and `run_kit_step(step_name: str, state: DomainKitState) -> DomainKitState`
- [X] T0XX [US1] Replace `src/ui/kit_generator.py`: remove single-shot `generate_domain_kit()` function; add thin wrappers that delegate to `run_kit_step()` for use by the Streamlit tab
- [X] T0XX [US1] Rewire "Generate Pack" tab in `src/ui/domain_kits.py`: (a) store `DomainKitState` in `st.session_state["domain_kit_state"]`; (b) call `run_kit_step` per node on button click with progress indicators; (c) after `hitl_review`, render 3 editable `st.text_area` widgets (one per YAML); (d) if existing domain detected, render diff view before Approve button; (e) show validation error warnings in degraded-HITL mode (retry exhausted); (f) single "Approve & Save All" button calls `run_kit_step("commit_to_disk", state)` with user edits merged
- [X] T0XX [US1] Fix post-commit "Run Pipeline" navigation in `app.py`: write `st.session_state["_mode_override"] = "Pipeline"` and pre-selected domain when "Run Pipeline" button clicked; consume and pop `_mode_override` in sidebar radio render on next rerun
- [X] T0XX [P] [US1] Add unit tests for `DomainKitGraph` node functions in `tests/unit/test_domain_kit_graph.py`: mock `call_llm_json`; cover analyze_csv parsing, validate_enrichment_rules retry counter, commit_to_disk `.bak` write and audit log, conditional routing logic
- [X] T0XX [P] [US1] Add unit tests for `validate_enrichment_rules_yaml()` in `tests/unit/test_domain_kit_validator.py`: one test per check, covering both error and warning levels, and the clean-pass case

**Checkpoint**: US1 fully functional — generation, retry, HITL, file write, nav fix all verified manually with `pharma_sample.csv`.

---

## Phase 4: User Story 2 — Generate Custom Block via AI Agent (Priority: P2)

**Goal**: ScaffoldGraph generates a Python `Block` subclass with syntax-fix retry and HITL gate before writing to `custom_blocks/`.

**Independent Test**: Select existing domain, enter extraction description → observe scaffold generation → verify class name follows `<domain>__<name>` convention, syntax validated → approve → confirm `.py` file written to `domain_packs/<domain>/custom_blocks/`.

### Implementation for User Story 2

- [X] T0XX [US2] Implement `generate_scaffold` node in `src/agents/domain_kit_graph.py`: call `SCAFFOLD_GENERATE_PROMPT` (or `SCAFFOLD_FIX_PROMPT` when `syntax_error` present) via LLM; strip markdown fences; set `scaffold_source`
- [X] T0XX [US2] Implement `validate_syntax` node in `src/agents/domain_kit_graph.py`: call `ast.parse(scaffold_source)`; set `syntax_valid`, `syntax_error` (empty string if valid), increment `retry_count`
- [X] T0XX [US2] Implement `fix_scaffold` node in `src/agents/domain_kit_graph.py`: re-call LLM with `SCAFFOLD_FIX_PROMPT` injecting `syntax_error` and previous `scaffold_source`; update `scaffold_source`
- [X] T0XX [US2] Implement scaffold `hitl_review` node (sets `pending_review=True`) and `save_to_custom_blocks` node (writes `user_source` or `scaffold_source` to `domain_packs/<domain>/custom_blocks/<block_name>.py`, appends audit entry with action `scaffold`) in `src/agents/domain_kit_graph.py`
- [X] T0XX [US2] Build `ScaffoldGraph` in `src/agents/domain_kit_graph.py`: wire 5 nodes, add conditional edge from `validate_syntax` → `fix_scaffold` when not valid and `retry_count < 2`, else → `hitl_review`; implement `build_scaffold_graph() -> StateGraph` and `run_scaffold_step(step_name: str, state: ScaffoldState) -> ScaffoldState`
- [X] T0XX [US2] Replace `src/ui/block_scaffolder.py`: remove single-shot `generate_block_scaffold()` function; add thin wrappers delegating to `run_scaffold_step()`
- [X] T0XX [US2] Rewire "Block Scaffold" tab in `src/ui/domain_kits.py`: store `ScaffoldState` in `st.session_state["scaffold_state"]`; call `run_scaffold_step` per node; show syntax validation status badge; show syntax error in degraded-HITL mode (retry exhausted); render editable `st.text_area` for Python source; "Approve & Save" button calls `run_scaffold_step("save_to_custom_blocks", state)` with `user_source` set
- [X] T0XX [P] [US2] Add unit tests for `ScaffoldGraph` nodes in `tests/unit/test_domain_kit_graph.py`: cover `ast.parse` valid/invalid branches, retry counter, `save_to_custom_blocks` file write, audit log entry

**Checkpoint**: US2 fully functional — scaffold, syntax retry, HITL, file write verified manually with a CPT-code extraction description.

---

## Phase 5: User Story 3 — Preview and Validate Domain Pack (Priority: P3)

**Goal**: Preview/Validate tab requires a CSV upload and runs all 5 deterministic checks, surfacing errors and warnings without any LLM call.

**Independent Test**: Select any domain pack, upload its source CSV → click "Run Validation" → confirm all 5 checks execute; introduce a known defect (remove `__generated__` from `block_sequence.yaml`) → re-run → confirm error appears with correct message.

### Implementation for User Story 3

- [X] T0XX [US3] Update "Preview/Validate" tab in `src/ui/domain_kits.py`: add `st.file_uploader` for CSV (required before Validate button is enabled); parse uploaded CSV headers; pass headers to `validate_enrichment_rules_yaml()`; render results grouped by error/warning with `level`, `check`, and `message` fields; also render fully resolved block sequence (sentinel + stage expansion) when pack is valid
- [X] T030 [US3] Verify `validate_enrichment_rules_yaml()` produces correct `ValidationIssue` output against `domain_packs/nutrition/` and a synthetic defective pack (missing sentinel, wrong dq_score order, duplicate field name); fix any discrepancies found in T004 implementation
- [X] T031 [P] [US3] Add unit tests for all 5 validator check scenarios in `tests/unit/test_domain_kit_validator.py`: missing `__generated__` sentinel → error; `dq_score_pre` not first → warning; missing custom block `.py` → error; enrichment field duplicates CSV header → warning; enrichment field duplicates custom block name → warning; all-valid pack → empty list

**Checkpoint**: US3 fully functional — validator runs on all existing domain packs without false positives.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [X] T032 [P] Add integration tests in `tests/integration/test_domain_kit_generation.py`: for each of 4 fixture CSVs (`healthcare_sample.csv`, `nutrition_sample.csv`, `pharma_sample.csv`, `fda_recalls_sample.csv`), run full `DomainKitGraph` invocation (mocked HITL), then call `validate_enrichment_rules_yaml()` on generated output and assert zero errors
- [X] T033 [P] Update `specs/019-agentic-domain-kit/quickstart.md` with final CLI commands, test invocations, and any constraint changes discovered during implementation
- [X] T034 Audit `src/agents/domain_kit_prompts.py` for hardcoded domain-specific field names (grep for `allergens`, `primary_category`, `dietary_tags`, `is_organic`); fix any found; confirm SC-002 test still passes; update `CLAUDE.md` Active Technologies section with new modules

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 — no dependency on US2 or US3
- **US2 (Phase 4)**: Depends on Phase 2 — no dependency on US1 or US3 (shares the same `domain_kit_graph.py` file, so coordinate writes)
- **US3 (Phase 5)**: Depends on T004 (`validate_enrichment_rules_yaml`) from Phase 2 — independently testable
- **Polish (Phase 6)**: Depends on all user story phases

### User Story Dependencies

- **US1**: After Phase 2 — independent
- **US2**: After Phase 2 — shares `domain_kit_graph.py` with US1; if working in parallel, split file ownership per session
- **US3**: After T004 — `validate_enrichment_rules_yaml()` is the only hard dependency

### Within Each User Story

- Node implementations (T007–T014) must precede graph assembly (T015)
- Graph assembly must precede UI rewiring (T016–T017)
- UI rewiring must precede manual checkpoint validation
- Unit tests (T019–T020) can be written in parallel with node implementation

### Parallel Opportunities

- T001 and T002: different files, run in parallel
- T019 and T020: different files, run in parallel
- T028 and T019/T020: different test cases in same file — sequential
- T032 and T033: different files, run in parallel
- US1 and US3: US3 can start after T004 (Phase 2) while US1 is still in progress

---

## Parallel Example: User Story 1

```bash
# After Phase 2 completes, these can run in parallel (different files):
Task: T019 — unit tests for graph nodes in tests/unit/test_domain_kit_graph.py
Task: T020 — unit tests for validator in tests/unit/test_domain_kit_validator.py

# Sequential within domain_kit_graph.py (same file):
T007 → T008 → T009 → T010 → T011 → T012 → T013 → T014 → T015
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T002)
2. Complete Phase 2: Foundational (T003–T006) — **CRITICAL BLOCK**
3. Complete Phase 3: US1 (T007–T020)
4. **STOP and VALIDATE**: run with `pharma_sample.csv`, check 3 YAMLs pass Preview, check audit log, check nav fix
5. Proceed to US2 and US3 if MVP approved

### Incremental Delivery

1. Setup + Foundational → Types, prompts, and validator ready
2. US1 → End-to-end generation with HITL → validate with all 4 fixtures → **MVP**
3. US2 → Scaffold agent with syntax retry
4. US3 → Enhanced Preview with CSV-upload gate
5. Polish → Integration tests, docs, constitution audit

---

## Notes

- `[P]` = different files, no in-flight dependencies — safe to parallelize
- `[Story]` label maps directly to spec.md user story for traceability
- `src/agents/domain_kit_graph.py` is a single shared file for both graphs — serialize writes across US1 and US2 phases
- `src/agents/graph.py` and `src/agents/prompts.py` are NOT touched by any task
- Each commit_to_disk / save_to_custom_blocks call is gated — no file write without explicit user approve action
- Verify `ast.parse` retry loop stays ≤2 attempts; same for enrichment rules retry
