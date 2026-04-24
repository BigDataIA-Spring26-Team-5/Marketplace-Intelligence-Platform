# Tasks: Domain Pack UI Builder

**Input**: Design documents from `specs/018-domain-kit-ui-builder/`  
**Branch**: `018-domain-kit-ui-builder`  
**Date**: 2026-04-24

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no pending dependencies)
- **[Story]**: User story this task belongs to (US1–US5)

---

## Phase 1: Setup

**Purpose**: Verify environment and create new module stubs so parallel work doesn't collide on file creation.

- [X] T001 Confirm `domain_packs/` is writable by Streamlit process on VM; document finding in `specs/018-domain-kit-ui-builder/research.md` under a new "Environment" entry

---

## Phase 2: Foundational — Tier 2 Parameterization

**Purpose**: Make the pipeline kernel truly domain-agnostic. **MUST complete before any UI user story can be tested end-to-end.**

**⚠️ CRITICAL**: A1 (T002) must land before A2–A6 (T003–T007). A2–A6 are parallel with each other.

- [X] T002 Extend `EnrichmentRulesLoader` in `src/enrichment/rules_loader.py`: store raw parsed YAML as `self._raw`; add properties `enrichment_column_names`, `llm_categories_string`, `text_columns` (with `["product_name","ingredients","category"]` fallback), `llm_rag_context_field`; add method `load_prompt_examples(domain)` returning `examples` list from `domain_packs/<domain>/prompt_examples.yaml`

- [X] T003 [P] Fix `src/blocks/llm_enrich.py` lines 16 and 19: remove module-level `ENRICHMENT_COLUMNS` and `_SAFETY_FIELDS`; derive them per call from `EnrichmentRulesLoader(domain).enrichment_column_names` and `.safety_field_names()` after the existing `rules_loader` init on line 39; update all downstream references in the same file to use local variables

- [X] T004 [P] Fix `src/enrichment/llm_tier.py` lines 36–54: remove module-level `CATEGORIES`, `SYSTEM_PROMPT`, `BATCH_SYSTEM_PROMPT`; add `_build_prompts(domain: str) -> tuple[str, str, str]` that calls `EnrichmentRulesLoader(domain)`, builds categories string from `llm_categories_string` (food 20-item string fallback if empty), and constructs both prompts via f-string; update `llm_enrich()` and async batch callers to invoke `_build_prompts(domain)` using `domain` already available in their config/call chain

- [X] T005 [P] Fix `src/enrichment/deterministic.py` line 42: replace `text_cols = ["product_name", "ingredients", "category"]` with `text_cols = EnrichmentRulesLoader(domain).text_columns`; confirm `domain` is threaded from `LLMEnrichBlock.run()` config dict into `deterministic_enrich()` — add it as a parameter if missing

- [X] T006 [P] Fix `src/agents/guardrails.py` lines 97 and 112–117: keep `SAFETY_COLUMNS` and `VALID_CATEGORIES` frozensets as module-level fallbacks; add `get_safety_columns(domain: str) -> frozenset` and `get_valid_categories(domain: str) -> frozenset` functions that load from `EnrichmentRulesLoader(domain)` with frozenset fallback; grep all internal uses of `SAFETY_COLUMNS` and `VALID_CATEGORIES` within guardrails.py and update callers that have `domain` available to use the new getters

- [X] T007 [P] Fix `src/agents/prompts.py` lines 94–105 and 283–290: remove all hardcoded food column names (`serving_size`, `ingredient_statement`, `foodNutrients`, `gtinUpc`, `serving_size_unit`) from base prompt strings; add `{domain_examples}` placeholder in `SCHEMA_ANALYSIS_PROMPT` and `FIRST_RUN_SCHEMA_PROMPT`; populate at call site in `analyze_schema_node` via `EnrichmentRulesLoader.load_prompt_examples(domain)` formatted as bullet strings

- [X] T008 Write unit tests in `tests/unit/test_rules_loader_extensions.py`: test all four new `EnrichmentRulesLoader` properties with nutrition fixture (assert existing behaviour unchanged) and a synthetic healthcare fixture YAML; test `text_columns` fallback when key absent; test `llm_categories_string` returns correct comma-separated string; test `safety_field_names()` returns only deterministic field names

- [X] T009 Write unit tests in `tests/unit/test_tier2_parameterization.py`: parametrize over a healthcare-domain loader fixture; assert `get_safety_columns("healthcare_test")` returns `{"icd10_codes"}` not `{"allergens"}`; assert `get_valid_categories("healthcare_test")` returns healthcare categories not food categories; assert running `deterministic_enrich` with healthcare domain does not reference food columns

**Checkpoint**: Run `poetry run pytest tests/unit/test_rules_loader_extensions.py tests/unit/test_tier2_parameterization.py` — all pass. Run `poetry run python demo.py` — nutrition pipeline output unchanged (SC-002 prerequisite).

---

## Phase 3: User Story 1 — Generate Domain Pack from Sample Data (Priority: P1) 🎯 MVP

**Goal**: DE types domain name + description, uploads sample CSV, clicks Generate → AI produces three YAML files → DE reviews inline → clicks Commit → domain appears in pipeline launcher.

**Independent Test**: Upload a 20-row healthcare CSV; verify three YAML files appear in editable text areas, all pass `yaml.safe_load()`, and the new domain name appears in the pipeline launcher domain selector — without running a pipeline pass.

- [X] T010 Create `src/ui/kit_generator.py`: implement `generate_domain_kit(domain_name: str, description: str, csv_content: str) -> dict[str, str]`; parse CSV headers + first 5 rows with `io.StringIO` + `csv.reader`; build LLM prompt with domain name, description, markdown sample table, nutrition `enrichment_rules.yaml` content as few-shot example, blank YAML templates for all three output files, and generation constraints (`domain:` key required, `__generated__` sentinel required in block_sequence, minimum 1 enrichment field, minimum 1 safety field); call `call_llm_json(get_orchestrator_llm(), prompt)`; validate each returned value with `yaml.safe_load()`; return `{"enrichment_rules.yaml": ..., "prompt_examples.yaml": ..., "block_sequence.yaml": ...}`; on LLM error return partial dict with `{"error": "<message>"}` value per failed file

- [X] T011 Create `src/ui/domain_kits.py`: implement `render_domain_kits_page()` with `st.title("Domain Packs")`, writable-check banner (`_check_writability()`), and `st.tabs(["Generate Pack", "Block Scaffold", "Preview / Validate", "Manage Packs"])` shell; implement `_check_writability()` that checks `os.access(DOMAIN_PACKS_DIR, os.W_OK)` and renders `st.error(...)` banner if false

- [X] T012 [US1] Implement `_render_generate_tab()` in `src/ui/domain_kits.py`: `st.text_input("Domain name")` with real-time slug validation (`[a-z][a-z0-9_]*`); `st.text_area("Domain description")`; `st.file_uploader("Sample CSV", type=["csv"])`; `st.button("Generate")` that calls `generate_domain_kit()` with `st.spinner("Generating domain pack…")` wrapping the call, stores result in `st.session_state["pack_gen"]`, shows `st.error(…) + st.button("Retry")` on failure with all inputs preserved; render three `st.text_area()` editors for generated files with per-file YAML validation on change; `st.button("Validate")` that checks YAML syntax + required keys; `st.button("Commit", disabled=not validated)` that writes files to `domain_packs/<domain_name>/` and appends `PackAuditEntry` to `domain_packs/<domain_name>/.audit.jsonl`; post-commit `st.success(…)` with "Run Pipeline" button that sets `st.session_state["app_mode"] = "Pipeline"` and `st.session_state["domain"] = domain_name`

- [X] T013 [US1] Extend `app.py` line 615: change `st.radio("Mode", ["Pipeline", "Observability"])` to `st.radio("Mode", ["Pipeline", "Observability", "Domain Packs"])`; add `elif mode == "Domain Packs": from src.ui.domain_kits import render_domain_kits_page; render_domain_kits_page()` dispatch block after the existing Observability branch

- [X] T014 [US1] Implement `PackAuditEntry` writer helper in `src/ui/domain_kits.py`: function `_append_audit(domain_name, action, outcome, detail)` that appends one JSON line to `domain_packs/<domain_name>/.audit.jsonl`, creating the file if absent; called by Generate, Commit, and Delete flows

- [X] T015 [US1] Write unit tests in `tests/unit/test_kit_generator.py`: mock `call_llm_json` to return valid YAML dict; assert output has all three file keys; assert each value passes `yaml.safe_load()`; assert `block_sequence.yaml` value contains `__generated__`; test LLM error path — assert returns partial dict with `"error"` key, no exception raised

**Checkpoint**: `poetry run streamlit run app.py` → "Domain Packs" tab visible in sidebar; upload a sample CSV → three YAML text areas populate; Commit writes to `domain_packs/<test_domain>/`; domain appears in Pipeline mode domain selector.

---

## Phase 4: User Story 2 — Scaffold a Custom Extraction Block (Priority: P2)

**Goal**: DE describes what to extract in plain language → system generates syntactically valid Python `Block` subclass scaffold → DE downloads after checkbox acknowledgment.

**Independent Test**: Enter "extract ICD-10 codes from diagnosis_text column" → generated `.py` content parses via `ast.parse()` without error → download button enabled only after security checkbox checked.

- [X] T016 [P] Create `src/ui/block_scaffolder.py`: implement `generate_block_scaffold(domain_name: str, extraction_description: str) -> tuple[str, bool]`; build LLM prompt with `Block` base class contract (attributes + method signatures from `src/blocks/base.py`), `domain_packs/nutrition/custom_blocks/extract_allergens.py` content as few-shot example, and user's extraction description; call LLM (plain text mode, not JSON); strip markdown fences if present; validate with `ast.parse(source)`; return `(source, True)` on success or `(source_with_inline_syntax_comment, False)` on parse failure

- [X] T017 [US2] Implement `_render_scaffold_tab()` in `src/ui/domain_kits.py`: `st.selectbox("Domain")` populated from registered domain names; `st.text_area("Describe what to extract", placeholder="Extract ICD-10 codes from the diagnosis_text column…")`; `st.button("Generate Block")` that calls `generate_block_scaffold()` with spinner and stores result in `st.session_state["scaffold"]`; `st.code(content, language="python")` display; `st.warning("Security notice: this file will execute on the server when placed in custom_blocks/")`; `st.checkbox("I understand this file will execute on the server when placed in custom_blocks/")` stored as `st.session_state["scaffold_ack"]`; `st.download_button(…, disabled=not (syntax_valid and ack))` that triggers file download

- [X] T018 [US2] Write unit tests in `tests/unit/test_block_scaffolder.py`: mock LLM to return a syntactically valid `Block` subclass string; assert `ast.parse()` succeeds and `syntax_valid` is `True`; assert returned class inherits from `Block` (check source string); test markdown fence stripping; test malformed LLM return → assert `syntax_valid` is `False` and no exception raised

**Checkpoint**: "Block Scaffold" tab generates valid Python; download button disabled until checkbox checked; downloaded file is syntactically valid Python.

---

## Phase 5: User Story 3 — Run Pipeline with User-Created Domain (Priority: P2)

**Goal**: Committed domain pack (non-food) drives a full pipeline run using that domain's enrichment fields, safety fields, categories, and text columns — with zero food-domain columns in output.

**Independent Test**: Full pipeline run on synthetic healthcare CSV → output CSV has `icd10_codes` column, zero `allergens`/`dietary_tags`/`is_organic` columns.

- [X] T019 [P] [US3] Create `domain_packs/healthcare_test/` fixture: `enrichment_rules.yaml` with `icd10_codes` (deterministic, safety), `medication_names` (deterministic, safety), `diagnosis_category` (llm); `block_sequence.yaml` with standard sequence; `prompt_examples.yaml` with 3 healthcare column mapping examples; `text_columns: [diagnosis_text, medications, procedures]`

- [X] T020 [P] [US3] Create `tests/fixtures/healthcare_sample.csv`: 25-row synthetic file with columns `patient_id`, `discharge_date`, `diagnosis_text` (containing ICD-10 patterns like "E11.9", "I10"), `medications`, `procedures`, `facility_code`

- [X] T021 [US3] Write integration test `tests/integration/test_healthcare_domain_pipeline.py` (mark `@pytest.mark.integration`): run pipeline with `--source tests/fixtures/healthcare_sample.csv --domain healthcare_test`; assert output CSV has `icd10_codes` non-null for rows with ICD patterns; assert `dq_score_pre` and `dq_score_post` columns present; assert none of `allergens`, `dietary_tags`, `is_organic`, `primary_category` appear in output; teardown removes `config/schemas/healthcare_test_schema.json`

**Checkpoint**: `poetry run pytest tests/integration/test_healthcare_domain_pipeline.py` passes. SC-002 verified.

---

## Phase 6: User Story 4 — Preview and Validate Domain Pack Before Committing (Priority: P3)

**Goal**: DE clicks Preview on a domain pack (generated or hand-edited) → system shows resolved block execution order, enrichment field list, and any validation errors — without writing to `domain_packs/`.

**Independent Test**: Submit a `block_sequence.yaml` referencing a non-existent block name → warning displayed listing the unknown name; submit `enrichment_rules.yaml` with an LLM-strategy field in the safety list → error displayed; no files written to `domain_packs/` in either case.

- [X] T022 [P] [US4] Implement block sequence resolver in `src/ui/domain_kits.py`: function `_resolve_block_sequence(block_sequence_yaml: str, domain: str) -> list[str]` that parses YAML, expands `__generated__` → `[DynamicMappingBlock name]`, expands `dedup_stage` → `["fuzzy_deduplicate", "column_wise_merge", "golden_record_select"]`, returns ordered list; also returns list of unrecognized block names by checking against `BlockRegistry` known names

- [X] T023 [P] [US4] Implement enrichment rules validator in `src/ui/domain_kits.py`: function `_validate_enrichment_rules(enrichment_yaml: str) -> tuple[list[str], list[str]]` returning `(errors, warnings)`; errors: missing `domain` key, missing `fields` key, LLM-strategy field whose name appears in `safety_field_names()`; warnings: zero fields declared, fields with no patterns

- [X] T024 [US4] Implement `_render_preview_tab()` in `src/ui/domain_kits.py`: `st.selectbox("Domain")` for existing packs OR text areas for paste-in YAML; `st.button("Preview")` that loads/parses files, calls `_resolve_block_sequence()` and `_validate_enrichment_rules()`, displays resolved block order as numbered list in `st.table()`, enrichment field summary as `st.json()`, errors as `st.error()` list, warnings as `st.warning()` list; no writes to `domain_packs/` at any point in this tab

**Checkpoint**: Preview tab shows correct block execution order for nutrition domain; submitting a YAML with LLM-strategy safety field shows error without touching `domain_packs/`.

---

## Phase 7: User Story 5 — Manage Registered Domain Packs (Priority: P3)

**Goal**: DE sees all registered domain packs (built-in and user-created) with type labels; can delete user-created packs; built-in packs protected.

**Independent Test**: List packs → built-in `nutrition` shows "built-in" label and no delete button; user-created `healthcare_test` shows "user-created" label and delete button; deleting `healthcare_test` removes `domain_packs/healthcare_test/` and it disappears from the list.

- [X] T025 [P] [US5] Implement `_list_domain_packs()` in `src/ui/domain_kits.py`: scans `domain_packs/` subdirectories; for each, runs `subprocess.run(["git", "ls-files", f"domain_packs/{name}/"], capture_output=True, text=True)` to detect built-in (non-empty stdout = built-in); reads filesystem `mtime` as `created_at`; loads `EnrichmentRulesLoader(name)` for `enrichment_column_names` and `safety_field_names()`; returns list of `DomainPack` dicts; fallback: if `git` not available, classify all as "user-created" with banner warning

- [X] T026 [P] [US5] Implement `PackAuditEntry` reader in `src/ui/domain_kits.py`: function `_load_audit_log(domain_name) -> list[dict]` that reads `domain_packs/<domain>/.audit.jsonl` (one JSON object per line); returns last 20 entries; returns `[]` if file absent

- [X] T027 [US5] Implement `_render_manage_tab()` in `src/ui/domain_kits.py`: call `_list_domain_packs()`; render `st.dataframe()` with columns Domain, Type, Created, Enrichment Fields, Safety Fields; for each user-created pack render `st.button(f"Delete {name}")` with two-step confirm (first click sets `st.session_state[f"confirm_delete_{name}"] = True`, second click confirmed shows `st.error()` confirm prompt); on confirmed delete: call `_append_audit(name, "delete", "pending", "pre-rmtree")`, then `shutil.rmtree(domain_packs/<name>)`, then reload; for built-in packs render `st.caption("Protected — built-in domain pack")` instead of delete button; render last 20 audit entries for selected domain from `_load_audit_log()`

**Checkpoint**: Manage tab lists nutrition/safety/pricing as built-in with no delete button; deleting `healthcare_test` removes the directory and it disappears from the list on next render.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [X] T028 [P] Update `domain_packs/nutrition/enrichment_rules.yaml`: add `text_columns: [product_name, ingredients, category]` as top-level key (documents the default, matches existing fallback; backward-compatible)

- [X] T029 [P] Update `CLAUDE.md` Active Technologies section: add `src/ui/domain_kits.py`, `src/ui/kit_generator.py`, `src/ui/block_scaffolder.py`; add `domain_packs/<domain>/.audit.jsonl` to filesystem layout note; note "Domain Packs" as third Streamlit mode

- [X] T030 Validate constitution alignment: run `poetry run python demo.py` and confirm nutrition pipeline output unchanged; confirm `dq_score_pre`/`dq_score_post` present; confirm no food columns in `healthcare_test` output from T021

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: T002 must complete before T003–T009; T003–T007 parallel after T002
- **Phase 3 (US1)**: Requires Phase 2 complete; T010–T011 parallel; T012 requires T011; T013–T014 parallel after T012; T015 parallel after T010
- **Phase 4 (US2)**: Requires Phase 2 complete; T016 parallel with Phase 3; T017 requires T011 (tab shell) and T016
- **Phase 5 (US3)**: Requires Phase 2 complete; T019–T020 parallel; T021 requires T019 + T020
- **Phase 6 (US4)**: Requires T011 (tab shell); T022–T023 parallel; T024 requires T022 + T023
- **Phase 7 (US5)**: Requires T011 (tab shell); T025–T026 parallel; T027 requires T025 + T026
- **Phase 8 (Polish)**: Requires all prior phases complete

### User Story Dependencies

- **US1 (P1)**: Blocked on Phase 2 (Tier 2 must pass before end-to-end pipeline test)
- **US2 (P2)**: Blocked only on T011 (tab shell); can proceed in parallel with US1 implementation
- **US3 (P2)**: Blocked on Phase 2 (needs Tier 2 to confirm zero food columns); fixture authoring (T019–T020) can proceed immediately
- **US4 (P3)**: Blocked only on T011 (tab shell) + T022–T023; fully independent of US1–US3 runtime
- **US5 (P3)**: Blocked only on T011 (tab shell); fully independent of US1–US4 runtime

### Within Each Phase

- T002 before T003–T007 (loader extensions before consumers)
- T010–T011 before T012 (generator + tab shell before generate tab content)
- T016 before T017 (scaffolder before scaffold tab)
- T022 + T023 before T024 (resolver + validator before preview tab)
- T025 + T026 before T027 (listers before manage tab)

---

## Parallel Execution Examples

### Phase 2 — After T002 lands

```
Parallel batch A (T003–T007 — different files, no cross-deps):
  T003: src/blocks/llm_enrich.py
  T004: src/enrichment/llm_tier.py
  T005: src/enrichment/deterministic.py
  T006: src/agents/guardrails.py
  T007: src/agents/prompts.py

Parallel batch B (tests — no file conflicts):
  T008: tests/unit/test_rules_loader_extensions.py
  T009: tests/unit/test_tier2_parameterization.py
```

### Phase 3+4 — After Phase 2 complete

```
Parallel batch C:
  T010: src/ui/kit_generator.py        (US1)
  T011: src/ui/domain_kits.py shell    (US1, US2, US4, US5 all need this)
  T016: src/ui/block_scaffolder.py     (US2)
  T019: domain_packs/healthcare_test/  (US3 fixture)
  T020: tests/fixtures/healthcare_sample.csv

After T010 + T011 complete:
  T012: _render_generate_tab()
  T015: tests/unit/test_kit_generator.py

After T011 + T016 complete:
  T017: _render_scaffold_tab()
  T018: tests/unit/test_block_scaffolder.py

After T011:
  T022 + T023 (parallel): resolver + validator (US4)
  T025 + T026 (parallel): lister + audit reader (US5)
```

---

## Implementation Strategy

### MVP (Phase 1 + 2 + 3 only)

1. T001 — environment check
2. T002–T009 — Tier 2 parameterization + tests
3. T010–T015 — domain pack generation UI (US1)
4. **STOP**: Demo generating a healthcare pack, committing, and launching pipeline

### Full Delivery Order

1. Setup → Foundational (T001–T009)
2. US1 MVP (T010–T015) → validate SC-001, SC-003, SC-005
3. US2 (T016–T018) → validate FR-010, FR-011
4. US3 (T019–T021) → validate SC-002 (zero food columns)
5. US4 (T022–T024) → validate SC-004
6. US5 (T025–T027) → validate SC-006, SC-007
7. Polish (T028–T030)

---

## Notes

- All `[P]` tasks touch different files — safe to run concurrently
- T002 is the single serial bottleneck at the start of Phase 2; unblock it first
- T011 (`domain_kits.py` shell with tab structure) is the single bottleneck for all four UI user stories — create the shell with empty tab functions first so US2/US4/US5 work can proceed in parallel with US1 tab content
- Integration test (T021) requires real LLM call unless mocked — mark `@pytest.mark.integration` and exclude from default pytest run with `-m "not integration"`
- Do not delete `domain_packs/healthcare_test/` after authoring — it serves as a reference fixture and test baseline
