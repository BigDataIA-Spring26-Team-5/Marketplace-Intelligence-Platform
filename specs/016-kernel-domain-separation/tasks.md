# Tasks: Kernel / Domain Separation

**Input**: Design documents from `specs/016-kernel-domain-separation/`
**Branch**: `016-kernel-domain-separation`

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no shared dependencies)
- **[Story]**: User story from spec.md (US1–US5)

---

## Phase 1: Setup

**Purpose**: Create domain pack directory skeleton before any code changes.

- [x] T001 Create `domain_packs/` directory at repo root with `.gitkeep`
- [x] T002 Create `domain_packs/nutrition/custom_blocks/` directory with `.gitkeep`
- [x] T003 [P] Create `domain_packs/pricing/` directory with `.gitkeep`
- [x] T004 [P] Create `domain_packs/safety/` directory with `.gitkeep`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Populate domain pack YAML artifacts and create the new `rules_loader.py`. All user story phases depend on these files existing.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T005 Create `domain_packs/nutrition/block_sequence.yaml` — extract the full nutrition sequence from `BlockRegistry.get_default_sequence()` in `src/registry/block_registry.py`. Sequence must list individual block names (no `enrich_stage` composite per FR-012): `dq_score_pre`, `__generated__`, `strip_whitespace`, `lowercase_brand`, `remove_noise_words`, `strip_punctuation`, `nutrition__extract_quantity_column`, `fuzzy_deduplicate`, `column_wise_merge`, `golden_record_select`, `nutrition__extract_allergens`, `llm_enrich`, `dq_score_post`
- [x] T006 [P] Create `domain_packs/nutrition/enrichment_rules.yaml` — extract `CATEGORY_RULES`, `DIETARY_RULES`, and `ORGANIC_PATTERN` from `src/enrichment/deterministic.py` into YAML format per the `enrichment_rules.yaml` schema in `contracts/domain-pack-contract.md`. Mark `allergens`, `dietary_tags`, `is_organic` as `strategy: deterministic`. Mark `primary_category` as `strategy: llm`
- [x] T007 [P] Create `domain_packs/nutrition/prompt_examples.yaml` — extract all food-domain few-shot column mapping examples (brand_name, product_name, ingredients, etc.) from `src/agents/prompts.py` `SCHEMA_ANALYSIS_PROMPT` and `FIRST_RUN_SCHEMA_PROMPT` strings into structured YAML per `contracts/domain-pack-contract.md`
- [x] T008 [P] Create `domain_packs/pricing/block_sequence.yaml` — extract pricing sequence from `BlockRegistry.get_default_sequence(domain="pricing")` in `src/registry/block_registry.py`. Keep `keep_quantity_in_name` as a kernel block name (not namespaced). No enrichment blocks.
- [x] T009 [P] Create `domain_packs/safety/block_sequence.yaml` — extract safety sequence from `src/registry/block_registry.py`. Safety has no enrichment blocks; sequence ends at `dedup_stage` expansion then `dq_score_post`
- [x] T010 Create `src/enrichment/rules_loader.py` — new module with `EnrichmentRulesLoader` class. Reads `domain_packs/<domain>/enrichment_rules.yaml`, builds compiled `re.Pattern` objects, splits fields into `deterministic_fields` and `llm_fields` lists. Must expose the same rule structures that `deterministic_enrich()` in `src/enrichment/deterministic.py` currently consumes from the hardcoded constants

**Checkpoint**: Domain pack files exist on disk. `rules_loader.py` is importable. No kernel code changed yet.

---

## Phase 3: US2 (P1) — Registry Reads Block Sequence from Domain Pack

**Goal**: `BlockRegistry.get_default_sequence(domain)` reads from `domain_packs/<domain>/block_sequence.yaml`. Zero inline domain branching in registry.

**Independent Test**: Call `get_default_sequence("nutrition")` → result matches `domain_packs/nutrition/block_sequence.yaml` exactly. Call `get_default_sequence("unknown_domain")` → returns `FALLBACK_SEQUENCE`. Call `get_default_sequence("pricing")` → returns pricing YAML sequence.

- [x] T011 Add `FALLBACK_SEQUENCE` constant and `_load_domain_sequence(domain: str) -> list[str]` private function to `src/registry/block_registry.py`. Function reads `domain_packs/<domain>/block_sequence.yaml`; returns `FALLBACK_SEQUENCE` with a WARNING log if file absent; raises `BlockNotFoundError` at init if any listed name is unresolvable
- [x] T012 Replace inline `if domain == "pricing" / else` branching in `BlockRegistry.get_default_sequence()` in `src/registry/block_registry.py` with a call to `_load_domain_sequence(domain)`
- [x] T013 [P] Replace inline domain branching in `BlockRegistry.get_silver_sequence()` in `src/registry/block_registry.py` with `_load_domain_sequence(domain)`. Silver sequence files (`domain_packs/<domain>/block_sequence_silver.yaml`) may reuse or subset the full sequence — add a `silver_sequence` key to the YAML if domain needs a distinct silver shape, otherwise derive from full sequence by stripping post-dedup blocks
- [x] T014 [P] Replace inline domain branching in `BlockRegistry.get_gold_sequence()` in `src/registry/block_registry.py` with domain-pack-driven logic. Add `gold_sequence` key to YAML if domain needs a distinct gold shape
- [x] T015 Add `_discover_domain_custom_blocks()` to `src/registry/block_registry.py`. Scans `domain_packs/*/custom_blocks/*.py` using `importlib.util.spec_from_file_location`. Registers each discovered `Block` subclass under the key `<domain>__<block.name>`. Call from `BlockRegistry.__init__` after `_load_generated_blocks()`
- [x] T016 Remove `enrich_stage` entry from `_STAGES` dict in `src/registry/block_registry.py` (FR-012). `dedup_stage` remains. Update `expand_stage()` docstring accordingly
- [x] T017 [P] Unit test: update `tests/unit/test_block_registry.py` — add tests for: (a) domain pack sequence load returns correct list, (b) missing domain pack returns `FALLBACK_SEQUENCE`, (c) unknown block name in YAML raises `BlockNotFoundError`, (d) custom block file in `domain_packs/<domain>/custom_blocks/` is discovered and registered under `<domain>__<name>`

**Checkpoint**: `poetry run pytest tests/unit/test_block_registry.py` passes. `get_default_sequence("nutrition")` reads YAML; `get_default_sequence("unknown")` returns fallback.

---

## Phase 4: US3 + US4 (P2) — Runner Column Names + Prompt Injection

**Goal**: Runner derives null-rate and DQ columns from domain schema. Agent 1 loads few-shot examples from domain pack at node entry.

**Independent Test (US3)**: Instantiate `PipelineRunner(registry, domain="retail_inventory")` against a schema with `required: true` on `["sku_id", "product_name"]` only. Block_end Kafka event null_rates keys = `["sku_id", "product_name"]`.

**Independent Test (US4)**: Call `build_schema_analysis_prompt("nutrition")` → returned string contains food examples from `prompt_examples.yaml`. Call with `"unknown_domain"` → returns prompt with generic examples, no crash.

- [x] T018 [P] [US3] Add `domain: str` parameter to `PipelineRunner.__init__` in `src/pipeline/runner.py`. Store as `self.domain`. Default `"nutrition"` for backward compat during transition
- [x] T019 [P] [US3] Replace `NULL_RATE_COLUMNS` constant in `src/pipeline/runner.py` with `_get_null_rate_columns(self) -> list[str]` method. Implementation: `get_domain_schema(self.domain)` → return `[name for name, col in schema.columns.items() if col.required]`. Import `get_domain_schema` from `src.schema.analyzer`
- [x] T020 [P] [US3] Replace `_DQ_COLS` constant in `src/pipeline/runner.py` with domain-derived list in `_compute_block_dq()`. Change signature to `_compute_block_dq(self, df)` and use `self._get_null_rate_columns()` for column derivation. Remove module-level `_DQ_COLS`
- [x] T021 [P] [US3] Update `run_pipeline_node` in `src/agents/graph.py` — pass `domain=state.get("domain", "nutrition")` when constructing `PipelineRunner`. Verify `state["domain"]` is always set before this node (it is — set in `load_source_node`)
- [x] T022 [P] [US4] Add `load_prompt_examples(domain: str) -> list[dict]` to `src/agents/prompts.py`. Reads `domain_packs/<domain>/prompt_examples.yaml`; returns generic placeholder examples if file absent (logs INFO warning)
- [x] T023 [P] [US4] Add `build_schema_analysis_prompt(domain: str) -> str` to `src/agents/prompts.py`. Calls `load_prompt_examples(domain)`, formats examples into the existing few-shot block, returns the full prompt string. Extract the generic prompt template as `SCHEMA_ANALYSIS_PROMPT_TEMPLATE` (the non-example body)
- [x] T024 [US4] Update `analyze_schema_node` in `src/agents/orchestrator.py` to call `build_schema_analysis_prompt(state["domain"])` at node entry instead of using the module-level `SCHEMA_ANALYSIS_PROMPT` constant. Apply same pattern to `FIRST_RUN_SCHEMA_PROMPT` via `build_first_run_prompt(domain)`

**Checkpoint**: `PipelineRunner` accepts `domain` kwarg. `NULL_RATE_COLUMNS` and `_DQ_COLS` constants gone from `runner.py`. Agent 1 prompt contains domain-appropriate few-shot examples.

---

## Phase 5: US1 (P1) — End-to-End Non-Food Domain Validation + Remove Food Imports

**Goal**: A retail domain pipeline runs cleanly; kernel Python has zero hardcoded food column names.

**Independent Test**: `grep -r "allergen\|brand_name\|ingredients\|is_organic\|dietary_tags" src/` returns zero matches in `.py` files (excluding test fixtures and comments). A CLI run with `--domain retail_inventory` against a minimal retail CSV completes without errors.

- [x] T025 Remove food-specific imports from `src/registry/block_registry.py`: `ExtractAllergensBlock`, `ExtractQuantityColumnBlock`, `KeepQuantityInNameBlock`. These will be loaded via `_discover_domain_custom_blocks()` after Phase 6 migration
- [x] T026 Remove `extract_allergens`, `extract_quantity_column` entries from `_BLOCKS` dict in `src/registry/block_registry.py` (they will be discovered as domain custom blocks). Keep `keep_quantity_in_name` in `_BLOCKS` — it stays as a kernel block
- [x] T027 Update `src/enrichment/deterministic.py` — remove `CATEGORY_RULES`, `DIETARY_RULES`, `ORGANIC_PATTERN` constants. Update `deterministic_enrich()` signature to accept `rules: list` parameter (compiled rule objects from `EnrichmentRulesLoader`). Update the function body to use the passed-in rules instead of module-level constants
- [x] T028 Update `src/blocks/llm_enrich.py` — replace direct usage of `CATEGORY_RULES`, `DIETARY_RULES`, `ORGANIC_PATTERN` imported from `deterministic.py` with `EnrichmentRulesLoader(domain).deterministic_fields` / `.llm_fields`. Import `EnrichmentRulesLoader` from `src.enrichment.rules_loader`. Pass `domain` via `LLMEnrichBlock` constructor or from the DataFrame's pipeline context
- [x] T029 Create minimal `domain_packs/retail_inventory/` test pack with `block_sequence.yaml` (cleaning + dedup only, no enrichment) and a stub `schema.json` with `sku_id` and `product_name` as required columns. Used only for end-to-end CLI smoke test
- [x] T030 Run `grep -r "allergen\|brand_name\|ingredients\|is_organic\|dietary_tags" src/` and confirm zero matches in kernel `.py` files. Fix any remaining references found

**Checkpoint**: SC-004 passes. `poetry run python -m src.pipeline.cli --source data/retail_sample.csv --domain retail_inventory` completes (or fails gracefully with "schema not found" — not a food-column error).

---

## Phase 6: US5 (P3) — Physical File Migration

**Goal**: Food-specific block files removed from `src/blocks/`. `deterministic.py` food rules gone. Nutrition pipeline still works via domain pack path.

**Independent Test**: `ls src/blocks/` shows no `extract_allergens.py` or `extract_quantity_column.py`. Nutrition CLI run (`--domain nutrition`) completes with allergen/quantity extraction working via domain pack custom blocks.

- [x] T031 Copy `src/blocks/extract_allergens.py` to `domain_packs/nutrition/custom_blocks/extract_allergens.py`. Update the `Block.name` class attribute from `"extract_allergens"` to `"nutrition__extract_allergens"`. Delete original `src/blocks/extract_allergens.py`
- [x] T032 [P] Copy `src/blocks/extract_quantity_column.py` to `domain_packs/nutrition/custom_blocks/extract_quantity_column.py`. Update `Block.name` to `"nutrition__extract_quantity_column"`. Delete original `src/blocks/extract_quantity_column.py`
- [x] T033 Verify `domain_packs/nutrition/block_sequence.yaml` (created in T005) uses `nutrition__extract_allergens` and `nutrition__extract_quantity_column` — update names if T005 used old names
- [x] T034 Update test files that imported from `src.blocks.extract_allergens` or `src.blocks.extract_quantity_column` — change import paths to load the block via `BlockRegistry.instance().get("nutrition__extract_allergens")` pattern, or import directly from `domain_packs.nutrition.custom_blocks.extract_allergens`
- [x] T035 [P] Add unit test in `tests/unit/test_block_registry.py` confirming `BlockRegistry` discovers `nutrition__extract_allergens` and `nutrition__extract_quantity_column` from `domain_packs/nutrition/custom_blocks/` at init via `importlib`
- [x] T036 Run `poetry run pytest` — all tests must pass. Nutrition pipeline integration test is the primary regression check

**Checkpoint**: SC-003 passes (nutrition tests pass). `src/blocks/` contains no food-specific files.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [x] T037 Run full success criteria validation: execute SC-001 through SC-006 manually. Document results in a comment or PR description
- [x] T038 [P] Update `CLAUDE.md` — add `domain_packs/` to the architecture section; note that block sequences, enrichment rules, and prompt examples now live in domain packs; remove any references to `enrich_stage` composite
- [x] T039 [P] Update `src/registry/block_registry.py` module docstring to remove references to food-domain block names; update `get_default_sequence` docstring to describe domain pack YAML loading
- [x] T040 [P] Update `revamp.md` Phase 2 checklist — mark kernel refactor items (prompt injection, registry reads YAML, runner column names from schema) as complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1
- **Phase 3 (US2)**: Depends on Phase 2 — `block_sequence.yaml` files must exist before registry reads them
- **Phase 4 (US3+US4)**: Depends on Phase 2 — `prompt_examples.yaml` must exist; independent of Phase 3
- **Phase 5 (US1 validation)**: Depends on Phase 3 AND Phase 4 being complete
- **Phase 6 (US5 migration)**: Depends on Phase 5 — physical removal safe only after kernel proven domain-agnostic
- **Phase 7 (Polish)**: Depends on Phase 6

### Parallel Opportunities Within Phases

**Phase 2**: T006, T007, T008, T009 all parallel (different files)
**Phase 3**: T013, T014 parallel with T012; T017 parallel with T011–T016
**Phase 4**: T018–T023 all parallel (different files); T024 depends on T022+T023
**Phase 6**: T031, T032 parallel; T035 parallel with T031–T034

### Parallel Example: Phase 4

```bash
# All of these touch different files — launch together:
Task T018: "Add domain param to PipelineRunner in src/pipeline/runner.py"
Task T019: "Add _get_null_rate_columns() to src/pipeline/runner.py"
Task T022: "Add load_prompt_examples() to src/agents/prompts.py"
Task T023: "Add build_schema_analysis_prompt() to src/agents/prompts.py"
# Then after the above:
Task T020: "_compute_block_dq uses derived cols in src/pipeline/runner.py"
Task T021: "Pass domain to PipelineRunner in src/agents/graph.py"
Task T024: "Update analyze_schema_node in src/agents/orchestrator.py"
```

---

## Implementation Strategy

### MVP (Phases 1–3 only)

1. Phase 1: Create directories
2. Phase 2: Create YAML files + rules_loader.py
3. Phase 3: Registry reads YAML, discovers custom blocks
4. **STOP**: `get_default_sequence("nutrition")` returns domain-pack-driven sequence. `get_default_sequence("unknown")` returns fallback. Zero inline branching in registry.
5. Nutrition pipeline still passes — no regression.

### Full Delivery

1. MVP (Phases 1–3)
2. Phase 4: Runner + prompts (can overlap with Phase 3 if working in parallel)
3. Phase 5: End-to-end validation — retail domain smoke test
4. Phase 6: Physical migration — remove food files from `src/`
5. Phase 7: Polish, CLAUDE.md, PR

### Parallel Team Strategy

- **Developer A**: Phases 1–3 (registry + domain pack YAMLs)
- **Developer B**: Phase 4 (runner + prompts) — can start after Phase 2 completes

---

## Notes

- `keep_quantity_in_name` stays in kernel `_BLOCKS` — not food-specific, referenced by pricing's `block_sequence.yaml`
- Do NOT delete `src/blocks/extract_allergens.py` before Phase 5 — delete happens in T031/T032 only after kernel is proven domain-agnostic in Phase 5
- Silver and gold sequences: if a domain's `block_sequence.yaml` doesn't have `silver_sequence` / `gold_sequence` keys, registry derives them by convention (silver = full sequence minus dedup/enrich; gold = dedup + enrich + dq_post). Exact derivation rule must be decided in T013/T014
- UC2 `block_end` event null_rates field changes shape by domain — downstream Postgres `block_trace` table uses JSONB; no schema migration needed (confirmed in Assumptions)
