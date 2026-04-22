---
description: "Task list for Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation"
---

# Tasks: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Branch**: `aqeel` | **Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)
**Input**: `specs/015-domain-scoped-schemas/`

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1 / US2 / US3 maps to user stories in spec.md
- No tests requested — implementation tasks only

---

## Phase 1: Setup

**Purpose**: Create the `config/schemas/` directory before any schema files are written

- [x] T001 Create `config/schemas/` directory in repo root

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Domain schema files on disk + refactored schema loader. ALL user stories depend on these.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T002 [P] Write `config/schemas/nutrition_schema.json` — exact copy of `config/unified_schema.json` (16 columns + dq_weights). Column set per data-model.md: product_name, brand_owner, brand_name, ingredients, category, serving_size, serving_size_unit, published_date, data_source, allergens, primary_category, dietary_tags, is_organic, dq_score_pre, dq_score_post, dq_delta
- [x] T003 [P] Write `config/schemas/safety_schema.json` — identical content to `config/schemas/nutrition_schema.json` (domain divergence out of scope)
- [x] T004 [P] Write `config/schemas/pricing_schema.json` — 12 columns (nutrition minus allergens, primary_category, dietary_tags, is_organic) per data-model.md; same dq_weights
- [x] T005 Refactor `src/schema/analyzer.py`:
  - Replace `UNIFIED_SCHEMA_PATH = CONFIG_DIR / "unified_schema.json"` with `SCHEMAS_DIR = CONFIG_DIR / "schemas"`
  - Change `_schema_cache: UnifiedSchema | None = None` → `_schema_cache: dict[str, UnifiedSchema] = {}`
  - Rename `get_unified_schema()` → `get_domain_schema(domain: str = "nutrition") -> UnifiedSchema`; load path `SCHEMAS_DIR / f"{domain}_schema.json"`; cache key = domain; FileNotFoundError message = `f"config/schemas/{domain}_schema.json not found. Create it or pass a valid domain."`
  - Update `_reset_schema_cache()` to accept `domain: str | None = None`; clear one or all per data-model.md contract
  - Rename `save_unified_schema(schema)` → `save_domain_schema(schema: UnifiedSchema, domain: str)`; write to `SCHEMAS_DIR / f"{domain}_schema.json"`
  - In `derive_unified_schema_from_source(...)`: use domain param (already present) to call `save_domain_schema(schema, domain)` instead of the old single-path write

**Checkpoint**: `poetry run python -c "from src.schema.analyzer import get_domain_schema; [get_domain_schema(d) for d in ['nutrition','safety','pricing']]"` must not raise.

---

## Phase 3: User Story 1 — Agent 1 Analyzes Against Domain Schema (Priority: P1) 🎯 MVP

**Goal**: Every call site that previously loaded `unified_schema.json` now loads `config/schemas/<domain>_schema.json` via `get_domain_schema(domain)`. Agent 1 prompt text references domain schema, not unified schema.

**Independent Test**: Run `poetry run python demo.py --domain nutrition` and confirm no `unified_schema.json` reference appears in active run path (`grep -rn "unified_schema.json" src/ --include="*.py"` returns zero matches).

- [x] T006 [US1] Update `src/agents/orchestrator.py`:
  - Remove `get_unified_schema` from import, add `get_domain_schema`
  - Line ~229 (`analyze_schema_node`): `get_unified_schema()` → `get_domain_schema(domain)` (domain already extracted at line ~226)
  - Line ~673 (`_deterministic_corrections`): `get_unified_schema()` → `get_domain_schema(domain)` (ensure domain is passed in)
- [x] T007 [US1] Update `src/agents/graph.py` call sites:
  - Remove `get_unified_schema` from import, add `get_domain_schema`
  - Line ~86 (`plan_sequence_node`): `get_unified_schema()` → `get_domain_schema(domain)` (domain extracted at line ~66)
  - Line ~206 (`run_pipeline_node`): `get_unified_schema()` → `get_domain_schema(domain)` (domain extracted at line ~205)
- [x] T008 [P] [US1] Update prompt text in `src/agents/prompts.py`:
  - `SCHEMA_ANALYSIS_PROMPT`: `## Unified Output Schema` → `## Domain Output Schema`; any inline "the unified schema" → "the domain schema (`config/schemas/<domain>_schema.json`)"
  - `FIRST_RUN_SCHEMA_PROMPT`: "There is no unified schema yet" → "There is no domain schema yet for this source"
  - **Do NOT rename the `{unified_schema}` Python format placeholder** — only human-readable text strings change

**Checkpoint**: `grep -rn "unified_schema.json" src/ --include="*.py"` returns zero matches. `get_domain_schema("nutrition")` call succeeds.

---

## Phase 4: User Story 2 — Silver Normalization Enforces Uniform Column Set (Priority: P2)

**Goal**: After block sequence runs, `run_pipeline_node` enforces exact domain schema column set/order. Missing required columns null-filled. `dq_score_pre` recomputed when required columns are added.

**Independent Test**: Run pipeline on a source CSV that lacks one required schema column. Inspect output DataFrame columns — they must match `list(domain_schema.columns.keys())` exactly, with the missing column present as null, and `dq_score_pre` lower than rows where column is non-null.

- [x] T009 [US2] Add `_silver_normalize(df: pd.DataFrame, domain_schema: UnifiedSchema, dq_weights: dict) -> pd.DataFrame` private function to `src/agents/graph.py` per the algorithm in data-model.md:
  - Build `canonical_cols = list(domain_schema.columns.keys())`
  - For each canonical col absent from df: `df[col] = pd.NA`; if `required=True` and `computed=False`, track in `added_required`
  - If `added_required` non-empty: `from src.blocks.dq_score import compute_dq_score; df["dq_score_pre"] = compute_dq_score(df, dq_weights)` (direct call, not via registry)
  - Return `df[canonical_cols]` (drops extra columns, enforces order)
- [x] T010 [US2] Call `_silver_normalize()` in `run_pipeline_node` in `src/agents/graph.py`:
  - Insert immediately after `result_df, audit_log = runner.run_chunked(...)` returns
  - Call: `result_df = _silver_normalize(result_df, unified, config["dq_weights"])` (`unified` is already the loaded domain schema at that point)
  - No state shape change — `result_df` stored as `working_df` as before

**Checkpoint**: Silver output DataFrame has exactly domain schema columns in declaration order. No extra columns, no missing columns.

---

## Phase 5: User Story 3 — Gold Concatenation Produces Domain-Scoped Output (Priority: P3)

**Goal**: Every successful pipeline run writes Silver parquet to `output/silver/<domain>/` then rebuilds `output/gold/<domain>.parquet` from all accumulated Silver files. Domain isolation guaranteed.

**Independent Test**: Run two sequential nutrition runs. `output/gold/nutrition.parquet` row count equals sum of both Silver DataFrames. Run once with safety domain — `output/gold/safety.parquet` exists separately with no nutrition rows.

- [x] T011 [US3] Add Silver local parquet write block in `save_output_node` in `src/agents/graph.py`:
  - Place inside `try:` block, after the existing `if pipeline_mode == "silver": ... else: ...` block (additive, not replacing)
  - Extract `domain = state.get("domain", "nutrition")` and `source_name = Path(source_path).stem`
  - `silver_local_dir = OUTPUT_DIR / "silver" / domain; silver_local_dir.mkdir(parents=True, exist_ok=True)`
  - `df.to_parquet(silver_local_dir / f"{source_name}.parquet", index=False)`
  - Log: `logger.info("Silver: %d rows → %s", len(df), silver_local_path)`
- [x] T012 [US3] Add Gold concatenation block in `save_output_node` in `src/agents/graph.py` immediately after T011's Silver write:
  - `silver_files = sorted(silver_local_dir.glob("*.parquet"))`
  - If no files: `logger.warning("No Silver parquet files for domain '%s' — Gold write skipped", domain)` and skip
  - Else: `gold_df = pd.concat([pd.read_parquet(p) for p in silver_files], ignore_index=True)`
  - `gold_dir = OUTPUT_DIR / "gold"; gold_dir.mkdir(parents=True, exist_ok=True)`
  - `gold_df.to_parquet(gold_dir / f"{domain}.parquet", index=False)`
  - Log: `logger.info("Gold: %d rows → %s", len(gold_df), gold_path)`
- [x] T013 [US3] Update `save_output_node` return dict in `src/agents/graph.py` — add `"silver_local_path": str(silver_local_path)` and `"gold_path": str(gold_path)` keys for run log capture

**Checkpoint**: `output/silver/nutrition/usda_fooddata_sample.parquet` and `output/gold/nutrition.parquet` both exist after a run. Re-running same source produces same row count in Gold (not doubled).

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation updates and test hygiene

- [x] T014 [P] Update `README.md`:
  - Section "Unified schema is auto-generated once, then reused" → retitle and rewrite to describe per-domain schema files at `config/schemas/<domain>_schema.json`
  - Replace `config/unified_schema.json` with `config/schemas/<domain>_schema.json` in architecture description
- [x] T015 [P] Update `CLAUDE.md`:
  - "Unified schema is auto-generated once, then reused" section → rewrite to describe per-domain schema files
  - Remove `config/unified_schema.json` as canonical target schema reference
  - Update "Development Workflow quality gates" if `unified-schema alignment` gate appears → `domain-schema alignment`
- [x] T016 Grep tests for `get_unified_schema` and update any test imports/mocks: `grep -rn "get_unified_schema" tests/ --include="*.py"` — update each hit to `get_domain_schema`
- [x] T017 Run quickstart verification per `specs/015-domain-scoped-schemas/quickstart.md` — confirm all 6 verification steps pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user stories
- **Phase 3 (US1)**: Depends on Phase 2 complete (needs `get_domain_schema` to exist)
- **Phase 4 (US2)**: Depends on Phase 2 complete (needs schema loader) + Phase 3 complete (needs `unified` var to be domain schema)
- **Phase 5 (US3)**: Depends on Phase 4 complete (`_silver_normalize` must run before Silver write so column set is canonical)
- **Phase 6 (Polish)**: Depends on Phases 3–5 complete

### User Story Dependencies

- **US1 (P1)**: After Foundational — no story dependencies
- **US2 (P2)**: After Foundational + US1 (Silver normalize uses `unified` from `get_domain_schema(domain)` set up in US1)
- **US3 (P3)**: After US2 (Silver write uses `result_df` produced by Silver normalization)

### Within Each Phase

- T002, T003, T004 (schema JSON files) are fully parallel — different files
- T005 (analyzer.py refactor) depends on T001 (directory must exist for SCHEMAS_DIR)
- T006 and T007 are independent (different files) — can run in parallel
- T008 (prompts.py) is independent of T006/T007 — can run in parallel with them
- T009 (`_silver_normalize` function) must complete before T010 (call site)
- T011 (Silver write) must complete before T012 (Gold concat uses same `silver_local_dir`)
- T014 and T015 are independent (different files) — parallel

---

## Parallel Opportunities

```bash
# Phase 2 — run in parallel:
Task T002: Write config/schemas/nutrition_schema.json
Task T003: Write config/schemas/safety_schema.json
Task T004: Write config/schemas/pricing_schema.json
# T005 follows after T001 (dir creation)

# Phase 3 — run T006 and T007 in parallel, T008 alongside:
Task T006: Update src/agents/orchestrator.py (imports + 2 call sites)
Task T007: Update src/agents/graph.py (imports + 2 call sites)
Task T008: Update src/agents/prompts.py (text only)

# Phase 6 — run in parallel:
Task T014: Update README.md
Task T015: Update CLAUDE.md
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001)
2. Complete Phase 2: Foundational (T002–T005)
3. Complete Phase 3: User Story 1 (T006–T008)
4. **STOP and VALIDATE**: `grep -rn "unified_schema.json" src/ --include="*.py"` returns zero matches
5. Pipeline runs correctly with `--domain nutrition`

### Incremental Delivery

1. Phases 1–2 → Schema loader ready
2. Phase 3 → Agent 1 uses domain schema (MVP)
3. Phase 4 → Silver normalization enforces column set
4. Phase 5 → Gold output written per domain
5. Phase 6 → Docs clean, tests verified

---

## Notes

- `unified_schema.json` stays on disk — do NOT delete it; just ensure no code loads it after this feature
- `{unified_schema}` Python format placeholder in prompts.py is **not renamed** — only human-readable text strings change (T008)
- `_silver_normalize()` and Gold concat are private/inline — never register them in `BlockRegistry`
- Seven-node graph order does not change — Silver norm + Gold concat are internal to existing nodes
- If `faiss-cpu` unavailable, S2 is skipped by existing code — unaffected by this feature
