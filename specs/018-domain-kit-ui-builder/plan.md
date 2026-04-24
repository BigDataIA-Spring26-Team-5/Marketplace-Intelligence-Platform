# Implementation Plan: Domain Kit UI Builder

**Branch**: `018-domain-kit-ui-builder` | **Date**: 2026-04-24 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `specs/018-domain-kit-ui-builder/spec.md`

---

## Summary

Two-deliverable feature. **(A) Tier 2 parameterization**: fix five `src/` files that still read food-specific constants at module level — replace with domain-aware calls through `EnrichmentRulesLoader`. **(B) Streamlit Domain Kits panel**: guided wizard that takes a domain name + description + sample CSV and uses the orchestrator LLM to generate `enrichment_rules.yaml`, `prompt_examples.yaml`, and `block_sequence.yaml`; plus a custom block scaffold generator, preview/validate mode, and kit management. All kit files write to `domain_packs/<domain>/` on the VM filesystem. No new LangGraph nodes, no GCS kit storage, no auth.

---

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: Streamlit (UI), LangGraph 0.4, LiteLLM 1.55, pandas 2.2, PyYAML, pathlib (stdlib), ast (stdlib), subprocess (stdlib)  
**Storage**: Local VM filesystem (`domain_packs/`), `output/kit_audit.jsonl` (append-only log)  
**Testing**: pytest — unit tests for Tier 2 param changes and new modules; one integration test with a non-food sample CSV  
**Target Platform**: GCP VM, Linux, single-tenant  
**Project Type**: Streamlit web application extension  
**Performance Goals**: Kit generation (LLM call) completes in < 30 seconds. Commit (filesystem write) completes in < 1 second. Tier 2 param changes must not measurably increase pipeline latency (one `EnrichmentRulesLoader` init per pipeline run, already happening in `llm_enrich.py`).  
**Constraints**: No new LangGraph nodes. No changes to `NODE_MAP`. No runtime Python code generation. No GCS kit storage. `domain_packs/` filesystem, single write path.  
**Scale/Scope**: ~5 domain kit files per domain, tens of domains max. No pagination needed. Kit management list is full-scan of `domain_packs/`.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Schema-First Gap Analysis | ✅ Pass | Committed domain kit triggers `derive_unified_schema_from_source()` on first pipeline run — creates `config/schemas/<domain>_schema.json`. Existing behavior, no change. |
| II. Three-Agent Pipeline | ✅ Pass | Kit generation is a separate LLM call outside the graph. No new agents or graph nodes. `NODE_MAP` unchanged. |
| III. Declarative YAML Execution Only | ✅ Pass | Generated `enrichment_rules.yaml` and `block_sequence.yaml` are config, not transform YAML. Custom block scaffold `.py` is a user-downloaded file placed manually — not runtime-generated Python invoked by the pipeline. The YAML action set is untouched. |
| IV. Human Approval Gates | ✅ Pass | Kit generation wizard is itself a HITL gate — user reviews and edits generated YAML before committing. Existing pipeline HITL gates (Gate 1: schema mapping, Gate 2: quarantine) unaffected. |
| V. Cascading Enrichment with Safety Boundaries | ✅ Pass | Tier 2 fix reads `safety_field_names()` from domain's `enrichment_rules.yaml`. Preview validation blocks commit if an `enrichment_rules.yaml` attempts to assign LLM strategy to a field declared as safety. The `LLMEnrichBlock` post-run assertion remains as the hard enforcement backstop. |
| VI. Self-Extending Mapping Memory | ✅ Pass | Redis/SQLite YAML cache keyed on schema fingerprint. Kit registration does not touch the cache. First pipeline run for new domain generates and caches normally. |
| VII. DQ and Quarantine | ✅ Pass | DQ scoring blocks (`dq_score_pre`, `dq_score_post`) are in the generated `block_sequence.yaml` template. Not affected. |
| VIII. Production Scale | ✅ Pass | Kit generation is a one-time config activity. No per-row LLM calls introduced. Pipeline chunked streaming unchanged. |
| IX. Domain-Scoped Schemas | ✅ Pass | `config/schemas/<domain>_schema.json` created on first run per existing `derive_unified_schema_from_source()`. Committed kit does not pre-create this file — the pipeline creates it. Gold concatenation unaffected (scoped to domain). |

**No violations. No complexity tracking required.**

---

## Project Structure

### Documentation (this feature)

```text
specs/018-domain-kit-ui-builder/
├── plan.md              # This file
├── research.md          # Phase 0 — decisions on EnrichmentRulesLoader, LLM strategy
├── data-model.md        # Phase 1 — entities, state transitions, YAML schemas
└── checklists/
    └── requirements.md  # Validation checklist
```

### Source Code

```text
# Tier 2 parameterization — existing files modified
src/enrichment/
  rules_loader.py          ← extend: add enrichment_column_names, llm_categories_string,
                                      text_columns, llm_rag_context_field properties;
                                      store raw YAML dict as self._raw for text_columns lookup
  llm_tier.py              ← fix: replace CATEGORIES, SYSTEM_PROMPT, BATCH_SYSTEM_PROMPT
                                   with domain-aware construction from EnrichmentRulesLoader
  deterministic.py         ← fix: load text_cols from EnrichmentRulesLoader(domain).text_columns

src/blocks/
  llm_enrich.py            ← fix: line 16 ENRICHMENT_COLUMNS → rules_loader.enrichment_column_names
                                   line 19 _SAFETY_FIELDS → rules_loader.safety_field_names()

src/agents/
  guardrails.py            ← fix: add get_safety_columns(domain) and get_valid_categories(domain)
                                   functions; update internal callers that validate enrichment output
  prompts.py               ← fix: load semantic mapping examples from prompt_examples.yaml
                                   via a new helper load_prompt_examples(domain) in rules_loader

# New UI modules
src/ui/
  domain_kits.py           ← new: render_domain_kits_page() — 4 tabs (Generate, Scaffold,
                                   Preview, Manage)
  kit_generator.py         ← new: generate_domain_kit(domain_name, description, csv_content)
                                   → dict[str, str]; LLM call via get_orchestrator_llm()
  block_scaffolder.py      ← new: generate_block_scaffold(domain_name, description)
                                   → str; LLM call + ast.parse() validation

# App router
app.py                     ← extend: add "Domain Kits" to sidebar radio (line 615);
                                      add elif branch calling render_domain_kits_page()

# Domain pack schema extension
domain_packs/nutrition/
  enrichment_rules.yaml    ← (no change needed — text_columns fallback covers existing schema)

# Tests
tests/
  unit/
    test_rules_loader_extensions.py      ← enrichment_column_names, text_columns, etc.
    test_tier2_parameterization.py       ← each of the 5 src/ changes, parametrized by domain
    test_kit_generator.py                ← generate_domain_kit() with mock LLM
    test_block_scaffolder.py             ← generate_block_scaffold() + ast.parse() check
  integration/
    test_healthcare_domain_pipeline.py   ← full pipeline run on synthetic healthcare CSV
                                           using a pre-authored healthcare domain kit;
                                           asserts zero food columns in output
```

**No contracts/ directory** — this feature adds a Streamlit UI panel; no new REST endpoints, CLI commands, or library APIs are exposed externally.

---

## Implementation Phases

### Phase A — Tier 2 Parameterization (prerequisite; implement first)

Sequence matters: `rules_loader.py` extensions must land before the five consuming files are updated.

#### A1 — Extend `EnrichmentRulesLoader`

File: `src/enrichment/rules_loader.py`

Changes:
1. In `__init__`, store raw parsed YAML dict as `self._raw: dict` (currently discarded after `_load()`).
2. Add property `enrichment_column_names → list[str]`: `[f.name for f in self.all_fields]`.
3. Add property `llm_categories_string → str`: join `classification_classes` from first LLM field; return `""` if no LLM fields.
4. Add property `text_columns → list[str]`: `self._raw.get("text_columns", ["product_name", "ingredients", "category"])`.
5. Add property `llm_rag_context_field → str | None`: first LLM field's `rag_context_field`.
6. Add method `load_prompt_examples(domain: str) -> list[dict]`: reads `domain_packs/<domain>/prompt_examples.yaml`, returns `examples` list; returns `[]` if file absent.

#### A2 — Fix `src/blocks/llm_enrich.py`

Lines 16, 19, 33–34:
- Line 16: Remove module-level `ENRICHMENT_COLUMNS`. Move to per-call derivation.
- Line 19: Remove module-level `_SAFETY_FIELDS`. Move to per-call derivation.
- In `run()`, after `rules_loader = EnrichmentRulesLoader(domain)` (line 39):
  - `enrich_cols = config.get("enrichment_columns") or rules_loader.enrichment_column_names`
  - `safety_fields = rules_loader.safety_field_names()`
- Replace all subsequent references to `ENRICHMENT_COLUMNS` and `_SAFETY_FIELDS` with local variables.
- The post-run assertion that warns when S3-resolved rows have safety field changes now uses `safety_fields` local var.

#### A3 — Fix `src/enrichment/llm_tier.py`

Lines 36–54:
- Remove module-level `CATEGORIES`, `SYSTEM_PROMPT`, `BATCH_SYSTEM_PROMPT`.
- Add `_build_prompts(domain: str) -> tuple[str, str, str]` internal function that:
  1. Loads `EnrichmentRulesLoader(domain)`
  2. Builds `categories_str = loader.llm_categories_string` (falls back to original 20-item string if empty)
  3. Builds `system_prompt` and `batch_system_prompt` with f-string substitution, replacing `{CATEGORIES}` with `categories_str`
  4. Also reads `rag_context_field = loader.llm_rag_context_field` for the RAG prompt field name (used in lines ~86–94)
- Callers of `llm_enrich()` and async batch functions already pass `domain` through config — thread it to `_build_prompts()`.

#### A4 — Fix `src/enrichment/deterministic.py`

Line 42:
- `deterministic_enrich()` signature already receives `domain` indirectly through its callers. Confirm domain flows through from `LLMEnrichBlock.run()` → `deterministic_enrich()`.
- Replace: `text_cols = ["product_name", "ingredients", "category"]`
- With: `text_cols = EnrichmentRulesLoader(domain).text_columns` (import already present via llm_enrich chain; add direct import if needed).
- `existing_text_cols` filter on line 43 still applies — safe if column absent.

#### A5 — Fix `src/agents/guardrails.py`

Lines 97, 112–117:
- Keep `SAFETY_COLUMNS` and `VALID_CATEGORIES` frozensets as module-level fallbacks (no breaking change for callers that don't supply domain).
- Add `get_safety_columns(domain: str) -> frozenset`: returns `frozenset(EnrichmentRulesLoader(domain).safety_field_names())` or falls back to `SAFETY_COLUMNS` if empty.
- Add `get_valid_categories(domain: str) -> frozenset`: returns `frozenset(EnrichmentRulesLoader(domain).llm_categories_string.split(", "))` or falls back to `VALID_CATEGORIES` if empty.
- Update the two internal functions that validate enrichment output to call `get_safety_columns(domain)` and `get_valid_categories(domain)` instead of reading frozensets directly. `domain` is available from the `state` dict already passed to these functions.

#### A6 — Fix `src/agents/prompts.py`

Lines 94–105, 283–290:
- Add `_load_domain_examples(domain: str) -> str` helper that:
  1. Calls `EnrichmentRulesLoader.load_prompt_examples(domain)`
  2. Formats examples list as bullet strings for injection into the prompt
  3. Returns empty string if no examples (safe — prompt still valid)
- In `SCHEMA_ANALYSIS_PROMPT` and `FIRST_RUN_SCHEMA_PROMPT`: replace hardcoded food examples block with `{domain_examples}` placeholder, populated by `_load_domain_examples(domain)` at call site in `analyze_schema_node`.
- Remove food-specific column names (`serving_size`, `ingredients`, `foodNutrients`, `gtinUpc`) from base prompt. These now come only from `prompt_examples.yaml` injected at call time.

---

### Phase B — Streamlit Domain Kits Panel

#### B1 — `src/ui/kit_generator.py`

New module. Single public function:

```python
def generate_domain_kit(
    domain_name: str,
    description: str,
    csv_content: str,          # raw CSV text
) -> dict[str, str]:           # filename → YAML string
```

Implementation:
1. Parse CSV headers + first 5 rows using `io.StringIO` + `csv.reader`.
2. Build LLM prompt containing: domain name, description, headers+sample as markdown table, nutrition `enrichment_rules.yaml` as few-shot example, blank templates for all three output files, generation instructions (must include `domain:` key, `fields:`, `__generated__` sentinel, etc.).
3. Call `call_llm_json()` with `get_orchestrator_llm()`.
4. Parse response — expected structure: `{"enrichment_rules": "...", "prompt_examples": "...", "block_sequence": "..."}`.
5. Validate each value with `yaml.safe_load()` — raises on syntax error.
6. Return `{"enrichment_rules.yaml": ..., "prompt_examples.yaml": ..., "block_sequence.yaml": ...}`.

Error handling: catch LLM errors + YAML parse errors; return partial dict with error strings instead of raising, so UI can show per-file error state.

#### B2 — `src/ui/block_scaffolder.py`

New module. Single public function:

```python
def generate_block_scaffold(
    domain_name: str,
    extraction_description: str,
) -> tuple[str, bool]:         # (python_source, syntax_valid)
```

Implementation:
1. Build prompt with: Block base class contract (attributes + method signatures), `extract_allergens.py` as few-shot example, user's extraction_description, instructions to produce a single Python class.
2. Call LLM (not JSON mode — expects raw Python). Strip markdown fences if present.
3. Validate with `ast.parse(source)` — catches syntax errors.
4. Return `(source, True)` or `(source_with_error_comment, False)`.

Block naming convention: `extract_<noun>` where noun is derived from extraction_description (LLM provides `block_name` in a comment on line 1).

#### B3 — `src/ui/domain_kits.py`

New module. Single public function `render_domain_kits_page()` called from `app.py`. Internal structure:

```python
def render_domain_kits_page():
    st.title("Domain Kits")
    _check_writability()        # banner if domain_packs/ not writable
    tab1, tab2, tab3, tab4 = st.tabs(["Generate Kit", "Block Scaffold", "Preview / Validate", "Manage Kits"])
    with tab1: _render_generate_tab()
    with tab2: _render_scaffold_tab()
    with tab3: _render_preview_tab()
    with tab4: _render_manage_tab()
```

**`_render_generate_tab()`**:
- `st.text_input("Domain name", placeholder="healthcare")` — validate slug on change
- `st.text_area("Domain description")`
- `st.file_uploader("Sample CSV", type=["csv"])`
- `st.button("Generate")` → calls `generate_domain_kit()` → stores in `st.session_state["kit_gen"]`
- Three `st.text_area()` editors (one per generated file), each with inline YAML validation
- `st.button("Validate")` → structural check + safety field constraint check
- `st.button("Commit", disabled=not validated)` → write files to `domain_packs/<domain>/`, append to `output/kit_audit.jsonl`
- Deep-link: after commit, show `st.success("Domain '<name>' registered. Switch to Pipeline mode to run.")` with button that sets `st.session_state["app_mode"] = "Pipeline"` and `st.session_state["domain"] = domain_name`.

**`_render_scaffold_tab()`**:
- `st.selectbox("Domain")` — from registered domains
- `st.text_area("Describe what to extract")`
- `st.button("Generate Block")` → calls `generate_block_scaffold()` → stores in `st.session_state["scaffold"]`
- `st.code(...)` — syntax-highlighted display
- `st.warning("Security notice...")` + `st.checkbox("I understand this file will run on the server")`
- `st.download_button(...)` — enabled when checkbox checked + syntax valid

**`_render_preview_tab()`**:
- `st.selectbox("Domain")` — from registered domains
- `st.button("Preview")`:
  1. Load kit files from `domain_packs/<domain>/`
  2. Resolve block sequence: expand `__generated__` → `DynamicMappingBlock`, `dedup_stage` → `[fuzzy_deduplicate, column_wise_merge, golden_record_select]`
  3. Validate `enrichment_rules.yaml`: check for LLM fields that match safety field names (error), unknown top-level keys (warning)
  4. Display results: `st.table()` of block execution order, `st.json()` of enrichment field summary, `st.error()` / `st.warning()` list

**`_render_manage_tab()`**:
- Load all `domain_packs/` subdirectories
- For each: classify built-in vs user-created via `git ls-files` check
- Display `st.dataframe()` with columns: Domain, Type, Created, Enrichment Fields
- Delete button per user-created domain:
  - `st.button("Delete")` → `st.confirm()` (rerun-based pattern) → `shutil.rmtree(domain_packs/<domain>)` → append audit entry
  - Built-in domains: delete button replaced with `st.badge("Protected", color="gray")`
- Last 20 audit log entries from `output/kit_audit.jsonl`

#### B4 — Extend `app.py`

Line 615: `st.radio("Mode", ["Pipeline", "Observability", "Domain Kits"])`.

Add to mode dispatch (after `elif mode == "Observability":` block):
```python
elif mode == "Domain Kits":
    from src.ui.domain_kits import render_domain_kits_page
    render_domain_kits_page()
```

---

### Phase C — Tests

#### Unit tests

`tests/unit/test_rules_loader_extensions.py`:
- Test `enrichment_column_names` returns correct list from fixture YAML
- Test `text_columns` falls back to food defaults when key absent
- Test `llm_categories_string` builds correct comma-separated string
- Test `safety_field_names()` returns only deterministic field names
- Parametrize with nutrition fixture and a synthetic healthcare fixture

`tests/unit/test_tier2_parameterization.py`:
- For each Tier 2 file (A2–A6): test that with a healthcare-domain `EnrichmentRulesLoader`, the output contains zero food-specific column names
- Test `get_safety_columns("healthcare")` returns `{"icd10_codes", "medication_names"}` given a healthcare fixture
- Test `get_valid_categories("healthcare")` returns healthcare categories, not food categories

`tests/unit/test_kit_generator.py`:
- Mock `call_llm_json` to return valid YAML strings
- Assert output dict has all three files
- Assert each file passes `yaml.safe_load()` without error
- Assert generated `block_sequence.yaml` contains `__generated__`

`tests/unit/test_block_scaffolder.py`:
- Mock LLM; return a syntactically valid Block subclass string
- Assert `ast.parse()` succeeds
- Assert class name starts with known prefix
- Test with malformed LLM return → assert `syntax_valid = False`

#### Integration test

`tests/integration/test_healthcare_domain_pipeline.py` (mark `@pytest.mark.integration`):
- Pre-author `domain_packs/healthcare_test/` with a minimal fixture kit
- Run pipeline with `--source tests/fixtures/healthcare_sample.csv --domain healthcare_test`
- Assert output CSV: no columns named `allergens`, `dietary_tags`, `is_organic`, `primary_category` (unless kit defines them)
- Assert `icd10_codes` column present and non-null for rows with diagnosis text
- Assert `dq_score_pre` and `dq_score_post` present
- Teardown: remove `domain_packs/healthcare_test/` and `config/schemas/healthcare_test_schema.json`

---

## Complexity Tracking

> No constitution violations — section not required.

---

## Sequencing Summary

```
A1 (rules_loader extensions)
    ↓
A2 (llm_enrich)   A3 (llm_tier)   A4 (deterministic)   A5 (guardrails)   A6 (prompts)
    ↓ all A-tasks complete
B1 (kit_generator)   B2 (block_scaffolder)
    ↓
B3 (domain_kits.py UI — depends on B1, B2)
    ↓
B4 (app.py router — depends on B3)
    ↓
C (tests — after all A + B complete)
```

A-tasks can be done in any order after A1. B1 and B2 are independent. B3 depends on both. B4 is last.

---

## Risk Notes

- **LLM generation quality** (B1): Generated `enrichment_rules.yaml` may miss safety fields or produce poor regex. Mitigated by: nutrition few-shot example in prompt, user review step before commit, preview validation catching safety field misuse.
- **`deterministic.py` domain threading** (A4): Confirm `domain` is threaded through the call chain `LLMEnrichBlock.run()` → `deterministic_enrich()`. If `domain` is not currently a parameter of `deterministic_enrich()`, add it. Inspect call site before editing.
- **`guardrails.py` internal callers** (A5): Locate all internal uses of `SAFETY_COLUMNS` and `VALID_CATEGORIES` before adding getters. Grep: `SAFETY_COLUMNS`, `VALID_CATEGORIES`. Do not miss any call site.
- **`git ls-files` for built-in detection** (B3): Requires git present on the VM. Add fallback: if `git` is not available, classify all as "user-created" with a warning banner.
