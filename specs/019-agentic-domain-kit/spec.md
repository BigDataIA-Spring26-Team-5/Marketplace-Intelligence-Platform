# Feature Specification: Agentic Domain Kit Builder

**Feature Branch**: `019-agentic-domain-kit`
**Created**: 2026-04-24
**Status**: Draft
**Input**: Implement the new Agentic architecture for the Domain Pack implementation

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Generate a Domain Pack via AI Agent (Priority: P1)

A data engineer has a new CSV dataset (e.g., pharmaceutical drug registry) and wants the
pipeline to process it. They need a Domain Pack — three YAML configuration files — generated
with high quality and reviewed before anything is saved to disk.

**Why this priority**: Core value of the feature. Without quality YAML output, the pipeline
produces wrong results or crashes. Current single-shot LLM approach produces structural errors
(phantom custom blocks, duplicate column mappings, wrong field extraction strategy).

**Independent Test**: Can be fully tested by providing a sample CSV and description, observing
the multi-step generation, reviewing the 3 output YAMLs, and committing — without any other
story being implemented.

**Acceptance Scenarios**:

1. **Given** a domain name, description, and sample CSV, **When** the user initiates generation,
   **Then** the system produces three YAML files in sequential steps (enrichment_rules first,
   then prompt_examples using enrichment context, then block_sequence aware of which fields
   are already handled by the enrichment layer).

2. **Given** the LLM generates an enrichment_rules.yaml with structural errors (missing `domain:`
   key, invalid regex, field extracting an already-structured column), **When** validation runs,
   **Then** the system automatically retries with errors injected into the prompt — without
   user intervention — up to 2 times before surfacing for manual fix.

3. **Given** all 3 YAMLs are generated and validated, **When** they are presented to the user,
   **Then** the user can read, edit any of the three files in-place and must explicitly approve
   before anything is written to `domain_packs/<domain>/`.

4. **Given** the generated block_sequence.yaml, **When** it is produced, **Then** it MUST NOT
   reference any custom block names for fields that are already defined in enrichment_rules.yaml
   (those are handled automatically by the enrichment layer).

5. **Given** a source column that already exists as a structured field in the CSV (e.g.,
   `classification`), **When** enrichment_rules.yaml is generated, **Then** that column is
   NOT added as an extraction field — it is a RENAME candidate for prompt_examples only.

---

### User Story 2 — Generate a Custom Block via AI Agent (Priority: P2)

A data engineer needs a custom Python extraction block (e.g., CPT procedure code extractor)
that cannot be expressed as simple regex patterns in enrichment_rules.yaml. They describe
what the block should do and receive reviewed, syntax-validated code before it is saved.

**Why this priority**: Custom blocks extend the pipeline for domain-specific logic. The current
approach generates code in one shot with no retry on syntax failure and saves without a
review gate.

**Independent Test**: Can be fully tested by selecting a domain, describing an extraction, and
verifying the generated Python block: correct class name, correct `Block` subclass contract,
syntax valid, saved only after user approval.

**Acceptance Scenarios**:

1. **Given** a domain selection and extraction description, **When** the user initiates scaffold
   generation, **Then** the system generates a Python `Block` subclass following the project's
   naming convention (`<domain>__<block_name>`).

2. **Given** the LLM generates syntactically invalid Python, **When** syntax validation fails,
   **Then** the system automatically retries with the specific syntax error injected back into
   the prompt — up to 2 times — before surfacing the broken code for manual edit.

3. **Given** valid code is produced, **When** it is presented to the user, **Then** the user
   sees the code, the syntax validation status, and can edit before approving. Nothing is
   written to `domain_packs/<domain>/custom_blocks/` until the user explicitly approves.

4. **Given** the user approves and saves, **When** the next pipeline run starts, **Then** the
   new block is auto-discovered by `BlockRegistry` and available in the block sequence.

---

### User Story 3 — Preview and Validate a Domain Pack (Priority: P3)

A data engineer wants to verify an existing or newly generated domain pack is structurally
correct before running the pipeline — catching issues like missing blocks, redundant
extractions, or mismatched field names without needing to start a pipeline run.

**Why this priority**: Prevents runtime crashes from misconfigured packs. Enhanced over current
basic YAML syntax check.

**Independent Test**: Can be fully tested by selecting any domain pack and observing the
validation report — no generation or pipeline run required.

**Acceptance Scenarios**:

1. **Given** a domain pack with a block referenced in `block_sequence.yaml` that has no
   corresponding `.py` file in `custom_blocks/`, **When** preview runs, **Then** the missing
   block is flagged as an error with the name and expected file path.

2. **Given** a domain pack where a field in `enrichment_rules.yaml` has the same name as an
   existing CSV source column, **When** preview runs, **Then** a warning is surfaced:
   "this field may be re-extracting an already-structured column — consider using RENAME instead."

3. **Given** a domain pack where `block_sequence.yaml` references a custom block AND
   `enrichment_rules.yaml` defines a field of the same logical name, **When** preview runs,
   **Then** a warning flags the double-extraction anti-pattern.

4. **Given** a valid, fully configured domain pack, **When** preview runs, **Then** the full
   resolved block sequence is shown (sentinels expanded, stages expanded) with each block's
   description and I/O columns listed.

---

## Functional Requirements

### FR-1: Sequential Multi-Step Domain Kit Generation Agent
- The generation process MUST execute as a LangGraph graph with distinct nodes:
  `analyze_csv → generate_enrichment_rules → validate_enrichment_rules →
  (revise_enrichment_rules?) → generate_prompt_examples → generate_block_sequence →
  hitl_review → commit_to_disk`
- Each node receives the outputs of all prior nodes via shared graph state
- `generate_block_sequence` MUST receive the list of field names already defined in
  `enrichment_rules.yaml` so it does not generate phantom custom blocks for those fields
- The agent MUST use domain-agnostic prompts from a dedicated prompt module
  (`src/agents/domain_kit_prompts.py`) separate from `src/agents/prompts.py`

### FR-2: Auto-Retry on Validation Failure
- After `generate_enrichment_rules`, a validation node MUST run structural checks
- On failure the system MUST automatically retry with errors injected into the LLM prompt
- Maximum 2 automatic retries; after 2 failures the broken YAML is shown in an editable text
  area with all validation errors listed as warnings above it — the Approve button remains
  available so the user can fix manually and still commit (degraded HITL, not a hard stop)
- Same retry mechanism applies to the block scaffold generator on syntax failure; exhausted
  scaffold retries surface the broken Python in the HITL text area with the syntax error shown

### FR-3: HITL Gate Before Any File Write
- No files are written to `domain_packs/<domain>/` until the user explicitly approves
- The review surface MUST show all 3 generated YAMLs as editable text areas
- A single "Approve & Save All" button atomically commits all 3 files — no per-file approval
- User edits made in the review step MUST be the content committed, not the LLM's raw output
- The scaffold HITL gate MUST show the Python source and syntax validation result
- If `domain_packs/<domain>/` already contains files, `commit_to_disk` MUST:
  1. Show a diff (existing vs. generated) in the UI before the Approve button is enabled
  2. Write `.bak` copies of each existing file (e.g. `enrichment_rules.yaml.bak`) before overwriting
  3. Record the overwrite in `.audit.jsonl` with action `overwrite` and detail listing backed-up files
- Every `commit_to_disk` call (fresh or overwrite) MUST append an entry to `.audit.jsonl`:
  fresh commits use action `generate`; overwrites use action `overwrite`

### FR-4: Domain-Agnostic Prompt Module
- A new module `src/agents/domain_kit_prompts.py` MUST contain all prompts for both agents
- These prompts MUST NOT contain hardcoded field names from any specific domain
- The nutrition domain pack MAY be used as a structural few-shot example only
- Prompts MUST include explicit rules distinguishing enrichment_rules fields (handled
  automatically by `llm_enrich`) from custom block fields (logic not expressible as regex)
- Prompts MUST explicitly state: structured CSV columns should be RENAME candidates in
  prompt_examples, NOT extraction fields in enrichment_rules

### FR-5: Block Scaffold Agent with Retry
- A separate LangGraph graph:
  `generate_scaffold → validate_syntax → (fix_scaffold?) → hitl_review → save_to_custom_blocks`
- `validate_syntax` uses `ast.parse()`
- On failure, `fix_scaffold` reinjects the `SyntaxError` message into a targeted fix prompt
- Maximum 2 automatic syntax-fix attempts

### FR-6: Enhanced Preview Validator (Deterministic, No Agent)
- The preview/validate tab MUST require a CSV upload before validation can run; the uploaded
  CSV headers are the reference for all header-dependent checks
- The validator MUST run deterministic checks:
  - Block referenced in sequence but no `.py` file exists → error
  - Field in enrichment_rules shares name with a CSV header → warning
  - Same logical name in both enrichment_rules field and custom block in sequence → warning
  - `__generated__` sentinel absent → error
  - `dq_score_pre` not first or `dq_score_post` not last → warning
- No LLM involvement in validation

### FR-7: Streamlit Integration via Step-by-Step Execution
- Both agents MUST integrate with Streamlit using the `run_step(node, state)` pattern
- Agent state MUST persist in `st.session_state` between Streamlit reruns
- Navigation and tab structure of the Domain Packs page remain unchanged

### FR-8: Run Pipeline Shortcut Post-Commit
- After a successful Domain Pack commit, "Run Pipeline with this domain" MUST reliably
  navigate to the Pipeline tab with the new domain pre-selected
- The current Streamlit radio state bug MUST be fixed

---

## Success Criteria

1. A domain pack generated via the UI for any of the 4 fixture CSVs passes Preview/Validate
   with zero errors on first use
2. The block_sequence.yaml generated for any domain contains no custom block references for
   fields already defined in enrichment_rules.yaml
3. Generating a pack for `fda_recalls` produces no phantom custom blocks and no duplicate
   column mappings — verifiable against known fixture
4. A syntax-broken scaffold is auto-corrected without user intervention in at least 80% of
   cases within 2 retries
5. No file is written to `domain_packs/` without an explicit user approval action
6. "Run Pipeline" button after commit navigates reliably to Pipeline tab with domain
   pre-selected in 100% of manual test runs

---

## Key Entities

| Entity | Description |
|--------|-------------|
| `DomainKitState` | LangGraph TypedDict state for the pack generation agent |
| `ScaffoldState` | LangGraph TypedDict state for the block scaffold agent |
| `src/agents/domain_kit_graph.py` | New: LangGraph graph definitions for both agents |
| `src/agents/domain_kit_prompts.py` | New: Domain-agnostic prompts for kit generation |
| `enrichment_rules.yaml` | Output artifact: enrichment field definitions for a domain |
| `prompt_examples.yaml` | Output artifact: column mapping examples for Agent 1 |
| `block_sequence.yaml` | Output artifact: ordered block execution plan |
| `custom_blocks/*.py` | Output artifact: domain-specific Python Block subclasses |

---

## Assumptions

- The existing `run_step(node, state)` Streamlit HITL pattern from `app.py` is the correct
  integration model — no new HITL infrastructure required
- The nutrition domain pack YAMLs remain valid structural few-shot examples; their field
  names (allergens, primary_category) will not appear in generated prompts
- The `validate_enrichment_rules` node reuses the same deterministic rules as FR-6
- Manage Packs tab requires no changes
- `src/agents/prompts.py` is NOT modified — it serves the main ETL pipeline agents only
- The 4 fixture CSVs in `tests/fixtures/` are canonical test inputs for generation quality

---

## Out of Scope

- Modifying the main ETL pipeline graph (`src/agents/graph.py`) or `src/agents/prompts.py`
- Generating enrichment_rules.yaml for incomplete `safety`, `pricing`, `retail_inventory` packs
- Removing the `nutrition__extract_allergens` double-extraction pattern (separate cleanup)

---

## Clarifications

### Session 2026-04-24

- Q: When existing `domain_packs/<domain>/` files are present, what should `commit_to_disk` do? → A: Warn + overwrite — show diff in UI, write `.bak` copies, then overwrite; log overwrite to `.audit.jsonl`
- Q: HITL approval granularity — single button or per-file? → A: Single "Approve & Save All" button; atomic commit of all 3 files
- Q: After 2 auto-retries still invalid, what does the user see? → A: Degraded HITL — broken YAML/code shown in editable text area with errors listed; Approve button still available for manual fix
- Q: Audit logging — log every commit or only overwrites? → A: Log every commit; action `generate` for fresh, `overwrite` for replacements
- Q: Preview validator CSV source for existing domain packs? → A: CSV upload always required; validation blocked without it
