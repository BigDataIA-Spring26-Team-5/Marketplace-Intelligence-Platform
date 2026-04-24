# Feature Specification: Domain Pack UI Builder

**Feature Branch**: `018-domain-kit-ui-builder`  
**Created**: 2026-04-24  
**Status**: Draft  
**Input**: User description: "Create a Streamlit UI where a new user will input Domain_Kit for a different domain and create an alternative version of this Dynamic ETL pipeline. Main intention is to have a Domain agnostic pipeline with user inputed Domain kit."

---

## Context

The pipeline kernel (LLM orchestration, dedup, DQ scoring, GCS/BQ connectors, checkpointing, observability) is domain-agnostic. All domain-specific behaviour is concentrated in two tiers:

- **Tier 1** — `domain_packs/<domain>/`: block_sequence.yaml, enrichment_rules.yaml, prompt_examples.yaml, custom_blocks/*.py. Already abstracted; the registry auto-discovers new domains when these files are present.
- **Tier 2** — Five `src/` files still read food-specific constants instead of consulting the domain pack: `llm_enrich.py` (ENRICHMENT_COLUMNS, _SAFETY_FIELDS), `guardrails.py` (SAFETY_COLUMNS, VALID_CATEGORIES), `llm_tier.py` (category taxonomy + system prompt), `deterministic.py` (text_cols), `prompts.py` (semantic mapping examples). These must be parameterized before any non-food domain pack can run correctly.

This feature has two deliverables: **(A)** fix Tier 2 so the kernel is truly domain-agnostic, and **(B)** build a Streamlit UI that guides a user through creating, previewing, registering, and running a new domain pack — without requiring the user to know YAML schema internals.

Deployment: GCP VM with persistent disk. `domain_packs/` lives on the VM filesystem and persists across restarts. Single-tenant; one team per VM. No GCS kit storage, no multi-tenancy, no auth layer in scope.

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Generate Domain Pack from Sample Data (Priority: P1)

A data engineer who wants to run the pipeline on a new domain (e.g., healthcare, e-commerce, logistics) opens the "Domain Packs" panel in the Streamlit UI, types a domain name and a two-sentence description, uploads a sample CSV (20–100 rows), and clicks Generate. The system uses the existing AI orchestration agents to analyse the sample schema and produce a complete domain pack: `enrichment_rules.yaml`, `prompt_examples.yaml`, and `block_sequence.yaml`. The engineer reviews the generated files in-UI and commits them to register the domain.

**Why this priority**: This is the core value proposition. Without AI-assisted generation, onboarding a new domain requires knowing the internal YAML schema — which defeats the purpose of a domain-agnostic product.

**Independent Test**: Can be fully tested by uploading a sample CSV for a non-food domain, verifying the three generated YAML files are structurally valid (pass schema checks), and confirming the new domain appears in the pipeline launcher — without running a full pipeline pass.

**Acceptance Scenarios**:

1. **Given** a user provides a domain name, a two-sentence description, and a sample CSV, **When** they click Generate, **Then** the system produces `enrichment_rules.yaml` (with at least one enrichment field and one safety field declaration), `prompt_examples.yaml` (with at least three column mapping examples derived from the sample headers), and `block_sequence.yaml` (with the standard kernel blocks and a `__generated__` sentinel) — all passing YAML schema validation.
2. **Given** generated files are displayed for review, **When** the user edits a field inline and clicks Commit, **Then** the edited files are written to `domain_packs/<domain_name>/` and the pipeline launcher's domain selector includes the new domain without requiring a page reload.
3. **Given** the sample CSV has fewer than 5 rows or is malformed, **When** the user clicks Generate, **Then** the system shows a clear error describing the problem and does not proceed to generation.
4. **Given** a domain name that matches an existing domain (built-in or user-created), **When** the user clicks Generate, **Then** the system warns of the conflict and requires explicit overwrite confirmation before proceeding.

---

### User Story 2 — Scaffold a Custom Extraction Block (Priority: P2)

After generating the core kit files, the user needs a domain-specific extraction block (equivalent to `extract_allergens` for food, but for their domain — e.g., ICD-10 code extraction for healthcare, SKU parsing for e-commerce). The user describes what they want extracted in plain language. The system generates a Python block scaffold that follows the `Block` base class contract, pre-populated with relevant regex patterns or extraction logic derived from the description. The user downloads the file, edits if needed, and places it in `domain_packs/<domain>/custom_blocks/`.

**Why this priority**: For most real domains, YAML enrichment rules alone are insufficient — custom extraction logic is needed. Without a scaffold, users must read internal code to understand the Block contract, which is a significant barrier.

**Independent Test**: Can be fully tested by describing an extraction goal ("extract ICD-10 diagnosis codes from a notes column"), verifying the generated `.py` file contains a class that extends `Block`, has correct `name`/`domain`/`inputs`/`outputs` attributes, and is syntactically valid Python — without running the pipeline.

**Acceptance Scenarios**:

1. **Given** a user describes an extraction goal in plain language, **When** they click Generate Block, **Then** the system produces a syntactically valid Python file containing a class that extends `Block`, with `name`, `domain`, `description`, `inputs`, `outputs` attributes populated, and a `run()` method scaffolded with relevant regex patterns or logic derived from the description.
2. **Given** the scaffold is generated, **When** it is displayed, **Then** a security notice clearly states the file will execute on the server once placed in `custom_blocks/` and requires explicit user acknowledgment before download.
3. **Given** a generated scaffold is downloaded and placed in `domain_packs/<domain>/custom_blocks/`, **When** the pipeline launcher is next opened, **Then** the block appears in the block preview for that domain's kit.

---

### User Story 3 — Run Pipeline with a User-Created Domain (Priority: P2)

After committing a domain pack, the user selects their domain in the pipeline launcher, provides a source data file, and runs the pipeline end-to-end. The pipeline uses the user-created domain's block sequence, enrichment rules, and prompt examples — and critically, the kernel's previously hardcoded food constants (safety fields, valid categories, text columns for deterministic enrichment) are now read from the user's `enrichment_rules.yaml`, not from food-specific defaults.

**Why this priority**: Registration is only valuable if the pipeline executes correctly for the new domain. This is also the proof point that Tier 2 parameterization works.

**Independent Test**: Can be fully tested by running a pipeline pass for a non-food domain with a sample CSV and verifying: (a) output columns reflect the domain pack's enrichment fields, not food fields; (b) no allergen/dietary/is_organic columns appear in output unless the user's kit defined them; (c) the audit trail names the correct domain.

**Acceptance Scenarios**:

1. **Given** a registered user-created domain pack, **When** the user selects it in the pipeline launcher and submits a source CSV, **Then** the pipeline executes using the kit's block_sequence, enrichment_rules, and prompt_examples; safety fields and valid categories are read from the kit's enrichment_rules.yaml — not from food defaults.
2. **Given** a pipeline run completes for a user-created domain, **When** the output is inspected, **Then** enrichment columns in the output exactly match the fields declared in that domain's `enrichment_rules.yaml` — no food-domain columns appear unless the domain pack defined them.
3. **Given** a user-created domain pack with a `silver_sequence`, **When** the user selects silver mode, **Then** the kit's silver_sequence is used.

---

### User Story 4 — Preview and Validate Kit Before Committing (Priority: P3)

Before committing generated (or manually edited) kit files to `domain_packs/`, the user can run a dry-run validation. The system checks YAML schema compliance, resolves the full block execution order (expanding `__generated__`, `dedup_stage`, custom block references), verifies all referenced built-in block names exist in the registry, and checks that `enrichment_rules.yaml` does not assign S2/S3 inference to fields the system will treat as safety-critical.

**Why this priority**: Catches misconfiguration before it causes a failed or silently incorrect pipeline run.

**Independent Test**: Can be fully tested by submitting kit files through the preview action and verifying the system returns a resolved block execution plan and validation report without writing any files to `domain_packs/`.

**Acceptance Scenarios**:

1. **Given** kit files are ready for review, **When** the user clicks Preview, **Then** the system displays the resolved block execution order (sentinels expanded, composite blocks expanded), the enrichment field list, safety field declarations, and any validation warnings — without writing to `domain_packs/`.
2. **Given** a domain pack references a block name that does not exist in the registry, **When** preview runs, **Then** the unknown reference is flagged as a warning with the exact block name and a list of valid built-in names.
3. **Given** an `enrichment_rules.yaml` attempts to configure LLM inference for a field flagged as safety-critical, **When** preview runs, **Then** the system flags this as a validation error and blocks commit until corrected.

---

### User Story 5 — Manage Registered Domain Packs (Priority: P3)

A user can list all registered domain packs (built-in and user-created), view their file manifest and enrichment field summary, and delete user-created domain packs. Built-in kits are protected.

**Why this priority**: Operational hygiene. Prevents stale domain pack accumulation; allows correction of bad commits.

**Independent Test**: Can be fully tested by listing kits, deleting a user-created one, and confirming it no longer appears in the pipeline launcher — no pipeline run required.

**Acceptance Scenarios**:

1. **Given** a mix of built-in and user-created domain packs, **When** the user opens the Domain Packs panel, **Then** all registered domains are listed with type labels (built-in / user-created), creation date, and enrichment field summary.
2. **Given** a user-created domain pack, **When** the user deletes it, **Then** its `domain_packs/<domain>/` directory is removed, the pipeline launcher no longer shows it, and the action is logged with timestamp.
3. **Given** a built-in domain pack (nutrition, safety, pricing), **When** the user attempts to delete it, **Then** the system refuses with a clear message.

---

### Edge Cases

- What happens when Agent 1 generates an `enrichment_rules.yaml` with zero enrichment fields? (Treat as valid — enrichment-less domains are supported. Show a warning in preview prompting the user to confirm intent.)
- What if the sample CSV has headers that exactly match the unified schema columns? (Generation succeeds; prompt_examples.yaml will have trivial identity mappings. This is valid and expected for well-structured sources.)
- What happens when the user edits generated YAML inline and introduces a syntax error before committing? (Inline editor validates YAML on each change; commit button stays disabled until all files pass syntax check.)
- What if `domain_packs/` is not writable by the Streamlit process? (Detected at startup; a persistent warning banner appears in the Domain Packs panel; generation and commit are disabled with a clear explanation.)
- What if the user uploads a custom block `.py` file that imports a package not in the Poetry environment? (The system detects missing imports at block registration time and surfaces the error with the missing package name. The pipeline launcher excludes the broken block from the sequence.)
- What if a pipeline run is active for a domain when the user deletes its domain pack? (The active run uses its in-memory state and completes normally. Deletion only affects future runs. The delete action is logged as "pending — active run detected".)
- What happens when the LLM API call fails or times out during pack generation? (The wizard shows a spinner during generation. On failure — timeout, API error, or malformed response — the system displays a clear error message and a **Retry** button. All user inputs (domain name, description, uploaded CSV) are preserved in session state so the user can retry without re-entering data.)

---

## Requirements *(mandatory)*

### Functional Requirements

#### Tier 2 Parameterization (prerequisite — must ship before UI is useful)

- **FR-001**: `src/blocks/llm_enrich.py` MUST load `ENRICHMENT_COLUMNS` and `_SAFETY_FIELDS` from the active domain's `enrichment_rules.yaml` via `EnrichmentRulesLoader`, falling back to current food defaults only when no domain pack is present.
- **FR-002**: `src/agents/guardrails.py` MUST load `SAFETY_COLUMNS` and `VALID_CATEGORIES` from the active domain's `enrichment_rules.yaml`, replacing the hardcoded frozensets on lines 97 and 112–117.
- **FR-003**: `src/enrichment/llm_tier.py` MUST build its category taxonomy and LLM system prompt from `enrichment_rules.yaml` `classification_classes` and `rag_context_field`, replacing the hardcoded food strings on lines 36–56.
- **FR-004**: `src/enrichment/deterministic.py` MUST read `text_cols` from the active domain's `enrichment_rules.yaml` instead of the hardcoded `["product_name", "ingredients", "category"]` on line 42.
- **FR-005**: `src/agents/prompts.py` MUST inject domain-specific column mapping examples from `prompt_examples.yaml` in place of the hardcoded food semantic examples on lines 94–105 and 283–290. The base prompt must contain no food-specific column names.

#### UI — Pack Generation

- **FR-006**: System MUST provide a Domain Packs panel in the existing Streamlit sidebar with a pack generation wizard accepting: domain name (slug, validated as lowercase + underscores only), plain-language domain description (free text), and a sample CSV upload (required).
- **FR-007**: System MUST use the orchestrator LLM against the uploaded sample CSV and domain description to generate `enrichment_rules.yaml`, `prompt_examples.yaml`, and `block_sequence.yaml`. During generation the UI MUST display a loading spinner. On LLM failure (timeout, API error, or unparseable response), the UI MUST show a descriptive error and a **Retry** button with all user inputs (domain name, description, CSV) preserved in session state.
- **FR-008**: Generated files MUST be displayed in editable in-UI text areas before commit. Commit button MUST remain disabled until all three files pass YAML syntax validation.
- **FR-009**: System MUST write committed kit files to `domain_packs/<domain_name>/` on the VM filesystem and immediately make the domain available in the pipeline launcher without requiring application restart.

#### UI — Custom Block Scaffold

- **FR-010**: System MUST provide a block scaffold generator accepting a plain-language extraction description. Output MUST be a syntactically valid Python file containing a class extending `Block` with correct `name`, `domain`, `inputs`, `outputs` attributes and a scaffolded `run()` method.
- **FR-011**: System MUST display a security notice before the scaffold is downloadable. Acknowledgment mechanism: a checkbox labelled "I understand this file will execute on the server when placed in `custom_blocks/`". The download button MUST remain disabled until the checkbox is checked AND the generated file passes `ast.parse()` syntax validation.

#### UI — Preview and Validation

- **FR-012**: System MUST provide a Preview / Validate action on kit files that resolves and displays the full block execution order (sentinels and composite blocks expanded), enrichment field list, and safety field declarations — without writing to `domain_packs/`.
- **FR-013**: Preview MUST flag as errors: missing required YAML keys, unknown block names, safety fields configured for S2/S3 inference. Unknown block names MUST be flagged as warnings (not errors) to allow forward-reference to custom blocks not yet in `custom_blocks/`.

#### UI — Pack Management

- **FR-014**: Domain Packs panel MUST list all registered domains with type label (built-in / user-created), creation date, and enrichment field summary.
- **FR-015**: System MUST allow deletion of user-created domain packs. Built-in domain packs (nutrition, safety, pricing) MUST be non-deletable through the UI.
- **FR-016**: Every domain pack management action (generate, commit, delete) MUST produce a log entry written to `domain_packs/<domain>/.audit.jsonl` (one JSON object per line). The delete action MUST be appended before `shutil.rmtree` executes. Because the audit file is co-located with the kit, it is removed when the kit is deleted — no permanent cross-domain deletion record is retained. The Manage Kits panel aggregates entries from all `domain_packs/*/.audit.jsonl` files for the live display.

### Pipeline Governance Constraints *(mandatory when applicable)*

- FR-001–005 changes must preserve backward compatibility: when `domain` is `nutrition` (or any built-in domain), behaviour must be identical to the current implementation. The fallback-to-default path covers this.
- The `__generated__` sentinel in `block_sequence.yaml` must be preserved in all generated kits. `PipelineRunner` expands it at runtime; removing it breaks dynamic mapping injection.
- Custom block `.py` files written to `custom_blocks/` are auto-discovered at registry init. The UI must not auto-register them — they take effect only after the next Streamlit session load or explicit registry reload.
- Safety field constraint: `enrichment_rules.yaml` fields with `strategy: deterministic` MUST NOT appear in S2/S3 resolution paths. FR-013 validation enforces this at commit time; `LLMEnrichBlock`'s post-run assertion remains the hard enforcement backstop.
- Generated `prompt_examples.yaml` examples must not include enrichment or computed columns (`dq_score_pre`, `dq_score_post`, `dq_delta`, `primary_category`, etc.) in the mappable set — consistent with `SCHEMA_ANALYSIS_PROMPT`'s exclusion filter in `analyze_schema_node`.
- `NODE_MAP` in `app.py` does not change — pack generation runs outside the LangGraph pipeline and does not add new pipeline nodes.

### Key Entities *(include if feature involves data)*

- **DomainKit**: Configuration bundle for one ETL domain. Attributes: domain_name (slug), type (built-in / user-created), file_manifest, creation_timestamp, enrichment_field_count, safety_field_names.
- **PackGenerationSession**: Ephemeral wizard state. Attributes: domain_name, description, sample_csv_path, generated_files (dict of filename → content), validation_status, committed (bool).
- **BlockScaffold**: Generated Python file for a custom extraction block. Attributes: domain_name, block_name, extraction_description, file_content, security_acknowledged.
- **PackAuditEntry**: Immutable log record written to `domain_packs/<domain>/.audit.jsonl`. Attributes: domain_name, action (generate/commit/delete), timestamp, outcome, detail. Co-located with the kit — removed when the kit directory is deleted. Delete action is written before directory removal.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A data engineer with no prior knowledge of the codebase can go from zero to a running pipeline on a new domain in under 10 minutes using only the UI — no manual file editing required for standard domains.
- **SC-002**: After Tier 2 parameterization, a pipeline run for any non-food domain produces zero food-specific columns (allergens, dietary_tags, is_organic, serving_size) in its output unless the user's `enrichment_rules.yaml` explicitly defines them.
- **SC-003**: The pack generation wizard produces YAML files that pass all schema validation checks on first generation for at least 80% of sample CSVs with more than 5 columns.
- **SC-004**: Preview validation catches 100% of missing required YAML keys and S2/S3 safety field violations before any file is written to `domain_packs/`.
- **SC-005**: A committed domain pack is available in the pipeline launcher within one UI interaction (no page reload, no application restart).
- **SC-006**: Zero built-in domain packs are modifiable or deletable through the UI under any user flow.
- **SC-007**: All pack management actions appear in the audit log within 1 second of completion.

---

## Assumptions

- The Streamlit process has write access to `domain_packs/` on the VM filesystem. If not, the panel surfaces a startup warning and disables generation/commit.
- A minimum viable domain pack is: `block_sequence.yaml` (required) + `enrichment_rules.yaml` (required for Tier 2 parameterization to work correctly) + `prompt_examples.yaml` (optional but generated by default). The UI marks these as required/optional accordingly.
- Domain names must be valid Python identifiers (lowercase letters and underscores only, no hyphens) to be safely usable as directory names, registry keys, and `--domain` arguments. The UI enforces this at input time.
- The custom block scaffold generator produces a starting point, not production-ready code. The user is expected to review and edit the `.py` file before use. The UI communicates this clearly.
- Built-in domain packs are those committed to the repository (`domain_packs/nutrition/`, `domain_packs/safety/`, `domain_packs/pricing/`). All other directories in `domain_packs/` are treated as user-created.
- Tier 2 parameterization changes (FR-001–005) must not alter the current behaviour for built-in domains — they must function as before, with `EnrichmentRulesLoader` providing the same constants that are currently hardcoded.
- `EnrichmentRulesLoader` (already in `src/enrichment/rules_loader.py`) is the sole interface for reading domain pack config in `src/` code. No direct YAML parsing is added to `src/` files.
- Single-tenant deployment: one team per VM, shared `domain_packs/` directory. Concurrent kit uploads from multiple users are assumed rare; last-write-wins is acceptable. No distributed locking in scope.
- Custom Python block files from the scaffold generator are downloaded by the user and placed manually into `domain_packs/<domain>/custom_blocks/`. The UI does not auto-place them — the user controls when and whether to add executable code to the server.

---

## Clarifications

### Session 2026-04-24

- Q: What happens when the LLM API call fails or times out during pack generation? → A: Show spinner during generation; on failure, show error message + **Retry** button with all inputs (domain name, description, CSV) preserved in session state.
- Q: What is the security acknowledgment mechanism for scaffold download (FR-011)? → A: Checkbox — "I understand this file will execute on the server when placed in `custom_blocks/`"; download button disabled until checked AND syntax valid.
- Q: Where should domain pack audit logs be stored? → A: Co-located at `domain_packs/<domain>/.audit.jsonl` — deleted with the domain pack. Delete action written before `shutil.rmtree`. No permanent cross-domain deletion record retained.
- Q: Should "Domain Kit" and "domain pack" be distinct terms or unified? → A: Unified to "domain pack" everywhere — matches internal `domain_packs/` directory convention used throughout codebase.
