# Feature Specification: Kernel / Domain Separation

**Feature Branch**: `016-kernel-domain-separation`
**Created**: 2026-04-24
**Status**: Draft
**Input**: User description: "Now lets focus on making the separation between Domain and Kernel now."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Non-Food Domain Runs Without Food Assumptions (Priority: P1)

A Data Engineer targeting a retail inventory domain clones the repo, drops a `domain_packs/retail_inventory/` directory, and runs the CLI. The pipeline executes without referencing any nutrition column names, allergen logic, or FDA joins.

**Why this priority**: This is the core sellability problem. Until a non-food domain can run cleanly, the product is not shippable to any new customer.

**Independent Test**: Run `python -m src.pipeline.cli --source data/retail_sample.csv --domain retail_inventory` with a minimal domain pack in place. No `allergens`, `brand_name`, `ingredients`, or `primary_category` errors. Block trace shows only blocks declared in `block_sequence.yaml`.

**Acceptance Scenarios**:

1. **Given** a `domain_packs/retail_inventory/` pack with `schema.json`, `block_sequence.yaml`, `enrichment_rules.yaml`, and `prompt_examples.yaml`, **When** the CLI runs with `--domain retail_inventory`, **Then** the pipeline completes with zero references to food-domain columns in logs, output, or errors.
2. **Given** no domain pack exists for a requested domain, **When** the CLI is invoked, **Then** the runner falls back to a generic cleaning-only sequence and logs a warning that no domain pack was found.
3. **Given** a nutrition domain pack exists at `domain_packs/nutrition/`, **When** the CLI runs with `--domain nutrition`, **Then** behavior is identical to current food-domain behavior — no regression.

---

### User Story 2 - Registry Reads Block Sequence from Domain Pack (Priority: P1)

The block registry no longer contains inline `if domain == "nutrition"` / `elif domain == "pricing"` branches. Instead it reads `domain_packs/<domain>/block_sequence.yaml` and constructs the sequence from that file.

**Why this priority**: The registry branching is the most visible tangle — adding a new domain today requires editing kernel Python. This must be config-driven.

**Independent Test**: Delete or rename the nutrition/pricing branches from `block_registry.py`. Create a `domain_packs/nutrition/block_sequence.yaml` mirroring the current hardcoded sequence. Run the existing nutrition CLI test — output must be identical.

**Acceptance Scenarios**:

1. **Given** `domain_packs/nutrition/block_sequence.yaml` lists the current nutrition block order, **When** `get_default_sequence("nutrition", ...)` is called, **Then** the returned list matches the file exactly, including `__generated__` sentinel position.
2. **Given** no `block_sequence.yaml` exists for a domain, **When** `get_default_sequence` is called, **Then** `FALLBACK_SEQUENCE` (generic cleaning only, no enrichment) is returned.
3. **Given** a domain pack's `custom_blocks/` directory contains `extract_hazard_code.py`, **When** the registry is initialized, **Then** that block is discoverable by name in the sequence.

---

### User Story 3 - Runner Null-Rate Columns Driven by Domain Schema (Priority: P2)

`NULL_RATE_COLUMNS` in `runner.py` is replaced by a function that reads required columns from the active domain's `schema.json`. No food column names remain hardcoded in the kernel.

**Why this priority**: Hardcoded food column names cause misleading null-rate stats and log noise for any non-food domain. Fixing the registry alone still leaves this tangle.

**Independent Test**: Point runner at a retail domain pack whose `schema.json` has `required: true` on `sku_id` and `product_name` only. Verify `block_end` Kafka events report null rates for those two columns and not for `brand_name`, `ingredients`, etc.

**Acceptance Scenarios**:

1. **Given** a domain schema with `required: true` columns `["sku_id", "product_name"]`, **When** the pipeline emits `block_end` events, **Then** null-rate stats are computed for exactly `["sku_id", "product_name"]`.
2. **Given** a domain schema with no `required: true` columns, **When** the pipeline runs, **Then** null-rate stats are empty (not erroring, not defaulting to food columns).

---

### User Story 4 - Prompts Load Few-Shot Examples from Domain Pack (Priority: P2)

Agent 1's schema analysis prompt injects few-shot column mapping examples from `domain_packs/<domain>/prompt_examples.yaml` instead of from the hardcoded food strings in `prompts.py`.

**Why this priority**: Without this change, Agent 1 will suggest food-domain column names (e.g., `ingredients`) when analyzing retail or financial source files, producing wrong YAML mappings.

**Independent Test**: Run Agent 1 on a retail CSV with `--domain retail_inventory` and a `prompt_examples.yaml` containing SKU/price examples. Verify the generated YAML operations reference `sku_id` / `product_name`, not `brand_name` / `ingredients`.

**Acceptance Scenarios**:

1. **Given** `domain_packs/retail_inventory/prompt_examples.yaml` with retail column examples, **When** Agent 1 runs schema analysis on a retail source file, **Then** the emitted YAML operations reference retail unified column names.
2. **Given** no `prompt_examples.yaml` exists for a domain, **When** Agent 1 runs, **Then** it falls back to generic examples (no domain-specific column names injected) and completes without error.
3. **Given** the nutrition domain pack exists with food examples, **When** Agent 1 runs with `--domain nutrition`, **Then** behavior is unchanged from current — full regression compatibility.

---

### User Story 5 - Nutrition Code Migrated to Domain Pack Reference Implementation (Priority: P3)

Food-specific files (`extract_allergens.py`, `extract_quantity_column.py`, `deterministic.py`, food schemas, food DAGs) are moved or copied to `domain_packs/nutrition/` and removed from their current kernel-adjacent locations.

**Why this priority**: Even after kernel changes, if food files remain in `src/blocks/` and `src/enrichment/`, a new customer sees them and must understand which files to ignore. Physical separation makes the boundary unambiguous.

**Independent Test**: After migration, `src/blocks/` must contain no file with "allergen" or "quantity_column" in its name. `src/enrichment/deterministic.py` must not exist or must contain only generic infrastructure with no food regex. Nutrition pipeline must still pass via the domain pack path.

**Acceptance Scenarios**:

1. **Given** the migration is complete, **When** `ls src/blocks/` is run, **Then** no food-specific block files are present in the kernel directory.
2. **Given** `domain_packs/nutrition/custom_blocks/` contains the migrated blocks, **When** the nutrition pipeline runs, **Then** all allergen/quantity extraction steps complete as before.
3. **Given** `src/enrichment/deterministic.py` is removed or made generic, **When** the nutrition domain pack's enrichment_rules.yaml is loaded, **Then** deterministic extraction still works via the domain pack rules path.

---

### Edge Cases

- What if a domain pack's `block_sequence.yaml` references a block name that does not exist in `src/blocks/` or `custom_blocks/`? → Registry must raise a clear `BlockNotFoundError` at startup, not at runtime mid-pipeline.
- What if `prompt_examples.yaml` is malformed YAML? → Agent 1 must fall back to generic examples and log a warning; pipeline must not crash.
- What if a nutrition run is attempted after migration but `domain_packs/nutrition/` is absent? → CLI must fail with a clear "domain pack not found" message, not a `FileNotFoundError` from deep in the kernel.
- What if two domain packs define a custom block with the same class name? → Registry must namespace by domain to avoid collisions.
- What if `schema.json` has no `required: true` columns? → Runner uses empty null-rate list; no error.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Registry MUST read block sequence from `domain_packs/<domain>/block_sequence.yaml` when the file exists, replacing all inline domain-branching logic in `block_registry.py`.
- **FR-002**: Registry MUST fall back to `FALLBACK_SEQUENCE` (strip_whitespace, remove_noise_words, dq_score_pre/post only) when no domain pack block sequence file is found.
- **FR-003**: Registry MUST discover and register custom blocks from `domain_packs/<domain>/custom_blocks/` at startup, namespaced to avoid cross-domain collisions.
- **FR-004**: Registry MUST raise `BlockNotFoundError` at startup if `block_sequence.yaml` references a block name not discoverable in kernel `src/blocks/` or domain `custom_blocks/`.
- **FR-005**: Runner MUST derive null-rate column list from required columns in `domain_packs/<domain>/schema.json`, replacing the hardcoded `NULL_RATE_COLUMNS` constant.
- **FR-006**: Agent 1's schema analysis prompt MUST inject few-shot examples loaded from `domain_packs/<domain>/prompt_examples.yaml` at node entry time, not at module import time.
- **FR-007**: Agent 1 MUST fall back to a generic (domain-neutral) few-shot example set when `prompt_examples.yaml` is absent for the active domain.
- **FR-008**: Food-specific block files (`extract_allergens.py`, `extract_quantity_column.py`) MUST be moved to `domain_packs/nutrition/custom_blocks/` and removed from `src/blocks/`.
- **FR-009**: `src/enrichment/deterministic.py` food-specific rules (CATEGORY_RULES, DIETARY_RULES, ORGANIC_PATTERN) MUST be moved to `domain_packs/nutrition/enrichment_rules.yaml`; `deterministic.py` becomes either a generic rule-executor or is removed if superseded by the enrichment rules loader.
- **FR-012**: The `enrich_stage` kernel composite (`_STAGES["enrich_stage"]`) MUST be removed. Domain packs MUST list all enrichment blocks individually in `block_sequence.yaml` (e.g., `nutrition__extract_allergens`, `llm_enrich`). No kernel stage composite may reference domain-specific block names.
- **FR-013**: `keep_quantity_in_name` MUST remain in the kernel block registry as a generic block. A minimal `domain_packs/pricing/block_sequence.yaml` MUST be created so the registry reads pricing's sequence from config rather than inline Python branching. No other pricing pack artifacts are required this phase.
- **FR-010**: The nutrition domain pack MUST produce identical pipeline output to the current food-domain implementation — no regression in allergen extraction, DQ scoring, or enrichment.
- **FR-011**: All kernel Python files (`src/agents/`, `src/blocks/`, `src/pipeline/`, `src/registry/`) MUST contain zero hardcoded food-domain column names after this change.

### Pipeline Governance Constraints *(mandatory when applicable)*

- `NULL_RATE_COLUMNS` removal changes what appears in `block_end` Kafka events. Downstream consumers of `pipeline.events` (UC2 `kafka_to_pg.py`, Prometheus metrics) must handle variable null-rate column sets without crashing.
- The `__generated__` sentinel in `block_sequence.yaml` must be preserved — runner still injects `DynamicMappingBlock` at that position.
- The composite stage sentinels `dedup_stage` and `enrich_stage` must continue to expand correctly when present in a domain's `block_sequence.yaml`.
- Safety boundary invariant remains: `allergens`, `dietary_tags`, `is_organic` are S1-only in the nutrition domain pack. The enrichment_rules.yaml for nutrition must mark these fields `strategy: deterministic`. Moving them to config does not weaken the constraint.
- `config/schemas/nutrition_schema.json` and `config/schemas/safety_schema.json` must either remain or be aliased so that existing Airflow DAGs referencing those paths do not break before the DAG factory (Phase 3 of revamp) is built.
- The Redis yaml-cache key is based on schema fingerprint. If domain pack files change the canonical schema, existing cache entries will miss (acceptable) but must not cause deserialization errors on stale hits.

### Key Entities

- **Domain Pack**: Directory at `domain_packs/<domain>/` containing the 5 artifact files. Treated as a unit — either all required files present or domain is considered unconfigured.
- **Block Sequence**: Ordered list of block names in `block_sequence.yaml`. Contains the `__generated__` sentinel and optionally composite stage names.
- **Custom Block**: Python `Block` subclass in `domain_packs/<domain>/custom_blocks/`. Discovered by registry at startup, namespaced to avoid collisions.
- **Null-Rate Column Set**: Derived from `required: true` columns in `schema.json`. Replaces the static `NULL_RATE_COLUMNS` constant.
- **Prompt Example Set**: Column mapping examples in `prompt_examples.yaml`. Injected into Agent 1's few-shot context at runtime.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A retail inventory domain pipeline runs end-to-end from CLI with zero food-domain column names appearing in logs, output headers, or Kafka events.
- **SC-002**: Adding a new domain requires zero edits to any file under `src/` — only a new `domain_packs/<domain>/` directory is needed.
- **SC-003**: All existing nutrition pipeline tests pass after import paths are updated to the domain pack location. A new unit test confirms registry correctly discovers and loads a custom block from `domain_packs/<domain>/custom_blocks/` via `importlib`.
- **SC-004**: `grep -r "allergen\|brand_name\|ingredients\|is_organic\|dietary_tags" src/` returns zero matches in kernel Python files (excluding test fixtures and comments).
- **SC-005**: Block registry initialization time does not increase by more than 10% for the nutrition domain compared to current baseline.
- **SC-006**: A non-food domain pipeline run produces a `block_end` Kafka event with null-rate stats referencing only the columns declared `required: true` in that domain's schema.

## Clarifications

### Session 2026-04-24

- Q: How should `enrich_stage` composite expansion work after `extract_allergens` moves to domain pack? → A: Eliminate `enrich_stage` as a kernel composite. Domain packs list all enrichment blocks individually in `block_sequence.yaml`. `llm_enrich` is just another named block in the sequence.
- Q: What is the scope for test updates when block files move out of `src/blocks/`? → A: Update existing test import paths to use new domain pack location; add a registry custom block discovery test confirming `importlib` loading works end-to-end.
- Q: Does pricing get a full domain pack or does `keep_quantity_in_name` stay in the kernel? → A: Minimal `domain_packs/pricing/block_sequence.yaml` only. `keep_quantity_in_name` stays in kernel as a generic block (no food-specific logic). No other pricing pack artifacts created this phase.

## Assumptions

- `domain_packs/nutrition/` will be created as part of this feature, populated with all artifacts derived from current food-domain code. The old food-domain files are removed from kernel locations only after the nutrition pack is validated.
- The DAG factory (Phase 3 of revamp) is not in scope here. Existing food-source DAGs (`usda_dag.py`, `openfda_incremental_dag.py`, `off_incremental_dag.py`) remain in `airflow/dags/` and continue to work via `config/schemas/nutrition_schema.json` aliasing.
- UC2 downstream consumers that receive `block_end` events are assumed to handle variable null-rate field sets without schema migration — the Postgres `block_trace` table uses a JSONB column for the stats payload.
- `domain_packs/` directory is created at the repo root, co-located with `src/` and `airflow/`.
- Custom blocks discovered from `domain_packs/<domain>/custom_blocks/` are loaded with Python's `importlib` at registry startup; they do not require a Poetry dependency entry unless they introduce new third-party imports.
- The generic fallback `FALLBACK_SEQUENCE` consists of: `dq_score_pre`, `__generated__`, `strip_whitespace`, `remove_noise_words`, `dq_score_post` — sufficient to produce a schema-normalized, whitespace-cleaned output for an unconfigured domain.
- `keep_quantity_in_name` is a generic kernel block (strips quantity tokens from product names — not food-specific). It remains in `src/blocks/` and `_BLOCKS`. Pricing accesses it via `domain_packs/pricing/block_sequence.yaml`.
- Safety domain (`domain == "safety"`) has no enrichment-stage blocks and no custom blocks; a minimal `domain_packs/safety/block_sequence.yaml` is created this phase for consistency, eliminating any remaining safety-specific branching in the registry.
