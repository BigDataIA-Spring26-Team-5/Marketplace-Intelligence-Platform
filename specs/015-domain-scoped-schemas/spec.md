# Feature Specification: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Feature Branch**: `aqeel`
**Created**: 2026-04-22
**Status**: Draft
**Depends on**: 013-silver-gold-pipeline, 014-gold-layer-revamp
**Constitution**: v2.0.0 (Principle I amended, Principle IX introduced)

---

## Clarifications

### Session 2026-04-22

- Q: Does Gold concatenation accumulate Silver output across runs (reading from disk), or overwrite with only the current run's in-memory Silver DataFrame? → A: Gold concat scans `output/silver/<domain>/` on disk for all Silver parquet files, concatenates them, and overwrites `output/gold/<domain>.parquet`. Each run writes its Silver output to `output/silver/<domain>/<source_name>.parquet` first; Gold is rebuilt from the full accumulated set.
- Q: Should Silver parquet files be named by run ID (accumulate) or source name (idempotent)? → A: Named by source (e.g., `usda_fooddata_sample.parquet`). Re-running a source overwrites its Silver file — no duplicate rows accumulate in Gold output.

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Agent 1 Analyzes Against Domain Schema (Priority: P1)

A data engineer runs the pipeline for a USDA nutrition dataset with `--domain nutrition`.
Agent 1 performs gap analysis against `config/schemas/nutrition_schema.json` — not the
retired `unified_schema.json`. The engineer sees schema gaps reported in terms of the
nutrition domain's canonical column set, and generated YAML mappings reference only
nutrition-domain columns.

**Why this priority**: All downstream blocks, DQ scoring, and YAML mapping registration
depend on which schema Agent 1 uses. Wrong schema → wrong gap detection → wrong mappings.
This is the root change all other stories depend on.

**Independent Test**: Run the pipeline on `data/usda_fooddata_sample.csv` with
`--domain nutrition`. Verify Agent 1's gap analysis output references
`config/schemas/nutrition_schema.json` (not `unified_schema.json`). The generated YAML
`src/blocks/generated/nutrition/DYNAMIC_MAPPING_usda_fooddata_sample.yaml` must not
contain columns absent from `nutrition_schema.json`.

**Acceptance Scenarios**:

1. **Given** `config/schemas/nutrition_schema.json` exists, **When** the pipeline runs with `--domain nutrition`, **Then** `analyze_schema_node` loads `nutrition_schema.json` as the schema contract and returns gap classifications against it — no reference to `unified_schema.json`.
2. **Given** `config/schemas/safety_schema.json` exists with a different column set, **When** the pipeline runs with `--domain safety`, **Then** Agent 1 loads `safety_schema.json` and reports gaps relative to the safety column set — not the nutrition column set.
3. **Given** an operator passes `--domain nutrition` and the source CSV contains a column not in `nutrition_schema.json`, **Then** Agent 1 classifies that column as `DELETE` — it is not carried forward.
4. **Given** `config/schemas/<domain>_schema.json` is absent for the requested domain, **When** the pipeline starts, **Then** it fails immediately with a clear error message naming the missing file — it does not fall back to `unified_schema.json`.
5. **Given** the domain is not passed (no `--domain` arg), **When** the pipeline starts, **Then** it uses `nutrition` as the default domain and loads `nutrition_schema.json`.

---

### User Story 2 — Silver Normalization Enforces Uniform Column Set (Priority: P2)

After blocks run on a USDA dataset (nutrition domain), `run_pipeline` applies a Silver
normalization step that guarantees the output DataFrame has exactly the columns defined
in `nutrition_schema.json` — in the canonical order, with null fills for any required
columns the blocks did not populate. Rows with null-filled required columns have their
`dq_score_pre` reduced to reflect the missing data before enrichment begins.

**Why this priority**: Silver normalization is the enforcement mechanism for domain-schema
compliance. Without it, every downstream consumer (Gold concatenation, BigQuery write)
would need to independently handle column-set drift. Uniform Silver output is the
prerequisite for safe Gold concatenation.

**Independent Test**: Run the pipeline on two datasets in the same domain (e.g.,
`usda_fooddata_sample.csv` and a stub CSV with fewer columns). Inspect both Silver output
DataFrames immediately after block execution. Both must have exactly the same columns and
column order. Any column required by the schema but absent in the raw source must appear
as null.

**Acceptance Scenarios**:

1. **Given** a Bronze dataset processed by all registered blocks, **When** `run_pipeline` reaches Silver normalization, **Then** the output DataFrame columns match exactly the column set and order defined in `config/schemas/<domain>_schema.json` — no extra columns, no missing columns.
2. **Given** a Bronze dataset missing a required column (e.g., `published_date` absent from source), **When** Silver normalization runs, **Then** the column is added with null values and affected rows have their `dq_score_pre` reduced before enrichment starts.
3. **Given** a Bronze dataset that contains columns not in the domain schema (e.g., internal source IDs), **When** Silver normalization runs, **Then** those columns are dropped from the Silver output DataFrame.
4. **Given** `run_pipeline` applies Silver normalization, **When** examining `BlockRegistry`, **Then** no block named `silver_normalize` or equivalent exists — normalization is not a registered block.
5. **Given** Silver normalization runs and a required column is null-filled, **When** the run log is written, **Then** `dq_score_pre` reflects the penalty for the null-filled required fields — the pre-enrichment score is not artificially inflated.

---

### User Story 3 — Gold Concatenation Produces Domain-Scoped Output (Priority: P3)

After all Silver datasets for the `nutrition` domain are processed, `run_pipeline`
concatenates them into a single file at `output/gold/nutrition.parquet`. A separate run
for the `safety` domain produces `output/gold/safety.parquet`. The two files are never
merged. Both files have schema-valid column sets for their respective domains.

**Why this priority**: Domain-scoped Gold output is the delivery artifact for downstream
consumers (BigQuery, analytics). Cross-domain concatenation would silently corrupt records
by conflating nutrition and safety columns. This story delivers the final governed output.

**Independent Test**: Run the pipeline twice on the same domain (two different source
CSVs, both `--domain nutrition`). Inspect `output/gold/nutrition.parquet` — it must
contain rows from both sources, identical column set, no safety-domain columns.
Run once with `--domain safety` and confirm `output/gold/safety.parquet` is written
separately and shares no rows with the nutrition output.

**Acceptance Scenarios**:

1. **Given** two nutrition-domain Silver DataFrames have been produced in the same pipeline run, **When** Gold concatenation runs, **Then** `output/gold/nutrition.parquet` contains all rows from both DataFrames with a single, consistent column set.
2. **Given** a safety-domain run has also been executed, **When** inspecting Gold output, **Then** `output/gold/safety.parquet` exists separately — rows from safety and nutrition outputs are never combined.
3. **Given** `run_pipeline` performs Gold concatenation, **When** examining `BlockRegistry`, **Then** no block named `gold_concat` or equivalent exists — concatenation is not a registered block.
4. **Given** `output/gold/<domain>.parquet` already exists from a prior run, **When** a new run produces Gold output for the same domain, **Then** the file is overwritten (not appended) with the current run's output.
5. **Given** a Gold concatenation run completes, **When** the run log `save_output` entry is written, **Then** it records the output path, row count, and domain — confirming which domain file was written.

---

### Edge Cases

- `config/schemas/<domain>_schema.json` file is malformed JSON — pipeline fails at startup with a parse error, not silently.
- Domain schema file exists but has zero columns — Silver normalization produces an empty DataFrame; `dq_score_pre` = 0 for all rows; run log records 0 output rows.
- Two source CSVs in the same domain run have different raw column sets — Silver normalization reconciles both to the same domain schema column set before concatenation; no schema mismatch at concat time.
- A domain schema column is marked required but the Bronze source has no mapping for it and no configured default — Silver normalization fills with null; `dq_score_pre` penalizes affected rows.
- `output/gold/` and `output/silver/<domain>/` directories do not exist at run time — `run_pipeline` creates both before writing.
- `output/silver/<domain>/` exists but contains no Parquet files when Gold concatenation runs (e.g., all prior Silver writes failed) — Gold concatenation is skipped with a WARNING; no empty Gold file is written.
- Pipeline runs with `--domain pricing` (no enrichment stage) — Silver normalization still enforces pricing schema columns; Gold concatenation still writes `output/gold/pricing.parquet`; no enrichment columns appear in output.
- Shared columns (e.g., `id`, `data_source`, `created_at`) present in multiple domain schemas — each domain schema independently declares them; no cross-domain column registry is needed.
- Same source re-run for the same domain — `output/silver/<domain>/<source_name>.parquet` is overwritten; Gold concat produces the same row count as a first run of that source (no duplicates).

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `config/schemas/` MUST contain exactly one schema file per supported domain: `nutrition_schema.json`, `safety_schema.json`, `pricing_schema.json`. Each file defines the canonical column set (name, type, required flag, enrichment-only flag) for that domain.
- **FR-002**: `analyze_schema_node` MUST load `config/schemas/<domain>_schema.json` using the domain value from `PipelineState["domain"]`. It MUST NOT load `config/unified_schema.json`.
- **FR-003**: If `config/schemas/<domain>_schema.json` does not exist, the pipeline MUST raise `FileNotFoundError` naming the missing file. It MUST NOT fall back to any other schema file.
- **FR-004**: Agent 1 prompts in `src/agents/prompts.py` MUST reference the domain schema (not `unified_schema.json`) as the schema contract for gap classification and operation generation.
- **FR-005**: Columns not present in `config/schemas/<domain>_schema.json` MUST be classified as `DELETE` by Agent 1 during gap analysis. They MUST NOT appear in the Silver output.
- **FR-006**: `run_pipeline` MUST apply Silver normalization as a fixed step after the block sequence completes, before saving output. Silver normalization is NOT a `BlockRegistry`-registered block.
- **FR-007**: Silver normalization MUST reorder and subset the DataFrame columns to match exactly the column set and order defined in `config/schemas/<domain>_schema.json`.
- **FR-008**: Silver normalization MUST add any column declared in the domain schema that is absent from the post-block DataFrame, filling it with null (or a configured per-column default if declared in the schema file).
- **FR-009**: For each row where Silver normalization added a null value to a required column, `dq_score_pre` MUST be recomputed (or adjusted) to reflect the missing required field before enrichment begins.
- **FR-010**: `run_pipeline` MUST perform Gold concatenation as a fixed step after Silver normalization. Gold concatenation is NOT a `BlockRegistry`-registered block.
- **FR-010a**: After Silver normalization, `run_pipeline` MUST write the normalized Silver DataFrame to `output/silver/<domain>/<source_name>.parquet`, where `<source_name>` is the dataset identifier (e.g., `usda_fooddata_sample`). Re-running the same source MUST overwrite the existing Silver file — Gold concatenation is idempotent with respect to repeated source runs.
- **FR-011**: Gold concatenation MUST scan all Parquet files under `output/silver/<domain>/`, concatenate them, and write the result to `output/gold/<domain>.parquet`, overwriting any existing Gold file. Gold output therefore accumulates across pipeline invocations — each new source run adds its Silver parquet to `output/silver/<domain>/` and triggers a full Gold rebuild.
- **FR-011a**: If `output/silver/<domain>/` contains no Parquet files when Gold concatenation runs, `run_pipeline` MUST log a WARNING and skip the Gold write — it MUST NOT write an empty Gold file.
- **FR-012**: Gold concatenation MUST be domain-scoped. Parquet files from different domain subdirectories (`output/silver/nutrition/`, `output/silver/safety/`) MUST NOT be concatenated together.
- **FR-013**: The seven-node graph order (`load_source → analyze_schema → critique_schema → check_registry → plan_sequence → run_pipeline → save_output`) MUST NOT change. Silver normalization and Gold concatenation are internal to `run_pipeline`.
- **FR-014**: `src/schema/analyzer.py` MUST be updated so `get_unified_schema()` (or a renamed equivalent) loads from `config/schemas/<domain>_schema.json`. Existing call sites that pass a domain MUST be updated; call sites with no domain context default to `nutrition`.
- **FR-015**: `README.md` and `CLAUDE.md` references to `config/unified_schema.json` as a governance artifact MUST be updated to reference `config/schemas/<domain>_schema.json`.

### Pipeline Governance Constraints

- `config/unified_schema.json` is retired as a governance artifact per Constitution v2.0.0 Principle I. The file MAY remain on disk during migration but MUST NOT be loaded by any pipeline component after this feature ships.
- YAML mapping behavior (`DynamicMappingBlock`, `src/blocks/generated/<domain>/DYNAMIC_MAPPING_*.yaml`) is unchanged. The domain schema change affects only what Agent 1 uses for gap analysis — not how generated YAML is executed.
- Silver normalization MUST NOT alter enrichment columns (`allergens`, `dietary_tags`, `is_organic`). Safety fields remain S1-extraction-only per Principle V.
- `dq_score_pre` recomputation after Silver normalization MUST use the same column weights defined in `config/schemas/<domain>_schema.json` (replacing the `dq_weights` from `unified_schema.json`).
- Quarantine behavior is unchanged: rows failing required-field checks after enrichment are still quarantined at the `save_output` gate.
- The `__generated__` sentinel in block sequences is unchanged. Silver normalization runs after all blocks (including generated blocks) complete.

### Key Entities

- **Domain Schema File** (`config/schemas/<domain>_schema.json`): Defines the canonical column set for one domain. Attributes per column: name, type, required (bool), enrichment_only (bool), default_value (optional). Also carries `dq_weights` (replacing the same key from `unified_schema.json`).
- **Silver Normalization**: Fixed internal step in `run_pipeline` that enforces domain-schema compliance post-block-sequence. Not a `BlockRegistry` block. Produces a DataFrame with exactly the domain schema column set and order, then writes it to `output/silver/<domain>/<run_id>.parquet`.
- **Silver Parquet Store** (`output/silver/<domain>/`): Durable intermediate directory. One Parquet file per source, named `<source_name>.parquet`. Re-running a source overwrites its file — the store is idempotent. Gold concatenation reads all files in this directory for its domain.
- **Gold Output** (`output/gold/<domain>.parquet`): Domain-scoped file rebuilt by scanning the full Silver Parquet Store each run. Overwrites prior Gold output. Represents the current complete union of all successfully processed Silver datasets for the domain.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Pipeline runs with `--domain nutrition` load `nutrition_schema.json` — verified by log output or schema-load trace — zero references to `unified_schema.json` in the active run path.
- **SC-002**: Silver output DataFrames for any two datasets in the same domain have identical column sets and column order after normalization — verified by comparing `list(df.columns)` for both.
- **SC-003**: `output/gold/<domain>.parquet` is written at the end of every successful domain run; row count matches the sum of all Silver DataFrames concatenated for that domain.
- **SC-004**: A dataset missing a required domain-schema column produces a Silver DataFrame where that column is present (null-filled) and affected rows have a lower `dq_score_pre` than rows where the column is non-null.
- **SC-005**: Running two separate domain runs (`--domain nutrition`, `--domain safety`) produces two separate Gold files with no overlapping rows and no cross-domain columns in either file.
- **SC-006**: No block named `silver_normalize`, `gold_concat`, or equivalent appears in `BlockRegistry.list_blocks()` after this feature ships.
- **SC-007**: All existing pytest tests pass without modification (YAML mapping, DQ scoring, quarantine, enrichment safety boundary tests all unaffected).

---

## Assumptions

- `config/unified_schema.json` currently contains one flat column set used by all domains. The migration extracts that column set into domain schema files — no columns are lost, they are re-homed into the appropriate domain file(s).
- Shared columns (`id`, `data_source`, `created_at`, `dq_score_pre`, `dq_score_post`, `dq_delta`) appear in all three domain schema files. Each domain schema file is self-contained and declares all columns it needs.
- The `dq_weights` block from `unified_schema.json` is replicated into each domain schema file. Domain-specific weight tuning is out of scope for this feature — all three files start with the same weights.
- `output/gold/` and `output/silver/<domain>/` directories do not exist in the current codebase; `run_pipeline` creates both on first write.
- Each pipeline invocation processes exactly one source. Gold concatenation reads ALL Silver parquet files accumulated under `output/silver/<domain>/` and rebuilds Gold output from the full set — so Gold reflects all sources run to date for that domain.
- The `pricing` domain currently has no enrichment stage. Silver normalization still applies for pricing; Gold concatenation still writes `output/gold/pricing.parquet`. Pricing schema contains no enrichment columns.
- `config/unified_schema.json` is NOT deleted as part of this feature — it is left in place but no longer loaded. Deletion is a separate cleanup task.
- The Gold pipeline (`src/pipeline/gold_pipeline.py`) and the ETL runner pipeline (`src/pipeline/runner.py`) both call `get_unified_schema()` or equivalent. Both call sites are updated to load the domain schema. The Gold pipeline already has a `domain` parameter; `runner.py` receives domain via `config["domain"]`.
- Existing YAML mapping files in `src/blocks/generated/<domain>/` are compatible with domain schema files. No re-generation of YAML mappings is required as part of this feature.
