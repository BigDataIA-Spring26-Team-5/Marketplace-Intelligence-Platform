# Feature Specification: Bronze → Silver Pipeline Execution

**Feature Branch**: `012-bronze-silver-pipeline`  
**Created**: 2026-04-21  
**Status**: Draft  
**Input**: User description: "Pipeline execution must takes ingested data from Bronze layer and perform necessary dynamic transformation after Agent 1, and save output to Silver layer. Most of the code is already implemented, I want to re-enforce it"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Operator runs per-source Bronze-to-Silver pipeline (Priority: P1)

An operator triggers a pipeline run for a single source (e.g., USDA, OpenFDA, OpenFoodFacts). The system reads raw JSONL partitions from the Bronze GCS bucket, applies Agent 1 schema analysis to derive a column mapping, applies dynamic transformation blocks (including any generated mapping blocks), and writes the unified, transformed rows to the Silver GCS bucket as Parquet — without enrichment or global deduplication.

**Why this priority**: Core medallion flow. Every downstream Gold-layer run depends on Silver output being present, correctly shaped, and traceable.

**Independent Test**: Trigger a pipeline run with `pipeline_mode = "silver"` and a valid Bronze GCS URI. Verify a Parquet file appears at the expected Silver path and contains the unified 14-column schema plus three metadata columns.

**Acceptance Scenarios**:

1. **Given** a Bronze GCS path `gs://mip-bronze-2024/usda/2026/04/20/*.jsonl` is set as `source_path` and `pipeline_mode = "silver"`, **When** the pipeline graph is invoked, **Then** Agent 1 runs `analyze_schema_node`, a column mapping is derived, transformation blocks execute (including any DYNAMIC_MAPPING blocks), and the result is written to `gs://mip-silver-2024/usda/2026/04/20/part_0000.parquet`.

2. **Given** a Bronze source whose column names differ from the unified schema, **When** Agent 1 completes, **Then** `column_mapping` in PipelineState contains a source-to-unified rename dict and all downstream blocks read unified column names.

3. **Given** the Silver Parquet is written successfully, **When** `save_output_node` completes, **Then** `silver_output_uri` is populated in state and the Silver watermark for the source is updated.

---

### User Story 2 — Silver output is schema-conformant (Priority: P1)

Every row in Silver output conforms to the 14-column unified schema, regardless of which Bronze source it came from. Three Silver-layer metadata columns (`_source`, `_bronze_file`, `_pipeline_run_id`) are appended. Enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and DQ columns are absent or null — Silver is transform-only.

**Why this priority**: Gold-layer dedup and enrichment assume a stable schema. Schema drift in Silver breaks Gold.

**Independent Test**: Read the written Parquet and assert column set matches unified schema + three metadata columns. Assert no enrichment inference occurred.

**Acceptance Scenarios**:

1. **Given** a Silver Parquet written from any source, **When** its columns are inspected, **Then** all 14 unified columns are present and the three metadata columns `_source`, `_bronze_file`, `_pipeline_run_id` are present.

2. **Given** `pipeline_mode = "silver"`, **When** the block sequence is selected, **Then** the silver block sequence is used (schema-transform blocks only, no `FuzzyDeduplicateBlock`, no `LLMEnrichBlock`).

---

### User Story 3 — Dynamic mapping blocks are applied after Agent 1 (Priority: P2)

When Agent 1 detects schema gaps that require generated transformation logic, any previously registered DYNAMIC_MAPPING blocks are injected into the block sequence at the `__generated__` sentinel and executed. On first run for a new source schema, Agent 2 may generate new mapping functions; on subsequent runs, registry hits bypass Agent 2 entirely.

**Why this priority**: Without dynamic mapping, heterogeneous Bronze sources cannot produce conformant Silver output. This is the self-extending feature of the pipeline.

**Independent Test**: Introduce a source column not covered by static mapping. Verify the pipeline generates or looks up a DYNAMIC_MAPPING block and applies it, producing the target column in output.

**Acceptance Scenarios**:

1. **Given** a Bronze source with a novel column (no registry match), **When** the pipeline runs, **Then** Agent 2 generates a DYNAMIC_MAPPING function, it is saved to the registry, and the column is present in Silver output.

2. **Given** the same source runs a second time, **When** `check_registry_node` runs, **Then** Agent 2 is skipped, the previously generated function is loaded from the registry, and the output is identical.

---

### Edge Cases

- What happens when a Bronze GCS path matches zero blobs? → `GCSSourceLoader` raises `FileNotFoundError`; the pipeline should surface this as a pipeline failure, not a silent empty output.
- What happens when a Bronze blob is empty? → The blob is skipped with a warning; the pipeline continues with remaining blobs.
- What happens when column mapping produces duplicate unified column names? → The runner keeps the last occurrence and logs a warning.
- What happens when `pipeline_mode` is absent from state? → Treated as `"full"` mode; Silver writer and silver block sequence are not activated.
- What happens when GCS write fails mid-chunk? → The Parquet upload fails; no partial Parquet is committed; the watermark is not updated; the run is marked failed.
- What happens when a Silver partition already exists for the same source + date? → Overwrite silently. Pipeline is idempotent; last write wins. Operator need not delete prior output before re-running.

## Clarifications

### Session 2026-04-21

- Q: When a Silver partition already exists for the same source + date, overwrite or error? → A: Overwrite silently — pipeline is idempotent, last write wins.
- Q: Is `_bronze_file` per-row hard requirement or best-effort? → A: Hard requirement — every Silver row must have `_bronze_file` non-null; blob name must be threaded through chunk iteration.
- Q: Should Silver writes retry on transient GCS failure? → A: Yes — retry up to 3× with exponential backoff, symmetric with read retry behavior.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST read Bronze data from a GCS URI pattern (`gs://mip-bronze-2024/{source}/{date}/*.jsonl`) when `source_path` is a GCS URI.
- **FR-002**: System MUST route to `analyze_schema_node` (Agent 1) after `load_source_node` to derive a `column_mapping` before any transformation blocks execute.
- **FR-003**: System MUST apply `column_mapping` to rename source columns to unified column names before iterating the block sequence.
- **FR-004**: System MUST inject DYNAMIC_MAPPING generated blocks at the `__generated__` sentinel position in the block sequence.
- **FR-005**: When `pipeline_mode = "silver"`, system MUST use the silver block sequence (schema-transform only; no dedup, no LLM enrichment).
- **FR-006**: When `pipeline_mode = "silver"`, system MUST write output to `gs://mip-silver-2024/{source}/{date}/part_{chunk:04d}.parquet` via `GCSSilverWriter`. If the object already exists, it MUST be overwritten silently (idempotent re-runs).
- **FR-007**: Every Silver row MUST carry three metadata columns: `_source` (source identifier), `_bronze_file` (originating GCS blob path, non-null — must be threaded through chunk iteration), `_pipeline_run_id` (run ID for lineage).
- **FR-008**: System MUST update the Silver watermark for the source after a successful write.
- **FR-009**: System MUST set `silver_output_uri` in PipelineState after a successful Silver write.
- **FR-010**: When `pipeline_mode = "silver"`, the `FuzzyDeduplicateBlock` and `LLMEnrichBlock` MUST NOT execute.
- **FR-011**: When a registry hit exists for a schema gap, `check_registry_node` MUST skip Agent 2 and load the existing function.
- **FR-012**: System MUST surface GCS read failures (no matching blobs, auth errors) as pipeline errors, not silent empty outputs.
- **FR-013**: `GCSSilverWriter` MUST retry Parquet uploads up to 3× with exponential backoff on transient failures, symmetric with `GCSSourceLoader` read retry behavior. Persistent failure after 3 attempts MUST propagate as a pipeline error.

### Pipeline Governance Constraints *(mandatory when applicable)*

- `pipeline_mode = "silver"` activates the `get_silver_sequence()` branch in `BlockRegistry`; this sequence must not include dedup or enrichment blocks.
- The `__generated__` sentinel in the block sequence is the injection point for DYNAMIC_MAPPING blocks. Its position must not change relative to static transformation blocks.
- Silver output rows must conform to `config/unified_schema.json` unified column names. Silver does not write enrichment or DQ columns.
- The three Silver metadata columns (`_source`, `_bronze_file`, `_pipeline_run_id`) are Silver-layer-only and must not appear in Gold's dedup key.
- HITL approval gates in `app.py` run between `analyze_schema_node` and `run_pipeline_node`; Silver mode must remain compatible with this flow.
- Safety fields (`allergens`, `is_organic`, `dietary_tags`) must remain S1-only; Silver mode must not infer them.

### Key Entities *(include if feature involves data)*

- **Bronze Partition**: A JSONL file in `gs://mip-bronze-2024/{source}/{date}/` representing raw ingested records for one source on one date.
- **Silver Partition**: A Parquet file in `gs://mip-silver-2024/{source}/{date}/` representing transformed, schema-conformant records ready for Gold-layer processing.
- **Column Mapping**: A dict (source column name → unified column name) derived by Agent 1 from the Bronze sample and the unified schema.
- **DYNAMIC_MAPPING Block**: A generated Python function stored in the function registry, injected at `__generated__` to handle novel source columns.
- **Silver Watermark**: A JSON object in the Bronze bucket at `_watermarks/{source}_silver_watermark.json` recording the last successfully written Silver partition date.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every pipeline run with `pipeline_mode = "silver"` produces a Parquet file at the correct Silver GCS path containing all unified column names plus three metadata columns — 100% of runs.
- **SC-002**: Schema gaps detected by Agent 1 are resolved (registry hit or new generation) on 100% of runs; no Bronze column in the unified schema target is silently dropped.
- **SC-003**: Second and subsequent runs against the same source schema do not invoke Agent 2 — registry lookup latency replaces LLM generation, reducing per-run cost.
- **SC-004**: Silver output contains zero rows from `FuzzyDeduplicateBlock` or `LLMEnrichBlock` execution paths — verified by audit log.
- **SC-005**: GCS read or write failures surface as explicit pipeline errors within one pipeline node execution, not after partial output is written.

## Assumptions

- Bronze data is already ingested by teammate Airflow DAGs into `gs://mip-bronze-2024/` before this pipeline runs; ingestion is out of scope.
- GCS authentication uses Application Default Credentials (ADC); `gcloud auth application-default login` is a prerequisite.
- The unified schema (`config/unified_schema.json`) is already established for the nutrition domain; this feature does not modify it.
- `pipeline_mode` is set by the caller (demo.py, app.py, or CLI); this feature does not add a new CLI flag — it re-enforces behavior already gated by that flag.
- Multi-source parallelism (running USDA + OFF + OpenFDA simultaneously) is achieved by multiple independent invocations; shared-state coordination is out of scope.
- The `_bronze_file` metadata column is populated from the GCS blob name during chunk iteration. It is a hard requirement (non-null); the blob name must be threaded through the chunk iterator and added as a column to each chunk before blocks execute.
