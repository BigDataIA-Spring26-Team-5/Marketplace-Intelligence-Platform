# Feature Specification: UC1 Data Enrichment Pipeline

**Feature Branch**: `[001-data-enrichment-pipeline]`  
**Created**: 2026-04-17  
**Status**: Draft  
**Input**: User description: "UC1: Data Enrichment Pipeline - schema-driven ETL with LangGraph and HITL"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Data Source Ingestion (Priority: P1)

A data operator wants to load a new CSV data source (e.g., USDA, FDA, Open Food Facts) into the pipeline and have it automatically analyzed against the unified schema.

**Why this priority**: This is the core entry point — without source ingestion, nothing else matters. Every new data source starts here.

**Independent Test**: Can be tested by providing a CSV file and verifying schema analysis output with gap detection results.

**Acceptance Scenarios**:

1. **Given** a CSV file with product data, **When** the user selects it as source and clicks "Analyze Schema", **Then** the system loads the data, profiles each column (dtype, null rate, unique count, sample values), and displays the source schema profile.
2. **Given** source schema profile, **When** the schema analysis runs, **Then** the system compares against the unified schema and produces a gap classification (columns to map, derivable gaps, missing columns) using the 8-primitive taxonomy.
3. **Given** schema gap results, **When** the user reviews the mapping, **Then** they can approve the mapping, exclude specific columns from required schema, or abort the ingestion.

---

### User Story 2 - HITL Schema Approval (Priority: P1)

A data operator wants to review and approve schema mapping decisions before the pipeline executes, with the ability to override missing column handling.

**Why this priority**: HITL ensures data quality and prevents invalid transformations from running. It gives operators control over critical schema decisions.

**Independent Test**: Can be tested by creating a pipeline run and verifying the approval gate appears with correct missing column options.

**Acceptance Scenarios**:

1. **Given** schema analysis output showing missing columns, **When** the HITL gate appears, **Then** the user can see each missing column and choose: accept null values, exclude from required schema, or provide a default value.
2. **Given** HITL decisions made, **When** the user clicks "Approve Mapping & Continue", **Then** the decisions are merged into the YAML mapping before pipeline execution.
3. **Given** truly unresolvable missing columns, **When** the user chooses "Force Continue", **Then** rows with null values in those columns will be quarantined post-enrichment.

---

### User Story 3 - Pipeline Execution with Enrichment (Priority: P1)

A data operator wants the pipeline to execute end-to-end, transforming source data to unified schema, running deduplication, and enriching missing columns through a 3-tier strategy.

**Why this priority**: This delivers the core value — transforming heterogeneous sources into a unified, enriched product catalog.

**Independent Test**: Can be tested by running a complete pipeline and verifying output contains all unified schema columns with enrichment values.

**Acceptance Scenarios**:

1. **Given** approved schema mapping, **When** the user clicks "Run Pipeline", **Then** the pipeline executes in order: DQ pre-score → schema transform → cleaning → dedup → enrichment → DQ post-score.
2. **Given** enrichment execution, **When** S1 (deterministic) runs, **Then** primary_category, allergens, dietary_tags, and is_organic are extracted via regex/keywords from source text.
3. **Given** remaining missing categories after S1, **When** S2 (KNN) runs, **Then** products are matched against FAISS corpus using embedding similarity.
4. **Given** still-missing categories after S2, **When** S3 (LLM) runs, **Then** the LLM categorizes using top-3 corpus neighbors as RAG context.

---

### User Story 4 - Quarantine Handling (Priority: P2)

A data operator wants to review rows that failed post-enrichment validation and decide whether to include them in output or exclude them.

**Why this priority**: Quarantine ensures data quality — operators can either accept incomplete data or override to include all rows.

**Independent Test**: Can be tested by running pipeline with intentionally missing required columns and verifying quarantine table displays correctly.

**Acceptance Scenarios**:

1. **Given** rows with null values in required columns post-enrichment, **When** results display, **Then** those rows are shown in quarantine table with reasons for each missing field.
2. **Given** quarantine table, **When** the user clicks "Accept Quarantine", **Then** quarantined rows are excluded from final output CSV.
3. **Given** quarantine table, **When** the user clicks "Override: Include All Rows", **Then** all rows are included in output and quarantine is cleared.

---

### User Story 5 - Pipeline Memory / Replay (Priority: P2)

A data operator wants to re-run a previously processed source and have the pipeline recognize the existing YAML mapping, avoiding redundant LLM analysis.

**Why this priority**: Self-extending behavior reduces cost and time for repeated sources — key for operational efficiency.

**Independent Test**: Can be tested by running pipeline twice on the same source and verifying second run shows "Pipeline remembered" without LLM schema analysis.

**Acceptance Scenarios**:

1. **Given** a source that was previously processed, **When** re-running the pipeline, **Then** the BlockRegistry auto-discovers the existing DYNAMIC_MAPPING_*.yaml file.
2. **Given** existing YAML mapping found, **When** check_registry runs, **Then** it reports block_registry_hits and skips schema analysis LLM call.
3. **Given** replay run, **When** pipeline completes, **Then** the output is generated without additional LLM cost.

---

### Edge Cases

- How does the system handle CSV files exceeding available memory (e.g., 12GB Open Food Facts)?
- What happens when the CSV file is empty or has no valid rows?
- How does system handle CSV files with encoding issues (UTF-8 vs Latin-1)?
- What when the unified schema is missing entirely (first run scenario)?
- How does the system handle columns with JSON/array structures that require SPLIT operations?
- What when LLM API fails during schema analysis or enrichment?
- How does the system behave when FAISS index is corrupted or missing for S2?

## Clarifications

### Session 2026-04-18

- Q: The spec mentions pipeline runs for datasets up to 10,000 rows (SC-001) but has no handling for large files. The 12.8GB Open Food Facts file caused a crash (OOM). What file size/row limits should the spec define, or should it support chunked processing? → A: Support chunked processing - Add chunking/streaming: process in batches with checkpoint/resume, no hard row limit

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST load CSV files from the data/ directory and profile each column (dtype, null_rate, unique_count, sample_values, detected_structure).
- **FR-002**: System MUST analyze source schema against unified schema and classify gaps using the 8-primitive taxonomy (RENAME, CAST, FORMAT, DELETE, ADD, SPLIT, UNIFY, DERIVE).
- **FR-003**: System MUST generate YAML mapping files for all schema operations and execute them via DynamicMappingBlock.
- **FR-004**: System MUST run a 3-strategy enrichment cascade: S1 (deterministic regex/keywords), S2 (FAISS KNN corpus), S3 (RAG-augmented LLM).
- **FR-005**: System MUST compute DQ scores pre-enrichment and post-enrichment using formula: (Completeness × 0.4) + (Freshness × 0.35) + (Ingredient Richness × 0.25).
- **FR-006**: System MUST quarantine rows with null required columns post-enrichment and provide user override.
- **FR-007**: System MUST auto-discover and reuse existing YAML mappings on replay runs.
- **FR-008**: System MUST provide Streamlit UI with 5-step wizard: Select Source → Schema Analysis → Schema Mapping → Pipeline Execution → Results.
- **FR-009**: System MUST enforce safety constraint: allergens, is_organic, dietary_tags are extraction-only (S2/S3 MUST NOT modify them).
- **FR-010**: System MUST provide HITL approval gates at schema mapping and quarantine stages.
- **FR-011**: System MUST support chunked processing for large files: stream data in configurable batches with checkpoint/resume capability for each chunk, enabling processing of files of any size without OOM.

### Key Entities

- **DataSource**: CSV file with heterogeneous schema — has columns, types, and sample values that differ from unified schema.
- **UnifiedSchema**: Target schema defining required columns (product_name, brand_owner, brand_name, ingredients, category, serving_size, serving_size_unit, published_date, data_source), enrichment columns (allergens, primary_category, dietary_tags, is_organic), and computed columns (dq_score_pre, dq_score_post, dq_delta).
- **SchemaGap**: Classification of how a unified column maps from source — includes gap type (MAP, ADD, DERIVE, MISSING), source column, target column, and action.
- **PipelineState**: LangGraph state that flows through all nodes — contains source_df, source_schema, gaps, operations, block_sequence, working_df, etc.
- **TransformationBlock**: Atomic operation that transforms data — base class defines run(df, config) → df interface.
- **EnrichmentCorpus**: FAISS index of previously enriched products used for KNN similarity matching.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can complete a full pipeline run (source to unified output) in under 5 minutes for datasets up to 10,000 rows.
- **SC-002**: System correctly classifies at least 95% of source columns against the unified schema on first analysis.
- **SC-003**: S1 deterministic extraction resolves at least 60% of enrichment column gaps without calling LLM.
- **SC-004**: S2 KNN corpus achieves at least 70% accuracy on category assignment (compared to ground truth).
- **SC-005**: Quarantine correctly identifies rows with null required columns with zero false positives.
- **SC-006**: Replay runs achieve zero LLM cost by reusing existing YAML mappings.
- **SC-007**: DQ score delta shows measurable improvement (post > pre) for at least 80% of rows that receive enrichment.

## Assumptions

- Users have access to DeepSeek API key for LLM calls (or other LiteLLM-supported provider).
- Data sources are CSV files in standard tabular format (no nested structures in CSV cells except JSON strings).
- The unified schema is pre-defined in config/unified_schema.json (not dynamically derived for new domains).
- Domain is limited to "nutrition", "safety", or "pricing" — domain determines block sequence.
- Users have basic familiarity with data pipelines and schema concepts.
- Network connectivity is required for LLM calls (S3 enrichment) — offline operation limited to S1+S2.