# Feature Specification: Agent 1 Representative Sampling

**Feature Branch**: `[002-agent-1-sampling]`  
**Created**: 2026-04-17  
**Status**: Draft  
**Input**: User description: "Agent 1 (Orchestrator) currently has insufficient context when analyzing schema gaps. It should sample enough rows from the input dataset to reliably detect field patterns, data types, and value distributions. The minimum sample must be representative enough to avoid false gap detections caused by sparse or missing values in single-row reads."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Representative Row Sampling (Priority: P1)

A data operator loads a CSV with sparse or unevenly distributed values, and needs Agent 1 to analyze enough rows to correctly identify schema gaps.

**Why this priority**: Without representative sampling, Agent 1 misclassifies columns as "missing" when they simply have sparse values in the first few rows, leading to false gap detections and incorrect YAML mappings.

**Independent Test**: Can be tested by providing a CSV where column X has values in rows 100-500 but null in rows 1-99. Verify Agent 1 correctly identifies it as a valid source column, not a gap.

**Acceptance Scenarios**:

1. **Given** a CSV with 10,000 rows where a column has 95% null values distributed throughout, **When** Agent 1 analyzes the schema, **Then** it samples enough rows to detect the non-null values and classifies the column as mappable, not missing.
2. **Given** a CSV with categorical values that only appear in the last 20% of rows, **When** Agent 1 runs, **Then** the sample captures those categories and generates appropriate value_map operations.
3. **Given** a CSV with JSON structures in a sparse column (1 in 100 rows), **When** Agent 1 profiles the schema, **Then** it detects `detected_structure: json_array` correctly.

---

### User Story 2 - Adaptive Sampling Strategy (Priority: P1)

The system automatically adjusts sampling size based on dataset characteristics to balance accuracy against LLM token cost.

**Why this priority**: Different datasets require different sample sizes. Small datasets need full scan; large datasets need statistical sampling. Too much sampling wastes tokens, too little causes false gaps.

**Independent Test**: Can be tested by running the pipeline on datasets of varying sizes (100 rows, 1K rows, 100K rows) and verifying sample size scales appropriately.

**Acceptance Scenarios**:

1. **Given** a dataset with 100 rows, **When** Agent 1 profiles, **Then** it samples 100% of rows (full scan) — token cost is acceptable.
2. **Given** a dataset with 10,000 rows, **When** Agent 1 profiles, **Then** it samples a statistically representative subset (e.g., 500-1000 rows) — enough to detect patterns.
3. **Given** a dataset with 1,000,000 rows, **When** Agent 1 profiles, **Then** it uses stratified sampling to capture edge cases without processing all rows.

---

### User Story 3 - Confidence Scoring for Gap Detection (Priority: P2)

Agent 1 provides a confidence score for each gap classification, enabling HITL to prioritize review of uncertain mappings.

**Why this priority**: Some gap detections are clear-cut (column completely missing), others are ambiguous (sparse values that might be real data). Users need to know which to trust.

**Independent Test**: Can be tested by comparing confidence scores for: (a) truly missing column, (b) sparse column with real values, (c) fully populated column.

**Acceptance Scenarios**:

1. **Given** a column with 0% values in sample but appears in later rows, **When** Agent 1 classifies as ADD set_null, **Then** it flags low confidence, alerting the user to review.
2. **Given** a column with consistent values across sample, **When** Agent 1 classifies as RENAME, **Then** it flags high confidence (≥90%).
3. **Given** a column with mixed types or inconsistent values, **When** Agent 1 classifies, **Then** it flags medium confidence and notes the ambiguity in the output.

---

### Edge Cases

- What happens when the CSV has only null values in the sampled rows but non-null in unsampled rows? (Fallback to full scan)
- How does the system handle extremely skewed distributions (99% same value)? (Log warning, continue with representative sample)
- What when the sample captures only one category out of 10 for a categorical column? (Flag low confidence, suggest full scan)
- How does sampling work for JSON columns with nested structures? (Detect structure from sample, handle sparse JSON similarly)
- What when the dataset is smaller than the minimum sample size? (Full scan for datasets <500 rows)
- **Exception**: What happens when LLM API fails during analysis? (Retry once, then fall back to default gap classifications)
- **Recovery**: What happens when sampling fails due to memory constraints? (Reduce sample size, log warning)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST sample a representative subset of rows from the input CSV before schema profiling, sufficient to detect field patterns, value distributions, and data types reliably.
- **FR-002**: The sampling algorithm MUST avoid false gap detections caused by sparse or missing values in the first N rows — it MUST ensure the sample includes enough rows to capture non-null values for columns with sparse data.
- **FR-003**: System MUST implement adaptive sampling that scales sample size based on dataset size, using statistical methods (min(500, sqrt(n)) + buffer) to ensure representativeness.
- **FR-004**: System MUST provide a confidence score (0.0-1.0) for each gap classification, indicating how certain the LLM is about the mapping decision.
- **FR-005**: System MUST log the sampling methodology (sample size, method, seed) alongside the schema analysis for auditability.
- **FR-006**: System MUST handle edge cases where sampling might miss critical data patterns, with fallback to larger samples or full scan.
- **FR-007**: System MUST display confidence scores in HITL Gate 1 with visual indicators: High (≥90%), Medium (50-89%), Low (<50%).

### Key Entities

- **SchemaProfile**: Enhanced profile that includes sampling metadata (sample_size, sample_method, confidence_scores) alongside column profiles.
- **GapClassification**: Extended to include confidence score and sample evidence (which rows contained the evidence).
- **SamplingStrategy**: Defines the sampling approach per dataset characteristics (full scan, random, stratified, reservoir).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: False gap detection rate drops to less than 5% for columns with sparse values (compared to current single-row baseline).
- **SC-002**: Sampling covers at least 95% of unique value patterns for categorical columns in the dataset.
- **SC-003**: Confidence scores correctly identify high-confidence (>90%) vs low-confidence (<50%) mappings with 85% accuracy. Ground truth methodology: Manually label a sample of 100 column mappings as high/low confidence, compare model predictions against labels.
- **SC-004**: Sample size formula ensures token usage stays within 2x the current baseline regardless of dataset size.
- **SC-005**: Edge case handling (full scan fallback) triggers automatically when sampling shows high null rates (>80%).
- **SC-006**: Sampling calculation completes in under 1 second for datasets up to 1 million rows (performance requirement).

## Assumptions

- Datasets are CSV files with standard tabular structure — no nested multi-file formats.
- The sampling happens during `load_source_node` before `analyze_schema_node` — no change to the 7-node pipeline flow.
- The unified schema definition remains unchanged — only the source profiling step is enhanced.
- LLM API cost is the primary constraint, not compute — sampling optimizes for token efficiency.
- Users are willing to accept a small increase in initial processing time for more accurate schema detection.
- **Dependency**: The system uses pandas for DataFrame operations; sampling relies on pandas' sampling capabilities.