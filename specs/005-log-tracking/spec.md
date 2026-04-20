# Feature Specification: Pipeline Run Log Tracking & Observability Chatbot

**Feature Branch**: `005-log-tracking`  
**Created**: 2026-04-20  
**Status**: Draft

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Query Pipeline History in Natural Language (Priority: P1)

A developer asks "Why did last Tuesday's USDA run produce fewer rows than Monday's?" and gets a grounded answer sourced from stored run logs — without manually digging through output files or terminal logs.

**Why this priority**: Core value of the feature. Without queryable storage, everything else is inert.

**Independent Test**: Ingest two sample run logs with known differences. Query the chatbot "which run had more quarantined rows?" and verify it returns the correct run_id with evidence.

**Acceptance Scenarios**:

1. **Given** two stored pipeline runs with different `row_count_quarantined` values, **When** user asks "which run quarantined more rows?", **Then** chatbot returns the correct run_id and the quarantine count cited from the log.
2. **Given** a run where `fuzzy_deduplicate` reduced rows by >10%, **When** user asks "which block caused the biggest row drop?", **Then** chatbot names `fuzzy_deduplicate` with the exact delta.
3. **Given** no stored runs, **When** user queries, **Then** chatbot responds "No pipeline runs found" — no hallucination.

---

### User Story 2 - Automatic Log Ingestion After Each Pipeline Run (Priority: P2)

After every pipeline execution, run metadata, per-block stats, DQ scores, enrichment stats, and quarantine reasons are automatically persisted to the log store — no manual action required.

**Why this priority**: Chatbot is useless without data. Ingestion must be automatic and zero-friction.

**Independent Test**: Run the pipeline against `data/usda_fooddata_sample.csv`. Check the log store directly — verify a run entry exists with correct `dq_score_pre`, `dq_score_post`, block-level `rows_in`/`rows_out`, and `run_type` tag.

**Acceptance Scenarios**:

1. **Given** a completed pipeline run, **When** `save_output_node` finishes, **Then** a `PipelineRunLog` entry is written with `run_id`, `run_type`, timestamp, DQ scores, enrichment stats, and all block audit entries.
2. **Given** a failed pipeline run, **When** the run errors mid-way, **Then** a partial log entry is written with `status=failed` and the last completed block recorded.
3. **Given** `run_type=dev`, **When** the chatbot is queried in demo mode, **Then** dev runs are excluded from responses unless explicitly requested.

---

### User Story 3 - Streamlit Observability Dashboard (Priority: P3)

A visual dashboard shows pipeline run history, per-block performance trends, DQ score distributions, and LLM cost tracking — sourced from the same log store the chatbot uses.

**Why this priority**: Complements the chatbot. Good for demos and quick visual sanity checks.

**Independent Test**: Open the dashboard with 3+ stored runs. Verify the run history table, DQ score chart, and block duration heatmap all render with correct data.

**Acceptance Scenarios**:

1. **Given** 3 stored runs, **When** dashboard opens, **Then** run history table shows all 3 with status, row counts, DQ scores, and timestamps.
2. **Given** runs across different domains, **When** filtering by domain, **Then** only matching runs shown.
3. **Given** a run with LLM enrichment stats, **When** viewing cost panel, **Then** LLM call count and estimated token cost are displayed.

---

### Edge Cases

- What happens when ChromaDB collection is empty and chatbot is queried?
- How does the system handle a pipeline crash before `save_output_node`?
- What if two concurrent runs write logs simultaneously?
- How does `run_type` filtering interact with date-range filtering in the chatbot?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST persist a `PipelineRunLog` entry after every pipeline run (success or failure).
- **FR-002**: Each log entry MUST include: `run_id`, `run_type` (`dev`|`demo`|`prod`), `status`, `source_path`, `domain`, `dq_score_pre`, `dq_score_post`, `row_count_in`, `row_count_out`, `row_count_quarantined`, `enrichment_stats`, `started_at`, `completed_at`.
- **FR-003**: Each log entry MUST include per-block audit records: `block_name`, `rows_in`, `rows_out`, `duration_ms`, `extra_meta`.
- **FR-004**: System MUST support natural language queries over stored run logs via a RAG chatbot.
- **FR-005**: Chatbot MUST filter by `run_type` — default query scope is `demo` and `prod` only; `dev` runs excluded unless caller passes `include_dev=True`.
- **FR-006**: Log entries MUST be vectorized and stored in ChromaDB for semantic search.
- **FR-007**: `run_type` MUST be configurable via environment variable `PIPELINE_RUN_TYPE` (default: `dev`).
- **FR-008**: Dashboard MUST render run history, DQ score trends, block duration heatmap, and LLM cost panel from log store.
- **FR-009**: Chatbot responses MUST cite the source run_id(s) used to generate the answer.

### Pipeline Governance Constraints *(mandatory when applicable)*

- Log ingestion hooks into `save_output_node` — no changes to upstream nodes or YAML mapping behavior.
- No impact on `config/unified_schema.json` or downstream required columns.
- Safety fields (`allergens`, `dietary_tags`, `is_organic`) appear in logs as read-only metadata — not re-inferred.
- DQ scoring behavior is unchanged; log tracking reads `dq_score_pre`/`dq_score_post` from existing `PipelineState`.
- YAML mapping replay behavior unaffected — log store is append-only, never modifies generated mappings.

### Key Entities *(include if feature involves data)*

- **PipelineRunLog**: Top-level record for a single pipeline execution. Contains run metadata, DQ scores, enrichment stats, quarantine summary.
- **BlockAuditEntry**: Per-block execution record within a run. `rows_in`, `rows_out`, `duration_ms`, `extra_meta`.
- **ObservabilityDocument**: ChromaDB document — serialized text representation of a run log, used for RAG retrieval.
- **RunTypeTag**: Enum `dev | demo | prod`. Attached to every log entry at write time. Controls chatbot query scope.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After any pipeline run, the log entry appears in ChromaDB within 5 seconds of `save_output_node` completing.
- **SC-002**: Chatbot correctly answers 4 out of 5 factual questions about stored runs (e.g., row counts, block deltas, DQ scores) when tested against a known log fixture.
- **SC-003**: Demo-mode chatbot returns zero results from `run_type=dev` runs without `include_dev=True`.
- **SC-004**: Dashboard renders run history table for 10 stored runs in under 3 seconds.
- **SC-005**: LLM cost panel is accurate to within 5% of actual token counts recorded in `enrichment_stats`.

## Assumptions

- ChromaDB runs locally (embedded mode) for now; upgrade to hosted ChromaDB when corpus exceeds 50K documents.
- Claude (via Anthropic SDK) is the chatbot LLM — consistent with existing LiteLLM setup.
- `run_type` is set at pipeline invocation time, not inferred post-hoc.
- Streamlit dashboard extends existing `app.py` rather than creating a separate app.
- Log store is append-only; no delete or edit operations on past run logs.
- Pipeline state (`PipelineState` from `src/agents/state.py`) is the authoritative source for all logged fields — no re-computation in the logging layer.
