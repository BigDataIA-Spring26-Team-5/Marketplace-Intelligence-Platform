# Feature Specification: Observability Log Persistence & RAG Chatbot Interface

**Feature Branch**: `011-observability-rag-chatbot`  
**Created**: 2026-04-21  
**Status**: Draft  
**Input**: User description: "I want to implement a functionality that saves pipeline run log data for the UC2 Observability layer. This log data will have a RAG/Chatbot interface layer where user can have more insights on the pipeline execution"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Pipeline Run Logs Persisted (Priority: P1)

After each pipeline run, all execution log data — node-level durations, row counts, block audit entries, schema gap resolutions, enrichment tier outcomes, DQ scores — is automatically saved to a persistent store.

**Why this priority**: Without persisted logs, the RAG chatbot has no data to answer from. This is the foundational data-collection layer everything else depends on.

**Independent Test**: A pipeline run completes; a user can then open the stored log file/record and verify the run's metadata (run ID, timestamp, node sequence, audit entries) are present and complete, delivering observable pipeline history with no chatbot needed.

**Acceptance Scenarios**:

1. **Given** a pipeline run completes successfully, **When** the run finishes, **Then** a structured log record is saved containing: run ID, start/end timestamps, source file name, node execution order, per-node duration, rows in/out per block, DQ scores (pre/post/delta), and enrichment tier breakdown.
2. **Given** a pipeline run fails mid-execution, **When** the run terminates with an error, **Then** a partial log record is saved capturing all nodes that completed, the failing node name, and the error message, so failure patterns are traceable.
3. **Given** multiple pipeline runs have completed, **When** a user views run history, **Then** each run appears as a distinct record with its own run ID and timestamp, preserving full history without overwriting prior entries.

---

### User Story 2 - Chatbot Answers Questions About Pipeline Runs (Priority: P2)

A user can ask natural-language questions about past pipeline executions — "Why did Run #5 have a low DQ score?", "Which runs triggered Agent 2 code generation?", "What was the average enrichment hit rate last week?" — and receive grounded, accurate answers drawn from the persisted log data.

**Why this priority**: This is the primary user-facing value: transforming opaque execution logs into conversational insights without requiring users to parse raw JSON or CSV.

**Independent Test**: Given at least 3 persisted run logs, a user asks "How many runs used the KNN enrichment tier?" and receives a correct count with run IDs cited as sources.

**Acceptance Scenarios**:

1. **Given** persisted run logs exist, **When** a user asks "What was the DQ score improvement in the last run?", **Then** the chatbot returns the correct `dq_delta` value from the most recent run log, citing the run ID.
2. **Given** persisted run logs exist, **When** a user asks a question whose answer spans multiple runs (e.g., "Which runs had schema gaps?"), **Then** the chatbot retrieves and synthesizes relevant entries from all matching runs.
3. **Given** no run logs exist yet, **When** a user asks any pipeline question, **Then** the chatbot responds clearly that no run data is available yet, rather than hallucinating an answer.
4. **Given** a user asks a question unrelated to pipeline execution (e.g., "What is the capital of France?"), **When** the chatbot processes it, **Then** the chatbot responds that it is scoped to pipeline observability topics.

---

### User Story 3 - Run Comparison & Trend Queries (Priority: P3)

A user can ask comparative questions across runs — "Is DQ score improving over time?", "Are enrichment hit rates declining?", "How long did Agent 2 code generation take on average?" — and receive answers with supporting data from multiple runs.

**Why this priority**: Single-run insights are useful; cross-run trend analysis is what turns observability into actionable feedback for pipeline operators.

**Independent Test**: Given 5+ persisted run logs, a user asks "Show me the trend in dq_delta over the last 5 runs" and receives a ranked/ordered list with values from each run.

**Acceptance Scenarios**:

1. **Given** 5 or more persisted run logs, **When** a user asks for a trend in any numeric metric (DQ scores, enrichment counts, node durations), **Then** the chatbot returns values ordered by run timestamp with run IDs cited.
2. **Given** run logs with varying source files, **When** a user asks "How does DQ performance compare between USDA and FDA sources?", **Then** the chatbot groups and compares the relevant metric by source type.

---

### Edge Cases

- What happens when a log record is malformed or partially written (e.g., crash during save)? — System must detect and skip corrupt records without crashing the chatbot.
- How does the chatbot handle very large log histories (100+ runs) where the full context exceeds retrieval capacity? — Retrieval must be top-K by relevance, not full history dump.
- What happens when two pipeline runs execute concurrently and write logs simultaneously? — Each run must get a unique run ID; log writes must not interleave or corrupt each other.
- How does the chatbot respond when asked about a specific run ID that does not exist? — It must report "run not found" rather than fabricating data.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST automatically save a structured log record at the end of every pipeline run (successful or failed), without requiring any user action.
- **FR-002**: Each log record MUST contain: unique run ID, start timestamp, end timestamp, source file name, list of executed nodes with durations, per-block audit entries (rows_in, rows_out, block name), DQ scores (pre/post/delta), enrichment tier outcome counts (S1/S2/S3 resolved), and error information if applicable.
- **FR-003**: System MUST preserve all historical run records; no run log may be overwritten or deleted by a subsequent run.
- **FR-004**: Users MUST be able to ask natural-language questions about pipeline execution history through a conversational interface and receive accurate, grounded answers.
- **FR-005**: Chatbot responses MUST cite the specific run ID(s) from which the answer was drawn, so users can verify provenance.
- **FR-006**: Chatbot MUST refuse to answer questions outside the domain of pipeline execution observability, clearly stating its scope.
- **FR-007**: System MUST handle the case where no run logs exist and communicate this clearly to the user rather than returning empty or confusing responses.
- **FR-008**: Log retrieval for chatbot queries MUST use relevance-based selection (not full history scan) to remain responsive as log volume grows.
- **FR-009**: System MUST integrate with the existing Streamlit wizard UI (`app.py`) so users can access the chatbot interface from the same tool they use to run pipelines.

### Pipeline Governance Constraints *(mandatory when applicable)*

- Log records are **read-only artifacts** of the pipeline; they must not modify `config/unified_schema.json` or any registry entries.
- Log saving happens **after** `save_output` node completes (or on exception exit); it must not be injected as a graph node that could interfere with the existing `StateGraph` control flow.
- Block audit data is already produced by each block's `audit_entry()` method — log persistence must consume that existing output, not duplicate or replace it.
- No enrichment changes involved; safety fields (`allergens`, `is_organic`, `dietary_tags`) are not part of observability logs.
- The chatbot interface is a **new UC2 surface** that reads persisted logs; it does not re-run or modify any pipeline state.
- If the Streamlit wizard exposes the chatbot, it must be added as a new tab/section without altering existing HITL approval gate behavior.

### Key Entities *(include if feature involves data)*

- **PipelineRunLog**: Represents one complete (or failed) pipeline execution. Key attributes: run_id, timestamp, source_file, status (success/partial/failed), node_sequence, error_info.
- **NodeExecutionRecord**: Child of PipelineRunLog. Represents one node's execution within a run. Key attributes: node_name, start_time, duration_seconds, rows_in, rows_out.
- **BlockAuditRecord**: Child of NodeExecutionRecord (for `run_pipeline` node). Represents one block's audit entry. Key attributes: block_name, rows_in, rows_out, additional block-specific fields from `audit_entry()`.
- **EnrichmentSummary**: Child of PipelineRunLog. Aggregate counts of enrichment tier outcomes per run: s1_resolved, s2_resolved, s3_resolved, unresolved, dq_score_pre, dq_score_post, dq_delta.
- **ChatMessage**: Represents one turn in the observability chatbot. Key attributes: query_text, response_text, cited_run_ids, timestamp.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every pipeline run — successful or failed — produces a persisted log record within 2 seconds of run completion, with no manual action required.
- **SC-002**: 100% of log records contain all mandatory fields defined in FR-002; no field may be silently null due to a collection gap.
- **SC-003**: Chatbot accurately answers factual queries about single-run metrics (DQ scores, node durations, enrichment counts) with correct values matching the stored log, measurable by spot-check comparison against raw log data.
- **SC-004**: Chatbot responds to user queries in under 5 seconds for a log history of up to 50 runs.
- **SC-005**: Chatbot cites at least one specific run ID in every answer that draws on run data; zero ungrounded answers when run data is available.
- **SC-006**: Log history survives application restarts — records written in a prior session are accessible in a subsequent session.
- **SC-007**: The chatbot interface is accessible from the existing Streamlit pipeline wizard without requiring a separate application or login.

## Assumptions

- Pipeline run logs are written locally (same machine as the ETL pipeline); remote/cloud log storage is out of scope for v1.
- Log volume will not exceed ~500 runs in a single deployment; scaling to thousands of runs is a future concern.
- The chatbot interface is embedded in the existing Streamlit wizard (`app.py`) as a new section, not as a standalone web service.
- The existing `PipelineState` and block `audit_entry()` outputs are the authoritative source of truth for log content; the log persistence layer consumes them as-is without re-computing metrics.
- Users accessing the chatbot are the same operators who run the pipeline (no separate authentication layer needed for chatbot access in v1).
- The RAG retrieval layer uses the same LLM routing already in place (`src/models/llm.py`) rather than introducing a new LLM provider.
- UC2 placeholder classes in `src/uc2_observability/` exist but raise `NotImplementedError`; this feature will implement or replace them.
