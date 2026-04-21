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

### User Story 3 - Grafana Dashboard for Pipeline Analytics (Priority: P3)

A user opens the Grafana dashboard and sees visual panels for pipeline health over time — DQ score trends, enrichment tier distributions, node durations, and run success/failure rates — without writing any queries or parsing JSON files.

**Why this priority**: The chatbot answers ad-hoc questions; Grafana gives a persistent at-a-glance view of pipeline health trends. Both are complementary observability surfaces. Grafana is P3 because it depends on P1 (logs) and builds on existing Prometheus metrics from the UC2 integration already in place.

**Independent Test**: Given 5+ completed pipeline runs, a user opens the Grafana dashboard and sees a time-series panel for `dq_delta` showing correct values per run without any manual data entry.

**Acceptance Scenarios**:

1. **Given** pipeline runs have completed and metrics are published, **When** a user opens the Grafana dashboard, **Then** they see a time-series panel showing DQ score (pre/post/delta) per run ordered by timestamp.
2. **Given** runs with varying enrichment outcomes, **When** a user views the enrichment panel, **Then** it shows S1/S2/S3 resolved counts and unresolved counts per run as a stacked or grouped visualization.
3. **Given** a run that failed, **When** the user views the run status panel, **Then** the failed run appears distinctly (e.g., different color) with its error label visible.
4. **Given** multiple source types (USDA, FDA), **When** a user filters the dashboard by source type, **Then** all panels update to show only runs for that source.

---

### User Story 4 - Run Comparison & Trend Queries via Chatbot (Priority: P4)

A user can ask comparative questions across runs — "Is DQ score improving over time?", "Are enrichment hit rates declining?", "How long did Agent 2 code generation take on average?" — and receive answers with supporting data from multiple runs.

**Why this priority**: Grafana (P3) covers visual trend analysis; the chatbot's trend queries are a complementary natural-language path to the same insights. Deferred to P4 as Grafana satisfies the core trend-visibility need visually.

**Independent Test**: Given 5+ persisted run logs, a user asks "Show me the trend in dq_delta over the last 5 runs" and receives a ranked/ordered list with values from each run.

**Acceptance Scenarios**:

1. **Given** 5 or more persisted run logs, **When** a user asks for a trend in any numeric metric (DQ scores, enrichment counts, node durations), **Then** the chatbot returns values ordered by run timestamp with run IDs cited.
2. **Given** run logs with varying source files, **When** a user asks "How does DQ performance compare between USDA and FDA sources?", **Then** the chatbot groups and compares the relevant metric by source type.

---

### Edge Cases

- What happens when a log record is malformed or partially written (e.g., crash during save)? — Log writes are atomic (temp-file + rename); a crash before rename leaves no partial file. Any pre-existing corrupt file is detected and skipped at read time without crashing the chatbot.
- How does the chatbot handle very large log histories (100+ runs) where the full context exceeds retrieval capacity? — Retrieval must be top-K by relevance, not full history dump.
- What happens when two pipeline runs execute concurrently and write logs simultaneously? — Each run must get a unique run ID; log writes must not interleave or corrupt each other.
- How does the chatbot respond when asked about a specific run ID that does not exist? — It must report "run not found" rather than fabricating data.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST automatically save a structured log record as a JSON file in `output/run_logs/` at the end of every pipeline run (successful or failed), without requiring any user action.
- **FR-002**: Each log record MUST contain: unique run ID, start timestamp, end timestamp, source file name, list of executed nodes with durations, per-block audit entries (rows_in, rows_out, block name), DQ scores (pre/post/delta), enrichment tier outcome counts (S1/S2/S3 resolved), and error information if applicable.
- **FR-003**: System MUST preserve all historical run records; no run log may be overwritten or deleted by a subsequent run.
- **FR-004**: Users MUST be able to ask natural-language questions about pipeline execution history through a multi-turn conversational interface; the chatbot retains conversation history within a session so follow-up questions can reference prior exchanges without restating context.
- **FR-005**: Chatbot responses MUST cite the specific run ID(s) from which the answer was drawn, so users can verify provenance.
- **FR-006**: Chatbot MUST refuse to answer questions outside the domain of pipeline execution observability, clearly stating its scope.
- **FR-007**: System MUST handle the case where no run logs exist and communicate this clearly to the user rather than returning empty or confusing responses.
- **FR-008**: Log retrieval for chatbot queries MUST use relevance-based selection via per-run summary embeddings: each run's key metrics are flattened to a text summary, embedded, and searched by semantic similarity; the full JSON log is loaded only for matched runs. This avoids full history scans as log volume grows.
- **FR-009**: System MUST integrate with the existing pipeline management interface so users can access the chatbot interface from the same tool they use to run pipelines.
- **FR-010**: System MUST publish key pipeline run metrics (DQ scores pre/post/delta, enrichment tier counts, node durations, run status) to Prometheus after each run so they are available as a Grafana data source.
- **FR-011**: A Grafana dashboard MUST provide at minimum: (a) time-series panel for DQ scores per run, (b) enrichment tier distribution panel (S1/S2/S3/unresolved counts), (c) run status panel (success/partial/failed), (d) source-type filter to scope all panels to a single data source.

### Pipeline Governance Constraints *(mandatory when applicable)*

- Log records are **read-only artifacts** of the pipeline; they must not modify `config/unified_schema.json` or any registry entries.
- Log saving happens **after** `save_output` node completes (or on exception exit); it must not be injected as a graph node that could interfere with the existing `StateGraph` control flow.
- Block audit data is already produced by each block's `audit_entry()` method — log persistence must consume that existing output, not duplicate or replace it.
- No enrichment changes involved; safety fields (`allergens`, `is_organic`, `dietary_tags`) are not part of observability logs.
- The chatbot interface is a **new UC2 surface** that reads persisted logs; it does not re-run or modify any pipeline state.
- If the Streamlit wizard exposes the chatbot, it must be added as a new tab/section without altering existing HITL approval gate behavior.
- The Grafana dashboard is a **read-only observability surface**; it reads from Prometheus only and has no write path back to the pipeline, registry, or schema.

### Key Entities *(include if feature involves data)*

- **PipelineRunLog**: Represents one complete (or failed) pipeline execution. Key attributes: run_id, timestamp, source_file, status (success/partial/failed), node_sequence, error_info.
- **NodeExecutionRecord**: Child of PipelineRunLog. Represents one node's execution within a run. Key attributes: node_name, start_time, duration_seconds, rows_in, rows_out.
- **BlockAuditRecord**: Child of NodeExecutionRecord (for `run_pipeline` node). Represents one block's audit entry. Key attributes: block_name, rows_in, rows_out, additional block-specific fields from `audit_entry()`.
- **EnrichmentSummary**: Child of PipelineRunLog. Aggregate counts of enrichment tier outcomes per run: s1_resolved, s2_resolved, s3_resolved, unresolved, dq_score_pre, dq_score_post, dq_delta.
- **ChatSession**: Represents one multi-turn conversation in the observability chatbot. Key attributes: session_id, start_timestamp, message_history (ordered list of ChatMessages).
- **ChatMessage**: Represents one turn within a ChatSession. Key attributes: role (user/assistant), content, cited_run_ids, timestamp.
- **GrafanaDashboard**: A provisioned Grafana dashboard definition (JSON) exported from this feature. Contains panel definitions for DQ trends, enrichment distribution, run status, and source-type filter variable. Deployed by placing the JSON in Grafana's provisioning directory.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every pipeline run — successful or failed — produces a persisted log record within 2 seconds of run completion, with no manual action required.
- **SC-002**: 100% of log records contain all mandatory fields defined in FR-002; no field may be silently null due to a collection gap.
- **SC-003**: Chatbot accurately answers factual queries about single-run metrics (DQ scores, node durations, enrichment counts) with correct values matching the stored log, measurable by spot-check comparison against raw log data.
- **SC-004**: Chatbot responds to user queries in under 5 seconds for a log history of up to 50 runs.
- **SC-005**: Chatbot cites at least one specific run ID in every answer that draws on run data; zero ungrounded answers when run data is available.
- **SC-006**: Log history survives application restarts — records written in a prior session are accessible in a subsequent session.
- **SC-007**: The chatbot interface is accessible from the existing pipeline management interface without requiring a separate application or additional login.
- **SC-008**: The Grafana dashboard displays correct metric values for all completed runs within 10 seconds of a run finishing, with no manual refresh required.

## Assumptions

- Pipeline run logs are written locally (same machine as the ETL pipeline); remote/cloud log storage is out of scope for v1.
- Each pipeline run produces one JSON file in `output/run_logs/`, named by run ID (e.g., `run_<id>.json`). The directory is gitignored.
- Log volume will not exceed ~500 runs in a single deployment; scaling to thousands of runs is a future concern. No automatic deletion or rotation is performed; operators may manually prune `output/run_logs/` if needed.
- The chatbot interface is embedded in the existing Streamlit wizard (`app.py`) as a new section, not as a standalone web service.
- The existing `PipelineState` and block `audit_entry()` outputs are the authoritative source of truth for log content; the log persistence layer consumes them as-is without re-computing metrics.
- Users accessing the chatbot are the same operators who run the pipeline (no separate authentication layer needed for chatbot access in v1).
- The RAG retrieval layer uses the same LLM routing already in place (`src/models/llm.py`) rather than introducing a new LLM provider.
- UC2 placeholder classes in `src/uc2_observability/` exist but raise `NotImplementedError`; this feature will implement or replace them.
- Prometheus metrics are already being pushed by the UC2 integration (`010-uc1-uc2-integration`); this feature extends that existing metric push to include per-run log metrics. No new Prometheus instance is needed.
- Grafana is assumed to be running locally (e.g., via Docker or local install) with a Prometheus data source configured. Dashboard provisioning (JSON export) is in scope; Grafana installation is out of scope.

## Clarifications

### Session 2026-04-21

- Q: What is the storage format for persisted pipeline run logs? → A: JSON files, one file per run, stored in `output/run_logs/`.
- Q: What is the retrieval unit for the RAG layer? → A: Per-run summary embedding (key metrics flattened to text; full JSON loaded on match).
- Q: How should log writes behave on crash? → A: Atomic write — write to temp file, rename to final path on success; corrupt partial files never appear on disk.
- Q: Is the chatbot stateless or multi-turn? → A: Multi-turn — retains conversation history within a session.
- Q: What is the run log retention policy? → A: Keep all runs indefinitely; no automatic deletion in v1.
- Scope addition (user-directed): Grafana dashboard added as User Story 3 (P3). Prometheus is the data source (already in use via UC2). Dashboard provisioned as JSON; Grafana install is out of scope. FR-010, FR-011, SC-008 added.
