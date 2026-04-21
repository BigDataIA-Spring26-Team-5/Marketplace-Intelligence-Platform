# Research: Observability Log Persistence & RAG Chatbot

**Feature**: 011-observability-rag-chatbot  
**Date**: 2026-04-21

## Decision Log

---

### 1. Log Storage Format

**Decision**: JSONL (newline-delimited JSON) files in `output/run_logs/`

**Rationale**:
- One file per run: `run_<ISO8601>_<uuid8>.jsonl` (e.g., `run_20260421T143012_a3f7b2c1.json`)
- Append-safe — no concurrent-write corruption; each run writes one file atomically (write temp + rename)
- Human-readable, no DB dependency, no migration risk
- `output/` is already gitignored — logs stay local without config change
- Survives application restarts (file-based persistence)

**Alternatives considered**:
- SQLite: more queryable but requires schema migration discipline; overkill for <500 runs
- Single JSONL file (append-per-run): simpler but partial writes on crash corrupt the file; concurrent runs would need locking
- Redis: already in the project for cache but inappropriate for durable long-term storage

---

### 2. Log Integration Point

**Decision**: Call `RunLogWriter.save(state, status, error)` at the end of `save_output_node` in `src/agents/graph.py`, and wrap `save_output_node` in try/except so partial-failure logs are captured too.

**Rationale**:
- `save_output` is the final node — all state fields are populated by then
- `PipelineState` already contains every needed field: `audit_log`, `enrichment_stats`, `dq_score_pre`, `dq_score_post`, `quarantine_reasons`, `block_sequence`, `source_path`, `column_mapping`, `critique_notes`, `errors`
- Wrapping in try/except preserves existing error behavior while still capturing partial-run data

**Alternatives considered**:
- New `save_run_log` LangGraph node: invasive change to graph topology; would need `NODE_MAP` update and Streamlit step registration
- Post-graph hook in `demo.py`/`app.py`: duplicates logic across entry points; easy to miss

---

### 3. RAG Retrieval Strategy

**Decision**: Two-phase retrieval — structured filter first (run_id, date range, source name, metric type) then LLM synthesis over matched log JSON.

**Rationale**:
- Log data is structured; most queries are metric lookups ("what was the DQ score on run X?", "which runs had Agent 2 corrections?") — these are filter operations, not semantic search
- For aggregate queries ("trend over last 5 runs"), filter by recency + project numeric fields
- LLM synthesizes the answer from the filtered JSON context, citing run IDs
- Avoids FAISS dependency for observability (FAISS already used for corpus; adding it for log embeddings would mix concerns and add startup cost)
- For <500 runs, scanning JSONL files in memory is fast enough (<100ms)

**Alternatives considered**:
- ChromaDB (referenced in existing placeholder): external process dependency; heavy for local dev; placeholder assumed it but spec doesn't mandate it
- FAISS on run embeddings: justified only if free-text search over log narratives is needed; current requirements are metric-based
- Full-text search (whoosh, tantivy): unnecessary complexity for structured JSON fields

---

### 4. LLM for Chatbot

**Decision**: Use `get_enrichment_llm()` from `src/models/llm.py` (DeepSeek chat) for chatbot synthesis.

**Rationale**:
- Existing routing; no new API keys or provider config
- DeepSeek chat sufficient for structured-data synthesis ("summarize these JSON run records and answer the question")
- Adding `get_observability_llm()` getter keeps routing centralized and allows future swap without touching chatbot code

**Alternatives considered**:
- Claude (Anthropic): not currently configured in LiteLLM config; would need new key + config
- Local model: too slow for interactive chatbot use

---

### 5. Streamlit Integration Mode

**Decision**: Sidebar radio button "Mode" (`Pipeline` / `Observability`) — when "Observability" selected, renders `ObservabilityPage` instead of the step-based pipeline wizard.

**Rationale**:
- Non-invasive: existing step logic untouched; no index reshuffling
- Mode switch is persistent in `st.session_state` — user can flip back to pipeline mid-session
- Cleaner UX than adding a 6th step to the linear pipeline flow (observability is not a pipeline step)

**Alternatives considered**:
- Streamlit multi-page (`pages/observability.py`): would work but loses shared session state (cache client, log entries); requires navigation outside the app
- New step 5 after Results: conceptually wrong — observability is cross-run, not a step in a single run
- `st.tabs()` at top level: would require restructuring entire app layout

---

### 6. UC2 Placeholder Disposition

**Decision**: Implement `rag_chatbot.py` fully. Leave `anomaly_detection.py` and `dashboard.py` as-is (still raise `NotImplementedError`).

**Rationale**:
- Spec scopes to log persistence + RAG chatbot only
- `PipelineDashboard` and `AnomalyDetector` placeholders are separate UC2 sub-features; implementing them would exceed spec scope
- The new `RunLogWriter` + `RunLogStore` make anomaly detection implementable in a future sprint (data contract established)

---

### 7. Concurrent Run Safety

**Decision**: One file per run with atomic write (write to `.tmp` then `os.rename()`). Run ID generated as `uuid4()` at run start.

**Rationale**:
- `os.rename()` is atomic on POSIX filesystems — partial writes never corrupt existing logs
- UUID run ID guarantees uniqueness even if two runs start at the same millisecond
- No file locking needed because each run writes a separate file

---

### 8. Log Schema Completeness Guarantee

**Decision**: `RunLogWriter` uses `.get()` with explicit defaults for every optional state field. Missing fields log a warning but never raise.

**Rationale**:
- `PipelineState` is `TypedDict(total=False)` — any field may be absent depending on which nodes ran
- A failed run may have `audit_log` but no `dq_score_post`; the log writer must handle this gracefully
- SC-002 ("100% of log records contain all mandatory fields") is satisfied by defining mandatory fields as the intersection of fields always set by `load_source` node (the first node): `source_path`, `run_id`, `timestamp`, `status`
