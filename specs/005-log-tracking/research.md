# Research: Pipeline Run Log Tracking & Observability Chatbot

**Feature**: 005-log-tracking  
**Date**: 2026-04-20

## Summary

All key architectural decisions resolved. This document records rationale and alternatives for each major decision in `plan.md`.

---

## Decision 1: ChromaDB (Embedded) for Vector Store

**Decision**: Use ChromaDB in embedded (local) mode as the primary log store and vector index.

**Rationale**:
- Already referenced in `src/uc2_observability/rag_chatbot.py` stub — this feature completes that planned work.
- Embedded mode requires zero infrastructure: no Docker service, no network hop. Fits current local + VM setup.
- ChromaDB stores both the raw document text and the embedding vector in one collection — no separate SQL + vector DB required.
- Upgrade path to hosted ChromaDB is a one-line client change when corpus grows past 50K documents.

**Alternatives Considered**:
- FAISS + SQLite: FAISS already used for KNN corpus search. Reusing it for log storage would conflate two separate concerns (product corpus vs. run audit logs). Separate store is cleaner.
- pgvector (PostgreSQL extension): Natural fit if PostgreSQL already deployed (spec-006). But spec-006 is not yet implemented; adding a PostgreSQL dependency here blocks this feature on spec-006.
- Pinecone: Managed SaaS, no local option. Breaks offline/demo use.

---

## Decision 2: run_type Tag for Demo-Stage Isolation

**Decision**: Tag every `PipelineRunLog` with `run_type: dev | demo | prod` at write time. Chatbot default scope excludes `dev`.

**Rationale**:
- During development, bad/test runs will outnumber real runs. Without filtering, chatbot answers about "average DQ score" or "most common quarantine reason" will reflect dev noise, not meaningful data.
- Tagging at write time (via `PIPELINE_RUN_TYPE` env var) is zero-friction — no manual labeling required.
- ChromaDB `where` filter on `run_type` metadata is O(1) — no performance cost.
- Design allows a future "include_dev=True" flag for debugging without polluting default responses.

**Alternatives Considered**:
- Separate ChromaDB collections per run_type: Cleaner isolation but complicates cross-type queries (e.g., "compare a dev run to a demo run"). Single collection with metadata filter is more flexible.
- Date-based filtering instead of run_type: Brittle — dev and demo runs can happen on the same day.

---

## Decision 3: Claude via Anthropic SDK as Chatbot LLM

**Decision**: Use Claude (Anthropic SDK) for the RAG chatbot, not LiteLLM/DeepSeek.

**Rationale**:
- Pipeline agents (Agent 1, 2, 3) already use DeepSeek via LiteLLM for schema analysis and sequence planning. Chatbot is a different concern — it answers free-form questions about run history, not structured ETL operations.
- Claude's instruction-following and citation behavior is well-suited for grounded Q&A ("only answer from the provided context, cite run_id").
- Anthropic SDK is already in the dependency tree (referenced in `uc2_observability/rag_chatbot.py`).
- Prompt caching available on Claude — repeated chatbot sessions with the same run log context will benefit from cache hits.

**Alternatives Considered**:
- DeepSeek via LiteLLM: Consistent with pipeline LLM choice. But DeepSeek's instruction-following for citation-grounded Q&A is weaker than Claude. Hallucination risk higher for a chatbot that must say "I don't know" when logs are absent.
- Local LLM (Ollama): Zero API cost. But quality gap is significant for natural language Q&A over structured logs. Not worth the tradeoff for a demo-facing feature.

---

## Decision 4: Log Ingestion Hook in save_output_node

**Decision**: Inject log persistence as a post-step inside `save_output_node` in `src/agents/orchestrator.py`, not as a separate LangGraph node.

**Rationale**:
- Adding a new node (`log_run_node`) would change the seven-node pipeline order mandated by the constitution. A post-step hook inside `save_output_node` keeps the node count and order unchanged.
- `save_output_node` already has access to the full `PipelineState` — all fields needed for the log are available there.
- Failed runs (before `save_output_node`) are handled by a separate error handler that writes a partial log entry.

**Alternatives Considered**:
- New `log_run_node` as node 8: Violates constitution §Development Workflow (seven-node order is mandated). Requires a constitution amendment.
- Middleware wrapper around the entire graph: More complex; harder to access `PipelineState` fields at the right granularity.

---

## Decision 5: Append-Only Log Store

**Decision**: `PipelineRunLog` entries are append-only. No update or delete operations.

**Rationale**:
- Immutable audit trail is the point. Editing past run logs undermines the observability purpose.
- ChromaDB documents have a stable `id` (the `run_id`) — re-running a pipeline with the same `run_id` would be an error, not an update case.
- Simplifies concurrency: two concurrent runs write two independent entries with different `run_id`s.

**Alternatives Considered**:
- Mutable logs with status updates: Needed if we want to mark a past run as "superseded". Deferred — not required for current scope.

---

## Open Questions / Deferred

| Item | Status | Notes |
|------|--------|-------|
| ChromaDB → PostgreSQL migration path | Deferred | When spec-006 PostgreSQL is live, consider moving log store there for unified querying |
| Multi-user chatbot sessions | Deferred | Currently single-user; session isolation not required for school demo |
| Log retention policy | Deferred | No TTL on ChromaDB documents for now; revisit when collection exceeds 10K entries |
| Anomaly detection integration | Deferred | `src/uc2_observability/anomaly_detection.py` stub exists; wire after US1–US3 complete |
