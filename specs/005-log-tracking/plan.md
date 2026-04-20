# Implementation Plan: Pipeline Run Log Tracking & Observability Chatbot

**Branch**: `005-log-tracking` | **Date**: 2026-04-20 | **Spec**: `specs/005-log-tracking/spec.md`  
**Input**: Feature specification — store all pipeline execution data and expose via RAG chatbot + Streamlit dashboard.

## Summary

Implement the `uc2_observability` layer (stubs already exist in `src/uc2_observability/`). After every pipeline run, persist a structured `PipelineRunLog` to ChromaDB and a JSON sidecar. Tag each run with `run_type: dev|demo|prod` to isolate demo/prod queries from noisy dev runs. Expose a Claude-powered RAG chatbot for natural language queries over run history, and extend the Streamlit UI with an observability dashboard panel.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: chromadb, anthropic (Anthropic SDK), sentence-transformers (existing), streamlit (existing)  
**Storage**: ChromaDB embedded (`.chroma/` directory), JSON sidecar files (`output/logs/`)  
**Testing**: pytest (existing)  
**Target Platform**: Local + GCP VM (same environment)  
**Project Type**: Extension to existing pipeline — no new services  
**Performance Goals**: Log ingestion completes within 5 seconds of `save_output_node`; chatbot response under 10 seconds  
**Constraints**: ChromaDB embedded — single writer at a time (acceptable for current single-pipeline-at-a-time runs); no PostgreSQL dependency (spec-006 not yet implemented)  
**Scale/Scope**: ~100 runs during course; ChromaDB embedded handles up to ~50K documents before hosted upgrade needed

## Constitution Check

- Unified-schema impact: None. Log tracking reads `PipelineState` fields; does not modify `config/unified_schema.json` or downstream required columns.
- Agent responsibilities unchanged: Log ingestion is a post-step hook inside `save_output_node` — seven-node order preserved.
- Transformations: No new YAML mappings. Log store is append-only, never modifies generated mapping files.
- HITL approval points: Unaffected. HITL gate decisions are logged as metadata but not altered.
- Enrichment safety fields: `allergens`, `dietary_tags`, `is_organic` appear in logs as read-only metadata — not re-inferred.
- DQ scoring, generated mapping persistence, and documentation: DQ scores read from existing `PipelineState`; no change to `DQScorePreBlock`/`DQScorePostBlock`; YAML mapping replay unaffected.

## Project Structure

### Documentation (this feature)

```text
specs/005-log-tracking/
├── plan.md              # This file
├── research.md          # Architecture decisions
├── data-model.md        # ChromaDB schema + JSON sidecar format + Python dataclasses
├── quickstart.md        # How to query the chatbot and view the dashboard
└── tasks.md             # Phase-by-phase task breakdown
```

### Source Code

```text
src/
└── uc2_observability/
    ├── __init__.py          # EXISTS — extend exports
    ├── models.py            # NEW: PipelineRunLog, BlockAuditEntry dataclasses
    ├── log_writer.py        # NEW: write_run_log(state, run_type) → ChromaDB + JSON sidecar
    ├── rag_chatbot.py       # EXISTS (stub) → IMPLEMENT: ObservabilityChatbot
    ├── dashboard.py         # EXISTS (stub) → IMPLEMENT: PipelineDashboard
    └── anomaly_detection.py # EXISTS (stub) → DEFERRED

src/agents/
└── orchestrator.py          # MODIFY: call write_run_log() at end of save_output_node

output/
└── logs/                    # NEW directory: JSON sidecar files per run

.chroma/                     # AUTO-CREATED by ChromaDB on first write
```

## Implementation Phases

### Phase 1 — Data Models + Log Writer

**Goal**: `write_run_log()` works end-to-end. ChromaDB document and JSON sidecar written after a real pipeline run.

| Task | File | Notes |
|------|------|-------|
| Create `PipelineRunLog` + `BlockAuditEntry` dataclasses | `src/uc2_observability/models.py` | Per `data-model.md` Python classes |
| Implement `write_run_log(state, run_type)` | `src/uc2_observability/log_writer.py` | Extract fields from `PipelineState`; serialize to ChromaDB doc text; write to collection + JSON sidecar |
| Hook into `save_output_node` | `src/agents/orchestrator.py` | Post-step call after output CSV written; read `PIPELINE_RUN_TYPE` env var (default `dev`) |
| Handle failed runs | `src/agents/orchestrator.py` | Try/except around main node logic; write partial log with `status=failed` in error handler |

**Deliverable**: Run `demo.py` Run 1 — verify `output/logs/{run_id}.json` exists and ChromaDB collection has one entry.

---

### Phase 2 — RAG Chatbot (US1 — P1 MVP)

**Goal**: `ObservabilityChatbot.query("which run quarantined the most rows?")` returns a grounded, cited answer.

| Task | File | Notes |
|------|------|-------|
| Implement `ingest_audit_logs(log_dir)` | `src/uc2_observability/rag_chatbot.py` | Scan `output/logs/`, build ChromaDB documents from JSON sidecars (backfill for existing runs) |
| Implement `get_relevant_context(question, top_k=5)` | `src/uc2_observability/rag_chatbot.py` | ChromaDB `collection.query()` with `run_type` filter; return list of document strings |
| Implement `query(question, include_dev=False)` | `src/uc2_observability/rag_chatbot.py` | Build RAG prompt: context docs + question; call Claude via Anthropic SDK; return answer with run_id citations |
| Add prompt caching to Claude calls | `src/uc2_observability/rag_chatbot.py` | System prompt + context marked as `cache_control: ephemeral` for cache hits on repeated sessions |

**Deliverable**: `python -c "from src.uc2_observability.rag_chatbot import ObservabilityChatbot; c = ObservabilityChatbot(); print(c.query('which run had the highest DQ score?'))"` returns a correct cited answer.

---

### Phase 3 — Streamlit Dashboard (US3 — P3)

**Goal**: Observability tab in existing Streamlit app showing run history, DQ trends, block durations, LLM cost.

| Task | File | Notes |
|------|------|-------|
| Implement `render_run_history()` | `src/uc2_observability/dashboard.py` | DataFrame from `output/logs/*.json`; table with run_id, domain, status, row counts, DQ scores, timestamp |
| Implement `render_dq_distribution()` | `src/uc2_observability/dashboard.py` | Bar/line chart of `dq_score_pre` vs `dq_score_post` per run; filter by run_type |
| Implement `render_block_trace()` | `src/uc2_observability/dashboard.py` | Heatmap: blocks × runs, color = duration_ms; highlights slowest block per run |
| Implement `render_cost_tracking()` | `src/uc2_observability/dashboard.py` | `llm_calls` + estimated token cost per run; cumulative cost chart |
| Wire chatbot into Streamlit UI | `app.py` | Add "Observability" tab; text input for chatbot query; display response with cited run_ids |
| Add run_type filter to dashboard | `src/uc2_observability/dashboard.py` | Streamlit selectbox: All / dev / demo / prod |

**Deliverable**: `streamlit run app.py` → Observability tab renders all panels; chatbot text box returns live answers.

## Complexity Tracking

No constitution violations. All work stays within existing `uc2_observability` module; no new nodes added.
