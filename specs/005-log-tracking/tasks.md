---
description: "Task list for 005-log-tracking"
---

# Tasks: Pipeline Run Log Tracking & Observability Chatbot

**Input**: `specs/005-log-tracking/plan.md`, `data-model.md`, `spec.md`, `research.md`  
**Branch**: `005-log-tracking`  
**Date**: 2026-04-20

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1 = chatbot, US2 = log ingestion, US3 = dashboard

## User Story Map

| Story | Phase | Goal | Priority |
|-------|-------|------|----------|
| US2 | Phase 2 — Log Ingestion | Automatic log write after every run | P2 (blocks US1 + US3) |
| US1 | Phase 3 — RAG Chatbot | Natural language queries over run history | P1 🎯 MVP |
| US3 | Phase 4 — Dashboard | Visual run history + DQ + block + cost panels | P3 |

---

## Phase 1: Setup

**Purpose**: Directory scaffold + dependency declarations

- [ ] T001 Create `output/logs/` directory (add `.gitkeep`)
- [ ] T002 Add `chromadb` and `anthropic` to `pyproject.toml` via `poetry add chromadb anthropic`
- [ ] T003 [P] Add `PIPELINE_RUN_TYPE` (default: `dev`) and `ANTHROPIC_API_KEY` to `.env.example`

---

## Phase 2: Foundational — Data Models

**Purpose**: Dataclasses required by all phases

**⚠️ CRITICAL**: Complete before log writer, chatbot, or dashboard work

- [ ] T004 Create `src/uc2_observability/models.py` with `BlockAuditEntry` and `PipelineRunLog` dataclasses per `data-model.md` Python classes; include `to_dict()` and `to_chroma_document()` methods on `PipelineRunLog`
- [ ] T005 [P] Update `src/uc2_observability/__init__.py` to export `PipelineRunLog`, `BlockAuditEntry`, `write_run_log`, `ObservabilityChatbot`, `PipelineDashboard`

**Checkpoint**: Models importable — all downstream tasks can begin

---

## Phase 3: User Story 2 — Log Ingestion (P2, blocks US1 + US3)

**Goal**: After any pipeline run, `output/logs/{run_id}.json` exists and ChromaDB has a matching document.

**Independent Test**:
```bash
PIPELINE_RUN_TYPE=demo python demo.py
ls output/logs/           # JSON file present
python -c "
import chromadb
c = chromadb.PersistentClient('.chroma')
col = c.get_collection('pipeline_audit')
print(col.count())        # should be 1
"
```

### Implementation for User Story 2

- [ ] T006 [US2] Implement `src/uc2_observability/log_writer.py`: `write_run_log(state: PipelineState, run_type: str)` — extract fields from `state`, build `PipelineRunLog`, write JSON sidecar to `output/logs/{run_id}.json`, write ChromaDB document via `PersistentClient('.chroma')`
- [ ] T007 [US2] Extract `BlockAuditEntry` list from `state["audit_log"]` inside `write_run_log` — map `block_name`, `rows_in`, `rows_out`, `duration_ms`, `extra_meta` fields
- [ ] T008 [US2] Hook `write_run_log()` into `save_output_node` in `src/agents/orchestrator.py`: call after output CSV written; read `PIPELINE_RUN_TYPE` from `os.environ` (default `"dev"`)
- [ ] T009 [US2] Add error-path log write in `src/agents/orchestrator.py`: wrap node logic in try/except; on exception, call `write_run_log()` with `status="failed"` and partial `PipelineState`; re-raise exception after logging
- [ ] T010 [P] [US2] Write `output/logs/` to `.gitignore` (run log data should not be committed)

**Checkpoint**: `demo.py` Run 1 produces `output/logs/{run_id}.json` + ChromaDB entry; Run 2 adds second entry

---

## Phase 4: User Story 1 — RAG Chatbot (P1) 🎯 MVP

**Goal**: `ObservabilityChatbot.query(question)` returns a Claude-generated answer citing specific run_ids.

**Independent Test**:
```bash
python -c "
from src.uc2_observability.rag_chatbot import ObservabilityChatbot
c = ObservabilityChatbot()
print(c.query('which run had the highest dq_score_post?'))
"
# Must print an answer that names a specific run_id and cites the score
```

### Implementation for User Story 1

- [ ] T011 [US1] Implement `ObservabilityChatbot.__init__()` in `src/uc2_observability/rag_chatbot.py`: init `chromadb.PersistentClient('.chroma')`, get or create `pipeline_audit` collection with default embedding function
- [ ] T012 [US1] Implement `ingest_audit_logs(log_dir="output/logs")`: scan directory for `*.json` files, skip run_ids already in collection (check by ID), build and upsert ChromaDB documents from JSON sidecar data
- [ ] T013 [US1] Implement `get_relevant_context(question, top_k=5, include_dev=False)`: build `where` filter based on `include_dev`; call `collection.query(query_texts=[question], where=..., n_results=top_k)`; return list of document strings
- [ ] T014 [US1] Implement `query(question, include_dev=False)`: call `get_relevant_context()`; if empty, return "No pipeline runs found matching your query."; otherwise build RAG prompt with system instructions (answer only from context, cite run_ids); call Claude via `anthropic.Anthropic().messages.create()`; return answer string
- [ ] T015 [US1] Add prompt caching to `query()`: mark system prompt as `cache_control={"type": "ephemeral"}` in Anthropic SDK call to reduce token cost on repeated chatbot sessions

**Checkpoint**: Chatbot correctly answers factual questions about 3+ stored runs; cites run_id; returns "No runs found" when collection is empty

---

## Phase 5: User Story 3 — Streamlit Dashboard (P3)

**Goal**: Observability tab in `app.py` with run history table, DQ chart, block heatmap, cost panel, and chatbot input.

**Independent Test**:
```bash
streamlit run app.py
# Navigate to Observability tab
# Verify: run history table shows all runs, DQ chart renders, chatbot input returns answer
```

### Implementation for User Story 3

- [ ] T016 [US3] Implement `PipelineDashboard.__init__(log_dir="output/logs")` in `src/uc2_observability/dashboard.py`: load all JSON sidecars into a `list[PipelineRunLog]`; cache as instance attribute
- [ ] T017 [US3] Implement `render_run_history()`: `st.dataframe` with columns run_id (truncated), domain, status, row_count_in, row_count_out, row_count_quarantined, dq_score_pre, dq_score_post, duration_seconds, run_type; add run_type selectbox filter
- [ ] T018 [P] [US3] Implement `render_dq_distribution()`: `st.bar_chart` or plotly bar chart with dq_score_pre vs dq_score_post per run; x-axis = run_id (short), grouped bars
- [ ] T019 [P] [US3] Implement `render_block_trace()`: build pivot table (blocks × runs, values = duration_ms); render as `st.dataframe` with color gradient (slow = red)
- [ ] T020 [P] [US3] Implement `render_cost_tracking()`: extract `llm_calls` per run; multiply by estimated cost per call (configurable, default `$0.0002`); show per-run bar chart + cumulative total
- [ ] T021 [US3] Add Observability tab to `app.py`: `st.tab` with dashboard panels (T017–T020) + chatbot text input that calls `ObservabilityChatbot().query()`; display chatbot response in `st.info` box

**Checkpoint**: All dashboard panels render without errors; chatbot in Streamlit UI returns live answers

---

## Phase 6: Polish & Cross-Cutting

- [ ] T022 [P] Add `ingest_audit_logs()` call to chatbot `__init__()` so chatbot auto-backfills from existing JSON sidecars on startup — no manual ingestion step needed
- [ ] T023 [P] Write unit test `tests/test_log_writer.py`: mock `PipelineState`; call `write_run_log()`; assert JSON sidecar written with correct fields; assert ChromaDB document count increments
- [ ] T024 [P] Update `specs/005-log-tracking/quickstart.md` with verified chatbot CLI command and Streamlit screenshot description
- [ ] T025 Validate constitution alignment: confirm seven-node order unchanged, no new nodes added, safety fields read-only in logs

---

## Dependencies & Execution Order

```
Phase 1 (Setup)
    └── Phase 2 (Foundational: models)
            ├── Phase 3 (US2: Log Ingestion) ← BLOCKS US1 + US3
            │       └── Phase 4 (US1: RAG Chatbot) ← MVP
            │       └── Phase 5 (US3: Dashboard)
            └── Phase 6 (Polish) ← after US1 complete
```

US1 and US3 can proceed in parallel after US2 is complete.

---

## Task Count Summary

| Phase | Tasks | Notes |
|-------|-------|-------|
| Setup | T001–T003 | 3 tasks |
| Foundational | T004–T005 | 2 tasks |
| US2 Log Ingestion | T006–T010 | 5 tasks |
| US1 Chatbot | T011–T015 | 5 tasks |
| US3 Dashboard | T016–T021 | 6 tasks |
| Polish | T022–T025 | 4 tasks |
| **Total** | **T001–T025** | **25 tasks** |
