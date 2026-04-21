# Tasks: Observability Log Persistence, RAG Chatbot & Grafana Dashboard

**Input**: Design documents from `specs/011-observability-rag-chatbot/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/ ✅

**Organization**: Grouped by user story (US1→US4) in priority order. Each phase independently testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Parallelizable (different files, no incomplete dependencies)
- **[Story]**: User story label (US1–US4)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Test harness structure that all phases depend on.

- [x] T001 Create `tests/uc2_observability/__init__.py` (empty, makes package discoverable by pytest)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: State-level and LLM-routing changes that US1–US4 all depend on. Complete before any user story begins.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T002 Record `_run_start_time = time.monotonic()` in `load_source_node` return dict in `src/agents/graph.py`
- [x] T003 [P] Add `get_observability_llm()` getter returning `"deepseek/deepseek-chat"` via `get_llm()` in `src/models/llm.py`

**Checkpoint**: Foundation ready — T002 and T003 complete, user story phases can begin.

---

## Phase 3: User Story 1 — Pipeline Run Logs Persisted (Priority: P1) 🎯 MVP

**Goal**: Every pipeline run (success or failure) writes an atomic JSON log to `output/run_logs/`.

**Independent Test**: Run `poetry run python demo.py`, then `ls output/run_logs/` — one `run_*.json` file appears per pipeline run with all mandatory fields present (`run_id`, `timestamp`, `source_path`, `status`).

### Implementation for User Story 1

- [x] T004 [P] [US1] Implement `RunLogWriter._extract_record(state, status, error, start_time) -> dict` pure helper (all field extractions with `.get()` defaults) in `src/uc2_observability/log_writer.py`
- [x] T005 [US1] Implement `RunLogWriter.__init__(log_dir)` and `RunLogWriter.save(state, status, error, start_time) -> Path | None` — generates UUID4 `run_id`, ISO timestamp, atomic write via temp+rename, never raises — in `src/uc2_observability/log_writer.py` (depends on T004)
- [x] T006 [US1] Call `RunLogWriter().save(state, "success", start_time=state.get("_run_start_time"))` at end of `save_output_node` success path in `src/agents/graph.py` (depends on T002, T005)
- [x] T007 [US1] Wrap `save_output_node` body in try/except; call `RunLogWriter().save(state, "partial", error=str(e), start_time=state.get("_run_start_time"))` in except clause, then re-raise in `src/agents/graph.py` (depends on T006)
- [x] T008 [P] [US1] Write unit tests: `_extract_record()` with fully-populated state; `_extract_record()` with minimal state (only `source_path`); `save()` writes valid JSON with correct content; `save()` returns None and logs warning when `log_dir` unwritable — in `tests/uc2_observability/test_log_writer.py`

**Checkpoint**: After T005–T008, run `demo.py` and verify log files appear. US1 is fully functional and testable without chatbot or Grafana.

---

## Phase 4: User Story 2 — Chatbot Answers Questions About Pipeline Runs (Priority: P2)

**Goal**: Operator can ask natural-language questions about pipeline history from the Streamlit wizard and receive accurate, grounded multi-turn answers with cited run IDs.

**Independent Test**: With ≥3 logs in `output/run_logs/`, open the Streamlit wizard, switch to "Observability" mode, ask "How many runs used the KNN enrichment tier?" — receive a correct count citing run IDs.

### Implementation for User Story 2

- [x] T009 [P] [US2] Implement `RunLogStore.__init__(log_dir)`, `load_all() -> list[dict]` (glob, parse, skip corrupt, sort timestamp ASC), and `get_by_run_id(run_id) -> dict | None` in `src/uc2_observability/log_store.py`
- [x] T010 [P] [US2] Implement `RunLogStore.filter(source_name, status, since, limit) -> list[dict]` (AND conditions, sort DESC) and `summary_stats() -> dict` in `src/uc2_observability/log_store.py`
- [x] T011 [P] [US2] Write unit tests: `load_all()` with 0/1/3 files; `load_all()` skips corrupt JSON; `filter()` by each dimension; `summary_stats()` with fixture data — in `tests/uc2_observability/test_log_store.py`
- [x] T012 [US2] Implement `ObservabilityChatbot.__init__(log_store)`, `ingest_audit_logs() -> int`, and `get_relevant_context(query, max_runs) -> list[dict]` with 5-branch routing (run_id → source → time-words → metric-keyword → default) in `src/uc2_observability/rag_chatbot.py` (depends on T009)
- [x] T013 [US2] Implement `ChatResponse` dataclass and `ObservabilityChatbot.query(question) -> ChatResponse` — LLM synthesis with system+user prompt, run ID regex extraction filtered to context IDs, empty-store guard (no LLM call), LLM-failure guard (never raises) — in `src/uc2_observability/rag_chatbot.py` (depends on T012, T003)
- [x] T014 [P] [US2] Write unit tests: `get_relevant_context()` routing for each branch; `query()` returns "no data" without calling LLM when store empty; `query()` returns cited run IDs from fixture logs (mock LLM); `query()` never raises on LLM exception — in `tests/uc2_observability/test_rag_chatbot.py`
- [x] T015 [US2] Add sidebar mode radio `st.sidebar.radio("Mode", ["Pipeline", "Observability"])` to `app.py`; move existing pipeline step rendering inside `if mode == "Pipeline":` guard (depends on T012, T013)
- [x] T016 [US2] Implement `_render_observability_page()` in `app.py`: init `ObservabilityChatbot(RunLogStore())` in `st.session_state.obs_chatbot` once; "Refresh logs" button; run count + last refresh timestamp; `st.chat_input`; message history with `st.chat_message()` and cited run IDs as collapsible `st.expander`; "Clear chat" button resetting `obs_messages` (depends on T015)

**Checkpoint**: After T009–T016, launch Streamlit, switch to Observability mode, verify multi-turn chat works with log data. US2 independently testable alongside US1.

---

## Phase 5: User Story 3 — Grafana Dashboard for Pipeline Analytics (Priority: P3)

**Goal**: Visual Grafana dashboard shows DQ score trends, enrichment tier distribution, run status, and row counts — updated automatically after each pipeline run.

**Independent Test**: Start Docker Compose stack (`cd grafana && docker compose up -d`), run `demo.py` once, open Grafana at `http://localhost:3000`, open "Pipeline Observability" dashboard — DQ score panel shows correct values from the run.

### Implementation for User Story 3

- [x] T017 [P] [US3] Implement `MetricsExporter.__init__(pushgateway_url, job)` and `push(run_log: dict) -> bool` — isolated `CollectorRegistry`, `push_to_gateway()` with all 12 gauge metrics labelled by `source_name`/`status`/`run_id`, missing fields default to `0.0`, returns False on network error (never raises) — in `src/uc2_observability/metrics_exporter.py`
- [x] T018 [US3] Call `MetricsExporter().push(run_log_dict)` after successful `RunLogWriter.save()` return in `save_output_node` in `src/agents/graph.py`; wrap in try/except, log warning on failure, never re-raise (depends on T017, T006)
- [x] T019 [P] [US3] Write unit tests: `push()` returns True on mocked 200 response; `push()` returns False (no raise) on ConnectionError; correct metric names and label values from fixture run log — in `tests/uc2_observability/test_metrics_exporter.py`
- [x] T020 [P] [US3] Create `grafana/docker-compose.yml` with services: `prometheus` (prom/prometheus, port 9090, mounts `prometheus.yml`), `pushgateway` (prom/pushgateway, port 9091), `grafana` (grafana/grafana, port 3000, mounts `provisioning/` and `dashboards/`)
- [x] T021 [P] [US3] Create `grafana/prometheus.yml` — scrape config targeting `pushgateway:9091` every 15s with `honor_labels: true`
- [x] T022 [P] [US3] Create `grafana/provisioning/datasources/prometheus.yml` — auto-provision Prometheus datasource at `http://prometheus:9090`, `access: proxy`, `isDefault: true`
- [x] T023 [P] [US3] Create `grafana/provisioning/dashboards/dashboards.yml` — provisioning config pointing to `/var/lib/grafana/dashboards/` folder
- [x] T024 [US3] Create `grafana/dashboards/pipeline-observability.json` — 6-panel dashboard: (1) DQ Scores Over Time (time-series: `etl_dq_score_pre/post/delta`), (2) Enrichment Tier Breakdown (stacked bar: s1/s2/s3/unresolved), (3) Run Status (stat: `etl_run_status` color-coded), (4) Row Counts (time-series: in/out/quarantined), (5) Run Duration (time-series: `etl_duration_seconds`), (6) Source Filter (dashboard variable `$source_name` applied as label filter on all panels) (depends on T020, T021, T022, T023)

**Checkpoint**: After T017–T024, start Docker stack and run pipeline — Grafana dashboard populates. US3 independently testable alongside US1–US2.

---

## Phase 6: User Story 4 — Run Comparison & Trend Queries via Chatbot (Priority: P4)

**Goal**: Chatbot answers comparative/trend questions across multiple runs ("Is DQ improving?", "Compare USDA vs FDA enrichment rates").

**Independent Test**: With ≥5 logs, ask "Show me the trend in dq_delta over the last 5 runs" — chatbot returns an ordered list of values with run IDs cited.

### Implementation for User Story 4

- [x] T025 [US4] Extend `get_relevant_context()` recency branch to support "last N runs" integer extraction; add source-grouped comparison path returning up to `max_runs` entries per source in `src/uc2_observability/rag_chatbot.py` (depends on T012)
- [x] T026 [US4] Update LLM synthesis prompt in `query()` to include trend/comparative framing instructions: "If multiple runs are provided, compare them chronologically and highlight trends" in `src/uc2_observability/rag_chatbot.py` (depends on T025)
- [x] T027 [P] [US4] Add tests for: "last 5 runs" integer extraction returning 5 logs; source-grouped comparison returning logs split by source_name — in `tests/uc2_observability/test_rag_chatbot.py`

**Checkpoint**: After T025–T027, trend and comparison queries work in chatbot. US4 complete.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [x] T028 Update `src/uc2_observability/__init__.py` to export `RunLogWriter`, `RunLogStore`, `ObservabilityChatbot`, `MetricsExporter`
- [x] T029 Update `CLAUDE.md` Active Technologies section: add `prometheus_client` (push mode via Pushgateway), `grafana/docker-compose.yml` Docker stack for UC2 dashboard
- [x] T030 [P] Run `poetry run pytest tests/uc2_observability/ -v` — confirm all tests pass (no skips, no errors)
- [x] T031 [P] Run `poetry run streamlit run app.py` — verify app launches without import errors and sidebar mode radio is present

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — **BLOCKS all user story phases**
- **US1 (Phase 3)**: Depends on Phase 2
- **US2 (Phase 4)**: Depends on Phase 2 + Phase 3 (`RunLogStore` queries logs written by Phase 3)
- **US3 (Phase 5)**: Depends on Phase 2 + Phase 3 (pushes metrics from same `save_output_node` callsite)
- **US4 (Phase 6)**: Depends on Phase 4 (extends `get_relevant_context()` from Phase 4)
- **Polish (Phase 7)**: Depends on all prior phases

### User Story Dependencies

| Story | Depends on | Can parallelize with |
|---|---|---|
| US1 (P1) | Phase 2 | — |
| US2 (P2) | Phase 2 + US1 | US3 |
| US3 (P3) | Phase 2 + US1 | US2 |
| US4 (P4) | US2 | — |

### Within-Phase Parallel Opportunities

- T004 and T008 (US1): both target different functions in same file but T008 depends on T004 — write T004 first, test T008 in parallel after
- T009, T010, T011 (US2 log store): T009+T010 can be done in parallel; T011 can be written alongside
- T017, T019, T020, T021, T022, T023 (US3): all fully parallel — different files

---

## Parallel Example: User Story 3

```
# All these tasks touch different files — run in parallel:
T017: src/uc2_observability/metrics_exporter.py
T020: grafana/docker-compose.yml
T021: grafana/prometheus.yml
T022: grafana/provisioning/datasources/prometheus.yml
T023: grafana/provisioning/dashboards/dashboards.yml
T019: tests/uc2_observability/test_metrics_exporter.py

# Then T018 and T024 after the above complete:
T018: src/agents/graph.py (integrate MetricsExporter)
T024: grafana/dashboards/pipeline-observability.json (Grafana JSON)
```

---

## Implementation Strategy

### MVP (User Story 1 Only — Phases 1–3)

1. Complete Phase 1: Setup (T001)
2. Complete Phase 2: Foundational (T002–T003)
3. Complete Phase 3: US1 (T004–T008)
4. **STOP and VALIDATE**: `poetry run python demo.py` → verify `output/run_logs/*.json` written
5. Demo: operators can inspect log files after each run

### Incremental Delivery

1. Phases 1–3 → US1 complete → log files persisted ✅
2. Phase 4 → US2 complete → chatbot in Streamlit ✅
3. Phase 5 → US3 complete → Grafana dashboard ✅
4. Phase 6 → US4 complete → trend queries in chatbot ✅
5. Phase 7 → Polish ✅

---

## Notes

- `[P]` = different files, no dependency on incomplete sibling tasks in same phase
- Each story phase has a **Checkpoint** — validate before proceeding to next priority
- Tests are included as implementation validation (not TDD-first; write alongside implementation)
- `output/run_logs/` is gitignored — verify this in `.gitignore` before first run
- Prometheus Pushgateway failure must never break pipeline — always wrap in try/except
