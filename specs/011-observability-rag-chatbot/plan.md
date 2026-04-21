# Implementation Plan: Observability Log Persistence & RAG Chatbot

**Branch**: `011-observability-rag-chatbot` | **Date**: 2026-04-21 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `/specs/011-observability-rag-chatbot/spec.md`

## Summary

Persist structured run logs (JSON, one file per run) after every pipeline execution, then expose a RAG chatbot in the Streamlit wizard for natural-language questions about past runs, and a Grafana dashboard for visual analytics. Log data is sourced directly from `PipelineState` fields already collected during execution. The chatbot uses structured filtering + LLM synthesis over stored JSON logs. The Grafana dashboard reads pipeline metrics pushed to Prometheus Pushgateway after each run.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: `redis-py` (existing cache), `litellm` (existing LLM routing), `pandas` (existing), `streamlit` (existing UI)  
**Storage**: Local JSON files in `output/run_logs/` (gitignored)  
**Testing**: pytest (existing harness)  
**Target Platform**: Local Linux/macOS developer machine  
**Project Type**: CLI + Streamlit web app (existing); this feature adds observability layer  
**Performance Goals**: Log write <2s after run completion; chatbot response <5s for ≤50 runs  
**Constraints**: No new external services; no new API keys; must not break existing pipeline flow  
**Scale/Scope**: ≤500 runs locally; single-user deployment

## Constitution Check

| Principle | Status | Notes |
|---|---|---|
| I. Schema-First Gap Analysis | ✅ No impact | Log persistence reads state after schema analysis; does not modify `config/unified_schema.json` |
| II. Three-Agent Pipeline | ✅ No impact | No new agents added; log writer is a side-effect of `save_output_node`, not a pipeline agent |
| III. Declarative YAML Execution Only | ✅ No impact | No YAML mappings changed; no runtime code generation |
| IV. Human Approval Gates | ✅ No impact | Log write happens after all HITL gates have been exercised |
| V. Enrichment Safety Boundaries | ✅ No impact | Safety fields (`allergens`, `is_organic`, `dietary_tags`) not referenced by log writer or chatbot beyond reading for display |
| VI. Self-Extending Mapping Memory | ✅ No impact | Registry and mapping files untouched |
| VII. DQ and Quarantine | ✅ Additive | Logs capture `dq_score_pre`, `dq_score_post`, `dq_delta`, and `quarantine_reasons` for observability — no changes to scoring or quarantine logic |
| VIII. Production Scale | ✅ No impact | Log write is post-pipeline, single-file, atomic; no per-record LLM calls |

**Constitution verdict**: No violations. Feature is purely additive to the existing pipeline.

## Project Structure

### Documentation (this feature)

```text
specs/011-observability-rag-chatbot/
├── plan.md              # This file
├── research.md          # Phase 0 decisions
├── data-model.md        # Log schema + class interfaces
├── contracts/
│   └── observability-interfaces.md
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created by /speckit.plan)
```

### Source Code Changes

```text
src/uc2_observability/
├── __init__.py                  # UNCHANGED
├── log_writer.py                # NEW — RunLogWriter: PipelineState → JSON file
├── log_store.py                 # NEW — RunLogStore: query persisted logs
├── rag_chatbot.py               # IMPLEMENT (replace placeholder) — ObservabilityChatbot
├── metrics_exporter.py          # NEW — MetricsExporter: push run metrics to Prometheus Pushgateway
├── anomaly_detection.py         # UNCHANGED (still placeholder)
└── dashboard.py                 # UNCHANGED (still placeholder)

src/agents/
└── graph.py                     # EDIT — call RunLogWriter.save() + MetricsExporter.push() at end of save_output_node

src/models/
└── llm.py                       # EDIT — add get_observability_llm() getter

app.py                           # EDIT — add sidebar Mode radio + ObservabilityPage render

grafana/
├── docker-compose.yml           # NEW — Prometheus + Pushgateway + Grafana stack
├── prometheus.yml               # NEW — Prometheus scrape config (scrapes Pushgateway)
├── provisioning/
│   ├── datasources/
│   │   └── prometheus.yml       # NEW — Grafana auto-provision Prometheus datasource
│   └── dashboards/
│       └── dashboards.yml       # NEW — Grafana dashboard provisioning config
└── dashboards/
    └── pipeline-observability.json  # NEW — Grafana dashboard definition

tests/
└── uc2_observability/
    ├── __init__.py              # NEW
    ├── test_log_writer.py       # NEW
    ├── test_log_store.py        # NEW
    ├── test_rag_chatbot.py      # NEW
    └── test_metrics_exporter.py # NEW
```

**Structure Decision**: Single-project layout. New code goes into the existing `src/uc2_observability/` tree. No new packages or top-level directories. Tests mirror source structure under `tests/uc2_observability/`.

## Implementation Phases

### Phase A: Log Persistence (no UI, no chatbot)

Delivers: every pipeline run writes a JSON log. Can be tested independently.

1. **`src/uc2_observability/log_writer.py`** — `RunLogWriter` class
   - `__init__(log_dir)`: default `PROJECT_ROOT / "output" / "run_logs"`
   - `save(state, status, error, start_time)`: extract fields from state, write JSON atomically, never raise
   - Helper `_extract_record(state, status, error, start_time) -> dict`: pure function, testable without disk I/O
   - Import guard for `PipelineState` (TYPE_CHECKING only — no circular import)

2. **`src/agents/graph.py`** — integration in `save_output_node`
   - Record `_pipeline_start_time = time.monotonic()` at start of `load_source_node` (add to returned state dict — new optional field `_run_start_time: float`)
   - At end of `save_output_node` (after CSV written): call `RunLogWriter().save(state, "success", start_time=state.get("_run_start_time"))`
   - Wrap the entire `save_output_node` body in try/except; on exception: call `RunLogWriter().save(state, "partial", error=str(e), start_time=...)`, then re-raise
   - `_run_start_time` is underscore-prefixed — consistent with `_schema_fingerprint` convention already in state

3. **`tests/uc2_observability/test_log_writer.py`**
   - Test `_extract_record()` with fully-populated state
   - Test `_extract_record()` with minimal state (only `source_path`)
   - Test `save()` writes a valid JSON file with correct content
   - Test `save()` returns `None` and logs warning when `log_dir` is not writable

### Phase B: Log Store

Delivers: queryable access to persisted logs. Prerequisite for chatbot.

1. **`src/uc2_observability/log_store.py`** — `RunLogStore` class
   - `load_all()`: glob `*.json` in `log_dir`, parse each, skip corrupt, sort by `timestamp` ASC
   - `get_by_run_id(run_id)`: scan `load_all()`, match on `run_id` field
   - `filter(source_name, status, since, limit)`: `load_all()` + in-memory filter + sort DESC
   - `summary_stats()`: aggregate over `load_all()` results

2. **`tests/uc2_observability/test_log_store.py`**
   - Test `load_all()` with 0 files, 1 file, 3 files
   - Test `load_all()` skips corrupt JSON file
   - Test `filter()` by each dimension
   - Test `summary_stats()` with known fixture data

### Phase C: RAG Chatbot Implementation

Delivers: working `ObservabilityChatbot` with LLM synthesis.

1. **`src/models/llm.py`** — add `get_observability_llm()` getter (returns `"deepseek/deepseek-chat"`)

2. **`src/uc2_observability/rag_chatbot.py`** — replace placeholder
   - `ObservabilityChatbot(log_store: RunLogStore)`
   - `ingest_audit_logs() -> int`: calls `log_store.load_all()`, caches in `self._logs`
   - `get_relevant_context(query, max_runs) -> list[dict]`: structured filter heuristic (see data-model.md)
   - `query(question) -> ChatResponse`: retrieval → LLM synthesis → extract cited run IDs from response
   - Scope guard: if `self._logs` is empty, return "no run data available" without calling LLM
   - Out-of-scope guard: if LLM response contains no run data references, answer stands — do not hallucinate run IDs
   - Cited run IDs extracted via regex `[a-f0-9]{8}-[a-f0-9]{4}-...` pattern from LLM response, filtered to IDs that exist in context

3. **`tests/uc2_observability/test_rag_chatbot.py`**
   - Test `get_relevant_context()` routing for each query type (run_id, source_name, recency, metric)
   - Test `query()` returns "no data" response when log store is empty (mock LLM not called)
   - Test `query()` includes cited_run_ids from fixture logs (mock LLM response)
   - Test `query()` never raises even if LLM call throws

### Phase D: Streamlit Integration

Delivers: chatbot UI accessible from the existing wizard.

1. **`app.py`** — add observability mode
   - In sidebar (before existing cache controls): `mode = st.sidebar.radio("Mode", ["Pipeline", "Observability"], key="app_mode")`
   - Move existing pipeline step rendering inside `if mode == "Pipeline":` block
   - Add `elif mode == "Observability":` block calling `_render_observability_page()`
   - `_render_observability_page()`:
     - Initialize `ObservabilityChatbot(RunLogStore())` in `st.session_state.obs_chatbot` (once per session)
     - "Refresh logs" button: calls `obs_chatbot.ingest_audit_logs()`, updates `st.session_state.obs_last_refresh`
     - Show run count + last refresh timestamp
     - Chat input via `st.chat_input("Ask about pipeline runs…")`
     - On submit: call `obs_chatbot.query(question)`, append to `st.session_state.obs_messages`
     - Render message history with `st.chat_message()`, show cited run IDs as collapsible expander
     - "Clear chat" button resets `obs_messages`

### Phase E: Grafana Dashboard

Delivers: visual Grafana dashboard showing pipeline metrics over time. Requires Phases A (log writer) for data and Prometheus Pushgateway running locally.

1. **`src/uc2_observability/metrics_exporter.py`** — `MetricsExporter` class
   - `__init__(pushgateway_url)`: default `"localhost:9091"`, `job="etl_pipeline"`
   - `push(run_log: dict) -> bool`: push labelled gauges derived from a run log dict; returns `True` on success, `False` on failure; never raises
   - Metrics pushed (all as `Gauge`, labelled with `source_name`, `status`, `run_id`):
     - `etl_dq_score_pre`, `etl_dq_score_post`, `etl_dq_delta`
     - `etl_rows_in`, `etl_rows_out`, `etl_rows_quarantined`
     - `etl_duration_seconds`
     - `etl_enrichment_s1_resolved`, `etl_enrichment_s2_resolved`, `etl_enrichment_s3_resolved`, `etl_enrichment_unresolved`
     - `etl_run_status` (1.0=success, 0.5=partial, 0.0=failed)

2. **`src/agents/graph.py`** — add Pushgateway push after log write
   - After `RunLogWriter().save()` call: if write succeeded (path returned), call `MetricsExporter().push(run_log_dict)`
   - Wrap in try/except — metrics push failure must never affect pipeline return

3. **`grafana/docker-compose.yml`** — local observability stack
   ```yaml
   services:
     prometheus:   image: prom/prometheus, ports: 9090, mounts: prometheus.yml
     pushgateway:  image: prom/pushgateway, ports: 9091
     grafana:      image: grafana/grafana, ports: 3000, mounts: provisioning/ + dashboards/
   ```

4. **`grafana/prometheus.yml`** — scrape config
   - Scrape `pushgateway:9091` every 15s with `honor_labels: true`

5. **`grafana/provisioning/datasources/prometheus.yml`** — auto-provision datasource
   - `url: http://prometheus:9090`, `access: proxy`, `isDefault: true`

6. **`grafana/dashboards/pipeline-observability.json`** — dashboard panels:
   - **DQ Scores Over Time**: time-series, metrics `etl_dq_score_pre`, `etl_dq_score_post`, `etl_dq_delta`, group by `run_id`
   - **Enrichment Tier Distribution**: stacked bar, S1/S2/S3/unresolved counts per run
   - **Run Status**: stat panel, `etl_run_status` last value per run, color-coded (green/yellow/red)
   - **Row Counts**: time-series, `etl_rows_in`, `etl_rows_out`, `etl_rows_quarantined`
   - **Run Duration**: time-series, `etl_duration_seconds`
   - **Source Filter**: dashboard variable `$source_name` applied as label filter on all panels

7. **`tests/uc2_observability/test_metrics_exporter.py`**
   - Test `push()` returns `True` when Pushgateway responds 200 (mock requests)
   - Test `push()` returns `False` (not raises) on connection error
   - Test `push()` sends correct metric names and label values from a fixture run log

## Complexity Tracking

No constitution violations — this section intentionally empty.
