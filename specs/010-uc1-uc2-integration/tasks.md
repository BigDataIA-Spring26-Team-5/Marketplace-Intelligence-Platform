# Tasks: UC1 → UC2 Observability Integration

**Input**: Design documents from `/specs/010-uc1-uc2-integration/`
**Prerequisites**: plan.md ✅ spec.md ✅ research.md ✅ data-model.md ✅ contracts/ ✅

**Organization**: Tasks grouped by user story (US1–US5 per spec.md). Each phase is independently testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no cross-task dependencies)
- **[Story]**: Maps to user story from spec.md (US1–US5)
- No story label = Setup / Foundational / Polish phase

---

## Phase 1: Setup

**Purpose**: Create test package so test tasks can be written without import errors.

- [X] T001 Create `tests/uc2_observability/__init__.py` (empty file, new test package)

**Checkpoint**: Test package exists; pytest can discover tests in `tests/uc2_observability/`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Import guard + LLM counter + state fields. MUST complete before any user story — every phase depends on `_UC2_AVAILABLE` and `run_id` infrastructure.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T00X Add `_llm_call_counter: int = 0` module-level var; add `reset_llm_counter()`, `get_llm_call_count()`, `get_observability_llm()` functions to `src/models/llm.py`
- [X] T00X [P] Add UC2 import guard at bottom of `src/models/llm.py` after all existing code: `try: from src.uc2_observability.kafka_to_pg import emit_event as _emit_event; from src.uc2_observability.metrics_collector import MetricsCollector as _MetricsCollector; _UC2_AVAILABLE = True` / `except ImportError: _emit_event = None; _MetricsCollector = None; _UC2_AVAILABLE = False`; expose all three names at module level
- [X] T00X [P] Add `_run_id: str` and `_run_start_time: float` optional fields to `PipelineState` TypedDict in `src/agents/state.py` (underscore prefix, consistent with `_schema_fingerprint`)

**Checkpoint**: `poetry run python demo.py` completes without error (import guard active, UC2 absent = no crash)

---

## Phase 3: User Story 3 — Run Lifecycle Events (Priority: P1) 🎯 MVP Foundation

**Goal**: Every pipeline run emits `run_started` at entry and `run_completed` (success or failure) at exit; `run_id` UUID generated once and threaded via `config` to all downstream phases.

**Independent Test**: After one UC1 run, query `SELECT event_type, source, run_id FROM audit_events ORDER BY ts DESC LIMIT 5;` — shows `run_started` + `run_completed` pair. Query `SELECT * FROM audit_events WHERE status = 'failed';` after deliberate exception — shows failure row.

- [X] T00X [US3] At start of `run_pipeline_node` in `src/agents/graph.py`: add imports (`uuid4`, `time`, `datetime`, `timezone`, `reset_llm_counter`, `get_llm_call_count`, `_UC2_AVAILABLE`, `_emit_event`); generate `run_id = str(uuid4())`; set `config["run_id"] = run_id` and `config["source_name"] = Path(state.get("source_path", "unknown")).stem`; call `reset_llm_counter()`
- [X] T00X [US3] In `run_pipeline_node` in `src/agents/graph.py`, immediately after T005 block: emit `run_started` event (`event_type`, `run_id`, `source`, `ts=datetime.now(timezone.utc).isoformat()`) inside `try/except Exception as e: logger.warning(...)`; record `_run_start = time.perf_counter()`
- [X] T00X [US3] Wrap the existing `runner.run_chunked()` call and all post-run logic in `run_pipeline_node` in `src/agents/graph.py` with `try/finally`; in `finally` block emit `run_completed` event (`run_id`, `source`, `status="success"` or `"failed"`, `total_rows=len(working_df)`, `ts`) inside `try/except`; determine status from whether an exception was raised
- [X] T00X [US3] Add `_run_id` and `_run_start_time` to the dict returned by `run_pipeline_node` in `src/agents/graph.py` so `save_output_node` can read them from state

**Checkpoint**: US3 fully testable. Kafka + Postgres running → `audit_events` table shows run pairs. Pipeline still runs normally when Kafka unreachable.

---

## Phase 4: User Story 1 — Block-Level Event Emission (Priority: P1)

**Goal**: Every block in Stage A and Stage B emits `block_start` + `block_end` to Kafka so UC2 can trace per-block row counts, duration, and null rates.

**Independent Test**: Run UC1 on openFDA source. Query `SELECT block, rows_in, rows_out, duration_ms FROM block_trace WHERE run_id = '<id>' ORDER BY ts;` — one row per block in execution order.

- [X] T00X [US1] Add imports to `src/pipeline/runner.py`: `import time`, `from datetime import datetime, timezone`, `from src.models.llm import _UC2_AVAILABLE, _emit_event`
- [X] T0XX [US1] Add `NULL_RATE_COLUMNS: list[str] = ["product_name", "brand_name", "ingredients", "primary_category"]` constant at module level in `src/pipeline/runner.py` (configurable per FR-008)
- [X] T0XX [US1] In `run()` block loop in `src/pipeline/runner.py` (around lines 83–95), before `block.run(df)`: record `ts_start = time.perf_counter()`; emit `block_start` event (`event_type`, `run_id=config.get("run_id")`, `source=config.get("source_name")`, `block=block_name`, `rows_in=len(df)`, `ts`) inside `try/except Exception as e: logger.warning(...)`; skip emit if `not _UC2_AVAILABLE`
- [X] T0XX [US1] In `run()` block loop in `src/pipeline/runner.py`, after `block.run(df)` returns: compute `duration_ms = int((time.perf_counter() - ts_start) * 1000)`; compute `null_rates = {col: float(df_out[col].isna().mean()) for col in NULL_RATE_COLUMNS if col in df_out.columns}`; emit `block_end` event (`event_type`, `run_id`, `source`, `block`, `rows_in`, `rows_out=len(df_out)`, `duration_ms`, `null_rates`, `ts`) inside `try/except Exception`; skip if `not _UC2_AVAILABLE`

**Checkpoint**: US1 fully testable independently of other stories. Block trace rows appear in Postgres.

---

## Phase 5: User Story 2 — Run-Level Metrics Push (Priority: P1)

**Goal**: After each Stage A run per source, push all 15 metrics to Prometheus Pushgateway so Grafana panels populate.

**Independent Test**: After one UC1 run, open `http://35.239.47.242:9091` — metric groups visible under job=`uc1_pipeline` for `(source, run_id)`. PromQL `uc1_dq_score_post{source="OFF"}` returns a value.

- [X] T0XX [US2] In `save_output_node` in `src/agents/graph.py`, after CSV write succeeds: build 15-key `metrics` dict from state — `rows_in` from `source_df`, `rows_out` from `working_df`, `dq_score_pre/post/delta` from state, `null_rate` as mean null rate over `NULL_RATE_COLUMNS` in `working_df`, `dedup_rate` from `fuzzy_dedup_block.last_dedup_rate` (or 0.0 if block not run), `s1_count/s2_count/s4_count` from `state.get("last_enrichment_stats", {})`, `s3_count=0`, `cost_usd = get_llm_call_count() * 0.002`, `llm_calls = get_llm_call_count()`, `quarantine_rows = len(state.get("quarantined_df", []))`, `block_duration_seconds = time.perf_counter() - state.get("_run_start_time", 0.0)`
- [X] T0XX [US2] In `save_output_node` in `src/agents/graph.py`, call `_MetricsCollector().push(metrics, source=source_name, run_id=state.get("_run_id", "unknown"))` wrapped in `try/except Exception as e: logger.warning(...)`; skip if `not _UC2_AVAILABLE`

**Checkpoint**: US2 fully testable. Pushgateway UI shows pushed metric group; Grafana panels render after Prometheus scrape (~15s).

---

## Phase 6: User Story 4 — Quarantine Row Emission (Priority: P2)

**Goal**: Each row rejected by quarantine logic emits a `quarantine` event to Kafka with the rejection reason so UC2 can log and surface it.

**Independent Test**: Feed a row with `product_name=None` through the pipeline. Query `SELECT reason, COUNT(*) FROM quarantine_rows GROUP BY reason;` — shows row with reason "Null in required field(s): product_name".

- [X] T0XX [US4] In `run_pipeline_node` in `src/agents/graph.py`, after quarantine loop collects `quarantined_df`: for each quarantined row, compute `row_hash = hashlib.sha256(str(row.to_dict()).encode()).hexdigest()[:16]`; build `row_data` with only `product_name`, `brand_name`, `ingredients` plus the offending field; emit `quarantine` event (`event_type`, `run_id`, `source`, `row_hash`, `row_data`, `reason`) per row inside `try/except`; skip if `not _UC2_AVAILABLE`

**Checkpoint**: US4 fully testable independently. `quarantine_rows` Postgres table populates on bad input.

---

## Phase 7: User Story 5 — Dedup Cluster Emission (Priority: P2)

**Goal**: After Stage B dedup runs, emit one `dedup_cluster` event per multi-member cluster so UC2 can show product merge decisions.

**Independent Test**: Run Stage B dedup on two sources with overlapping products. Query `SELECT cluster_id, members FROM dedup_clusters LIMIT 5;` — shows cluster records with 2+ members.

- [X] T0XX [US5] Add `last_clusters: list[dict] = []` and `last_dedup_rate: float = 0.0` as class-level attributes to `FuzzyDeduplicateBlock` in `src/blocks/fuzzy_deduplicate.py`
- [X] T0XX [US5] At start of `run()` in `FuzzyDeduplicateBlock`: reset `self.last_clusters = []`, `self.last_dedup_rate = 0.0`; after cluster assignment loop completes (after `df["canonical"]` is set): build `self.last_clusters` list — one dict per cluster where `len(members) > 1` with fields `cluster_id`, `member_product_names`, `canonical_product_name`, `canonical_brand_name`, `size`, `dedup_key`; compute `self.last_dedup_rate = (n - unique_clusters) / n` where `n = len(df)` at run entry in `src/blocks/fuzzy_deduplicate.py`
- [X] T0XX [US5] In `run_pipeline_node` in `src/agents/graph.py`, after `runner.run_chunked()` returns: retrieve `fuzzy_dedup_block` from block registry; for each cluster in `fuzzy_dedup_block.last_clusters`, emit `dedup_cluster` event (`event_type`, `run_id`, `cluster_id`, `members`, `canonical`, `merge_decisions`) inside `try/except`; skip if `not _UC2_AVAILABLE` or `last_clusters` empty

**Checkpoint**: US5 fully testable. `dedup_clusters` Postgres table populates after cross-source dedup run.

---

## Phase 8: Tests

**Purpose**: Verify all 5 user stories and the import guard with mocked UC2 services.

- [X] T0XX [P] [US1] Write `test_block_events_emitted` in `tests/uc2_observability/test_uc2_integration.py`: patch `_emit_event` and `_UC2_AVAILABLE=True`; run `PipelineRunner.run()` with 3-block sequence; assert `_emit_event` called exactly 6 times (2 per block)
- [X] T0XX [P] [US1] Write `test_block_events_suppressed_when_unavailable` in `tests/uc2_observability/test_uc2_integration.py`: patch `_UC2_AVAILABLE=False`; run `PipelineRunner.run()`; assert `_emit_event` never called
- [X] T0XX [P] [US3] Write `test_run_lifecycle_success` in `tests/uc2_observability/test_uc2_integration.py`: patch `_emit_event` and `_UC2_AVAILABLE=True`; invoke `run_pipeline_node`; assert first call has `event_type="run_started"`, last call has `event_type="run_completed"` with `status="success"`
- [X] T0XX [P] [US3] Write `test_run_completed_on_exception` in `tests/uc2_observability/test_uc2_integration.py`: patch `run_chunked` to raise `RuntimeError`; assert `run_completed` event still emitted with `status="failed"` (finally block fires)
- [X] T0XX [P] [US4] Write `test_quarantine_events` in `tests/uc2_observability/test_uc2_integration.py`: feed DataFrame with one row where `product_name=None` through `run_pipeline_node`; assert `quarantine` event emitted with `reason` containing `"product_name"` and `row_data` does NOT contain full row
- [X] T0XX [P] [US5] Write `test_dedup_cluster_populated` in `tests/uc2_observability/test_uc2_integration.py`: run `FuzzyDeduplicateBlock.run()` on DataFrame with known duplicate pairs; assert `last_clusters` non-empty and every entry has `size > 1`
- [X] T0XX [P] [US2] Write `test_metrics_push_called` in `tests/uc2_observability/test_uc2_integration.py`: patch `_MetricsCollector` and `_UC2_AVAILABLE=True`; invoke `save_output_node`; assert `push()` called once with all 15 required keys in `metrics` dict
- [X] T0XX [P] Foundational Write `test_llm_counter_lifecycle` in `tests/uc2_observability/test_uc2_integration.py`: call `call_llm()` twice (mock litellm); assert `get_llm_call_count() == 2`; call `reset_llm_counter()`; assert `get_llm_call_count() == 0`

**Checkpoint**: `poetry run pytest tests/uc2_observability/ -v` — all 8 tests pass.

---

## Phase 9: Polish & Cross-Cutting Concerns

- [X] T0XX Update `CLAUDE.md` Active Technologies entry for `010-uc1-uc2-integration`: confirm `uuid`, `hashlib`, `time`, `datetime` stdlib imports are listed; note `NULL_RATE_COLUMNS` constant in `runner.py`
- [X] T0XX [P] Smoke test: run `poetry run python demo.py` with UC2 services unreachable — confirm no crash, no behaviour change (import guard verified end-to-end)
- [X] T0XX [P] Run quickstart.md verification commands against live GCP VM (`35.239.47.242`) to confirm Postgres `block_trace`, `audit_events`, `quarantine_rows`, `dedup_clusters` tables populate after one UC1 run

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US3)**: Depends on Phase 2 — generates `run_id` and `config` keys used by US1
- **Phase 4 (US1)**: Depends on Phase 3 — reads `config["run_id"]` set by US3 work
- **Phase 5 (US2)**: Depends on Phase 2 + Phase 3 — reads `_run_id` from state
- **Phase 6 (US4)**: Depends on Phase 3 — reads `run_id` in quarantine loop
- **Phase 7 (US5)**: Depends on Phase 2 only — `FuzzyDeduplicateBlock` changes are independent; `graph.py` emission (T018) depends on Phase 3 for `run_id`
- **Phase 8 (Tests)**: Depends on all implementation phases complete
- **Phase 9 (Polish)**: Depends on Phase 8

### User Story Dependencies

- **US3 (P1)**: Must precede US1 — generates `run_id` threaded via config
- **US1 (P1)**: Depends on US3 for `config["run_id"]`; otherwise independent
- **US2 (P1)**: Depends on Foundational + US3 (state fields); independent of US1
- **US4 (P2)**: Depends on US3 for `run_id`; can be done alongside US1/US2
- **US5 (P2)**: Block attr changes (T016–T017) independent; graph emission (T018) depends on US3

### Parallel Opportunities

Within Phase 2:
- T003 (import guard) and T004 (state.py) can run in parallel (different files)

Within Phase 8 (Tests):
- T019–T026 all write to the same test file — write sequentially (same file)

---

## Parallel Example: Phase 2

```text
# Run in parallel (different files):
T003 — src/models/llm.py import guard
T004 — src/agents/state.py state fields
```

## Parallel Example: Phase 7 (US5)

```text
# Run in parallel (different files):
T016 + T017 — src/blocks/fuzzy_deduplicate.py attrs
T018 — src/agents/graph.py emission (after T016-T017 complete)
```

---

## Implementation Strategy

### MVP First (P1 Stories Only)

1. Phase 1: Setup
2. Phase 2: Foundational (CRITICAL — blocks all)
3. Phase 3: US3 (run lifecycle + run_id) — **stop and smoke test**
4. Phase 4: US1 (block events)
5. Phase 5: US2 (Prometheus push)
6. **STOP and VALIDATE**: Check Pushgateway, Grafana, Postgres `block_trace` + `audit_events`
7. Demo-ready after this point

### Full Delivery (All Stories)

After MVP validated:
1. Phase 6: US4 (quarantine events)
2. Phase 7: US5 (dedup clusters)
3. Phase 8: Tests
4. Phase 9: Polish

### Pre-Merge Gate

Pull team's shared branch containing `src/uc2_observability/metrics_collector.py` and `kafka_to_pg.py` before expecting emission to activate. Import guard keeps UC1 functional without them — safe to implement and merge UC1 side first.

---

## Notes

- [P] = different files, safe to parallelize
- US3 MUST precede US1 — `run_id` flows from graph.py → config → runner.py
- All emit calls fire-and-forget (spawned thread or non-blocking) per FR-006 / Clarification Q1
- `MetricsCollector().push()` called once per source after Stage A per FR-002 / Clarification Q2
- `NULL_RATE_COLUMNS` constant in `runner.py` is the single source of truth for null_rate columns per FR-008 / Clarification Q3
- `row_data` in quarantine events = key fields only (NOT full row) per FR-004 / Clarification Q4
- Stage B blocks emit block_start/block_end AND dedup_cluster events per FR-001 / Clarification Q5
