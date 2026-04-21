# Implementation Plan: UC1 → UC2 Observability Integration

**Branch**: `010-uc1-uc2-integration` | **Date**: 2026-04-21 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/010-uc1-uc2-integration/spec.md`

## Summary

Wire UC1's pipeline runner to emit structured Kafka events and Prometheus metrics to the UC2 observability stack running on the shared GCP VM. All emission is fire-and-forget with full try/except guards — the pipeline must not crash if UC2 services are unreachable. Integration adds ~12 call sites across 4 existing files; no new source files created.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: `kafka-python`/`confluent-kafka` and `prometheus_client` imported indirectly via UC2 modules; `uuid`, `hashlib`, `time`, `datetime` (stdlib) added directly  
**External Services**: Kafka `localhost:9092`, Prometheus Pushgateway `localhost:9091`, Postgres `localhost:5432` — all on shared GCP VM `35.239.47.242`  
**Testing**: pytest (existing)  
**Target Platform**: GCP VM for UC2 services; UC1 runs wherever the pipeline runs  
**Project Type**: CLI + Streamlit app (existing); this feature adds observability emission hooks  
**Performance Goals**: Emission overhead < 50ms per block event; Prometheus push < 500ms total per run  
**Constraints**: Pipeline MUST NOT crash or slow meaningfully if UC2 services unreachable; no new `pyproject.toml` dependencies for UC1 itself  
**Scale/Scope**: ~12–20 block events per run per chunk + 2 lifecycle events + N quarantine + M dedup clusters

## Constitution Check

| Principle | Status | Notes |
|---|---|---|
| I. Schema-First Gap Analysis | ✅ No impact | Event emission is a side-effect; schema analysis flow untouched |
| II. Three-Agent Pipeline | ✅ No impact | No new agents; hooks are observability side-effects, not agent logic |
| III. Declarative YAML Execution Only | ✅ No impact | No new YAML mappings; no runtime code generation |
| IV. Human Approval Gates | ✅ No impact | Emission hooks in `run_pipeline_node` / `save_output_node`, both post-gate |
| V. Enrichment Safety Boundaries | ✅ No impact | Quarantine events read existing `quarantine_reasons`; safety field logic untouched |
| VI. Self-Extending Mapping Memory | ✅ No impact | Registry unchanged |
| VII. DQ and Quarantine | ✅ Additive | Events report existing quarantine data; DQ scoring logic unchanged |
| VIII. Production Scale | ✅ Compliant | Events are async/fire-and-forget; no per-record LLM calls added |

**Constitution verdict**: No violations.

## Project Structure

### Documentation (this feature)

```text
specs/010-uc1-uc2-integration/
├── plan.md              # This file
├── research.md          # Phase 0 decisions
├── data-model.md        # Event payloads + modified interfaces
├── contracts/
│   └── uc2-integration-contracts.md
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created by /speckit.plan)
```

### Source Code Changes

```text
src/agents/
└── state.py                     # EDIT — add _run_id: str, _run_start_time: float

src/agents/
└── graph.py                     # EDIT — run_pipeline_node: run_id gen, run_started/completed,
                                 #                            quarantine events, dedup events
                                 #        save_output_node: MetricsCollector.push()

src/pipeline/
└── runner.py                    # EDIT — run(): block_start/block_end events per block

src/blocks/
└── fuzzy_deduplicate.py         # EDIT — add last_clusters, last_dedup_rate attrs; populate in run()

src/models/
└── llm.py                       # EDIT — add _llm_call_counter, reset/get, UC2 import guard,
                                 #        get_observability_llm()

tests/
└── uc2_observability/
    ├── __init__.py              # NEW
    └── test_uc2_integration.py  # NEW
```

**No new source files in `src/`.** All changes are additive edits to existing files.

## Implementation Phases

### Phase A: Import Guard + LLM Counter

Prerequisite for all other phases. Guard ensures the pipeline runs cleanly even before UC2 modules are merged.

**`src/models/llm.py`**:
- Add `_llm_call_counter: int = 0` module-level variable
- Increment by 1 in `call_llm()` before the `litellm.completion()` call
- Add `reset_llm_counter()`, `get_llm_call_count()`, `get_observability_llm()` functions
- Add import guard at module bottom (after all existing code):
  ```python
  try:
      from src.uc2_observability.kafka_to_pg import emit_event as _emit_event
      from src.uc2_observability.metrics_collector import MetricsCollector as _MetricsCollector
      _UC2_AVAILABLE = True
  except ImportError:
      _emit_event = None
      _MetricsCollector = None
      _UC2_AVAILABLE = False
  ```
- Expose `_UC2_AVAILABLE`, `_emit_event`, `_MetricsCollector` so other modules import from here (single import guard location)

**`src/agents/state.py`**:
- Add `_run_id: str` and `_run_start_time: float` to `PipelineState` TypedDict (total=False, consistent with `_schema_fingerprint`)

---

### Phase B: Runner Block Events

**`src/pipeline/runner.py`** — edit `run()`, block loop at lines 83–95:

- Add `import time` and `from datetime import datetime, timezone` at top
- Add `from src.models.llm import _UC2_AVAILABLE, _emit_event` at top
- Before `block.run()`: record `ts_start = time.perf_counter()`, emit `block_start` event
- After `block.run()`: compute `duration_ms`, compute `null_rates` for 4 key columns, emit `block_end` event
- Each emit wrapped in `try/except Exception as e: logger.warning(...)`
- Read `run_id` and `source_name` from `config.get(...)` — passed by `run_pipeline_node`

---

### Phase C: Dedup Block Attributes

**`src/blocks/fuzzy_deduplicate.py`** — `FuzzyDeduplicateBlock`:

- Add `last_clusters: list[dict] = []` and `last_dedup_rate: float = 0.0` class attrs
- At top of `run()`: reset both to defaults
- After cluster assignment (after line 179 `df["canonical"] = canonical`): build `last_clusters` from `cluster_map` + `group_ids` + product_name/brand_name columns; compute `last_dedup_rate = (n - unique_clusters) / n`
- Only include clusters with `len(members) > 1` in `last_clusters`

---

### Phase D: Graph Node Events

**`src/agents/graph.py`** — `run_pipeline_node`:

New imports at top:
```python
import hashlib, time
from uuid import uuid4
from datetime import datetime, timezone
from src.models.llm import _UC2_AVAILABLE, _emit_event, reset_llm_counter, get_llm_call_count
```

At start of function (before existing logic):
```python
run_id = str(uuid4())
_run_start_time = time.perf_counter()
source_name = Path(state.get("source_path", "unknown")).stem
reset_llm_counter()
config["run_id"] = run_id
config["source_name"] = source_name
# emit run_started (try/except)
```

Wrap `runner.run_chunked()` + quarantine logic in try/finally:
- On success: emit quarantine events (one per quarantined row), emit dedup_cluster events (read from `block_registry.get("fuzzy_deduplicate").last_clusters`)
- `finally`: emit `run_completed` with `status="success"` or `"failed"`

Return `_run_id` and `_run_start_time` in the returned dict.

**`src/agents/graph.py`** — `save_output_node`:

After CSV write succeeds, build the 15-key `metrics` dict (see data-model.md) and call:
```python
_MetricsCollector().push(metrics, source=source_name, run_id=state.get("_run_id", "unknown"))
```
Wrapped in `try/except Exception`.

---

### Phase E: Tests

**`tests/uc2_observability/test_uc2_integration.py`** — 8 test cases using `unittest.mock.patch`:

| Test | What it verifies |
|---|---|
| `test_block_events_emitted` | `block_start` + `block_end` called per block in `run()` |
| `test_block_events_suppressed_when_unavailable` | `_UC2_AVAILABLE=False` → emit never called |
| `test_run_lifecycle_success` | `run_started` + `run_completed(status="success")` emitted |
| `test_run_completed_on_exception` | Exception in `run_chunked()` → `run_completed(status="failed")` still fires |
| `test_quarantine_events` | Null required field → `quarantine` event with correct reason |
| `test_dedup_cluster_populated` | `FuzzyDeduplicateBlock.last_clusters` contains clusters with size>1 |
| `test_metrics_push_called` | `save_output_node` calls `MetricsCollector().push()` with all 15 keys |
| `test_llm_counter_lifecycle` | Counter increments per `call_llm()`, resets per `reset_llm_counter()` |

## Complexity Tracking

No constitution violations — this section intentionally empty.

## Pre-Merge Dependency

`src/uc2_observability/metrics_collector.py` and `src/uc2_observability/kafka_to_pg.py` must be pulled from the team's shared branch before UC2 emission activates. The import guard keeps UC1 working without them. Verify: `poetry run python demo.py` before and after branch merge — behavior identical when GCP services unreachable.
