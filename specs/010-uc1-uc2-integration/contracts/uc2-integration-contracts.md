# Interface Contracts: UC1 → UC2 Observability Integration

**Feature**: 010-uc1-uc2-integration  
**Date**: 2026-04-21

These define the call-site contracts for UC1 code calling UC2 modules. UC2 module internals are out of scope — they're already written.

---

## Contract 1: emit_event()

**Caller**: UC1 (`runner.py`, `graph.py`)  
**Callee**: `src/uc2_observability/kafka_to_pg.emit_event` (UC2, read-only)

```python
def emit_event(event: dict) -> None:
    """Publish event JSON to Kafka pipeline.events topic. Fire-and-forget."""
```

**UC1 obligations**:
- Every call MUST be wrapped in `try/except Exception`: exception logged as `logger.warning`, never re-raised
- `event["ts"]` MUST be `datetime.utcnow().isoformat() + "Z"` (ISO 8601 UTC)
- `event["run_id"]` MUST be the UUID4 string generated at start of `run_pipeline_node`
- `event["source"]` MUST be `Path(state["source_path"]).stem`
- All event dict values MUST be JSON-serializable (no `pd.NA`, `np.nan`, `pd.Timestamp` — convert to `None`, `None`, `str` respectively)

**UC1 guarantees**:
- If `_UC2_AVAILABLE is False` (import guard failed), `emit_event` is never called
- `emit_event` is never called from within a block's `run()` method — only from `runner.py`'s loop and `graph.py` nodes
- Calling `emit_event` does NOT modify any DataFrames or pipeline state

---

## Contract 2: MetricsCollector().push()

**Caller**: `save_output_node` in `graph.py`  
**Callee**: `src/uc2_observability.metrics_collector.MetricsCollector` (UC2, read-only)

```python
class MetricsCollector:
    def push(self, metrics: dict, source: str, run_id: str) -> None:
        """Push metrics dict to Prometheus Pushgateway. Fire-and-forget."""
```

**UC1 obligations**:
- Call MUST be wrapped in `try/except Exception`: failure logged as warning, never re-raised
- `metrics` dict MUST contain all 15 keys defined in the spec (use 0/0.0 defaults for missing values — never pass `None` or `NaN` for numeric fields)
- `source` = `Path(state["source_path"]).stem`
- `run_id` = `state.get("_run_id")` — must match the run_id used for Kafka events

**UC1 guarantees**:
- `push()` is called ONCE per pipeline run, from `save_output_node`, after CSV write
- `push()` is called regardless of enrichment/quarantine outcomes — always fires for completed runs
- If `_UC2_AVAILABLE is False`, `push()` is never called

---

## Contract 3: PipelineRunner.run() — new signature (no breaking change)

**Callers**: `run_pipeline_node` (via `run_chunked()`), tests  
**Current signature** (unchanged):

```python
def run(
    self,
    df: pd.DataFrame,
    block_sequence: list[str],
    column_mapping: dict[str, str] | None = None,
    config: dict | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
```

**New behavior** (additive only):
- If `config["run_id"]` present and `_UC2_AVAILABLE`: emit `block_start` before `block.run()`, `block_end` after
- Emission is best-effort: `try/except` wraps each emit call independently
- Returned `(df, audit_log)` tuple is unchanged — callers unaffected

**`config` keys read by the runner for emission**:

| Key | Type | Used for |
|---|---|---|
| `run_id` | str | included in every event |
| `source_name` | str | included in every event (`source` field) |

`source_name` is NEW — added to config by `run_pipeline_node` when building `config` dict.

---

## Contract 4: FuzzyDeduplicateBlock — new attributes

**Consumers**: `run_pipeline_node` in `graph.py` (reads after `run_chunked()` returns)

```python
class FuzzyDeduplicateBlock(Block):
    last_clusters: list[dict]   # populated after run(); empty list if no clusters
    last_dedup_rate: float      # 0.0–1.0; 0.0 if no dedup occurred
```

**Invariants**:
- `last_clusters` and `last_dedup_rate` are only valid after at least one call to `run()`
- `last_clusters` contains only clusters with `size > 1`
- `last_dedup_rate` = `(n - unique_clusters) / n` where `n = len(df)` at entry to `run()`
- Both attributes are reset at the start of each `run()` call (`self.last_clusters = []`, `self.last_dedup_rate = 0.0`)

---

## Contract 5: llm.py counter interface

**Consumers**: `run_pipeline_node` (reset), `save_output_node` (read)

```python
def reset_llm_counter() -> None:
    """Reset _llm_call_counter to 0. Call at start of each pipeline run."""

def get_llm_call_count() -> int:
    """Return current value of _llm_call_counter."""
```

**Invariants**:
- Counter is module-global — not thread-safe. Acceptable since single-run, single-thread execution model.
- `call_llm()` increments counter by 1 for every API call (including retries if any are added later).
- Counter is never decremented.

---

## Non-Goals (out of scope for this contract)

- UC2 Kafka consumer correctness — not UC1's responsibility
- Postgres schema for `block_trace`, `audit_events`, `quarantine_rows`, `dedup_clusters` — defined by UC2
- Prometheus metric naming — defined by `MetricsCollector` (UC2)
- MCP server tools (`get_run_metrics`, `get_quarantine`) — defined by UC2
