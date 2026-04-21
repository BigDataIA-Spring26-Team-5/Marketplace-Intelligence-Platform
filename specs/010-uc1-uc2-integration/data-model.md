# Data Model: UC1 → UC2 Observability Integration

**Feature**: 010-uc1-uc2-integration  
**Date**: 2026-04-21

---

## State Additions to PipelineState

Two new optional fields added to `src/agents/state.py`:

| Field | Type | Set by | Consumed by |
|---|---|---|---|
| `_run_id` | `str` (UUID4) | `run_pipeline_node` | `save_output_node`, `run()` via config |
| `_run_start_time` | `float` (perf_counter) | `run_pipeline_node` | `save_output_node` |

Both use underscore prefix (consistent with `_schema_fingerprint`).

---

## Kafka Event Payloads

All events serialized as JSON. Published to topic `pipeline.events`.

### run_started

```json
{
  "event_type": "run_started",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "source": "usda_fooddata_sample",
  "ts": "2026-04-21T14:30:12.456789Z"
}
```

### run_completed

```json
{
  "event_type": "run_completed",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "source": "usda_fooddata_sample",
  "status": "success",
  "total_rows": 1000,
  "ts": "2026-04-21T14:31:02.123456Z"
}
```

### block_start

```json
{
  "event_type": "block_start",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "source": "usda_fooddata_sample",
  "block": "normalize_text",
  "rows_in": 1000,
  "ts": "2026-04-21T14:30:15.000000Z"
}
```

### block_end

```json
{
  "event_type": "block_end",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "source": "usda_fooddata_sample",
  "block": "normalize_text",
  "rows_in": 1000,
  "rows_out": 1000,
  "duration_ms": 245,
  "null_rates": {
    "product_name": 0.0,
    "brand_name": 0.02,
    "ingredients": 0.15,
    "primary_category": 0.45
  },
  "ts": "2026-04-21T14:30:15.245000Z"
}
```

### quarantine

```json
{
  "event_type": "quarantine",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "source": "usda_fooddata_sample",
  "row_hash": "d4e5f67890ab1234",
  "row_data": {
    "product_name": null,
    "brand_name": "General Mills",
    "ingredients": "Whole grain oats"
  },
  "reason": "Null in required field(s): product_name",
  "ts": "2026-04-21T14:31:00.000000Z"
}
```

`row_hash` = SHA-256-16 of `str(row.to_dict())`. `row_data` is JSON-safe (NaN → null, Timestamps → ISO strings).

### dedup_cluster

```json
{
  "event_type": "dedup_cluster",
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "cluster_id": "42",
  "members": ["cheerios original 12oz", "cheerios 12 oz original", "cheerios"],
  "canonical": {"product_name": "cheerios original 12oz", "brand_name": "General Mills"},
  "merge_decisions": {"size": 3, "dedup_key": "che"},
  "ts": "2026-04-21T14:30:30.000000Z"
}
```

Only emitted for clusters with `size > 1`.

---

## Prometheus Metrics Dict

Passed to `MetricsCollector().push(metrics, source=source_name, run_id=run_id)`:

```python
{
    "rows_in":               int,    # rows before pipeline (from source_df)
    "rows_out":              int,    # rows in working_df (clean output)
    "dq_score_pre":          float,  # state["dq_score_pre"]
    "dq_score_post":         float,  # state["dq_score_post"]
    "dq_delta":              float,  # dq_score_post - dq_score_pre
    "null_rate":             float,  # mean null rate over 4 key cols in working_df
    "dedup_rate":            float,  # FuzzyDeduplicateBlock.last_dedup_rate (new attr)
    "s1_count":              int,    # enrichment_stats.get("deterministic", 0)
    "s2_count":              int,    # enrichment_stats.get("embedding", 0)
    "s3_count":              0,      # cluster propagation (Stage B, N/A for Stage A)
    "s4_count":              int,    # enrichment_stats.get("llm", 0)
    "cost_usd":              float,  # llm._llm_call_counter * 0.002
    "llm_calls":             int,    # llm._llm_call_counter
    "quarantine_rows":       int,    # len(quarantined_df)
    "block_duration_seconds": float, # time.perf_counter() delta from run start to run end
}
```

---

## New Block Attributes

### FuzzyDeduplicateBlock

```python
class FuzzyDeduplicateBlock(Block):
    name = "fuzzy_deduplicate"
    ...
    last_clusters: list[dict]  # NEW — populated after each run()
    last_dedup_rate: float     # NEW — fraction of rows removed (0.0–1.0)
```

`last_clusters` schema (one dict per multi-member cluster):
```python
{
    "cluster_id": int,
    "member_product_names": list[str],
    "canonical_product_name": str,
    "canonical_brand_name": str,
    "size": int,
    "dedup_key": str,   # 3-char blocking prefix
}
```

---

## LLM Call Counter

New module-level state in `src/models/llm.py`:

```python
_llm_call_counter: int = 0  # incremented by call_llm() each invocation

def reset_llm_counter() -> None:
    global _llm_call_counter
    _llm_call_counter = 0

def get_llm_call_count() -> int:
    return _llm_call_counter
```

`reset_llm_counter()` called at start of `run_pipeline_node` before generating `run_id`.

---

## Files Modified

| File | Change |
|---|---|
| `src/agents/state.py` | Add `_run_id: str` and `_run_start_time: float` optional fields |
| `src/agents/graph.py` | `run_pipeline_node`: generate run_id, emit run_started/run_completed, emit quarantine events; `save_output_node`: call MetricsCollector.push() |
| `src/pipeline/runner.py` | `run()`: emit block_start/block_end per block |
| `src/blocks/fuzzy_deduplicate.py` | Add `last_clusters`, `last_dedup_rate` attributes; populate in `run()` |
| `src/models/llm.py` | Add `_llm_call_counter`, `reset_llm_counter()`, `get_llm_call_count()`, `get_observability_llm()` |

## Files Created

| File | Purpose |
|---|---|
| `tests/uc2_observability/__init__.py` | Test package init |
| `tests/uc2_observability/test_uc2_integration.py` | Integration tests for all emission points |
