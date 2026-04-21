# Research: UC1 → UC2 Observability Integration

**Feature**: 010-uc1-uc2-integration  
**Date**: 2026-04-21

## Decision Log

---

### 1. Import Guard for UC2 Modules

**Decision**: Guard all `from src.uc2_observability.kafka_to_pg import emit_event` and `from src.uc2_observability.metrics_collector import MetricsCollector` imports with `try/except ImportError`, setting `_UC2_AVAILABLE = False` when absent.

**Rationale**:
- `metrics_collector.py` and `kafka_to_pg.py` exist on the shared team branch, not yet on `aqeel`. Without the guard, the entire pipeline breaks on import if the branch hasn't been merged.
- The guard preserves the existing pipeline behavior when UC2 is unavailable (local dev without GCP).
- All emission call sites check `_UC2_AVAILABLE` before calling. FR-006 (pipeline must not crash) is satisfied structurally.

**Alternatives considered**:
- Optional package in `pyproject.toml`: doesn't help since these are internal modules, not PyPI packages
- Runtime `getattr` checks: more verbose than a single module-level flag

---

### 2. run_id Generation and Threading

**Decision**: Generate `run_id = str(uuid4())` at the top of `run_pipeline_node` in `graph.py`, add it to the `config` dict passed to `PipelineRunner.run_chunked()`, and store it in state as `_run_id`.

**Rationale**:
- `run_pipeline_node` is the single entry point for pipeline execution in both `demo.py` and `app.py`.
- `run_chunked()` already reads `run_id` from `config` (line 178 in runner.py): `run_id = config.get("run_id", "run")` — the infrastructure is already there.
- Storing in state as `_run_id` (underscore prefix, consistent with `_schema_fingerprint`) allows `save_output_node` to use it for the Prometheus push.

**Alternatives considered**:
- Generate in `load_source_node`: earlier but `run_pipeline_node` is where execution semantics start
- Generate in the top-level `demo.py`/`app.py` callers: leaks observability concerns into entry points

---

### 3. Block Event Emission Location

**Decision**: Emit `block_start` and `block_end` events inside `PipelineRunner.run()` in `runner.py`, wrapping the existing block execution loop (lines 83-95).

**Rationale**:
- `run()` is the single place where every block executes — no emission sites can be missed.
- For chunked runs, events fire per chunk (each chunk is a real execution pass). This is intentional — UC2 gets per-chunk granularity for large files.
- `block_start` fires before `block.run(df, config)`. `block_end` fires after, with `duration_ms` measured via `time.perf_counter()`.
- `null_rates` for `block_end` computed over key unified columns present in the result df: `{col: df[col].isna().mean() for col in KEY_NULL_COLS if col in df.columns}`.

**Alternatives considered**:
- Emit in `run_pipeline_node` using `audit_log` after-the-fact: loses real-time streaming, doesn't give per-block timing to UC2 incrementally
- Instrument each `Block.run()` in `base.py`: would require overriding or wrapping every block

---

### 4. run_started / run_completed Emission

**Decision**: Emit `run_started` at top of `run_pipeline_node`. Emit `run_completed` in the same function's `finally` block. Status = `"success"` if no exception; `"failed"` if exception caught.

**Rationale**:
- `run_pipeline_node` is the logical "run" boundary from UC2's perspective.
- `try/finally` ensures `run_completed` fires even on crash (FR-003 requirement).
- `total_rows` in `run_completed` = `len(clean_df) + len(quarantined_df)` = all rows processed.

**Alternatives considered**:
- Emit `run_completed` in `save_output_node`: misses the case where `run_pipeline_node` succeeds but `save_output_node` fails
- Emit at `graph.invoke()` call site in `demo.py`: duplicates logic across entry points

---

### 5. Quarantine Event Emission

**Decision**: Emit `quarantine` events from `run_pipeline_node` in `graph.py` after the quarantine loop (lines 262-274), batching to avoid one Kafka message per row. Emit one event per row but only when `quarantined_df` is non-empty.

**Rationale**:
- Quarantine logic lives in `run_pipeline_node` — that's where `quarantine_reasons` list is built.
- `row_hash = hashlib.sha256(str(row.to_dict()).encode()).hexdigest()[:16]` gives stable row identity.
- `row_data = row.to_dict()` serialized to JSON-safe form.

**Alternatives considered**:
- Emit in a separate block after quarantine: adds a block to the sequence, which changes graph topology and violates "don't add blocks"
- Batch quarantine rows into one event: loses per-row queryability in Postgres

---

### 6. Dedup Cluster Emission

**Decision**: Add `last_clusters: list[dict]` attribute to `FuzzyDeduplicateBlock`. Populate it at end of `run()` with cluster summaries. `run_pipeline_node` reads `last_clusters` after `run_chunked()` returns and emits `dedup_cluster` events.

**Rationale**:
- Same pattern as `LLMEnrichBlock.last_enrichment_stats` — already established in this codebase.
- Cluster data (members, canonical row, merge decisions) is available inside `FuzzyDeduplicateBlock.run()` where `group_ids`, `canonical`, `cluster_map` are computed.
- Emit from `run_pipeline_node` after run completes (not inside the block) to keep block code focused on transformation.

**Cluster summary format** (stored in `last_clusters`):
```python
[
  {
    "cluster_id": int,
    "member_indices": [int, ...],    # df row indices
    "member_product_names": [str, ...],
    "canonical_index": int,
    "size": int,
  },
  ...
]
```
Only emit clusters with `size > 1` — singletons are not dedup events.

---

### 7. MetricsCollector.push() Call Site

**Decision**: Call `MetricsCollector().push(metrics, source=source_name, run_id=run_id)` from `save_output_node` in `graph.py`, after the CSV write succeeds.

**Rationale**:
- `save_output_node` runs after `run_pipeline_node` — all metrics are available in state by then.
- Pushing after CSV write ensures metrics include the complete final row counts.
- Failure to push is logged as warning and does not prevent CSV from being returned.

---

### 8. Enrichment Tier Metric Mapping (S1/S2/S3/S4)

**Decision**: Map current enrichment_stats keys to spec metric names as follows:

| Spec metric | `enrichment_stats` key | Notes |
|---|---|---|
| `s1_count` | `"deterministic"` | S1 = rule-based extraction |
| `s2_count` | `"embedding"` | S2 = FAISS KNN |
| `s3_count` | `0` | S3 = cluster propagation (Stage B, not applicable Stage A) |
| `s4_count` | `"llm"` | S4 = LLM enrichment |

**Rationale**:
- Current codebase has 3 enrichment tiers (S1/S2/S3 in code, but mapped to S1/S2/S4 in the spec's 4-tier numbering).
- S3 cluster propagation is a cross-source Stage B operation; setting to 0 for Stage A runs is correct and honest.
- If Stage B integration is added later, `s3_count` can be wired from the cluster propagation block.

---

### 9. cost_usd Estimation

**Decision**: Add `_llm_call_counter` module-level counter in `src/models/llm.py`, incremented by `call_llm()`. Read in `save_output_node` for the `cost_usd` metric. Estimate: `cost_usd = call_count * 0.002` (conservative per-call estimate for DeepSeek at ~$0.14/1M tokens, assuming ~15k tokens per call).

**Rationale**:
- Simple, no external tracking needed.
- The counter is reset at pipeline start (by `run_pipeline_node` setting `_run_id`).
- Actual cost accuracy is secondary to having a non-zero estimate for Grafana visualization.

---

### 10. `null_rate` Metric Computation

**Decision**: Compute `null_rate` from `working_df` in `save_output_node` as mean null rate across key unified columns: `["product_name", "brand_name", "ingredients", "primary_category"]`.

**Rationale**:
- These 4 columns are the richest signal for data quality.
- Computing mean across columns and rows gives a single scalar suitable for Prometheus gauge.
- Computed post-enrichment, so it reflects the final output quality.
