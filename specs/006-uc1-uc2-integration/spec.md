# Feature Specification: UC1 → UC2 Observability Integration

**Feature Branch**: `006-uc1-uc2-integration`  
**Created**: 2026-04-21  
**Status**: Draft  
**Owner**: Aqeel  
**Input**: Wire UC1 pipeline runner to emit events and metrics consumed by UC2 observability layer.

---

## Context

UC2 infrastructure is already running on the shared GCP VM (`35.239.47.242`):

| Service | Host | Port |
|---------|------|------|
| Prometheus Pushgateway | localhost | 9091 |
| Postgres (uc2 DB) | localhost | 5432 |
| Kafka (`pipeline.events` topic) | localhost | 9092 |
| ChromaDB | localhost | 8000 |
| MCP Server | localhost | 8001 |

Two UC2 modules are already written and importable from the shared codebase:

```python
from src.uc2_observability.metrics_collector import MetricsCollector
from src.uc2_observability.kafka_to_pg import emit_event
```

UC1 must call these at the right points in its pipeline runner. Nothing else needs to change.

---

## User Scenarios & Testing

### User Story 1 — Block-level event emission (Priority: P1)

Aqeel wants every preprocessing block in UC1 to emit a start/end event so UC2 can trace exactly which block ran, how many rows passed through, and how long it took.

**Why this priority**: Without block traces, the Grafana dashboard has no data and the chatbot cannot answer "which block caused the null spike."

**Independent Test**: Run UC1 on a single source (e.g., openFDA). Query Postgres: `SELECT * FROM block_trace LIMIT 10;` — should show one row per block with rows_in, rows_out, duration_ms.

**Acceptance Scenarios**:

1. **Given** UC1 starts a pipeline run, **When** a block begins executing, **Then** an event `{event_type: "block_start", run_id, source, block, rows_in, ts}` is published to Kafka `pipeline.events` topic.
2. **Given** a block finishes, **When** execution completes, **Then** an event `{event_type: "block_end", run_id, source, block, rows_in, rows_out, duration_ms, null_rates}` is published to Kafka.
3. **Given** events published to Kafka, **When** the `kafka_to_pg` consumer is running, **Then** rows appear in `block_trace` table in Postgres within 5 seconds.

---

### User Story 2 — Run-level metrics push to Prometheus (Priority: P1)

After each UC1 run completes, push a summary of 15+ metrics to Prometheus Pushgateway so Grafana panels populate with real data.

**Why this priority**: Grafana dashboard is entirely driven by Prometheus metrics. Without this push, all panels show "No data."

**Independent Test**: After a UC1 run, open `http://35.239.47.242:9091` — should show metric groups pushed for that `(source, run_id)`. Open Grafana at `http://35.239.47.242:3000` — DQ score, null rate, enrichment tier panels should render.

**Acceptance Scenarios**:

1. **Given** UC1 run completes for a source, **When** `MetricsCollector().push()` is called, **Then** metrics appear in Prometheus Pushgateway under job=`uc1_pipeline`, grouped by `(source, run_id)`.
2. **Given** metrics pushed, **When** Prometheus scrapes Pushgateway (every 15s), **Then** all 15 metrics are queryable via PromQL: `uc1_dq_score_post{source="OFF"}`.
3. **Given** Prometheus has data, **When** Grafana dashboard auto-refreshes (15s), **Then** all 8 panels render with real values.

---

### User Story 3 — Run lifecycle events (Priority: P1)

Emit `run_started` and `run_completed` events so UC2 can track full run history and trigger anomaly detection after each run.

**Why this priority**: The anomaly detector fires on `run_completed`. The chatbot needs run history to answer timeline questions.

**Independent Test**: After a UC1 run, query `SELECT event_type, source, run_id FROM audit_events ORDER BY ts DESC LIMIT 5;` — should show `run_started` and `run_completed` pairs.

**Acceptance Scenarios**:

1. **Given** UC1 begins a pipeline run, **When** execution starts, **Then** `{event_type: "run_started", run_id, source, ts}` is emitted to Kafka.
2. **Given** UC1 finishes a pipeline run, **When** execution ends, **Then** `{event_type: "run_completed", run_id, source, status: "success"|"failed", total_rows, ts}` is emitted to Kafka.
3. **Given** `run_completed` lands in Postgres, **When** anomaly detector runs, **Then** it queries Prometheus for last N runs and scores them with Isolation Forest.

---

### User Story 4 — Quarantine row emission (Priority: P2)

When HITL 3 rejects rows (schema contract failures), emit each quarantined row to Kafka so UC2 can log it and the chatbot can explain "why were 47 rows quarantined."

**Why this priority**: Required for the demo narrative — "47 rows quarantined because brand_name was literal NULL string."

**Independent Test**: Intentionally feed a bad row through HITL 3. Query `SELECT reason, COUNT(*) FROM quarantine_rows GROUP BY reason;` — should show rows with reasons.

**Acceptance Scenarios**:

1. **Given** HITL 3 rejects a row, **When** quarantine happens, **Then** `{event_type: "quarantine", run_id, source, row_hash, row_data, reason}` is emitted to Kafka.
2. **Given** quarantine event in Kafka, **When** `kafka_to_pg` consumer processes it, **Then** row appears in `quarantine_rows` table.
3. **Given** quarantine rows in Postgres, **When** chatbot is asked "how many rows were quarantined?", **Then** MCP tool `get_quarantine(run_id)` returns the correct count and reasons.

---

### User Story 5 — Dedup cluster emission (Priority: P2)

Emit deduplication cluster decisions so UC2 can show which products were merged and from which sources.

**Why this priority**: Key for the demo — "Cheerios collapsed to ONE row with sources=[OFF, USDA]."

**Independent Test**: After Stage B dedup runs, query `SELECT cluster_id, members FROM dedup_clusters LIMIT 5;` — should show clusters with member product IDs.

**Acceptance Scenarios**:

1. **Given** fuzzy_dedup creates a cluster, **When** cluster is finalized, **Then** `{event_type: "dedup_cluster", run_id, cluster_id, members: [...], canonical: {...}, merge_decisions: {...}}` is emitted.
2. **Given** dedup event in Kafka, **Then** row appears in `dedup_clusters` table in Postgres.

---

### Edge Cases

- What if Kafka is down when UC1 tries to emit? → `emit_event` must fail silently (log warning, do NOT crash the pipeline).
- What if Pushgateway is unreachable? → `MetricsCollector.push()` must catch exceptions and log, not raise.
- What if `run_id` is not provided? → Auto-generate as `f"{source}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"`.
- What if UC1 crashes mid-run? → Emit `{event_type: "run_completed", status: "failed"}` in a try/finally block.

---

## Requirements

### Functional Requirements

- **FR-001**: UC1 pipeline runner MUST call `emit_event()` at block_start and block_end for every preprocessing block.
- **FR-002**: UC1 pipeline runner MUST call `MetricsCollector().push()` once after every run completes (both Stage A and Stage B).
- **FR-003**: UC1 MUST emit `run_started` at the top of the run and `run_completed` in a `finally` block so it fires even on failure.
- **FR-004**: HITL 3 quarantine logic MUST call `emit_event()` for each rejected row with a `reason` string.
- **FR-005**: fuzzy_dedup / collapse_cluster MUST emit `dedup_cluster` events for every cluster produced.
- **FR-006**: All `emit_event()` calls MUST be wrapped in try/except — pipeline MUST NOT crash due to UC2 emission failures.
- **FR-007**: `run_id` MUST be consistent across all events for the same run (pass it as a parameter from the top-level runner).

### Metrics dict passed to MetricsCollector.push()

```python
metrics = {
    "rows_in":          int,   # rows entering Stage A for this source
    "rows_out":         int,   # rows exiting preprocessing
    "dq_score_pre":     float, # mean DQ score before enrichment (0-1)
    "dq_score_post":    float, # mean DQ score after enrichment (0-1)
    "dq_delta":         float, # dq_score_post - dq_score_pre
    "null_rate":        float, # mean null rate across key fields
    "dedup_rate":       float, # fraction of rows removed by dedup
    "s1_count":         int,   # rows resolved by S1 deterministic rules
    "s2_count":         int,   # rows resolved by S2 FAISS KNN
    "s3_count":         int,   # rows resolved by S3 cluster propagation
    "s4_count":         int,   # rows resolved by S4 LLM
    "cost_usd":         float, # estimated LLM cost for this run
    "llm_calls":        int,   # total LLM API calls made
    "quarantine_rows":  int,   # rows rejected by HITL 3
    "block_duration_seconds": float,  # total preprocessing wall time
}
```

### Event schemas for emit_event()

```python
# Run lifecycle
{"event_type": "run_started",   "run_id": str, "source": str, "ts": iso_str}
{"event_type": "run_completed", "run_id": str, "source": str,
 "status": "success"|"failed",  "total_rows": int, "ts": iso_str}

# Block trace
{"event_type": "block_start", "run_id": str, "source": str,
 "block": str, "rows_in": int, "ts": iso_str}
{"event_type": "block_end",   "run_id": str, "source": str,
 "block": str, "rows_in": int, "rows_out": int,
 "duration_ms": int, "null_rates": dict, "ts": iso_str}

# Quarantine
{"event_type": "quarantine", "run_id": str, "source": str,
 "row_hash": str, "row_data": dict, "reason": str, "ts": iso_str}

# Dedup cluster
{"event_type": "dedup_cluster", "run_id": str,
 "cluster_id": str, "members": list, "canonical": dict,
 "merge_decisions": dict, "ts": iso_str}
```

### Where to add the hooks in UC1

```
src/pipeline/runner.py          ← emit block_start/block_end, run_started/run_completed
src/blocks/llm_enrich.py        ← count s1/s2/s3/s4, compute cost_usd
src/blocks/fuzzy_deduplicate.py ← emit dedup_cluster events
src/blocks/dq_score.py          ← capture dq_score_pre / dq_score_post
src/ui/hitl3.py                 ← emit quarantine events on row rejection
```

---

## Success Criteria

- **SC-001**: After any UC1 run, `SELECT COUNT(*) FROM block_trace WHERE run_id = '<id>';` returns ≥ 1 row per block executed.
- **SC-002**: After any UC1 run, Prometheus Pushgateway at `http://35.239.47.242:9091` shows metrics for that `(source, run_id)`.
- **SC-003**: Grafana dashboard at `http://35.239.47.242:3000/d/uc1-pipeline` renders all 8 panels with real data after one UC1 run.
- **SC-004**: Chatbot correctly answers "How many rows were processed in the last run?" using `get_run_metrics` MCP tool.
- **SC-005**: A deliberate bad row through HITL 3 appears in `quarantine_rows` table with a non-null reason.
- **SC-006**: UC1 pipeline does NOT crash if Kafka or Pushgateway is unreachable.

---

## Assumptions

- UC2 services (Postgres, Kafka, Pushgateway, ChromaDB) are already running on `localhost` of the shared GCP VM.
- The `kafka_to_pg` consumer is already started separately (`nohup python3 -m src.uc2_observability.kafka_to_pg &`).
- `run_id` is generated once at the start of each UC1 pipeline run and passed down to all blocks.
- UC1's pipeline runner is in `src/pipeline/runner.py` and calls each block's `.run()` method in sequence.
- Aqeel has pulled the latest `main` branch which contains `src/uc2_observability/`.

---

## Files Aqeel Should NOT modify

- `src/uc2_observability/` — already implemented, do not change.
- `config/prometheus.yml` — already configured.
- Any Docker container config.
