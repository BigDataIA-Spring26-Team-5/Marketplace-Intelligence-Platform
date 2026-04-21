# Data Model: Observability Log Persistence & RAG Chatbot

**Feature**: 011-observability-rag-chatbot  
**Date**: 2026-04-21

## Storage Location

```
output/
└── run_logs/
    ├── run_20260421T143012_a3f7b2c1.json
    ├── run_20260421T150345_d9e2f801.json
    └── ...
```

`output/` is gitignored. Each file is one complete run log, written atomically.

---

## Run Log Schema (JSON, per file)

```json
{
  "run_id": "a3f7b2c1-4d5e-6f78-9012-abcdef012345",
  "timestamp": "2026-04-21T14:30:12.456789",
  "source_path": "data/usda_fooddata_sample.csv",
  "source_name": "usda_fooddata_sample",
  "domain": "nutrition",
  "status": "success",
  "error": null,
  "duration_seconds": 45.3,

  "rows_in": 1000,
  "rows_out": 987,
  "rows_quarantined": 13,

  "dq_score_pre": 0.82,
  "dq_score_post": 0.91,
  "dq_delta": 0.09,

  "enrichment_stats": {
    "deterministic": 200,
    "embedding": 50,
    "llm": 30,
    "unresolved": 20
  },

  "block_sequence": [
    "column_mapping", "dq_score_pre", "normalize_text",
    "extract_allergens", "llm_enrich", "dq_score_post"
  ],

  "audit_log": [
    {
      "block": "column_mapping",
      "rows_in": 1000,
      "rows_out": 1000,
      "rows_delta": 0
    },
    {
      "block": "dq_score_pre",
      "rows_in": 1000,
      "rows_out": 1000,
      "rows_delta": 0
    }
  ],

  "column_mapping": {
    "fdc_id": "product_id",
    "description": "product_name",
    "brandOwner": "brand_name"
  },

  "operations": [
    {"type": "RENAME", "source": "fdc_id", "target": "product_id"},
    {"type": "CAST", "source": "serving_size", "target": "serving_size", "to": "float"}
  ],

  "critique_notes": [
    {"rule": "null_rate", "column": "ingredients", "correction": "flagged high null rate"}
  ],

  "quarantine_reasons": [
    {"row_idx": 42, "missing_fields": ["product_name"], "reason": "required field null after enrichment"}
  ],

  "cache_stats": {
    "yaml": {"hits": 1, "misses": 0, "hit_rate_pct": 100.0},
    "llm": {"hits": 15, "misses": 5, "hit_rate_pct": 75.0}
  },

  "registry_hits": {
    "allergens": "extract_allergens",
    "is_organic": "detect_organic"
  },

  "schema_fingerprint": "a3f7b2c1d4e5f678"
}
```

### Field Definitions

| Field | Type | Required | Source in PipelineState |
|---|---|---|---|
| `run_id` | string (UUID4) | yes | generated at write time |
| `timestamp` | string (ISO8601) | yes | generated at write time |
| `source_path` | string | yes | `state["source_path"]` |
| `source_name` | string | yes | derived: `Path(source_path).stem` |
| `domain` | string | no | `state.get("domain", "unknown")` |
| `status` | enum: success/partial/failed | yes | passed by caller |
| `error` | string or null | yes | passed by caller |
| `duration_seconds` | float | no | measured by writer |
| `rows_in` | int | no | `len(state["source_df"])` if present |
| `rows_out` | int | no | `len(state["working_df"])` if present |
| `rows_quarantined` | int | no | `len(state["quarantined_df"])` if present |
| `dq_score_pre` | float | no | `state.get("dq_score_pre")` |
| `dq_score_post` | float | no | `state.get("dq_score_post")` |
| `dq_delta` | float | no | computed: `dq_score_post - dq_score_pre` |
| `enrichment_stats` | dict | no | `state.get("enrichment_stats", {})` |
| `block_sequence` | list[str] | no | `state.get("block_sequence", [])` |
| `audit_log` | list[dict] | no | `state.get("audit_log", [])` |
| `column_mapping` | dict | no | `state.get("column_mapping", {})` |
| `operations` | list[dict] | no | `state.get("revised_operations", state.get("operations", []))` |
| `critique_notes` | list[dict] | no | `state.get("critique_notes", [])` |
| `quarantine_reasons` | list[dict] | no | `state.get("quarantine_reasons", [])` |
| `cache_stats` | dict | no | `state["cache_client"].get_stats().summary()` if present |
| `registry_hits` | dict | no | `state.get("block_registry_hits", {})` |
| `schema_fingerprint` | string | no | `state.get("_schema_fingerprint")` |

### Status Values

| Status | Meaning |
|---|---|
| `success` | All nodes completed, output written |
| `partial` | Some nodes completed, pipeline raised an exception mid-run |
| `failed` | Pipeline failed before producing any output (e.g., load error) |

---

## RunLogWriter

```
src/uc2_observability/log_writer.py
```

```python
class RunLogWriter:
    def __init__(self, log_dir: Path = PROJECT_ROOT / "output" / "run_logs"):
        ...

    def save(self, state: PipelineState, status: str, error: str | None = None,
             start_time: float | None = None) -> Path:
        """Write run log to disk. Returns path to written file. Never raises."""
        ...
```

**Behavior**:
- Creates `log_dir` if absent
- Generates `run_id = uuid4()` and `timestamp = datetime.utcnow().isoformat()`
- Extracts all fields from state with `.get()` defaults
- Writes to `{log_dir}/run_{timestamp_compact}_{run_id[:8]}.json` via temp+rename for atomicity
- On any exception during write: logs warning, returns None — never propagates to pipeline

---

## RunLogStore

```
src/uc2_observability/log_store.py
```

```python
class RunLogStore:
    def __init__(self, log_dir: Path = PROJECT_ROOT / "output" / "run_logs"):
        ...

    def load_all(self) -> list[dict]:
        """Load all run logs from disk, sorted by timestamp ascending. Skips corrupt files."""
        ...

    def get_by_run_id(self, run_id: str) -> dict | None:
        """Return single run log by run_id, or None."""
        ...

    def filter(self,
               source_name: str | None = None,
               status: str | None = None,
               since: datetime | None = None,
               limit: int | None = None) -> list[dict]:
        """Return logs matching filters, sorted by timestamp descending."""
        ...

    def summary_stats(self) -> dict:
        """Return aggregate stats: total_runs, success_rate, avg_dq_delta, etc."""
        ...
```

---

## ObservabilityChatbot

```
src/uc2_observability/rag_chatbot.py  (replace existing placeholder)
```

```python
class ObservabilityChatbot:
    def __init__(self, log_store: RunLogStore):
        ...

    def ingest_audit_logs(self) -> int:
        """Reload logs from store. Returns count of loaded records."""
        ...

    def get_relevant_context(self, query: str, max_runs: int = 10) -> list[dict]:
        """Return subset of run logs most relevant to query (keyword + recency heuristic)."""
        ...

    def query(self, question: str) -> ChatResponse:
        """Answer question. Returns response with answer text and cited run IDs."""
        ...
```

```python
@dataclass
class ChatResponse:
    answer: str
    cited_run_ids: list[str]
    context_run_count: int
```

**Retrieval logic** (inside `get_relevant_context`):
1. If query mentions a run ID (UUID pattern): return that run's log directly
2. If query mentions a source name: filter by `source_name`
3. If query mentions time words ("last", "recent", "latest N"): filter by recency
4. If query mentions a metric ("dq", "score", "enrichment", "quarantine", "block"): include all runs, sort by timestamp desc, take `max_runs`
5. Default: return last `max_runs` runs

**LLM synthesis prompt structure**:
```
System: You are a pipeline observability assistant. Answer questions about pipeline
execution history using ONLY the provided run log data. Cite run_ids for every claim.
If the answer cannot be determined from the logs, say so.

User: Run logs (JSON):
{json.dumps(relevant_logs, indent=2)}

Question: {question}
```

---

## MetricsExporter

```
src/uc2_observability/metrics_exporter.py
```

```python
class MetricsExporter:
    def __init__(self, pushgateway_url: str = "localhost:9091", job: str = "etl_pipeline"):
        ...

    def push(self, run_log: dict) -> bool:
        """Push run metrics to Prometheus Pushgateway. Returns True on success. Never raises."""
        ...
```

**Prometheus metrics pushed per run** (all `Gauge` type):

| Metric name | Value source | Labels |
|---|---|---|
| `etl_dq_score_pre` | `run_log["dq_score_pre"]` | `source_name`, `status`, `run_id` |
| `etl_dq_score_post` | `run_log["dq_score_post"]` | `source_name`, `status`, `run_id` |
| `etl_dq_delta` | `run_log["dq_delta"]` | `source_name`, `status`, `run_id` |
| `etl_rows_in` | `run_log["rows_in"]` | `source_name`, `status`, `run_id` |
| `etl_rows_out` | `run_log["rows_out"]` | `source_name`, `status`, `run_id` |
| `etl_rows_quarantined` | `run_log["rows_quarantined"]` | `source_name`, `status`, `run_id` |
| `etl_duration_seconds` | `run_log["duration_seconds"]` | `source_name`, `status`, `run_id` |
| `etl_enrichment_s1_resolved` | `enrichment_stats["deterministic"]` | `source_name`, `run_id` |
| `etl_enrichment_s2_resolved` | `enrichment_stats["embedding"]` | `source_name`, `run_id` |
| `etl_enrichment_s3_resolved` | `enrichment_stats["llm"]` | `source_name`, `run_id` |
| `etl_enrichment_unresolved` | `enrichment_stats["unresolved"]` | `source_name`, `run_id` |
| `etl_run_status` | 1.0=success / 0.5=partial / 0.0=failed | `source_name`, `run_id` |

**Behavior**:
- Uses `prometheus_client.CollectorRegistry` (isolated per push) + `push_to_gateway()`
- Missing fields (e.g., `dq_score_pre` absent on failed runs) default to `0.0` — metric is still pushed
- Returns `False` and logs a warning on any network error; never raises

---

## Grafana Dashboard

```
grafana/dashboards/pipeline-observability.json
```

Provisioned dashboard with 6 panels:

| Panel | Type | Metric(s) |
|---|---|---|
| DQ Scores Over Time | Time series | `etl_dq_score_pre`, `etl_dq_score_post`, `etl_dq_delta` |
| Enrichment Tier Breakdown | Stacked bar | `etl_enrichment_s1_resolved`, `_s2_`, `_s3_`, `_unresolved` |
| Run Status | Stat (last value) | `etl_run_status` |
| Row Counts | Time series | `etl_rows_in`, `etl_rows_out`, `etl_rows_quarantined` |
| Run Duration | Time series | `etl_duration_seconds` |
| Source Filter | Dashboard variable | `$source_name` label filter on all panels |

All panels use `run_id` as a series label to distinguish individual runs on the time axis.

---

## Chatbot Session State (Streamlit)

Stored in `st.session_state` under `obs_*` prefix to avoid collisions:

| Key | Type | Description |
|---|---|---|
| `obs_chatbot` | `ObservabilityChatbot` | singleton, initialized once |
| `obs_messages` | `list[dict]` | `[{role, content, cited_run_ids}]` |
| `obs_last_refresh` | `datetime` | when logs were last reloaded |
