# Data Model: UC REST API Layer

**Phase**: 1 — Design
**Feature**: specs/017-uc-rest-api-layer

All models below become Pydantic classes under `src/api/models/`.

---

## Pipeline Domain (`src/api/models/pipeline.py`)

### RunRequest
```
source_path:    str          # local CSV path or gs:// URI glob
domain:         str          # "nutrition" | "safety" | "pricing"
pipeline_mode:  str = "full" # "full" | "silver" | "gold"
with_critic:    bool = False
force_fresh:    bool = False
no_cache:       bool = False
chunk_size:     int = 10000
source_name:    str | None   # override logical source name
```

### RunStatus
```
run_id:         str          # UUID from CheckpointManager
status:         str          # "pending" | "running" | "completed" | "failed" | "cancelled"
stage:          str | None   # current graph node name
chunk_index:    int | None   # last completed chunk (0-based)
started_at:     datetime
updated_at:     datetime
error:          str | None
```

### RunResult
```
run_id:         str
status:         str
output_path:    str | None   # file path or GCS URI
rows_in:        int | None
rows_out:       int | None
rows_quarantined: int | None
dq_score_pre:   float | None
dq_score_post:  float | None
dq_delta:       float | None
block_audit:    list[BlockAuditEntry]
completed_at:   datetime | None
```

### BlockAuditEntry
```
block:          str
rows_in:        int
rows_out:       int
duration_ms:    float | None
extra:          dict         # block-specific fields from audit_entry()
```

### ResumeRequest
```
run_id:         str
```

---

## Observability Domain (`src/api/models/observability.py`)

### RunSummary
```
run_id:         str
source:         str
domain:         str
status:         str
dq_score_pre:   float | None
dq_score_post:  float | None
started_at:     datetime
completed_at:   datetime | None
rows_in:        int | None
rows_out:       int | None
```

### RunListResponse
```
runs:           list[RunSummary]
total:          int
page:           int
page_size:      int
```

### BlockTrace
```
run_id:         str
blocks:         list[BlockTraceEntry]
```

### BlockTraceEntry
```
block:          str
rows_in:        int
rows_out:       int
started_at:     datetime | None
duration_ms:    float | None
```

### AnomalyRecord
```
source:         str
anomaly_score:  float
flagged_at:     datetime
metrics:        dict         # the Prometheus values that triggered the flag
```

### QuarantineRecord
```
run_id:         str
row_index:      int | None
reason:         str
fields:         dict         # subset of the row that failed validation
```

### CostReport
```
period_start:   datetime
period_end:     datetime
by_source:      list[SourceCost]
total_tokens:   int
estimated_usd:  float | None
```

### SourceCost
```
source:         str
model_tier:     str
tokens_used:    int
requests:       int
```

### DedupStats
```
run_id:         str | None
source:         str | None
clusters:       int
merged_rows:    int
dedup_rate:     float
```

---

## Search Domain (`src/api/models/search.py`)

### SearchRequest
```
query:          str           # non-empty
domain:         str | None    # filter to domain
category:       str | None    # filter to primary_category
top_k:          int = 10      # max 100
mode:           str = "hybrid"  # "hybrid" | "bm25" | "semantic"
```

### SearchResult
```
query:          str
mode:           str
total:          int
results:        list[SearchHit]
index_ready:    bool
```

### SearchHit
```
product_name:   str
brand_name:     str | None
primary_category: str | None
data_source:    str | None
is_recalled:    bool | None
recall_class:   str | None
score:          float
rank:           int
```

---

## Recommendations Domain (`src/api/models/recommendations.py`)

### RecommendationResult
```
product_id:     str
rec_type:       str           # "also_bought" | "you_might_like"
top_k:          int
results:        list[RecHit]
graph_ready:    bool
```

### RecHit
```
product_id:     str
product_name:   str | None
primary_category: str | None
score:          float         # confidence (also_bought) or affinity_score (you_might_like)
rank:           int
extra:          dict          # lift (also_bought) or hops (you_might_like)
```

---

## Ops Domain (`src/api/models/ops.py`)

### CacheStats
```
redis_connected:    bool
total_keys:         int
by_prefix:          dict[str, int]  # {"yaml": N, "llm": N, "emb": N, "dedup": N}
sqlite_fallback:    bool
sqlite_key_count:   int | None
```

### CacheFlushRequest
```
prefix:         str | None   # flush specific prefix; null = all prefixes
domain:         str | None   # filter by domain (deletes matching yaml keys)
confirm:        bool         # must be True or 422 returned
```

### CacheFlushResult
```
deleted_count:  int
prefix:         str | None
domain:         str | None
```

### SchemaResponse
```
domain:         str
columns:        list[ColumnDef]
source_file:    str          # path to config/schemas/<domain>_schema.json
```

### ColumnDef
```
name:           str
dtype:          str
required:       bool
enrichment:     bool         # true for allergens, primary_category, dietary_tags, is_organic
computed:       bool         # true for dq_score_pre, dq_score_post, dq_delta
```

---

## State Transitions: RunStatus

```
[submit] → pending
pending  → running     (background task picks up)
running  → completed   (save_output_node finishes)
running  → failed      (unhandled exception in graph)
running  → cancelled   (reserved, 501 for now)
failed   → running     (resume request, new chunk_index)
```

---

## Entity Relationships

```
RunRequest      →  creates →   RunStatus (persisted in CheckpointManager)
RunStatus       →  enriches →  RunResult (after save_output_node writes)
RunResult       →  contains → BlockAuditEntry[]
RunSummary      →  read from → RunLogStore (UC2 run logs)
BlockTrace      →  read from → Postgres block_trace table (UC2)
QuarantineRecord → read from → Postgres quarantine_rows table (UC2)
AnomalyRecord   →  read from → Postgres anomaly_reports table (UC2)
CostReport      →  read from → Prometheus metrics (etl_llm_tokens_total)
SearchHit       →  read from → ChromaDB + BM25 index (UC3 HybridSearch)
RecHit          →  read from → ProductGraph + AssociationRuleMiner (UC4)
CacheStats      →  read from → CacheClient.get_stats()
SchemaResponse  →  read from → config/schemas/<domain>_schema.json
```
