# Data Model: Pipeline Run Log Tracking

**Feature**: 005-log-tracking  
**Date**: 2026-04-20  
**Storage**: ChromaDB (embedded), JSON sidecar files

---

## ChromaDB Collection: `pipeline_audit`

Primary store. Each document represents one complete pipeline run.

### Document Schema

```python
{
    # ChromaDB document ID — matches run_id
    "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",

    # Embedded text — what gets vectorized for semantic search
    "document": """
        Run f47ac10b | domain: nutrition | status: completed
        DQ pre: 0.68 → post: 0.84 | rows: 50000 in, 49200 out, 800 quarantined
        Blocks: dq_score_pre(50000→50000, 95ms), DYNAMIC_MAPPING_nutrition(50000→50000, 280ms),
                strip_whitespace(50000→50000, 12ms), fuzzy_deduplicate(50000→49300, 3200ms),
                llm_enrich(49300→49200, 18400ms), dq_score_post(49200→49200, 88ms)
        Enrichment: deterministic=12000 embedding=28000 llm=9000 unresolved=200
        Quarantine reasons: missing_brand_owner=450 missing_category=350
        Source: data/usda_fooddata_sample.csv | run_type: demo
        Started: 2026-04-20T10:00:00Z | Completed: 2026-04-20T10:05:30Z
    """,

    # ChromaDB metadata — used for filtering (where clauses)
    "metadata": {
        "run_id":               "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "run_type":             "demo",          # dev | demo | prod
        "status":               "completed",     # completed | failed | partial
        "domain":               "nutrition",
        "source_path":          "data/usda_fooddata_sample.csv",
        "dq_score_pre":         0.68,
        "dq_score_post":        0.84,
        "row_count_in":         50000,
        "row_count_out":        49200,
        "row_count_quarantined": 800,
        "llm_calls":            450,
        "started_at":           "2026-04-20T10:00:00Z",
        "completed_at":         "2026-04-20T10:05:30Z",
        "duration_seconds":     330
    }
}
```

### Query Patterns

```python
# Default chatbot query — exclude dev runs
collection.query(
    query_texts=["which run had most quarantined rows"],
    where={"run_type": {"$in": ["demo", "prod"]}},
    n_results=5
)

# Domain-filtered query
collection.query(
    query_texts=["nutrition pipeline DQ scores"],
    where={"$and": [{"domain": "nutrition"}, {"run_type": {"$in": ["demo", "prod"]}}]},
    n_results=5
)

# Include dev runs (debug mode)
collection.query(
    query_texts=["why did the last run fail"],
    where={"run_type": {"$in": ["dev", "demo", "prod"]}},
    n_results=5
)
```

---

## JSON Sidecar: `output/logs/{run_id}.json`

Full structured log persisted alongside ChromaDB document. Source of truth for detailed per-block data not embedded in the ChromaDB document text.

```json
{
    "run_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "run_type": "demo",
    "status": "completed",
    "domain": "nutrition",
    "source_path": "data/usda_fooddata_sample.csv",
    "dq_score_pre": 0.68,
    "dq_score_post": 0.84,
    "row_count_in": 50000,
    "row_count_out": 49200,
    "row_count_quarantined": 800,
    "enrichment_stats": {
        "deterministic": 12000,
        "embedding": 28000,
        "llm": 9000,
        "unresolved": 200
    },
    "quarantine_reasons": {
        "missing_brand_owner": 450,
        "missing_category": 350
    },
    "llm_calls": 450,
    "started_at": "2026-04-20T10:00:00Z",
    "completed_at": "2026-04-20T10:05:30Z",
    "duration_seconds": 330,
    "block_audit": [
        {
            "block_name": "dq_score_pre",
            "rows_in": 50000,
            "rows_out": 50000,
            "duration_ms": 95,
            "extra_meta": {}
        },
        {
            "block_name": "DYNAMIC_MAPPING_nutrition",
            "rows_in": 50000,
            "rows_out": 50000,
            "duration_ms": 280,
            "extra_meta": {"mapping_source": "registry_hit"}
        },
        {
            "block_name": "strip_whitespace",
            "rows_in": 50000,
            "rows_out": 50000,
            "duration_ms": 12,
            "extra_meta": {}
        },
        {
            "block_name": "fuzzy_deduplicate",
            "rows_in": 50000,
            "rows_out": 49300,
            "duration_ms": 3200,
            "extra_meta": {"duplicate_clusters": 700}
        },
        {
            "block_name": "llm_enrich",
            "rows_in": 49300,
            "rows_out": 49200,
            "duration_ms": 18400,
            "extra_meta": {"llm_calls": 450, "cache_hits": 120}
        },
        {
            "block_name": "dq_score_post",
            "rows_in": 49200,
            "rows_out": 49200,
            "duration_ms": 88,
            "extra_meta": {}
        }
    ]
}
```

---

## Python Data Classes (`src/uc2_observability/models.py`)

```python
from dataclasses import dataclass, field
from typing import Literal

RunType = Literal["dev", "demo", "prod"]
RunStatus = Literal["completed", "failed", "partial"]

@dataclass
class BlockAuditEntry:
    block_name: str
    rows_in: int
    rows_out: int
    duration_ms: int
    extra_meta: dict = field(default_factory=dict)

@dataclass
class PipelineRunLog:
    run_id: str
    run_type: RunType
    status: RunStatus
    domain: str
    source_path: str
    dq_score_pre: float
    dq_score_post: float
    row_count_in: int
    row_count_out: int
    row_count_quarantined: int
    enrichment_stats: dict
    quarantine_reasons: dict
    llm_calls: int
    started_at: str       # ISO 8601
    completed_at: str     # ISO 8601
    duration_seconds: int
    block_audit: list[BlockAuditEntry] = field(default_factory=list)
```

---

## File Layout

```text
output/
└── logs/
    ├── f47ac10b-....json     # one JSON sidecar per run
    └── ...

corpus/                       # existing — unchanged
    ├── faiss_index.bin
    └── corpus_metadata.json

# ChromaDB embedded DB — persisted to disk
.chroma/
└── pipeline_audit/           # ChromaDB collection directory
```

---

## Entity Relationships

```
PipelineRunLog ──< BlockAuditEntry     (1 run → N block records)
PipelineRunLog ──> ObservabilityDocument  (1 run → 1 ChromaDB doc)
```

---

## Validation Rules

| Field | Rule |
|-------|------|
| `run_type` | Must be `dev`, `demo`, or `prod` — read from `PIPELINE_RUN_TYPE` env var |
| `status` | `completed` only if `save_output_node` finished; `failed` if exception raised; `partial` if killed mid-chunk |
| `run_id` | Must be unique in ChromaDB collection — duplicate write raises error, does not overwrite |
| `dq_score_pre` / `dq_score_post` | Float 0.0–1.0; `None` allowed for failed runs where DQ scoring did not complete |
| `block_audit` | Must contain at least `dq_score_pre` entry; empty list only for runs that failed before Node 5 |
