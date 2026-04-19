# Data Model: Checkpoint and Resume

## Entities

### Checkpoint

Persistent record containing pipeline run context and state.

| Field | Type | Description |
|-------|------|-------------|
| id | INTEGER PRIMARY KEY | Auto-increment checkpoint ID |
| run_id | TEXT | Unique run identifier (UUID) |
| source_file | TEXT | Path to source data file |
| source_sha256 | TEXT | SHA256 hash of source file |
| schema_version | INTEGER | Pipeline schema version |
| created_at | TEXT | ISO timestamp |
| resume_state | TEXT | One of: "resume", "force-fresh", "none" |

**Relationships**: Has many ChunkState entries

### ChunkState

Metadata for each chunk processed.

| Field | Type | Description |
|-------|------|-------------|
| id | INTEGER PRIMARY KEY |
| checkpoint_id | INTEGER FK | Reference to Checkpoint |
| chunk_index | INTEGER | 0-based chunk number |
| status | TEXT | One of: "pending", "completed", "failed" |
| record_count | INTEGER | Number of records in chunk |
| dq_score_pre | REAL | DQ score before enrichment |
| dq_score_post | REAL | DQ score after enrichment |
| completed_at | TEXT | ISO timestamp if completed |

### TransformationPlan

YAML transformation plan stored as-is.

| Field | Type | Description |
|-------|------|-------------|
| id | INTEGER PRIMARY KEY |
| checkpoint_id | INTEGER FK | Reference to Checkpoint |
| plan_yaml | TEXT | Full YAML content |
| plan_md5 | TEXT | MD5 for validation |

### CorpusSnapshot

Corpus state for fast resumption.

| Field | Type | Description |
|-------|------|-------------|
| id | INTEGER PRIMARY KEY |
| checkpoint_id | INTEGER FK | Reference to Checkpoint |
| index_path | TEXT | Path to FAISS index file |
| metadata_path | TEXT | Path to corpus metadata JSON |
| vector_count | INTEGER | Number of vectors in index |

---

## State Transitions

### Checkpoint Lifecycle

```
none -> (first chunk starts) -> pending -> (all chunks done) -> completed
                            -> (crash) -> resume
```

### Resume Flow

```
1. Load checkpoint.db
2. Validate schema_version matches current
3. Validate source_sha256 matches current source
4. If invalid -> warn operator, require force-fresh
5. If valid -> load corpus, load plan, start from first "pending" chunk
```

---

## Validation Rules

- source_sha256 must match for resume to proceed
- schema_version must match for resume to proceed  
- If corruption detected (missing required fields) -> warn + offer fresh start
- ChunkState.status can only be "pending", "completed", "failed"

---

## Integration with Existing Schema

Checkpoint stores schema gaps applied at each chunk but does not modify the unified schema contract. DQ scores are preserved per chunk via ChunkState.