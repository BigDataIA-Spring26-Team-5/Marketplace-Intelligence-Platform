# Data Model: Chunked CSV Processing

## Entities

### 1. StreamingConfig

Configuration for chunked processing.

| Field | Type | Description |
|-------|------|-------------|
| `chunk_size` | int | Rows per chunk (default: 10000) |
| `max_memory_mb` | int | Memory budget per chunk (default: 512) |
| `skip_first_rows` | int | Rows to skip at start (for resume) |
| `total_rows` | int | Total rows in file (computed) |
| `total_chunks` | int | Computed: ceil(total_rows / chunk_size) |

**Relationships**: Passed to CsvStreamReader, used by PipelineRunner.

---

### 2. CsvStreamReader

Streaming CSV loader.

| Field | Type | Description |
|-------|------|-------------|
| `file_path` | Path | Path to CSV file |
| `chunk_size` | int | Rows per iteration |
| `encoding` | string | File encoding (default: UTF-8) |
| `delimiter` | string | CSV delimiter (default: ,) |

**Methods**:
- `__iter__()`: Yields DataFrame chunks
- `get_total_rows()`: Count total rows without loading full file

**Relationships**: Yields chunks consumed by PipelineRunner.

---

### 3. ChunkState

Tracks progress per chunk.

| Field | Type | Description |
|-------|------|-------------|
| `chunk_index` | int | 0-based chunk number |
| `record_count` | int | Rows in this chunk |
| `stage` | string | Current stage: transform/enrich/dq/complete |
| `status` | string | pending/in_progress/completed/failed |
| `completed_at` | datetime | Timestamp when completed |
| `error_message` | string | If failed, the error |

**State Transitions**:

```
pending → in_progress → completed
                      → failed
```

**Relationships**: Belongs to Checkpoint, stored in CheckpointManager.

---

### 4. ChunkProcessingResult

Result from processing a single chunk.

| Field | Type | Description |
|-------|------|-------------|
| `chunk_index` | int | Which chunk |
| `record_count` | int | Output rows |
| `dq_score_pre` | float | Pre-enrichment DQ |
| `dq_score_post` | float | Post-enrichment DQ |
| `quarantine_count` | int | Rows in quarantine |
| `enrichment_stats` | dict | S1/S2/S3 counts |

**Relationships**: Returned by PipelineRunner.process_chunk().

---

### 5. SchemaProfile (Enhanced)

| Field | Type | Description |
|-------|------|-------------|
| `source_file` | Path | Source CSV path |
| `total_rows` | int | Total rows in file |
| `total_columns` | int | Column count |
| `file_sha256` | str | File hash for checkpoint |
| `analyzed_at` | datetime | When analyzed |
| `is_cached` | bool | Whether reused from cache |

**Relationships**: Generated once per source file, shared across chunks.

---

## Validation Rules

1. **Chunk size bounds**: 100 ≤ chunk_size ≤ 100,000
2. **Memory safety**: If max_memory_mb < 256, warn user
3. **Resume valid**: If resuming, file SHA must match original
4. **Stage progression**: Chunk stage must advance sequentially

---

## Schema Analysis Flow

```
Source File
    ↓
1. Read header + sample (1000 rows)
    ↓
2. Run schema analysis (once)
    ↓ (results cached)
3. For each chunk:
   - Load chunk
   - Apply transforms
   - Run enrichment
   - Compute DQ
   - Save checkpoint
    ↓
4. Aggregate output
```

---

## Checkpoint Schema (Existing)

The CheckpointManager schema already supports chunk tracking:

```sql
CREATE TABLE chunk_states (
    id INTEGER PRIMARY KEY,
    checkpoint_id INTEGER,
    chunk_index INTEGER,
    status TEXT,
    record_count INTEGER,
    dq_score_pre REAL,
    dq_score_post REAL,
    completed_at TEXT
);
```

This extends naturally to track streaming state.