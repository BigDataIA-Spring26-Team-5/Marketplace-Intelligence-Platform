# Quickstart: Checkpoint and Resume

## Usage

### Running with Checkpointing

```bash
# First run (no checkpoint) - starts normally
python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition

# After crash - automatically detects checkpoint and resumes
python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition --resume

# Force fresh start (ignore checkpoint)
python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition --force-fresh
```

### Checkpoint File

SQLite database at `checkpoint/checkpoint.db`:

```bash
# View checkpoint status
sqlite3 checkpoint/checkpoint.db "SELECT * FROM checkpoints ORDER BY created_at DESC LIMIT 1;"

# View chunk states
sqlite3 checkpoint/checkpoint.db "SELECT chunk_index, status, record_count FROM chunk_states;"
```

---

## Architecture

```
src/pipeline/
├── runner.py           # Executes blocks (existing)
└── checkpoint/
    ├── __init__.py     # Module exports
    ├── manager.py     # CheckpointManager class
    └── schema.sql     # SQLite schema
├── cli.py              # NEW: CLI with --resume/--force-fresh
```

**Checkpoint location**: `checkpoint/checkpoint.db`

**Corpus location**: `corpus/faiss_index.bin` (unchanged)

**Schema version**: stored in `.specify/requiredlimits.yaml` (`checkpoint_schema_version: 1`)

---

## Testing

```bash
# Checkpointing is integrated into the main CLI
# Integration: simulate crash mid-chunk
# 1. Start pipeline with checkpointing enabled
# 2. Kill process at chunk boundary
# 3. Restart with --resume - should resume from last checkpoint

# View checkpoint status
sqlite3 checkpoint/checkpoint.db "SELECT run_id, source_file, schema_version, created_at FROM checkpoints ORDER BY created_at DESC LIMIT 1;"

# View chunk states
sqlite3 checkpoint/checkpoint.db "SELECT chunk_index, status, record_count, dq_score_pre, dq_score_post FROM chunk_states;"
```

## Performance

- Checkpoint write: <5s per chunk (target)
- Resume detection: <2s (target)
- Storage: SQLite + FAISS index (~10MB per 100k vectors)