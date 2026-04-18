# Quickstart: Checkpoint and Resume

## Usage

### Running with Checkpointing

```bash
# First run (no checkpoint) - starts normally
python -m src.pipeline.cli run --input data.csv

# After crash - automatically detects checkpoint and resumes
python -m src.pipeline.cli run --input data.csv

# Force fresh start (ignore checkpoint)
python -m src.pipeline.cli run --input data.csv --force-fresh

# Explicit resume (default behavior if checkpoint exists)
python -m src.pipeline.cli run --input data.csv --resume
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
├── runner.py       # Executes blocks (existing)
└── checkpoint.py  # NEW: save/load/resume
```

**Checkpoint location**: `checkpoint/checkpoint.db`

**Corpus location**: `corpus/faiss_index.bin` (unchanged)

---

## Testing

```bash
# Run unit tests
pytest tests/unit/test_checkpoint.py -v

# Integration: simulate crash mid-chunk
# 1. Start pipeline
# 2. Kill process at chunk boundary
# 3. Restart - should resume from next chunk
```