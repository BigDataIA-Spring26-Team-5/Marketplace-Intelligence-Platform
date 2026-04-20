# Quickstart: Chunked CSV Processing

## New Features

### Large File Processing

The pipeline now supports arbitrarily large CSV files through chunked processing.

**CLI Options:**

```bash
# Default chunk size (10,000 rows)
python -m src.pipeline.cli --source data/large_file.csv --domain nutrition

# Custom chunk size
python -m src.pipeline.cli --source data/large_file.csv --domain nutrition --chunk-size 5000

# Resume from checkpoint
python -m src.pipeline.cli --source data/large_file.csv --domain nutrition --resume

# Force fresh (ignore checkpoint)
python -m src.pipeline.cli --source data/large_file.csv --domain nutrition --force-fresh
```

**Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `CHUNK_SIZE` | 10000 | Rows per chunk |
| `MAX_MEMORY_MB` | 512 | Memory budget per chunk |

## How It Works

1. **Schema Analysis** runs once on full file metadata (header + statistical sample)
2. **Each Chunk** processes sequentially:
   - Transform (YAML mapping applied)
   - Enrich (S1→S2→S3 cascade)
   - DQ Score computed
   - Checkpoint saved
3. **Output** aggregated from all chunks

## Checkpointing

The pipeline saves progress after each chunk. If interrupted:

```bash
# Check checkpoint status
ls checkpoint/

# Resume from last checkpoint
python -m src.pipeline.cli --source data/large_file.csv --domain nutrition --resume
```

## Troubleshooting

- **OOM errors**: Reduce CHUNK_SIZE (try 5000 or 1000)
- **Schema analysis wrong**: Ensure file has proper headers
- **Restart from scratch**: Use `--force-fresh` flag