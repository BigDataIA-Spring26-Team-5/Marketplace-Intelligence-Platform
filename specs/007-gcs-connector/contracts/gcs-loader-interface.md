# Contract: GCSSourceLoader Interface

Module: `src/pipeline/loaders/gcs_loader`

## Public API

### `is_gcs_uri(path: str) -> bool`

Returns `True` if `path` starts with `gs://`.

### `GCSSourceLoader(uri_pattern: str, project: str | None = None)`

Constructor. Does not connect to GCS.

- `uri_pattern`: `gs://bucket/prefix/*.jsonl` or single-file URI
- `project`: GCP project ID. Falls back to `GOOGLE_CLOUD_PROJECT` env var.

### `load_sample(n_rows: int = 5000) -> pd.DataFrame`

Downloads the **first matching blob only**. Used by `load_source` node for schema analysis.

- Returns at most `n_rows` rows.
- Nested dict/list values serialized to JSON strings.
- **Raises `FileNotFoundError`** if no blobs match the pattern.

### `iter_chunks(chunk_size: int = 10000) -> Iterator[pd.DataFrame]`

Streams all matching blobs in sorted order, yielding `chunk_size`-row DataFrames.

- Chunks span partition boundaries.
- Large blobs streamed line-by-line (no full materialization).
- Nested dict/list values serialized to JSON strings.
- Empty blobs skipped with warning.
- **Raises `FileNotFoundError`** if no blobs match (propagated from `_list_blobs`).
- GCS API failures: retried up to 3× with exponential backoff (1s, 2s, 4s), then re-raised.

## Errors

| Condition | Exception | Message pattern |
|-----------|-----------|-----------------|
| No blobs match pattern | `FileNotFoundError` | `"No blobs matched GCS pattern: gs://..."` |
| GCS auth failure (after 3 retries) | `google.api_core.exceptions.GoogleAPIError` | Re-raised as-is |
| Invalid URI (no `gs://` prefix) | `ValueError` | `"Not a GCS URI: ..."` |

## Integration Points

| Caller | Method | Purpose |
|--------|--------|---------|
| `src/agents/orchestrator.py` `load_source_node` | `load_sample()` | Schema analysis sample |
| `src/pipeline/runner.py` `run_chunked()` | `iter_chunks()` | Full pipeline execution |
| `src/pipeline/cli.py` | `is_gcs_uri()` | Source-type routing |
