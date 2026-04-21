# Data Model: GCS Bronze Layer Connector

## GCS URI

```
gs://<bucket>/<prefix>/<glob-pattern>
```

| Field | Type | Example | Constraint |
|-------|------|---------|-----------|
| `bucket` | string | `mip-bronze-2024` | No `gs://` prefix |
| `prefix` | string | `usda/2026/04/20/` | Trailing `/` or empty |
| `blob_pattern` | string | `*.jsonl` | fnmatch glob |

Parsed by `_parse_gcs_uri()`. Stored as-is in `checkpoints.db.source_file`.

## Checkpoint Record (GCS variant)

| Column | Value |
|--------|-------|
| `source_file` | Full GCS URI (`gs://...`) |
| `source_sha256` | SHA-256 of URI string (not file contents) |
| `resume_state` | `"none"` \| `"partial"` \| `"complete"` |

## Loaded DataFrame Shape

After `load_sample()` or a chunk from `iter_chunks()`:

- Rows: up to `n_rows` (sample) or `chunk_size` (full run)
- Columns: all top-level JSONL keys
- Nested dict/list values: serialized to JSON strings (`str` dtype)
- No schema enforcement at loader level — Orchestrator LLM handles gap analysis

## GCSSourceLoader State

```
GCSSourceLoader
├── uri_pattern   : str
├── project       : str | None   (from GOOGLE_CLOUD_PROJECT env)
├── _bucket_name  : str
├── _prefix       : str
├── _blob_pattern : str
└── _client       : storage.Client | None   (lazy init)
```
