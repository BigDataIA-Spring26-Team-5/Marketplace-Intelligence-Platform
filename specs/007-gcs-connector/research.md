# Research: GCS Bronze Layer Connector

## Decision: Line-by-line streaming for large JSONL partitions

**Decision**: Use `blob.open(mode="rb")` + manual line iteration, accumulate into 10K-row chunks via `pd.DataFrame(records)`.

**Rationale**: `pd.read_json(io.BytesIO(data), lines=True)` materializes the full blob in memory before parsing. `blob.open()` returns a streaming file-like object; reading line-by-line and batching into `records[]` lists keeps peak memory proportional to chunk size, not blob size.

**Alternative considered**: `pd.read_json(..., chunksize=N, lines=True)` — requires materializing into `TextIOWrapper` first, still downloads full blob. Less control over chunk boundaries across partitions.

**Alternative considered**: `blob.download_as_text()` split by `\n` — same as current impl, full materialization.

## Decision: Retry strategy for GCS API failures

**Decision**: Manual 3× retry loop with exponential backoff (1s → 2s → 4s) using `time.sleep`. No external library needed.

**Rationale**: `tenacity` adds a dependency for a simple 3-attempt pattern. `google-cloud-storage` already retries some transient errors internally via its own retry policy; our retry layer handles higher-level failures (permission flaps, blob deleted mid-listing). 3 attempts with doubling backoff covers most transient network issues within ~7s.

**Alternative considered**: `tenacity` library — cleaner decorator syntax but adds dep for 10 lines of logic.

**Alternative considered**: `google.api_core.retry.Retry` — GCS SDK retry, but only covers transport-level errors, not application-level 404s on blob deletion.

## Decision: Nested JSON fields → JSON string serialization

**Decision**: After `pd.read_json(lines=True)`, detect columns whose dtype is `object` and whose first non-null value is a `dict` or `list`; apply `json.dumps` to those columns.

**Rationale**: `pd.read_json` with `lines=True` parses nested JSON into Python dicts/lists by default, not strings. Downstream blocks (DynamicMappingBlock, DQ scorer) expect scalar or string values. Serializing to JSON string is reversible and keeps the bronze data intact.

**Alternative considered**: Keep as Python objects — breaks DQ scoring (non-scalar values) and YAML-based transforms.

## Decision: Zero-file error location

**Decision**: Raise `FileNotFoundError` in `GCSSourceLoader._list_blobs()` when no blobs match the pattern, before returning to any caller.

**Rationale**: Spec clarification Q2 says fail-fast "before agent graph entry." Raising in `_list_blobs()` enforces this regardless of which caller (`load_sample`, `iter_chunks`, future methods) invokes it. The orchestrator's secondary check (`if df.empty: raise`) becomes a safety net for empty blobs, not the primary guard.

**Impact on tests**: `test_load_sample_empty_bucket_returns_empty_df` must be updated to expect `FileNotFoundError`.
