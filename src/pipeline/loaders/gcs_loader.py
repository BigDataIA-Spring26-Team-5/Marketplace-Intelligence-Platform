"""GCS Bronze Layer connector — reads JSONL partitions from GCS into DataFrames."""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import time
from typing import Iterator

import pandas as pd

logger = logging.getLogger(__name__)

_RETRY_DELAYS = (1, 2, 4)


def is_gcs_uri(path: str) -> bool:
    return path.startswith("gs://")


def _parse_gcs_uri(uri: str) -> tuple[str, str, str]:
    """Return (bucket, prefix, blob_pattern) from a gs:// URI.

    Example:
        gs://mip-bronze-2024/usda/2026/04/20/*.jsonl
        → ("mip-bronze-2024", "usda/2026/04/20/", "*.jsonl")
    """
    without_scheme = uri[len("gs://"):]
    bucket, _, rest = without_scheme.partition("/")
    if "/" in rest:
        prefix = rest.rsplit("/", 1)[0] + "/"
        pattern = rest.rsplit("/", 1)[1]
    else:
        prefix = ""
        pattern = rest
    return bucket, prefix, pattern


def _with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) up to 3×, exponential backoff on failure."""
    last_exc = None
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            logger.warning(f"GCS call failed (attempt {attempt}/3): {exc}. Retrying in {delay}s...")
            time.sleep(delay)
    raise last_exc


class GCSSourceLoader:
    """Loads JSONL files from a GCS URI pattern into pandas DataFrames.

    Supports:
    - Glob patterns: gs://bucket/path/*.jsonl
    - Single files: gs://bucket/path/part_0000.jsonl
    - Sampling: reads only first partition for schema analysis
    - Chunked iteration: streams all matching partitions in row chunks
    """

    def __init__(self, uri_pattern: str, project: str | None = None):
        self.uri_pattern = uri_pattern
        self.project = project or os.environ.get("GOOGLE_CLOUD_PROJECT")
        self._bucket_name, self._prefix, self._blob_pattern = _parse_gcs_uri(uri_pattern)
        self._client = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client(project=self.project)
        return self._client

    def _list_blobs(self) -> list:
        """List blobs matching the URI pattern, sorted by name.

        Raises FileNotFoundError if no blobs match.
        """
        client = self._get_client()
        bucket = client.bucket(self._bucket_name)
        blobs = _with_retry(lambda: list(bucket.list_blobs(prefix=self._prefix)))
        matching = [
            b for b in blobs
            if fnmatch.fnmatch(b.name.split("/")[-1], self._blob_pattern)
        ]
        matching.sort(key=lambda b: b.name)
        if not matching:
            raise FileNotFoundError(f"No blobs matched GCS pattern: {self.uri_pattern}")
        logger.info(f"GCS: found {len(matching)} blobs matching {self.uri_pattern}")
        return matching

    def _blob_to_df(self, blob) -> pd.DataFrame:
        """Stream a single blob line-by-line and parse as JSONL.

        Nested dict/list values are serialized to JSON strings.
        Returns empty DataFrame for empty blobs.
        """
        records = []
        def _read():
            with blob.open("rb") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as exc:
                            logger.warning(
                                f"Skipping malformed JSON line in {blob.name}: {exc}"
                            )

        _with_retry(_read)

        if not records:
            logger.warning(f"Empty blob: {blob.name}")
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Serialize nested dict/list columns to JSON strings
        for col in df.columns:
            first_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_val, (dict, list)):
                df[col] = df[col].apply(
                    lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
                )

        return df

    def load_sample(self, n_rows: int = 5000) -> pd.DataFrame:
        """Read first partition only for schema analysis.

        Returns up to n_rows rows from the first matching blob.
        Raises FileNotFoundError if no blobs match the pattern.
        """
        blobs = self._list_blobs()
        first_blob = blobs[0]
        logger.info(f"GCS schema sample: streaming {first_blob.name}")
        df = self._blob_to_df(first_blob)

        if len(df) > n_rows:
            df = df.head(n_rows)

        logger.info(f"GCS schema sample: {len(df)} rows from {first_blob.name}")
        return df

    def iter_chunks(self, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """Yield DataFrame chunks across all matching partitions.

        Streams partitions sequentially. Emits chunks of up to chunk_size rows,
        spanning partition boundaries. Raises FileNotFoundError if no blobs match.
        """
        blobs = self._list_blobs()

        buffer = pd.DataFrame()
        for blob in blobs:
            logger.info(f"GCS: streaming {blob.name}")
            partition_df = self._blob_to_df(blob)
            if partition_df.empty:
                continue

            buffer = pd.concat([buffer, partition_df], ignore_index=True)

            while len(buffer) >= chunk_size:
                yield buffer.iloc[:chunk_size].copy()
                buffer = buffer.iloc[chunk_size:].reset_index(drop=True)

        if not buffer.empty:
            yield buffer

    def iter_chunks_with_blob_name(self, chunk_size: int = 10000) -> Iterator[tuple[str, pd.DataFrame]]:
        """Yield (gs:// blob URI, DataFrame) per chunk, preserving blob-level provenance.

        Unlike iter_chunks, chunks do not span blob boundaries so each chunk
        carries a single, unambiguous _bronze_file value.
        """
        blobs = self._list_blobs()
        for blob in blobs:
            logger.info(f"GCS: streaming {blob.name} (with blob name)")
            partition_df = self._blob_to_df(blob)
            if partition_df.empty:
                continue
            blob_uri = f"gs://{self._bucket_name}/{blob.name}"
            for start in range(0, len(partition_df), chunk_size):
                yield blob_uri, partition_df.iloc[start:start + chunk_size].copy()
