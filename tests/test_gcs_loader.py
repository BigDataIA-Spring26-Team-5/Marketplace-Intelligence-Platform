"""Unit tests for GCSSourceLoader — mocks GCS client."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.loaders.gcs_loader import (
    GCSSourceLoader,
    _parse_gcs_uri,
    is_gcs_uri,
)


# ── URI helpers ───────────────────────────────────────────────────────

def test_is_gcs_uri_true():
    assert is_gcs_uri("gs://bucket/path/file.jsonl") is True


def test_is_gcs_uri_false():
    assert is_gcs_uri("data/local_file.csv") is False
    assert is_gcs_uri("/abs/path/file.csv") is False


def test_parse_gcs_uri_with_glob():
    bucket, prefix, pattern = _parse_gcs_uri("gs://mip-bronze-2024/usda/2026/04/20/*.jsonl")
    assert bucket == "mip-bronze-2024"
    assert prefix == "usda/2026/04/20/"
    assert pattern == "*.jsonl"


def test_parse_gcs_uri_single_file():
    bucket, prefix, pattern = _parse_gcs_uri("gs://mip-bronze-2024/usda/part_0000.jsonl")
    assert bucket == "mip-bronze-2024"
    assert prefix == "usda/"
    assert pattern == "part_0000.jsonl"


def test_parse_gcs_uri_root_file():
    bucket, prefix, pattern = _parse_gcs_uri("gs://bucket/file.jsonl")
    assert bucket == "bucket"
    assert prefix == ""
    assert pattern == "file.jsonl"


# ── Fixtures ──────────────────────────────────────────────────────────

def _make_jsonl_bytes(records: list[dict]) -> bytes:
    lines = [json.dumps(r) for r in records]
    return "\n".join(lines).encode()


def _make_mock_blob(name: str, records: list[dict]):
    """Create a mock blob whose open("rb") returns a line-iterable BytesIO."""
    blob = MagicMock()
    blob.name = name
    blob.open.return_value = io.BytesIO(_make_jsonl_bytes(records))
    return blob


SAMPLE_RECORDS = [{"id": i, "name": f"product_{i}", "price": float(i)} for i in range(100)]


# ── load_sample ───────────────────────────────────────────────────────

@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_load_sample_returns_dataframe(mock_get_client):
    blob = _make_mock_blob("usda/2026/04/20/part_0000.jsonl", SAMPLE_RECORDS)
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/2026/04/20/*.jsonl")
    df = loader.load_sample(n_rows=5000)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 100
    assert "id" in df.columns
    assert "name" in df.columns


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_load_sample_truncates_to_n_rows(mock_get_client):
    blob = _make_mock_blob("usda/part_0000.jsonl", SAMPLE_RECORDS)
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    df = loader.load_sample(n_rows=10)
    assert len(df) == 10


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_load_sample_only_reads_first_blob(mock_get_client):
    blob0 = _make_mock_blob("usda/part_0000.jsonl", SAMPLE_RECORDS[:50])
    blob1 = _make_mock_blob("usda/part_0001.jsonl", SAMPLE_RECORDS[50:])
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob0, blob1]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    loader.load_sample()

    blob0.open.assert_called_once()
    blob1.open.assert_not_called()


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_load_sample_empty_bucket_raises(mock_get_client):
    bucket = MagicMock()
    bucket.list_blobs.return_value = []
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    with pytest.raises(FileNotFoundError, match="No blobs matched GCS pattern"):
        loader.load_sample()


# ── iter_chunks ───────────────────────────────────────────────────────

@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_iter_chunks_yields_correct_row_count(mock_get_client):
    records = [{"id": i} for i in range(250)]
    blob = _make_mock_blob("usda/part_0000.jsonl", records)
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    chunks = list(loader.iter_chunks(chunk_size=100))

    assert len(chunks) == 3
    assert len(chunks[0]) == 100
    assert len(chunks[1]) == 100
    assert len(chunks[2]) == 50


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_iter_chunks_spans_partitions(mock_get_client):
    blob0 = _make_mock_blob("usda/part_0000.jsonl", [{"id": i} for i in range(60)])
    blob1 = _make_mock_blob("usda/part_0001.jsonl", [{"id": i} for i in range(60, 120)])
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob0, blob1]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    chunks = list(loader.iter_chunks(chunk_size=100))

    total_rows = sum(len(c) for c in chunks)
    assert total_rows == 120


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_iter_chunks_filters_by_pattern(mock_get_client):
    jsonl_blob = _make_mock_blob("usda/part_0000.jsonl", SAMPLE_RECORDS[:10])
    other_blob = _make_mock_blob("usda/part_0000.csv", SAMPLE_RECORDS[:10])
    bucket = MagicMock()
    bucket.list_blobs.return_value = [jsonl_blob, other_blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    chunks = list(loader.iter_chunks(chunk_size=100))

    assert len(chunks) == 1
    other_blob.open.assert_not_called()


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_iter_chunks_skips_empty_blobs(mock_get_client):
    empty_blob = MagicMock()
    empty_blob.name = "usda/part_0000.jsonl"
    empty_blob.open.return_value = io.BytesIO(b"")
    real_blob = _make_mock_blob("usda/part_0001.jsonl", [{"id": 1}])
    bucket = MagicMock()
    bucket.list_blobs.return_value = [empty_blob, real_blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    chunks = list(loader.iter_chunks(chunk_size=100))

    assert len(chunks) == 1
    assert len(chunks[0]) == 1


@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_iter_chunks_no_match_raises(mock_get_client):
    bucket = MagicMock()
    bucket.list_blobs.return_value = []
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    with pytest.raises(FileNotFoundError, match="No blobs matched GCS pattern"):
        list(loader.iter_chunks())


# ── Nested JSON serialization ─────────────────────────────────────────

@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_blob_to_df_serializes_nested_json(mock_get_client):
    nested_records = [
        {"id": 1, "nutrients": {"protein": 5.0, "fat": 2.0}, "tags": ["organic", "vegan"]},
        {"id": 2, "nutrients": {"protein": 3.0, "fat": 1.0}, "tags": ["gluten-free"]},
    ]
    blob = _make_mock_blob("usda/part_0000.jsonl", nested_records)
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    df = loader.load_sample()

    assert isinstance(df["nutrients"].iloc[0], str)
    parsed = json.loads(df["nutrients"].iloc[0])
    assert parsed["protein"] == 5.0

    assert isinstance(df["tags"].iloc[0], str)
    parsed_tags = json.loads(df["tags"].iloc[0])
    assert "organic" in parsed_tags


# ── Retry behavior ────────────────────────────────────────────────────

@patch("src.pipeline.loaders.gcs_loader.time.sleep")
@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_retry_succeeds_after_transient_failures(mock_get_client, mock_sleep):
    good_content = io.BytesIO(b'{"id": 1}\n')
    blob = MagicMock()
    blob.name = "usda/part_0000.jsonl"
    blob.open.side_effect = [Exception("transient 1"), Exception("transient 2"), good_content]

    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    df = loader.load_sample()

    assert len(df) == 1
    assert blob.open.call_count == 3
    assert mock_sleep.call_count == 2


@patch("src.pipeline.loaders.gcs_loader.time.sleep")
@patch("src.pipeline.loaders.gcs_loader.GCSSourceLoader._get_client")
def test_retry_raises_after_max_attempts(mock_get_client, mock_sleep):
    blob = MagicMock()
    blob.name = "usda/part_0000.jsonl"
    blob.open.side_effect = Exception("persistent error")

    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    mock_get_client.return_value.bucket.return_value = bucket

    loader = GCSSourceLoader("gs://mip-bronze-2024/usda/*.jsonl")
    with pytest.raises(Exception, match="persistent error"):
        loader.load_sample()

    assert blob.open.call_count == 3
    assert mock_sleep.call_count == 3


# ── Integration (real GCS — skipped if no credentials) ───────────────

@pytest.mark.integration
def test_integration_load_sample_from_real_gcs():
    import os
    if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        pytest.skip("GOOGLE_CLOUD_PROJECT not set — skipping real GCS test")

    loader = GCSSourceLoader(
        "gs://mip-bronze-2024/usda/2026/04/20/part_0000.jsonl",
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
    )
    df = loader.load_sample(n_rows=100)
    assert not df.empty
    assert len(df) <= 100
    assert len(df.columns) > 0
