"""Unit tests for GCSSilverWriter and GCSGoldWriter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.writers.gcs_silver_writer import GCSSilverWriter, _with_retry as _silver_retry
from src.pipeline.writers.gcs_gold_writer import GCSGoldWriter, _with_retry as _gold_retry


@pytest.fixture
def df():
    return pd.DataFrame({"product_name": ["a", "b"], "brand_name": ["x", "y"]})


@pytest.fixture
def mock_gcs():
    client = MagicMock()
    bucket = MagicMock()
    blob = MagicMock()
    client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    return client, bucket, blob


class TestSilverWriter:
    def test_write_uses_default_date(self, df, mock_gcs):
        client, bucket, blob = mock_gcs
        with patch("src.pipeline.writers.gcs_silver_writer._gcs_client", return_value=client):
            uri = GCSSilverWriter().write(df, "off", chunk_idx=0)
        assert uri.startswith("gs://mip-silver-2024/off/")
        assert uri.endswith("part_0000.parquet")
        blob.upload_from_file.assert_called_once()

    def test_write_with_date(self, df, mock_gcs):
        client, bucket, blob = mock_gcs
        with patch("src.pipeline.writers.gcs_silver_writer._gcs_client", return_value=client):
            uri = GCSSilverWriter().write(df, "usda", date="2026/04/21", chunk_idx=3)
        assert "usda/2026/04/21/part_0003.parquet" in uri

    def test_read_watermark_success(self, mock_gcs):
        client, bucket, blob = mock_gcs
        blob.download_as_bytes.return_value = b'{"last_partition": "2026/04/20"}'
        with patch("src.pipeline.writers.gcs_silver_writer._gcs_client", return_value=client):
            result = GCSSilverWriter().read_watermark("off")
        assert result == "2026/04/20"

    def test_read_watermark_missing_returns_none(self, mock_gcs):
        client, bucket, blob = mock_gcs
        blob.download_as_bytes.side_effect = Exception("404")
        with patch("src.pipeline.writers.gcs_silver_writer._gcs_client", return_value=client):
            assert GCSSilverWriter().read_watermark("off") is None

    def test_update_watermark(self, mock_gcs):
        client, bucket, blob = mock_gcs
        with patch("src.pipeline.writers.gcs_silver_writer._gcs_client", return_value=client):
            GCSSilverWriter().update_watermark("off", "2026/04/21")
        blob.upload_from_string.assert_called_once()


class TestSilverRetry:
    def test_retry_succeeds_first_try(self):
        fn = MagicMock(return_value="ok")
        assert _silver_retry(fn) == "ok"
        assert fn.call_count == 1

    def test_retry_eventually_raises(self):
        fn = MagicMock(side_effect=RuntimeError("boom"))
        with patch("time.sleep"):
            with pytest.raises(RuntimeError):
                _silver_retry(fn)
        assert fn.call_count == 3

    def test_retry_succeeds_second_try(self):
        fn = MagicMock(side_effect=[RuntimeError("x"), "ok"])
        with patch("time.sleep"):
            assert _silver_retry(fn) == "ok"


class TestGoldWriter:
    def test_write_default_date(self, df, mock_gcs):
        client, bucket, blob = mock_gcs
        with patch("src.pipeline.writers.gcs_gold_writer._gcs_client", return_value=client):
            uri = GCSGoldWriter().write(df, "nutrition")
        assert uri.startswith("gs://mip-gold-2024/nutrition/")
        blob.upload_from_file.assert_called_once()

    def test_write_with_date_and_chunk(self, df, mock_gcs):
        client, bucket, blob = mock_gcs
        with patch("src.pipeline.writers.gcs_gold_writer._gcs_client", return_value=client):
            uri = GCSGoldWriter().write(df, "safety", date="2026/04/21", chunk_idx=7)
        assert "safety/2026/04/21/part_0007.parquet" in uri


class TestGoldRetry:
    def test_gold_retry_success(self):
        fn = MagicMock(return_value=42)
        assert _gold_retry(fn) == 42

    def test_gold_retry_exhausts(self):
        fn = MagicMock(side_effect=Exception("fail"))
        with patch("time.sleep"):
            with pytest.raises(Exception):
                _gold_retry(fn)
        assert fn.call_count == 3
