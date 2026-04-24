"""Unit tests for gold_pipeline helpers."""

from __future__ import annotations

import io
import math
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.pipeline import gold_pipeline as gp


class TestSanitizeNan:
    def test_nan_to_none(self):
        assert gp._sanitize_nan(float("nan")) is None

    def test_inf_to_none(self):
        assert gp._sanitize_nan(float("inf")) is None

    def test_int_passthrough(self):
        assert gp._sanitize_nan(5) == 5

    def test_str_passthrough(self):
        assert gp._sanitize_nan("x") == "x"

    def test_dict_recurses(self):
        assert gp._sanitize_nan({"a": float("nan"), "b": 1}) == {"a": None, "b": 1}

    def test_list_recurses(self):
        assert gp._sanitize_nan([float("nan"), 1, "x"]) == [None, 1, "x"]

    def test_nested(self):
        out = gp._sanitize_nan({"x": [float("nan"), {"y": float("inf")}]})
        assert out == {"x": [None, {"y": None}]}


class TestValidateSchema:
    def test_missing_required_raises(self):
        df = pd.DataFrame({"foo": [1]})
        with pytest.raises(ValueError):
            gp._validate_silver_schema(df, "off")

    def test_required_only_ok(self, caplog):
        df = pd.DataFrame({"product_name": ["x"]})
        gp._validate_silver_schema(df, "off")  # no raise

    def test_full_columns(self):
        df = pd.DataFrame({
            "product_name": ["x"],
            "brand_name": ["y"],
            "ingredients": [""],
            "dq_score_pre": [0.5],
            "source_name": ["off"],
        })
        gp._validate_silver_schema(df, "off")


class TestReadSilverParquet:
    def test_no_blobs_raises(self):
        client = MagicMock()
        bucket = MagicMock()
        client.bucket.return_value = bucket
        bucket.list_blobs.return_value = []
        with patch.object(gp, "_gcs_client", return_value=client):
            with pytest.raises(FileNotFoundError):
                gp._read_silver_parquet("off", "2026/04/21")

    def test_loads_and_concats(self):
        df = pd.DataFrame({"product_name": ["a", "b"]})
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
        raw = buf.getvalue()

        blob = MagicMock()
        blob.name = "off/2026/04/21/part_0000.parquet"
        blob.download_as_bytes.return_value = raw
        client = MagicMock()
        bucket = MagicMock()
        client.bucket.return_value = bucket
        bucket.list_blobs.return_value = [blob]
        with patch.object(gp, "_gcs_client", return_value=client):
            out = gp._read_silver_parquet("off", "2026/04/21")
        assert len(out) == 2

    def test_skips_sample_blobs(self):
        blob = MagicMock()
        blob.name = "off/2026/04/21/sample_part.parquet"
        client = MagicMock()
        bucket = MagicMock()
        client.bucket.return_value = bucket
        bucket.list_blobs.return_value = [blob]
        with patch.object(gp, "_gcs_client", return_value=client):
            with pytest.raises(FileNotFoundError):
                gp._read_silver_parquet("off", "2026/04/21")


class TestBuildRunLog:
    def test_basic(self):
        df = pd.DataFrame({
            "dq_score_pre": [0.5, 0.6],
            "dq_score_post": [0.9, 0.95],
        })
        log = gp._build_gold_run_log(
            run_id="r1", source_name="off", domain="nutrition",
            rows_in=10, result_df=df, audit_log=[{"b": "x"}],
            duration_seconds=1.234, status="success",
        )
        assert log["run_id"] == "r1"
        assert log["status"] == "success"
        assert log["rows_out"] == 2
        assert log["dq_score_pre"] is not None
        assert log["dq_delta"] is not None

    def test_no_dq_columns(self):
        df = pd.DataFrame({"product_name": ["x"]})
        log = gp._build_gold_run_log(
            run_id="r", source_name="s", domain="d",
            rows_in=1, result_df=df, audit_log=[], duration_seconds=0.1,
        )
        assert log["dq_score_pre"] is None
        assert log["dq_delta"] is None

    def test_error_status(self):
        df = pd.DataFrame({"product_name": []})
        log = gp._build_gold_run_log(
            run_id="r", source_name="s", domain="d",
            rows_in=0, result_df=df, audit_log=[], duration_seconds=0.0,
            status="failed", error="boom",
        )
        assert log["status"] == "failed"
        assert log["error"] == "boom"


class TestSaveGoldRunLog:
    def test_writes_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gp, "PROJECT_ROOT", tmp_path)
        log = {"run_id": "abcdef12345678ff", "some": "data"}
        path = gp._save_gold_run_log(log)
        assert path is not None
        assert path.exists()
        assert path.suffix == ".json"

    def test_swallows_errors(self, monkeypatch):
        monkeypatch.setattr(gp, "PROJECT_ROOT", None)  # triggers failure
        result = gp._save_gold_run_log({"run_id": "x"})
        assert result is None


class TestPushGoldMetrics:
    def test_swallows_all_errors(self):
        gp._push_gold_metrics({"run_id": "x", "source_name": "off"})  # no raise


class TestPushGoldAudit:
    def test_swallows_psycopg2_errors(self):
        with patch.dict("sys.modules", {"psycopg2": MagicMock()}):
            gp._push_gold_audit({"run_id": "x", "source_name": "off", "status": "ok"})


class TestReadDomainFromBq:
    def test_empty_raises(self):
        fake_client = MagicMock()
        fake_query = MagicMock()
        fake_query.to_dataframe.return_value = pd.DataFrame()
        fake_client.query.return_value = fake_query
        with patch("google.cloud.bigquery.Client", return_value=fake_client):
            with pytest.raises(ValueError):
                gp._read_domain_from_bq(["off"])

    def test_success(self):
        fake_client = MagicMock()
        fake_query = MagicMock()
        fake_query.to_dataframe.return_value = pd.DataFrame({"product_name": ["x"]})
        fake_client.query.return_value = fake_query
        with patch("google.cloud.bigquery.Client", return_value=fake_client):
            df = gp._read_domain_from_bq(["off", "usda"])
        assert len(df) == 1


class TestEnrichWithSafetySignals:
    def test_nonfatal_failure(self):
        df = pd.DataFrame({"product_name": ["x"], "brand_name": ["y"], "allergens": [None]})
        with patch.object(gp, "_gcs_client", side_effect=RuntimeError("down")):
            out = gp._enrich_with_safety_signals(df, "2026/04/21")
        assert "is_recalled" in out.columns
        assert bool(out["is_recalled"].iloc[0]) is False

    def test_no_blobs(self):
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = []
        client.bucket.return_value = bucket
        df = pd.DataFrame({"product_name": ["x"], "brand_name": ["y"], "allergens": [None]})
        with patch.object(gp, "_gcs_client", return_value=client):
            out = gp._enrich_with_safety_signals(df, "2026/04/21")
        assert "is_recalled" in out.columns
        assert bool(out["is_recalled"].iloc[0]) is False


class TestWriteGoldBq:
    def test_write_invokes_bq_load(self):
        df = pd.DataFrame({
            "product_name": ["a", "b"],
            "brand_name": ["x", None],
            "published_date": ["2024-01-01", None],
        })
        fake_client = MagicMock()
        fake_job = MagicMock()
        fake_client.load_table_from_dataframe.return_value = fake_job
        with patch("google.cloud.bigquery.Client", return_value=fake_client):
            rows = gp._write_gold_bq(df, "off")
        assert rows == 2
        fake_client.load_table_from_dataframe.assert_called_once()

    def test_write_handles_list_cells(self):
        df = pd.DataFrame({
            "product_name": ["a"],
            "brand_name": [["BrandA"]],
            "published_date": [["2024-01-01"]],
        })
        fake_client = MagicMock()
        with patch("google.cloud.bigquery.Client", return_value=fake_client):
            rows = gp._write_gold_bq(df, "off")
        assert rows == 1
