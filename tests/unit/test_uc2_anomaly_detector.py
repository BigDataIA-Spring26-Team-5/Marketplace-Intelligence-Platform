"""Unit tests for UC2 anomaly_detector."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.uc2_observability import anomaly_detector as ad


# ---------------------------------------------------------------------------
# _prom_query_all_runs / _prom_instant
# ---------------------------------------------------------------------------

class TestPromHelpers:
    def test_query_all_runs_success(self):
        resp = MagicMock()
        resp.json.return_value = {
            "status": "success",
            "data": {"result": [
                {"metric": {"run_id": "r1"}, "value": [0, "0.5"]},
                {"metric": {"run_id": "r2"}, "value": [0, "0.7"]},
            ]},
        }
        resp.raise_for_status = MagicMock()
        with patch.object(ad.requests, "get", return_value=resp):
            out = ad._prom_query_all_runs("query")
        assert out == {"r1": 0.5, "r2": 0.7}

    def test_query_all_runs_failure(self):
        with patch.object(ad.requests, "get", side_effect=Exception("down")):
            assert ad._prom_query_all_runs("q") == {}

    def test_query_all_runs_nonsuccess(self):
        resp = MagicMock()
        resp.json.return_value = {"status": "error"}
        resp.raise_for_status = MagicMock()
        with patch.object(ad.requests, "get", return_value=resp):
            assert ad._prom_query_all_runs("q") == {}

    def test_query_all_runs_skips_nan(self):
        resp = MagicMock()
        resp.json.return_value = {
            "status": "success",
            "data": {"result": [
                {"metric": {"run_id": "r1"}, "value": [0, "NaN"]},
            ]},
        }
        resp.raise_for_status = MagicMock()
        with patch.object(ad.requests, "get", return_value=resp):
            assert ad._prom_query_all_runs("q") == {}

    def test_prom_instant_success(self):
        resp = MagicMock()
        resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [0, "3.14"]}]},
        }
        resp.raise_for_status = MagicMock()
        with patch.object(ad.requests, "get", return_value=resp):
            assert ad._prom_instant("q") == 3.14

    def test_prom_instant_empty(self):
        resp = MagicMock()
        resp.json.return_value = {"status": "success", "data": {"result": []}}
        resp.raise_for_status = MagicMock()
        with patch.object(ad.requests, "get", return_value=resp):
            assert ad._prom_instant("q") is None

    def test_prom_instant_failure(self):
        with patch.object(ad.requests, "get", side_effect=Exception("x")):
            assert ad._prom_instant("q") is None


# ---------------------------------------------------------------------------
# _build_feature_matrix
# ---------------------------------------------------------------------------

class TestBuildFeatureMatrix:
    def test_empty_when_no_data(self):
        with patch.object(ad, "_prom_query_all_runs", return_value={}):
            m, names, rids = ad._build_feature_matrix("OFF", 10)
        assert m.shape[0] == 0
        assert rids == []
        assert len(names) == 5

    def test_builds_matrix(self):
        def fake(q):
            return {"r1": 0.1, "r2": 0.2, "r3": 0.3}
        with patch.object(ad, "_prom_query_all_runs", side_effect=fake):
            m, names, rids = ad._build_feature_matrix("OFF", 10)
        assert m.shape == (3, 5)
        assert rids == ["r1", "r2", "r3"]

    def test_nan_imputation(self):
        calls = [{"r1": 0.1, "r2": 0.3}, {"r1": 1.0}, {}, {}, {}]
        it = iter(calls)
        with patch.object(ad, "_prom_query_all_runs", side_effect=lambda q: next(it)):
            m, _, _ = ad._build_feature_matrix("s", 10)
        assert not np.any(np.isnan(m))


# ---------------------------------------------------------------------------
# AnomalyDetector
# ---------------------------------------------------------------------------

@pytest.fixture
def detector():
    with patch.object(ad, "MetricsCollector"):
        return ad.AnomalyDetector()


class TestAnomalyDetector:
    def test_init_sets_contamination(self, detector):
        assert detector.contamination == ad.CONTAMINATION

    def test_run_detection_insufficient_data(self, detector):
        with patch.object(ad, "_build_feature_matrix",
                          return_value=(np.empty((0, 5)), ["a"], [])):
            assert detector.run_detection("OFF") == []

    def test_run_detection_few_samples(self, detector):
        m = np.zeros((3, 5))
        with patch.object(ad, "_build_feature_matrix",
                          return_value=(m, ["a", "b", "c", "d", "e"], ["r1", "r2", "r3"])):
            assert detector.run_detection("OFF") == []

    def test_run_detection_with_outlier(self, detector):
        rows = np.array([
            [0.1, 0.9, 0.01, 5000, 0.001],
            [0.12, 0.88, 0.02, 5100, 0.002],
            [0.11, 0.91, 0.015, 4900, 0.0015],
            [0.13, 0.89, 0.018, 5050, 0.0018],
            [0.14, 0.87, 0.017, 5020, 0.0021],
            [9.0, 0.1, 0.99, 10, 99.0],  # outlier
        ])
        names = ["null_rate", "dq_score", "dedup_rate", "rows_out", "cost_usd"]
        rids = [f"r{i}" for i in range(6)]
        with patch.object(ad, "_build_feature_matrix", return_value=(rows, names, rids)), \
             patch.object(detector, "_insert_anomaly_report") as ins:
            reports = detector.run_detection("OFF")
        assert len(reports) >= 1
        assert ins.called

    def test_identify_signals_z_spike(self, detector):
        baseline = np.array([[1.0, 1.0], [1.1, 1.0], [0.9, 1.0], [1.0, 1.0]])
        values = np.array([10.0, 1.0])
        signals = detector._identify_signals(["a", "b"], values, baseline)
        assert any("a_spike" in s for s in signals)

    def test_identify_signals_none(self, detector):
        baseline = np.array([[1.0], [1.0], [1.1], [0.9]])
        values = np.array([1.0])
        signals = detector._identify_signals(["a"], values, baseline)
        assert signals == ["general_outlier"]

    def test_insert_anomaly_report_success(self, detector):
        mock_conn = MagicMock()
        with patch.object(ad.psycopg2, "connect", return_value=mock_conn):
            detector._insert_anomaly_report(
                run_id="r1", source="OFF", anomaly_score=-0.5,
                feature_names=["f1"], feature_values=[1.0],
                flagged_signals=["x"],
            )
        assert mock_conn.commit.called
        assert mock_conn.close.called

    def test_insert_anomaly_report_pg_error(self, detector):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = ad.psycopg2.Error("boom")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        with patch.object(ad.psycopg2, "connect", return_value=mock_conn):
            detector._insert_anomaly_report(
                run_id="r1", source="OFF", anomaly_score=-0.5,
                feature_names=["f1"], feature_values=[1.0],
                flagged_signals=[],
            )
        assert mock_conn.rollback.called
