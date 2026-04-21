"""Tests for MetricsExporter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.uc2_observability.metrics_exporter import MetricsExporter


_FIXTURE_LOG = {
    "run_id": "aaaaaaaa-0000-0000-0000-000000000001",
    "source_name": "usda_fooddata_sample",
    "status": "success",
    "dq_score_pre": 0.82,
    "dq_score_post": 0.91,
    "dq_delta": 0.09,
    "rows_in": 1000,
    "rows_out": 987,
    "rows_quarantined": 13,
    "duration_seconds": 45.3,
    "enrichment_stats": {
        "deterministic": 200,
        "embedding": 50,
        "llm": 30,
        "unresolved": 20,
    },
}


class TestPush:
    def test_returns_true_on_success(self) -> None:
        exporter = MetricsExporter()
        with patch("src.uc2_observability.metrics_exporter.push_to_gateway") as mock_push, \
             patch("src.uc2_observability.metrics_exporter.CollectorRegistry"), \
             patch("src.uc2_observability.metrics_exporter.Gauge"):
            mock_push.return_value = None
            # Import inside to trigger real code path
            result = exporter.push(_FIXTURE_LOG)
        assert result is True

    def test_returns_false_on_connection_error(self) -> None:
        exporter = MetricsExporter()
        with patch("src.uc2_observability.metrics_exporter.push_to_gateway",
                   side_effect=ConnectionError("refused")), \
             patch("src.uc2_observability.metrics_exporter.CollectorRegistry"), \
             patch("src.uc2_observability.metrics_exporter.Gauge"):
            result = exporter.push(_FIXTURE_LOG)
        assert result is False

    def test_never_raises(self) -> None:
        exporter = MetricsExporter()
        with patch("src.uc2_observability.metrics_exporter.push_to_gateway",
                   side_effect=RuntimeError("boom")), \
             patch("src.uc2_observability.metrics_exporter.CollectorRegistry"), \
             patch("src.uc2_observability.metrics_exporter.Gauge"):
            result = exporter.push(_FIXTURE_LOG)
        assert result is False

    def test_correct_metric_names_and_labels(self) -> None:
        exporter = MetricsExporter()
        created_gauges: list[tuple] = []

        class FakeGauge:
            def __init__(self, name: str, doc: str, labels: list, registry=None):
                self.name = name
                self._labels = labels

            def labels(self, *args):
                self._label_vals = args
                return self

            def set(self, value: float):
                created_gauges.append((self.name, self._labels, self._label_vals, value))

        with patch("src.uc2_observability.metrics_exporter.CollectorRegistry"), \
             patch("src.uc2_observability.metrics_exporter.Gauge", FakeGauge), \
             patch("src.uc2_observability.metrics_exporter.push_to_gateway"):
            exporter.push(_FIXTURE_LOG)

        names = [g[0] for g in created_gauges]
        assert "etl_dq_score_pre" in names
        assert "etl_dq_score_post" in names
        assert "etl_dq_delta" in names
        assert "etl_rows_in" in names
        assert "etl_rows_out" in names
        assert "etl_rows_quarantined" in names
        assert "etl_duration_seconds" in names
        assert "etl_enrichment_s1_resolved" in names
        assert "etl_enrichment_s2_resolved" in names
        assert "etl_enrichment_s3_resolved" in names
        assert "etl_enrichment_unresolved" in names
        assert "etl_run_status" in names

        dq_pre = next(g for g in created_gauges if g[0] == "etl_dq_score_pre")
        assert "usda_fooddata_sample" in dq_pre[2]
        assert "aaaaaaaa-0000-0000-0000-000000000001" in dq_pre[2]
