"""Unit tests for UC2 metrics_collector."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.uc2_observability.metrics_collector import MetricsCollector


@pytest.fixture
def mock_push():
    with patch("src.uc2_observability.metrics_collector.push_to_gateway") as p:
        yield p


class TestMetricsCollectorPush:
    def test_push_all_metrics(self, mock_push):
        c = MetricsCollector()
        c.push("r1", "OFF", {
            "rows_in": 100, "rows_out": 95, "null_rate": 0.05,
            "dq_score_pre": 0.5, "dq_score_post": 0.9, "dq_delta": 0.4,
            "dedup_rate": 0.05, "s1_count": 50, "s2_count": 20,
            "s3_count": 10, "s4_count": 5, "quarantine_rows": 2,
            "block_duration_seconds": 10.5, "llm_calls": 3, "cost_usd": 0.01,
        })
        assert mock_push.called

    def test_push_partial_metrics(self, mock_push):
        c = MetricsCollector()
        c.push("r1", "USDA", {"rows_in": 50})
        assert mock_push.called

    def test_push_bad_value_warns(self, mock_push):
        c = MetricsCollector()
        # Non-numeric value triggers warning but doesn't raise
        c.push("r1", "s", {"rows_in": "not-a-number"})
        assert mock_push.called

    def test_push_raises_on_gateway_failure(self, mock_push):
        mock_push.side_effect = Exception("gateway down")
        c = MetricsCollector()
        with pytest.raises(Exception):
            c.push("r1", "s", {"rows_in": 1})

    def test_push_block_dq_success(self, mock_push):
        c = MetricsCollector()
        c.push_block_dq("r1", "OFF", "clean", 1, 0.9, 100, 50.0)
        assert mock_push.called

    def test_push_block_dq_no_duration(self, mock_push):
        c = MetricsCollector()
        c.push_block_dq("r1", "OFF", "clean", 1, 0.9, 100, None)
        assert mock_push.called

    def test_push_block_dq_swallows_errors(self, mock_push):
        mock_push.side_effect = Exception("down")
        c = MetricsCollector()
        # should not raise
        c.push_block_dq("r1", "OFF", "clean", 1, 0.9, 100)

    def test_push_anomaly_flag(self, mock_push):
        c = MetricsCollector()
        c.push_anomaly_flag("r1", "OFF", "spike")
        assert mock_push.called

    def test_push_anomaly_flag_raises(self, mock_push):
        mock_push.side_effect = Exception("down")
        c = MetricsCollector()
        with pytest.raises(Exception):
            c.push_anomaly_flag("r1", "OFF", "x")

    def test_custom_pushgateway_url(self):
        c = MetricsCollector(pushgateway_url="other:9091")
        assert c.pushgateway_url == "other:9091"
