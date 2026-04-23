"""Tests for src/uc2_observability/mcp_server.py — FastAPI MCP endpoints.

These tests use FastAPI's TestClient and patch the Postgres / Prometheus / Redis
backends so they run without external services.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("psycopg2")
pytest.importorskip("redis")
from fastapi.testclient import TestClient  # noqa: E402

from src.uc2_observability import mcp_server  # noqa: E402

client = TestClient(mcp_server.app)


# ---------------------------------------------------------------------------
# Cache key helpers
# ---------------------------------------------------------------------------


class TestCacheKey:
    def test_cache_key_deterministic(self):
        k1 = mcp_server._cache_key("a", "b", 1)
        k2 = mcp_server._cache_key("a", "b", 1)
        assert k1 == k2

    def test_cache_key_starts_with_prefix(self):
        assert mcp_server._cache_key("a").startswith("mcp:")

    def test_different_inputs_different_keys(self):
        assert mcp_server._cache_key("a") != mcp_server._cache_key("b")


class TestSerializeRows:
    def test_datetime_serialized(self):
        from datetime import datetime
        rows = [{"ts": datetime(2026, 1, 1, 12, 0, 0)}]
        out = mcp_server._serialize(rows)
        assert isinstance(out[0]["ts"], str)
        assert "2026-01-01" in out[0]["ts"]

    def test_decimal_to_float(self):
        from decimal import Decimal
        rows = [{"score": Decimal("73.27")}]
        out = mcp_server._serialize(rows)
        assert out[0]["score"] == 73.27
        assert isinstance(out[0]["score"], float)

    def test_passthrough(self):
        rows = [{"a": 1, "b": "x"}]
        assert mcp_server._serialize(rows) == rows


# ---------------------------------------------------------------------------
# Endpoints — discovery + health
# ---------------------------------------------------------------------------


class TestDiscoveryEndpoints:
    def test_list_tools(self):
        resp = client.get("/tools")
        assert resp.status_code == 200
        body = resp.json()
        assert "tools" in body
        names = [t["name"] for t in body["tools"]]
        assert "get_run_metrics" in names
        assert "get_block_trace" in names
        assert "list_runs" in names

    def test_each_tool_has_input_schema(self):
        body = client.get("/tools").json()
        for tool in body["tools"]:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool

    def test_health_endpoint(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "redis" in body


# ---------------------------------------------------------------------------
# Endpoints — get_run_metrics
# ---------------------------------------------------------------------------


class TestGetRunMetrics:
    def test_missing_run_id_returns_400(self):
        resp = client.post("/tools/get_run_metrics", json={})
        assert resp.status_code == 400

    def test_returns_flat_metric_dict(self):
        with patch.object(mcp_server, "_prom_query", return_value=([{"value": [0, "100"]}], False)):
            resp = client.post("/tools/get_run_metrics", json={"run_id": "r1"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["tool"] == "get_run_metrics"
        assert body["run_id"] == "r1"
        assert body["data"]["rows_in"] == 100.0

    def test_null_metric_when_prometheus_returns_nothing(self):
        with patch.object(mcp_server, "_prom_query", return_value=([], False)):
            resp = client.post("/tools/get_run_metrics", json={"run_id": "r1"})
        assert resp.status_code == 200
        assert resp.json()["data"]["rows_in"] is None

    def test_source_filter_propagated_to_promql(self):
        captured = []

        def fake_prom(promql):
            captured.append(promql)
            return ([], False)

        with patch.object(mcp_server, "_prom_query", side_effect=fake_prom):
            client.post("/tools/get_run_metrics", json={"run_id": "r1", "source": "OFF"})

        assert any('source="OFF"' in q for q in captured)


# ---------------------------------------------------------------------------
# Endpoints — get_block_trace
# ---------------------------------------------------------------------------


class TestGetBlockTrace:
    def test_missing_run_id_returns_400(self):
        resp = client.post("/tools/get_block_trace", json={})
        assert resp.status_code == 400

    def test_returns_rows_from_postgres(self):
        rows = [{"run_id": "r1", "source": "OFF", "block": "dq_score_pre"}]
        with patch.object(mcp_server, "_pg_query", return_value=(rows, False)):
            resp = client.post("/tools/get_block_trace", json={"run_id": "r1"})
        assert resp.status_code == 200
        assert resp.json()["data"] == rows

    def test_source_filter_uses_different_query_path(self):
        captured = []

        def fake_pg(sql, params=(), cache_ttl=30):
            captured.append((sql, params))
            return ([], False)

        with patch.object(mcp_server, "_pg_query", side_effect=fake_pg):
            client.post("/tools/get_block_trace", json={"run_id": "r1", "source": "OFF"})
        sql, params = captured[0]
        assert "source = %s" in sql
        assert "OFF" in params


# ---------------------------------------------------------------------------
# Endpoints — get_source_stats
# ---------------------------------------------------------------------------


class TestGetSourceStats:
    def test_missing_source_returns_400(self):
        resp = client.post("/tools/get_source_stats", json={})
        assert resp.status_code == 400

    def test_returns_metric_dict_keyed_by_run_id(self):
        with patch.object(mcp_server, "_prom_flat", return_value={"r1": 100.0, "r2": 200.0}):
            resp = client.post("/tools/get_source_stats", json={"source": "OFF"})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["rows_in"] == {"r1": 100.0, "r2": 200.0}


# ---------------------------------------------------------------------------
# Endpoints — get_anomalies
# ---------------------------------------------------------------------------


class TestGetAnomalies:
    def test_no_filters_works(self):
        with patch.object(mcp_server, "_pg_query", return_value=([], False)):
            resp = client.post("/tools/get_anomalies", json={})
        assert resp.status_code == 200

    def test_run_and_source_filter_combined(self):
        captured = []

        def fake_pg(sql, params=(), cache_ttl=30):
            captured.append((sql, params))
            return ([], False)

        with patch.object(mcp_server, "_pg_query", side_effect=fake_pg):
            client.post("/tools/get_anomalies", json={"run_id": "r1", "source": "OFF", "limit": 10})
        sql, params = captured[0]
        assert "run_id = %s" in sql
        assert "source = %s" in sql
        assert "r1" in params and "OFF" in params and 10 in params


# ---------------------------------------------------------------------------
# Endpoints — get_quarantine
# ---------------------------------------------------------------------------


class TestGetQuarantine:
    def test_missing_run_id_returns_400(self):
        resp = client.post("/tools/get_quarantine", json={})
        assert resp.status_code == 400

    def test_returns_quarantined_rows(self):
        rows = [{"row_hash": "abc", "reason": "Null in required field(s): product_name"}]
        with patch.object(mcp_server, "_pg_query", return_value=(rows, False)):
            resp = client.post("/tools/get_quarantine", json={"run_id": "r1"})
        assert resp.status_code == 200
        assert resp.json()["data"] == rows


# ---------------------------------------------------------------------------
# Endpoints — list_runs
# ---------------------------------------------------------------------------


class TestListRuns:
    def test_returns_unique_runs_sorted(self):
        prom_results = [
            {"metric": {"run_id": "r2", "source": "OFF"}, "value": [0, "1"]},
            {"metric": {"run_id": "r1", "source": "OFF"}, "value": [0, "1"]},
            {"metric": {"run_id": "r2", "source": "OFF"}, "value": [0, "1"]},  # dup
        ]
        with patch.object(mcp_server, "_prom_query", return_value=(prom_results, False)):
            resp = client.post("/tools/list_runs", json={})
        body = resp.json()
        assert body["data"]["count"] == 2
        # Sorted by run_id ascending — r1 first, r2 last
        run_ids = [r["run_id"] for r in body["data"]["runs"]]
        assert run_ids == ["r1", "r2"]
