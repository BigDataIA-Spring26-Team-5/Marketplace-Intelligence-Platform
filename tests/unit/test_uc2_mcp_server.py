"""Unit tests for UC2 mcp_server."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.uc2_observability import mcp_server as ms


class TestCacheHelpers:
    def test_cache_get_no_redis(self, monkeypatch):
        monkeypatch.setattr(ms, "_redis_client", None)
        assert ms._cache_get("k") is None

    def test_cache_get_hit(self, monkeypatch):
        r = MagicMock()
        r.get.return_value = '{"a":1}'
        monkeypatch.setattr(ms, "_redis_client", r)
        assert ms._cache_get("k") == {"a": 1}

    def test_cache_get_miss(self, monkeypatch):
        r = MagicMock()
        r.get.return_value = None
        monkeypatch.setattr(ms, "_redis_client", r)
        assert ms._cache_get("k") is None

    def test_cache_get_exception(self, monkeypatch):
        r = MagicMock()
        r.get.side_effect = Exception("boom")
        monkeypatch.setattr(ms, "_redis_client", r)
        assert ms._cache_get("k") is None

    def test_cache_set_no_redis(self, monkeypatch):
        monkeypatch.setattr(ms, "_redis_client", None)
        ms._cache_set("k", {"a": 1}, 10)  # no raise

    def test_cache_set_ok(self, monkeypatch):
        r = MagicMock()
        monkeypatch.setattr(ms, "_redis_client", r)
        ms._cache_set("k", {"a": 1}, 10)
        assert r.setex.called

    def test_cache_set_exception_swallowed(self, monkeypatch):
        r = MagicMock()
        r.setex.side_effect = Exception("x")
        monkeypatch.setattr(ms, "_redis_client", r)
        ms._cache_set("k", {}, 10)

    def test_cache_key_format(self):
        k = ms._cache_key("prom", "etl_rows_in")
        assert k.startswith("mcp:")


class TestSerialize:
    def test_serialize_dates_and_decimal(self):
        import datetime, decimal
        rows = [{"a": datetime.datetime(2024, 1, 1),
                 "b": decimal.Decimal("1.5"), "c": "ok"}]
        out = ms._serialize(rows)
        assert out[0]["b"] == 1.5
        assert "2024" in out[0]["a"]
        assert out[0]["c"] == "ok"


class TestPromQuery:
    def test_prom_query_cache_hit(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: [{"metric": {}, "value": [0, "1"]}])
        results, cached = ms._prom_query("etl_x")
        assert cached is True
        assert results

    def test_prom_query_fetch(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: None)
        monkeypatch.setattr(ms, "_cache_set", lambda *a, **kw: None)
        resp = MagicMock()
        resp.json.return_value = {"status": "success",
                                   "data": {"result": [{"value": [0, "2"]}]}}
        resp.raise_for_status = MagicMock()
        with patch.object(ms.requests, "get", return_value=resp):
            results, cached = ms._prom_query("q")
        assert not cached
        assert results

    def test_prom_query_error(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: None)
        with patch.object(ms.requests, "get", side_effect=Exception("x")):
            results, cached = ms._prom_query("q")
        assert results == []

    def test_prom_query_non_success(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: None)
        resp = MagicMock()
        resp.json.return_value = {"status": "error"}
        resp.raise_for_status = MagicMock()
        with patch.object(ms.requests, "get", return_value=resp):
            results, cached = ms._prom_query("q")
        assert results == []

    def test_prom_flat(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_query", lambda q: (
            [{"metric": {"run_id": "r1"}, "value": [0, "1.5"]},
             {"metric": {"run_id": "r2"}, "value": [0, "NaN"]}], False))
        flat = ms._prom_flat("q")
        assert flat == {"r1": 1.5}

    def test_prom_flat_empty(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_query", lambda q: ([], False))
        assert ms._prom_flat("q") == {}


class TestPgQuery:
    def test_pg_query_cache_hit(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: [{"a": 1}])
        rows, cached = ms._pg_query("SELECT 1")
        assert cached is True
        assert rows == [{"a": 1}]

    def test_pg_query_fetch(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: None)
        monkeypatch.setattr(ms, "_cache_set", lambda *a, **kw: None)
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [{"a": 1}]
        conn.cursor.return_value.__enter__.return_value = cur
        with patch.object(ms.psycopg2, "connect", return_value=conn):
            rows, cached = ms._pg_query("SELECT 1")
        assert rows == [{"a": 1}]
        assert cached is False

    def test_pg_query_error(self, monkeypatch):
        monkeypatch.setattr(ms, "_cache_get", lambda k: None)
        with patch.object(ms.psycopg2, "connect", side_effect=ms.psycopg2.Error("x")):
            with pytest.raises(Exception):
                ms._pg_query("SELECT 1")


class TestToolEndpoints:
    def test_get_run_metrics_requires_run_id(self):
        with pytest.raises(Exception):
            ms.get_run_metrics(ms.ToolInput())

    def test_get_run_metrics_ok(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_query", lambda q: ([{"value": [0, "5"]}], False))
        result = ms.get_run_metrics(ms.ToolInput(run_id="r1", source="OFF"))
        assert result.tool == "get_run_metrics"
        assert result.data["rows_in"] == 5.0

    def test_get_block_trace_requires_run_id(self):
        with pytest.raises(Exception):
            ms.get_block_trace(ms.ToolInput())

    def test_get_block_trace_with_source(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([{"block": "b"}], False))
        r = ms.get_block_trace(ms.ToolInput(run_id="r1", source="OFF"))
        assert r.data == [{"block": "b"}]

    def test_get_block_trace_no_source(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([], True))
        r = ms.get_block_trace(ms.ToolInput(run_id="r1"))
        assert r.cached is True

    def test_get_source_stats_requires_source(self):
        with pytest.raises(Exception):
            ms.get_source_stats(ms.ToolInput())

    def test_get_source_stats_ok(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_flat", lambda q: {"r1": 0.9})
        r = ms.get_source_stats(ms.ToolInput(source="OFF", run_id="r1"))
        assert "dq_score_post" in r.data

    def test_get_anomalies(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([{"signal": "s"}], False))
        r = ms.get_anomalies(ms.ToolInput(run_id="r1", source="OFF"))
        assert r.data == [{"signal": "s"}]

    def test_get_anomalies_no_filters(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([], False))
        r = ms.get_anomalies(ms.ToolInput())
        assert r.data == []

    def test_get_cost_report(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_flat", lambda q: {"r1": 0.1})
        r = ms.get_cost_report(ms.ToolInput(run_id="r1", source="OFF"))
        assert "cost_usd" in r.data

    def test_get_cost_report_no_filters(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_flat", lambda q: {})
        r = ms.get_cost_report(ms.ToolInput())
        assert "llm_calls" in r.data

    def test_get_quarantine_requires_run_id(self):
        with pytest.raises(Exception):
            ms.get_quarantine(ms.ToolInput())

    def test_get_quarantine_ok(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([{"row_hash": "h"}], False))
        r = ms.get_quarantine(ms.ToolInput(run_id="r1", source="OFF"))
        assert r.data == [{"row_hash": "h"}]

    def test_get_dedup_stats(self, monkeypatch):
        monkeypatch.setattr(ms, "_pg_query", lambda *a, **kw: ([{"cluster_id": "c"}], False))
        r = ms.get_dedup_stats(ms.ToolInput(run_id="r1"))
        assert r.data == [{"cluster_id": "c"}]

    def test_list_runs(self, monkeypatch):
        monkeypatch.setattr(ms, "_prom_query", lambda q: (
            [{"metric": {"run_id": "r1", "source": "OFF"}},
             {"metric": {"run_id": "r2", "source": "USDA"}}], False))
        r = ms.list_runs(ms.ToolInput(source="OFF"))
        assert r.data["count"] == 2

    def test_list_tools(self):
        out = ms.list_tools()
        assert "tools" in out
        assert len(out["tools"]) == 8

    def test_health_no_redis(self, monkeypatch):
        monkeypatch.setattr(ms, "_redis_client", None)
        h = ms.health()
        assert h["redis"] == "unavailable"
