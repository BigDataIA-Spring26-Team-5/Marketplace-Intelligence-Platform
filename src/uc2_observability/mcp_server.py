"""
UC2 Observability Layer — MCP Server

FastAPI app exposing 7 MCP-style tool endpoints backed by:
  - Prometheus  (metrics queries)
  - Postgres    (structured event log)
  - Redis       (query cache — 15s TTL for Prometheus, 30s for Postgres)

Run with:
    uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001
"""

from __future__ import annotations

import decimal
import hashlib
import json
import logging
import time
from datetime import datetime, date
from typing import Any

import psycopg2
import psycopg2.extras
import redis
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

PG_DSN         = "host=localhost port=5432 dbname=uc2 user=mip password=mip_pass"
PROMETHEUS_URL = "http://localhost:9090"
REDIS_HOST     = "localhost"
REDIS_PORT     = 6379

PROM_CACHE_TTL = 15    # seconds — matches Prometheus scrape interval
PG_CACHE_TTL   = 30    # seconds — block_trace / quarantine / dedup
ANOMALY_TTL    = 300   # seconds — anomaly reports change infrequently

# ── Redis client (graceful degradation if Redis is down) ──────────────────────

def _get_redis() -> redis.Redis | None:
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=2)
        r.ping()
        return r
    except Exception:
        return None

_redis_client: redis.Redis | None = _get_redis()


def _cache_get(key: str) -> Any | None:
    if not _redis_client:
        return None
    try:
        raw = _redis_client.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _cache_set(key: str, value: Any, ttl: int) -> None:
    if not _redis_client:
        return
    try:
        _redis_client.setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        pass


def _cache_key(*parts: Any) -> str:
    raw = ":".join(str(p) for p in parts)
    return "mcp:" + hashlib.md5(raw.encode()).hexdigest()[:16] + ":" + raw[:80]


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="UC2 MCP Server",
    description="Model Context Protocol tool endpoints for UC2 Observability",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── request / response models ──────────────────────────────────────────────────

class ToolInput(BaseModel):
    run_id: str | None = None
    source: str | None = None
    limit:  int | None = 100


class ToolResult(BaseModel):
    tool:   str
    run_id: str | None
    source: str | None
    data:   Any
    cached: bool = False
    error:  str | None = None


# ── shared helpers ─────────────────────────────────────────────────────────────

def _serialize(rows: list[dict]) -> list[dict]:
    def _fix(v: Any) -> Any:
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, decimal.Decimal):
            return float(v)
        return v
    return [{k: _fix(v) for k, v in row.items()} for row in rows]


def _pg_query(sql: str, params: tuple = (), cache_ttl: int = PG_CACHE_TTL) -> tuple[list[dict], bool]:
    """Execute SELECT, return (rows, from_cache)."""
    key = _cache_key("pg", sql.strip()[:60], str(params))
    cached = _cache_get(key)
    if cached is not None:
        return cached, True

    try:
        conn = psycopg2.connect(PG_DSN)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = _serialize([dict(r) for r in cur.fetchall()])
        finally:
            conn.close()
    except psycopg2.Error as exc:
        logger.error("Postgres error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Postgres error: {exc}")

    _cache_set(key, rows, cache_ttl)
    return rows, False


def _prom_query(promql: str) -> tuple[list[dict], bool]:
    """Instant Prometheus query, return (results, from_cache)."""
    key = _cache_key("prom", promql)
    cached = _cache_get(key)
    if cached is not None:
        return cached, True

    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return [], False
        results = data.get("data", {}).get("result", [])
    except Exception as exc:
        logger.warning("Prometheus query failed (%r): %s", promql, exc)
        return [], False

    _cache_set(key, results, PROM_CACHE_TTL)
    return results, False


def _prom_flat(promql: str) -> dict[str, Any]:
    """
    Run a Prometheus instant query and return a flat dict:
      {run_id: value, ...}  keyed by run_id label (or "value" if only one series).
    Claude reads this much more naturally than nested {labels, value} arrays.
    """
    results, _ = _prom_query(promql)
    if not results:
        return {}
    out = {}
    for series in results:
        run_id = series.get("metric", {}).get("run_id", "latest")
        val    = series.get("value", [None, None])[1]
        if val is not None and val != "NaN":
            out[run_id] = float(val)
    return out


# ── tool 1: get_run_metrics ────────────────────────────────────────────────────

@app.post("/tools/get_run_metrics", response_model=ToolResult)
def get_run_metrics(body: ToolInput) -> ToolResult:
    if not body.run_id:
        raise HTTPException(status_code=400, detail="run_id is required")

    rid = body.run_id
    src_filter = f', source="{body.source}"' if body.source else ""

    metrics = {
        "rows_in":       f'uc1_rows_in{{run_id="{rid}"{src_filter}}}',
        "rows_out":      f'uc1_rows_out{{run_id="{rid}"{src_filter}}}',
        "null_rate":     f'uc1_null_rate{{run_id="{rid}"{src_filter}}}',
        "dq_score_pre":  f'uc1_dq_score_pre{{run_id="{rid}"{src_filter}}}',
        "dq_score_post": f'uc1_dq_score_post{{run_id="{rid}"{src_filter}}}',
        "dq_delta":      f'uc1_dq_delta{{run_id="{rid}"{src_filter}}}',
        "dedup_rate":    f'uc1_dedup_rate{{run_id="{rid}"{src_filter}}}',
        "llm_calls":     f'uc1_llm_calls_total{{run_id="{rid}"{src_filter}}}',
        "cost_usd":      f'uc1_llm_cost_usd_total{{run_id="{rid}"{src_filter}}}',
        "s1_count":      f'uc1_s1_count{{run_id="{rid}"{src_filter}}}',
        "s2_count":      f'uc1_s2_count{{run_id="{rid}"{src_filter}}}',
        "s3_count":      f'uc1_s3_count{{run_id="{rid}"{src_filter}}}',
        "s4_count":      f'uc1_s4_count{{run_id="{rid}"{src_filter}}}',
        "quarantine":    f'uc1_quarantine_rows{{run_id="{rid}"{src_filter}}}',
        "block_duration_s": f'uc1_block_duration_seconds{{run_id="{rid}"{src_filter}}}',
    }

    # Return flat {metric_name: value} — Claude reads this directly
    data: dict[str, Any] = {}
    any_cached = False
    for name, pql in metrics.items():
        results, from_cache = _prom_query(pql)
        any_cached = any_cached or from_cache
        if results:
            val = results[0].get("value", [None, None])[1]
            data[name] = float(val) if val not in (None, "NaN") else None
        else:
            data[name] = None

    data["run_id"] = rid
    data["_note"]  = "null means metric not yet pushed for this run_id"

    return ToolResult(tool="get_run_metrics", run_id=rid, source=body.source,
                      data=data, cached=any_cached)


# ── tool 2: get_block_trace ────────────────────────────────────────────────────

@app.post("/tools/get_block_trace", response_model=ToolResult)
def get_block_trace(body: ToolInput) -> ToolResult:
    if not body.run_id:
        raise HTTPException(status_code=400, detail="run_id is required")

    if body.source:
        rows, cached = _pg_query(
            """
            SELECT run_id, source, block,
                   rows_in, rows_out, null_rates, duration_ms, ts
            FROM   block_trace
            WHERE  run_id = %s AND source = %s
            ORDER  BY ts ASC
            LIMIT  %s
            """,
            (body.run_id, body.source, body.limit or 500),
        )
    else:
        rows, cached = _pg_query(
            """
            SELECT run_id, source, block,
                   rows_in, rows_out, null_rates, duration_ms, ts
            FROM   block_trace
            WHERE  run_id = %s
            ORDER  BY ts ASC
            LIMIT  %s
            """,
            (body.run_id, body.limit or 500),
        )

    return ToolResult(tool="get_block_trace", run_id=body.run_id,
                      source=body.source, data=rows, cached=cached)


# ── tool 3: get_source_stats ───────────────────────────────────────────────────

@app.post("/tools/get_source_stats", response_model=ToolResult)
def get_source_stats(body: ToolInput) -> ToolResult:
    if not body.source:
        raise HTTPException(status_code=400, detail="source is required")

    src = body.source
    run_filter = f', run_id="{body.run_id}"' if body.run_id else ""

    queries = {
        "dq_score_post":  f'uc1_dq_score_post{{source="{src}"{run_filter}}}',
        "null_rate":      f'uc1_null_rate{{source="{src}"{run_filter}}}',
        "rows_in":        f'uc1_rows_in{{source="{src}"{run_filter}}}',
        "rows_out":       f'uc1_rows_out{{source="{src}"{run_filter}}}',
        "dedup_rate":     f'uc1_dedup_rate{{source="{src}"{run_filter}}}',
        "cost_usd":       f'uc1_llm_cost_usd_total{{source="{src}"{run_filter}}}',
        "quarantine_rows":f'uc1_quarantine_rows{{source="{src}"{run_filter}}}',
        "anomaly_flag":   f'uc1_anomaly_flag{{source="{src}"{run_filter}}}',
    }

    # Return {metric: {run_id: value}} so caller can see all runs at once
    data: dict[str, Any] = {}
    any_cached = False
    for name, pql in queries.items():
        flat = _prom_flat(pql)
        data[name] = flat
        if not flat:
            data[name] = {}

    return ToolResult(tool="get_source_stats", run_id=body.run_id,
                      source=body.source, data=data, cached=any_cached)


# ── tool 4: get_anomalies ──────────────────────────────────────────────────────

@app.post("/tools/get_anomalies", response_model=ToolResult)
def get_anomalies(body: ToolInput) -> ToolResult:
    clauses: list[str] = []
    params:  list[Any] = []

    if body.run_id:
        clauses.append("run_id = %s")
        params.append(body.run_id)
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(body.limit or 50)

    rows, cached = _pg_query(
        f"""
        SELECT run_id, source, signal, score, details, ts
        FROM   anomaly_reports
        {where}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
        cache_ttl=ANOMALY_TTL,
    )

    return ToolResult(tool="get_anomalies", run_id=body.run_id,
                      source=body.source, data=rows, cached=cached)


# ── tool 5: get_cost_report ────────────────────────────────────────────────────

@app.post("/tools/get_cost_report", response_model=ToolResult)
def get_cost_report(body: ToolInput) -> ToolResult:
    src_filter = f'source="{body.source}"' if body.source else ""
    run_filter = f'run_id="{body.run_id}"' if body.run_id else ""
    label_filter = ", ".join(f for f in [src_filter, run_filter] if f)

    def q(metric: str) -> str:
        return f"{metric}{{{label_filter}}}" if label_filter else metric

    data: dict[str, Any] = {}
    for name, pql in [
        ("cost_usd",   q("uc1_llm_cost_usd_total")),
        ("llm_calls",  q("uc1_llm_calls_total")),
        ("s1_count",   q("uc1_s1_count")),
        ("s2_count",   q("uc1_s2_count")),
        ("s3_count",   q("uc1_s3_count")),
        ("s4_count",   q("uc1_s4_count")),
    ]:
        data[name] = _prom_flat(pql)

    return ToolResult(tool="get_cost_report", run_id=body.run_id,
                      source=body.source, data=data)


# ── tool 6: get_quarantine ─────────────────────────────────────────────────────

@app.post("/tools/get_quarantine", response_model=ToolResult)
def get_quarantine(body: ToolInput) -> ToolResult:
    if not body.run_id:
        raise HTTPException(status_code=400, detail="run_id is required")

    clauses = ["run_id = %s"]
    params: list[Any] = [body.run_id]
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)
    params.append(body.limit or 200)

    rows, cached = _pg_query(
        f"""
        SELECT run_id, source, row_hash, reason, row_data, ts
        FROM   quarantine_rows
        WHERE  {" AND ".join(clauses)}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
    )

    return ToolResult(tool="get_quarantine", run_id=body.run_id,
                      source=body.source, data=rows, cached=cached)


# ── tool 7: get_dedup_stats ────────────────────────────────────────────────────

@app.post("/tools/get_dedup_stats", response_model=ToolResult)
def get_dedup_stats(body: ToolInput) -> ToolResult:
    clauses: list[str] = []
    params:  list[Any] = []

    if body.run_id:
        clauses.append("run_id = %s")
        params.append(body.run_id)
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(body.limit or 200)

    rows, cached = _pg_query(
        f"""
        SELECT run_id, source, cluster_id, canonical, members, merge_decisions, ts
        FROM   dedup_clusters
        {where}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
    )

    return ToolResult(tool="get_dedup_stats", run_id=body.run_id,
                      source=body.source, data=rows, cached=cached)


# ── tool discovery ─────────────────────────────────────────────────────────────

@app.post("/tools/list_runs", response_model=ToolResult)
def list_runs(body: ToolInput) -> ToolResult:
    """
    List all known run_ids from Prometheus, optionally filtered by source.
    Use this FIRST when the user mentions a run by number (e.g. "run 6")
    to resolve the exact run_id string before calling other tools.
    """
    src_filter = f'source="{body.source}"' if body.source else ""
    promql = f"uc1_rows_in{{{src_filter}}}" if src_filter else "uc1_rows_in"

    results, cached = _prom_query(promql)

    runs = []
    seen = set()
    for series in results:
        rid = series["metric"].get("run_id")
        src = series["metric"].get("source")
        if rid and rid not in seen:
            seen.add(rid)
            runs.append({"run_id": rid, "source": src})

    # Sort so most recent (highest lexicographic) run_id is last
    runs.sort(key=lambda r: r["run_id"])

    return ToolResult(tool="list_runs", run_id=None, source=body.source,
                      data={"runs": runs, "count": len(runs)}, cached=cached)


@app.get("/tools")
def list_tools() -> dict:
    return {
        "tools": [
            {"name": "get_run_metrics",  "description": "All Prometheus metrics for a run_id (flat dict: metric→value).",           "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}}, "required": ["run_id"]}},
            {"name": "get_block_trace",  "description": "Block-level execution trace from Postgres for a run_id.",                   "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}}, "required": ["run_id"]}},
            {"name": "get_source_stats", "description": "All Prometheus metrics for a source, keyed by run_id.",                     "input_schema": {"type": "object", "properties": {"source": {"type": "string"}, "run_id": {"type": "string"}}, "required": ["source"]}},
            {"name": "get_anomalies",    "description": "Anomaly reports from Postgres (signal, score, details) for a run or source.","input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}, "limit": {"type": "integer"}}}},
            {"name": "get_cost_report",  "description": "LLM cost and enrichment tier counts from Prometheus.",                       "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}}}},
            {"name": "get_quarantine",   "description": "Quarantined rows from Postgres for a run_id with failure reasons.",          "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["run_id"]}},
            {"name": "get_dedup_stats",  "description": "Deduplication cluster stats from Postgres for a run or source.",             "input_schema": {"type": "object", "properties": {"run_id": {"type": "string"}, "source": {"type": "string"}, "limit": {"type": "integer"}}}},
            {"name": "list_runs",        "description": "List all known run_ids from Prometheus. Call this FIRST when the user refers to a run by number (e.g. 'run 6', 'last run', 'latest') to resolve the exact run_id string before calling other tools.", "input_schema": {"type": "object", "properties": {"source": {"type": "string", "description": "Filter by source: OFF, USDA, openFDA, ESCI"}}}},
        ]
    }


@app.get("/health")
def health() -> dict:
    redis_ok = _redis_client is not None and _redis_client.ping()
    return {"status": "ok", "redis": "connected" if redis_ok else "unavailable"}


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=8001)
