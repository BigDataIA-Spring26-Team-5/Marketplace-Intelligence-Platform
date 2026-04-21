"""
UC2 Observability Layer — MCP Server

FastAPI app exposing 7 MCP-style tool endpoints.  Each endpoint accepts
a JSON body with optional `run_id` and `source` fields and returns a
structured JSON result.

Run with:
    uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001
"""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

PG_DSN = "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD"
PROMETHEUS_URL = "http://localhost:9090"

app = FastAPI(
    title="UC2 MCP Server",
    description="Model Context Protocol tool endpoints for UC2 Observability",
    version="1.0.0",
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
    tool: str
    run_id: str | None
    source: str | None
    data: Any
    error: str | None = None


# ── shared helpers ─────────────────────────────────────────────────────────────

def _pg_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return rows as a list of dicts."""
    try:
        conn = psycopg2.connect(PG_DSN)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        finally:
            conn.close()
    except psycopg2.Error as exc:
        logger.error("Postgres error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Postgres error: {exc}")


def _prom_query(promql: str) -> list[dict]:
    """Execute a Prometheus instant query and return result list."""
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return []
        return data.get("data", {}).get("result", [])
    except Exception as exc:
        logger.warning("Prometheus query failed (%r): %s", promql, exc)
        return []


def _prom_range_query(promql: str, hours: int = 24) -> list[dict]:
    """Execute a Prometheus range query over the last `hours` hours."""
    import time
    end = time.time()
    start = end - hours * 3600
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": "300"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return []
        return data.get("data", {}).get("result", [])
    except Exception as exc:
        logger.warning("Prometheus range query failed (%r): %s", promql, exc)
        return []


def _serialize(rows: list[dict]) -> list[dict]:
    """Convert non-serialisable types (datetime, Decimal, etc.) to strings."""
    import decimal
    from datetime import datetime, date

    def _fix(v: Any) -> Any:
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, decimal.Decimal):
            return float(v)
        return v

    return [{k: _fix(v) for k, v in row.items()} for row in rows]


# ── tool 1: get_run_metrics ────────────────────────────────────────────────────

@app.post("/tools/get_run_metrics", response_model=ToolResult)
def get_run_metrics(body: ToolInput) -> ToolResult:
    """
    Query Prometheus for all uc1_* metrics for a specific run_id.
    """
    if not body.run_id:
        raise HTTPException(status_code=400, detail="run_id is required")

    queries = {
        "rows_in":     f'uc1_rows_in{{run_id="{body.run_id}"}}',
        "rows_out":    f'uc1_rows_out{{run_id="{body.run_id}"}}',
        "null_rate":   f'uc1_null_rate{{run_id="{body.run_id}"}}',
        "dq_score_pre":  f'uc1_dq_score_pre{{run_id="{body.run_id}"}}',
        "dq_score_post": f'uc1_dq_score_post{{run_id="{body.run_id}"}}',
        "dq_delta":    f'uc1_dq_delta{{run_id="{body.run_id}"}}',
        "dedup_rate":  f'uc1_dedup_rate{{run_id="{body.run_id}"}}',
        "llm_calls":   f'uc1_llm_calls_total{{run_id="{body.run_id}"}}',
        "cost_usd":    f'uc1_llm_cost_usd_total{{run_id="{body.run_id}"}}',
        "s1_count":    f'uc1_s1_count{{run_id="{body.run_id}"}}',
        "s2_count":    f'uc1_s2_count{{run_id="{body.run_id}"}}',
        "s3_count":    f'uc1_s3_count{{run_id="{body.run_id}"}}',
        "s4_count":    f'uc1_s4_count{{run_id="{body.run_id}"}}',
        "quarantine":  f'uc1_quarantine_rows{{run_id="{body.run_id}"}}',
    }
    result: dict[str, Any] = {}
    for metric_name, pql in queries.items():
        prom_result = _prom_query(pql)
        if prom_result:
            result[metric_name] = [
                {"labels": r.get("metric", {}), "value": r.get("value", [None, None])[1]}
                for r in prom_result
            ]
        else:
            result[metric_name] = []

    return ToolResult(tool="get_run_metrics", run_id=body.run_id, source=body.source, data=result)


# ── tool 2: get_block_trace ────────────────────────────────────────────────────

@app.post("/tools/get_block_trace", response_model=ToolResult)
def get_block_trace(body: ToolInput) -> ToolResult:
    """
    Query Postgres block_trace table for a run_id (and optionally source).
    """
    if not body.run_id:
        raise HTTPException(status_code=400, detail="run_id is required")

    if body.source:
        rows = _pg_query(
            """
            SELECT run_id, source, block_name, event_type,
                   rows_in, rows_out, null_rates, duration_ms, ts
            FROM   block_trace
            WHERE  run_id = %s AND source = %s
            ORDER  BY ts ASC
            LIMIT  %s
            """,
            (body.run_id, body.source, body.limit or 500),
        )
    else:
        rows = _pg_query(
            """
            SELECT run_id, source, block_name, event_type,
                   rows_in, rows_out, null_rates, duration_ms, ts
            FROM   block_trace
            WHERE  run_id = %s
            ORDER  BY ts ASC
            LIMIT  %s
            """,
            (body.run_id, body.limit or 500),
        )

    return ToolResult(
        tool="get_block_trace",
        run_id=body.run_id,
        source=body.source,
        data=_serialize(rows),
    )


# ── tool 3: get_source_stats ───────────────────────────────────────────────────

@app.post("/tools/get_source_stats", response_model=ToolResult)
def get_source_stats(body: ToolInput) -> ToolResult:
    """
    Query Prometheus for aggregated stats by source (last 24h).
    """
    if not body.source:
        raise HTTPException(status_code=400, detail="source is required")

    src = body.source
    # Optional run_id filter
    run_filter = f', run_id="{body.run_id}"' if body.run_id else ""

    queries = {
        "dq_score_post":   f'uc1_dq_score_post{{source="{src}"{run_filter}}}',
        "null_rate":        f'uc1_null_rate{{source="{src}"{run_filter}}}',
        "rows_in":          f'uc1_rows_in{{source="{src}"{run_filter}}}',
        "rows_out":         f'uc1_rows_out{{source="{src}"{run_filter}}}',
        "dedup_rate":       f'uc1_dedup_rate{{source="{src}"{run_filter}}}',
        "llm_cost_usd":     f'uc1_llm_cost_usd_total{{source="{src}"{run_filter}}}',
        "quarantine_rows":  f'uc1_quarantine_rows{{source="{src}"{run_filter}}}',
        "anomaly_flag":     f'uc1_anomaly_flag{{source="{src}"{run_filter}}}',
    }

    data: dict[str, Any] = {}
    for name, pql in queries.items():
        res = _prom_query(pql)
        data[name] = [
            {"labels": r.get("metric", {}), "value": r.get("value", [None, None])[1]}
            for r in res
        ]

    return ToolResult(tool="get_source_stats", run_id=body.run_id, source=body.source, data=data)


# ── tool 4: get_anomalies ──────────────────────────────────────────────────────

@app.post("/tools/get_anomalies", response_model=ToolResult)
def get_anomalies(body: ToolInput) -> ToolResult:
    """
    Query Postgres anomaly_reports for a source and/or run_id.
    """
    clauses: list[str] = []
    params: list[Any] = []

    if body.run_id:
        clauses.append("run_id = %s")
        params.append(body.run_id)
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(body.limit or 50)

    rows = _pg_query(
        f"""
        SELECT run_id, source, anomaly_score, features,
               flagged_signals, detected_at
        FROM   anomaly_reports
        {where}
        ORDER  BY detected_at DESC
        LIMIT  %s
        """,
        tuple(params),
    )

    return ToolResult(
        tool="get_anomalies",
        run_id=body.run_id,
        source=body.source,
        data=_serialize(rows),
    )


# ── tool 5: get_cost_report ────────────────────────────────────────────────────

@app.post("/tools/get_cost_report", response_model=ToolResult)
def get_cost_report(body: ToolInput) -> ToolResult:
    """
    Query Prometheus cost metrics (uc1_llm_cost_usd_total, uc1_llm_calls_total).
    """
    src_filter = f', source="{body.source}"' if body.source else ""
    run_filter = f', run_id="{body.run_id}"' if body.run_id else ""
    label_filter = src_filter + run_filter

    queries = {
        "cost_usd_total":  f"uc1_llm_cost_usd_total{{{label_filter.lstrip(', ')}}}",
        "llm_calls_total": f"uc1_llm_calls_total{{{label_filter.lstrip(', ')}}}",
        "s1_count":        f"uc1_s1_count{{{label_filter.lstrip(', ')}}}",
        "s2_count":        f"uc1_s2_count{{{label_filter.lstrip(', ')}}}",
        "s3_count":        f"uc1_s3_count{{{label_filter.lstrip(', ')}}}",
        "s4_count":        f"uc1_s4_count{{{label_filter.lstrip(', ')}}}",
    }

    data: dict[str, Any] = {}
    for name, pql in queries.items():
        res = _prom_query(pql)
        data[name] = [
            {"labels": r.get("metric", {}), "value": r.get("value", [None, None])[1]}
            for r in res
        ]

    # Also fetch 24h range for cost trend
    cost_trend = _prom_range_query(
        f'uc1_llm_cost_usd_total{{{label_filter.lstrip(", ")}}}',
        hours=24,
    )
    data["cost_trend_24h"] = cost_trend

    return ToolResult(tool="get_cost_report", run_id=body.run_id, source=body.source, data=data)


# ── tool 6: get_quarantine ─────────────────────────────────────────────────────

@app.post("/tools/get_quarantine", response_model=ToolResult)
def get_quarantine(body: ToolInput) -> ToolResult:
    """
    Query Postgres quarantine_rows for a run_id and/or source.
    """
    clauses: list[str] = []
    params: list[Any] = []

    if body.run_id:
        clauses.append("run_id = %s")
        params.append(body.run_id)
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(body.limit or 200)

    rows = _pg_query(
        f"""
        SELECT run_id, source, row_hash, reason, row_data, ts
        FROM   quarantine_rows
        {where}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
    )

    return ToolResult(
        tool="get_quarantine",
        run_id=body.run_id,
        source=body.source,
        data=_serialize(rows),
    )


# ── tool 7: get_dedup_stats ────────────────────────────────────────────────────

@app.post("/tools/get_dedup_stats", response_model=ToolResult)
def get_dedup_stats(body: ToolInput) -> ToolResult:
    """
    Query Postgres dedup_clusters for a run_id and/or source.
    """
    clauses: list[str] = []
    params: list[Any] = []

    if body.run_id:
        clauses.append("run_id = %s")
        params.append(body.run_id)
    if body.source:
        clauses.append("source = %s")
        params.append(body.source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(body.limit or 200)

    rows = _pg_query(
        f"""
        SELECT run_id, source, cluster_id, canonical,
               members, merge_decisions, ts
        FROM   dedup_clusters
        {where}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
    )

    return ToolResult(
        tool="get_dedup_stats",
        run_id=body.run_id,
        source=body.source,
        data=_serialize(rows),
    )


# ── tool schema endpoint (for Claude's tool-discovery) ─────────────────────────

@app.get("/tools")
def list_tools() -> dict:
    """Return MCP tool definitions that Claude can use for tool-calling."""
    return {
        "tools": [
            {
                "name": "get_run_metrics",
                "description": "Retrieve all Prometheus metrics for a specific pipeline run_id.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string", "description": "Pipeline run identifier"},
                        "source": {"type": "string", "description": "Data source (OFF, USDA, openFDA, ESCI)"},
                    },
                    "required": ["run_id"],
                },
            },
            {
                "name": "get_block_trace",
                "description": "Retrieve block-level execution trace from Postgres for a run_id.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["run_id"],
                },
            },
            {
                "name": "get_source_stats",
                "description": "Retrieve aggregated Prometheus stats for a data source.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "run_id": {"type": "string"},
                    },
                    "required": ["source"],
                },
            },
            {
                "name": "get_anomalies",
                "description": "Retrieve anomaly reports from Postgres for a source or run_id.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "source": {"type": "string"},
                        "limit":  {"type": "integer"},
                    },
                },
            },
            {
                "name": "get_cost_report",
                "description": "Retrieve LLM cost metrics from Prometheus.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "source": {"type": "string"},
                    },
                },
            },
            {
                "name": "get_quarantine",
                "description": "Retrieve quarantined rows from Postgres for a run_id.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "source": {"type": "string"},
                        "limit":  {"type": "integer"},
                    },
                    "required": ["run_id"],
                },
            },
            {
                "name": "get_dedup_stats",
                "description": "Retrieve deduplication cluster stats from Postgres.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "source": {"type": "string"},
                        "limit":  {"type": "integer"},
                    },
                },
            },
        ]
    }


# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=8001)
