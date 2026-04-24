"""
UC2 Observability — Claude Desktop MCP Server

Exposes the same 8 tools as mcp_server.py (FastAPI) but over stdio transport
so Claude Desktop can discover and call them directly.

Run (for Claude Desktop config):
    python src/uc2_observability/mcp_claude_desktop.py
"""

from __future__ import annotations

import decimal
import hashlib
import json
import logging
from datetime import datetime, date
from typing import Any

import psycopg2
import psycopg2.extras
import redis
import requests
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

PG_DSN         = "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD"
PROMETHEUS_URL = "http://localhost:9090"
REDIS_HOST     = "localhost"
REDIS_PORT     = 6379

PROM_CACHE_TTL = 15
PG_CACHE_TTL   = 30
ANOMALY_TTL    = 300

# ── Redis (graceful degradation) ───────────────────────────────────────────────

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
        return [{"error": str(exc)}], False
    _cache_set(key, rows, cache_ttl)
    return rows, False


def _prom_query(promql: str) -> tuple[list[dict], bool]:
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
    results, _ = _prom_query(promql)
    out = {}
    for series in results:
        run_id = series.get("metric", {}).get("run_id", "latest")
        val    = series.get("value", [None, None])[1]
        if val is not None and val != "NaN":
            out[run_id] = float(val)
    return out


# ── MCP server ─────────────────────────────────────────────────────────────────

mcp = FastMCP("uc2_observability")


@mcp.tool()
def list_runs(source: str = "") -> dict:
    """List all known pipeline run_ids from Prometheus.

    Call this FIRST when the user refers to a run by number (e.g. 'run 6',
    'last run', 'latest') to resolve the exact run_id string before calling
    other tools.

    Args:
        source: Optional filter by source name (e.g. OFF, USDA, openFDA, ESCI).
    """
    src_filter = f'source="{source}"' if source else ""
    promql = f"etl_rows_in{{{src_filter}}}" if src_filter else "etl_rows_in"
    results, cached = _prom_query(promql)

    runs = []
    seen: set[str] = set()
    for series in results:
        rid = series["metric"].get("run_id")
        src = series["metric"].get("source")
        if rid and rid not in seen:
            seen.add(rid)
            runs.append({"run_id": rid, "source": src})
    runs.sort(key=lambda r: r["run_id"])
    return {"runs": runs, "count": len(runs), "cached": cached}


@mcp.tool()
def get_run_metrics(run_id: str, source: str = "") -> dict:
    """Return all Prometheus metrics for a specific pipeline run.

    Returns a flat dict of metric_name → value so Claude can read it directly.
    Covers rows_in/out, null_rate, DQ scores, dedup_rate, LLM cost/calls,
    enrichment tier counts (S1/S2/S3/unresolved), quarantine count, and
    block duration.

    Args:
        run_id: Exact run_id string (use list_runs first to resolve).
        source: Optional source label to narrow the query.
    """
    src_filter = f', source="{source}"' if source else ""

    metric_queries = {
        "rows_in":          f'etl_rows_in{{run_id="{run_id}"{src_filter}}}',
        "rows_out":         f'etl_rows_out{{run_id="{run_id}"{src_filter}}}',
        "null_rate":        f'etl_null_rate{{run_id="{run_id}"{src_filter}}}',
        "dq_score_pre":     f'etl_dq_score_pre{{run_id="{run_id}"{src_filter}}}',
        "dq_score_post":    f'etl_dq_score_post{{run_id="{run_id}"{src_filter}}}',
        "dq_delta":         f'etl_dq_delta{{run_id="{run_id}"{src_filter}}}',
        "dedup_rate":       f'etl_dedup_rate{{run_id="{run_id}"{src_filter}}}',
        "llm_calls":        f'etl_llm_calls_total{{run_id="{run_id}"{src_filter}}}',
        "cost_usd":         f'etl_llm_cost_usd_total{{run_id="{run_id}"{src_filter}}}',
        "s1_count":         f'etl_enrichment_s1_resolved{{run_id="{run_id}"{src_filter}}}',
        "s2_count":         f'etl_enrichment_s2_resolved{{run_id="{run_id}"{src_filter}}}',
        "s3_count":         f'etl_enrichment_s3_resolved{{run_id="{run_id}"{src_filter}}}',
        "s4_count":         f'etl_enrichment_unresolved{{run_id="{run_id}"{src_filter}}}',
        "quarantine":       f'etl_rows_quarantined{{run_id="{run_id}"{src_filter}}}',
        "block_duration_s": f'etl_duration_seconds{{run_id="{run_id}"{src_filter}}}',
    }

    data: dict[str, Any] = {"run_id": run_id}
    for name, pql in metric_queries.items():
        results, _ = _prom_query(pql)
        if results:
            val = results[0].get("value", [None, None])[1]
            data[name] = float(val) if val not in (None, "NaN") else None
        else:
            data[name] = None
    data["_note"] = "null means metric not yet pushed for this run_id"
    return data


@mcp.tool()
def get_block_trace(run_id: str, source: str = "", limit: int = 500) -> dict:
    """Return block-level execution trace from Postgres for a run.

    Shows each block's rows_in, rows_out, null_rates, duration_ms, and
    timestamp in execution order.

    Args:
        run_id: Exact run_id string.
        source: Optional source filter.
        limit: Max rows to return (default 500).
    """
    if source:
        rows, cached = _pg_query(
            """
            SELECT run_id, source, block,
                   rows_in, rows_out, null_rates, duration_ms, ts
            FROM   block_trace
            WHERE  run_id = %s AND source = %s
            ORDER  BY ts ASC
            LIMIT  %s
            """,
            (run_id, source, limit),
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
            (run_id, limit),
        )
    return {"run_id": run_id, "source": source, "rows": rows, "cached": cached}


@mcp.tool()
def get_source_stats(source: str, run_id: str = "") -> dict:
    """Return all Prometheus metrics for a source, keyed by run_id.

    Useful for comparing quality across multiple runs of the same source.
    Returns {metric: {run_id: value}} dicts.

    Args:
        source: Source name (e.g. OFF, USDA, openFDA, ESCI).
        run_id: Optional run_id to narrow the query.
    """
    run_filter = f', run_id="{run_id}"' if run_id else ""

    queries = {
        "dq_score_post":   f'etl_dq_score_post{{source="{source}"{run_filter}}}',
        "null_rate":       f'etl_null_rate{{source="{source}"{run_filter}}}',
        "rows_in":         f'etl_rows_in{{source="{source}"{run_filter}}}',
        "rows_out":        f'etl_rows_out{{source="{source}"{run_filter}}}',
        "dedup_rate":      f'etl_dedup_rate{{source="{source}"{run_filter}}}',
        "cost_usd":        f'etl_llm_cost_usd_total{{source="{source}"{run_filter}}}',
        "quarantine_rows": f'etl_rows_quarantined{{source="{source}"{run_filter}}}',
        "anomaly_flag":    f'etl_anomaly_flag{{source="{source}"{run_filter}}}',
    }

    data: dict[str, Any] = {}
    for name, pql in queries.items():
        data[name] = _prom_flat(pql)
    return {"source": source, "metrics": data}


@mcp.tool()
def get_anomalies(run_id: str = "", source: str = "", limit: int = 50) -> dict:
    """Return anomaly reports from Postgres.

    Anomalies are detected by Isolation Forest on pipeline metrics.
    Each report includes signal name, anomaly score, and details.

    Args:
        run_id: Filter by run_id (optional).
        source: Filter by source (optional).
        limit: Max rows to return (default 50).
    """
    clauses: list[str] = []
    params: list[Any] = []

    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)
    if source:
        clauses.append("source = %s")
        params.append(source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

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
    return {"run_id": run_id, "source": source, "anomalies": rows, "cached": cached}


@mcp.tool()
def get_cost_report(run_id: str = "", source: str = "") -> dict:
    """Return LLM cost and enrichment tier breakdown from Prometheus.

    Shows cost_usd, llm_calls, and per-tier enrichment counts
    (S1 deterministic, S2 KNN, S3 RAG-LLM, unresolved).

    Args:
        run_id: Optional run_id filter.
        source: Optional source filter.
    """
    src_filter = f'source="{source}"' if source else ""
    run_filter = f'run_id="{run_id}"' if run_id else ""
    label_filter = ", ".join(f for f in [src_filter, run_filter] if f)

    def q(metric: str) -> str:
        return f"{metric}{{{label_filter}}}" if label_filter else metric

    data: dict[str, Any] = {}
    for name, pql in [
        ("cost_usd",  q("etl_llm_cost_usd_total")),
        ("llm_calls", q("etl_llm_calls_total")),
        ("s1_count",  q("etl_enrichment_s1_resolved")),
        ("s2_count",  q("etl_enrichment_s2_resolved")),
        ("s3_count",  q("etl_enrichment_s3_resolved")),
        ("s4_count",  q("etl_enrichment_unresolved")),
    ]:
        data[name] = _prom_flat(pql)
    return {"run_id": run_id, "source": source, "cost": data}


@mcp.tool()
def get_quarantine(run_id: str, source: str = "", limit: int = 200) -> dict:
    """Return quarantined rows from Postgres for a run.

    Quarantined rows failed validation during the pipeline. Each row includes
    the failure reason and raw row_data.

    Args:
        run_id: Exact run_id string (required).
        source: Optional source filter.
        limit: Max rows to return (default 200).
    """
    clauses = ["run_id = %s"]
    params: list[Any] = [run_id]
    if source:
        clauses.append("source = %s")
        params.append(source)
    params.append(limit)

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
    return {"run_id": run_id, "source": source, "quarantine_rows": rows, "cached": cached}


@mcp.tool()
def get_dedup_stats(run_id: str = "", limit: int = 200) -> dict:
    """Return deduplication cluster stats from Postgres.

    Shows cluster_id, canonical record, member list, and merge decisions
    from the fuzzy-dedup stage.

    Args:
        run_id: Optional run_id filter.
        limit: Max clusters to return (default 200).
    """
    clauses: list[str] = []
    params: list[Any] = []

    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    rows, cached = _pg_query(
        f"""
        SELECT run_id, cluster_id, canonical, members, merge_decisions, ts
        FROM   dedup_clusters
        {where}
        ORDER  BY ts DESC
        LIMIT  %s
        """,
        tuple(params),
    )
    return {"run_id": run_id, "clusters": rows, "cached": cached}


def main() -> None:
    import sys
    logging.basicConfig(level=logging.WARNING)
    transport = "sse" if "--sse" in sys.argv else "stdio"
    if transport == "sse":
        port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8002
        mcp.settings.host = "0.0.0.0"
        mcp.settings.port = port
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
