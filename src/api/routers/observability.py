"""
Observability router — /v1/observability/*

Run history, block traces, anomalies, quarantine, cost, dedup stats.
Borrows _pg_query / _prom_flat helpers from mcp_server to avoid duplication.
"""

from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from src.api.models.observability import (
    AnomalyRecord,
    BlockTrace,
    BlockTraceEntry,
    CostReport,
    DedupStats,
    QuarantineRecord,
    RunListResponse,
    RunSummary,
    SourceCost,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _pg():
    from src.uc2_observability.mcp_server import _pg_query
    return _pg_query


def _pf():
    from src.uc2_observability.mcp_server import _prom_flat
    return _prom_flat


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ── GET /v1/observability/runs ───────────────────────────────────────────────

@router.get("/runs", response_model=RunListResponse)
def list_runs(
    source: str | None = Query(None),
    domain: str | None = Query(None),
    status: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    try:
        from src.uc2_observability.log_store import RunLogStore
        store = RunLogStore()
        all_runs = store.load_all()
    except Exception as exc:
        logger.warning("RunLogStore unavailable: %s", exc)
        all_runs = []

    # Apply filters
    filtered = []
    for r in all_runs:
        if source and r.get("source") != source:
            continue
        if domain and r.get("domain") != domain:
            continue
        if status and r.get("status") != status:
            continue
        if from_date:
            try:
                if r.get("started_at", "") < from_date:
                    continue
            except Exception:
                pass
        if to_date:
            try:
                if r.get("started_at", "") > to_date:
                    continue
            except Exception:
                pass
        filtered.append(r)

    total = len(filtered)
    start = (page - 1) * page_size
    page_items = filtered[start: start + page_size]

    summaries = []
    for r in page_items:
        try:
            summaries.append(RunSummary(
                run_id=r.get("run_id", ""),
                source=r.get("source", ""),
                domain=r.get("domain", ""),
                status=r.get("status", "unknown"),
                dq_score_pre=r.get("dq_score_pre"),
                dq_score_post=r.get("dq_score_post"),
                started_at=_parse_dt(r.get("started_at")) or datetime.utcnow(),
                completed_at=_parse_dt(r.get("completed_at")),
                rows_in=r.get("rows_in"),
                rows_out=r.get("rows_out"),
            ))
        except Exception:
            continue

    return RunListResponse(runs=summaries, total=total, page=page, page_size=page_size)


# ── GET /v1/observability/runs/{run_id}/trace ────────────────────────────────

@router.get("/runs/{run_id}/trace", response_model=BlockTrace)
def get_block_trace(run_id: str):
    rows, _ = _pg()(
        "SELECT block, rows_in, rows_out, started_at, duration_ms FROM block_trace WHERE run_id = %s ORDER BY started_at",
        (run_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail={"error": "not_found", "detail": f"No trace found for run_id '{run_id}'"})

    entries = [
        BlockTraceEntry(
            block=r.get("block", ""),
            rows_in=r.get("rows_in", 0),
            rows_out=r.get("rows_out", 0),
            started_at=_parse_dt(r.get("started_at")),
            duration_ms=r.get("duration_ms"),
        )
        for r in rows
    ]
    return BlockTrace(run_id=run_id, blocks=entries)


# ── GET /v1/observability/anomalies ─────────────────────────────────────────

@router.get("/anomalies", response_model=list[AnomalyRecord])
def get_anomalies(
    source: str | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    sql = "SELECT source, anomaly_score, flagged_at, metrics FROM anomaly_reports"
    params: tuple = ()
    if source:
        sql += " WHERE source = %s"
        params = (source,)
    sql += " ORDER BY flagged_at DESC LIMIT %s"
    params = params + (limit,)

    rows, _ = _pg()(sql, params)
    import json as _json

    results = []
    for r in rows:
        try:
            metrics = r.get("metrics") or {}
            if isinstance(metrics, str):
                metrics = _json.loads(metrics)
            results.append(AnomalyRecord(
                source=r.get("source", ""),
                anomaly_score=float(r.get("anomaly_score", 0)),
                flagged_at=_parse_dt(str(r.get("flagged_at", ""))) or datetime.utcnow(),
                metrics=metrics,
            ))
        except Exception:
            continue
    return results


# ── GET /v1/observability/quarantine ────────────────────────────────────────

@router.get("/quarantine", response_model=list[QuarantineRecord])
def get_quarantine(
    run_id: str | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    conditions = []
    params: list = []
    if run_id:
        conditions.append("run_id = %s")
        params.append(run_id)
    if source:
        conditions.append("source = %s")
        params.append(source)

    sql = "SELECT run_id, row_index, reason, fields FROM quarantine_rows"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)

    rows, _ = _pg()(sql, tuple(params))
    import json as _json

    results = []
    for r in rows:
        try:
            fields = r.get("fields") or {}
            if isinstance(fields, str):
                fields = _json.loads(fields)
            results.append(QuarantineRecord(
                run_id=r.get("run_id", ""),
                row_index=r.get("row_index"),
                reason=r.get("reason", ""),
                fields=fields,
            ))
        except Exception:
            continue
    return results


# ── GET /v1/observability/cost ───────────────────────────────────────────────

@router.get("/cost", response_model=CostReport)
def get_cost(
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
):
    try:
        pf = _pf()
        raw = pf('sum by (source, model_tier) (etl_llm_tokens_total)')
    except Exception as exc:
        logger.warning("Prometheus unavailable for cost report: %s", exc)
        return CostReport()

    by_source = []
    total = 0
    for metric in raw.get("data", {}).get("result", []):
        try:
            labels = metric.get("metric", {})
            value = int(float(metric.get("value", [0, "0"])[1]))
            total += value
            by_source.append(SourceCost(
                source=labels.get("source", "unknown"),
                model_tier=labels.get("model_tier", "unknown"),
                tokens_used=value,
                requests=0,
            ))
        except Exception:
            continue

    return CostReport(by_source=by_source, total_tokens=total)


# ── GET /v1/observability/dedup ──────────────────────────────────────────────

@router.get("/dedup", response_model=DedupStats)
def get_dedup_stats(
    run_id: str | None = Query(None),
    source: str | None = Query(None),
):
    conditions = []
    params: list = []
    if run_id:
        conditions.append("run_id = %s")
        params.append(run_id)
    if source:
        conditions.append("source = %s")
        params.append(source)

    sql = "SELECT COUNT(*) as clusters, SUM(cluster_size - 1) as merged FROM dedup_clusters"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    rows, _ = _pg()(sql, tuple(params))
    if not rows:
        return DedupStats(run_id=run_id, source=source)

    r = rows[0]
    clusters = int(r.get("clusters") or 0)
    merged = int(r.get("merged") or 0)
    # Estimate total rows to compute rate
    total_sql = "SELECT COUNT(*) as total FROM dedup_clusters"
    if conditions:
        total_sql += " WHERE " + " AND ".join(conditions)
    total_rows_result, _ = _pg()(total_sql, tuple(params))
    total_rows = int((total_rows_result[0].get("total") or 1)) if total_rows_result else 1

    dedup_rate = round(merged / total_rows, 4) if total_rows else 0.0

    return DedupStats(
        run_id=run_id,
        source=source,
        clusters=clusters,
        merged_rows=merged,
        dedup_rate=dedup_rate,
    )
