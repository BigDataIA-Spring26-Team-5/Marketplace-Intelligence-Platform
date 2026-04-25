"""Unified API client: MCP server, Prometheus, MLflow, ChromaDB."""
from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

from .redis_cache import cached_query

logger = logging.getLogger(__name__)

MCP_URL      = os.getenv("MCP_SERVER_URL",     "http://localhost:8001")
PROM_URL     = os.getenv("PROMETHEUS_URL",      "http://localhost:9090")
MLFLOW_URL   = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
CHROMA_URL   = os.getenv("CHROMA_URL",          "http://localhost:8000")
LOG_DIR      = Path(os.getenv("RUN_LOG_DIR",
               "/home/aq/work/Marketplace-Intelligence-Platform/output/run_logs"))

_TIMEOUT = 8


# ── Prometheus ────────────────────────────────────────────────────────────────

def prom_query(promql: str) -> list[dict]:
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query",
                         params={"query": promql}, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json().get("data", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Prometheus query failed [{promql}]: {e}")
        return []


def prom_scalar(promql: str) -> float | None:
    results = prom_query(promql)
    if results:
        val = results[0].get("value", [None, None])[1]
        try:
            f = float(val)
            return None if f != f else f  # NaN check
        except Exception:
            pass
    return None


def prom_series(promql: str) -> list[tuple[dict, float]]:
    """Return [(labels_dict, value), ...] for a metric."""
    out = []
    for r in prom_query(promql):
        try:
            val = float(r["value"][1])
            if val == val:  # not NaN
                out.append((r.get("metric", {}), val))
        except Exception:
            pass
    return out


# ── MCP Server ────────────────────────────────────────────────────────────────

def mcp_post(tool: str, payload: dict = {}) -> dict:
    try:
        r = requests.post(f"{MCP_URL}/tools/{tool}",
                          json=payload, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"MCP {tool} failed: {e}")
        return {}


def get_run_list() -> list[dict]:
    def _fetch():
        data = mcp_post("list_runs")
        return data.get("data", {}).get("runs", [])
    return cached_query("ui:mcp:run_list", _fetch, ttl=15)


def get_source_stats(source: str) -> dict:
    def _fetch():
        return mcp_post("get_source_stats", {"source": source}).get("data", {})
    return cached_query(f"ui:mcp:source:{source}", _fetch, ttl=30)


def get_anomalies(limit: int = 20) -> list[dict]:
    def _fetch():
        data = mcp_post("get_anomalies", {"limit": limit})
        return data.get("data", {}).get("anomalies", [])
    return cached_query("ui:mcp:anomalies", _fetch, ttl=30)


def get_cost_report() -> dict:
    def _fetch():
        return mcp_post("get_cost_report").get("data", {})
    return cached_query("ui:mcp:cost", _fetch, ttl=30)


# ── RunLogStore ───────────────────────────────────────────────────────────────

def _dq(val) -> float | None:
    """Normalize DQ score to 0-100 range."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN
            return None
        return round(f * 100, 2) if 0.0 <= f <= 1.0 else round(f, 2)
    except Exception:
        return None


def _dq_delta(val) -> float | None:
    """Parse DQ delta — never scale, just clamp to sane range."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN
            return None
        if abs(f) > 100:
            return None  # corrupt value — drop silently
        return round(f, 2)
    except Exception:
        return None


def load_run_logs(limit: int | None = None) -> list[dict]:
    def _fetch():
        import glob
        files = sorted(glob.glob(str(LOG_DIR / "*.json")))
        logs = []
        for f in files:
            try:
                d = json.loads(Path(f).read_text())
                pre  = _dq(d.get("dq_score_pre"))
                post = _dq(d.get("dq_score_post"))
                d["dq_score_pre"]  = pre
                d["dq_score_post"] = post
                # Recompute delta from normalized scores; drop run if post=0 (block never ran)
                if pre is not None and post is not None and post > 0:
                    d["dq_delta"] = round(post - pre, 2)
                else:
                    d["dq_delta"] = None
                logs.append(d)
            except Exception:
                pass
        logs.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
        return logs
    all_logs = cached_query("ui:run_logs:all", _fetch, ttl=15)
    return all_logs[:limit] if limit else all_logs


def dashboard_kpis() -> dict:
    def _fetch():
        logs = load_run_logs()
        today = datetime.now(timezone.utc).date().isoformat()
        today_logs = [r for r in logs if r.get("timestamp", "")[:10] == today]
        if not today_logs:
            today_logs = logs[:20]  # fallback: last 20

        total  = len(today_logs)
        success = sum(1 for r in today_logs if r.get("status") == "success")
        rate   = round(success / total * 100, 1) if total else 0.0

        deltas = [r["dq_delta"] for r in today_logs
                  if r.get("dq_delta") is not None]
        avg_delta = round(sum(deltas) / len(deltas), 2) if deltas else 0.0

        quaran = sum(r.get("rows_quarantined") or 0 for r in today_logs)
        rows_in = sum(r.get("rows_in") or 0 for r in today_logs)
        qrate  = round(quaran / rows_in * 100, 2) if rows_in else 0.0

        return {"runs_today": total, "success_rate": rate,
                "avg_dq_delta": avg_delta, "quarantine_rate": qrate}
    return cached_query("ui:dashboard:kpis", _fetch, ttl=15)


# ── MLflow ────────────────────────────────────────────────────────────────────

def mlflow_experiments() -> list[dict]:
    def _fetch():
        try:
            import mlflow
            mlflow.set_tracking_uri(MLFLOW_URL)
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            exps = client.search_experiments()
            return [{"id": e.experiment_id, "name": e.name} for e in exps]
        except Exception as e:
            logger.warning(f"MLflow experiments failed: {e}")
            return []
    return cached_query("ui:mlflow:experiments", _fetch, ttl=30)


def mlflow_runs(experiment_id: str, time_range_days: int = 30) -> list[dict]:
    def _fetch():
        try:
            import mlflow
            mlflow.set_tracking_uri(MLFLOW_URL)
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            runs = client.search_runs(
                experiment_ids=[experiment_id],
                order_by=["start_time DESC"],
                max_results=200,
            )
            result = []
            for run in runs:
                m = run.data.metrics
                p = run.data.params
                result.append({
                    "run_id":       run.info.run_id[:8],
                    "run_name":     run.info.run_name or run.info.run_id[:8],
                    "status":       run.info.status,
                    "start_time":   datetime.fromtimestamp(
                                        run.info.start_time / 1000,
                                        tz=timezone.utc
                                    ).strftime("%b %d, %H:%M")
                                    if run.info.start_time else "",
                    "source":       p.get("source", ""),
                    "dq_score_pre": round(m.get("dq_score_pre", 0), 2),
                    "dq_score_post":round(m.get("dq_score_post", 0), 2),
                    "dq_delta":     round(m.get("dq_delta", 0), 2),
                    "rows_in":      int(m.get("rows_in", 0)),
                    "rows_out":     int(m.get("rows_out", 0)),
                    "cost_usd":     round(m.get("cost_usd", 0), 4),
                    "llm_calls":    int(m.get("llm_calls", 0)),
                    "s1_count":     int(m.get("s1_count", 0)),
                    "s2_count":     int(m.get("s2_count", 0)),
                    "s3_count":     int(m.get("s3_count", 0)),
                    "anomaly_count":int(m.get("anomaly_count", 0)),
                })
            return result
        except Exception as e:
            logger.warning(f"MLflow runs failed: {e}")
            return []
    return cached_query(f"ui:mlflow:runs:{experiment_id}", _fetch, ttl=30)


# ── ChromaDB ──────────────────────────────────────────────────────────────────

def chroma_collections() -> list[dict]:
    def _fetch():
        try:
            r = requests.get(
                f"{CHROMA_URL}/api/v2/tenants/default_tenant/databases/default_database/collections",
                timeout=5)
            r.raise_for_status()
            return [{"name": c["name"], "id": c["id"]} for c in r.json()]
        except Exception as e:
            logger.warning(f"ChromaDB collections failed: {e}")
            return []
    return cached_query("ui:chroma:collections", _fetch, ttl=60)
