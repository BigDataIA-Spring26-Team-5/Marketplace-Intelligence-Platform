"""
MLflow Bridge — UC2 Observability

Two responsibilities:
  1. log_run_to_mlflow(state)  — called from save_output_node for every new run
  2. backfill_from_prometheus() — one-time script to populate MLflow with
                                  all historical runs already in Prometheus + Postgres

MLflow tracking URI: http://localhost:5000
Experiments are named by source (e.g. "off", "usda/foundation", "openfda").
"""

from __future__ import annotations

import decimal
import logging
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = "http://localhost:5000"
PROMETHEUS_URL      = "http://localhost:9090"
PG_DSN              = "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD"

# All Prometheus metrics we care about per run
PROM_METRICS = {
    "rows_in":          "etl_rows_in",
    "rows_out":         "etl_rows_out",
    "null_rate":        "etl_null_rate",
    "dq_score_pre":     "etl_dq_score_pre",
    "dq_score_post":    "etl_dq_score_post",
    "dq_delta":         "etl_dq_delta",
    "dedup_rate":       "etl_dedup_rate",
    "llm_calls":        "etl_llm_calls_total",
    "cost_usd":         "etl_llm_cost_usd_total",
    "s1_count":         "etl_enrichment_s1_resolved",
    "s2_count":         "etl_enrichment_s2_resolved",
    "s3_count":         "etl_enrichment_s3_resolved",
    "s4_unresolved":    "etl_enrichment_unresolved",
    "quarantine_rows":  "etl_rows_quarantined",
    "duration_s":       "etl_duration_seconds",
    "anomaly_flag":     "etl_anomaly_flag",
}


# ── Prometheus helpers ─────────────────────────────────────────────────────────

def _prom_instant(promql: str) -> list[dict]:
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "success":
            return data.get("data", {}).get("result", [])
    except Exception as exc:
        logger.warning("Prometheus query failed: %s", exc)
    return []


def _prom_scalar(promql: str) -> float | None:
    results = _prom_instant(promql)
    if results:
        val = results[0].get("value", [None, None])[1]
        if val not in (None, "NaN"):
            return float(val)
    return None


def _fetch_all_run_ids() -> list[dict]:
    """Return [{run_id, source}] for every run known to Prometheus."""
    results = _prom_instant("etl_rows_in")
    seen: set[str] = set()
    runs = []
    for series in results:
        rid = series["metric"].get("run_id")
        src = series["metric"].get("source", "unknown")
        if rid and rid not in seen:
            seen.add(rid)
            runs.append({"run_id": rid, "source": src})
    return runs


def _fetch_metrics_for_run(run_id: str, source: str) -> dict[str, float]:
    src_filter = f', source="{source}"' if source and source != "*" else ""
    metrics: dict[str, float] = {}
    for name, prom_metric in PROM_METRICS.items():
        val = _prom_scalar(f'{prom_metric}{{run_id="{run_id}"{src_filter}}}')
        if val is not None:
            metrics[name] = val
    return metrics


# ── Postgres helpers ───────────────────────────────────────────────────────────

def _pg_anomaly_count(run_id: str) -> int:
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM anomaly_reports WHERE run_id = %s",
                    (run_id,),
                )
                return cur.fetchone()[0]
        finally:
            conn.close()
    except Exception:
        return 0


def _pg_block_count(run_id: str) -> int:
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM block_trace WHERE run_id = %s",
                    (run_id,),
                )
                return cur.fetchone()[0]
        finally:
            conn.close()
    except Exception:
        return 0


# ── MLflow logging ─────────────────────────────────────────────────────────────

def _get_or_create_experiment(mlflow_client: Any, source: str) -> str:
    import mlflow
    name = f"mip-pipeline/{source}" if source and source not in ("*", "unknown") else "mip-pipeline/other"
    exp = mlflow_client.get_experiment_by_name(name)
    if exp:
        return exp.experiment_id
    return mlflow_client.create_experiment(name)


def log_run_to_mlflow(state: dict) -> None:
    """Log a completed pipeline run to MLflow. Called from save_output_node.
    Wrapped in try/except — never raises, never blocks pipeline.
    """
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        source    = state.get("source_name", "unknown")
        run_id    = state.get("run_id", "unknown")
        domain    = state.get("domain", "unknown")
        mode      = state.get("pipeline_mode", "full")
        df        = state.get("result_df")

        experiment_name = f"mip-pipeline/{source}" if source and source not in ("*", "unknown") else "mip-pipeline/other"
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=run_id):
            # params
            mlflow.log_params({
                "run_id":        run_id,
                "source":        source,
                "domain":        domain,
                "pipeline_mode": mode,
                "with_critic":   str(state.get("with_critic", False)),
            })

            # metrics from state
            if df is not None:
                if "dq_score_pre" in df.columns:
                    mlflow.log_metric("dq_score_pre",  float(df["dq_score_pre"].mean()))
                if "dq_score_post" in df.columns:
                    mlflow.log_metric("dq_score_post", float(df["dq_score_post"].mean()))
                if "dq_delta" in df.columns:
                    mlflow.log_metric("dq_delta",      float(df["dq_delta"].mean()))
                mlflow.log_metric("rows_out", float(len(df)))

            rows_in = state.get("rows_in")
            if rows_in is not None:
                mlflow.log_metric("rows_in", float(rows_in))

            cost = state.get("llm_cost_usd")
            if cost is not None:
                mlflow.log_metric("cost_usd", float(cost))

            llm_calls = state.get("llm_calls")
            if llm_calls is not None:
                mlflow.log_metric("llm_calls", float(llm_calls))

            # enrichment tier counts from state if present
            for tier, key in [("s1_count", "s1_resolved"), ("s2_count", "s2_resolved"),
                               ("s3_count", "s3_resolved"), ("s4_unresolved", "unresolved")]:
                val = state.get(key)
                if val is not None:
                    mlflow.log_metric(tier, float(val))

            # supplement missing metrics from Prometheus
            prom_metrics = _fetch_metrics_for_run(run_id, source)
            for name, val in prom_metrics.items():
                try:
                    mlflow.log_metric(name, val)
                except Exception:
                    pass

            # anomaly count from Postgres
            anomaly_count = _pg_anomaly_count(run_id)
            mlflow.log_metric("anomaly_count", float(anomaly_count))

            block_count = _pg_block_count(run_id)
            mlflow.log_metric("block_count", float(block_count))

    except Exception as exc:
        logger.warning("MLflow logging failed (non-fatal): %s", exc)


# ── backfill ───────────────────────────────────────────────────────────────────

def backfill_from_prometheus(dry_run: bool = False) -> None:
    """Read all historical runs from Prometheus + Postgres → log to MLflow."""
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    runs = _fetch_all_run_ids()
    logger.info("Found %d runs in Prometheus", len(runs))
    print(f"Backfilling {len(runs)} runs into MLflow at {MLFLOW_TRACKING_URI}...")

    skipped = 0
    logged  = 0

    for entry in runs:
        run_id = entry["run_id"]
        source = entry["source"]

        exp_name = f"mip-pipeline/{source}" if source and source not in ("*", "unknown") else "mip-pipeline/other"

        # skip if already logged (search by run_name tag)
        exp = client.get_experiment_by_name(exp_name)
        if exp:
            existing = client.search_runs(
                experiment_ids=[exp.experiment_id],
                filter_string=f"tags.mlflow.runName = '{run_id}'",
                max_results=1,
            )
            if existing:
                skipped += 1
                continue

        metrics = _fetch_metrics_for_run(run_id, source)
        if not metrics:
            continue  # no data in Prometheus for this run

        anomaly_count = _pg_anomaly_count(run_id)
        block_count   = _pg_block_count(run_id)

        if dry_run:
            print(f"  [DRY RUN] {run_id} ({source}) → {len(metrics)} metrics")
            continue

        if not dry_run:
            mlflow.set_experiment(exp_name)
            with mlflow.start_run(run_name=run_id):
                mlflow.log_params({"run_id": run_id, "source": source})
                for name, val in metrics.items():
                    try:
                        mlflow.log_metric(name, val)
                    except Exception:
                        pass
                mlflow.log_metric("anomaly_count", float(anomaly_count))
                mlflow.log_metric("block_count",   float(block_count))
            logged += 1
            print(f"  ✓ {run_id} ({source}) — {len(metrics)} metrics")

    print(f"\nDone. Logged: {logged}, Skipped (already exist): {skipped}")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    dry = "--dry-run" in sys.argv
    backfill_from_prometheus(dry_run=dry)
