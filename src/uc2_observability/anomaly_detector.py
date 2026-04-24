"""
UC2 Observability Layer — Anomaly Detector

Queries Prometheus for the last N pipeline runs of key metrics, builds a
feature matrix (one row per run), runs Isolation Forest, and for any
outlier run:
  1. Pushes etl_anomaly_flag=1 to Prometheus Pushgateway.
  2. Inserts a row into the anomaly_reports Postgres table.

The detector is called after each `run_completed` Kafka event and also
runs as a scheduled job every hour.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
import requests
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from .metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

# ── configuration ──────────────────────────────────────────────────────────────

import os as _os
PROMETHEUS_URL = _os.getenv("UC2_PROMETHEUS_URL", "http://localhost:9090")
PG_DSN = _os.getenv("UC2_PG_DSN", "host=localhost port=5432 dbname=uc2 user=mip password=REMOVED_PG_PASSWORD")

# PromQL expressions used to build the feature matrix.
# Each entry: (feature_name, promql_template)
# {source} is substituted at call time.
_FEATURE_QUERIES: list[tuple[str, str]] = [
    ("null_rate",   'etl_null_rate{{source="{source}"}}'),
    ("dq_score",    'etl_dq_score_post{{source="{source}"}}'),
    ("dedup_rate",  'etl_dedup_rate{{source="{source}"}}'),
    ("rows_out",    'etl_rows_out{{source="{source}"}}'),
    ("cost_usd",    'etl_llm_cost_usd_total{{source="{source}"}}'),
]

_INSERT_ANOMALY = """
INSERT INTO anomaly_reports
    (run_id, source, signal, score, details, ts)
VALUES
    (%(run_id)s, %(source)s, %(signal)s, %(score)s, %(details)s, %(ts)s)
ON CONFLICT DO NOTHING;
"""

CONTAMINATION = 0.15   # expected fraction of anomalous runs
RANDOM_STATE = 42


# ── Prometheus helpers ─────────────────────────────────────────────────────────

def _prom_query_all_runs(query: str) -> dict[str, float]:
    """
    Instant query returning {run_id: value} for every run_id label found.
    Each push to Pushgateway with a distinct run_id creates a separate
    time-series — this collects the latest value per run_id.
    """
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return {}
        results = data.get("data", {}).get("result", [])
        out = {}
        for series in results:
            run_id = series.get("metric", {}).get("run_id", series.get("metric", {}).get("instance", str(len(out))))
            val = series.get("value", [None, "NaN"])[1]
            if val != "NaN":
                out[run_id] = float(val)
        return out
    except Exception as exc:
        logger.warning("Prometheus query failed (%r): %s", query, exc)
        return {}


def _prom_instant(query: str) -> float | None:
    """Execute a Prometheus instant query and return the scalar value."""
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return None
        results = data.get("data", {}).get("result", [])
        if not results:
            return None
        return float(results[0]["value"][1])
    except Exception as exc:
        logger.warning("Prometheus instant query failed (%r): %s", query, exc)
        return None


# ── feature matrix builder ─────────────────────────────────────────────────────

def _build_feature_matrix(source: str, n_runs: int) -> tuple[np.ndarray, list[str], list[str]]:
    """
    Query Prometheus for each feature across all run_ids for this source.
    Each distinct run_id pushed to Pushgateway becomes one row in the matrix.

    Returns:
        matrix       — shape (n_samples, n_features)
        feature_names
        run_ids      — run_id label per row
    """
    feature_names = [f for f, _ in _FEATURE_QUERIES]

    # For each feature, get {run_id: value} from Prometheus instant query
    per_feature: dict[str, dict[str, float]] = {}
    for feat_name, query_template in _FEATURE_QUERIES:
        query = query_template.format(source=source)
        per_feature[feat_name] = _prom_query_all_runs(query)

    # Union of all run_ids seen across any feature
    all_run_ids: list[str] = sorted(
        {rid for vals in per_feature.values() for rid in vals}
    )[-n_runs:]

    if not all_run_ids:
        return np.empty((0, len(feature_names))), feature_names, []

    rows = []
    for run_id in all_run_ids:
        row = [per_feature[f].get(run_id, np.nan) for f in feature_names]
        rows.append(row)

    matrix = np.array(rows, dtype=float)
    # Impute NaN with column means so IsolationForest doesn't fail
    if matrix.shape[0] >= 1:
        with np.errstate(all="ignore"):
            col_means = np.nanmean(matrix, axis=0)
        col_means = np.where(np.isnan(col_means), 0.0, col_means)
        inds = np.where(np.isnan(matrix))
        matrix[inds] = np.take(col_means, inds[1])

    return matrix, feature_names, all_run_ids


# ── main detector class ────────────────────────────────────────────────────────

class AnomalyDetector:
    """
    Isolation Forest anomaly detection on pipeline run metrics pulled from
    Prometheus.  Call `run_detection(source)` after each pipeline run.
    """

    def __init__(
        self,
        pushgateway_url: str = "localhost:9091",
        contamination: float = CONTAMINATION,
    ) -> None:
        self.metrics_collector = MetricsCollector(pushgateway_url)
        self.contamination = contamination

    def _insert_anomaly_report(
        self,
        run_id: str,
        source: str,
        anomaly_score: float,
        feature_names: list[str],
        feature_values: list[float],
        flagged_signals: list[str],
    ) -> None:
        details_json = json.dumps({
            "features":        dict(zip(feature_names, feature_values)),
            "flagged_signals": flagged_signals,
            "anomaly_score":   anomaly_score,
        })
        # One row per flagged signal (matches table schema: one signal per row)
        signal_str = ", ".join(flagged_signals) if flagged_signals else "general_outlier"
        pg_conn = psycopg2.connect(PG_DSN)
        try:
            with pg_conn.cursor() as cur:
                cur.execute(_INSERT_ANOMALY, {
                    "run_id":  run_id,
                    "source":  source,
                    "signal":  signal_str[:500],
                    "score":   anomaly_score,
                    "details": details_json,
                    "ts":      datetime.now(timezone.utc),
                })
            pg_conn.commit()
            logger.info("Inserted anomaly_report for run_id=%s source=%s", run_id, source)
        except psycopg2.Error as exc:
            pg_conn.rollback()
            logger.error("Failed to insert anomaly_report: %s", exc)
        finally:
            pg_conn.close()

    def _identify_signals(
        self,
        feature_names: list[str],
        feature_values: np.ndarray,
        baseline: np.ndarray,
    ) -> list[str]:
        """
        Return human-readable signal names where the latest run deviates
        most from the historical mean (> 2 std deviations).
        """
        signals = []
        std = np.std(baseline, axis=0) + 1e-9
        mean = np.mean(baseline, axis=0)
        z_scores = np.abs((feature_values - mean) / std)
        for i, z in enumerate(z_scores):
            if z > 2.0:
                signals.append(
                    f"{feature_names[i]}_spike (z={z:.2f}, "
                    f"value={feature_values[i]:.4f}, mean={mean[i]:.4f})"
                )
        return signals if signals else ["general_outlier"]

    def run_detection(self, source: str, n_runs: int = 20) -> list[dict[str, Any]]:
        """
        Detect anomalies for the given source using Isolation Forest.

        Returns a list of anomaly report dicts (may be empty if no outliers).
        """
        matrix, feature_names, run_ids = _build_feature_matrix(source, n_runs)

        if matrix.shape[0] < 5:
            logger.warning(
                "Not enough data for anomaly detection (source=%s, samples=%d); "
                "need at least 5.",
                source, matrix.shape[0],
            )
            return []

        scaler = StandardScaler()
        X = scaler.fit_transform(matrix)

        clf = IsolationForest(
            contamination=self.contamination,
            random_state=RANDOM_STATE,
            n_estimators=100,
        )
        clf.fit(X)

        # scores_samples returns negative values; more negative = more anomalous
        scores = clf.score_samples(X)
        predictions = clf.predict(X)  # -1 = outlier, 1 = normal

        reports = []
        for i, (pred, score) in enumerate(zip(predictions, scores)):
            if pred != -1:
                continue

            run_id = run_ids[i] if i < len(run_ids) else f"{source}_ts{int(time.time())}"
            feature_values = matrix[i]
            signals = self._identify_signals(feature_names, feature_values, matrix)

            logger.warning(
                "ANOMALY DETECTED source=%s run_id=%s score=%.4f signals=%s",
                source, run_id, score, signals,
            )

            # 1. Push anomaly flag to Pushgateway
            for signal in signals:
                try:
                    self.metrics_collector.push_anomaly_flag(
                        run_id=run_id,
                        source=source,
                        signal=signal[:64],  # label value length limit
                        value=1.0,
                    )
                except Exception as exc:
                    logger.error("Failed to push anomaly flag: %s", exc)

            # 2. Write to Postgres anomaly_reports
            self._insert_anomaly_report(
                run_id=run_id,
                source=source,
                anomaly_score=float(score),
                feature_names=feature_names,
                feature_values=feature_values.tolist(),
                flagged_signals=signals,
            )

            reports.append({
                "run_id":          run_id,
                "source":          source,
                "anomaly_score":   float(score),
                "features":        dict(zip(feature_names, feature_values.tolist())),
                "flagged_signals": signals,
            })

        if not reports:
            logger.info("No anomalies detected for source=%s (checked %d runs).",
                        source, matrix.shape[0])

        return reports


# ── entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Scheduled runner: detect anomalies for all known sources every hour.
    """
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="UC2 Anomaly Detector")
    parser.add_argument("--source", default="ALL",
                        help="Source to check (OFF, USDA, openFDA, ESCI, or ALL)")
    parser.add_argument("--n-runs", type=int, default=20,
                        help="Number of past runs to consider")
    parser.add_argument("--once", action="store_true",
                        help="Run once then exit (default: loop every hour)")
    args = parser.parse_args()

    detector = AnomalyDetector()
    sources = ["OFF", "USDA", "openFDA", "ESCI"] if args.source == "ALL" else [args.source]

    while True:
        for src in sources:
            try:
                reports = detector.run_detection(src, n_runs=args.n_runs)
                logger.info("source=%s: %d anomalies detected.", src, len(reports))
            except Exception as exc:
                logger.error("Detector error for source=%s: %s", src, exc)
        if args.once:
            break
        time.sleep(3600)


if __name__ == "__main__":
    main()
