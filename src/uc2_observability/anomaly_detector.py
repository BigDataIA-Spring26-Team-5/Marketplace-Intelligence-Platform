"""
UC2 Observability Layer — Anomaly Detector

Queries Prometheus for the last N pipeline runs of key metrics, builds a
feature matrix (one row per run), runs Isolation Forest, and for any
outlier run:
  1. Pushes uc1_anomaly_flag=1 to Prometheus Pushgateway.
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

PROMETHEUS_URL = "http://localhost:9090"
PG_DSN = "host=localhost port=5432 dbname=uc2 user=mip password=mip_pass"

# PromQL expressions used to build the feature matrix.
# Each entry: (feature_name, promql_template)
# {source} is substituted at call time.
_FEATURE_QUERIES: list[tuple[str, str]] = [
    ("null_rate",   'uc1_null_rate{{source="{source}"}}'),
    ("dq_score",    'uc1_dq_score_post{{source="{source}"}}'),
    ("dedup_rate",  'uc1_dedup_rate{{source="{source}"}}'),
    ("rows_out",    'uc1_rows_out{{source="{source}"}}'),
    ("cost_usd",    'uc1_llm_cost_usd_total{{source="{source}"}}'),
]

_INSERT_ANOMALY = """
INSERT INTO anomaly_reports
    (run_id, source, anomaly_score, features, flagged_signals, detected_at)
VALUES
    (%(run_id)s, %(source)s, %(anomaly_score)s, %(features)s,
     %(flagged_signals)s, %(detected_at)s)
ON CONFLICT DO NOTHING;
"""

CONTAMINATION = 0.15   # expected fraction of anomalous runs
RANDOM_STATE = 42


# ── Prometheus helpers ─────────────────────────────────────────────────────────

def _prom_query_range(query: str, n_runs: int) -> list[tuple[float, float]]:
    """
    Execute a Prometheus instant-range query and return up to n_runs
    (timestamp, value) pairs from the result vector.
    """
    end = time.time()
    start = end - n_runs * 3600  # look back n_runs hours as a proxy
    step = max((end - start) / n_runs, 15)

    params = {
        "query": query,
        "start": start,
        "end":   end,
        "step":  step,
    }
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return []
        results = data.get("data", {}).get("result", [])
        if not results:
            return []
        # Take the first matching time-series
        values = results[0].get("values", [])
        return [(float(ts), float(v)) for ts, v in values if v != "NaN"]
    except Exception as exc:
        logger.warning("Prometheus query failed (%r): %s", query, exc)
        return []


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

def _build_feature_matrix(source: str, n_runs: int) -> tuple[np.ndarray, list[str], list[float]]:
    """
    Query Prometheus for each feature over the last n_runs time windows.
    Returns:
        matrix  — shape (n_samples, n_features), aligned by time-bucket
        feature_names
        timestamps  — approximate Unix timestamp per sample
    """
    series: dict[str, list[tuple[float, float]]] = {}
    for feat_name, query_template in _FEATURE_QUERIES:
        query = query_template.format(source=source)
        pairs = _prom_query_range(query, n_runs)
        series[feat_name] = pairs

    if not any(series.values()):
        return np.empty((0, len(_FEATURE_QUERIES))), [f for f, _ in _FEATURE_QUERIES], []

    # Align all series by round-bucketing timestamps to the nearest 300s
    bucket_size = 300
    buckets: dict[int, dict[str, float]] = {}
    for feat_name, pairs in series.items():
        for ts, val in pairs:
            bucket = int(ts // bucket_size) * bucket_size
            buckets.setdefault(bucket, {})[feat_name] = val

    feature_names = [f for f, _ in _FEATURE_QUERIES]
    sorted_buckets = sorted(buckets.keys())[-n_runs:]
    rows = []
    timestamps = []
    for bucket in sorted_buckets:
        row_vals = buckets[bucket]
        row = [row_vals.get(f, np.nan) for f in feature_names]
        rows.append(row)
        timestamps.append(float(bucket))

    matrix = np.array(rows, dtype=float)
    # Impute NaN with column means
    col_means = np.nanmean(matrix, axis=0)
    inds = np.where(np.isnan(matrix))
    matrix[inds] = np.take(col_means, inds[1])

    return matrix, feature_names, timestamps


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
        features_json = json.dumps(dict(zip(feature_names, feature_values)))
        signals_json = json.dumps(flagged_signals)
        pg_conn = psycopg2.connect(PG_DSN)
        try:
            with pg_conn.cursor() as cur:
                cur.execute(_INSERT_ANOMALY, {
                    "run_id":          run_id,
                    "source":          source,
                    "anomaly_score":   anomaly_score,
                    "features":        features_json,
                    "flagged_signals": signals_json,
                    "detected_at":     datetime.now(timezone.utc),
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
        matrix, feature_names, timestamps = _build_feature_matrix(source, n_runs)

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

            # Use timestamp as a synthetic run_id if we don't have a real one
            ts = timestamps[i] if i < len(timestamps) else time.time()
            run_id = f"{source}_ts{int(ts)}"
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
