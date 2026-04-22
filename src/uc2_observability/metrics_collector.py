"""
UC2 Observability Layer — Metrics Collector

Pushes per-run pipeline metrics to Prometheus Pushgateway using the
batch-job pattern (push, don't scrape). Each metric carries `source`
and `run_id` labels so Grafana can slice by source.
"""

from __future__ import annotations

import logging
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

logger = logging.getLogger(__name__)

import os
PUSHGATEWAY_URL = os.getenv("UC2_PUSHGATEWAY_URL", "localhost:9091")

# Metric definitions: (metric_name, prometheus_name, type, description)
_GAUGE_METRICS = [
    ("rows_in",               "etl_rows_in",               "Rows entering the pipeline run"),
    ("rows_out",              "etl_rows_out",              "Rows exiting the pipeline run"),
    ("null_rate",             "etl_null_rate",             "Mean null rate across all fields"),
    ("dq_score_pre",          "etl_dq_score_pre",          "Mean DQ score before enrichment"),
    ("dq_score_post",         "etl_dq_score_post",         "Mean DQ score after enrichment"),
    ("dq_delta",              "etl_dq_delta",              "Mean DQ delta (post - pre)"),
    ("dedup_rate",            "etl_dedup_rate",            "Fraction of rows identified as duplicates"),
    ("s1_count",              "etl_enrichment_s1_resolved","Rows resolved by S1 deterministic enrichment"),
    ("s2_count",              "etl_enrichment_s2_resolved","Rows resolved by S2 KNN enrichment"),
    ("s3_count",              "etl_enrichment_s3_resolved","Rows resolved by S3 RAG-LLM enrichment"),
    ("s4_count",              "etl_enrichment_unresolved", "Rows resolved by S4 fallback enrichment"),
    ("quarantine_rows",       "etl_rows_quarantined",      "Rows sent to quarantine"),
    ("block_duration_seconds","etl_duration_seconds",      "Total pipeline wall-clock duration in seconds"),
]

_COUNTER_METRICS = [
    ("llm_calls",  "etl_llm_calls_total",   "Total LLM API calls made"),
    ("cost_usd",   "etl_llm_cost_usd_total", "Total LLM cost in USD"),
]


class MetricsCollector:
    """
    Pushes pipeline run metrics to Prometheus Pushgateway.

    Usage::

        collector = MetricsCollector()
        collector.push(
            run_id="run_20240328_001",
            source="OFF",
            metrics_dict={
                "rows_in": 5000,
                "rows_out": 4953,
                "null_rate": 0.04,
                "dq_score_pre": 0.61,
                "dq_score_post": 0.87,
                "dq_delta": 0.26,
                "dedup_rate": 0.03,
                "llm_calls": 42,
                "cost_usd": 0.017,
                "s1_count": 3200,
                "s2_count": 1100,
                "s3_count": 620,
                "s4_count": 33,
                "quarantine_rows": 47,
                "block_duration_seconds": 38.4,
            },
        )
    """

    def __init__(self, pushgateway_url: str = PUSHGATEWAY_URL) -> None:
        self.pushgateway_url = pushgateway_url

    def push(self, run_id: str, source: str, metrics_dict: dict[str, Any]) -> None:
        """
        Build a fresh CollectorRegistry, register all metrics with
        (source, run_id) labels, set their values from metrics_dict,
        then push atomically to Pushgateway.

        A fresh registry per call avoids duplicate-registration errors
        when the same process calls push() multiple times.
        """
        registry = CollectorRegistry()
        labels = ["source", "run_id"]
        label_values = [source, run_id]

        # --- gauges ---
        for key, prom_name, description in _GAUGE_METRICS:
            if key not in metrics_dict:
                continue
            g = Gauge(prom_name, description, labels, registry=registry)
            try:
                g.labels(*label_values).set(float(metrics_dict[key]))
            except (TypeError, ValueError) as exc:
                logger.warning("Could not set gauge %s=%r: %s", key, metrics_dict[key], exc)

        # --- counters ---
        for key, prom_name, description in _COUNTER_METRICS:
            if key not in metrics_dict:
                continue
            c = Counter(prom_name, description, labels, registry=registry)
            try:
                c.labels(*label_values).inc(float(metrics_dict[key]))
            except (TypeError, ValueError) as exc:
                logger.warning("Could not increment counter %s=%r: %s", key, metrics_dict[key], exc)

        # --- run-level counters (always increment by 1) ---
        run_completed = Counter(
            "etl_run_completed_total",
            "Total completed pipeline runs",
            ["source", "run_id", "status"],
            registry=registry,
        )
        run_completed.labels(source, run_id, metrics_dict.get("status", "success")).inc()

        # --- push ---
        job_name = f"etl_pipeline_{source}"
        try:
            push_to_gateway(
                self.pushgateway_url,
                job=job_name,
                registry=registry,
                grouping_key={"run_id": run_id, "source": source},
            )
            logger.info("Pushed %d metrics to Pushgateway for run_id=%s source=%s",
                        len(metrics_dict), run_id, source)
        except Exception as exc:
            logger.error("Failed to push metrics to Pushgateway (%s): %s",
                         self.pushgateway_url, exc)
            raise

    def push_anomaly_flag(self, run_id: str, source: str, signal: str, value: float = 1.0) -> None:
        """
        Push etl_anomaly_flag gauge to Pushgateway.
        Called by the anomaly detector when an outlier run is detected.
        """
        registry = CollectorRegistry()
        g = Gauge(
            "etl_anomaly_flag",
            "Isolation Forest anomaly flag (1 = outlier)",
            ["source", "run_id", "signal"],
            registry=registry,
        )
        g.labels(source, run_id, signal).set(value)
        job_name = f"etl_anomaly_{source}"
        try:
            push_to_gateway(
                self.pushgateway_url,
                job=job_name,
                registry=registry,
                grouping_key={"run_id": run_id, "source": source, "signal": signal},
            )
            logger.info("Pushed anomaly_flag=%s for run_id=%s source=%s signal=%s",
                        value, run_id, source, signal)
        except Exception as exc:
            logger.error("Failed to push anomaly flag: %s", exc)
            raise
