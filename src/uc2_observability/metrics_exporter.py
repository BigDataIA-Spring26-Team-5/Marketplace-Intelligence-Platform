"""MetricsExporter: push run metrics to Prometheus Pushgateway."""

from __future__ import annotations

import logging

try:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    CollectorRegistry = None  # type: ignore[assignment,misc]
    Gauge = None  # type: ignore[assignment,misc]
    push_to_gateway = None  # type: ignore[assignment]
    _PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

_STATUS_VALUES = {"success": 1.0, "partial": 0.5, "failed": 0.0}


import os as _os
_DEFAULT_PUSHGATEWAY = _os.getenv("UC2_PUSHGATEWAY_URL", "localhost:9091")


class MetricsExporter:
    def __init__(self, pushgateway_url: str = _DEFAULT_PUSHGATEWAY, job: str = "etl_pipeline"):
        self.pushgateway_url = pushgateway_url
        self.job = job

    def push(self, run_log: dict) -> bool:
        """Push run metrics to Pushgateway. Returns True on success. Never raises."""
        try:
            if push_to_gateway is None:
                raise ImportError("prometheus_client not installed")

            run_id = run_log.get("run_id", "unknown")
            source_name = run_log.get("source_name", "unknown")
            status = run_log.get("status", "unknown")
            enrichment = run_log.get("enrichment_stats") or {}

            registry = CollectorRegistry()
            labels = ["source_name", "status", "run_id"]
            label_vals = [source_name, status, run_id]
            short_labels = ["source_name", "run_id"]
            short_vals = [source_name, run_id]

            def _gauge(name: str, value: float, lnames: list, lvals: list) -> None:
                g = Gauge(name, name, lnames, registry=registry)
                g.labels(*lvals).set(value)

            _gauge("etl_dq_score_pre", float(run_log.get("dq_score_pre") or 0.0), labels, label_vals)
            _gauge("etl_dq_score_post", float(run_log.get("dq_score_post") or 0.0), labels, label_vals)
            _gauge("etl_dq_delta", float(run_log.get("dq_delta") or 0.0), labels, label_vals)
            _gauge("etl_rows_in", float(run_log.get("rows_in") or 0), labels, label_vals)
            _gauge("etl_rows_out", float(run_log.get("rows_out") or 0), labels, label_vals)
            _gauge("etl_rows_quarantined", float(run_log.get("rows_quarantined") or 0), labels, label_vals)
            _gauge("etl_duration_seconds", float(run_log.get("duration_seconds") or 0.0), labels, label_vals)
            _gauge("etl_enrichment_s1_resolved", float(enrichment.get("deterministic") or 0), short_labels, short_vals)
            _gauge("etl_enrichment_s2_resolved", float(enrichment.get("embedding") or 0), short_labels, short_vals)
            _gauge("etl_enrichment_s3_resolved", float(enrichment.get("llm") or 0), short_labels, short_vals)
            _gauge("etl_enrichment_unresolved", float(enrichment.get("unresolved") or 0), short_labels, short_vals)
            _gauge("etl_corpus_augmented", float(enrichment.get("corpus_augmented") or 0), short_labels, short_vals)
            _gauge("etl_corpus_size_after", float(enrichment.get("corpus_size_after") or 0), short_labels, short_vals)
            _gauge("etl_run_status", _STATUS_VALUES.get(status, 0.0), short_labels, short_vals)

            push_to_gateway(
                self.pushgateway_url,
                job=self.job,
                grouping_key={"run_id": run_id},
                registry=registry,
            )
            logger.info(f"Metrics pushed to Pushgateway for run_id={run_id}")
            return True
        except Exception as exc:
            logger.warning(f"MetricsExporter.push failed: {exc}")
            return False
