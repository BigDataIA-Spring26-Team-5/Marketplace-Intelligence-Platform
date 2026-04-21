"""
UC2 Observability Layer — Anomaly Detection (public re-export).

The full implementation (Isolation Forest on Prometheus metrics,
Pushgateway push-back, Postgres anomaly_reports insert) lives in
anomaly_detector.py.  This module re-exports the public API so that
existing imports of `uc2_observability.anomaly_detection` continue to
work.
"""

from .anomaly_detector import AnomalyDetector, main  # noqa: F401

__all__ = ["AnomalyDetector", "main"]
