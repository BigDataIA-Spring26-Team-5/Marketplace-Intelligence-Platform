"""
UC2 Anomaly Detector DAG — Isolation Forest on UC1 pipeline metrics.

Runs hourly. Queries Prometheus for the last N runs of each source,
scores them with Isolation Forest, and pushes uc1_anomaly_flag=1 to
Pushgateway + writes anomaly_reports to Postgres when an outlier is found.

Needs at least 5 completed UC1 runs per source to activate (returns early
with a warning if fewer runs are available).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

WORKSPACE = os.getenv("MIP_WORKSPACE", "/home/bhavyalikhitha_bbl/bhavya-workspace")
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)

default_args = {
    "owner": "mip",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def run_anomaly_detector(**kwargs):
    from src.uc2_observability.anomaly_detector import AnomalyDetector
    detector = AnomalyDetector()
    total_flagged = 0
    for source in ["OFF", "USDA", "openFDA", "ESCI"]:
        reports = detector.run_detection(source, n_runs=20)
        total_flagged += len(reports)
        for r in reports:
            print(f"  ANOMALY: source={r['source']} run_id={r['run_id']} score={r['anomaly_score']:.4f} signals={r['flagged_signals']}")
    print(f"Anomaly detector complete: {total_flagged} anomalies flagged.")
    return total_flagged


with DAG(
    dag_id="uc2_anomaly_detector",
    default_args=default_args,
    description="UC2: Isolation Forest anomaly detection on UC1 Prometheus metrics (hourly)",
    schedule="0 * * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["uc2", "observability", "anomaly"],
) as dag:

    detect = PythonOperator(
        task_id="run_detection",
        python_callable=run_anomaly_detector,
        execution_timeout=timedelta(minutes=10),
    )
