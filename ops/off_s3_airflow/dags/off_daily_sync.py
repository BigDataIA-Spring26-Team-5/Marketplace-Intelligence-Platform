from __future__ import annotations

import datetime as dt
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
PROJECT_ROOT = os.getenv("OFF_PIPELINE_ROOT", AIRFLOW_HOME)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from jobs.sync_off_to_s3 import sync_off_to_s3


def _run_sync(**context):
    logical_date = context["logical_date"].strftime("%Y-%m-%d")
    return sync_off_to_s3(execution_date=logical_date)


with DAG(
    dag_id="open_food_facts_daily_to_s3",
    description="Download the daily Open Food Facts CSV export and push it to S3-compatible object storage.",
    start_date=dt.datetime(2026, 4, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "bhavya",
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=15),
    },
    tags=["off", "s3", "daily"],
) as dag:
    sync_task = PythonOperator(
        task_id="sync_open_food_facts_to_s3",
        python_callable=_run_sync,
    )

    sync_task
