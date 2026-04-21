"""
UC2 Chunker DAG — Postgres audit_events → ChromaDB embeddings.

Runs every 5 minutes. Picks up new audit events since last cursor,
embeds them with all-MiniLM-L6-v2, and upserts into ChromaDB audit_corpus.

This is a proper Airflow DAG wrapping the standalone chunker module so it
gets retries, alerting, and history tracking like any other pipeline job.
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
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}


def run_chunker(**kwargs):
    from src.uc2_observability.chunker import chunk_new_events
    rows_processed = chunk_new_events()
    print(f"Chunker: embedded {rows_processed} new audit events into ChromaDB.")
    return rows_processed


with DAG(
    dag_id="uc2_chunker",
    default_args=default_args,
    description="UC2: embed new Postgres audit events into ChromaDB every 5 min",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["uc2", "observability", "chromadb"],
) as dag:

    chunk = PythonOperator(
        task_id="chunk_new_events",
        python_callable=run_chunker,
        execution_timeout=timedelta(minutes=4),
    )
