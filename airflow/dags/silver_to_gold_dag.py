"""
Silver → Gold DAG.

Pipeline position:
  GCS silver (Parquet) → [dedup + enrichment] → BigQuery mip_gold.products

Flow per source (all 3 sources run in parallel):
  1. Read Silver Parquet for the current execution_date partition.
  2. Run gold block sequence: fuzzy_deduplicate → column_wise_merge →
     golden_record_select → extract_allergens → llm_enrich → dq_score_post.
  3. Append deduplicated + enriched rows to BQ mip_gold.products.

Schedule: daily 09:00 — after bronze_to_silver (07:00) has written Silver files.
  Extra 2-hour buffer lets Silver writes complete across all sources before Gold reads.

ExternalTaskSensor waits for bronze_to_silver DAG to complete before starting.

Gold BQ table: mip_gold.products (schema auto-detected via BQ load job, append mode).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────

SOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "off":     {"domain": "nutrition"},
    "usda":    {"domain": "nutrition"},
    "openfda": {"domain": "safety"},
}

default_args = {
    "owner": "mip",
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}


# ── per-source gold load ──────────────────────────────────────────────────────

def load_source_to_gold(source: str, **kwargs) -> int:
    """
    Read Silver Parquet for today's partition, run gold pipeline, write to BQ.
    Returns rows written to BigQuery.
    """
    from dotenv import load_dotenv
    load_dotenv()

    from src.pipeline.gold_pipeline import run_gold_pipeline

    # Execution date from Airflow context is the logical date of the DAG run
    execution_date: datetime = kwargs["execution_date"]
    date_partition = execution_date.strftime("%Y/%m/%d")

    cfg = SOURCE_CONFIG[source]
    print(f"[{source}] Gold pipeline for partition {date_partition} (domain={cfg['domain']})")

    rows = run_gold_pipeline(
        source_name=source,
        date=date_partition,
        domain=cfg["domain"],
    )
    print(f"[{source}] Gold complete: {rows} rows written to BQ.")
    return rows


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="silver_to_gold",
    default_args=default_args,
    description="Silver GCS Parquet → BigQuery mip_gold.products (dedup + enrichment, all sources parallel)",
    schedule="0 9 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "bigquery", "etl", "incremental"],
) as dag:

    # Wait for bronze_to_silver to succeed for the same execution_date
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_bronze_to_silver",
        external_dag_id="bronze_to_silver",
        external_task_id=None,          # wait for entire DAG to complete
        allowed_states=["success"],
        execution_delta=timedelta(hours=2),  # silver DAG ran at 07:00, gold at 09:00
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # All gold tasks run in parallel after the sensor clears
    for _source_name in SOURCE_CONFIG:
        gold_task = PythonOperator(
            task_id=f"gold_{_source_name}",
            python_callable=load_source_to_gold,
            op_kwargs={"source": _source_name},
            provide_context=True,
        )
        wait_for_silver >> gold_task
