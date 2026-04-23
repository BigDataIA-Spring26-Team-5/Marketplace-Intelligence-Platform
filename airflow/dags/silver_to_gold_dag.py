"""
Silver → Gold DAG.

Pipeline position:
  GCS silver (Parquet) → [dedup + enrichment] → BigQuery mip_gold.products
                                               → GCS mip-gold-2024/{domain}/{date}/

Flow:
  1. Per-source tasks (parallel): read Silver Parquet → gold blocks → append to BQ.
  2. Per-domain fan-in tasks: concatenate all source Silver Parquets for the domain
     → cross-source dedup → write canonical Parquet to gs://mip-gold-2024/{domain}/.

Domain fan-in waits for all per-source BQ tasks in the domain to finish, then
writes the unified deduplicated view to GCS Gold.

Schedule: daily 09:00 — after bronze_to_silver (07:00) has written Silver files.
ExternalTaskSensor waits for bronze_to_silver DAG to complete before starting.
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

# Which sources contribute to each domain (for the GCS Gold fan-in step)
DOMAIN_SOURCES: dict[str, list[str]] = {
    "nutrition": ["off", "usda"],
    "safety":    ["openfda"],
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


# ── domain GCS gold fan-in ────────────────────────────────────────────────────

def write_domain_to_gcs_gold(domain: str, sources: list[str], **kwargs) -> int:
    """
    Concatenate Silver Parquet for all sources in domain, cross-source dedup,
    and write canonical Gold records to gs://mip-gold-2024/{domain}/{date}/.
    Returns rows written.
    """
    from dotenv import load_dotenv
    load_dotenv()

    from src.pipeline.gold_pipeline import run_domain_gold_gcs

    execution_date: datetime = kwargs["execution_date"]
    date_partition = execution_date.strftime("%Y/%m/%d")

    print(f"[{domain}] GCS Gold fan-in for partition {date_partition} (sources={sources})")
    rows = run_domain_gold_gcs(domain=domain, date=date_partition, sources=sources)
    print(f"[{domain}] GCS Gold complete: {rows} rows → gs://mip-gold-2024/{domain}/{date_partition}/")
    return rows


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="silver_to_gold",
    default_args=default_args,
    description="Silver GCS Parquet → BigQuery mip_gold.products + GCS mip-gold-2024/{domain}/ (dedup + enrichment)",
    schedule="0 9 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "bigquery", "gcs", "etl", "incremental"],
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

    # Per-source BQ tasks — run in parallel after sensor clears
    source_tasks: dict[str, PythonOperator] = {}
    for _source_name in SOURCE_CONFIG:
        source_tasks[_source_name] = PythonOperator(
            task_id=f"gold_{_source_name}",
            python_callable=load_source_to_gold,
            op_kwargs={"source": _source_name},
            provide_context=True,
        )
        wait_for_silver >> source_tasks[_source_name]

    # Per-domain GCS fan-in tasks — run after all per-source BQ tasks for that domain
    for _domain, _sources in DOMAIN_SOURCES.items():
        gcs_gold_task = PythonOperator(
            task_id=f"gold_gcs_{_domain}",
            python_callable=write_domain_to_gcs_gold,
            op_kwargs={"domain": _domain, "sources": _sources},
            provide_context=True,
        )
        for _src in _sources:
            source_tasks[_src] >> gcs_gold_task
