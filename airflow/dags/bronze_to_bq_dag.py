"""
Bronze → BigQuery incremental load DAG.

Pipeline position:
  GCS bronze (JSONL) → BigQuery → [UC1 trigger — NOT YET WIRED]

Flow per source:
  1. Read watermark from GCS (_watermarks/{source}_bq_watermark.json)
     to know which GCS date-partitions are new since last load.
  2. bq load --append each new partition into bronze_raw.{source}.
  3. Update watermark.
  4. UC1 trigger is a placeholder task — commented out until UC1 is ready.

Schedule: runs 30 min after each incremental ingest DAG completes.
  - OFF:     daily  05:00 (ingest runs 04:00)
  - USDA:    monthly 1st  03:00 (ingest runs 02:00)
  - openFDA: daily  06:00 (ingest runs 05:00)
  - ESCI:    @once  (already loaded, no increment)

Watermark key per source:
  gs://mip-bronze-2024/_watermarks/off_bq_watermark.json
  gs://mip-bronze-2024/_watermarks/usda_bq_watermark.json
  gs://mip-bronze-2024/_watermarks/openfda_bq_watermark.json
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

# ── config ─────────────────────────────────────────────────────────────────────

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
BQ_PROJECT     = os.getenv("GCP_PROJECT", "mip-platform-2024")
BQ_DATASET     = "bronze_raw"

# GCS prefix, BQ table name, incremental type for each source
SOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "off": {
        "gcs_prefix":    "off/",
        "bq_table":      "off",
        "watermark_key": "_watermarks/off_bq_watermark.json",
        "partition_depth": 3,   # off/YYYY/MM/DD/
    },
    "usda": {
        "gcs_prefix":    "usda/",
        "bq_table":      "usda_branded",   # load into existing split tables
        "watermark_key": "_watermarks/usda_bq_watermark.json",
        "partition_depth": 3,   # usda/YYYY/MM/DD/
    },
    "openfda": {
        "gcs_prefix":    "openfda/",
        "bq_table":      "openfda",
        "watermark_key": "_watermarks/openfda_bq_watermark.json",
        "partition_depth": 3,   # openfda/YYYY/MM/DD/
    },
}

default_args = {
    "owner": "mip",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


# ── helpers ─────────────────────────────────────────────────────────────────────

def _gcs():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _read_bq_watermark(client, watermark_key: str) -> str | None:
    """Return last loaded GCS partition date, or None if first run."""
    try:
        obj = client.get_object(Bucket=BRONZE_BUCKET, Key=watermark_key)
        return json.loads(obj["Body"].read()).get("last_partition")
    except client.exceptions.NoSuchKey:
        return None


def _write_bq_watermark(client, watermark_key: str, partition: str) -> None:
    body = json.dumps({"last_partition": partition, "updated_at": datetime.utcnow().isoformat()})
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=watermark_key,
        Body=body.encode(), ContentType="application/json",
    )


def _list_gcs_partitions(client, prefix: str, depth: int) -> list[str]:
    """
    List all date-partition prefixes under the given GCS prefix.
    depth=3 → finds paths like off/2026/04/21/ (YYYY/MM/DD)
    Returns sorted list of partition strings like "2026/04/21".
    """
    paginator = client.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            all_keys.append(obj["Key"])

    partitions = set()
    for key in all_keys:
        # Strip the source prefix and extract the date partition portion
        rest = key[len(prefix):]
        parts = rest.split("/")
        if len(parts) >= depth:
            partition = "/".join(parts[:depth])
            partitions.add(partition)

    return sorted(partitions)


def _bq_load(gcs_uri: str, table: str, append: bool = True) -> None:
    """
    Run bq load to load a GCS JSONL path into BigQuery.
    Uses --append_table so existing data is preserved.
    """
    cmd = [
        "bq", "load",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--autodetect",
        f"--max_bad_records=10000",
        "--noreplace" if append else "--replace",
        f"{BQ_PROJECT}:{BQ_DATASET}.{table}",
        gcs_uri,
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"bq load failed:\n{result.stderr}")
    print(result.stdout.strip() or "bq load OK")


# ── per-source load function ────────────────────────────────────────────────────

def load_source_to_bq(source: str, **kwargs) -> int:
    """
    Find all GCS partitions newer than the watermark and bq load them.
    Returns number of partitions loaded.
    """
    cfg      = SOURCE_CONFIG[source]
    client   = _gcs()

    last_partition = _read_bq_watermark(client, cfg["watermark_key"])
    print(f"[{source}] BQ watermark: {last_partition or 'none (first run)'}")

    all_partitions = _list_gcs_partitions(client, cfg["gcs_prefix"], cfg["partition_depth"])
    if not all_partitions:
        print(f"[{source}] No GCS partitions found under {cfg['gcs_prefix']}. Nothing to load.")
        return 0

    # Filter to only partitions newer than the watermark
    new_partitions = [
        p for p in all_partitions
        if last_partition is None or p > last_partition
    ]

    if not new_partitions:
        print(f"[{source}] Already up to date (watermark={last_partition}). No new partitions.")
        return 0

    print(f"[{source}] {len(new_partitions)} new partition(s) to load: {new_partitions}")

    loaded = 0
    for partition in new_partitions:
        gcs_uri = f"gs://{BRONZE_BUCKET}/{cfg['gcs_prefix']}{partition}/*.jsonl"
        print(f"[{source}] Loading partition {partition} → {BQ_DATASET}.{cfg['bq_table']}")
        try:
            _bq_load(gcs_uri, cfg["bq_table"], append=(last_partition is not None or loaded > 0))
            _write_bq_watermark(client, cfg["watermark_key"], partition)
            loaded += 1
            print(f"[{source}] Partition {partition} loaded OK.")
        except Exception as exc:
            print(f"[{source}] ERROR loading {partition}: {exc}")
            # Stop here — watermark stays at last successful partition
            break

    print(f"[{source}] Done. Loaded {loaded}/{len(new_partitions)} partition(s).")
    return loaded


# ── UC1 trigger placeholder ─────────────────────────────────────────────────────

def trigger_uc1_pipeline(source: str, **kwargs) -> None:
    """
    Placeholder: trigger the UC1 pipeline for this source once new BQ data is loaded.

    NOT WIRED YET — UC1 pipeline runner location and invocation method TBD.

    When ready, this will:
      1. Resolve the latest BigQuery partition for this source.
      2. Call the UC1 pipeline runner (likely a BashOperator or TriggerDagRunOperator).
      3. UC1 will emit events to Kafka → UC2 will pick them up automatically.

    To activate: replace the print statement below with the actual trigger call.
    """
    # ── PLACEHOLDER: uncomment and fill in when UC1 is ready ──────────────────
    # from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    # TriggerDagRunOperator(
    #     task_id=f"trigger_uc1_{source}",
    #     trigger_dag_id="uc1_pipeline",
    #     conf={"source": source, "bq_table": f"{BQ_DATASET}.{SOURCE_CONFIG[source]['bq_table']}"},
    # ).execute(context=kwargs)
    # ──────────────────────────────────────────────────────────────────────────
    print(
        f"[{source}] UC1 trigger PLACEHOLDER — new BQ data ready in "
        f"{BQ_DATASET}.{SOURCE_CONFIG[source]['bq_table']}. "
        f"Wire UC1 here when pipeline runner is confirmed."
    )


# ── OFF DAG ────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="off_bronze_to_bq",
    default_args=default_args,
    description="OFF: load new GCS bronze partitions → BigQuery (incremental append)",
    schedule="0 5 * * *",        # daily 05:00, 1h after off_incremental_ingest
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "bigquery", "off", "incremental"],
) as off_dag:

    load_off = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_source_to_bq,
        op_kwargs={"source": "off"},
    )

    uc1_placeholder_off = PythonOperator(
        task_id="uc1_trigger_placeholder",
        python_callable=trigger_uc1_pipeline,
        op_kwargs={"source": "off"},
    )

    load_off >> uc1_placeholder_off


# ── USDA DAG ───────────────────────────────────────────────────────────────────

with DAG(
    dag_id="usda_bronze_to_bq",
    default_args=default_args,
    description="USDA: load new GCS bronze partitions → BigQuery (incremental append)",
    schedule="0 3 1 * *",        # monthly 1st 03:00, 1h after usda_incremental_ingest
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "bigquery", "usda", "incremental"],
) as usda_dag:

    load_usda = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_source_to_bq,
        op_kwargs={"source": "usda"},
    )

    uc1_placeholder_usda = PythonOperator(
        task_id="uc1_trigger_placeholder",
        python_callable=trigger_uc1_pipeline,
        op_kwargs={"source": "usda"},
    )

    load_usda >> uc1_placeholder_usda


# ── openFDA DAG ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="openfda_bronze_to_bq",
    default_args=default_args,
    description="openFDA: load new GCS bronze partitions → BigQuery (incremental append)",
    schedule="0 6 * * *",        # daily 06:00, 1h after openfda_incremental_ingest
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "bigquery", "openfda", "incremental"],
) as openfda_dag:

    load_openfda = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_source_to_bq,
        op_kwargs={"source": "openfda"},
    )

    uc1_placeholder_openfda = PythonOperator(
        task_id="uc1_trigger_placeholder",
        python_callable=trigger_uc1_pipeline,
        op_kwargs={"source": "openfda"},
    )

    load_openfda >> uc1_placeholder_openfda
