"""
Bronze → Silver incremental DAG.

Pipeline position:
  GCS bronze (JSONL) → [UC1 ETL pipeline, silver mode] → GCS silver (Parquet)

Flow per source (all 3 sources run in parallel):
  1. Read Silver watermark from GCS (_watermarks/{source}_silver_watermark.json).
  2. List Bronze partitions newer than watermark.
  3. For each new partition: run ETL pipeline in silver mode → Parquet to Silver GCS.
  4. Update Silver watermark.

Schedule: daily 07:00 — runs after all source ingest DAGs + BQ load DAGs complete.
  - OFF:     ingest 04:00, BQ load 05:00 → Silver at 07:00
  - USDA:    ingest monthly 02:00, BQ load 03:00 → Silver at 07:00
  - openFDA: ingest 05:00, BQ load 06:00 → Silver at 07:00

Silver watermark keys (stored in Bronze bucket):
  gs://mip-bronze-2024/_watermarks/off_silver_watermark.json
  gs://mip-bronze-2024/_watermarks/usda_silver_watermark.json
  gs://mip-bronze-2024/_watermarks/openfda_silver_watermark.json
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

logger = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT",   "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET",  "mip-bronze-2024")

SOURCE_CONFIG: dict[str, dict[str, Any]] = {
    "off": {
        "gcs_prefix":      "off/",
        "domain":          "nutrition",
        "watermark_key":   "_watermarks/off_silver_watermark.json",
        "partition_depth": 3,
    },
    "usda": {
        "gcs_prefix":      "usda/",
        "domain":          "nutrition",
        "watermark_key":   "_watermarks/usda_silver_watermark.json",
        "partition_depth": 3,
    },
    "openfda": {
        "gcs_prefix":      "openfda/",
        "domain":          "safety",
        "watermark_key":   "_watermarks/openfda_silver_watermark.json",
        "partition_depth": 3,
    },
}

default_args = {
    "owner": "mip",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _gcs():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _read_silver_watermark(client, watermark_key: str) -> str | None:
    try:
        obj = client.get_object(Bucket=BRONZE_BUCKET, Key=watermark_key)
        return json.loads(obj["Body"].read()).get("last_partition")
    except client.exceptions.NoSuchKey:
        return None


def _list_bronze_partitions(client, prefix: str, depth: int) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    partitions = set()
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            rest = obj["Key"][len(prefix):]
            parts = rest.split("/")
            if len(parts) >= depth:
                partitions.add("/".join(parts[:depth]))
    return sorted(partitions)


# ── per-source silver load ────────────────────────────────────────────────────

def load_source_to_silver(source: str, **kwargs) -> int:
    """
    Find Bronze partitions newer than the Silver watermark and run the ETL pipeline
    in silver mode for each new partition. Returns total rows written to Silver.
    """
    from dotenv import load_dotenv
    load_dotenv()

    from src.pipeline.cli import run_pipeline

    cfg    = SOURCE_CONFIG[source]
    client = _gcs()

    last_partition = _read_silver_watermark(client, cfg["watermark_key"])
    print(f"[{source}] Silver watermark: {last_partition or 'none (first run)'}")

    all_partitions = _list_bronze_partitions(client, cfg["gcs_prefix"], cfg["partition_depth"])
    if not all_partitions:
        print(f"[{source}] No Bronze partitions found. Nothing to process.")
        return 0

    new_partitions = [p for p in all_partitions if last_partition is None or p > last_partition]
    if not new_partitions:
        print(f"[{source}] Already up to date (watermark={last_partition}).")
        return 0

    print(f"[{source}] {len(new_partitions)} new partition(s): {new_partitions}")

    total_rows = 0
    for partition in new_partitions:
        gcs_uri = f"gs://{BRONZE_BUCKET}/{cfg['gcs_prefix']}{partition}/*.jsonl"
        print(f"[{source}] Processing partition {partition} → Silver (mode=silver)")
        try:
            result = run_pipeline(
                source_path=gcs_uri,
                domain=cfg["domain"],
                pipeline_mode="silver",
            )
            rows = len(result.get("working_df", []))
            total_rows += rows
            print(f"[{source}] Partition {partition}: {rows} rows written to Silver.")
        except Exception as exc:
            print(f"[{source}] ERROR on partition {partition}: {exc}")
            raise

    print(f"[{source}] Done. {total_rows} total rows written to Silver.")
    return total_rows


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    description="Bronze GCS JSONL → Silver GCS Parquet (schema transform, all sources parallel)",
    schedule="0 7 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "etl", "incremental"],
) as dag:

    # All tasks have no inter-dependencies → Airflow runs them in parallel
    for _source_name in SOURCE_CONFIG:
        PythonOperator(
            task_id=f"silver_{_source_name}",
            python_callable=load_source_to_silver,
            op_kwargs={"source": _source_name},
        )
