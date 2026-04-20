"""
USDA FoodData Central ingestion DAG.
Uses /foods/list endpoint to pull ALL records (not search-filtered).
Writes JSONL to gs://mip-bronze-2024/usda/ partitioned by date.
Schedule: monthly. Flushes to GCS every FLUSH_EVERY pages to avoid OOM.
"""

import json
import os
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

USDA_API_KEY  = os.getenv("USDA_API_KEY", "gZJUqbshltC7qfQ9lk0meZcMJjazosPLfPVnEbgF")
USDA_LIST_URL = "https://api.nal.usda.gov/fdc/v1/foods/list"
PAGE_SIZE     = 200
FLUSH_EVERY   = 50   # write to GCS every 50 pages (10k records) to avoid OOM

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")

default_args = {
    "owner": "mip",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _flush(client, records, ds, chunk_idx):
    key = f"usda/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=BytesIO(body),
        ContentType="application/x-ndjson",
    )
    print(f"Flushed {len(records):>6} records → gs://{BRONZE_BUCKET}/{key}")


def fetch_and_upload(execution_date=None, **kwargs):
    """Page through /foods/list and write all USDA records to GCS bronze."""
    ds = execution_date.strftime("%Y/%m/%d") if execution_date else datetime.utcnow().strftime("%Y/%m/%d")
    gcs = _gcs_client()

    page        = 1
    chunk_idx   = 0
    total       = 0
    buffer      = []

    while True:
        resp = requests.get(
            USDA_LIST_URL,
            params={
                "api_key":    USDA_API_KEY,
                "pageSize":   PAGE_SIZE,
                "pageNumber": page,
            },
            timeout=30,
        )
        resp.raise_for_status()
        foods = resp.json()

        if not foods:
            break

        buffer.extend(foods)
        total += len(foods)

        if page % FLUSH_EVERY == 0:
            _flush(gcs, buffer, ds, chunk_idx)
            chunk_idx += 1
            buffer = []

        if len(foods) < PAGE_SIZE:
            break

        page += 1

    # flush remainder
    if buffer:
        _flush(gcs, buffer, ds, chunk_idx)

    print(f"USDA ingestion complete. Total records: {total}")
    return total


with DAG(
    dag_id="usda_bronze_ingest",
    default_args=default_args,
    description="Monthly USDA FoodData Central → GCS bronze (full list)",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "usda"],
) as dag:

    ingest = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
    )
