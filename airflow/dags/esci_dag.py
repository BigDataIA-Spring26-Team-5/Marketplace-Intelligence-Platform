"""
Amazon ESCI ingestion DAG.
One-time load from HuggingFace tasksource/esci → gs://mip-bronze-2024/esci/
Streams without full local download. Flushes every 10k records.
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

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
FLUSH_EVERY    = 10_000

default_args = {
    "owner": "mip",
    "retries": 2,
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
    key = f"esci/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=BytesIO(body),
        ContentType="application/x-ndjson",
    )
    print(f"Flushed {len(records):>6} records → gs://{BRONZE_BUCKET}/{key}")


def fetch_and_upload(execution_date=None, **kwargs):
    from datasets import load_dataset

    ds_str = execution_date.strftime("%Y/%m/%d") if execution_date else datetime.utcnow().strftime("%Y/%m/%d")
    gcs = _gcs_client()

    # Stream all splits: train, test, validation
    dataset = load_dataset(
        "tasksource/esci",
        split="train",
        streaming=True,
    )

    buffer    = []
    chunk_idx = 0
    total     = 0

    for record in dataset:
        # Keep only fields relevant to the unified schema
        row = {
            "product_id":          record.get("product_id", ""),
            "product_title":       record.get("product_title", ""),
            "product_description": (record.get("product_description") or "")[:2048],
            "product_bullet_point":(record.get("product_bullet_point") or "")[:1024],
            "product_brand":       record.get("product_brand", ""),
            "product_color":       record.get("product_color", ""),
            "product_locale":      record.get("product_locale", ""),
            "esci_label":          record.get("esci_label", ""),
        }
        buffer.append(row)
        total += 1

        if len(buffer) >= FLUSH_EVERY:
            _flush(gcs, buffer, ds_str, chunk_idx)
            chunk_idx += 1
            buffer = []

    if buffer:
        _flush(gcs, buffer, ds_str, chunk_idx)

    print(f"ESCI ingestion complete. Total records: {total}")
    return total


with DAG(
    dag_id="esci_bronze_ingest",
    default_args=default_args,
    description="One-time ESCI HuggingFace → GCS bronze",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "esci"],
) as dag:

    ingest = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
    )
