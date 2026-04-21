"""
USDA FoodData Central — incremental monthly DAG.

Strategy: USDA API supports filtering by modifiedDate.
  GET /foods/list?modifiedDate={YYYY-MM-DD}&...

Watermark stored in GCS: mip-bronze-2024/_watermarks/usda_watermark.json
  {"last_date": "2026-03-01"}

Each run pulls only records updated since the watermark date, appending to
a new GCS partition (so history is preserved and reruns are safe).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

USDA_API_KEY   = os.getenv("USDA_API_KEY", "gZJUqbshltC7qfQ9lk0meZcMJjazosPLfPVnEbgF")
USDA_LIST_URL  = "https://api.nal.usda.gov/fdc/v1/foods/list"
PAGE_SIZE      = 200
MAX_PAGES      = 500
DATA_TYPES     = ["Branded", "Foundation"]

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
WATERMARK_KEY  = "_watermarks/usda_watermark.json"
FLUSH_EVERY    = 10_000

default_args = {
    "owner": "mip",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _gcs():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _read_watermark(client) -> str:
    try:
        obj = client.get_object(Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY)
        return json.loads(obj["Body"].read())["last_date"]
    except client.exceptions.NoSuchKey:
        return (datetime.utcnow() - timedelta(days=31)).strftime("%Y-%m-%d")


def _write_watermark(client, date_str: str) -> None:
    body = json.dumps({"last_date": date_str, "updated_at": datetime.utcnow().isoformat()})
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY,
        Body=body.encode(), ContentType="application/json",
    )


def _flush(client, records: list, ds: str, chunk_idx: int) -> None:
    key = f"usda/{ds}/incremental/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(records):>6} → gs://{BRONZE_BUCKET}/{key}")


def ingest_incremental(execution_date=None, **kwargs):
    client     = _gcs()
    since_date = _read_watermark(client)
    today      = datetime.utcnow().strftime("%Y-%m-%d")
    ds         = datetime.utcnow().strftime("%Y/%m/%d")

    print(f"USDA incremental: pulling records modified since {since_date}")

    chunk_idx   = 0
    grand_total = 0

    for data_type in DATA_TYPES:
        page   = 1
        buffer = []
        dt_total = 0
        print(f"--- dataType={data_type} since {since_date} ---")

        while page <= MAX_PAGES:
            params = {
                "api_key":      USDA_API_KEY,
                "pageSize":     PAGE_SIZE,
                "pageNumber":   page,
                "dataType":     data_type,
                "modifiedDate": since_date,
            }
            resp = requests.get(USDA_LIST_URL, params=params, timeout=30)
            resp.raise_for_status()
            foods = resp.json()

            if not foods:
                break

            for f in foods:
                f["_dataType"] = data_type
            buffer.extend(foods)
            dt_total += len(foods)

            if len(buffer) >= FLUSH_EVERY:
                _flush(client, buffer, ds, chunk_idx)
                chunk_idx += 1
                buffer = []

            if len(foods) < PAGE_SIZE:
                break
            page += 1

        if buffer:
            _flush(client, buffer, ds, chunk_idx)
            chunk_idx += 1

        print(f"  {data_type}: {dt_total} new/updated records")
        grand_total += dt_total

    _write_watermark(client, today)
    print(f"USDA incremental done. Total: {grand_total}. Watermark → {today}")
    return grand_total


with DAG(
    dag_id="usda_incremental_ingest",
    default_args=default_args,
    description="Monthly USDA incremental → GCS bronze (modifiedDate filter)",
    schedule="0 2 1 * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "usda", "incremental"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_incremental",
        python_callable=ingest_incremental,
    )
