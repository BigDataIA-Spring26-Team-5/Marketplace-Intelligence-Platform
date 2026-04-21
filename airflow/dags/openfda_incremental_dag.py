"""
openFDA Food Enforcement — incremental daily DAG.

Strategy: openFDA API supports date-range search on receivedate:
  search=receivedate:[{last_date}+TO+{today}]

This is the most natural incremental source — new recalls come in daily.
Watermark: mip-bronze-2024/_watermarks/openfda_watermark.json
  {"last_date": "2026-04-20"}
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

FDA_BASE_URL   = "https://api.fda.gov/food/enforcement.json"
LIMIT          = 100
GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
WATERMARK_KEY  = "_watermarks/openfda_watermark.json"
FLUSH_EVERY    = 1_000

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
        return (datetime.utcnow() - timedelta(days=7)).strftime("%Y%m%d")


def _write_watermark(client, date_str: str) -> None:
    body = json.dumps({"last_date": date_str, "updated_at": datetime.utcnow().isoformat()})
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY,
        Body=body.encode(), ContentType="application/json",
    )


def _flush(client, records: list, ds: str, chunk_idx: int) -> None:
    key = f"openfda/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(records):>6} → gs://{BRONZE_BUCKET}/{key}")


def ingest_incremental(execution_date=None, **kwargs):
    client     = _gcs()
    last_date  = _read_watermark(client)
    today      = datetime.utcnow().strftime("%Y%m%d")
    ds         = datetime.utcnow().strftime("%Y/%m/%d")

    if last_date >= today:
        print(f"openFDA: already up to date ({last_date}). Skipping.")
        return 0

    # openFDA date filter format: YYYYMMDD
    date_filter = f"receivedate:[{last_date}+TO+{today}]"
    print(f"openFDA incremental: {date_filter}")

    buffer    = []
    chunk_idx = 0
    skip      = 0
    total     = 0

    while True:
        params = {
            "search": date_filter,
            "limit":  LIMIT,
            "skip":   skip,
        }
        try:
            resp = requests.get(FDA_BASE_URL, params=params, timeout=30)
            if resp.status_code == 404:
                # No results for this date range
                break
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            print(f"  openFDA request error at skip={skip}: {exc}")
            break

        results = data.get("results", [])
        if not results:
            break

        buffer.extend(results)
        total += len(results)
        skip  += len(results)

        if len(buffer) >= FLUSH_EVERY:
            _flush(client, buffer, ds, chunk_idx)
            chunk_idx += 1
            buffer = []

        meta      = data.get("meta", {})
        total_api = meta.get("results", {}).get("total", 0)
        if skip >= total_api or len(results) < LIMIT:
            break

    if buffer:
        _flush(client, buffer, ds, chunk_idx)

    _write_watermark(client, today)
    print(f"openFDA incremental done. {total} new records. Watermark → {today}")
    return total


with DAG(
    dag_id="openfda_incremental_ingest",
    default_args=default_args,
    description="Daily openFDA enforcement incremental → GCS bronze",
    schedule="0 5 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "openfda", "incremental"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_incremental",
        python_callable=ingest_incremental,
    )
