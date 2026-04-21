"""
Open Food Facts — incremental daily DAG.

Strategy: OFF publishes daily delta JSONL.GZ files at:
  https://static.openfoodfacts.org/data/delta/{YYYY-MM-DD}.jsonl.gz

Watermark stored in GCS: mip-bronze-2024/_watermarks/off_watermark.json
  {"last_date": "2026-04-20"}

Each run:
  1. Read watermark → find all delta dates not yet ingested
  2. Download each delta file, decompress, write JSONL to GCS bronze
  3. Update watermark to today
"""
from __future__ import annotations

import gzip
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
WATERMARK_KEY  = "_watermarks/off_watermark.json"
OFF_DELTA_URL  = "https://static.openfoodfacts.org/data/delta/{date}.jsonl.gz"
FLUSH_EVERY    = 10_000

KEEP_FIELDS = [
    "code", "product_name", "brands", "ingredients_text",
    "categories", "pnns_groups_1", "pnns_groups_2",
    "allergens", "traces", "labels", "countries",
    "serving_size", "energy_100g", "fat_100g",
    "carbohydrates_100g", "proteins_100g", "salt_100g",
    "nova_group", "nutriscore_grade", "data_quality_tags",
    "last_modified_t",
]

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
    """Return last ingested date string YYYY-MM-DD, or 7 days ago as default."""
    try:
        obj = client.get_object(Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY)
        data = json.loads(obj["Body"].read())
        return data["last_date"]
    except client.exceptions.NoSuchKey:
        return (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")


def _write_watermark(client, date_str: str) -> None:
    body = json.dumps({"last_date": date_str, "updated_at": datetime.utcnow().isoformat()})
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY,
        Body=body.encode(), ContentType="application/json",
    )


def _flush(client, buffer: list, date_str: str, chunk_idx: int) -> None:
    key = f"off/{date_str.replace('-', '/')}/delta/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(buffer):>6} → gs://{BRONZE_BUCKET}/{key}")


def ingest_incremental(execution_date=None, **kwargs):
    client = _gcs()
    last_date = _read_watermark(client)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Build list of dates to backfill since watermark
    start = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    end   = datetime.utcnow()
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    if not dates:
        print("OFF: already up to date, nothing to ingest.")
        return 0

    grand_total = 0
    last_successful = last_date

    for date_str in dates:
        url = OFF_DELTA_URL.format(date=date_str)
        print(f"Fetching OFF delta: {url}")
        try:
            resp = requests.get(url, timeout=120, stream=True)
            if resp.status_code == 404:
                print(f"  No delta file for {date_str} (404) — skipping.")
                last_successful = date_str
                continue
            resp.raise_for_status()

            raw = gzip.decompress(resp.content)
            lines = raw.decode("utf-8").splitlines()

            buffer    = []
            chunk_idx = 0
            total     = 0

            for line in lines:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not record.get("product_name"):
                    continue
                row = {k: record.get(k) for k in KEEP_FIELDS}
                buffer.append(row)
                total += 1

                if len(buffer) >= FLUSH_EVERY:
                    _flush(client, buffer, date_str, chunk_idx)
                    chunk_idx += 1
                    buffer = []

            if buffer:
                _flush(client, buffer, date_str, chunk_idx)

            print(f"  {date_str}: {total} records ingested.")
            grand_total += total
            last_successful = date_str

        except Exception as exc:
            print(f"  ERROR on {date_str}: {exc} — stopping here, watermark preserved at {last_successful}.")
            break

    _write_watermark(client, last_successful)
    print(f"OFF incremental done. Total: {grand_total}. Watermark → {last_successful}")
    return grand_total


with DAG(
    dag_id="off_incremental_ingest",
    default_args=default_args,
    description="Daily OFF delta ingestion → GCS bronze (incremental)",
    schedule="0 4 * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "off", "incremental"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_incremental",
        python_callable=ingest_incremental,
    )
