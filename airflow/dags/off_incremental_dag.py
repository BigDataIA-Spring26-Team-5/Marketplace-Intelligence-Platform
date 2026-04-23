"""
Open Food Facts — incremental daily DAG.

Strategy: OFF publishes delta files indexed at:
  https://static.openfoodfacts.org/data/delta/index.txt

Each line in index.txt is a filename like:
  openfoodfacts_products_{start_ts}_{end_ts}.json.gz

Watermark stored in GCS: mip-bronze-2024/_watermarks/off_watermark.json
  {"last_ts": 1776000000}  (Unix timestamp of last processed end_ts)

Each run:
  1. Fetch index.txt → parse all available delta files
  2. Download files whose start_ts >= last_ts watermark (i.e. not yet ingested)
  3. Decompress, parse JSON, filter fields, write JSONL to GCS bronze
  4. Update watermark to latest end_ts processed
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

GCS_ACCESS_KEY  = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY  = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT    = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET   = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
WATERMARK_KEY   = "_watermarks/off_watermark.json"
OFF_INDEX_URL   = "https://static.openfoodfacts.org/data/delta/index.txt"
OFF_DELTA_BASE  = "https://static.openfoodfacts.org/data/delta/"
FLUSH_EVERY     = 10_000

KEEP_FIELDS = [
    "code", "product_name", "brands", "ingredients_text",
    "categories", "pnns_groups_1", "pnns_groups_2",
    "allergens", "traces", "labels", "countries",
    "serving_size", "nova_group", "nutriscore_grade",
    "data_quality_tags", "last_modified_t",
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


def _read_watermark(client) -> int:
    """Return last processed end_ts as Unix int. Default: oldest file in index."""
    try:
        obj = client.get_object(Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY)
        data = json.loads(obj["Body"].read())
        # support old date-string watermark — treat as 0 so all available deltas are fetched
        if "last_ts" in data:
            return int(data["last_ts"])
        return 0
    except Exception:
        return 0


def _write_watermark(client, end_ts: int) -> None:
    body = json.dumps({"last_ts": end_ts, "updated_at": datetime.utcnow().isoformat()})
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY,
        Body=body.encode(), ContentType="application/json",
    )


def _parse_index(index_text: str) -> list[tuple[int, int, str]]:
    """Parse index.txt → sorted list of (start_ts, end_ts, filename)."""
    files = []
    for line in index_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # openfoodfacts_products_{start}_{end}.json.gz
        parts = line.replace(".json.gz", "").split("_")
        try:
            end_ts   = int(parts[-1])
            start_ts = int(parts[-2])
            files.append((start_ts, end_ts, line))
        except (ValueError, IndexError):
            continue
    return sorted(files, key=lambda x: x[0])


def _flush(client, buffer: list, date_str: str, chunk_idx: int) -> None:
    key = f"off/{date_str}/delta_part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(buffer):>6} → gs://{BRONZE_BUCKET}/{key}")


def ingest_incremental(execution_date=None, **kwargs):
    client   = _gcs()
    last_ts  = _read_watermark(client)

    headers = {"User-Agent": "MIP-Pipeline/1.0 (mip-data-platform; contact@mip.io)"}

    # Fetch available delta file list
    resp = requests.get(OFF_INDEX_URL, timeout=30, headers=headers)
    resp.raise_for_status()
    all_files = _parse_index(resp.text)

    # Only files not yet ingested (start_ts >= last_ts)
    pending = [(s, e, f) for s, e, f in all_files if s >= last_ts]

    if not pending:
        print(f"OFF: already up to date (last_ts={last_ts}). Nothing to ingest.")
        return 0

    print(f"OFF: {len(pending)} delta file(s) to process since ts={last_ts}")

    grand_total  = 0
    last_end_ts  = last_ts

    for start_ts, end_ts, filename in pending:
        url      = OFF_DELTA_BASE + filename
        date_str = datetime.utcfromtimestamp(start_ts).strftime("%Y/%m/%d")
        print(f"Fetching: {filename}  ({date_str})")

        try:
            r = requests.get(url, timeout=180, stream=True, headers=headers)
            r.raise_for_status()

            raw   = gzip.decompress(r.content)
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
                nutriments = record.get("nutriments") or {}
                row["energy_100g"]        = nutriments.get("energy-kcal_100g") or nutriments.get("energy_100g")
                row["fat_100g"]           = nutriments.get("fat_100g")
                row["carbohydrates_100g"] = nutriments.get("carbohydrates_100g")
                row["proteins_100g"]      = nutriments.get("proteins_100g")
                row["salt_100g"]          = nutriments.get("salt_100g")
                buffer.append(row)
                total += 1

                if len(buffer) >= FLUSH_EVERY:
                    _flush(client, buffer, date_str, chunk_idx)
                    chunk_idx += 1
                    buffer = []

            if buffer:
                _flush(client, buffer, date_str, chunk_idx)

            print(f"  {filename}: {total} records.")
            grand_total += total
            last_end_ts  = end_ts

        except Exception as exc:
            print(f"  ERROR on {filename}: {exc} — stopping, watermark preserved at {last_end_ts}.")
            break

    _write_watermark(client, last_end_ts)
    print(f"OFF incremental done. Total: {grand_total}. Watermark → {last_end_ts}")
    return grand_total


with DAG(
    dag_id="off_incremental_ingest",
    default_args=default_args,
    description="Daily OFF delta ingestion → GCS bronze (incremental, timestamp-based)",
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
