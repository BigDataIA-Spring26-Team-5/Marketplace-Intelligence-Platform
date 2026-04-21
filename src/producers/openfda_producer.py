"""
openFDA food enforcement (recall) producer.
Polls https://api.fda.gov/food/enforcement.json and writes
all records directly to GCS bronze (gs://mip-bronze-2024/openfda/).
Also produces to Kafka topic source.openfda.recalls for pipeline events.
Run: python -m src.producers.openfda_producer
"""

import json
import os
import time
from datetime import datetime
from io import BytesIO

import boto3
import requests
from botocore.config import Config

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")

FDA_URL    = "https://api.fda.gov/food/enforcement.json"
PAGE_LIMIT = 100
RETRY_WAIT = 5


def gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def fetch_page(skip: int) -> list:
    for attempt in range(3):
        try:
            resp = requests.get(
                FDA_URL,
                params={"limit": PAGE_LIMIT, "skip": skip},
                timeout=30,
            )
            if resp.status_code in (404, 400):
                return []   # 400 = past API's max skip limit (25k)
            if resp.status_code in (429, 503):
                time.sleep(RETRY_WAIT * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json().get("results", [])
        except requests.RequestException as e:
            print(f"  Page skip={skip} attempt {attempt+1} failed: {e}")
            time.sleep(RETRY_WAIT)
    return []


FLUSH_EVERY = 5_000


def flush(client, buffer, ds, chunk_idx):
    key = f"openfda/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Uploaded {len(buffer)} records → gs://{BRONZE_BUCKET}/{key}", flush=True)


def main():
    gcs       = gcs_client()
    ds        = datetime.utcnow().strftime("%Y/%m/%d")
    skip      = 0
    total     = 0
    chunk_idx = 0
    buffer    = []

    print("Fetching openFDA food enforcement records...", flush=True)

    while True:
        records = fetch_page(skip)
        if not records:
            break
        buffer.extend(records)
        total += len(records)
        print(f"  skip={skip:>6}  fetched={len(records):>3}  total={total:>6}", flush=True)

        if len(buffer) >= FLUSH_EVERY:
            flush(gcs, buffer, ds, chunk_idx)
            chunk_idx += 1
            buffer = []

        if len(records) < PAGE_LIMIT:
            break
        skip += PAGE_LIMIT

    if buffer:
        flush(gcs, buffer, ds, chunk_idx)

    print(f"Done. Total openFDA records: {total}", flush=True)


if __name__ == "__main__":
    main()
