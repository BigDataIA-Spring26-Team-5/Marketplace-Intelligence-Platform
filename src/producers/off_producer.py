"""
Open Food Facts producer.
Streams openfoodfacts/product-database (food split, ~4.48M records)
from HuggingFace and writes directly to GCS bronze.
Run: python -m src.producers.off_producer
"""

import json
import os
from datetime import datetime
from io import BytesIO

import boto3
from botocore.config import Config
from datasets import load_dataset

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
FLUSH_EVERY    = 10_000
MAX_RECORDS    = 1_000_000

KEEP_FIELDS = [
    "code", "product_name", "brands", "ingredients_text",
    "categories", "pnns_groups_1", "pnns_groups_2",
    "allergens", "traces", "labels", "countries",
    "serving_size", "energy_100g", "fat_100g",
    "carbohydrates_100g", "proteins_100g", "salt_100g",
    "nova_group", "nutriscore_grade", "data_quality_tags",
    "last_modified_t",
]


def gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def flush(client, buffer, ds, chunk_idx):
    key = f"off/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET, Key=key,
        Body=BytesIO(body), ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(buffer):>6} records → gs://{BRONZE_BUCKET}/{key}")


def main():
    gcs = gcs_client()
    ds  = datetime.utcnow().strftime("%Y/%m/%d")

    print("Streaming openfoodfacts/product-database (food split)...")
    dataset = load_dataset(
        "openfoodfacts/product-database",
        split="food",
        streaming=True,
    )

    buffer    = []
    chunk_idx = 0
    total     = 0
    skipped   = 0

    for record in dataset:
        if total >= MAX_RECORDS:
            break
        if not record.get("product_name"):
            skipped += 1
            continue
        row = {k: record.get(k) for k in KEEP_FIELDS}
        buffer.append(row)
        total += 1

        if len(buffer) >= FLUSH_EVERY:
            flush(gcs, buffer, ds, chunk_idx)
            chunk_idx += 1
            buffer = []

        if total % 100_000 == 0:
            print(f"  {total:>7} records streamed...")

    if buffer:
        flush(gcs, buffer, ds, chunk_idx)

    print(f"Done. Total: {total}, skipped (no name): {skipped}")


if __name__ == "__main__":
    main()
