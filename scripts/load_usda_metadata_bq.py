"""
Load USDA bronze metadata + sample records into BigQuery.
Creates two tables:
  mip_platform_2024.bronze_metadata.usda_files   — one row per file (size, record count, path)
  mip_platform_2024.bronze_metadata.usda_records — actual food records (flattened)
"""

import json
import os
import boto3
from botocore.config import Config
from google.cloud import bigquery
from datetime import datetime

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "REMOVED_GCS_ACCESS_KEY ")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "REMOVED_GCS_SECRET_KEY")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET  = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
GCP_PROJECT    = os.getenv("GCP_PROJECT", "mip-platform-2024")
DATASET        = "bronze_metadata"
PREFIX         = "usda/2026/04/20/"


def gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def main():
    gcs = gcs_client()
    bq  = bigquery.Client(project=GCP_PROJECT)

    bq.create_dataset(f"{GCP_PROJECT}.{DATASET}", exists_ok=True)

    # ── Table 1: file-level metadata ─────────────────────────────────────────
    files_schema = [
        bigquery.SchemaField("source",        "STRING"),
        bigquery.SchemaField("gcs_path",      "STRING"),
        bigquery.SchemaField("query",         "STRING"),
        bigquery.SchemaField("partition_date","DATE"),
        bigquery.SchemaField("file_size_bytes","INTEGER"),
        bigquery.SchemaField("record_count",  "INTEGER"),
        bigquery.SchemaField("loaded_at",     "TIMESTAMP"),
    ]
    files_table_id = f"{GCP_PROJECT}.{DATASET}.usda_files"
    bq.delete_table(files_table_id, not_found_ok=True)
    bq.create_table(bigquery.Table(files_table_id, schema=files_schema))

    # ── Table 2: flattened food records ──────────────────────────────────────
    records_schema = [
        bigquery.SchemaField("fdc_id",           "INTEGER"),
        bigquery.SchemaField("description",      "STRING"),
        bigquery.SchemaField("brand_owner",      "STRING"),
        bigquery.SchemaField("brand_name",       "STRING"),
        bigquery.SchemaField("ingredients",      "STRING"),
        bigquery.SchemaField("food_category",    "STRING"),
        bigquery.SchemaField("data_type",        "STRING"),
        bigquery.SchemaField("gtin_upc",         "STRING"),
        bigquery.SchemaField("published_date",   "STRING"),
        bigquery.SchemaField("serving_size",     "FLOAT"),
        bigquery.SchemaField("serving_size_unit","STRING"),
        bigquery.SchemaField("query_category",   "STRING"),
        bigquery.SchemaField("gcs_source_file",  "STRING"),
        bigquery.SchemaField("loaded_at",        "TIMESTAMP"),
    ]
    records_table_id = f"{GCP_PROJECT}.{DATASET}.usda_records"
    bq.delete_table(records_table_id, not_found_ok=True)
    bq.create_table(bigquery.Table(records_table_id, schema=records_schema))

    # ── Process each file ────────────────────────────────────────────────────
    resp = gcs.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=PREFIX)
    files_rows   = []
    records_rows = []
    loaded_at    = datetime.utcnow().isoformat()

    for obj in resp.get("Contents", []):
        key   = obj["Key"]
        query = key.split("/")[-1].replace(".jsonl", "")
        body  = gcs.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read().decode()
        lines = [l for l in body.strip().splitlines() if l]

        files_rows.append({
            "source":         "usda",
            "gcs_path":       f"gs://{BRONZE_BUCKET}/{key}",
            "query":          query,
            "partition_date": "2026-04-20",
            "file_size_bytes": obj["Size"],
            "record_count":   len(lines),
            "loaded_at":      loaded_at,
        })

        for line in lines:
            r = json.loads(line)
            records_rows.append({
                "fdc_id":            r.get("fdcId"),
                "description":       r.get("description", "")[:1024],
                "brand_owner":       r.get("brandOwner", ""),
                "brand_name":        r.get("brandName", ""),
                "ingredients":       (r.get("ingredients") or "")[:2048],
                "food_category":     r.get("foodCategory", ""),
                "data_type":         r.get("dataType", ""),
                "gtin_upc":          r.get("gtinUpc", ""),
                "published_date":    r.get("publishedDate", ""),
                "serving_size":      r.get("servingSize"),
                "serving_size_unit": r.get("servingSizeUnit", ""),
                "query_category":    query,
                "gcs_source_file":   f"gs://{BRONZE_BUCKET}/{key}",
                "loaded_at":         loaded_at,
            })

        print(f"  {query:<12} {len(lines):>6} records  {obj['Size']:>12,} bytes")

    # ── Insert into BigQuery ──────────────────────────────────────────────────
    errors = bq.insert_rows_json(files_table_id, files_rows)
    if errors:
        print(f"Files table errors: {errors}")
    else:
        print(f"\nLoaded {len(files_rows)} rows → {files_table_id}")

    # Insert records in batches of 10k (BQ streaming limit)
    batch_size = 10_000
    total_inserted = 0
    for i in range(0, len(records_rows), batch_size):
        batch  = records_rows[i:i+batch_size]
        errors = bq.insert_rows_json(records_table_id, batch)
        if errors:
            print(f"Records batch {i//batch_size} errors: {errors[:2]}")
        else:
            total_inserted += len(batch)

    print(f"Loaded {total_inserted} rows → {records_table_id}")
    print("\nRun in BigQuery:")
    print(f"  SELECT * FROM `{records_table_id}` LIMIT 100;")
    print(f"  SELECT * FROM `{files_table_id}`;")


if __name__ == "__main__":
    main()
