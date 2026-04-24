"""
ESCI Bronze → BigQuery Gold (direct hack, skip silver/enrichment).

Reads ESCI JSONL from gs://mip-bronze-2024/esci/{date}/,
renames columns to retail schema, deduplicates on product_id,
writes to BigQuery mip_gold.products.

Usage:
  python scripts/esci_bronze_to_gold.py
  python scripts/esci_bronze_to_gold.py --date 2026/04/21
  python scripts/esci_bronze_to_gold.py --date 2026/04/21 --limit 500000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BRONZE_BUCKET  = os.environ.get("BRONZE_BUCKET",  "mip-bronze-2024")
GCS_ENDPOINT   = os.environ.get("GCS_ENDPOINT",   "https://storage.googleapis.com")
GCS_ACCESS_KEY = os.environ.get("GCS_ACCESS_KEY", "")
GCS_SECRET_KEY = os.environ.get("GCS_SECRET_KEY", "")
BQ_PROJECT     = os.environ.get("GCP_PROJECT",    "mip-platform-2024")
BQ_DATASET     = os.environ.get("BQ_GOLD_DATASET","mip_gold")
BQ_TABLE       = os.environ.get("BQ_GOLD_TABLE",  "products")

RENAME = {
    "product_title":        "product_name",
    "product_brand":        "brand_name",
    "product_bullet_point": "product_features",
    "product_locale":       "locale",
    "esci_label":           "relevance_label",
    "product_color":        "color",
}


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _list_keys(client, date: str) -> list[str]:
    prefix = f"esci/{date}/"
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                keys.append(obj["Key"])
    if not keys:
        raise FileNotFoundError(f"No JSONL files at gs://{BRONZE_BUCKET}/{prefix}")
    return sorted(keys)


def _read_jsonl_key(client, key: str) -> list[dict]:
    resp = client.get_object(Bucket=BRONZE_BUCKET, Key=key)
    body = resp["Body"].read().decode("utf-8")
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _load_bronze(date: str) -> pd.DataFrame:
    client = _s3()
    keys = _list_keys(client, date)
    logger.info("Found %d JSONL files for date=%s", len(keys), date)

    frames = []
    for key in keys:
        rows = _read_jsonl_key(client, key)
        frames.append(pd.DataFrame(rows))
        logger.info("  %s → %d rows", key, len(rows))

    df = pd.concat(frames, ignore_index=True)
    logger.info("Total bronze rows: %d", len(df))
    return df


def _transform(df: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    df = df.rename(columns=RENAME)

    # Dedup on product_id — same ASIN repeats across queries
    before = len(df)
    df = df.drop_duplicates(subset=["product_id"], keep="first").reset_index(drop=True)
    logger.info("Dedup on product_id: %d → %d rows", before, len(df))

    if limit and len(df) > limit:
        df = df.iloc[:limit].copy()
        logger.info("--limit %d applied: %d rows", limit, len(df))

    df["data_source"] = "esci"
    df["source_name"] = "esci"

    # DQ score — retail weights (no ingredients, so richness=0)
    from src.blocks.dq_score import compute_dq_score
    retail_weights = {"completeness": 0.5, "freshness": 0.2, "ingredient_richness": 0.3}
    score = compute_dq_score(df, weights=retail_weights)
    df["dq_score_pre"]  = score
    df["dq_score_post"] = score  # no enrichment step, pre==post
    df["dq_delta"]      = 0.0

    # Ensure all retail schema string cols exist
    for col in ("product_name", "brand_name", "product_description",
                "product_features", "color", "locale", "relevance_label"):
        if col not in df.columns:
            df[col] = None

    # Clean empty strings → None
    str_cols = ["product_name", "brand_name", "product_description",
                "product_features", "color", "locale", "relevance_label",
                "product_id", "data_source", "source_name"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].replace("", None)

    return df


def _write_bq(df: pd.DataFrame) -> int:
    from google.cloud import bigquery

    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    client = bigquery.Client(project=BQ_PROJECT)

    # Fetch live table schema — only write columns that exist in the table
    table = client.get_table(table_ref)
    bq_cols = {f.name for f in table.schema}

    # Drop any df columns not in BQ schema to avoid field mismatch errors
    write_cols = [c for c in df.columns if c in bq_cols]
    df = df[write_cols].copy()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=[f for f in table.schema if f.name in write_cols],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    rows_written = len(df)
    logger.info("BQ write complete: %d rows → %s", rows_written, table_ref)
    return rows_written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  default="2026/04/21", help="Bronze date partition (YYYY/MM/DD)")
    parser.add_argument("--limit", type=int, default=None, help="Max unique products to write")
    args = parser.parse_args()

    df = _load_bronze(args.date)
    rows_in = len(df)
    df = _transform(df, limit=args.limit)
    logger.info("Final shape before BQ write: %d rows, %d cols", *df.shape)

    run_id = f"esci_gold_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = time.monotonic()
    status = "success"
    error = None
    rows = 0
    try:
        rows = _write_bq(df)
    except Exception as exc:
        status = "failed"
        error = str(exc)
        logger.error("BQ write failed: %s", exc)

    duration = round(time.monotonic() - start_time, 3)

    try:
        from src.pipeline.gold_pipeline import (
            _build_gold_run_log,
            _save_gold_run_log,
            _push_gold_metrics,
            _push_gold_audit,
        )
        run_log = _build_gold_run_log(
            run_id=run_id,
            source_name="esci",
            domain="retail",
            rows_in=rows_in,
            result_df=df,
            audit_log=[],
            duration_seconds=duration,
            status=status,
            error=error,
        )
        _save_gold_run_log(run_log)
        _push_gold_metrics(run_log)
        _push_gold_audit(run_log)
    except Exception as exc:
        logger.warning("Observability push failed (non-fatal): %s", exc)

    if error:
        raise RuntimeError(f"BQ write failed: {error}")

    logger.info("Done. %d rows written to BigQuery.", rows)


if __name__ == "__main__":
    main()
