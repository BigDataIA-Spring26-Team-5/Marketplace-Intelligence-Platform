"""
Backfill UC2 metrics for existing silver layer runs.

Aqeel ran the UC1 pipeline and data landed in GCS silver layer, but the
UC2 hooks didn't fire (wrong localhost). This script reads the silver layer
Parquet files from GCS and pushes metrics retroactively to Prometheus and Postgres.

Usage (run on the VM):
    python3 scripts/backfill_uc2_metrics.py --source off --run_id OFF_run_20260421
    python3 scripts/backfill_uc2_metrics.py --source usda --run_id USDA_run_20260421
    python3 scripts/backfill_uc2_metrics.py --source openfda --run_id FDA_run_20260421
    python3 scripts/backfill_uc2_metrics.py --all   # auto-discover all silver partitions
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

import boto3
import pandas as pd
import psycopg2
from botocore.config import Config

from src.uc2_observability.metrics_collector import MetricsCollector
from src.uc2_observability.kafka_to_pg import PG_DSN

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

GCS_ACCESS_KEY = os.getenv("GCS_ACCESS_KEY", "")
GCS_SECRET_KEY = os.getenv("GCS_SECRET_KEY", "")
GCS_ENDPOINT   = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
SILVER_BUCKET  = os.getenv("SILVER_BUCKET", "mip-silver-2024")

NULL_RATE_COLS = ["product_name", "brand_name", "primary_category",
                  "allergens", "dietary_tags", "is_organic"]


def _gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def _list_silver_partitions(source: str) -> list[str]:
    s3 = _gcs_client()
    prefix = f"{source.lower()}/"
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix, Delimiter="/")
    return [p["Prefix"] for p in resp.get("CommonPrefixes", [])]


def _read_silver_parquet(s3_prefix: str) -> pd.DataFrame | None:
    s3 = _gcs_client()
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=s3_prefix)
    parquet_keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not parquet_keys:
        return None

    dfs = []
    for key in parquet_keys[:5]:  # sample first 5 files for metrics
        obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        import io
        dfs.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(dfs, ignore_index=True) if dfs else None


def _compute_metrics(df: pd.DataFrame, source: str) -> dict:
    null_cols = [c for c in NULL_RATE_COLS if c in df.columns]
    null_rate = float(df[null_cols].isna().mean().mean()) if null_cols else 0.0

    dq_pre  = float(df["dq_score_pre"].mean())  if "dq_score_pre"  in df.columns else 0.0
    dq_post = float(df["dq_score_post"].mean()) if "dq_score_post" in df.columns else 0.0

    return {
        "rows_in":               len(df),
        "rows_out":              len(df),
        "null_rate":             round(null_rate, 4),
        "dq_score_pre":          round(dq_pre, 4),
        "dq_score_post":         round(dq_post, 4),
        "dq_delta":              round(dq_post - dq_pre, 4),
        "dedup_rate":            0.0,
        "s1_count":              int(df["_s1_resolved"].sum()) if "_s1_resolved" in df.columns else 0,
        "s2_count":              int(df["_s2_resolved"].sum()) if "_s2_resolved" in df.columns else 0,
        "s3_count":              0,
        "s4_count":              0,
        "quarantine_rows":       0,
        "llm_calls":             0,
        "cost_usd":              0.0,
        "block_duration_seconds": 0.0,
        "status":                "success",
    }


def _write_audit_event(run_id: str, source: str, event_type: str, status: str = "success"):
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO audit_events (run_id, source, event_type, status, ts, payload)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                (run_id, source, event_type, status,
                 datetime.now(timezone.utc), json.dumps({"backfilled": True})),
            )
        conn.commit()
        conn.close()
        logger.info("Wrote audit_event %s for %s/%s", event_type, source, run_id)
    except Exception as e:
        logger.warning("Failed to write audit_event: %s", e)


def backfill_run(source: str, run_id: str, df: pd.DataFrame):
    metrics = _compute_metrics(df, source)
    logger.info("Pushing metrics for %s / %s: rows=%d dq_post=%.3f",
                source, run_id, metrics["rows_in"], metrics["dq_score_post"])

    MetricsCollector().push(run_id=run_id, source=source, metrics_dict=metrics)
    _write_audit_event(run_id, source, "run_started")
    _write_audit_event(run_id, source, "run_completed", status="success")
    logger.info("Done: %s / %s", source, run_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="Source name: off, usda, openfda")
    parser.add_argument("--run_id", help="Run ID to assign")
    parser.add_argument("--all", action="store_true", help="Auto-discover all silver partitions")
    args = parser.parse_args()

    if args.all:
        for source in ["off", "usda", "openfda"]:
            partitions = _list_silver_partitions(source)
            for i, partition in enumerate(partitions, start=1):
                run_id = f"{source.upper()}_silver_{i:02d}"
                df = _read_silver_parquet(partition)
                if df is not None and not df.empty:
                    backfill_run(source.upper(), run_id, df)
    elif args.source and args.run_id:
        partitions = _list_silver_partitions(args.source)
        if not partitions:
            logger.error("No silver partitions found for source: %s", args.source)
            sys.exit(1)
        df = _read_silver_parquet(partitions[-1])  # latest partition
        if df is None or df.empty:
            logger.error("Could not read silver Parquet for %s", args.source)
            sys.exit(1)
        backfill_run(args.source.upper(), args.run_id, df)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
