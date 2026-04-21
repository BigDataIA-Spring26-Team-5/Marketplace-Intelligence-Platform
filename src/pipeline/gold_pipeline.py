"""
Gold pipeline: Silver GCS Parquet → dedup + enrichment → BigQuery mip_gold.products.

Reads all Parquet files for a given source+date from GCS Silver, runs the gold
block sequence (fuzzy_deduplicate → column_wise_merge → golden_record_select →
extract_allergens → llm_enrich → dq_score_post), and appends the result to BQ.

Usage:
    python -m src.pipeline.gold_pipeline --source off --date 2026/04/21
    python -m src.pipeline.gold_pipeline --source usda --date 2026/04/21 --domain nutrition
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SILVER_BUCKET  = os.environ.get("SILVER_BUCKET",  "mip-silver-2024")
BRONZE_BUCKET  = os.environ.get("BRONZE_BUCKET",  "mip-bronze-2024")
GCS_ENDPOINT   = os.environ.get("GCS_ENDPOINT",   "https://storage.googleapis.com")
GCS_ACCESS_KEY = os.environ.get("GCS_ACCESS_KEY", "")
GCS_SECRET_KEY = os.environ.get("GCS_SECRET_KEY", "")
BQ_PROJECT     = os.environ.get("GCP_PROJECT",    "mip-platform-2024")
BQ_GOLD_DATASET = os.environ.get("BQ_GOLD_DATASET", "mip_gold")
BQ_GOLD_TABLE   = os.environ.get("BQ_GOLD_TABLE",   "products")


def _gcs_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _read_silver_parquet(source_name: str, date: str) -> pd.DataFrame:
    """
    Load all Parquet part-files for source_name/date from GCS Silver.
    Returns a concatenated DataFrame. Raises if no files found.
    """
    prefix = f"{source_name}/{date}/"
    client = _gcs_client()

    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        raise FileNotFoundError(
            f"No Silver Parquet files found at gs://{SILVER_BUCKET}/{prefix}"
        )

    logger.info(f"Reading {len(keys)} Silver Parquet file(s) for {source_name}/{date}")
    frames = []
    for key in sorted(keys):
        obj = client.get_object(Bucket=SILVER_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        frames.append(pd.read_parquet(buf, engine="pyarrow"))

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Loaded {len(df)} rows from Silver")
    return df


def _write_gold_bq(df: pd.DataFrame, source_name: str) -> int:
    """
    Append Gold DataFrame to BigQuery mip_gold.products.
    Adds a 'source_name' column for lineage. Returns rows written.
    """
    from google.cloud import bigquery

    df = df.copy()
    df["source_name"] = source_name

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_GOLD_DATASET}.{BQ_GOLD_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    rows = len(df)
    logger.info(f"Gold: wrote {rows} rows → {table_ref}")
    return rows


def run_gold_pipeline(
    source_name: str,
    date: str,
    domain: str = "nutrition",
    cache_client=None,
) -> int:
    """
    Read Silver Parquet for source_name/date, run gold block sequence, write to BQ.
    Returns number of rows written to BigQuery.
    """
    from src.registry.block_registry import BlockRegistry
    from src.pipeline.runner import PipelineRunner
    from src.schema.analyzer import get_unified_schema

    df = _read_silver_parquet(source_name, date)

    block_reg = BlockRegistry.instance()
    gold_sequence = block_reg.get_gold_sequence(domain=domain)

    # Expand stages to individual block names for PipelineRunner
    expanded: list[str] = []
    for item in gold_sequence:
        if block_reg.is_stage(item):
            expanded.extend(block_reg.expand_stage(item))
        else:
            expanded.append(item)

    unified = get_unified_schema()
    config = {
        "dq_weights": unified.dq_weights.model_dump(),
        "domain": domain,
        "unified_schema": unified,
        "cache_client": cache_client,
    }

    runner = PipelineRunner(block_reg)

    # Run gold blocks directly (no chunked loading — Silver is already in memory)
    result_df, audit_log = runner.run(
        df=df,
        block_sequence=expanded,
        config=config,
    )

    logger.info(f"Gold blocks complete: {len(result_df)} rows after dedup/enrichment")
    rows_written = _write_gold_bq(result_df, source_name=source_name)
    return rows_written


def main():
    parser = argparse.ArgumentParser(description="Silver → Gold pipeline (dedup + enrichment → BQ)")
    parser.add_argument("--source", required=True, choices=["off", "usda", "openfda"], help="Source name")
    parser.add_argument("--date",   required=True, help="Silver partition date YYYY/MM/DD")
    parser.add_argument("--domain", default="nutrition", choices=["nutrition", "safety", "pricing"])
    args = parser.parse_args()

    rows = run_gold_pipeline(source_name=args.source, date=args.date, domain=args.domain)
    logger.info(f"Gold pipeline complete: {rows} rows written to BQ {BQ_GOLD_DATASET}.{BQ_GOLD_TABLE}")


if __name__ == "__main__":
    main()
