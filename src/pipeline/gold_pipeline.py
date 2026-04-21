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
import time
import uuid
from datetime import datetime, timezone
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

SILVER_BUCKET   = os.environ.get("SILVER_BUCKET",   "mip-silver-2024")
BQ_PROJECT      = os.environ.get("GCP_PROJECT",     "mip-platform-2024")
BQ_GOLD_DATASET = os.environ.get("BQ_GOLD_DATASET", "mip_gold")
BQ_GOLD_TABLE   = os.environ.get("BQ_GOLD_TABLE",   "products")


def _gcs_client():
    from google.cloud import storage
    return storage.Client()


def _read_silver_parquet(source_name: str, date: str) -> pd.DataFrame:
    """
    Load all Parquet part-files for source_name/date from GCS Silver.
    Returns a concatenated DataFrame. Raises if no files found.
    """
    prefix = f"{source_name}/{date}/"
    client = _gcs_client()
    bucket = client.bucket(SILVER_BUCKET)
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]

    if not blobs:
        raise FileNotFoundError(
            f"No Silver Parquet files found at gs://{SILVER_BUCKET}/{prefix}"
        )

    logger.info(f"Reading {len(blobs)} Silver Parquet file(s) for {source_name}/{date}")
    frames = []
    for blob in sorted(blobs, key=lambda b: b.name):
        buf = io.BytesIO(blob.download_as_bytes())
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
    from src.blocks.dq_score import _SKIP_ALWAYS

    run_id = f"{source_name.upper()}_gold_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = time.monotonic()

    df = _read_silver_parquet(source_name, date)
    rows_in = len(df)

    # Fix: restore dq_reference_columns so dq_score_post uses the same column set
    # as dq_score_pre did during the silver run (df.attrs is not preserved in Parquet)
    df.attrs["dq_reference_columns"] = [c for c in df.columns if c not in _SKIP_ALWAYS]

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

    result_df, audit_log = runner.run(
        df=df,
        block_sequence=expanded,
        config=config,
    )

    duration_seconds = round(time.monotonic() - start_time, 3)
    logger.info(f"Gold blocks complete: {len(result_df)} rows after dedup/enrichment")
    rows_written = _write_gold_bq(result_df, source_name=source_name)

    _push_uc2_metrics(
        run_id=run_id,
        source_name=source_name,
        rows_in=rows_in,
        result_df=result_df,
        audit_log=audit_log,
        duration_seconds=duration_seconds,
    )

    return rows_written


def _push_uc2_metrics(
    run_id: str,
    source_name: str,
    rows_in: int,
    result_df,
    audit_log: list,
    duration_seconds: float,
) -> None:
    """Push gold run metrics to Prometheus Pushgateway and write audit events to Postgres."""
    try:
        from src.uc2_observability.metrics_collector import MetricsCollector
        import psycopg2
        from src.uc2_observability.kafka_to_pg import PG_DSN

        dq_post = float(result_df["dq_score_post"].mean()) if "dq_score_post" in result_df.columns else 0.0
        dq_pre  = float(result_df["dq_score_pre"].mean())  if "dq_score_pre"  in result_df.columns else 0.0
        null_rate = float(result_df.isna().mean().mean())

        # Enrichment stats from LLMEnrichBlock.last_enrichment_stats
        try:
            from src.blocks.llm_enrich import LLMEnrichBlock
            es = LLMEnrichBlock.last_enrichment_stats
        except Exception:
            es = {}

        metrics = {
            "rows_in":                rows_in,
            "rows_out":               len(result_df),
            "null_rate":              round(null_rate, 4),
            "dq_score_pre":           round(dq_pre, 4),
            "dq_score_post":          round(dq_post, 4),
            "dq_delta":               round(dq_post - dq_pre, 4),
            "dedup_rate":             round(1 - len(result_df) / rows_in, 4) if rows_in else 0.0,
            "s1_count":               es.get("deterministic", 0),
            "s2_count":               es.get("embedding", 0),
            "s3_count":               es.get("llm", 0),
            "s4_count":               0,
            "quarantine_rows":        0,
            "llm_calls":              0,
            "cost_usd":               0.0,
            "block_duration_seconds": duration_seconds,
            "status":                 "success",
        }

        MetricsCollector().push(run_id=run_id, source=source_name.upper(), metrics_dict=metrics)
        logger.info("UC2 metrics pushed for run_id=%s", run_id)

        # Write audit events to Postgres
        conn = psycopg2.connect(PG_DSN)
        ts = datetime.now(timezone.utc)
        import json
        with conn.cursor() as cur:
            for event_type in ("run_started", "run_completed"):
                cur.execute(
                    """INSERT INTO audit_events (run_id, source, event_type, status, ts, payload)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (run_id, source_name.upper(), event_type, "success", ts, json.dumps(metrics)),
                )
        conn.commit()
        conn.close()
        logger.info("UC2 audit events written for run_id=%s", run_id)

    except Exception as exc:
        logger.warning("UC2 metrics push failed (non-fatal): %s", exc)


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
