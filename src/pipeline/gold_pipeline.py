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
import json
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


def _sanitize_nan(obj):
    """Replace NaN/Inf floats with None for valid JSON serialization."""
    import math
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]
    return obj


def _enrich_with_safety_signals(gold_df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    LEFT JOIN nutrition Gold with safety Silver on (product_name, brand_name).
    Adds is_recalled (bool), recall_class, recall_reason columns.
    Overwrites allergens with recall_reason where an OpenFDA match is found
    (ground-truth allergen data overrides S1 extraction).
    Non-fatal — on any failure returns gold_df with is_recalled=False.
    """
    try:
        prefix = f"openfda/{date}/"
        client = _gcs_client()
        bucket = client.bucket(SILVER_BUCKET)
        blobs = [
            b for b in bucket.list_blobs(prefix=prefix)
            if b.name.endswith(".parquet") and not b.name.split("/")[-1].startswith("sample")
        ]
        if not blobs:
            logger.warning("No safety Silver Parquet found at gs://%s/%s — skipping safety join", SILVER_BUCKET, prefix)
            gold_df["is_recalled"]  = False
            gold_df["recall_class"] = None
            gold_df["recall_reason"] = None
            return gold_df

        frames = []
        for blob in sorted(blobs, key=lambda b: b.name):
            buf = io.BytesIO(blob.download_as_bytes())
            frames.append(pd.read_parquet(buf, engine="pyarrow"))
        safety_df = pd.concat(frames, ignore_index=True)

        # Keep only rows with actual recall info; deduplicate on join keys
        recall_cols = [c for c in ["product_name", "brand_name", "recall_class", "recall_reason", "recall_status"] if c in safety_df.columns]
        safety_df = safety_df[recall_cols].dropna(subset=["recall_class"])
        safety_df = safety_df.drop_duplicates(subset=["product_name", "brand_name"], keep="first")

        logger.info("Safety join: %d recall records from openfda/%s", len(safety_df), date)

        merged = gold_df.merge(
            safety_df.rename(columns={
                "recall_class":  "_rc_class",
                "recall_reason": "_rc_reason",
                "recall_status": "_rc_status",
            }),
            on=["product_name", "brand_name"],
            how="left",
        )

        merged["is_recalled"]  = merged["_rc_class"].notna()
        merged["recall_class"] = merged.pop("_rc_class")
        merged["recall_reason"] = merged.pop("_rc_reason")
        if "_rc_status" in merged.columns:
            merged.pop("_rc_status")

        # Ground-truth allergen override for matched rows
        matched = merged["is_recalled"] & merged["recall_reason"].notna()
        if matched.any():
            merged.loc[matched, "allergens"] = merged.loc[matched, "recall_reason"]
            logger.info("Allergen override: %d rows updated from recall_reason", matched.sum())

        logger.info("Safety join complete: %d recalled products in nutrition Gold", merged["is_recalled"].sum())
        return merged

    except Exception as exc:
        logger.warning("Safety enrichment failed (non-fatal) — setting is_recalled=False: %s", exc)
        gold_df["is_recalled"]   = False
        gold_df["recall_class"]  = None
        gold_df["recall_reason"] = None
        return gold_df


_REQUIRED_SILVER_COLUMNS = {"product_name"}
_EXPECTED_SILVER_COLUMNS = {
    "product_name", "brand_name", "ingredients",
    "dq_score_pre", "source_name",
}


def _validate_silver_schema(df: pd.DataFrame, source_name: str) -> None:
    """Raise ValueError if required columns are absent; warn on expected-but-missing."""
    missing_required = _REQUIRED_SILVER_COLUMNS - set(df.columns)
    if missing_required:
        raise ValueError(
            f"Silver Parquet for source '{source_name}' is missing required columns: "
            f"{sorted(missing_required)}. Found: {sorted(df.columns)}"
        )
    missing_expected = _EXPECTED_SILVER_COLUMNS - set(df.columns)
    if missing_expected:
        logger.warning(
            "Silver Parquet for '%s' is missing expected columns (pipeline will continue): %s",
            source_name,
            sorted(missing_expected),
        )


def _read_silver_parquet(source_name: str, date: str) -> pd.DataFrame:
    """
    Load all Parquet part-files for source_name/date from GCS Silver.
    Returns a concatenated DataFrame. Raises if no files found.
    """
    prefix = f"{source_name}/{date}/"
    client = _gcs_client()
    bucket = client.bucket(SILVER_BUCKET)
    blobs = [
        b for b in bucket.list_blobs(prefix=prefix)
        if b.name.endswith(".parquet") and not b.name.split("/")[-1].startswith("sample")
    ]

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
    string_cols = [c for c in df.columns if str(df[c].dtype) == "string"]
    if string_cols:
        df[string_cols] = df[string_cols].astype(object)
        logger.debug("Cast %d StringDtype columns to object: %s", len(string_cols), string_cols)
    logger.info(f"Loaded {len(df)} rows from Silver")
    _validate_silver_schema(df, source_name)
    return df



def _write_gold_bq(df: pd.DataFrame, source_name: str) -> int:
    """
    Append Gold DataFrame to BigQuery mip_gold.products.
    Adds a 'source_name' column for lineage. Returns rows written.
    """
    from google.cloud import bigquery

    df = df.copy()
    df["source_name"] = source_name

    # Force-cast expected string columns to object (str) so BQ autodetect
    # never infers INTEGER/FLOAT for null-only columns in sparse sources.
    _STRING_COLS = {
        "product_name", "brand_name", "brand_owner", "ingredients",
        "category", "serving_size_unit", "published_date", "data_source",
        "allergens", "primary_category", "dietary_tags", "source_name",
        "recall_class", "recall_reason", "recall_number", "recall_status",
        "distribution_pattern",
    }
    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].where(df[col].isna(), df[col].astype(str)).astype(object)

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
    skip_enrichment: bool = False,
) -> int:
    """
    Read Silver Parquet for source_name/date, run gold block sequence, write to BQ.
    Returns number of rows written to BigQuery.
    """
    from src.registry.block_registry import BlockRegistry
    from src.pipeline.runner import PipelineRunner
    from src.schema.analyzer import get_domain_schema
    from src.blocks.dq_score import _SKIP_ALWAYS

    if cache_client is None:
        try:
            from src.cache.client import CacheClient
            cache_client = CacheClient()
            if not cache_client._available:
                logger.warning("Redis unavailable — running without cache (SQLite fallback active)")
        except Exception as e:
            logger.warning(f"Cache init failed — running without cache: {e}")

    run_id = f"{source_name.upper()}_gold_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = time.monotonic()

    df = _read_silver_parquet(source_name, date)
    rows_in = len(df)

    # Fix: restore dq_reference_columns so dq_score_post uses the same column set
    # as dq_score_pre did during the silver run (df.attrs is not preserved in Parquet)
    df.attrs["dq_reference_columns"] = [c for c in df.columns if c not in _SKIP_ALWAYS]

    block_reg = BlockRegistry.instance()
    gold_sequence = block_reg.get_gold_sequence(domain=domain)

    if skip_enrichment:
        _ENRICHMENT_BLOCKS = {"extract_allergens", "llm_enrich"}
        gold_sequence = [b for b in gold_sequence if b not in _ENRICHMENT_BLOCKS]
        logger.info("--skip-enrichment: removed enrichment blocks from gold sequence")

    # Expand stages to individual block names for PipelineRunner
    expanded: list[str] = []
    for item in gold_sequence:
        if block_reg.is_stage(item):
            expanded.extend(block_reg.expand_stage(item))
        else:
            expanded.append(item)

    unified = get_domain_schema(domain)
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

    if domain == "nutrition":
        result_df = _enrich_with_safety_signals(result_df, date)

    bq_error: str | None = None
    rows_written = 0
    try:
        rows_written = _write_gold_bq(result_df, source_name=source_name)
    except Exception as exc:
        bq_error = str(exc)
        logger.error("BQ write failed (observability will still be saved): %s", exc)

    run_log = _build_gold_run_log(
        run_id=run_id,
        source_name=source_name,
        domain=domain,
        rows_in=rows_in,
        result_df=result_df,
        audit_log=audit_log,
        duration_seconds=duration_seconds,
        status="failed" if bq_error else "success",
        error=bq_error,
    )
    _save_gold_run_log(run_log)
    _push_gold_metrics(run_log)
    _push_gold_audit(run_log)

    if bq_error:
        raise RuntimeError(f"BQ write failed: {bq_error}")

    return rows_written


def _build_gold_run_log(
    run_id: str,
    source_name: str,
    domain: str,
    rows_in: int,
    result_df: pd.DataFrame,
    audit_log: list,
    duration_seconds: float,
    status: str = "success",
    error: str | None = None,
) -> dict:
    """Build run-log dict consumed by _save_gold_run_log, _push_gold_metrics, _push_gold_audit."""
    dq_pre  = float(result_df["dq_score_pre"].mean())  if "dq_score_pre"  in result_df.columns else None
    dq_post = float(result_df["dq_score_post"].mean()) if "dq_score_post" in result_df.columns else None
    dq_delta = round(dq_post - dq_pre, 4) if (dq_pre is not None and dq_post is not None) else None

    try:
        from src.blocks.llm_enrich import LLMEnrichBlock
        es = LLMEnrichBlock.last_enrichment_stats
    except Exception:
        es = {}

    return {
        "run_id":           run_id,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "source_name":      source_name,
        "domain":           domain,
        "status":           status,
        "error":            error,
        "duration_seconds": round(duration_seconds, 3),
        "rows_in":          rows_in,
        "rows_out":         len(result_df),
        "rows_quarantined": 0,
        "dq_score_pre":     round(dq_pre,  4) if dq_pre  is not None else None,
        "dq_score_post":    round(dq_post, 4) if dq_post is not None else None,
        "dq_delta":         dq_delta,
        "enrichment_stats": {
            "deterministic":    es.get("deterministic",    0),
            "embedding":        es.get("embedding",        0),
            "llm":              es.get("llm",              0),
            "unresolved":       es.get("unresolved",       0),
            "corpus_augmented": es.get("corpus_augmented", 0),
            "corpus_size_after": es.get("corpus_size_after", 0),
        },
        "audit_log": audit_log,
    }


def _save_gold_run_log(run_log: dict) -> Path | None:
    """Write run_log to output/run_logs/ as JSON. No external deps — always local."""
    try:
        log_dir = PROJECT_ROOT / "output" / "run_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        short_id = run_log["run_id"][-8:]
        path = log_dir / f"run_{ts}_{short_id}.json"
        path.write_text(json.dumps(run_log, indent=2, default=str))
        logger.info("Run log written: %s", path)
        return path
    except Exception as exc:
        logger.warning("Run log write failed (non-fatal): %s", exc)
        return None


def _push_gold_metrics(run_log: dict) -> None:
    """Push metrics to Prometheus Pushgateway via MetricsExporter (source_name label → Grafana)."""
    try:
        from src.uc2_observability.metrics_exporter import MetricsExporter
        MetricsExporter().push(run_log)
        logger.info("Prometheus metrics pushed for run_id=%s", run_log["run_id"])
    except Exception as exc:
        logger.warning("Prometheus push failed (non-fatal): %s", exc)


def _push_gold_audit(run_log: dict) -> None:
    """Write audit events to Postgres. Independent try block — psycopg2 failure never blocks metrics."""
    try:
        import psycopg2
        from src.uc2_observability.kafka_to_pg import PG_DSN
        conn = psycopg2.connect(PG_DSN)
        ts = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for event_type in ("run_started", "run_completed"):
                cur.execute(
                    """INSERT INTO audit_events (run_id, source, event_type, status, ts, payload)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (run_log["run_id"], run_log["source_name"], event_type,
                     run_log["status"], ts, json.dumps(_sanitize_nan(run_log))),
                )
        conn.commit()
        conn.close()
        logger.info("Postgres audit events written for run_id=%s", run_log["run_id"])
    except Exception as exc:
        logger.warning("Postgres audit write failed (non-fatal): %s", exc)


def main():
    parser = argparse.ArgumentParser(description="Silver → Gold pipeline (dedup + enrichment → BQ)")
    parser.add_argument("--source", required=True, help="Source name (e.g. off, usda/branded, usda/survey)")
    parser.add_argument("--date",   required=True, help="Silver partition date YYYY/MM/DD")
    parser.add_argument("--domain", default="nutrition", choices=["nutrition", "safety", "pricing", "retail"])
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip enrichment blocks (dedup-only run)")
    args = parser.parse_args()

    rows = run_gold_pipeline(
        source_name=args.source,
        date=args.date,
        domain=args.domain,
        skip_enrichment=args.skip_enrichment,
    )
    logger.info(f"Gold pipeline complete: {rows} rows written to BQ {BQ_GOLD_DATASET}.{BQ_GOLD_TABLE}")


if __name__ == "__main__":
    main()
