"""Batch Bronze → Silver: discovers all source/date partitions in mip-bronze-2024
and runs the ETL pipeline for each partition.

Usage:
    poetry run python scripts/run_bronze_to_silver.py
    poetry run python scripts/run_bronze_to_silver.py --source usda --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.pipeline.cli import run_pipeline  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BRONZE_BUCKET = "mip-bronze-2024"

SOURCE_DOMAIN = {
    "usda":    "nutrition",
    "openfda": "safety",
    "off":     "nutrition",
}


def _list_partitions(bucket_name: str, source_filter: str | None) -> list[tuple[str, str, str]]:
    """Return [(source, date_path, gcs_glob), ...] for all date-partitioned prefixes."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Group blobs by source/YYYY/MM/DD prefix
    partitions: dict[tuple[str, str], bool] = defaultdict(bool)
    prefix = f"{source_filter}/" if source_filter else ""

    for blob in bucket.list_blobs(prefix=prefix):
        parts = blob.name.split("/")
        # Expect: {source}/{YYYY}/{MM}/{DD}/part_NNNN.jsonl
        if len(parts) >= 5 and parts[-1].endswith(".jsonl"):
            source = parts[0]
            date_path = "/".join(parts[1:4])  # YYYY/MM/DD
            partitions[(source, date_path)] = True

    result = []
    for (source, date_path) in sorted(partitions.keys()):
        if source not in SOURCE_DOMAIN:
            logger.warning("Unknown source '%s' — skipping (add to SOURCE_DOMAIN map)", source)
            continue
        gcs_glob = f"gs://{bucket_name}/{source}/{date_path}/*.jsonl"
        result.append((source, date_path, gcs_glob))

    return result


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver batch runner")
    parser.add_argument("--source", help="Limit to one source (usda / openfda / off)")
    parser.add_argument("--dry-run", action="store_true", help="Print partitions, skip execution")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint per run")
    parser.add_argument("--chunk-size", type=int, default=10000)
    args = parser.parse_args()

    partitions = _list_partitions(BRONZE_BUCKET, args.source)

    if not partitions:
        logger.error("No partitions found in gs://%s (source_filter=%s)", BRONZE_BUCKET, args.source)
        sys.exit(1)

    logger.info("Found %d partition(s) to process:", len(partitions))
    for source, date_path, gcs_glob in partitions:
        logger.info("  [%s] %s → domain=%s", date_path, gcs_glob, SOURCE_DOMAIN[source])

    if args.dry_run:
        logger.info("Dry run — exiting without running pipeline.")
        return

    failed = []
    for source, date_path, gcs_glob in partitions:
        domain = SOURCE_DOMAIN[source]
        label = f"{source}/{date_path}"
        logger.info("=" * 60)
        logger.info("Running: %s  domain=%s", label, domain)
        logger.info("=" * 60)
        try:
            result = run_pipeline(
                source_path=gcs_glob,
                domain=domain,
                resume=args.resume,
                pipeline_mode="silver",
                chunk_size=args.chunk_size,
            )
            rows = len(result.get("working_df") or [])
            logger.info("Done: %s — %d rows written to Silver", label, rows)
        except Exception as exc:
            logger.error("FAILED: %s — %s", label, exc)
            failed.append((label, exc))

    logger.info("=" * 60)
    if failed:
        logger.error("%d partition(s) failed:", len(failed))
        for label, exc in failed:
            logger.error("  %s: %s", label, exc)
        sys.exit(1)
    else:
        logger.info("All %d partition(s) complete.", len(partitions))


if __name__ == "__main__":
    main()
