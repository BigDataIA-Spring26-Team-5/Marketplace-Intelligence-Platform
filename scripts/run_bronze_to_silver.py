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
    "esci":    "pricing",
}

import re as _re
_DATE_RE = _re.compile(r"(\d{4}/\d{2}/\d{2})$")


def _extract_partition(blob_name: str) -> tuple[str, str] | None:
    """Extract (source, partition_prefix) from a blob name.

    Handles both standard and sub-prefixed layouts:
      usda/2026/04/20/part_0000.jsonl        → ('usda', 'usda/2026/04/20')
      usda/bulk/2026/04/21/part_0000.jsonl   → ('usda', 'usda/bulk/2026/04/21')
      esci/2026/04/20/part_0000.jsonl        → ('esci', 'esci/2026/04/20')
    """
    if not blob_name.endswith(".jsonl"):
        return None
    parts = blob_name.split("/")
    if len(parts) < 5:
        return None
    source = parts[0]
    # Walk from the end to find YYYY/MM/DD (last 3 dir components before filename)
    dir_parts = parts[:-1]  # drop filename
    if len(dir_parts) < 4:
        return None
    date_str = "/".join(dir_parts[-3:])
    if not _DATE_RE.match(date_str):
        return None
    partition_prefix = "/".join(dir_parts)
    return source, partition_prefix


def _list_partitions(bucket_name: str, source_filter: str | None) -> list[tuple[str, str, str]]:
    """Return [(source, partition_prefix, gcs_glob), ...] for all date-partitioned prefixes."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    seen: dict[tuple[str, str], bool] = {}
    prefix = f"{source_filter}/" if source_filter else ""

    for blob in bucket.list_blobs(prefix=prefix):
        result = _extract_partition(blob.name)
        if result is None:
            continue
        source, partition_prefix = result
        if source not in SOURCE_DOMAIN:
            seen.setdefault((source, ""), False)
            seen[(source, "")] = True  # track for warning
            continue
        seen[(source, partition_prefix)] = True

    # Emit warnings for unknown sources once
    unknown = {s for (s, p) in seen if p == "" and s not in SOURCE_DOMAIN}
    for s in sorted(unknown):
        logger.warning("Unknown source '%s' — skipping (add to SOURCE_DOMAIN map)", s)

    result_list = []
    for (source, partition_prefix) in sorted(seen.keys()):
        if not partition_prefix or source not in SOURCE_DOMAIN:
            continue
        gcs_glob = f"gs://{bucket_name}/{partition_prefix}/*.jsonl"
        result_list.append((source, partition_prefix, gcs_glob))

    return result_list


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
    for source, partition_prefix, gcs_glob in partitions:
        logger.info("  [%s] %s → domain=%s", partition_prefix, gcs_glob, SOURCE_DOMAIN[source])

    if args.dry_run:
        logger.info("Dry run — exiting without running pipeline.")
        return

    failed = []
    for source, partition_prefix, gcs_glob in partitions:
        domain = SOURCE_DOMAIN[source]
        label = partition_prefix
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
