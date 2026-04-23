"""Batch Bronze → Silver: discovers all source/date partitions in mip-bronze-2024
and runs the ETL pipeline for each partition.

Usage:
    poetry run python scripts/run_bronze_to_silver.py
    poetry run python scripts/run_bronze_to_silver.py --source usda --dry-run
    poetry run python scripts/run_bronze_to_silver.py --source usda/branded
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
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

# Domain keyed by root source (first path component).
SOURCE_DOMAIN = {
    "usda":    "nutrition",
    "openfda": "safety",
    "off":     "nutrition",
    "esci":    "retail",
}

# Logical source names to never process (leave in Bronze).
# usda/usda/incremental: superseded by branded+foundation sub-type partitions.
SKIP_SOURCES = {"usda/sr_legacy", "usda/survey", "usda", "usda/incremental"}

# Remap logical source names to canonical Silver source names.
# Use when a DAG sub-directory is an infra artifact, not a semantic sub-type.
# off/delta is a DAG-generated delta dir on top of the full 04/21 snapshot.
SOURCE_ALIAS: dict[str, str] = {
    "off/delta": "off",
}

# For these sources, only the latest date partition is processed.
# Older date partitions are superseded by the most recent full snapshot.
# off: 04/21 is a full snapshot that includes all prior incremental dates (04/09-04/20).
# esci: 2024/01/01 and 2026/04/20 are the same dataset re-partitioned; keep latest.
LATEST_ONLY_SOURCES: set[str] = {"off", "esci"}


def _extract_partition(blob_name: str) -> tuple[str, str] | None:
    """Extract (logical_source_name, partition_prefix) from a blob name.

    Finds the YYYY date component, derives the logical source name as
    root + any sub-type directory after the date, and builds the partition
    prefix that points directly to those files.

    Examples:
      usda/2026/04/20/part_0000.jsonl              → ('usda',          'usda/2026/04/20')
      usda/bulk/2026/04/21/branded/part_0000.jsonl → ('usda/branded',  'usda/bulk/2026/04/21/branded')
      usda/bulk/2026/04/21/survey/part_0000.jsonl  → ('usda/survey',   'usda/bulk/2026/04/21/survey')
      off/2026/04/21/part_0000.jsonl               → ('off',           'off/2026/04/21')
      esci/2024/01/01/part_0000.jsonl              → ('esci',          'esci/2024/01/01')
    """
    if not blob_name.endswith(".jsonl"):
        return None
    parts = blob_name.split("/")
    year_idx = next((i for i, p in enumerate(parts) if re.match(r"^\d{4}$", p)), None)
    if year_idx is None or year_idx + 2 >= len(parts):
        return None

    root = parts[0]
    # Sub-type dirs: anything between YYYY/MM/DD and the filename
    after_date = [p for p in parts[year_idx + 3:] if "." not in p]
    logical_source = f"{root}/{'/'.join(after_date)}" if after_date else root

    # Partition prefix = everything up to and including sub-type (no filename)
    partition_prefix = "/".join(parts[:year_idx + 3])
    if after_date:
        partition_prefix = f"{partition_prefix}/{'/'.join(after_date)}"

    return logical_source, partition_prefix


def _list_partitions(bucket_name: str, source_filter: str | None) -> list[tuple[str, str, str, str]]:
    """Return [(logical_source, partition_prefix, gcs_glob, domain), ...].

    source_filter may be a root source ('usda') or a full logical source ('usda/branded').
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Use root of filter as GCS prefix for efficient listing
    prefix = ""
    if source_filter:
        prefix = f"{source_filter.split('/')[0]}/"

    seen: dict[tuple[str, str], None] = {}

    for blob in bucket.list_blobs(prefix=prefix):
        result = _extract_partition(blob.name)
        if result is None:
            continue
        logical_source, partition_prefix = result
        root_source = logical_source.split("/")[0]
        if root_source not in SOURCE_DOMAIN:
            continue
        seen[(logical_source, partition_prefix)] = None

    result_list = []
    for (logical_source, partition_prefix) in sorted(seen.keys()):
        if logical_source in SKIP_SOURCES:
            logger.info("Skipping %s (SKIP_SOURCES)", logical_source)
            continue
        # Apply source filter (exact match or prefix)
        if source_filter and logical_source != source_filter and not logical_source.startswith(source_filter + "/"):
            continue
        root_source = logical_source.split("/")[0]
        domain = SOURCE_DOMAIN[root_source]
        gcs_glob = f"gs://{bucket_name}/{partition_prefix}/*.jsonl"
        # Apply alias: map infra sub-dirs to canonical Silver source name
        canonical_source = SOURCE_ALIAS.get(logical_source, logical_source)
        if canonical_source != logical_source:
            logger.info("Aliasing %s → %s", logical_source, canonical_source)
        result_list.append((canonical_source, partition_prefix, gcs_glob, domain))

    # For LATEST_ONLY_SOURCES: drop all partitions except the newest date per source.
    if LATEST_ONLY_SOURCES:
        max_dates: dict[str, str] = {}
        for canonical_source, partition_prefix, _, _ in result_list:
            if canonical_source in LATEST_ONLY_SOURCES:
                m = re.search(r"/(\d{4}/\d{2}/\d{2})(?:/|$)", partition_prefix)
                if m:
                    date = m.group(1)
                    if canonical_source not in max_dates or date > max_dates[canonical_source]:
                        max_dates[canonical_source] = date
        filtered = []
        for item in result_list:
            canonical_source, partition_prefix, _, _ = item
            if canonical_source in LATEST_ONLY_SOURCES:
                m = re.search(r"/(\d{4}/\d{2}/\d{2})(?:/|$)", partition_prefix)
                date = m.group(1) if m else ""
                if date != max_dates.get(canonical_source, ""):
                    logger.info(
                        "Skipping older %s partition %s (latest=%s)",
                        canonical_source, partition_prefix, max_dates[canonical_source],
                    )
                    continue
            filtered.append(item)
        result_list = filtered

    # Warn about unknown root sources (once)
    all_roots = set()
    for blob in bucket.list_blobs(prefix=prefix, max_results=500):
        if "/" in blob.name:
            all_roots.add(blob.name.split("/")[0])
    for root in sorted(all_roots - set(SOURCE_DOMAIN.keys())):
        logger.warning("Unknown root source '%s' — skipping (add to SOURCE_DOMAIN)", root)

    return result_list


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver batch runner")
    parser.add_argument(
        "--source",
        help="Limit to one source root or logical name (e.g. usda, usda/branded, off)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print partitions, skip execution")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint per run")
    parser.add_argument("--chunk-size", type=int, default=10000)
    args = parser.parse_args()

    partitions = _list_partitions(BRONZE_BUCKET, args.source)

    if not partitions:
        logger.error("No partitions found in gs://%s (source_filter=%s)", BRONZE_BUCKET, args.source)
        sys.exit(1)

    logger.info("Found %d partition(s) to process:", len(partitions))
    for logical_source, partition_prefix, gcs_glob, domain in partitions:
        logger.info("  [%s] %s → domain=%s", logical_source, gcs_glob, domain)

    if args.dry_run:
        logger.info("Dry run — exiting without running pipeline.")
        return

    failed = []
    for logical_source, partition_prefix, gcs_glob, domain in partitions:
        logger.info("=" * 60)
        logger.info("Running: %s  domain=%s", logical_source, domain)
        logger.info("=" * 60)
        try:
            result = run_pipeline(
                source_path=gcs_glob,
                domain=domain,
                resume=args.resume,
                pipeline_mode="silver",
                chunk_size=args.chunk_size,
                source_name_override=logical_source,
            )
            rows = len(result.get("working_df") or [])
            logger.info("Done: %s — %d rows written to Silver", logical_source, rows)
        except Exception as exc:
            logger.error("FAILED: %s — %s", logical_source, exc)
            failed.append((logical_source, exc))

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
