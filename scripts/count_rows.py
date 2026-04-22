"""
Row-count audit: Bronze (JSONL) vs Silver (Parquet) per source.

Bronze counts: download each JSONL blob, count non-empty lines.
Silver counts: read Parquet row-group metadata only (no full data scan).

Usage:
    poetry run python scripts/count_rows.py
    poetry run python scripts/count_rows.py --sources off usda openfda
    poetry run python scripts/count_rows.py --date 2026/04/21   # single partition
    poetry run python scripts/count_rows.py --all-dates         # scan all partitions
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from collections import defaultdict
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "mip-bronze-2024")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "mip-silver-2024")
SOURCES = ["off", "usda", "openfda"]


def _gcs_client():
    from google.cloud import storage
    return storage.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT"))


def _count_jsonl_blob(blob) -> int:
    """Download blob bytes and count non-empty lines."""
    data = blob.download_as_bytes()
    return sum(1 for line in data.splitlines() if line.strip())


def _count_parquet_blob(blob) -> int:
    """Read only Parquet metadata to get row count — no full data scan."""
    import pyarrow.parquet as pq
    data = blob.download_as_bytes()
    pf = pq.ParquetFile(io.BytesIO(data))
    return pf.metadata.num_rows


def list_bronze_blobs(client, source: str, date_prefix: Optional[str] = None) -> list:
    """List all .jsonl blobs for a source, optionally under a date prefix."""
    prefix = f"{source}/"
    if date_prefix:
        prefix += date_prefix.rstrip("/") + "/"
    bucket = client.bucket(BRONZE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [b for b in blobs if b.name.endswith(".jsonl")]


def list_silver_blobs(client, source: str, date_prefix: Optional[str] = None) -> list:
    """List all .parquet blobs for a source, optionally under a date prefix."""
    prefix = f"{source}/"
    if date_prefix:
        prefix += date_prefix.rstrip("/") + "/"
    bucket = client.bucket(SILVER_BUCKET)
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [b for b in blobs if b.name.endswith(".parquet")]


def list_quarantine_blobs(client, source: str, date_prefix: Optional[str] = None) -> list:
    """List all .parquet blobs for the quarantine prefix of a source."""
    prefix = f"{source}_quarantine/"
    if date_prefix:
        prefix += date_prefix.rstrip("/") + "/"
    bucket = client.bucket(SILVER_BUCKET)
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [b for b in blobs if b.name.endswith(".parquet")]


def count_source(client, source: str, date_prefix: Optional[str]) -> dict:
    """Return row counts for one source across both layers."""
    print(f"\n{'='*60}")
    print(f"SOURCE: {source.upper()}")
    print(f"{'='*60}")

    # --- Bronze ---
    bronze_blobs = list_bronze_blobs(client, source, date_prefix)
    print(f"\nBronze: {len(bronze_blobs)} JSONL files found")

    bronze_total = 0
    bronze_by_date: dict[str, int] = defaultdict(int)
    for blob in bronze_blobs:
        try:
            n = _count_jsonl_blob(blob)
            # Extract date from path: off/2026/04/21/part_0000.jsonl → 2026/04/21
            parts = blob.name.split("/")
            date_key = "/".join(parts[1:4]) if len(parts) >= 4 else "unknown"
            bronze_by_date[date_key] += n
            bronze_total += n
            logger.info(f"  Bronze {blob.name}: {n:,} rows")
        except Exception as exc:
            logger.warning(f"  Bronze {blob.name}: ERROR — {exc}")

    # --- Silver ---
    silver_blobs = list_silver_blobs(client, source, date_prefix)
    print(f"Silver: {len(silver_blobs)} Parquet files found")

    silver_total = 0
    silver_by_date: dict[str, int] = defaultdict(int)
    for blob in silver_blobs:
        try:
            n = _count_parquet_blob(blob)
            parts = blob.name.split("/")
            date_key = "/".join(parts[1:4]) if len(parts) >= 4 else "unknown"
            silver_by_date[date_key] += n
            silver_total += n
            logger.info(f"  Silver {blob.name}: {n:,} rows")
        except Exception as exc:
            logger.warning(f"  Silver {blob.name}: ERROR — {exc}")

    # --- Quarantine ---
    quarantine_blobs = list_quarantine_blobs(client, source, date_prefix)
    print(f"Quarantine: {len(quarantine_blobs)} Parquet files found")

    quarantine_total = 0
    quarantine_by_date: dict[str, int] = defaultdict(int)
    for blob in quarantine_blobs:
        try:
            n = _count_parquet_blob(blob)
            parts = blob.name.split("/")
            date_key = "/".join(parts[1:4]) if len(parts) >= 4 else "unknown"
            quarantine_by_date[date_key] += n
            quarantine_total += n
            logger.info(f"  Quarantine {blob.name}: {n:,} rows")
        except Exception as exc:
            logger.warning(f"  Quarantine {blob.name}: ERROR — {exc}")

    # --- Per-date breakdown ---
    all_dates = sorted(set(
        list(bronze_by_date.keys()) + list(silver_by_date.keys()) + list(quarantine_by_date.keys())
    ))
    if len(all_dates) > 1:
        print(f"\n{'Date':<14} {'Bronze':>12} {'Silver':>12} {'Quarantine':>12} {'Drop':>8} {'Drop%':>7}")
        print("-" * 72)
        for d in all_dates:
            b = bronze_by_date.get(d, 0)
            s = silver_by_date.get(d, 0)
            q = quarantine_by_date.get(d, 0)
            accounted = s + q
            drop = b - accounted
            pct = (drop / b * 100) if b > 0 else 0.0
            flag = " <-- MISSING DATE IN SILVER" if accounted == 0 and b > 0 else ""
            print(f"{d:<14} {b:>12,} {s:>12,} {q:>12,} {drop:>8,} {pct:>6.1f}%{flag}")

    # --- Totals ---
    accounted_total = silver_total + quarantine_total
    drop = bronze_total - accounted_total
    drop_pct = (drop / bronze_total * 100) if bronze_total > 0 else 0.0
    print(f"\n{'TOTAL':<14} {bronze_total:>12,} {silver_total:>12,} {quarantine_total:>12,} {drop:>8,} {drop_pct:>6.1f}%")
    if quarantine_total > 0:
        q_pct = quarantine_total / bronze_total * 100
        print(f"  → {quarantine_total:,} rows ({q_pct:.1f}%) quarantined in Silver bucket at {source}_quarantine/")

    return {
        "source": source,
        "bronze_files": len(bronze_blobs),
        "silver_files": len(silver_blobs),
        "quarantine_files": len(quarantine_blobs),
        "bronze_rows": bronze_total,
        "silver_rows": silver_total,
        "quarantine_rows": quarantine_total,
        "drop": drop,
        "drop_pct": drop_pct,
    }


def main():
    parser = argparse.ArgumentParser(description="Bronze vs Silver row count audit")
    parser.add_argument(
        "--sources", nargs="+", default=SOURCES,
        choices=SOURCES, help="Sources to audit (default: all)"
    )
    parser.add_argument(
        "--date", default=None,
        help="Limit to a single partition date, e.g. 2026/04/21"
    )
    parser.add_argument(
        "--all-dates", action="store_true",
        help="Scan all partitions (default: same as omitting --date)"
    )
    args = parser.parse_args()

    date_prefix = args.date  # None = all dates

    try:
        client = _gcs_client()
    except Exception as exc:
        print(f"ERROR: Could not create GCS client: {exc}")
        print("Run: gcloud auth application-default login")
        sys.exit(1)

    results = []
    for source in args.sources:
        try:
            r = count_source(client, source, date_prefix)
            results.append(r)
        except Exception as exc:
            logger.error(f"{source}: failed — {exc}")

    # --- Summary table ---
    print(f"\n\n{'='*72}")
    print("SUMMARY")
    print(f"{'='*72}")
    print(f"{'Source':<10} {'Bronze':>12} {'Silver':>12} {'Quarantine':>12} {'Drop':>8} {'Drop%':>7}")
    print("-" * 66)
    grand_b = grand_s = grand_q = 0
    for r in results:
        print(
            f"{r['source']:<10} {r['bronze_rows']:>12,} {r['silver_rows']:>12,} "
            f"{r['quarantine_rows']:>12,} {r['drop']:>8,} {r['drop_pct']:>6.1f}%"
        )
        grand_b += r["bronze_rows"]
        grand_s += r["silver_rows"]
        grand_q += r["quarantine_rows"]

    if len(results) > 1:
        grand_accounted = grand_s + grand_q
        grand_drop = grand_b - grand_accounted
        grand_pct = (grand_drop / grand_b * 100) if grand_b > 0 else 0.0
        print("-" * 66)
        print(
            f"{'TOTAL':<10} {grand_b:>12,} {grand_s:>12,} "
            f"{grand_q:>12,} {grand_drop:>8,} {grand_pct:>6.1f}%"
        )

    print(f"\nDone. Bronze bucket: {BRONZE_BUCKET} | Silver bucket: {SILVER_BUCKET}")


if __name__ == "__main__":
    main()
