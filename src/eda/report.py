"""Reusable EDA library for bronze / silver / gold layers.

Loaders are defensive: missing data returns an empty DataFrame with a warning,
never raises. That keeps the Streamlit EDA page and CLI driver usable when a
source is only partially populated.

Canonical anchors (see docs/data_inventory.md):
    ('usda',    '2026/04/21', 'nutrition')
    ('off',     '2026/04/22', 'nutrition')
    ('openfda', '2026/04/20', 'safety')
    ('esci',    '2026/04/20', 'retail')
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "mip-bronze-2024"
SILVER_BUCKET = "mip-silver-2024"
GOLD_BQ_TABLE = "mip_gold.products"
LOCAL_RUN_LOGS = Path("output/run_logs")
LOCAL_GOLD_DIR = Path("output/gold")

# USDA silver has domain-alias paths in addition to source-prefixed paths.
_SILVER_ALIASES = {
    "usda": ["usda", "branded", "foundation"],
    "off": ["off"],
    "openfda": ["openfda"],
    "esci": ["esci"],
}


# ---------------------------------------------------------------------------
# Bronze loader
# ---------------------------------------------------------------------------
def load_bronze(
    source: str,
    date: str,
    limit: int | None = 5000,
) -> pd.DataFrame:
    """Load Bronze JSONL for a (source, date) partition.

    Args:
        source: canonical source name (usda / off / openfda / esci).
        date:   partition date as 'YYYY/MM/DD'.
        limit:  cap rows read per file for EDA sampling (None = read all).

    Returns:
        DataFrame of raw source records, or empty DataFrame if no files.
    """
    prefixes = [f"{source}/{date}/"]
    # USDA has a parallel `bulk/<date>/branded/` layout for backfills.
    if source == "usda":
        prefixes += [f"usda/{date}/incremental/", f"usda/bulk/{date}/branded/"]

    blobs: list[str] = []
    for prefix in prefixes:
        blobs.extend(_list_gcs_blobs(BRONZE_BUCKET, prefix, suffix=".jsonl"))

    if not blobs:
        logger.warning(f"Bronze: no blobs for {source} {date}")
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    rows_left = limit
    for blob in blobs:
        df = _read_jsonl_from_gcs(BRONZE_BUCKET, blob, nrows=rows_left)
        if df.empty:
            continue
        frames.append(df)
        if rows_left is not None:
            rows_left -= len(df)
            if rows_left <= 0:
                break

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Silver loader
# ---------------------------------------------------------------------------
def load_silver(
    source: str,
    date: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load Silver Parquet for a (source, date) partition.

    Handles the USDA domain-alias layout where Silver is split into
    `branded/` and `foundation/` instead of `usda/`.
    """
    aliases = _SILVER_ALIASES.get(source, [source])
    frames: list[pd.DataFrame] = []

    for alias in aliases:
        prefix = f"{alias}/{date}/"
        blobs = _list_gcs_blobs(SILVER_BUCKET, prefix, suffix=".parquet")
        for blob in blobs:
            if "sample.parquet" in blob:
                continue  # skip developer samples; part_*.parquet is canonical
            df = _read_parquet_from_gcs(SILVER_BUCKET, blob)
            if not df.empty:
                frames.append(df)

    if not frames:
        logger.warning(f"Silver: no blobs for {source} {date}")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    if limit is not None:
        out = out.head(limit)
    return out


# ---------------------------------------------------------------------------
# Gold loader
# ---------------------------------------------------------------------------
def load_gold(
    source: str | None = None,
    limit: int | None = 50_000,
    use_bq: bool = True,
) -> pd.DataFrame:
    """Load Gold rows.

    Try BigQuery `mip_gold.products` first (if `use_bq=True`); fall back to the
    local parquet `output/gold/nutrition.parquet` so the library still works
    offline.

    NOTE: ~89% of Gold rows have NULL `_source`/`_bronze_file` — filtering by
    source will under-count. Pass `source=None` to get the full table.
    """
    if use_bq:
        try:
            from google.cloud import bigquery

            client = bigquery.Client()
            where = f"WHERE _source = '{source}'" if source else ""
            cap = f"LIMIT {limit}" if limit else ""
            sql = f"SELECT * FROM `{GOLD_BQ_TABLE}` {where} {cap}"
            return client.query(sql).to_dataframe()
        except Exception as exc:
            logger.warning(f"Gold BQ fetch failed ({exc}); falling back to local parquet.")

    # Local fallback
    candidates = list(LOCAL_GOLD_DIR.glob("*.parquet"))
    if not candidates:
        logger.warning("Gold: no local parquet in output/gold/.")
        return pd.DataFrame()

    frames = [pd.read_parquet(p) for p in candidates]
    out = pd.concat(frames, ignore_index=True)
    if source and "_source" in out.columns:
        filtered = out[out["_source"] == source]
        # If filtering eliminates everything (NULL _source), fall back to unfiltered.
        if not filtered.empty:
            out = filtered
    if limit is not None:
        out = out.head(limit)
    return out


# ---------------------------------------------------------------------------
# Run logs loader
# ---------------------------------------------------------------------------
def load_run_logs(include_gcs: bool = True) -> pd.DataFrame:
    """Load all pipeline run logs as one DataFrame.

    Reads local `output/run_logs/*.json` + (optional) `gs://mip-silver-2024/run-logs/`.
    One row per run; nested structures (block_sequence, audit_log) stay as lists.
    """
    records: list[dict[str, Any]] = []

    if LOCAL_RUN_LOGS.exists():
        for p in sorted(LOCAL_RUN_LOGS.glob("*.json")):
            try:
                records.append(_load_json(p))
            except Exception as exc:
                logger.warning(f"run_log {p.name}: {exc}")

    if include_gcs:
        blobs = _list_gcs_blobs(SILVER_BUCKET, "run-logs/", suffix=".json")
        for blob in blobs:
            try:
                records.append(_read_json_from_gcs(SILVER_BUCKET, blob))
            except Exception as exc:
                logger.warning(f"run_log gs://{SILVER_BUCKET}/{blob}: {exc}")

    if not records:
        return pd.DataFrame()

    # Deduplicate on run_id if present
    df = pd.DataFrame(records)
    if "run_id" in df.columns:
        df = df.drop_duplicates(subset=["run_id"], keep="last")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values("timestamp")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Stats aggregation
# ---------------------------------------------------------------------------
@dataclass
class EDAStats:
    """Aggregated EDA statistics across bronze / silver / gold layers."""

    source: str
    date: str

    # Shape
    bronze_shape: tuple[int, int] = (0, 0)
    silver_shape: tuple[int, int] = (0, 0)
    gold_shape: tuple[int, int] = (0, 0)

    # Nulls (column -> null fraction)
    bronze_nulls: dict[str, float] = field(default_factory=dict)
    silver_nulls: dict[str, float] = field(default_factory=dict)
    gold_nulls: dict[str, float] = field(default_factory=dict)

    # Schema diff
    bronze_only: list[str] = field(default_factory=list)   # dropped by transform
    silver_only: list[str] = field(default_factory=list)   # added by transform
    shared: list[str] = field(default_factory=list)

    # DQ distribution (silver/gold)
    dq_pre_stats: dict[str, float] = field(default_factory=dict)
    dq_post_stats: dict[str, float] = field(default_factory=dict)
    dq_delta_stats: dict[str, float] = field(default_factory=dict)

    # Enrichment tier mix (gold) — fraction of rows populated per field
    enrichment_fill_rate: dict[str, float] = field(default_factory=dict)

    # Category distribution (gold)
    top_categories: list[tuple[str, int]] = field(default_factory=list)

    # Dedup summary (derived from gold duplicate_group_id)
    dedup_rows: int = 0
    dedup_groups: int = 0
    dedup_ratio: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "date": self.date,
            "bronze_shape": list(self.bronze_shape),
            "silver_shape": list(self.silver_shape),
            "gold_shape": list(self.gold_shape),
            "bronze_nulls": self.bronze_nulls,
            "silver_nulls": self.silver_nulls,
            "gold_nulls": self.gold_nulls,
            "bronze_only": self.bronze_only,
            "silver_only": self.silver_only,
            "shared": self.shared,
            "dq_pre_stats": self.dq_pre_stats,
            "dq_post_stats": self.dq_post_stats,
            "dq_delta_stats": self.dq_delta_stats,
            "enrichment_fill_rate": self.enrichment_fill_rate,
            "top_categories": self.top_categories,
            "dedup_rows": self.dedup_rows,
            "dedup_groups": self.dedup_groups,
            "dedup_ratio": self.dedup_ratio,
        }


def compute_stats(
    bronze: pd.DataFrame,
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    source: str = "",
    date: str = "",
    top_n_categories: int = 20,
) -> EDAStats:
    """Aggregate per-layer statistics into an EDAStats object.

    None of the DataFrames are required — any that are empty contribute empty
    fields to the result. That is deliberate; callers frequently have only
    bronze+silver or only silver+gold.
    """
    stats = EDAStats(source=source, date=date)

    if not bronze.empty:
        stats.bronze_shape = bronze.shape
        stats.bronze_nulls = _null_rates(bronze)

    if not silver.empty:
        stats.silver_shape = silver.shape
        stats.silver_nulls = _null_rates(silver)
        stats.dq_pre_stats = _series_stats(silver.get("dq_score_pre"))
        stats.dq_post_stats = _series_stats(silver.get("dq_score_post"))
        stats.dq_delta_stats = _series_stats(silver.get("dq_delta"))

    if not gold.empty:
        stats.gold_shape = gold.shape
        stats.gold_nulls = _null_rates(gold)
        # DQ stats preferred from gold when available
        if "dq_score_pre" in gold.columns:
            stats.dq_pre_stats = _series_stats(gold["dq_score_pre"])
        if "dq_score_post" in gold.columns:
            stats.dq_post_stats = _series_stats(gold["dq_score_post"])
        if "dq_delta" in gold.columns:
            stats.dq_delta_stats = _series_stats(gold["dq_delta"])

        stats.enrichment_fill_rate = _enrichment_fill(gold)
        stats.top_categories = _top_categories(gold, top_n_categories)
        stats.dedup_rows, stats.dedup_groups, stats.dedup_ratio = _dedup_summary(gold)

    # Schema diff: bronze vs silver is most informative (transform boundary).
    if not bronze.empty and not silver.empty:
        b_cols = set(bronze.columns)
        s_cols = set(silver.columns)
        stats.bronze_only = sorted(b_cols - s_cols)
        stats.silver_only = sorted(s_cols - b_cols)
        stats.shared = sorted(b_cols & s_cols)

    return stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _null_rates(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {}
    rates = df.isna().mean().sort_values(ascending=False)
    return {c: float(v) for c, v in rates.items()}


def _series_stats(s: pd.Series | None) -> dict[str, float]:
    if s is None or s.empty or s.isna().all():
        return {}
    desc = s.describe()
    return {
        "count": float(desc.get("count", 0)),
        "mean": float(desc.get("mean", 0)),
        "std": float(desc.get("std", 0)),
        "min": float(desc.get("min", 0)),
        "p25": float(desc.get("25%", 0)),
        "p50": float(desc.get("50%", 0)),
        "p75": float(desc.get("75%", 0)),
        "max": float(desc.get("max", 0)),
    }


def _enrichment_fill(df: pd.DataFrame) -> dict[str, float]:
    fields = ["primary_category", "allergens", "dietary_tags", "is_organic"]
    return {
        f: float(df[f].notna().mean())
        for f in fields
        if f in df.columns
    }


def _top_categories(df: pd.DataFrame, n: int) -> list[tuple[str, int]]:
    col = "primary_category" if "primary_category" in df.columns else "category"
    if col not in df.columns:
        return []
    counts = df[col].fillna("(null)").astype(str).value_counts().head(n)
    return [(str(k), int(v)) for k, v in counts.items()]


def _dedup_summary(df: pd.DataFrame) -> tuple[int, int, float]:
    if "duplicate_group_id" not in df.columns:
        return 0, 0, 0.0
    total = len(df)
    groups = df["duplicate_group_id"].nunique(dropna=True)
    ratio = (total - groups) / total if total else 0.0
    return int(total), int(groups), float(ratio)


# ---------------------------------------------------------------------------
# GCS plumbing
# ---------------------------------------------------------------------------
def _list_gcs_blobs(bucket: str, prefix: str, suffix: str = "") -> list[str]:
    """List blob names under a bucket/prefix. Returns [] on failure."""
    try:
        from google.cloud import storage

        client = storage.Client()
        return sorted(
            b.name
            for b in client.list_blobs(bucket, prefix=prefix)
            if b.name.endswith(suffix)
        )
    except Exception as exc:
        logger.warning(f"GCS list gs://{bucket}/{prefix} failed: {exc}")
        return []


def _read_jsonl_from_gcs(bucket: str, blob_name: str, nrows: int | None = None) -> pd.DataFrame:
    try:
        from google.cloud import storage

        client = storage.Client()
        blob = client.bucket(bucket).blob(blob_name)
        text = blob.download_as_text()
        records = []
        for i, line in enumerate(text.splitlines()):
            if nrows is not None and i >= nrows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return pd.DataFrame(records)
    except Exception as exc:
        logger.warning(f"GCS jsonl read gs://{bucket}/{blob_name} failed: {exc}")
        return pd.DataFrame()


def _read_parquet_from_gcs(bucket: str, blob_name: str) -> pd.DataFrame:
    try:
        import io

        from google.cloud import storage

        client = storage.Client()
        blob = client.bucket(bucket).blob(blob_name)
        data = blob.download_as_bytes()
        return pd.read_parquet(io.BytesIO(data))
    except Exception as exc:
        logger.warning(f"GCS parquet read gs://{bucket}/{blob_name} failed: {exc}")
        return pd.DataFrame()


def _read_json_from_gcs(bucket: str, blob_name: str) -> dict:
    from google.cloud import storage

    client = storage.Client()
    blob = client.bucket(bucket).blob(blob_name)
    return json.loads(blob.download_as_text())


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)
