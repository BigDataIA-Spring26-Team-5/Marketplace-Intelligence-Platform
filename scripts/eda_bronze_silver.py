"""
EDA: Compare Bronze vs Silver layer for a given source/date.

Usage:
    poetry run python scripts/eda_bronze_silver.py --source off --date 2026/04/21
    poetry run python scripts/eda_bronze_silver.py --source off --date 2026/04/21 --part 0
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from google.cloud import storage

BRONZE_BUCKET = "mip-bronze-2024"
SILVER_BUCKET = "mip-silver-2024"


def _gcs():
    return storage.Client()


# ── loaders ──────────────────────────────────────────────────────────────────

def load_bronze(source: str, date: str, part: int) -> pd.DataFrame:
    key = f"{source}/{date}/part_{part:04d}.jsonl"
    client = _gcs()
    blob = client.bucket(BRONZE_BUCKET).blob(key)
    lines = blob.download_as_text().strip().splitlines()
    records = [json.loads(l) for l in lines]
    df = pd.json_normalize(records)
    print(f"Bronze loaded: gs://{BRONZE_BUCKET}/{key}  ({len(df)} rows, {len(df.columns)} cols)")
    return df


def load_silver(source: str, date: str, part: int) -> pd.DataFrame:
    key = f"{source}/{date}/part_{part:04d}.parquet"
    client = _gcs()
    blob = client.bucket(SILVER_BUCKET).blob(key)
    buf = io.BytesIO(blob.download_as_bytes())
    df = pd.read_parquet(buf, engine="pyarrow")
    print(f"Silver loaded: gs://{SILVER_BUCKET}/{key}  ({len(df)} rows, {len(df.columns)} cols)")
    return df


# ── EDA sections ─────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def eda_shape(bronze: pd.DataFrame, silver: pd.DataFrame):
    section("SHAPE")
    print(f"{'':20s} {'Bronze':>10s} {'Silver':>10s}")
    print(f"{'Rows':20s} {len(bronze):>10,} {len(silver):>10,}")
    print(f"{'Columns':20s} {len(bronze.columns):>10,} {len(silver.columns):>10,}")
    print(f"{'Quarantined':20s} {'':>10s} {len(bronze) - len(silver):>10,}")


def eda_columns(bronze: pd.DataFrame, silver: pd.DataFrame):
    section("COLUMNS DIFF")
    b_cols = set(bronze.columns)
    s_cols = set(silver.columns)
    dropped = sorted(b_cols - s_cols)
    added   = sorted(s_cols - b_cols)
    kept    = sorted(b_cols & s_cols)
    print(f"Kept ({len(kept)}):    {kept}")
    print(f"Dropped ({len(dropped)}): {dropped}")
    print(f"Added ({len(added)}):   {added}")


def eda_nulls(bronze: pd.DataFrame, silver: pd.DataFrame):
    section("NULL RATES — Silver unified columns")
    key_cols = [
        "product_name", "brand_name", "ingredients",
        "serving_size", "serving_size_unit", "published_date",
        "dq_score_pre",
    ]
    rows = []
    for col in key_cols:
        b_null = bronze[col].isna().mean() * 100 if col in bronze.columns else float("nan")
        s_null = silver[col].isna().mean() * 100 if col in silver.columns else float("nan")
        rows.append({"column": col, "bronze_null%": b_null, "silver_null%": s_null})
    print(pd.DataFrame(rows).to_string(index=False, float_format=lambda x: f"{x:.1f}"))


def eda_dtypes(silver: pd.DataFrame):
    section("SILVER DTYPES")
    print(silver.dtypes.to_string())


def eda_dq_score(silver: pd.DataFrame):
    section("DQ SCORE PRE (Silver)")
    if "dq_score_pre" not in silver.columns:
        print("dq_score_pre not present")
        return
    s = silver["dq_score_pre"]
    print(f"mean={s.mean():.1f}%  min={s.min():.1f}%  max={s.max():.1f}%  p25={s.quantile(.25):.1f}%  p75={s.quantile(.75):.1f}%")
    bins = pd.cut(s, bins=[0,20,40,60,80,100], labels=["0-20","20-40","40-60","60-80","80-100"])
    print(bins.value_counts().sort_index().to_string())


def eda_samples(silver: pd.DataFrame, n: int = 5):
    section(f"SILVER SAMPLE ROWS (n={n})")
    cols = [c for c in ["product_name","brand_name","ingredients","serving_size","dq_score_pre"] if c in silver.columns]
    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 200)
    print(silver[cols].dropna(subset=["product_name"]).head(n).to_string(index=False))


def eda_top_brands(silver: pd.DataFrame, n: int = 10):
    section(f"TOP {n} BRANDS (Silver)")
    if "brand_name" not in silver.columns:
        return
    print(silver["brand_name"].value_counts().head(n).to_string())


def eda_bronze_raw_keys(bronze: pd.DataFrame):
    section("BRONZE RAW COLUMN SAMPLE (first 20)")
    print(list(bronze.columns[:20]))


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EDA: Bronze vs Silver layer comparison")
    parser.add_argument("--source", default="off",        help="Source name (off/usda/openfda)")
    parser.add_argument("--date",   default="2026/04/21", help="Partition date YYYY/MM/DD")
    parser.add_argument("--part",   type=int, default=0,  help="Part file index (default 0)")
    args = parser.parse_args()

    print(f"\nEDA — source={args.source}  date={args.date}  part={args.part}")

    bronze = load_bronze(args.source, args.date, args.part)
    silver = load_silver(args.source, args.date, args.part)

    eda_shape(bronze, silver)
    eda_columns(bronze, silver)
    eda_bronze_raw_keys(bronze)
    eda_nulls(bronze, silver)
    eda_dtypes(silver)
    eda_dq_score(silver)
    eda_top_brands(silver)
    eda_samples(silver)

    print("\nDone.")


if __name__ == "__main__":
    main()
