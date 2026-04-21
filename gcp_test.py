"""
GCP Bronze Layer Data Explorer
================================
Deep inspection of all bronze sources to understand data shape,
quality, and readiness for Silver layer transformation.

Prerequisites:
    pip install google-cloud-storage google-cloud-bigquery pandas db-dtypes

Auth:
    gcloud auth application-default login
"""

import json
import sys
from collections import Counter
from google.cloud import storage, bigquery
import pandas as pd

PROJECT_ID = "mip-platform-2024"
BUCKET_NAME = "mip-bronze-2024"
BQ_DATASET = "bronze_metadata"
BQ_TABLE = "usda_records"

# Reusable clients (avoid re-creating per call)
_gcs_client = None
_bq_client = None


def gcs_client():
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client(project=PROJECT_ID)
    return _gcs_client


def bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def section(title: str):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def subsection(title: str):
    print(f"\n--- {title} ---")


# =====================================================================
# 1. GCS BUCKET OVERVIEW
# =====================================================================

def bucket_overview():
    """Show all sources, file counts, and total sizes in the bronze bucket."""
    section("1. BRONZE BUCKET OVERVIEW")

    bucket = gcs_client().bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    # Group by top-level prefix (source)
    sources = {}
    for blob in blobs:
        source = blob.name.split("/")[0]
        if source not in sources:
            sources[source] = {"count": 0, "total_bytes": 0, "files": []}
        sources[source]["count"] += 1
        sources[source]["total_bytes"] += blob.size or 0
        sources[source]["files"].append(blob.name)

    print(f"\n{'Source':<15} {'Files':>8} {'Total Size':>12} {'Status':<15}")
    print(f"{'-'*15} {'-'*8} {'-'*12} {'-'*15}")
    for source, info in sorted(sources.items()):
        size_mb = info["total_bytes"] / (1024 * 1024)
        status = "READY" if info["count"] > 0 else "EMPTY"
        print(f"{source:<15} {info['count']:>8} {size_mb:>10.1f} MB {status:<15}")

    # Check for expected but missing sources
    expected = {"usda", "off", "openfda", "esci"}
    missing = expected - set(sources.keys())
    if missing:
        print(f"\n  MISSING SOURCES: {', '.join(missing)}")

    return sources


# =====================================================================
# 2. JSONL RAW FILE INSPECTION
# =====================================================================

def inspect_jsonl(blob_path: str, n_records: int = 5, show_nested: bool = True):
    """Download a JSONL file and deeply inspect its structure."""
    section(f"2. JSONL INSPECTION: {blob_path}")

    bucket = gcs_client().bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    lines = content.strip().split("\n")
    total_lines = len(lines)

    print(f"\n  File: gs://{BUCKET_NAME}/{blob_path}")
    print(f"  Total records: {total_lines}")
    print(f"  File size: {blob.size / (1024*1024):.2f} MB")
    print(f"  Avg record size: {blob.size / max(total_lines,1):.0f} bytes")

    # Parse sample records
    sample = [json.loads(line) for line in lines[:n_records]]

    # Show all keys from first record
    subsection("Schema (keys from first record)")
    first = sample[0]
    for key, value in first.items():
        vtype = type(value).__name__
        val_str = str(value)
        is_nested = isinstance(value, (dict, list))
        if len(val_str) > 80:
            val_str = val_str[:80] + "..."
        nested_flag = " [NESTED]" if is_nested else ""
        print(f"  {key:<35} {vtype:<10}{nested_flag}  {val_str}")

    # Key consistency across sample
    subsection(f"Key consistency across {n_records} records")
    all_keys = [set(r.keys()) for r in sample]
    common = set.intersection(*all_keys)
    varying = set.union(*all_keys) - common
    print(f"  Keys in ALL records:  {len(common)} — {sorted(common)}")
    if varying:
        print(f"  Keys in SOME records: {len(varying)} — {sorted(varying)}")

    # Nested field deep dive
    if show_nested:
        for key, value in first.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                subsection(f"Nested array: '{key}' ({len(value)} items in first record)")
                inner_keys = list(value[0].keys())
                print(f"  Inner keys: {inner_keys}")
                for i, item in enumerate(value[:3]):
                    print(f"  [{i}] {json.dumps(item, default=str)[:120]}")
                # Check how many records have this field populated
                has_field = sum(1 for r in sample if key in r and r[key])
                print(f"  Populated in {has_field}/{len(sample)} sample records")

            elif isinstance(value, dict):
                subsection(f"Nested object: '{key}'")
                for k, v in value.items():
                    v_str = str(v)[:80]
                    print(f"  .{k:<30} {v_str}")

    # Sample records (compact)
    subsection(f"Sample records (first {min(3, n_records)})")
    for i, record in enumerate(sample[:3]):
        print(f"\n  Record {i+1}:")
        for k, v in record.items():
            v_str = str(v)
            if len(v_str) > 100:
                v_str = v_str[:100] + f"... ({len(str(v))} chars)"
            print(f"    {k:<35} {v_str}")

    return sample


# =====================================================================
# 3. BIGQUERY DATA PROFILING
# =====================================================================

def bq_query(query: str) -> pd.DataFrame:
    """Run a BQ query and return a DataFrame."""
    return bq_client().query(query).to_dataframe()


def profile_bigquery_table():
    """Full data profile of the USDA BigQuery table."""
    section("3. BIGQUERY DATA PROFILE — usda_records")

    table_ref = f"{BQ_DATASET}.{BQ_TABLE}"

    # 3a. Row count
    subsection("Row count")
    df = bq_query(f"SELECT COUNT(*) as total FROM {table_ref}")
    total_rows = df["total"].iloc[0]
    print(f"  Total rows: {total_rows:,}")

    # 3b. Schema
    subsection("Schema")
    schema_df = bq_query(f"""
        SELECT column_name, data_type, is_nullable
        FROM {BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{BQ_TABLE}'
        ORDER BY ordinal_position
    """)
    for _, row in schema_df.iterrows():
        print(f"  {row['column_name']:<25} {row['data_type']:<12} nullable={row['is_nullable']}")

    # 3c. Null rates per column
    subsection("Null / empty rates per column")
    columns = schema_df["column_name"].tolist()
    null_cases = []
    for col in columns:
        null_cases.append(
            f"ROUND(COUNTIF({col} IS NULL OR CAST({col} AS STRING) = '') * 100.0 / COUNT(*), 1) AS {col}"
        )
    null_query = f"SELECT {', '.join(null_cases)} FROM {table_ref}"
    null_df = bq_query(null_query)
    print(f"\n  {'Column':<25} {'Null/Empty %':>12} {'Status':<10}")
    print(f"  {'-'*25} {'-'*12} {'-'*10}")
    for col in columns:
        pct = null_df[col].iloc[0]
        status = "OK" if pct < 5 else "WARN" if pct < 50 else "CRITICAL"
        bar = "#" * int(pct / 2)
        print(f"  {col:<25} {pct:>10.1f}%  {status:<10} {bar}")

    # 3d. data_type distribution (Branded vs SR Legacy vs Survey)
    subsection("Record types (data_type distribution)")
    df = bq_query(f"""
        SELECT data_type, COUNT(*) as cnt,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM {table_ref}
        GROUP BY data_type
        ORDER BY cnt DESC
    """)
    for _, row in df.iterrows():
        print(f"  {row['data_type']:<25} {row['cnt']:>8,} ({row['pct']}%)")

    # 3e. food_category distribution (top 20)
    subsection("Top 20 food categories")
    df = bq_query(f"""
        SELECT food_category, COUNT(*) as cnt
        FROM {table_ref}
        WHERE food_category IS NOT NULL
        GROUP BY food_category
        ORDER BY cnt DESC
        LIMIT 20
    """)
    for _, row in df.iterrows():
        print(f"  {row['food_category']:<45} {row['cnt']:>6,}")

    # 3f. Brand coverage
    subsection("Brand field coverage")
    df = bq_query(f"""
        SELECT
          COUNTIF(brand_owner IS NOT NULL AND brand_owner != '') as has_brand_owner,
          COUNTIF(brand_name IS NOT NULL AND brand_name != '') as has_brand_name,
          COUNTIF(gtin_upc IS NOT NULL AND gtin_upc != '') as has_gtin,
          COUNTIF(ingredients IS NOT NULL AND ingredients != '') as has_ingredients,
          COUNT(*) as total
        FROM {table_ref}
    """)
    r = df.iloc[0]
    for field in ["has_brand_owner", "has_brand_name", "has_gtin", "has_ingredients"]:
        pct = r[field] * 100.0 / r["total"]
        print(f"  {field:<25} {r[field]:>8,} / {r['total']:>8,} ({pct:.1f}%)")

    # 3g. Serving size analysis
    subsection("Serving size analysis")
    df = bq_query(f"""
        SELECT
          COUNTIF(serving_size IS NOT NULL) as has_serving_size,
          ROUND(AVG(CASE WHEN serving_size IS NOT NULL THEN serving_size END), 2) as avg_serving_size,
          ROUND(MIN(CASE WHEN serving_size IS NOT NULL THEN serving_size END), 2) as min_serving_size,
          ROUND(MAX(CASE WHEN serving_size IS NOT NULL THEN serving_size END), 2) as max_serving_size,
          COUNT(*) as total
        FROM {table_ref}
    """)
    r = df.iloc[0]
    print(f"  Records with serving_size: {r['has_serving_size']:,} / {r['total']:,}")
    print(f"  Min: {r['min_serving_size']}  Avg: {r['avg_serving_size']}  Max: {r['max_serving_size']}")

    # Serving size unit distribution
    df = bq_query(f"""
        SELECT serving_size_unit, COUNT(*) as cnt
        FROM {table_ref}
        WHERE serving_size_unit IS NOT NULL AND serving_size_unit != ''
        GROUP BY serving_size_unit
        ORDER BY cnt DESC
        LIMIT 10
    """)
    print(f"\n  Serving size units:")
    for _, row in df.iterrows():
        print(f"    {row['serving_size_unit']:<15} {row['cnt']:>8,}")

    # 3h. Duplicate detection preview
    subsection("Potential duplicates (same description, different fdc_id)")
    df = bq_query(f"""
        SELECT description, COUNT(*) as cnt, COUNT(DISTINCT fdc_id) as distinct_ids
        FROM {table_ref}
        GROUP BY description
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 15
    """)
    print(f"\n  {'Description':<55} {'Rows':>6} {'Distinct IDs':>12}")
    print(f"  {'-'*55} {'-'*6} {'-'*12}")
    for _, row in df.iterrows():
        desc = row["description"][:55]
        print(f"  {desc:<55} {row['cnt']:>6} {row['distinct_ids']:>12}")

    total_dupes = bq_query(f"""
        SELECT SUM(cnt) as total_dupe_rows, COUNT(*) as dupe_groups FROM (
            SELECT description, COUNT(*) as cnt
            FROM {table_ref}
            GROUP BY description
            HAVING COUNT(*) > 1
        )
    """)
    r = total_dupes.iloc[0]
    print(f"\n  Total duplicate groups: {r['dupe_groups']:,}")
    print(f"  Total rows in duplicate groups: {r['total_dupe_rows']:,}")

    # 3i. Date range
    subsection("Published date range")
    df = bq_query(f"""
        SELECT
          MIN(published_date) as earliest,
          MAX(published_date) as latest,
          COUNT(DISTINCT published_date) as distinct_dates
        FROM {table_ref}
        WHERE published_date IS NOT NULL
    """)
    r = df.iloc[0]
    print(f"  Earliest: {r['earliest']}  Latest: {r['latest']}  Distinct dates: {r['distinct_dates']}")

    # 3j. Ingredients text length distribution
    subsection("Ingredients text length (where populated)")
    df = bq_query(f"""
        SELECT
          ROUND(AVG(LENGTH(ingredients)), 0) as avg_len,
          MIN(LENGTH(ingredients)) as min_len,
          MAX(LENGTH(ingredients)) as max_len,
          APPROX_QUANTILES(LENGTH(ingredients), 4)[OFFSET(2)] as median_len
        FROM {table_ref}
        WHERE ingredients IS NOT NULL AND ingredients != ''
    """)
    r = df.iloc[0]
    print(f"  Min: {r['min_len']}  Median: {r['median_len']}  Avg: {r['avg_len']}  Max: {r['max_len']} chars")


# =====================================================================
# 4. CROSS-SOURCE RAW SCHEMA COMPARISON
# =====================================================================

def compare_jsonl_schemas():
    """Pull first record from each source's JSONL and compare schemas."""
    section("4. CROSS-SOURCE SCHEMA COMPARISON (raw JSONL)")

    sources = {
        "usda": "usda/2026/04/20/part_0000.jsonl",
        "openfda": "openfda/2026/04/20/part_0000.jsonl",
        "esci": "esci/2024/01/01/part_0000.jsonl",
    }

    schemas = {}
    bucket = gcs_client().bucket(BUCKET_NAME)

    for source, path in sources.items():
        try:
            blob = bucket.blob(path)
            first_line = blob.download_as_text().split("\n")[0]
            record = json.loads(first_line)
            schemas[source] = record
            subsection(f"{source.upper()} — {len(record)} top-level keys")
            for key, value in record.items():
                vtype = type(value).__name__
                nested = " [NESTED]" if isinstance(value, (dict, list)) else ""
                val_preview = str(value)[:60]
                print(f"  {key:<35} {vtype:<8}{nested}  {val_preview}")
        except Exception as e:
            print(f"\n  {source}: FAILED — {e}")

    # Find overlapping field names
    if len(schemas) > 1:
        subsection("Field name overlap across sources")
        all_keys = {src: set(rec.keys()) for src, rec in schemas.items()}
        sources_list = list(all_keys.keys())
        for i, s1 in enumerate(sources_list):
            for s2 in sources_list[i+1:]:
                overlap = all_keys[s1] & all_keys[s2]
                print(f"  {s1} ∩ {s2}: {overlap if overlap else 'NONE'}")


# =====================================================================
# 5. SILVER LAYER READINESS SUMMARY
# =====================================================================

def readiness_summary():
    """Summarize what's ready for Silver layer transformation."""
    section("5. SILVER LAYER READINESS SUMMARY")

    print("""
  Source     Bronze Status    Schema Complexity    Silver Priority    Notes
  ------     -------------    -----------------    ---------------    -----
  USDA       READY (84k BQ)   Medium (14 cols)     HIGH               Two record types (Branded vs SR Legacy)
                               + nested nutrients                      Need to handle null brand fields
                               in raw JSONL                            serving_size units need normalization

  OFF        NOT LANDED        High (200+ fields)   HIGH               4.48M records pending Kafka Connect
                                                                       Richest product data when it arrives

  openFDA    LANDED (5 files)  Unknown              LOW                Recall data — different semantic purpose
                                                                       May attach to product entities later

  ESCI       LANDED (10 files) Medium               SEPARATE           Search relevance data, not product catalog
                                                                       Used for search evaluation, not Silver

  NEXT STEPS:
  1. Profile USDA raw JSONL (especially nested foodNutrients)
  2. Build Silver pipeline for USDA Branded + SR Legacy
  3. Wait for OFF to land, then profile and build its Silver pipeline
  4. Run global dedup on unified Silver tables
    """)


# =====================================================================
# MAIN
# =====================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("  GCP BRONZE LAYER — FULL DATA EXPLORATION")
    print("  Project: mip-platform-2024")
    print("  Bucket:  gs://mip-bronze-2024")
    print("="*70)

    # Run all sections
    try:
        bucket_overview()
    except Exception as e:
        print(f"  ERROR in bucket overview: {e}")

    # Inspect raw JSONL from USDA (includes nested foodNutrients)
    try:
        inspect_jsonl("usda/2026/04/20/part_0000.jsonl", n_records=5)
    except Exception as e:
        print(f"  ERROR inspecting USDA JSONL: {e}")

    # Inspect raw JSONL from openFDA
    try:
        inspect_jsonl("openfda/2026/04/20/part_0000.jsonl", n_records=3)
    except Exception as e:
        print(f"  ERROR inspecting openFDA JSONL: {e}")

    # Inspect raw JSONL from ESCI
    try:
        inspect_jsonl("esci/2024/01/01/part_0000.jsonl", n_records=3)
    except Exception as e:
        print(f"  ERROR inspecting ESCI JSONL: {e}")

    # Full BigQuery profiling
    try:
        profile_bigquery_table()
    except Exception as e:
        print(f"  ERROR in BigQuery profiling: {e}")

    # Cross-source schema comparison
    try:
        compare_jsonl_schemas()
    except Exception as e:
        print(f"  ERROR in schema comparison: {e}")

    # Readiness summary
    readiness_summary()

    print("\n\nDone. Copy any section output to start planning Silver layer transforms.\n")