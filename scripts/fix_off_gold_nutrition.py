"""
Fix OFF data quality issues in nutrition gold parquet (in-place GCS overwrite).

Issues addressed:
  1. product_name: multilingual template "lang main text <val> lang xx text <val>" → extract main text
  2. product_name: broken unicode escapes "u00e9" (missing backslash) → proper chars
  3. ingredients: JSON array [{"lang": "main", "text": "..."}] → extract main text

Usage:
  python scripts/fix_off_gold_nutrition.py
  python scripts/fix_off_gold_nutrition.py --date 2026/04/24
  python scripts/fix_off_gold_nutrition.py --date 2026/04/24 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import html
import re
import sys
from io import BytesIO
from urllib.parse import unquote
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GOLD_BUCKET   = os.environ.get("GOLD_BUCKET",   "mip-gold-2024")
GCS_ENDPOINT  = os.environ.get("GCS_ENDPOINT",  "https://storage.googleapis.com")
GCS_ACCESS_KEY = os.environ.get("GCS_ACCESS_KEY", "")
GCS_SECRET_KEY = os.environ.get("GCS_SECRET_KEY", "")

# Matches: "lang main text <value>" optionally followed by "lang xx text ..."
_LANG_MAIN_RE = re.compile(
    r"lang\s+main\s+text\s+(.*?)(?:\s+lang\s+\w+\s+text\s+.*)?$",
    re.IGNORECASE | re.DOTALL,
)
# Matches broken unicode escapes: u00e9 / U00F6 / u0101 (case-insensitive, no leading backslash)
_BROKEN_UNICODE_RE = re.compile(r"(?<![\\a-zA-Z])[uU]([0-9a-fA-F]{4})(?![0-9a-fA-F])")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _list_parquet_keys(client, date: str) -> list[str]:
    prefix = f"nutrition/{date}/"
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    if not keys:
        raise FileNotFoundError(f"No parquet files at gs://{GOLD_BUCKET}/{prefix}")
    return sorted(keys)


def _read_parquet(client, key: str) -> pd.DataFrame:
    resp = client.get_object(Bucket=GOLD_BUCKET, Key=key)
    buf = BytesIO(resp["Body"].read())
    return pd.read_parquet(buf)


def _sanitize_surrogates(df: pd.DataFrame) -> pd.DataFrame:
    """Strip lone surrogate characters that pyarrow cannot encode as UTF-8."""
    def clean(val):
        if not isinstance(val, str):
            return val
        return val.encode("utf-8", errors="surrogatepass").decode("utf-8", errors="replace")

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(clean)
    return df


def _write_parquet(df: pd.DataFrame, key: str) -> None:
    from google.cloud import storage as gcs
    df = _sanitize_surrogates(df)
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    client = gcs.Client()
    blob = client.bucket(GOLD_BUCKET).blob(key)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Written %d rows → gs://%s/%s", len(df), GOLD_BUCKET, key)


def _fix_url_encoding(text: str) -> str:
    """Decode URL-encoded sequences like %C3%A9 → é."""
    if "%" not in text:
        return text
    try:
        decoded = unquote(text, encoding="utf-8", errors="strict")
        return decoded if decoded != text else text
    except Exception:
        return text


def _fix_html_entities(text: str) -> str:
    """Unescape HTML entities: &quot; → ", &amp; → &, &eacute; → é, etc."""
    if "&" not in text:
        return text
    return html.unescape(text)


def _fix_unicode_escapes(text: str) -> str:
    """Replace broken uXXXX (4-digit, no backslash) with proper unicode character."""
    def replace(m: re.Match) -> str:
        try:
            return chr(int(m.group(1), 16))
        except ValueError:
            return m.group(0)
    return _BROKEN_UNICODE_RE.sub(replace, text)


def _extract_lang_main(text: str) -> str:
    """Extract the 'main' language text from OFF multilingual template."""
    m = _LANG_MAIN_RE.match(text.strip())
    if m:
        return m.group(1).strip()
    return text


# Lone © mid-word: orphaned second byte of Ã© pair (é = U+00E9, UTF-8: C3 A9)
_LONE_COPYRIGHT_RE = re.compile(r"(?<=[A-Za-zÀ-ɏ])©(?=[A-Za-zÀ-ɏ])")


def _fix_lone_symbols(text: str) -> str:
    """Replace lone © flanked by letters with é (orphaned from Ã© split)."""
    if "©" not in text:
        return text
    return _LONE_COPYRIGHT_RE.sub("é", text)


def _fix_mojibake(text: str) -> str:
    """Fix UTF-8 bytes wrongly decoded as CP1252/Latin-1.

    Covers:
      - Latin mojibake:   Ã© → é, Ã‰ → É  (marker: Ã or Â)
      - Cyrillic mojibake: Ð§ÐµÑ€ → Чер   (marker: Ð)
    """
    if "Ã" not in text and "Â" not in text and "Ð" not in text:
        return text  # No mojibake markers, skip cheaply
    # Try CP1252 first — covers Ã‰ (‰=0x89) and Cyrillic Ð (0xD0)
    for enc in ("cp1252", "latin-1"):
        try:
            fixed = text.encode(enc).decode("utf-8")
            return fixed
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    # Fallback: ftfy for mixed / partial mojibake
    try:
        import ftfy
        return ftfy.fix_text(text)
    except Exception:
        return text


def _to_title_case(text: str) -> str:
    """Title-case each word, preserving digits/punctuation boundaries."""
    return text.title()


def _clean_product_name(val) -> object:
    if not isinstance(val, str) or not val:
        return val
    # Step 1: extract main-language portion if template present
    if re.search(r"lang\s+\w+\s+text\s+", val, re.IGNORECASE):
        val = _extract_lang_main(val)
    # Step 2: fix broken unicode escapes (u00e9, u0101, etc.)
    val = _fix_unicode_escapes(val)
    # Step 3: normalize to Title Case
    val = _to_title_case(val)
    return val


def _clean_ingredients(val) -> object:
    if not isinstance(val, str) or not val:
        return val
    stripped = val.strip()
    if not stripped.startswith("["):
        return val
    try:
        items = json.loads(stripped)
        if not isinstance(items, list):
            return val
        # Prefer lang == "main", fall back to first entry
        main_text = None
        first_text = None
        for item in items:
            if not isinstance(item, dict):
                continue
            text = item.get("text", "")
            if first_text is None and text:
                first_text = text
            if item.get("lang") == "main" and text:
                main_text = text
                break
        result = main_text or first_text
        if not result:
            # Empty array or no text found — treat as null
            return None
        # Proper JSON unicode (é) decoded automatically by json.loads;
        # apply broken-escape fix defensively in case some values aren't JSON
        result = _fix_unicode_escapes(result)
        return result
    except (json.JSONDecodeError, TypeError):
        return val


def _apply_to_str_col(df: pd.DataFrame, col: str, fn) -> int:
    """Apply fn to a string column in-place, return count of changed rows."""
    if col not in df.columns:
        return 0
    # Use object dtype for reliable comparison — StringDtype vs object gives false positives
    _SENTINEL = "\x00__NULL__\x00"
    before = df[col].astype(object).fillna(_SENTINEL).copy()
    df[col] = df[col].apply(lambda v: fn(v) if isinstance(v, str) and v else v)
    after = df[col].astype(object).fillna(_SENTINEL)
    return int((before != after).sum())


def _fix_off_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    off_mask = df["data_source"].str.lower() == "off"
    off_count = off_mask.sum()
    total_rows = len(df)
    stats: dict[str, int] = {"off_rows": int(off_count), "total_rows": total_rows}

    df = df.copy()

    # --- Step 1: OFF-specific lang template strip (product_name only) ---
    if off_count > 0:
        pn_before_off = df.loc[off_mask, "product_name"].copy()
        df.loc[off_mask, "product_name"] = df.loc[off_mask, "product_name"].apply(
            lambda v: _extract_lang_main(v)
            if isinstance(v, str) and re.search(r"lang\s+\w+\s+text\s+", v, re.IGNORECASE)
            else v
        )
        stats["product_name_lang_fixed"] = int(
            (df.loc[off_mask, "product_name"] != pn_before_off).sum()
        )
        logger.info("product_name (lang strip): %d OFF rows", stats["product_name_lang_fixed"])

    # --- Step 2: ingredients OFF-only JSON parse (must run before encoding fixes) ---
    if off_count > 0:
        ing_before = df.loc[off_mask, "ingredients"].copy()
        df.loc[off_mask, "ingredients"] = df.loc[off_mask, "ingredients"].apply(
            _clean_ingredients
        )
        stats["ingredients_json_fixed"] = int((df.loc[off_mask, "ingredients"] != ing_before).sum())
        logger.info("ingredients (JSON): fixed %d / %d OFF rows", stats["ingredients_json_fixed"], off_count)

    # --- Steps 3-8: url → html → unicode → mojibake → lone_sym → title case ---
    # Applied to product_name, brand_name, ingredients
    for col in ["product_name", "brand_name", "ingredients"]:
        if col not in df.columns:
            continue
        u  = _apply_to_str_col(df, col, _fix_url_encoding)
        h  = _apply_to_str_col(df, col, _fix_html_entities)
        x  = _apply_to_str_col(df, col, _fix_unicode_escapes)
        m  = _apply_to_str_col(df, col, _fix_mojibake)
        s  = _apply_to_str_col(df, col, _fix_lone_symbols)
        t  = _apply_to_str_col(df, col, _to_title_case)
        stats[f"{col}_url_fixed"]      = u
        stats[f"{col}_html_fixed"]     = h
        stats[f"{col}_unicode_fixed"]  = x
        stats[f"{col}_mojibake_fixed"] = m
        stats[f"{col}_lone_sym_fixed"] = s
        stats[f"{col}_title_cased"]    = t
        logger.info("%s: url=%d html=%d unicode=%d mojibake=%d lone_sym=%d title=%d",
                    col, u, h, x, m, s, t)

    return df, stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026/04/24", help="Gold date partition (YYYY/MM/DD)")
    parser.add_argument("--dry-run", action="store_true", help="Read and clean but do NOT write back")
    args = parser.parse_args()

    client = _s3()
    keys = _list_parquet_keys(client, args.date)
    logger.info("Found %d parquet file(s) for nutrition/%s", len(keys), args.date)

    total_stats: dict[str, int] = {}

    for key in keys:
        logger.info("Processing gs://%s/%s ...", GOLD_BUCKET, key)
        df = _read_parquet(client, key)
        logger.info("  Loaded %d rows, %d cols", *df.shape)

        df_clean, stats = _fix_off_rows(df)

        for k, v in stats.items():
            total_stats[k] = total_stats.get(k, 0) + v

        if args.dry_run:
            logger.info("  [DRY RUN] would overwrite gs://%s/%s", GOLD_BUCKET, key)
            # Show samples for each target column — find rows that actually changed
            for col in ["product_name", "brand_name", "ingredients"]:
                if col not in df_clean.columns:
                    continue
                changed_mask = df_clean[col] != df[col]
                sample_rows = df_clean.loc[changed_mask, col].dropna().head(3)
                for val in sample_rows:
                    logger.info("  [%s] %s", col, str(val)[:120])
        else:
            _write_parquet(df_clean, key)

    logger.info("Done. Summary: %s", total_stats)


if __name__ == "__main__":
    main()
