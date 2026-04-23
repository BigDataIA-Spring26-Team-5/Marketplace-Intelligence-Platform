"""
Write domain-level Gold output to the GCS Gold layer as Parquet.

Gold layer path: gs://mip-gold-2024/{domain}/{YYYY/MM/DD}/part_{chunk:04d}.parquet

Rows here are cross-source deduplicated canonical records for the domain.
GCS access uses Application Default Credentials (ADC) — same as the Silver writer.
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

_RETRY_DELAYS = (1, 2, 4)

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "mip-gold-2024")


def _gcs_client():
    from google.cloud import storage
    return storage.Client()


def _with_retry(fn):
    last_exc = None
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            logger.warning("GCS gold write failed (attempt %d/3): %s. Retrying in %ds...", attempt, exc, delay)
            time.sleep(delay)
    raise last_exc


class GCSGoldWriter:
    """Writes a domain-level Gold DataFrame as Parquet to the GCS Gold bucket via ADC."""

    def write(
        self,
        df: pd.DataFrame,
        domain: str,
        date: str | None = None,
        chunk_idx: int = 0,
    ) -> str:
        """
        Serialize df to Parquet and upload to Gold GCS.

        Args:
            df: Gold DataFrame (cross-source deduped, canonical records only).
            domain: domain tag (nutrition, safety, pricing, etc.).
            date: partition date string "YYYY/MM/DD". Defaults to today (UTC).
            chunk_idx: part file index within the partition.

        Returns:
            Full gs:// URI of the written object.
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        key = f"{domain}/{date}/part_{chunk_idx:04d}.parquet"
        uri = f"gs://{GOLD_BUCKET}/{key}"

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        client = _gcs_client()
        blob = client.bucket(GOLD_BUCKET).blob(key)
        _with_retry(lambda: blob.upload_from_file(buf, content_type="application/octet-stream"))

        logger.info("Gold GCS: wrote %d rows → %s", len(df), uri)
        return uri
