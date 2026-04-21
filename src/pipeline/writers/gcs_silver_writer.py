"""
Write pipeline output to the GCS Silver layer as Parquet.

Silver layer path: gs://mip-silver-2024/{source}/{YYYY/MM/DD}/part_{chunk:04d}.parquet
All rows are written (including duplicates). No enrichment columns expected.

GCS access uses Application Default Credentials (ADC) via google-cloud-storage —
same auth path used by GCSSourceLoader for Bronze reads.
Run: gcloud auth application-default login
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "mip-silver-2024")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "mip-bronze-2024")


def _gcs_client():
    from google.cloud import storage
    return storage.Client()


class GCSSilverWriter:
    """Writes DataFrame chunks as Parquet to the GCS Silver bucket via ADC."""

    def write(
        self,
        df: pd.DataFrame,
        source_name: str,
        date: str | None = None,
        chunk_idx: int = 0,
    ) -> str:
        """
        Serialize df to Parquet and upload to Silver GCS.

        Args:
            df: DataFrame to write (all rows, no filtering).
            source_name: logical source name (off, usda, openfda).
            date: partition date string "YYYY/MM/DD". Defaults to today.
            chunk_idx: part file index for this partition.

        Returns:
            Full gs:// URI of the written object.
        """
        if date is None:
            date = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        key = f"{source_name}/{date}/part_{chunk_idx:04d}.parquet"
        uri = f"gs://{SILVER_BUCKET}/{key}"

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        client = _gcs_client()
        blob = client.bucket(SILVER_BUCKET).blob(key)
        blob.upload_from_file(buf, content_type="application/octet-stream")

        logger.info(f"Silver: wrote {len(df)} rows → {uri}")
        return uri

    def read_watermark(self, source_name: str) -> str | None:
        """Return the last Silver partition date for this source, or None."""
        key = f"_watermarks/{source_name}_silver_watermark.json"
        try:
            client = _gcs_client()
            blob = client.bucket(BRONZE_BUCKET).blob(key)
            return json.loads(blob.download_as_bytes()).get("last_partition")
        except Exception:
            return None

    def update_watermark(self, source_name: str, partition: str) -> None:
        """Write the Silver watermark for source_name to the Bronze watermarks prefix."""
        key = f"_watermarks/{source_name}_silver_watermark.json"
        body = json.dumps({
            "last_partition": partition,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).encode()

        client = _gcs_client()
        blob = client.bucket(BRONZE_BUCKET).blob(key)
        blob.upload_from_string(body, content_type="application/json")

        logger.info(f"Silver watermark updated: {source_name} → {partition}")
