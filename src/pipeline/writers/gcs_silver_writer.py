"""
Write pipeline output to the GCS Silver layer as Parquet.

Silver layer path: gs://mip-silver-2024/{source}/{YYYY/MM/DD}/part_{chunk:04d}.parquet
All rows are written (including duplicates). No enrichment columns expected.

GCS access uses the S3-compatible HMAC API (same as bronze_to_bq_dag.py) so no
extra credentials are needed beyond what's already configured.
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
GCS_ENDPOINT  = os.environ.get("GCS_ENDPOINT",  "https://storage.googleapis.com")
GCS_ACCESS_KEY = os.environ.get("GCS_ACCESS_KEY", "")
GCS_SECRET_KEY = os.environ.get("GCS_SECRET_KEY", "")


def _gcs_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


class GCSSilverWriter:
    """Writes DataFrame chunks as Parquet to the GCS Silver bucket."""

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
        client.put_object(
            Bucket=SILVER_BUCKET,
            Key=key,
            Body=buf.read(),
            ContentType="application/octet-stream",
        )
        logger.info(f"Silver: wrote {len(df)} rows → {uri}")
        return uri

    def read_watermark(self, source_name: str) -> str | None:
        """Return the last Silver partition date for this source, or None."""
        key = f"_watermarks/{source_name}_silver_watermark.json"
        try:
            client = _gcs_client()
            obj = client.get_object(Bucket=BRONZE_BUCKET, Key=key)
            return json.loads(obj["Body"].read()).get("last_partition")
        except Exception:
            return None

    def update_watermark(self, source_name: str, partition: str) -> None:
        """Write the Silver watermark for source_name to the Bronze watermarks prefix."""
        key = f"_watermarks/{source_name}_silver_watermark.json"
        body = json.dumps({
            "last_partition": partition,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        client = _gcs_client()
        client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=body.encode(),
            ContentType="application/json",
        )
        logger.info(f"Silver watermark updated: {source_name} → {partition}")
