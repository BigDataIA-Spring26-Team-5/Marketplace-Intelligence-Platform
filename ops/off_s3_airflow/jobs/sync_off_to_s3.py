from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

DEFAULT_OFF_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
DEFAULT_PREFIX = "open-food-facts"
DEFAULT_TIMEOUT = 120
DEFAULT_CHUNK_SIZE = 1024 * 1024 * 8
DEFAULT_USER_AGENT = "Marketplace-Intelligence-Platform/1.0 (+https://world.openfoodfacts.org)"


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _required_env(name: str) -> str:
    value = _env(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _build_s3_client():
    endpoint_url = _env("AWS_ENDPOINT_URL")
    region_name = _env("AWS_DEFAULT_REGION", "us-east-1")
    access_key = _required_env("AWS_ACCESS_KEY_ID")
    secret_key = _required_env("AWS_SECRET_ACCESS_KEY")
    session_token = _env("AWS_SESSION_TOKEN")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        config=Config(
            retries={"max_attempts": 10, "mode": "standard"},
            signature_version="s3v4",
        ),
    )


def _download_file(url: str, destination: Path, timeout: int) -> dict:
    sha256 = hashlib.sha256()
    total_bytes = 0
    headers = {
        "User-Agent": _env("OFF_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }

    print(f"Starting download from: {url}")
    started_at = time.time()

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        last_modified = response.headers.get("Last-Modified")
        etag = response.headers.get("ETag")
        content_length = response.headers.get("Content-Length")
        expected_bytes = int(content_length) if content_length and content_length.isdigit() else None
        last_log_at = started_at

        if expected_bytes:
            print(f"Expected download size: {expected_bytes / (1024 * 1024):.2f} MB")
        else:
            print("Expected download size: unknown")

        with destination.open("wb") as output:
            for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                if not chunk:
                    continue
                output.write(chunk)
                sha256.update(chunk)
                total_bytes += len(chunk)

                now = time.time()
                if now - last_log_at >= 2:
                    elapsed = max(now - started_at, 1e-6)
                    speed_mb_s = (total_bytes / (1024 * 1024)) / elapsed
                    if expected_bytes:
                        pct = (total_bytes / expected_bytes) * 100
                        print(
                            f"Downloaded {total_bytes / (1024 * 1024):.2f} / "
                            f"{expected_bytes / (1024 * 1024):.2f} MB "
                            f"({pct:.1f}%) at {speed_mb_s:.2f} MB/s"
                        )
                    else:
                        print(
                            f"Downloaded {total_bytes / (1024 * 1024):.2f} MB "
                            f"at {speed_mb_s:.2f} MB/s"
                        )
                    last_log_at = now

    total_elapsed = max(time.time() - started_at, 1e-6)
    print(
        f"Download complete: {total_bytes / (1024 * 1024):.2f} MB "
        f"in {total_elapsed:.1f}s "
        f"({(total_bytes / (1024 * 1024)) / total_elapsed:.2f} MB/s)"
    )

    return {
        "bytes": total_bytes,
        "sha256": sha256.hexdigest(),
        "content_type": content_type,
        "last_modified": last_modified,
        "etag": etag,
    }


def _copy_object(s3_client, bucket: str, source_key: str, target_key: str) -> None:
    s3_client.copy(
        {"Bucket": bucket, "Key": source_key},
        bucket,
        target_key,
    )


class _ProgressPrinter:
    def __init__(self, label: str, total_bytes: int | None = None, log_interval_seconds: int = 2):
        self.label = label
        self.total_bytes = total_bytes
        self.log_interval_seconds = log_interval_seconds
        self.transferred_bytes = 0
        self.started_at = time.time()
        self.last_log_at = self.started_at

    def __call__(self, bytes_amount: int) -> None:
        self.transferred_bytes += bytes_amount
        now = time.time()
        if now - self.last_log_at < self.log_interval_seconds:
            return

        elapsed = max(now - self.started_at, 1e-6)
        speed_mb_s = (self.transferred_bytes / (1024 * 1024)) / elapsed
        if self.total_bytes:
            pct = (self.transferred_bytes / self.total_bytes) * 100
            print(
                f"{self.label}: {self.transferred_bytes / (1024 * 1024):.2f} / "
                f"{self.total_bytes / (1024 * 1024):.2f} MB ({pct:.1f}%) "
                f"at {speed_mb_s:.2f} MB/s"
            )
        else:
            print(
                f"{self.label}: {self.transferred_bytes / (1024 * 1024):.2f} MB "
                f"at {speed_mb_s:.2f} MB/s"
            )
        self.last_log_at = now

    def finish(self) -> None:
        elapsed = max(time.time() - self.started_at, 1e-6)
        print(
            f"{self.label} complete: {self.transferred_bytes / (1024 * 1024):.2f} MB "
            f"in {elapsed:.1f}s ({(self.transferred_bytes / (1024 * 1024)) / elapsed:.2f} MB/s)"
        )


def sync_off_to_s3(
    off_url: str | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    execution_date: str | None = None,
) -> dict:
    off_url = off_url or _env("OFF_SOURCE_URL", DEFAULT_OFF_URL)
    bucket = bucket or _required_env("S3_BUCKET")
    prefix = (prefix or _env("S3_PREFIX", DEFAULT_PREFIX)).strip("/")
    run_date = execution_date or dt.datetime.utcnow().date().isoformat()

    dataset_filename = Path(off_url).name
    dated_key = f"{prefix}/raw/dt={run_date}/{dataset_filename}"
    latest_key = f"{prefix}/latest/{dataset_filename}"
    manifest_key = f"{prefix}/manifests/latest.json"

    s3_client = _build_s3_client()

    with tempfile.TemporaryDirectory(prefix="off-sync-") as tmp_dir:
        tmp_path = Path(tmp_dir) / dataset_filename
        stats = _download_file(
            url=off_url,
            destination=tmp_path,
            timeout=int(_env("OFF_REQUEST_TIMEOUT", str(DEFAULT_TIMEOUT))),
        )

        print(f"Uploading to s3://{bucket}/{dated_key}")
        upload_progress = _ProgressPrinter(
            label="Upload progress",
            total_bytes=stats["bytes"],
        )
        extra_args = {
            "Metadata": {
                "source_url": off_url,
                "sync_date": run_date,
                "sha256": stats["sha256"],
            }
        }
        if stats["content_type"]:
            extra_args["ContentType"] = stats["content_type"]

        s3_client.upload_file(
            str(tmp_path),
            bucket,
            dated_key,
            ExtraArgs=extra_args,
            Callback=upload_progress,
        )
        upload_progress.finish()
        print(f"Upload complete: s3://{bucket}/{dated_key}")
        print(f"Copying latest snapshot to s3://{bucket}/{latest_key}")
        _copy_object(s3_client, bucket, dated_key, latest_key)
        print(f"Latest snapshot updated: s3://{bucket}/{latest_key}")

        manifest = {
            "dataset": "open_food_facts",
            "source_url": off_url,
            "bucket": bucket,
            "dated_key": dated_key,
            "latest_key": latest_key,
            "manifest_key": manifest_key,
            "filename": dataset_filename,
            "sync_date": run_date,
            "file_size_bytes": stats["bytes"],
            "sha256": stats["sha256"],
            "source_last_modified": stats["last_modified"],
            "source_etag": stats["etag"],
            "synced_at_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }

        s3_client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Manifest written: s3://{bucket}/{manifest_key}")

    print(json.dumps(manifest, indent=2))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download the daily Open Food Facts export and upload it to S3-compatible storage."
    )
    parser.add_argument("--off-url", default=None, help="Override OFF source URL.")
    parser.add_argument("--bucket", default=None, help="Target S3 bucket.")
    parser.add_argument("--prefix", default=None, help="Target S3 key prefix.")
    parser.add_argument(
        "--execution-date",
        default=None,
        help="Partition date in YYYY-MM-DD format. Defaults to current UTC date.",
    )

    args = parser.parse_args()
    sync_off_to_s3(
        off_url=args.off_url,
        bucket=args.bucket,
        prefix=args.prefix,
        execution_date=args.execution_date,
    )


if __name__ == "__main__":
    main()
