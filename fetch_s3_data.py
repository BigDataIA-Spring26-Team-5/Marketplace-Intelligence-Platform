from __future__ import annotations

import gzip
import io
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


def _print_kv(label: str, value: str | None) -> None:
    print(f"{label}: {value if value else '<empty>'}")


def _safe_client_error_details(exc: ClientError) -> tuple[str, str, int | None, str | None, str | None]:
    response = exc.response or {}
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    return (
        error.get("Code", "Unknown"),
        error.get("Message", str(exc)),
        metadata.get("HTTPStatusCode"),
        metadata.get("RequestId"),
        metadata.get("HostId"),
    )


def _verify_connection(s3, sts, bucket: str, region: str, endpoint_url: str | None) -> int:
    try:
        print("Checking AWS credentials with STS")
        identity = sts.get_caller_identity()
        print(f"AWS account: {identity.get('Account')}")
        print(f"AWS ARN: {identity.get('Arn')}")
        print()

        print("Checking whether the bucket is visible in ListBuckets")
        try:
            buckets_response = s3.list_buckets()
            visible_buckets = sorted(b["Name"] for b in buckets_response.get("Buckets", []))
            print(f"Visible buckets: {len(visible_buckets)}")
            if bucket in visible_buckets:
                print(f"Target bucket '{bucket}' is visible to these credentials")
            else:
                print(f"Target bucket '{bucket}' is not returned by ListBuckets")
        except ClientError as exc:
            code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
            print("ListBuckets failed")
            print(f"Code: {code}  Message: {message}  HTTP: {status_code}")
        print()

        print("Checking bucket region with GetBucketLocation")
        try:
            location = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
            resolved_region = "us-east-1" if location in (None, "") else location
            print(f"Bucket region: {resolved_region}")
            if resolved_region != region:
                print(f"Region mismatch: .env has '{region}' but bucket is in '{resolved_region}'")
            else:
                print("Bucket region matches .env")
        except ClientError as exc:
            code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
            print(f"GetBucketLocation failed: {code} — {message}")
        print()

        print("Running HeadBucket")
        response = s3.head_bucket(Bucket=bucket)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        print("S3 connection OK")
        print(f"Bucket: {bucket}  Region: {region}  Status: {status_code}")
        if endpoint_url:
            print(f"Endpoint: {endpoint_url}")
        return 0

    except ClientError as exc:
        print("S3 connection failed")
        code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
        print(f"Code: {code}  Message: {message}  HTTP: {status_code}")
        print(f"Request ID: {request_id}  Host ID: {host_id}")
        print()
        print("Most likely causes")
        if status_code == 404:
            print("1. S3_BUCKET is misspelled or does not exist")
            print("2. AWS_DEFAULT_REGION is wrong for that bucket")
        elif status_code == 403:
            print("1. Credentials valid but no access to this bucket")
            print("2. Bucket policy blocks this IAM user/role")
        else:
            print("1. Credentials may be wrong")
            print("2. Region or endpoint may be wrong")
        return 2

    except Exception as exc:
        print(f"S3 connection failed: {exc}")
        return 3


def _download_and_decompress(s3, bucket: str, s3_key: str, download_dir: Path) -> int:
    print()
    print(f"Downloading s3://{bucket}/{s3_key}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
    except ClientError as exc:
        code, message, status_code, _, _ = _safe_client_error_details(exc)
        if code == "NoSuchKey":
            print(f"Key not found: {s3_key}")
            print("Check that S3_KEY is correct and credentials have s3:GetObject permission")
        else:
            print(f"Download failed: {code} — {message}  HTTP: {status_code}")
        return 2

    raw_key = Path(s3_key).name
    output_name = raw_key[:-3] if raw_key.endswith(".gz") else raw_key
    output_path = download_dir / output_name

    download_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.monotonic()
    try:
        compressed = obj["Body"].read()
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as gz:
            data = gz.read()
    except (gzip.BadGzipFile, OSError) as exc:
        print(f"Decompression failed: {exc}")
        print("File may be corrupt or not a valid gzip archive")
        return 3

    try:
        output_path.write_bytes(data)
    except IOError as exc:
        print(f"Write failed at {output_path}: {exc}")
        return 4

    elapsed = time.monotonic() - t0
    size_mb = len(data) / 1_048_576
    print(f"Decompressed → {output_path}  ({size_mb:.2f} MB, {elapsed:.1f}s)")
    return 0


def main() -> int:
    load_dotenv()

    region = os.getenv("AWS_DEFAULT_REGION")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL") or None
    bucket = os.getenv("S3_BUCKET")
    s3_key = os.getenv("S3_KEY")
    download_dir = Path(os.getenv("S3_DOWNLOAD_DIR", "data"))

    missing = [
        name
        for name, value in {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_DEFAULT_REGION": region,
            "S3_BUCKET": bucket,
            "S3_KEY": s3_key,
        }.items()
        if not value
    ]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        return 1

    # Strip s3://bucket/ prefix if user pasted a full URI into S3_KEY
    s3_uri_prefix = f"s3://{bucket}/"
    if s3_key and s3_key.startswith("s3://"):
        s3_key = s3_key[len(s3_uri_prefix):] if s3_key.startswith(s3_uri_prefix) else s3_key.split("/", 3)[-1]

    print("Loaded configuration")
    _print_kv("AWS_DEFAULT_REGION", region)
    _print_kv("S3_BUCKET", bucket)
    _print_kv("S3_KEY", s3_key)
    _print_kv("S3_DOWNLOAD_DIR", str(download_dir))
    _print_kv("AWS_ENDPOINT_URL", endpoint_url)
    masked_key = f"{access_key[:4]}...{access_key[-4:]}" if access_key and len(access_key) >= 8 else access_key
    _print_kv("AWS_ACCESS_KEY_ID", masked_key)
    print()

    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
    )

    sts = boto3.client(
        "sts",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=None,
    )

    rc = _verify_connection(s3, sts, bucket, region, endpoint_url)
    if rc != 0:
        return rc

    return _download_and_decompress(s3, bucket, s3_key, download_dir)


if __name__ == "__main__":
    sys.exit(main())
