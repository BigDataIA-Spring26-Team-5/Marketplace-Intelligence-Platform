from __future__ import annotations

import os
import sys

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


def main() -> int:
    load_dotenv()

    region = os.getenv("AWS_DEFAULT_REGION")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL") or None
    bucket = os.getenv("S3_BUCKET")

    missing = [
        name
        for name, value in {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_DEFAULT_REGION": region,
            "S3_BUCKET": bucket,
        }.items()
        if not value
    ]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        return 1

    print("Loaded configuration")
    _print_kv("AWS_DEFAULT_REGION", region)
    _print_kv("S3_BUCKET", bucket)
    _print_kv("AWS_ENDPOINT_URL", endpoint_url)
    _print_kv("AWS_ACCESS_KEY_ID", f"{access_key[:4]}...{access_key[-4:]}" if access_key and len(access_key) >= 8 else access_key)
    print()

    try:
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

        print("Checking AWS credentials with STS")
        identity = sts.get_caller_identity()
        print(f"AWS account: {identity.get('Account')}")
        print(f"AWS ARN: {identity.get('Arn')}")
        print()

        print("Checking whether the bucket is visible in ListBuckets")
        visible_buckets: list[str] = []
        try:
            buckets_response = s3.list_buckets()
            visible_buckets = sorted(bucket_info["Name"] for bucket_info in buckets_response.get("Buckets", []))
            print(f"Visible buckets: {len(visible_buckets)}")
            if bucket in visible_buckets:
                print(f"Target bucket '{bucket}' is visible to these credentials")
            else:
                print(f"Target bucket '{bucket}' is not returned by ListBuckets")
        except ClientError as exc:
            code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
            print("ListBuckets failed")
            print(f"Code: {code}")
            print(f"Message: {message}")
            print(f"HTTP status: {status_code}")
            print(f"Request ID: {request_id}")
            print(f"Host ID: {host_id}")
        print()

        print("Checking bucket region with GetBucketLocation")
        try:
            location = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
            resolved_region = "us-east-1" if location in (None, "") else location
            print(f"Bucket region according to AWS: {resolved_region}")
            if resolved_region != region:
                print(f"Region mismatch: .env has '{region}' but bucket is in '{resolved_region}'")
            else:
                print("Bucket region matches .env")
        except ClientError as exc:
            code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
            print("GetBucketLocation failed")
            print(f"Code: {code}")
            print(f"Message: {message}")
            print(f"HTTP status: {status_code}")
            print(f"Request ID: {request_id}")
            print(f"Host ID: {host_id}")
        print()

        print("Running HeadBucket")
        response = s3.head_bucket(Bucket=bucket)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        print("S3 connection OK")
        print(f"Bucket: {bucket}")
        print(f"Region: {region}")
        print(f"Status code: {status_code}")
        if endpoint_url:
            print(f"Endpoint: {endpoint_url}")
        return 0
    except ClientError as exc:
        print("S3 connection failed")
        code, message, status_code, request_id, host_id = _safe_client_error_details(exc)
        print(f"Code: {code}")
        print(f"Message: {message}")
        print(f"HTTP status: {status_code}")
        print(f"Request ID: {request_id}")
        print(f"Host ID: {host_id}")
        print()
        print("Most likely causes")
        if status_code == 404:
            print("1. S3_BUCKET is misspelled or the bucket does not exist")
            print("2. AWS_DEFAULT_REGION is wrong for that bucket")
            print("3. The bucket exists in another AWS account and these credentials cannot resolve it properly")
        elif status_code == 403:
            print("1. Credentials are valid but do not have access to this bucket")
            print("2. Bucket policy blocks this IAM user/role")
        else:
            print("1. Credentials may be wrong")
            print("2. Region or endpoint may be wrong")
            print("3. Network access to AWS may be blocked")
        return 2
    except Exception as exc:
        print("S3 connection failed")
        print(str(exc))
        return 3


if __name__ == "__main__":
    sys.exit(main())
