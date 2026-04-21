"""
Kafka → GCS Bronze sink.
Replaces Kafka Connect S3 Sink. Consumes from one Kafka topic and
writes JSONL part files to GCS, flushing every FLUSH_SIZE records.

Usage:
  python -m src.consumers.kafka_gcs_sink --topic source.openfda.recalls --prefix openfda
  python -m src.consumers.kafka_gcs_sink --topic source.off.deltas --prefix off
"""

import argparse
import json
import os
import signal
import sys
from datetime import datetime
from io import BytesIO

import boto3
from botocore.config import Config
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GCS_ACCESS_KEY  = os.getenv("GCS_ACCESS_KEY", "REMOVED_GCS_ACCESS_KEY ")
GCS_SECRET_KEY  = os.getenv("GCS_SECRET_KEY", "REMOVED_GCS_SECRET_KEY")
GCS_ENDPOINT    = os.getenv("GCS_ENDPOINT", "https://storage.googleapis.com")
BRONZE_BUCKET   = os.getenv("BRONZE_BUCKET", "mip-bronze-2024")
FLUSH_SIZE      = 10_000


def gcs_client():
    return boto3.client(
        "s3",
        endpoint_url=GCS_ENDPOINT,
        aws_access_key_id=GCS_ACCESS_KEY,
        aws_secret_access_key=GCS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def flush(client, buffer, prefix, ds, chunk_idx):
    key = f"{prefix}/{ds}/part_{chunk_idx:04d}.jsonl"
    body = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=BytesIO(body),
        ContentType="application/x-ndjson",
    )
    print(f"  Flushed {len(buffer):>6} records → gs://{BRONZE_BUCKET}/{key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic",  required=True, help="Kafka topic to consume")
    parser.add_argument("--prefix", required=True, help="GCS prefix (e.g. off, openfda)")
    args = parser.parse_args()

    ds = datetime.utcnow().strftime("%Y/%m/%d")
    gcs = gcs_client()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"gcs-sink-{args.prefix}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=600_000,        # 10 min idle timeout
        max_poll_records=500,
        api_version=(2, 5, 0),
    )

    buffer    = []
    chunk_idx = 0
    total     = 0

    # flush on SIGTERM/SIGINT so we don't lose the last partial buffer
    def _shutdown(sig, frame):
        print(f"\nShutting down — flushing {len(buffer)} remaining records...")
        if buffer:
            flush(gcs, buffer, args.prefix, ds, chunk_idx)
        consumer.close()
        print(f"Done. Total: {total}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"Consuming {args.topic} → gs://{BRONZE_BUCKET}/{args.prefix}/")

    for msg in consumer:
        buffer.append(msg.value)
        total += 1

        if len(buffer) >= FLUSH_SIZE:
            flush(gcs, buffer, args.prefix, ds, chunk_idx)
            chunk_idx += 1
            buffer = []

        if total % 50_000 == 0:
            print(f"  {total:>7} records consumed so far...")

    # consumer_timeout_ms hit — producer is done
    if buffer:
        flush(gcs, buffer, args.prefix, ds, chunk_idx)

    consumer.close()
    print(f"Sink complete. Total records written to GCS: {total}")


if __name__ == "__main__":
    main()
