#!/bin/bash
# USDA FoodData Central bulk download → GCS bronze
# Downloads all 4 official zip files directly on the GCP VM,
# converts JSON to JSONL, and uploads to gs://mip-bronze-2024/usda/bulk/
# Run: bash scripts/usda_bulk_download.sh

set -e

BUCKET="mip-bronze-2024"
GCS_PREFIX="usda/bulk"
TMP_DIR="/tmp/usda_bulk"
PARTITION=$(date +%Y/%m/%d)

GCS_ACCESS_KEY="${GCS_ACCESS_KEY:-GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535}"
GCS_SECRET_KEY="${GCS_SECRET_KEY:-/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx}"

mkdir -p "$TMP_DIR"
cd "$TMP_DIR"

declare -A DOWNLOADS=(
  ["branded"]="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_branded_food_json_2025-12-18.zip"
  ["foundation"]="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_json_2025-12-18.zip"
  ["sr_legacy"]="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_json_2018-04.zip"
  ["survey"]="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_survey_food_json_2024-10-31.zip"
)

for dtype in branded foundation sr_legacy survey; do
  url="${DOWNLOADS[$dtype]}"
  zipfile="${dtype}.zip"

  echo "=== Downloading $dtype ==="
  wget -q --show-progress -O "$zipfile" "$url"

  echo "Extracting $zipfile..."
  unzip -o -q "$zipfile" -d "${dtype}_extracted"

  echo "Converting JSON → JSONL and uploading..."
  python3 - <<PYEOF
import json, os, boto3
from io import BytesIO
from botocore.config import Config

dtype = "${dtype}"
extract_dir = "${TMP_DIR}/${dtype}_extracted"
bucket = "${BUCKET}"
prefix = "${GCS_PREFIX}/${PARTITION}/${dtype}"

client = boto3.client(
    "s3",
    endpoint_url="https://storage.googleapis.com",
    aws_access_key_id="${GCS_ACCESS_KEY}",
    aws_secret_access_key="${GCS_SECRET_KEY}",
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

chunk_idx = 0
buffer = []
total = 0
FLUSH = 10_000

from decimal import Decimal
class _Enc(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal): return float(o)
        return super().default(o)

def flush(buf, idx):
    key = f"{prefix}/part_{idx:04d}.jsonl"
    body = "\n".join(json.dumps(r, cls=_Enc) for r in buf).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=BytesIO(body), ContentType="application/x-ndjson")
    print(f"  Uploaded {len(buf):>6} records → gs://{bucket}/{key}")

import ijson, re

for root, dirs, files in os.walk(extract_dir):
    for fname in sorted(files):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(root, fname)
        try:
            with open(fpath, "rb") as f:
                head = f.read(500).decode("utf-8", errors="ignore").lstrip()
                f.seek(0)
                if head.startswith("["):
                    prefix_path = "item"
                else:
                    m = re.search(r'"(\w+)"\s*:', head)
                    prefix_path = f"{m.group(1)}.item" if m else "item"
                for record in ijson.items(f, prefix_path):
                    if isinstance(record, dict):
                        record["_dataType"] = dtype
                    buffer.append(record)
                    total += 1
                    if len(buffer) >= FLUSH:
                        flush(buffer[:FLUSH], chunk_idx)
                        buffer = buffer[FLUSH:]
                        chunk_idx += 1
        except Exception as e:
            print(f"  Skipping {fname}: {e}")

if buffer:
    flush(buffer, chunk_idx)

print(f"Done {dtype}: {total} total records")
PYEOF

  # Cleanup to save disk space before next download
  rm -rf "${dtype}_extracted" "$zipfile"
  echo "Cleaned up $dtype temp files"
  echo ""
done

echo "=== USDA bulk download complete ==="
echo "Data at: gs://${BUCKET}/${GCS_PREFIX}/${PARTITION}/"
