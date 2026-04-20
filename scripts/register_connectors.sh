#!/bin/bash
# Register Kafka Connect S3 Sink connectors for OFF and openFDA → GCS bronze
# Run after kafka-connect container is healthy: bash scripts/register_connectors.sh

CONNECT_URL="http://localhost:8083"
GCS_ACCESS_KEY="${GCS_ACCESS_KEY:-GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535}"
GCS_SECRET_KEY="${GCS_SECRET_KEY:-/yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx}"
BRONZE_BUCKET="${BRONZE_BUCKET:-mip-bronze-2024}"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  sleep 5
done
echo "Kafka Connect is up."

# ── OFF connector ──────────────────────────────────────────────────────────────
curl -sf -X DELETE "$CONNECT_URL/connectors/off-gcs-sink" > /dev/null 2>&1 || true

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"off-gcs-sink\",
    \"config\": {
      \"connector.class\": \"io.confluent.connect.s3.S3SinkConnector\",
      \"tasks.max\": \"1\",
      \"topics\": \"source.off.deltas\",
      \"s3.region\": \"us-east-1\",
      \"s3.bucket.name\": \"$BRONZE_BUCKET\",
      \"s3.part.size\": \"67108864\",
      \"store.url\": \"https://storage.googleapis.com\",
      \"aws.access.key.id\": \"$GCS_ACCESS_KEY\",
      \"aws.secret.access.key\": \"$GCS_SECRET_KEY\",
      \"storage.class\": \"io.confluent.connect.s3.storage.S3Storage\",
      \"format.class\": \"io.confluent.connect.s3.format.json.JsonFormat\",
      \"flush.size\": \"10000\",
      \"rotate.interval.ms\": \"600000\",
      \"topics.dir\": \"off\",
      \"locale\": \"en_US\",
      \"timezone\": \"UTC\",
      \"timestamp.extractor\": \"WallClock\",
      \"s3.object.tagging\": \"false\",
      \"s3.ssea.name\": \"\",
      \"schema.compatibility\": \"NONE\"
    }
  }" | python3 -m json.tool

echo ""

# ── openFDA connector ──────────────────────────────────────────────────────────
curl -sf -X DELETE "$CONNECT_URL/connectors/openfda-gcs-sink" > /dev/null 2>&1 || true

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"openfda-gcs-sink\",
    \"config\": {
      \"connector.class\": \"io.confluent.connect.s3.S3SinkConnector\",
      \"tasks.max\": \"1\",
      \"topics\": \"source.openfda.recalls\",
      \"s3.region\": \"us-east-1\",
      \"s3.bucket.name\": \"$BRONZE_BUCKET\",
      \"s3.part.size\": \"67108864\",
      \"store.url\": \"https://storage.googleapis.com\",
      \"aws.access.key.id\": \"$GCS_ACCESS_KEY\",
      \"aws.secret.access.key\": \"$GCS_SECRET_KEY\",
      \"storage.class\": \"io.confluent.connect.s3.storage.S3Storage\",
      \"format.class\": \"io.confluent.connect.s3.format.json.JsonFormat\",
      \"flush.size\": \"1000\",
      \"rotate.interval.ms\": \"300000\",
      \"topics.dir\": \"openfda\",
      \"locale\": \"en_US\",
      \"timezone\": \"UTC\",
      \"timestamp.extractor\": \"WallClock\",
      \"s3.object.tagging\": \"false\",
      \"s3.ssea.name\": \"\",
      \"schema.compatibility\": \"NONE\"
    }
  }" | python3 -m json.tool

echo ""
echo "Done. Check connector status:"
echo "  curl http://localhost:8083/connectors/off-gcs-sink/status | python3 -m json.tool"
echo "  curl http://localhost:8083/connectors/openfda-gcs-sink/status | python3 -m json.tool"
