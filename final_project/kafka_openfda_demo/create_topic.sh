#!/usr/bin/env bash
set -e

TOPICS=(
  openfda_raw
  usda_foods_raw
  openfoodfacts_products_raw
)

for topic in "${TOPICS[@]}"; do
  docker exec kafka-local kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done
