"""
Open Food Facts producer.
Streams openfoodfacts/product-database (food split, ~4.48M records)
from HuggingFace without local download and produces to Kafka topic
source.off.deltas.
Run: python -m src.producers.off_producer
"""

import json
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datasets import load_dataset

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "34.42.181.128:9092")
TOPIC           = "source.off.deltas"
BATCH_SIZE      = 500   # flush producer every N messages for throughput

# Fields to keep — maps to unified schema columns
KEEP_FIELDS = [
    "code",                  # barcode → gtin_upc match with USDA
    "product_name",
    "brands",
    "ingredients_text",
    "categories",
    "pnns_groups_1",         # broad food category
    "pnns_groups_2",
    "allergens",
    "traces",
    "labels",                # organic, fair-trade, etc.
    "countries",
    "serving_size",
    "energy_100g",
    "fat_100g",
    "carbohydrates_100g",
    "proteins_100g",
    "salt_100g",
    "nova_group",
    "nutriscore_grade",
    "data_quality_tags",
    "last_modified_t",
]


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,             # acks=1 for throughput on large dataset
        retries=3,
        linger_ms=50,       # batch messages for throughput
        batch_size=65536,
        max_block_ms=60_000,
    )


def main():
    producer = make_producer()

    print("Streaming openfoodfacts/product-database (food split)...")
    dataset = load_dataset(
        "openfoodfacts/product-database",
        split="food",
        streaming=True,
        trust_remote_code=True,
    )

    total   = 0
    skipped = 0

    for record in dataset:
        # Drop records with no product name — unusable for pipeline
        if not record.get("product_name"):
            skipped += 1
            continue

        row = {k: record.get(k) for k in KEEP_FIELDS}
        producer.send(TOPIC, value=row)
        total += 1

        if total % BATCH_SIZE == 0:
            producer.flush()

        if total % 50_000 == 0:
            print(f"  Produced {total:>7} records (skipped {skipped} no-name)")

    producer.flush()
    producer.close()
    print(f"Done. Total produced: {total}, skipped (no name): {skipped}")


if __name__ == "__main__":
    main()
