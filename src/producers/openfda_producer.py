"""
openFDA food enforcement (recall) producer.
Polls https://api.fda.gov/food/enforcement.json and produces
all records to Kafka topic source.openfda.recalls.
Run once: python -m src.producers.openfda_producer
"""

import json
import os
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "34.42.181.128:9092")
TOPIC           = "source.openfda.recalls"
FDA_URL         = "https://api.fda.gov/food/enforcement.json"
PAGE_LIMIT      = 100   # openFDA max per request
RETRY_WAIT      = 5     # seconds between retries on 429/503


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_block_ms=30_000,
    )


def fetch_page(skip: int, limit: int = PAGE_LIMIT) -> list:
    for attempt in range(3):
        try:
            resp = requests.get(
                FDA_URL,
                params={"limit": limit, "skip": skip},
                timeout=30,
            )
            if resp.status_code == 404:
                return []   # past end of results
            if resp.status_code in (429, 503):
                time.sleep(RETRY_WAIT * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json().get("results", [])
        except requests.RequestException as e:
            print(f"  Page skip={skip} attempt {attempt+1} failed: {e}")
            time.sleep(RETRY_WAIT)
    return []


def main():
    producer = make_producer()
    skip     = 0
    total    = 0

    print(f"Producing openFDA recalls → {TOPIC}")

    while True:
        records = fetch_page(skip)
        if not records:
            break

        for rec in records:
            producer.send(TOPIC, value=rec)

        total += len(records)
        print(f"  skip={skip:>6}  fetched={len(records):>3}  total={total:>6}")

        if len(records) < PAGE_LIMIT:
            break

        skip += PAGE_LIMIT

    producer.flush()
    producer.close()
    print(f"Done. Total records produced: {total}")


if __name__ == "__main__":
    main()
