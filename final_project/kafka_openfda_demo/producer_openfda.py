import json
import os
import sys
from typing import Dict, List

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openfda_raw")
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "").strip()
PAGE_LIMIT = int(os.getenv("OPENFDA_PAGE_LIMIT", "10"))

OPENFDA_URL = "https://api.fda.gov/food/enforcement.json"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)
    else:
        key = msg.key().decode("utf-8") if msg.key() else None
        print(f"Delivered key={key} to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def get_openfda_page(limit: int = 10, skip: int = 0) -> Dict:
    if not OPENFDA_API_KEY:
        raise ValueError("OPENFDA_API_KEY is required in .env")

    params = {
        "api_key": OPENFDA_API_KEY,
        "limit": limit,
        "skip": skip,
    }

    response = requests.get(OPENFDA_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def choose_record_key(record: Dict) -> str:
    key_parts = [
        str(record.get("recall_number", "")),
        str(record.get("event_id", "")),
        str(record.get("recalling_firm", "")),
    ]
    key = "||".join(key_parts).strip()
    return key if key else json.dumps(record, sort_keys=True)


def produce_records(records: List[Dict]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for record in records:
        key = choose_record_key(record)
        value = json.dumps(record, ensure_ascii=False)

        producer.produce(
            topic=TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()


def main():
    payload = get_openfda_page(limit=PAGE_LIMIT, skip=0)
    records = payload.get("results", [])

    print(f"Fetched {len(records)} records from openFDA using API key")
    produce_records(records)
    print("Finished producing records to Kafka")


if __name__ == "__main__":
    main()