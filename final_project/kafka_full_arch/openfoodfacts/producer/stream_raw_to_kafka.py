import csv
import json
import sys
import time

from common.config import get_env
from common.kafka_utils import create_producer
from openfoodfacts.constants import RAW_CSV_PATH, OPENFOODFACTS_RAW_TOPIC
from openfoodfacts.record_utils import choose_off_record_key


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)


def clean_value(value):
    if value == "":
        return None
    return value


def normalize_row(row: dict) -> dict:
    cleaned = {}
    for k, v in row.items():
        if k is None:
            continue
        cleaned[str(k)] = clean_value(v)
    return cleaned


def set_max_csv_field_size():
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = limit // 10


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)

    producer = create_producer(bootstrap_servers)

    # If you want, later this can be moved into create_producer() with extra config:
    # Producer({
    #   "bootstrap.servers": bootstrap_servers,
    #   "queue.buffering.max.messages": 500000,
    #   "queue.buffering.max.kbytes": 1048576,
    # })

    print(f"Reading OFF CSV from: {RAW_CSV_PATH}")
    print(f"Publishing to Kafka topic: {OPENFOODFACTS_RAW_TOPIC}")

    total = 0
    set_max_csv_field_size()

    try:
        with open(RAW_CSV_PATH, "r", encoding="utf-8", newline="", errors="replace") as f:
            reader = csv.DictReader(f)

            for row in reader:
                record = normalize_row(row)

                key = choose_off_record_key(record)
                value = json.dumps(record, ensure_ascii=False, default=str)

                # Retry if local producer queue is full
                while True:
                    try:
                        producer.produce(
                            topic=OPENFOODFACTS_RAW_TOPIC,
                            key=key.encode("utf-8"),
                            value=value.encode("utf-8"),
                            callback=delivery_report,
                        )
                        break
                    except BufferError:
                        # Let Kafka client send queued messages, then retry
                        producer.poll(1.0)
                        time.sleep(0.01)

                # Serve delivery callbacks and drain queue a bit
                producer.poll(0)

                total += 1

                if total % 10000 == 0:
                    print(f"Queued {total} OpenFoodFacts records so far...")

        # Wait for everything outstanding to be delivered
        producer.flush()

    finally:
        print(f"Finished loading OFF CSV into Kafka. Total queued before exit: {total}")


if __name__ == "__main__":
    main()