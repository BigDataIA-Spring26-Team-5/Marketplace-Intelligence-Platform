import json
import sys

from common.config import get_env
from common.file_utils import read_json_file
from common.kafka_utils import create_producer
from fda.constants import RAW_JSON_PATH, FDA_RAW_TOPIC


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)
    else:
        key = msg.key().decode("utf-8") if msg.key() else None
        print(
            f"Delivered key={key} to {msg.topic()} "
            f"[{msg.partition()}] @ offset {msg.offset()}"
        )


def choose_record_key(record: dict) -> str:
    key_parts = [
        str(record.get("recall_number", "")),
        str(record.get("event_id", "")),
        str(record.get("recalling_firm", "")),
    ]
    key = "||".join(key_parts).strip()
    return key if key else json.dumps(record, sort_keys=True)


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)

    data = read_json_file(RAW_JSON_PATH)

    if not isinstance(data, dict) or "results" not in data:
        raise ValueError("Expected FDA JSON with top-level 'results' key.")

    records = data["results"]
    print(f"Loaded {len(records)} records from local FDA file")

    producer = create_producer(bootstrap_servers)

    for idx, record in enumerate(records, start=1):
        key = choose_record_key(record)
        value = json.dumps(record, ensure_ascii=False)

        producer.produce(
            topic=FDA_RAW_TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

        if idx % 1000 == 0:
            print(f"Queued {idx} records so far...")

    producer.flush()
    print("Finished loading local FDA raw file into Kafka")


if __name__ == "__main__":
    main()