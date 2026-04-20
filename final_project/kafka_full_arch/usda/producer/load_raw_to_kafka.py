import json
import sys
from pathlib import Path
from decimal import Decimal

import ijson

from common.config import get_env
from common.kafka_utils import create_producer
from usda.constants import RAW_JSON_PATH, USDA_FOODS_RAW_TOPIC
from usda.record_utils import choose_usda_record_key


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)


def normalize_for_json(obj):
    """
    Recursively convert Decimal values into JSON-serializable Python values.
    """
    if isinstance(obj, Decimal):
        # preserve integers as int, decimals as float
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)

    if isinstance(obj, dict):
        return {k: normalize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [normalize_for_json(v) for v in obj]

    return obj


def iter_usda_records(path: str):
    """
    Stream USDA records from a very large JSON file.

    Supports:
    1. top-level list: [ {...}, {...}, ... ]
    2. top-level dict with BrandedFoods/FoundationFoods arrays
    """
    file_path = Path(path)

    with file_path.open("rb") as f:
        parser = ijson.parse(f)
        first_event = None

        for prefix, event, value in parser:
            first_event = (prefix, event, value)
            if event in ("start_array", "start_map"):
                break

    with file_path.open("rb") as f:
        if first_event and first_event[1] == "start_array":
            for record in ijson.items(f, "item"):
                yield record
            return

    with file_path.open("rb") as f:
        try:
            yielded_any = False
            for record in ijson.items(f, "BrandedFoods.item"):
                yielded_any = True
                yield record
            if yielded_any:
                return
        except Exception:
            pass

    with file_path.open("rb") as f:
        try:
            yielded_any = False
            for record in ijson.items(f, "FoundationFoods.item"):
                yielded_any = True
                yield record
            if yielded_any:
                return
        except Exception:
            pass

    raise ValueError(
        "Unsupported USDA JSON structure. Expected a top-level list or a dict "
        "containing 'BrandedFoods' or 'FoundationFoods'."
    )


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)

    producer = create_producer(bootstrap_servers)

    print(f"Streaming USDA records from: {RAW_JSON_PATH}")
    print(f"Publishing to Kafka topic: {USDA_FOODS_RAW_TOPIC}")

    total = 0

    for record in iter_usda_records(RAW_JSON_PATH):
        record = normalize_for_json(record)

        key = choose_usda_record_key(record)
        value = json.dumps(record, ensure_ascii=False)

        producer.produce(
            topic=USDA_FOODS_RAW_TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

        total += 1

        if total % 1000 == 0:
            print(f"Queued {total} USDA records so far...")

    producer.flush()
    print(f"Finished loading local USDA raw file into Kafka. Total: {total}")


if __name__ == "__main__":
    main()