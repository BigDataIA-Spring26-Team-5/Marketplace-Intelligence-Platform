import json
import sys
from datetime import datetime
from decimal import Decimal

import requests

from common.config import get_env
from common.kafka_utils import create_producer
from usda.constants import USDA_FOODS_LIST_URL, USDA_FOODS_RAW_TOPIC
from usda.record_utils import choose_usda_record_key
from usda.state_store import get_last_state, save_state


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)


def normalize_for_json(obj):
    if isinstance(obj, Decimal):
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)

    if isinstance(obj, dict):
        return {k: normalize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [normalize_for_json(v) for v in obj]

    return obj


def parse_publication_date(date_str: str):
    """
    Support both:
    - MM/DD/YYYY   e.g. 12/18/2025
    - YYYY-MM-DD   e.g. 2024-10-31
    """
    if not date_str:
        raise ValueError("Empty publicationDate")

    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unsupported publicationDate format: {date_str}")


def fetch_usda_page(api_key: str, page_size: int = 200, page_number: int = 1) -> list[dict]:
    params = {
        "api_key": api_key,
        "pageSize": page_size,
        "pageNumber": page_number,
    }

    print(f"Requesting USDA page {page_number} with params: {params}")
    response = requests.get(USDA_FOODS_LIST_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Expected USDA /foods/list response to be a list of records.")

    return data


def is_new_record(record: dict, last_publication_date: str | None, last_seen_fdc_id: int | None) -> bool:
    pub_date_raw = record.get("publicationDate")
    fdc_id = record.get("fdcId")

    if not pub_date_raw or fdc_id is None:
        return False

    if last_publication_date is None:
        return True

    current_date = parse_publication_date(pub_date_raw)
    watermark_date = parse_publication_date(last_publication_date)

    if current_date > watermark_date:
        return True

    if current_date == watermark_date and last_seen_fdc_id is not None and fdc_id > last_seen_fdc_id:
        return True

    return False


def update_latest_seen(record: dict, latest_publication_date: str | None, latest_fdc_id: int | None):
    pub_date_raw = record.get("publicationDate")
    fdc_id = record.get("fdcId")

    if not pub_date_raw or fdc_id is None:
        return latest_publication_date, latest_fdc_id

    if latest_publication_date is None:
        return pub_date_raw, fdc_id

    current_date = parse_publication_date(pub_date_raw)
    latest_date = parse_publication_date(latest_publication_date)

    if current_date > latest_date:
        return pub_date_raw, fdc_id

    if current_date == latest_date and latest_fdc_id is not None and fdc_id > latest_fdc_id:
        return pub_date_raw, fdc_id

    return latest_publication_date, latest_fdc_id


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)
    api_key = get_env("USDA_API_KEY", required=True)

    state = get_last_state()
    last_publication_date = state.get("last_publication_date")
    last_seen_fdc_id = state.get("last_seen_fdc_id")

    print(f"Previous publicationDate watermark: {last_publication_date}")
    print(f"Previous last_seen_fdc_id: {last_seen_fdc_id}")

    producer = create_producer(bootstrap_servers)

    page_size = 200
    max_pages_per_run = 3
    page_number = 1

    total_fetched = 0
    total_published = 0

    newest_publication_date = last_publication_date
    newest_fdc_id = last_seen_fdc_id

    while page_number <= max_pages_per_run:
        records = fetch_usda_page(api_key, page_size=page_size, page_number=page_number)

        if not records:
            print(f"No records returned on page {page_number}. Stopping.")
            break

        total_fetched += len(records)
        page_had_new_records = False

        for record in records:
            record = normalize_for_json(record)

            if not is_new_record(record, last_publication_date, last_seen_fdc_id):
                continue

            page_had_new_records = True

            key = choose_usda_record_key(record)
            value = json.dumps(record, ensure_ascii=False)

            producer.produce(
                topic=USDA_FOODS_RAW_TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            total_published += 1

            newest_publication_date, newest_fdc_id = update_latest_seen(
                record,
                newest_publication_date,
                newest_fdc_id,
            )

        print(
            f"Processed USDA page {page_number}: "
            f"fetched={len(records)}, published_new={total_published}"
        )

        if not page_had_new_records and last_publication_date is not None:
            print("No new records found on this page. Stopping manual refresh.")
            break

        page_number += 1

    producer.flush()

    if total_published > 0:
        save_state(newest_publication_date, newest_fdc_id)

    print("Run status: SUCCESS")
    print(f"Records fetched: {total_fetched}")
    print(f"Records published: {total_published}")
    print(f"Watermark after publicationDate: {newest_publication_date}")
    print(f"Watermark after last_seen_fdc_id: {newest_fdc_id}")


if __name__ == "__main__":
    main()