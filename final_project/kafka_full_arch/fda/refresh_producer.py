import json
import sys
import requests
from datetime import datetime, timedelta

from common.config import get_env
from common.kafka_utils import create_producer
from common.state_store import get_last_watermark, save_last_watermark
from fda.constants import FDA_REFRESH_URL, FDA_RAW_TOPIC, FDA_SOURCE_NAME
from fda.record_utils import choose_fda_record_key


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}", file=sys.stderr)


def next_report_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d")
    return (dt + timedelta(days=1)).strftime("%Y%m%d")


def fetch_fda_incremental(api_key: str, watermark: str | None, limit: int = 100) -> dict:
    params = {
        "api_key": api_key,
        "limit": limit,
        "sort": "report_date:asc",
    }

    if watermark:
        next_day = next_report_date(watermark)
        params["search"] = f"report_date:[{next_day} TO *]"

    print("API params being sent:", params)

    response = requests.get(FDA_REFRESH_URL, params=params, timeout=60)

    if response.status_code == 404:
        print("No new FDA records found for the requested date range.")
        return {"results": []}

    response.raise_for_status()
    return response.json()


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)
    api_key = get_env("OPENFDA_API_KEY", required=True)

    watermark_before = get_last_watermark(FDA_SOURCE_NAME)
    print(f"Previous watermark: {watermark_before}")

    producer = create_producer(bootstrap_servers)

    payload = fetch_fda_incremental(api_key, watermark_before, limit=100)
    records = payload.get("results", [])

    records_published = 0
    watermark_after = watermark_before

    for record in records:
        key = choose_fda_record_key(record)
        value = json.dumps(record, ensure_ascii=False)

        producer.produce(
            topic=FDA_RAW_TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        records_published += 1

    producer.flush()

    if records:
        watermark_after = max(
            r.get("report_date", watermark_before or "")
            for r in records
            if r.get("report_date")
        )
        save_last_watermark(FDA_SOURCE_NAME, watermark_after)

    print("Run status: SUCCESS")
    print(f"Records fetched: {len(records)}")
    print(f"Records published: {records_published}")
    print(f"Watermark after: {watermark_after}")


if __name__ == "__main__":
    main()