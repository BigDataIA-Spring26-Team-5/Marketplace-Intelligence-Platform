import json

from common.file_utils import read_json_file
from common.state_store import save_last_watermark
from fda.constants import FDA_SOURCE_NAME, RAW_JSON_PATH


def main():
    data = read_json_file(RAW_JSON_PATH)

    if not isinstance(data, dict) or "results" not in data:
        raise ValueError("Expected FDA JSON with top-level 'results' key.")

    records = data["results"]

    report_dates = [
        r.get("report_date")
        for r in records
        if r.get("report_date")
    ]

    if not report_dates:
        raise ValueError("No report_date values found in FDA raw file.")

    latest_report_date = max(report_dates)
    save_last_watermark(FDA_SOURCE_NAME, latest_report_date)

    print(f"Seeded watermark from raw FDA dump: {latest_report_date}")


if __name__ == "__main__":
    main()