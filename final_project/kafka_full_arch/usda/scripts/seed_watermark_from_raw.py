import ijson
from pathlib import Path
from datetime import datetime

from usda.constants import RAW_JSON_PATH
from usda.state_store import save_state


def parse_publication_date(date_str: str):
    """
    USDA dates may appear like '9/29/2020'.
    Convert them into datetime objects for proper comparison.
    """
    return datetime.strptime(date_str, "%m/%d/%Y")


def iter_usda_records(path: str):
    """
    Stream USDA records from a large JSON file.

    Supports:
    1. top-level list
    2. dict with BrandedFoods / FoundationFoods arrays
    """
    file_path = Path(path)

    with file_path.open("rb") as f:
        parser = ijson.parse(f)
        first_event = None

        for prefix, event, value in parser:
            first_event = (prefix, event, value)
            if event in ("start_array", "start_map"):
                break

    if first_event and first_event[1] == "start_array":
        with file_path.open("rb") as f:
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
        "Unsupported USDA JSON structure. Expected a top-level list or "
        "a dict containing 'BrandedFoods' or 'FoundationFoods'."
    )


def main():
    latest_publication_date_raw = None
    latest_publication_date_parsed = None
    last_seen_fdc_id = None
    total = 0

    for record in iter_usda_records(RAW_JSON_PATH):
        total += 1

        pub_date_raw = record.get("publicationDate")
        fdc_id = record.get("fdcId")

        if not pub_date_raw or fdc_id is None:
            continue

        try:
            pub_date_parsed = parse_publication_date(pub_date_raw)
        except ValueError:
            # skip malformed dates
            continue

        if (
            latest_publication_date_parsed is None
            or pub_date_parsed > latest_publication_date_parsed
        ):
            latest_publication_date_parsed = pub_date_parsed
            latest_publication_date_raw = pub_date_raw
            last_seen_fdc_id = fdc_id
        elif pub_date_parsed == latest_publication_date_parsed and fdc_id > last_seen_fdc_id:
            last_seen_fdc_id = fdc_id

        if total % 100000 == 0:
            print(f"Scanned {total} USDA records so far...")

    if latest_publication_date_raw is None:
        raise ValueError("No valid publicationDate found in USDA raw file.")

    save_state(latest_publication_date_raw, last_seen_fdc_id)

    print(f"Finished scanning USDA raw file. Total records scanned: {total}")
    print(f"Seeded USDA watermark: {latest_publication_date_raw}")
    print(f"Seeded USDA last_seen_fdc_id: {last_seen_fdc_id}")


if __name__ == "__main__":
    main()