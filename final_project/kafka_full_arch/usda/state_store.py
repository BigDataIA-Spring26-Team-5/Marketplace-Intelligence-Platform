import json
from pathlib import Path

STATE_FILE = Path("usda/data/state/refresh_state.json")


def get_last_state() -> dict:
    if not STATE_FILE.exists():
        return {
            "last_publication_date": None,
            "last_seen_fdc_id": None,
        }

    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(last_publication_date: str | None, last_seen_fdc_id: int | None):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "last_publication_date": last_publication_date,
        "last_seen_fdc_id": last_seen_fdc_id,
    }

    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)