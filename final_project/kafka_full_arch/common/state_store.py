import json
from pathlib import Path


def _state_file(source: str) -> Path:
    return Path(f"{source}/data/state/refresh_state.json")


def get_last_watermark(source: str) -> str | None:
    state_file = _state_file(source)
    if not state_file.exists():
        return None

    with state_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("last_watermark")


def save_last_watermark(source: str, watermark: str) -> None:
    state_file = _state_file(source)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    with state_file.open("w", encoding="utf-8") as f:
        json.dump({"last_watermark": watermark}, f, indent=2)
