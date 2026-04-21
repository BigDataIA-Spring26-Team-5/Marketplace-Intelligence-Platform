import json
from pathlib import Path


def read_json_file(path: str):
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_jsonl(records, path: str):
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_jsonl(path: str):
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)