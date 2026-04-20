import json


def choose_fda_record_key(record: dict) -> str:
    key_parts = [
        str(record.get("recall_number", "")),
        str(record.get("event_id", "")),
        str(record.get("recalling_firm", "")),
    ]
    key = "||".join(key_parts).strip()
    return key if key else json.dumps(record, sort_keys=True)