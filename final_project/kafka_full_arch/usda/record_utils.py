import json


def choose_usda_record_key(record: dict) -> str:
    fdc_id = record.get("fdcId")
    if fdc_id is not None:
        return str(fdc_id)
    return json.dumps(record, sort_keys=True)
