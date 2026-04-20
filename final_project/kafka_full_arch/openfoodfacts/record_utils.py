import json


def choose_off_record_key(record: dict) -> str:
    code = record.get("code")
    if code is not None and str(code).strip():
        return str(code)

    product_url = record.get("url")
    if product_url is not None and str(product_url).strip():
        return str(product_url)

    product_name = record.get("product_name")
    if product_name is not None and str(product_name).strip():
        return str(product_name)

    # Safe fallback without sort_keys, and with stringified keys
    safe_record = {str(k): v for k, v in record.items()}
    return json.dumps(safe_record, ensure_ascii=False, default=str)