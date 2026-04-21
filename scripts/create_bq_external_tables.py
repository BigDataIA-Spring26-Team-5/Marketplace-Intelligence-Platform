"""
Creates BigQuery external tables pointing to GCS bronze JSONL files.
Dataset: bronze_raw
Tables: usda_branded, usda_foundation, usda_sr_legacy, usda_survey, esci, openfda
Run: python3 scripts/create_bq_external_tables.py
"""
from google.cloud import bigquery

PROJECT = "mip-platform-2024"
DATASET = "bronze_raw"
BUCKET  = "mip-bronze-2024"

TABLES = {
    "usda_branded":   [f"gs://{BUCKET}/usda/bulk/2026/04/21/branded/*.jsonl"],
    "usda_foundation":[f"gs://{BUCKET}/usda/bulk/2026/04/21/foundation/*.jsonl"],
    "usda_sr_legacy": [f"gs://{BUCKET}/usda/bulk/2026/04/21/sr_legacy/*.jsonl"],
    "usda_survey":    [f"gs://{BUCKET}/usda/bulk/2026/04/21/survey/*.jsonl"],
    "esci":           [f"gs://{BUCKET}/esci/2024/01/01/*.jsonl"],
    "openfda":        [f"gs://{BUCKET}/openfda/2026/04/20/*.jsonl"],
}

client = bigquery.Client(project=PROJECT)

# Create dataset if needed
ds_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
ds_ref.location = "US"
try:
    client.create_dataset(ds_ref, exists_ok=True)
    print(f"Dataset {DATASET} ready")
except Exception as e:
    print(f"Dataset: {e}")

for table_name, uris in TABLES.items():
    table_ref = f"{PROJECT}.{DATASET}.{table_name}"

    ext_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    ext_config.source_uris = uris
    ext_config.autodetect = True
    ext_config.ignore_unknown_values = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = ext_config

    try:
        client.delete_table(table_ref, not_found_ok=True)
        client.create_table(table)
        print(f"✓ Created external table: {DATASET}.{table_name}  ({len(uris)} URI(s))")
    except Exception as e:
        print(f"✗ {table_name}: {e}")

print("\nDone. Query in BigQuery console:")
for t in TABLES:
    print(f"  SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{t}`")
