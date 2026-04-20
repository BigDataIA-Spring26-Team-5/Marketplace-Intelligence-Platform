# Open Food Facts Daily Sync to S3

This isolated folder gives you a minimal Airflow-in-Docker setup that downloads the daily Open Food Facts CSV export and uploads it to S3-compatible object storage.

## What it does

- Pulls the OFF daily CSV export from:
  - `https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz`
- Uploads a date-partitioned copy to:
  - `s3://<bucket>/<prefix>/raw/dt=YYYY-MM-DD/en.openfoodfacts.org.products.csv.gz`
- Copies the newest file to:
  - `s3://<bucket>/<prefix>/latest/en.openfoodfacts.org.products.csv.gz`
- Writes a manifest to:
  - `s3://<bucket>/<prefix>/manifests/latest.json`

## Why daily

Open Food Facts publishes daily exports. The Open Food Facts wiki lists:
- a MongoDB daily export
- a JSONL daily export
- a CSV daily export

Sources:
- https://wiki.openfoodfacts.org/index.php?mobileaction=toggle_view_desktop&title=Reusing_Open_Food_Facts_Data
- https://openfoodfacts.github.io/documentation/docs/Product-Opener/api/

## Start

1. Copy `.env.example` to `.env`
2. Fill in your S3 credentials and bucket
3. Run:

```bash
cd ops/off_s3_airflow
docker compose up --build
```

The Airflow UI will be at `http://localhost:8080`.

## DAG schedule

- DAG id: `open_food_facts_daily_to_s3`
- Schedule: `0 3 * * *` (daily at 03:00 UTC)

## Run the sync job directly

You can also run the job without Airflow:

```bash
cd ops/off_s3_airflow
python jobs/sync_off_to_s3.py --execution-date 2026-04-17
```

## Notes

- The OFF CSV export is gzip-compressed and tab-separated even though the filename ends in `.csv.gz`.
- This setup is isolated on purpose and does not modify the current app or pipeline files.
