# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

Two parallel trees live side-by-side:

- `kafka_openfda_demo/` — the original minimal demo (single producer/consumer script pair plus `docker-compose.yml` and `create_topic.sh`). Treat this as the "starter" reference; new work goes into `kafka_full_arch`.
- `kafka_full_arch/` — the real architecture. A Python package structured as `common/` (infra utilities) plus one directory per data source (`fda/`, `usda/`, `openfoodfacts/`). All active development targets this tree.

The two trees share the same Kafka broker defined in `kafka_openfda_demo/docker-compose.yml` (Kafka 3.9.2 in KRaft mode on `127.0.0.1:9092`).

## Running things

All Python entry points in `kafka_full_arch` import as `common.*` and `fda.*`, so commands must be run **from inside the `kafka_full_arch` directory** (not the repo root), otherwise imports break. Relative paths in `fda/constants.py` (`fda/data/raw/...`), `usda/constants.py` (`usda/data/raw/...`), and the state-store files (`{source}/data/state/refresh_state.json`) also assume that working directory.

```bash
# Start Kafka + create all topics (openfda_raw, usda_foods_raw, openfoodfacts_products_raw)
docker compose -f kafka_openfda_demo/docker-compose.yml up -d
bash kafka_openfda_demo/create_topic.sh
# On Windows without bash, use the PowerShell equivalents instead:
#   pwsh kafka_full_arch/scripts/create_topics.ps1
#   pwsh kafka_full_arch/scripts/list_topics.ps1   # sanity check

# --- FDA (local raw dump → Kafka) ---
cd kafka_full_arch && python -m fda.producer.load_raw_to_kafka
cd kafka_full_arch && python -m fda.consumer.consume_raw
cd kafka_full_arch && python -m fda.scripts.seed_watermark_from_raw   # once before refresh
cd kafka_full_arch && python -m fda.refresh_producer                  # incremental API fetch

# --- USDA (local raw dump → Kafka, then incremental API refresh) ---
cd kafka_full_arch && python -m usda.scripts.inspect_raw_file         # peek at shape
cd kafka_full_arch && python -m usda.producer.load_raw_to_kafka       # bulk load from local file
cd kafka_full_arch && python -m usda.consumer.consume_raw
cd kafka_full_arch && python -m usda.scripts.seed_watermark_from_raw  # once before refresh
cd kafka_full_arch && python -m usda.refresh_producer                 # incremental API fetch

# --- OpenFoodFacts product database (HuggingFace dataset → Kafka, no key) ---
cd kafka_full_arch && python -m openfoodfacts.scripts.inspect_raw_file
cd kafka_full_arch && python -m openfoodfacts.producer.load_raw_to_kafka
cd kafka_full_arch && python -m openfoodfacts.consumer.consume_raw
```

Dependencies are managed with Poetry (`pyproject.toml`, `poetry.lock`). `requirements.txt` at the repo root is stale/partial and only reflects the older demo — prefer Poetry. Python is pinned to `>=3.11,<3.13`. Declared deps: `confluent-kafka`, `requests`, `python-dotenv`, `snowflake-connector-python`, `ijson` (USDA streaming loader + seeder), `datasets` (OpenFoodFacts HuggingFace stream).

## Required environment

Loaded from `.env` at the repo root via `python-dotenv`:

- `KAFKA_BOOTSTRAP_SERVERS` — required by every producer/consumer (`127.0.0.1:9092` for the local docker-compose broker).
- `OPENFDA_API_KEY` — required only by `fda.refresh_producer`.
- `USDA_API_KEY` — required by `usda.refresh_producer`, `usda.scripts.inspect_raw_file`, and `usda.test_api_connection` (data.gov FoodData Central key; default rate limit 1000 req/hour). **Not** needed by `usda.producer.load_raw_to_kafka` — that script now streams a local file.
- `OFF_MAX_RECORDS` (default `5000`) — cap on the OpenFoodFacts bulk load. Set to empty/`0` to stream the full ~4.4M product rows. No API key needed.
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE` — required by anything touching `common/snowflake_utils.py` or `common/metadata_store.py`.

## Architecture

The pipeline is a **raw-first, watermark-based ingestion** into Kafka with Snowflake as the metadata/state backend:

1. **Initial load** (`fda/producer/load_raw_to_kafka.py`) reads the bundled openFDA food-enforcement dump (`fda/data/raw/food-enforcement-0001-of-0001.json`) and publishes every record to the `openfda_raw` topic. Kafka message keys are composed from `recall_number||event_id||recalling_firm` (see `fda/record_utils.choose_fda_record_key`) so the same logical record always lands on the same partition — this is the de-dup anchor for downstream consumers.

2. **Incremental refresh** (`fda/refresh_producer.py`) calls the openFDA API (`FDA_REFRESH_URL`) sorted by `report_date:asc`, filtered to `report_date:[watermark+1 TO *]`. The watermark is the max `report_date` seen so far. A 404 from the API is treated as "no new records" — not an error.

3. **Watermark storage** has three file-based implementations in play, plus an unused Snowflake path:
   - `common/state_store.py` — source-aware single-value watermark: `get_last_watermark(source)` / `save_last_watermark(source, value)` write to `{source}/data/state/refresh_state.json`. **Only FDA uses this** (`fda/refresh_producer.py`, `fda/scripts/seed_watermark_from_raw.py`). It stores a single `last_watermark` field.
   - `usda/state_store.py` — USDA-specific compound watermark: `get_last_state()` / `save_state(last_publication_date, last_seen_fdc_id)` write a dict with both fields to `usda/data/state/refresh_state.json`. The `last_seen_fdc_id` tiebreaker exists because USDA `publicationDate` is day-level, so many records share a date — `(date, fdcId)` ordering is needed to avoid republishing records already seen on the boundary day. This store has a hardcoded path constant (`STATE_FILE`), unlike the source-parameterized `common/state_store.py`. Do not try to "unify" it with the common store without understanding the compound-watermark semantics.
   - `common/metadata_store.py` — Snowflake-based replacement using `INGESTION_SOURCE_STATE` and `INGESTION_RUN_LOG` tables. Not yet wired into any refresh producer, and `INGESTION_RUN_LOG` has no callers anywhere — it's greenfield, not partially adopted. When adding run logging or switching watermark storage, use this module rather than extending the JSON files.

4. **Consumer** (`fda/consumer/consume_raw.py`) is currently a printing-only subscriber to `openfda_raw` under group `openfda-consumer-group` with `auto.offset.reset=earliest`. It is the scaffolding point for anything that needs to land records into Snowflake or transformed storage.

### Module boundaries

- `common/` is source-agnostic infrastructure: Kafka client factories (`kafka_utils.py`), env loading (`config.py`), JSON/JSONL IO (`file_utils.py`), FDA watermark persistence (`state_store.py`), Snowflake metadata store (`metadata_store.py`), and the Snowflake connection contextmanager (`snowflake_utils.py`). Keep source-specific logic out of here — note that USDA deliberately keeps its own `usda/state_store.py` rather than pushing its compound watermark into `common/`.
- `fda/`, `usda/`, `openfoodfacts/` are source plugins that all follow the same rough shape: `constants.py` (topic names, API/dataset identifiers, group ID), `record_utils.py` (key chooser — FDA uses `recall_number||event_id||recalling_firm`, USDA uses `fdcId`, OFF uses product `code`/barcode), `producer/load_raw_to_kafka.py`, `consumer/consume_raw.py`, and `scripts/inspect_raw_file.py`. Each source has its own **ingestion strategy** chosen to fit the upstream:
  - **FDA** reads a bundled local JSON dump (`fda/data/raw/food-enforcement-0001-of-0001.json`) for the initial load via `read_json_file` (whole-file in-memory — fine, it's ~6 MB). Incremental refresh against the openFDA API uses a server-side `report_date:[watermark+1 TO *]` filter.
  - **USDA** reads a local FoodData Central JSON download (`usda/data/raw/FoodData_Central_branded_food_json_*.json`, ~3.3 GB, top-level key `BrandedFoods`) for the initial load. It **must** stream with `ijson` — a plain `json.load()` would OOM. Both `usda/producer/load_raw_to_kafka.py` and `usda/scripts/seed_watermark_from_raw.py` define their own `iter_usda_records()` that auto-detects the top-level shape (list → `BrandedFoods.item` → `FoundationFoods.item`), so the same code works on either dataset variant. Records pass through `normalize_for_json()` to convert `ijson`'s `Decimal` output into int/float before `json.dumps`. Incremental refresh paginates the FDC `foods/list` REST API with `page_size=200, max_pages_per_run=3` **hardcoded** in `refresh_producer.main()` (not env-driven). It does *not* try to sort or push a filter server-side — instead, every record is tested by `is_new_record(record, last_publication_date, last_seen_fdc_id)` which compares the compound `(date, fdcId)` watermark, and the loop stops early on the first page with zero new records. USDA watermarks are stored in **raw `M/D/YYYY`** form (not normalized to ISO), parsed on demand via `parse_publication_date`.
  - **OpenFoodFacts** streams the HuggingFace `openfoodfacts/product-database` dataset via `datasets.load_dataset(..., streaming=True)`. This is deliberately *not* using REST pagination — the dataset is ~4.4M rows, so `load_dataset` (backed by parquet shards) is orders of magnitude faster than paginating the `datasets-server/rows` endpoint, and streaming keeps memory bounded.

### Known rough edges

- `fda/constants.py` defines `FDA_RAW_TOPIC` twice (lines 1 and 8) — harmless but worth tidying if you touch the file.
- `iter_usda_records()` is duplicated verbatim between `usda/producer/load_raw_to_kafka.py` and `usda/scripts/seed_watermark_from_raw.py`. It's a candidate for extraction into `common/`, but don't refactor it silently — the auto-detect ordering (list → BrandedFoods → FoundationFoods) matters for the currently-loaded dataset.
- `usda/scripts/inspect_raw_file.py` is named like a file inspector but actually calls the FDC API (`foods/list`, `pageSize=1`). Despite the path, it probes the API, not the local dump — don't be misled.
- `usda/test_api_connection.py` is a standalone API sanity check that bypasses `common/config.py` and uses `dotenv` directly. It exists intentionally as a debug helper; don't "normalize" it into the shared config path.
- `common/state_store.get_last_watermark(source)` and `common/metadata_store.get_last_watermark(...)` have similar names but different signatures and storage backends — they are not interchangeable.
- All watermark state files use working-directory-relative paths (`{source}/data/state/refresh_state.json` for FDA via the parameterized `common/state_store`, and a hardcoded `usda/data/state/refresh_state.json` in `usda/state_store`). This is why commands must be run from `kafka_full_arch/`.
