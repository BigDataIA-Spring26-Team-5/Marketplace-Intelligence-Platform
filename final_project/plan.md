# plan.md — kafka_full_arch onboarding guide

Welcome! This document walks you through `final_project/kafka_full_arch/` — the real Kafka ingestion architecture for the Marketplace Intelligence Platform. (There is also a smaller `kafka_openfda_demo/` folder right next door, but **that is only the starter demo** and is not covered here. All active development happens in `kafka_full_arch`.)

By the end of this guide you should be able to:

1. Bring up Kafka locally with Docker.
2. Create the Kafka topics, list them, and verify they exist.
3. Run a consumer.
4. Run a bulk-load producer (from a local raw file).
5. Read the records the producer just published.
6. Run the **refresh producer** to understand how the manual incremental refresh and fresh API calls work against openFDA / USDA.

We use the **FDA** datasource as the main worked example because it is the smallest and fastest to iterate on. USDA and OpenFoodFacts follow the same pattern and are listed at the end.

---

## 0. One-time setup

### 0.1 Install Python dependencies with Poetry

From the `final_project/` directory:

```bash
cd final_project
poetry install
```

This installs everything listed in `final_project/pyproject.toml`:

- `confluent-kafka` — the Kafka client used by every producer and consumer.
- `requests` — used by `fda/refresh_producer.py` and `usda/refresh_producer.py` for REST calls.
- `python-dotenv` — loads `.env` variables into the process.
- `snowflake-connector-python` — used by `common/snowflake_utils.py` and `common/metadata_store.py` (Snowflake is wired up but not yet required for the Kafka-only flow).
- `ijson` — streaming JSON parser. USDA's raw dump is ~3 GB and **must** be streamed; a plain `json.load` would OOM.
- `datasets` — HuggingFace streaming client used only by OpenFoodFacts.

Python must be **>=3.11, <3.13**.

### 0.2 Create your `.env` file

At the repo root (not inside `final_project/`), create a `.env` file with:

```
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
OPENFDA_API_KEY=<your openFDA key — only needed for fda.refresh_producer>
USDA_API_KEY=<your data.gov FoodData Central key — only needed for usda.refresh_producer / usda.scripts.inspect_raw_file>
OFF_MAX_RECORDS=5000
```

The bulk loaders (`load_raw_to_kafka.py`) do **not** need the API keys — they read local files. Only the incremental refresh producers hit the live APIs.

### 0.3 Know which directory to run commands from

**All Python commands must be run from inside `final_project/kafka_full_arch/`**, not the repo root and not `final_project/`. Reason: the package imports are `common.*`, `fda.*`, `usda.*`, `openfoodfacts.*`, and the state-file paths (`fda/data/state/refresh_state.json`, `usda/data/state/refresh_state.json`) are all working-directory-relative. If you run from the wrong folder, either the imports or the state files will break.

```bash
cd final_project/kafka_full_arch
```

Keep that as your working directory for everything below.

---

## 1. Start Docker and Kafka

### 1.1 Make sure the Docker engine is running

Before running any `docker` command, **Docker Desktop must be started** on your machine (on Windows: launch Docker Desktop from the Start menu and wait until it says "Engine running"; on macOS: launch the Docker app from Applications). If the engine isn't running, `docker compose up` will fail with something like `error during connect: ... The system cannot find the file specified`.

### 1.2 Bring up the Kafka broker

The only `docker-compose.yml` in the repo lives inside `kafka_openfda_demo/` — we reuse that broker for `kafka_full_arch`. From the repo root:

```bash
docker compose -f final_project/kafka_openfda_demo/docker-compose.yml up -d
```

What this does:
- Pulls and starts `apache/kafka:3.9.2` in **KRaft mode** (no Zookeeper).
- Names the container `kafka-local`.
- Exposes the broker on `127.0.0.1:9092` — this is what `KAFKA_BOOTSTRAP_SERVERS` in your `.env` points at.
- Runs the broker in the background (`-d` = detached).

You can check it is alive with:

```bash
docker ps
docker logs kafka-local --tail 50
```

You should see lines like `Kafka Server started`. If not, wait another 5–10 seconds — KRaft mode takes a moment to finish the quorum election.

### 1.3 Shut it down later

When you are done for the day:

```bash
docker compose -f final_project/kafka_openfda_demo/docker-compose.yml down
```

Add `-v` if you want to also wipe the Kafka volume (useful when you want a clean slate with zero topics and zero offsets).

---

## 2. Create the Kafka topics

Kafka will auto-create topics on first produce by default, but we prefer to create them explicitly so we control partitions and replication.

### 2.1 Create the three topics

The three topics we need are:

| Topic | Source | Defined in |
|---|---|---|
| `openfda_raw` | openFDA food enforcement (recalls) | `fda/constants.py` |
| `usda_foods_raw` | USDA FoodData Central branded foods | `usda/constants.py` |
| `openfoodfacts_raw` | OpenFoodFacts product database | `openfoodfacts/constants.py` |

The easiest way to create all three in one shot is the bash script that lives in the demo folder (works on macOS, Linux, and Git Bash on Windows):

```bash
bash final_project/kafka_openfda_demo/create_topic.sh
```

Note: that script creates `openfoodfacts_products_raw`, but `kafka_full_arch`'s `openfoodfacts/constants.py` uses the topic name `openfoodfacts_raw`. If you are running the OFF producer, also create that topic explicitly (next subsection).

If you are on Windows PowerShell without bash, or you want to create / add topics manually, run `kafka-topics` directly inside the container:

```bash
docker exec kafka-local kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic openfda_raw \
  --partitions 3 --replication-factor 1

docker exec kafka-local kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic usda_foods_raw \
  --partitions 3 --replication-factor 1

docker exec kafka-local kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic openfoodfacts_raw \
  --partitions 3 --replication-factor 1
```

Partitions = 3 matches the demo. Replication factor = 1 because we have a single broker.

> There are two files at `kafka_full_arch/scripts/create_topics.ps1` and `kafka_full_arch/scripts/list_topics.ps1`, but right now **they are empty (0 bytes)**. Don't rely on them — use the `docker exec` commands above until we fill those scripts in.

### 2.2 List the topics to verify

```bash
docker exec kafka-local kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

You should see all three topic names printed. To get more detail (partition count, replication factor, leader per partition):

```bash
docker exec kafka-local kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic openfda_raw
```

If a topic is missing, create it again with the `--create` command above. Idempotent thanks to `--if-not-exists`.

---

## 3. The FDA datasource — walking through every file

Before running anything, it helps to understand *why* each file exists. The FDA folder (`kafka_full_arch/fda/`) is the template every other source follows.

```
fda/
├── constants.py                       # Topic name, group id, API URL, file paths
├── record_utils.py                    # How to build the Kafka message key from a record
├── producer/
│   └── load_raw_to_kafka.py           # Bulk-load the local JSON dump into Kafka
├── consumer/
│   └── consume_raw.py                 # Print records from the topic (scaffolding sink)
├── refresh_producer.py                # Incremental refresh from the live openFDA API
├── scripts/
│   ├── inspect_raw_file.py            # Peek at the local JSON to sanity-check its shape
│   └── seed_watermark_from_raw.py     # Initialize the watermark from the bundled dump
└── data/
    ├── raw/food-enforcement-0001-of-0001.json   # The bundled openFDA recall dump (~6 MB)
    └── state/refresh_state.json                 # Watermark file, written by the refresh producer
```

### Why each file exists

- **`constants.py`** — the single source of truth for topic name (`FDA_RAW_TOPIC = "openfda_raw"`), consumer group id (`FDA_GROUP_ID = "openfda-consumer-group"`), refresh URL (`FDA_REFRESH_URL`), and the path to the local raw JSON. If you ever rename a topic, change it **here** — every other file imports from this.
- **`record_utils.py`** — defines `choose_fda_record_key()`, which builds the Kafka message key as `recall_number || event_id || recalling_firm`. Why it matters: Kafka uses the key to assign partitions, so **the same logical recall always lands on the same partition**. That is the de-dup anchor for downstream consumers — if an upstream record is republished, it lands on the same partition and any stateful consumer can detect the duplicate.
- **`producer/load_raw_to_kafka.py`** — the bulk loader. Reads the bundled `food-enforcement-0001-of-0001.json` whole-file (it's only ~6 MB, safe in memory), iterates `data["results"]`, and publishes every record to `openfda_raw`. Use this **once** to seed the topic from the local dump.
- **`consumer/consume_raw.py`** — a printing-only subscriber. It subscribes to `openfda_raw` under `FDA_GROUP_ID`, polls messages in a loop, and prints `recall_number`, `recalling_firm`, `product_description`, and `status`. It is scaffolding — the place where later work will write to Snowflake or another sink. For now, it is how you verify that records are actually in the topic. It uses `auto.offset.reset=earliest`, so a **new consumer group** starts reading from the beginning of the topic.
- **`refresh_producer.py`** — the **incremental** producer. Reads the latest watermark (max `report_date` published so far), calls the live openFDA API with `search=report_date:[watermark+1 TO *]` and `sort=report_date:asc`, publishes the new records to the same `openfda_raw` topic, and saves the new watermark. A 404 from the API is interpreted as "no new records" rather than an error — that is intentional.
- **`scripts/inspect_raw_file.py`** — a sanity-check helper. Prints the top-level structure of the local JSON dump so you can see it has a `results` key, how many records are inside, and what fields each record has. Run it once the first time you touch the dataset so you know what you're looking at.
- **`scripts/seed_watermark_from_raw.py`** — initializes the watermark **from the bundled dump** before the first refresh run. It finds the `max(report_date)` in the local file and writes it to `fda/data/state/refresh_state.json`. Without this step, the first `refresh_producer` run would have no watermark and would pull the entire API history, duplicating everything already in the local dump. **Run this exactly once**, before running `refresh_producer` for the first time.
- **`data/raw/food-enforcement-0001-of-0001.json`** — the bundled openFDA dump used by `load_raw_to_kafka.py` and `seed_watermark_from_raw.py`.
- **`data/state/refresh_state.json`** — the watermark file. Written by `refresh_producer.py` (and seeded by `seed_watermark_from_raw.py`). The format is `{"last_watermark": "YYYYMMDD"}`. Managed by `common/state_store.py`.

### 3.1 Inspect the raw file (optional, recommended the first time)

```bash
python -m fda.scripts.inspect_raw_file
```

Expected output: top-level type, top-level keys (should include `results`), number of records, and the list of keys on a sample record.

### 3.2 Start the consumer first

**Start the consumer before the producer.** This isn't strictly required — Kafka retains records on disk and `auto.offset.reset=earliest` means a fresh consumer group will read from offset 0 anyway — but starting the consumer first lets you watch records flow in real time and is the cleanest way to sanity-check the pipeline the first time.

Open a terminal, `cd final_project/kafka_full_arch`, then:

```bash
python -m fda.consumer.consume_raw
```

You should see `Subscribed to topic: openfda_raw` and then nothing yet. Leave it running.

### 3.3 Run the bulk-load producer

In a **second** terminal, `cd final_project/kafka_full_arch` again, then:

```bash
python -m fda.producer.load_raw_to_kafka
```

What you will see:
- `Loaded <N> records from local FDA file`
- A stream of `Delivered key=... to openfda_raw [<partition>] @ offset <n>` lines.
- `Queued <N> records so far...` every 1000 records.
- `Finished loading local FDA raw file into Kafka` at the end.

In the consumer terminal you should now see each record printed (key, recall_number, firm, product, status).

### 3.4 Check the records (alternatives)

If you want an offset-level view instead of the formatted consumer output, you can read directly from the container:

```bash
# Count records currently in the topic (messages, not bytes)
docker exec kafka-local kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic openfda_raw

# Dump a few messages from the console consumer (Ctrl+C to stop)
docker exec -it kafka-local kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic openfda_raw \
  --from-beginning \
  --max-messages 5
```

### 3.5 Seed the watermark — **run this exactly once**

Before you run `refresh_producer` for the first time, give it a starting watermark so it doesn't redundantly re-fetch everything already in the bundled dump:

```bash
python -m fda.scripts.seed_watermark_from_raw
```

What this does: reads the bundled `food-enforcement-0001-of-0001.json`, finds `max(report_date)`, and writes `fda/data/state/refresh_state.json` as `{"last_watermark": "YYYYMMDD"}`. You'll see `Seeded watermark from raw FDA dump: <date>`.

### 3.6 Run the refresh producer — manual refresh / fresh API call

Now run the incremental refresh:

```bash
python -m fda.refresh_producer
```

What happens on each invocation:

1. Reads `OPENFDA_API_KEY` and `KAFKA_BOOTSTRAP_SERVERS` from `.env`.
2. Loads the previous watermark from `fda/data/state/refresh_state.json` — prints `Previous watermark: <date>`.
3. Builds `fetch_fda_incremental` API params: `api_key`, `limit=100`, `sort=report_date:asc`, and (if a watermark exists) `search=report_date:[<watermark+1> TO *]`. Prints the params so you can see exactly what was sent.
4. Calls the live openFDA API (`FDA_REFRESH_URL`). A `404` response is treated as "no new records" (returns empty list) — **not** an error.
5. For each new record, rebuilds the key with `choose_fda_record_key()` and publishes to `openfda_raw`.
6. If at least one record was published, updates the watermark to `max(report_date)` across the fetched records and saves it back to `refresh_state.json`.
7. Prints a summary: `Run status: SUCCESS`, `Records fetched`, `Records published`, `Watermark after`.

**Calling it repeatedly** is how we simulate "manual refresh": each run asks the API only for records strictly newer than the last watermark, publishes them, and advances the watermark. The second call immediately after the first should return 0 new records — that's the correct, expected behavior, not a bug.

Your still-running `consume_raw` terminal will print any newly-published records as they arrive.

---

## 4. USDA datasource — same pattern, bigger raw file

Files are analogous. The key differences are:

- **`usda/record_utils.py`** → `choose_usda_record_key()` uses `fdcId` as the key (USDA's stable primary identifier).
- **`usda/state_store.py`** (not `common/state_store.py`!) stores a **compound watermark**: `{"last_publication_date": ..., "last_seen_fdc_id": ...}`. The `fdcId` tiebreaker exists because `publicationDate` is day-level and many records share a date — `(date, fdcId)` is needed to avoid republishing records already seen on the boundary day. Do **not** try to unify this with `common/state_store.py`; the compound semantics are load-bearing.
- **`usda/producer/load_raw_to_kafka.py`** → the raw file (`usda/data/raw/FoodData_Central_branded_food_json_*.json`) is ~3 GB, so it **must** be streamed with `ijson`. The `iter_usda_records()` helper auto-detects whether the top-level is a list, a dict with `BrandedFoods`, or a dict with `FoundationFoods`. Records go through `normalize_for_json()` to convert `ijson`'s `Decimal` output into int/float before JSON encoding.
- **`usda/refresh_producer.py`** → paginates `foods/list` (page_size=200, max 3 pages per run, **hardcoded** — not env-driven). Does not push a server-side filter; every fetched record is tested locally with `is_new_record()` against the compound watermark, and the loop stops early on the first page with zero new records.
- **`usda/scripts/inspect_raw_file.py`** → despite the name, this script **actually calls the FDC API** (`pageSize=1`) — it is probing the API, not the local dump. Don't be misled by the filename.
- **`usda/test_api_connection.py`** → standalone API sanity-check that bypasses `common/config.py` and uses `dotenv` directly. It exists as a debug helper. Don't "normalize" it into the shared config path.

### Commands

```bash
# (optional) quick API sanity check — needs USDA_API_KEY
python -m usda.scripts.inspect_raw_file

# bulk-load the local 3 GB dump (streams with ijson)
python -m usda.producer.load_raw_to_kafka

# consumer — prints fdcId, description, dataType, brandOwner (stops after 10 messages)
python -m usda.consumer.consume_raw

# seed the compound watermark from the raw dump — run once before the first refresh
python -m usda.scripts.seed_watermark_from_raw

# incremental refresh from the USDA FDC API
python -m usda.refresh_producer
```

---

## 5. OpenFoodFacts datasource — no API key, HuggingFace stream

The bulk loader for OFF currently reads a local CSV (`openfoodfacts/data/raw/en.openfoodfacts.org.products.csv`) via `openfoodfacts/producer/stream_raw_to_kafka.py` (re-exported by `load_raw_to_kafka.py`). The CSV has a huge field-size limit which the producer raises to `sys.maxsize` via `set_max_csv_field_size()`. Record keys come from `choose_off_record_key()`, which prefers the product `code` (barcode), then `url`, then `product_name`.

There is **no refresh producer** for OpenFoodFacts and **no API key needed** — the bulk HuggingFace streaming loader is the only ingestion path we have wired up right now.

### Commands

```bash
# (optional) peek at the HuggingFace dataset shape
python -m openfoodfacts.scripts.inspect_raw_file

# bulk-load from the local CSV into Kafka
python -m openfoodfacts.producer.load_raw_to_kafka

# consumer — prints code, product_name, brands, countries (stops after 10 messages)
python -m openfoodfacts.consumer.consume_raw
```

---

## 6. Quick end-to-end checklist (FDA, happy path)

A condensed copy-paste sequence for your first run:

```bash
# 1. Start Docker Desktop, then:
docker compose -f final_project/kafka_openfda_demo/docker-compose.yml up -d

# 2. Create topics
bash final_project/kafka_openfda_demo/create_topic.sh

# 3. Verify
docker exec kafka-local kafka-topics --bootstrap-server localhost:9092 --list

# 4. Move into the kafka_full_arch working directory
cd final_project/kafka_full_arch

# 5. Terminal A — start the consumer
python -m fda.consumer.consume_raw

# 6. Terminal B — bulk-load the local FDA dump
python -m fda.producer.load_raw_to_kafka

# 7. Terminal B — seed the watermark (once)
python -m fda.scripts.seed_watermark_from_raw

# 8. Terminal B — run the incremental refresh from the live openFDA API
python -m fda.refresh_producer

# 9. When done for the day
docker compose -f final_project/kafka_openfda_demo/docker-compose.yml down
```

---

## 7. Common gotchas

- **Wrong working directory.** `ModuleNotFoundError: No module named 'common'` or a missing `refresh_state.json` path almost always means you are not inside `final_project/kafka_full_arch/`.
- **Kafka not ready.** If a producer prints `%3|... Connect to broker failed`, wait a few seconds after `docker compose up` — KRaft takes a moment to elect and open the listener.
- **`FDA_RAW_TOPIC` is defined twice** in `fda/constants.py` (lines 1 and 8). Harmless, but worth tidying if you touch the file.
- **`usda.scripts.inspect_raw_file` is misnamed** — it hits the live FDC API, not the local file. Keep that in mind when debugging.
- **404 from openFDA refresh** is success, not failure. It means "no new records since watermark".
- **Refresh ran but published 0 records.** On a second invocation with no new data, this is expected — the watermark already covers everything available.
- **The empty PowerShell scripts** at `kafka_full_arch/scripts/create_topics.ps1` / `list_topics.ps1` don't do anything yet. Use the `docker exec` commands above.
- **Don't confuse `common/state_store.py` with `common/metadata_store.py`.** The former is the JSON-file watermark used by FDA today. The latter is a Snowflake-backed replacement (tables `INGESTION_SOURCE_STATE` / `INGESTION_RUN_LOG`) that is greenfield — not yet wired into any refresh producer. When you add run logging, use `metadata_store.py` rather than extending the JSON files.

That's it — after running the FDA walkthrough once end-to-end, the USDA and OpenFoodFacts flows will feel identical. Ping me if anything above is stale or unclear and I'll update the doc.
