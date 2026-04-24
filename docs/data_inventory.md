# Data Inventory

Snapshot: 2026-04-24. What physically exists in Bronze / Silver / Gold / run logs vs. what the code and docs reference.

## 1. Bronze — `gs://mip-bronze-2024/`

| Source | Size | Date partitions present | Layout |
|---|---|---|---|
| `usda` | 4.28 GiB | `2026/04/20`, `2026/04/21`, `2026/04/23` + `usda/bulk/2026/04/21/branded/part_*.jsonl` | daily incremental + one-shot bulk backfill under `bulk/` |
| `off` | 1.31 GiB | `2026/04/09` → `2026/04/22` (14 consecutive days) | `delta_part_*.jsonl` per day |
| `openfda` | 28.63 MiB | `2026/04/20` only | `part_*.jsonl` |
| `esci` | 4.32 GiB | `2024/01/01` (seed) + `2026/04/20` | `part_*.jsonl` |

Watermarks (`gs://mip-bronze-2024/_watermarks/`):

- `usda_watermark.json` → `last_date: 20260424`
- `off_silver_watermark.json` → `last_partition: 2026/04/22` (updated 2026-04-24)
- `off_watermark.json` → `last_ts: 1776930346` (2026-04-24)
- `openfda_silver_watermark.json` → `last_partition: 2026/04/20`
- `openfda_watermark.json` → `last_date: 20260424`
- `esci_silver_watermark.json` → `last_partition: 2026/04/20`
- `part_0000_silver_watermark.json` → `last_partition: 2026/04/21` (stray; not tied to a canonical source)

`test.txt` also present at bucket root (ignore).

## 2. Silver — `gs://mip-silver-2024/`

Layout is inconsistent; documented as `<source>/<YYYY/MM/DD>/` but in practice uses a mix of **source** and **domain alias** prefixes. Both patterns co-exist.

| Prefix | Size | Partitions | Notes |
|---|---|---|---|
| `branded/` | 106.84 MiB | `2026/04/21`, `2026/04/23` | USDA branded domain alias |
| `foundation/` | 35.51 KiB | `2026/04/21`, `2026/04/23` | USDA foundation alias |
| `off/` | 176.62 MiB | `2026/04/21`, `2026/04/22` (`part_0000.parquet` + `sample.parquet`) | |
| `openfda/` | 3.23 MiB | `2026/04/20` | |
| `usda/branded/`, `usda/foundation/` | 54.18 MiB combined | `2026/04/21` | duplicate of top-level branded/foundation (same data, different path) |
| `esci/` | 426.55 MiB | `2026/04/20` | |
| `*_quarantine/` | — | `off_quarantine`, `foundation_quarantine`, `esci_quarantine` | rejects |
| `run-logs/` | — | 20 JSON run logs (earliest `2026-04-21T15:42`) | pipeline telemetry mirror |

⚠️ **Silver layout divergence.** `CLAUDE.md` claims `gs://mip-silver-2024/<source>/<YYYY/MM/DD>/`. Real state has both `branded/2026/04/21/...` and `usda/branded/2026/04/21/...` with the same content. Needs cleanup or doc alignment.

## 3. Gold — BigQuery `mip_gold.products`

- **Total rows:** 1,723,649
- **Distinct `_pipeline_run_id`:** 1 (all NULL)
- **`_source` breakdown:** `glob` = 189,472 rows; `NULL` = 1,534,177 rows
- **`published_date` range:** 2019-04-01 → 2025-12-18
- Schema: unified nutrition schema incl. `duplicate_group_id`, `product_name`, `brand_owner`, `brand_name`, `ingredients`, `serving_size(_unit)`, `published_date`, `_bronze_file`, `_source`, `_pipeline_run_id`, `dq_score_pre`, … (full schema via `bq show --schema mip_gold.products`)

Top 20 `_bronze_file` values all point to `gs://mip-bronze-2024/usda/bulk/2026/04/21/branded/part_*.jsonl` — the Gold table was primarily loaded from the one-shot USDA branded backfill.

⚠️ **Metadata gap.** 89% of Gold rows have NULL `_source` / `_bronze_file` / `_pipeline_run_id`. Provenance columns not populated on most paths. Fix before EDA claims depend on per-source Gold counts.

## 4. Run logs

| Location | Count | Range |
|---|---|---|
| `output/run_logs/` (local) | 1 | `run_20260424T144936_5734fc85.json` — OFF, 2026/04/22 bronze, nutrition domain, 5000 rows in / 7094 out (rows_out > rows_in — chunk expansion from `SPLIT`) |
| `gs://mip-silver-2024/run-logs/` | 20 | `2026-04-21T15:42` → recent |

The single local run log has `dq_score_post: NaN` → `dq_delta: NaN` (enrichment_stats empty, block sequence truncated pre-enrichment). Silver-mode run, not full pipeline.

## 5. Local artifacts (`output/`)

- `output/silver/nutrition/off.parquet` (932K)
- `output/gold/nutrition.parquet` (932K)
- `output/.chunks/` (checkpoint chunks)

## 6. Code-vs-reality cross-check

| Code reference | Reality |
|---|---|
| `gs://mip-bronze-2024/<source>/<YYYY/MM/DD>/*.jsonl` | ✅ for usda/off/openfda/esci |
| `gs://mip-silver-2024/<source>/<YYYY/MM/DD>/` | ⚠️ divergent — uses domain aliases (`branded/`, `foundation/`) at top level |
| `mip_gold.products` BigQuery sink | ✅ present, 1.72M rows |
| `output/run_logs/` populated by `save_output_node` | ⚠️ only 1 local log; canonical store is GCS `run-logs/` |
| Domains: `nutrition / safety / pricing / retail / finance / manufacturing` | only `nutrition` + `safety` (openfda) active in data |

## 7. Recommended EDA anchors

Based on coverage + freshness:

- **USDA (branded)** — biggest Gold footprint; `bulk/2026/04/21/branded/` covers ~189k rows with populated `_source=glob`. Use as Bronze→Silver→Gold end-to-end showcase.
- **OFF** — richest *date* range (14 days); best for freshness / incremental telemetry demos.
- **OpenFDA** — smallest but only cross-domain (`safety`) example.
- **ESCI** — large, 2 dates; **retail** domain showcase + UC3 search corpus.

Locked `(source, date, domain)` tuples for EDA report:

| Source | Date | Domain |
|---|---|---|
| `usda` | `2026/04/21` | nutrition — bulk backfill, best Gold correlation |
| `off` | `2026/04/22` | nutrition — matches existing local run log |
| `openfda` | `2026/04/20` | safety |
| `esci` | `2026/04/20` | retail |

## 8. Known gaps (flagged, not fixing now)

- Gold `_source`/`_bronze_file`/`_pipeline_run_id` NULL on ~89% of rows.
- Silver layout: `branded/` and `usda/branded/` duplicates.
- Single local run log; primary log store is `gs://mip-silver-2024/run-logs/`.
- Only `nutrition` + `safety` + `retail` domains have actual data; `pricing`, `finance`, `manufacturing` schemas exist but no Bronze.
