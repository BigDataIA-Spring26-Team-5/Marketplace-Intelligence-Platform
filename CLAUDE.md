# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

```bash
# Install (Poetry, Python ^3.11)
poetry install

# API keys — see .env.example (DEEPSEEK_API_KEY, ANTHROPIC_API_KEY, GROQ_API_KEY)
cp .env.example .env

# Full platform stack (Kafka, Airflow, Postgres, Prometheus, Pushgateway, Grafana,
# ChromaDB, Redis, MLflow) — see docker-compose.yml at repo root
docker-compose -p mip up -d

# CLI demo — runs 3 pipeline passes (USDA → FDA → FDA replay)
# CLI demo — runs 3 passes (USDA → FDA → FDA replay) showcasing YAML cache hits
poetry run python demo.py
poetry run python demo.py --no-cache        # bypass Redis
poetry run python demo.py --flush-cache     # clear pipeline cache keys first

# Primary CLI with checkpoint/resume — local CSV or gs:// URIs
poetry run python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
poetry run python -m src.pipeline.cli --source data/fda_recalls_sample.csv --resume
poetry run python -m src.pipeline.cli --source data/usda_sample_raw.csv --force-fresh
poetry run python -m src.pipeline.cli --source gs://mip-bronze-2024/usda/2026/04/20/*.jsonl --mode silver
poetry run python -m src.pipeline.cli --source ... --with-critic   # enable Agent 2 (off by default)

# Gold layer (Silver Parquet on GCS → BigQuery mip_gold.products) — separate entry point
poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21

# Streamlit wizard (HITL gates + Observability chatbot)
poetry run streamlit run app.py

# MCP observability server (FastAPI, Redis-cached, port 8001)
uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001

# REST API (UC1 pipeline + UC2 observability + UC3 search + UC4 recommendations, port 8002)
# Swagger UI: http://localhost:8002/docs — all endpoints versioned under /v1/
uvicorn src.api.main:app --host 0.0.0.0 --port 8002

# (One-time) Build the KNN enrichment corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py
poetry run python scripts/build_corpus.py --limit 10000

# Tests
poetry run pytest
poetry run pytest tests/uc2_observability/test_log_writer.py::test_name
poetry run pytest tests/unit/test_cache_client.py
poetry run pytest tests/integration/test_cache_pipeline.py
# (One-time) build FAISS KNN corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py
poetry run python scripts/build_corpus.py --limit 10000

# Full platform stack (Kafka, Airflow, Postgres, Prometheus, Pushgateway, Grafana, ChromaDB, Redis, MLflow)
docker-compose -p mip up -d

# Tests
poetry run pytest
poetry run pytest -m "not integration"                         # skip GCS-dependent tests
poetry run pytest tests/unit/test_cache_client.py::test_name
```

`demo.py` expects `data/usda_fooddata_sample.csv` and `data/fda_recalls_sample.csv`. The `data/` and `output/` directories are **gitignored** — CSVs must be placed there before running.

## Architecture — the load-bearing ideas

This is a **three-agent LangGraph pipeline** that ingests heterogeneous food-product datasets (CSV locally, JSONL from GCS Bronze), detects schema gaps, synthesizes declarative YAML transforms, and produces a unified catalog with DQ scores and cascading enrichment. The README is the longer tour; what follows captures what is non-obvious from reading the code.

### The graph is the control flow

[src/agents/graph.py](src/agents/graph.py) builds a `StateGraph` with **7 nodes**:

```
load_source → analyze_schema → [critique_schema?] → check_registry → plan_sequence → run_pipeline → save_output
```

- **Agent 1** (`analyze_schema`, orchestrator): maps source → unified schema, emits `RENAME`/`CAST`/`FORMAT`/`DELETE`/`ADD`/`SPLIT`/`UNIFY`/`DERIVE` operations. Default model: `claude-sonnet-4-5`.
- **Agent 2** (`critique_schema`, critic): **off by default** — `--with-critic` or `state["with_critic"]` enables it. Uses a reasoning model (`anthropic/claude-sonnet-4-6`). `route_after_analyze_schema` skips this node when `cache_yaml_hit` is set (Redis returned a complete cached mapping) or when `with_critic` is false.
- **Agent 3** (`plan_sequence`): reorders the block pool. **It can only reorder — it cannot add or remove blocks.** `plan_sequence_node` appends any block the LLM dropped back into the sequence (before `dq_score_post`) before returning.

`NODE_MAP` and `run_step(step_name, state)` are how [app.py](app.py) executes one node at a time with HITL gates in between. If you add a node, register it in `NODE_MAP` too or the Streamlit wizard won't see it.

State flows through `PipelineState` ([src/agents/state.py](src/agents/state.py)), a `TypedDict(total=False)` — most fields are absent at most nodes, so **never assume a key exists**.

### No runtime Python code generation — YAML-only is a constitutional constraint

Per the README's contributor notes: **generated transforms live as YAML**, not Python. `function_registry/registry.json` is vestigial (empty `[]`). When Agent 1 detects a schema gap, operations are serialized to `src/blocks/generated/<domain>/DYNAMIC_MAPPING_<source_stem>.yaml` via `src/blocks/mapping_io.py`. On subsequent runs, [src/registry/block_registry.py](src/registry/block_registry.py) `_discover_generated_blocks()` loads each YAML as a `DynamicMappingBlock` ([src/blocks/dynamic_mapping.py](src/blocks/dynamic_mapping.py)) which executes a fixed set of declarative actions (`set_null`, `type_cast`, `rename`, `coalesce`, `concat_columns`, `json_array_extract_multi`, `regex_replace`, etc.). Do not reintroduce runtime-generated Python — `src/agents/sandbox.py` no longer exists, and the YAML action set is the supported surface.

### Redis cache short-circuits Agents 1-3

[src/cache/client.py](src/cache/client.py) is a Redis wrapper with a **SQLite fallback** (`output/cache.db`, WAL-mode) when Redis is unavailable. Prefixes and TTLs:

| Prefix | TTL | Contents |
|--------|-----|----------|
| `yaml`  | 30 days | Complete cacheable mapping (column_mapping, operations, gaps, block_sequence, full YAML text) — keyed on schema fingerprint |
| `llm`   | 7 days  | S3 enrichment LLM responses |
| `emb`   | 30 days | Sentence-transformers embeddings |
| `dedup` | 14 days | Fuzzy dedup cluster keys |

A `yaml` cache hit sets `cache_yaml_hit` in state, which causes `route_after_analyze_schema` to skip Agent 2 entirely. `plan_sequence_node` writes the **full** cacheable blob (including the YAML text and the planned sequence) — the cache entry is only complete after all three agents have run. **If you change what Agent 1/2/3 produces, update the write site in `plan_sequence_node` too** or cache hits will replay stale state.

### Block sequence has a `"__generated__"` sentinel

[src/registry/block_registry.py](src/registry/block_registry.py) reads `domain_packs/<domain>/block_sequence.yaml` and returns the ordered list for that domain, containing the sentinel `"__generated__"`. [src/pipeline/runner.py](src/pipeline/runner.py) `PipelineRunner` replaces the sentinel with the `DynamicMappingBlock` for the current source at execution time. One composite name expands at runtime: `dedup_stage` → `fuzzy_deduplicate, column_wise_merge, golden_record_select`. `enrich_stage` is removed — domain packs list enrichment blocks individually. Don't remove the sentinel or reorder around it without understanding what is being injected where.

### Domain Packs

`domain_packs/<domain>/` is the canonical location for all domain-specific configuration. Adding a new domain requires **zero edits to `src/`**:

- `block_sequence.yaml` — ordered block names (supports `sequence`, `silver_sequence`, `gold_sequence` keys)
- `enrichment_rules.yaml` — S1 deterministic patterns and S3 LLM rules per field
- `prompt_examples.yaml` — few-shot column mapping examples injected into Agent 1's prompt
- `custom_blocks/*.py` — domain-specific `Block` subclasses; auto-discovered at registry init and registered as `<domain>__<block.name>`

`EnrichmentRulesLoader(domain)` in `src/enrichment/rules_loader.py` reads `enrichment_rules.yaml` and exposes `s1_fields` / `llm_fields` / `safety_field_names()` consumed by `LLMEnrichBlock`.

### Pipeline modes change the block sequence

`--mode` / `state["pipeline_mode"]` selects one of three shapes:

- **`full`** (default): sequence from `domain_packs/<domain>/block_sequence.yaml` (e.g. nutrition: `dq_score_pre → __generated__ → cleaning → dedup_stage → nutrition__extract_allergens → llm_enrich → dq_score_post`). Output: CSV to `output/`.
- **`silver`**: schema transform only via `get_silver_sequence()` — **no dedup, no enrichment**. Output: Parquet to `gs://mip-silver-2024/<source>/<YYYY/MM/DD>/`. `save_output_node` also updates the watermark.
- **`gold`**: invoked via `src/pipeline/gold_pipeline.py` (separate entry point, not the graph). Reads all Silver Parquet for a `source+date`, runs dedup + enrichment + DQ scoring, appends to BigQuery `mip_gold.products`.

### Column mapping happens before blocks run

`PipelineRunner.run()` applies `column_mapping` (source → unified column names) **before** iterating the block sequence. Every block reads unified column names (`product_name`, `brand_name`, `ingredients`, etc.), regardless of which source produced the DataFrame. If you're adding a block that reads a raw source column, you're doing it at the wrong layer — the mapping step is the boundary.

### Chunked streaming is the default

`PipelineRunner.run_chunked()` drives execution through [src/utils/csv_stream.py](src/utils/csv_stream.py) `CsvStreamReader` at `DEFAULT_CHUNK_SIZE` (10K rows). `run_pipeline_node` always calls `run_chunked`, never `run` directly — the in-memory `run()` exists for tests and small synthetic inputs.

### Enrichment is a cost cascade with a hard safety rule

[src/blocks/llm_enrich.py](src/blocks/llm_enrich.py) orchestrates three tiers:

1. **S1 deterministic** ([src/enrichment/deterministic.py](src/enrichment/deterministic.py)) — regex/keyword extraction
2. **S2 KNN corpus** ([src/enrichment/embedding.py](src/enrichment/embedding.py)) — FAISS product-to-product similarity
3. **S3 RAG-LLM** ([src/enrichment/llm_tier.py](src/enrichment/llm_tier.py)) — LLM with top-3 S2 neighbors as RAG context

**Hard safety rule: S2 and S3 touch only `primary_category`.** The safety fields `allergens`, `is_organic`, `dietary_tags` are **extraction-only** (S1 from the product's own text) or null. They are never inferred by KNN similarity or the LLM, because dangerous false positives (e.g., tagging a barley-containing product "gluten-free") are worse than nulls. `LLMEnrichBlock` has a post-run assertion that warns if any S3-resolved row has a safety field that differs from its post-S1 state — **if that fires, something upstream broke the invariant; fix it rather than silencing the warning**.

S2's `_knn_neighbors` column is a pipeline-internal JSON string consumed only by S3. `LLMEnrichBlock` drops it from the DataFrame before returning — don't write output code that expects it to be present.

### Corpus persists across runs (feedback loop)

[src/enrichment/corpus.py](src/enrichment/corpus.py) manages a persistent FAISS `IndexFlatIP` (inner product on L2-normalized vectors = cosine similarity) at `corpus/faiss_index.bin` + `corpus/corpus_metadata.json`. Seeded by `scripts/build_corpus.py` (USDA FoodData Central) or bootstrapped in-run from S1-resolved rows if the persistent corpus has fewer than `MIN_CORPUS_SIZE` (10) vectors. **Both S2 and S3 add resolved rows back into the corpus**, so later runs get better. `.bin` is gitignored; `corpus_metadata.json` / `corpus_summary.json` are committed. If `faiss-cpu` is not installed, S2 is skipped with a warning and everything falls through to S3 — **do not treat missing FAISS as a hard error**.

Key thresholds in `corpus.py`: `VOTE_SIMILARITY_THRESHOLD=0.45`, `CONFIDENCE_THRESHOLD_CATEGORY=0.60`, `K_NEIGHBORS=5`.

### Domain schemas are the canonical target per-run

`config/schemas/<domain>_schema.json` is the canonical target schema for each domain (`nutrition`, `safety`, `pricing`). The domain is always operator-supplied via `--domain <domain>` (CLI) or `PipelineState["domain"]` (Streamlit) — it is never auto-inferred. On the **first run** for a new source, Agent 1 uses `FIRST_RUN_SCHEMA_PROMPT` to derive unified column names, then [src/schema/analyzer.py](src/schema/analyzer.py) `derive_unified_schema_from_source()` writes to the correct domain schema file — including auto-added enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns (`dq_score_pre`, `dq_score_post`, `dq_delta`). On **subsequent runs**, Agent 1 uses `SCHEMA_ANALYSIS_PROMPT` which excludes enrichment and computed columns from the mappable set. After block execution, `_silver_normalize()` enforces exact domain schema column set/order and writes `output/silver/<domain>/<source_name>.parquet`. Gold output is rebuilt each run by concatenating all Silver parquets for the domain into `output/gold/<domain>.parquet`. `config/unified_schema.json` is retired — do not load it.
[config/unified_schema.json](config/unified_schema.json) is the canonical target schema. On the **first run** (file absent), Agent 1 uses `FIRST_RUN_SCHEMA_PROMPT` and [src/schema/analyzer.py](src/schema/analyzer.py) `derive_unified_schema_from_source()` writes the JSON — including auto-added enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns (`dq_score_pre`, `dq_score_post`, `dq_delta`). On **subsequent runs**, Agent 1 uses `SCHEMA_ANALYSIS_PROMPT` which explicitly **excludes enrichment and computed columns from the mappable set** — those aren't sourceable. If you add a computed or enrichment column, extend both `derive_unified_schema_from_source()` **and** the exclusion filter in `analyze_schema_node`, otherwise the LLM will be asked to map columns that don't come from the source.

### LLM routing is centralized and multi-provider

[src/models/llm.py](src/models/llm.py) wraps [LiteLLM](https://github.com/BerriAI/litellm) with five getters, each overridable via env var:

| Getter | Default model | Env var |
|--------|--------------|---------|
| `get_orchestrator_llm` (Agent 1, Agent 3) | `claude-sonnet-4-5` | `ORCHESTRATOR_LLM` |
| `get_critic_llm` (Agent 2) | `anthropic/claude-sonnet-4-6` | `CRITIC_LLM` |
| `get_codegen_llm` (legacy, unused by graph) | `deepseek/deepseek-chat` | `CODEGEN_LLM` |
| `get_enrichment_llm` (S3 RAG) | `groq/llama-3.3-70b-versatile` | `ENRICHMENT_LLM` |
| `get_observability_llm` (UC2 chatbot) | `groq/llama-3.1-8b-instant` | `OBSERVABILITY_LLM` |

Swap models here or via env, not at call sites. `call_llm_json()` parses responses and has a markdown-fence fallback for models that wrap JSON in ` ```json ... ``` `.

`src/models/llm.py` is also the **UC2 import gateway** — `_UC2_AVAILABLE`, `_emit_event`, and `_MetricsCollector` are exported from there. All other files import UC2 symbols from `src.models.llm`, not directly from `src.uc2_observability`. This guards against import failures when UC2 deps are absent.

### UC2 observability layer is fully implemented

`src/uc2_observability/` is all working code. The two placeholder files (`anomaly_detection.py`, `dashboard.py`) exist only as re-export shims or stubs — the real implementation lives in the files listed below:

- `log_writer.py` — `RunLogWriter`: writes atomic JSON run logs to `output/run_logs/` after every pipeline run. Called from `save_output_node`.
- `log_store.py` — `RunLogStore`: read-only query interface (`load_all`, `filter`, `get_by_run_id`, `summary_stats`) over the persisted JSON logs.
- `rag_chatbot.py` — `ObservabilityChatbot`: structured retrieval + LLM synthesis answering natural-language questions about run history. Returns `ChatResponse(answer, cited_run_ids, context_run_count)`.
- `metrics_exporter.py` — `MetricsExporter`: pushes 12 labelled Prometheus gauges to Pushgateway (`:9091`). Uses isolated `CollectorRegistry`; never raises on network failure.
- `metrics_collector.py` — `MetricsCollector`: in-process collector called by `_emit_event` at each pipeline event.
- `anomaly_detector.py` — `AnomalyDetector`: Isolation Forest on Prometheus metrics for the last N runs per source; pushes `etl_anomaly_flag=1` to Pushgateway and writes to Postgres `anomaly_reports` table. Called after each `run_completed` event and on an hourly schedule.
- `chunker.py` — reads new `audit_events` rows from Postgres since last cursor, embeds with `all-MiniLM-L6-v2`, upserts into ChromaDB collection `audit_corpus`. Runs as a 5-minute sleep loop.
- `kafka_to_pg.py` — Kafka → Postgres consumer. Demuxes `pipeline.events` topic by `event_type` into four tables: `audit_events`, `block_trace`, `quarantine_rows`, `dedup_clusters`. Reconnects with exponential back-off.
- `mcp_server.py` — FastAPI app on `:8001` with 7 MCP-style tool endpoints backed by Prometheus, Postgres, and Redis (15s TTL for Prometheus queries, 30s for Postgres).
- `streamlit_app.py` — standalone Streamlit UI for observability (separate from `app.py`'s sidebar mode).
- `dashboard.py` — still placeholder (raises `NotImplementedError`).

`app.py` has a sidebar Mode radio ("Pipeline" / "Observability"). Observability mode renders `_render_observability_page()` with multi-turn chat UI, refresh button, and cited run ID expanders.

Postgres schema (UC2, `localhost:5432`, db `uc2`, user `mip`/`mip_pass`): tables `audit_events`, `block_trace`, `quarantine_rows`, `dedup_clusters`, `anomaly_reports`.

### Airflow DAG pipeline

`airflow/dags/` contains the production orchestration. All DAGs mount `src/` and `config/` from the repo root into the Airflow container (see `docker-compose.yml` volumes). Daily schedule chain (all UTC):

| DAG | Schedule | What it does |
|---|---|---|
| `usda_incremental_dag` / `off_incremental_dag` / `openfda_incremental_dag` | 02:00–05:00 | Ingest source → GCS Bronze JSONL (`gs://mip-bronze-2024/`) |
| `bronze_to_bq_dag` | 03:00–06:00 | Load Bronze JSONL → BigQuery staging tables |
| `bronze_to_silver_dag` | 07:00 | Watermark-gated: reads new Bronze partitions → UC1 ETL → GCS Silver Parquet (`gs://mip-silver-2024/`). Watermarks stored at `gs://mip-bronze-2024/_watermarks/{source}_silver_watermark.json`. |
| `silver_to_gold_dag` | 09:00 | ExternalTaskSensor waits for bronze_to_silver. Silver Parquet → dedup + enrichment → BigQuery `mip_gold.products` (append mode). |
| `uc2_anomaly_dag` | Hourly | Isolation Forest on UC1 Prometheus metrics; needs ≥5 completed runs per source. |
| `uc2_chunker_dag` | Every 5 min | Postgres audit_events → ChromaDB embeddings. |
| `esci_dag` | Manual | ESCI product dataset ingestion. |
| `usda_dag` | Manual | Full USDA backfill. |

Airflow UI: `http://localhost:8080` (admin / admin).

### Kafka and GCS sink

The pipeline emits events to Kafka topic `pipeline.events`. `src/consumers/kafka_gcs_sink.py` replaces the Kafka Connect S3 Sink for Bronze ingestion — runs as `python -m src.consumers.kafka_gcs_sink --topic <topic> --prefix <prefix>`, writes JSONL part files to GCS, flushing every `FLUSH_SIZE` records. `src/producers/` contains `openfda_producer.py` and `off_producer.py` for source-specific Kafka producers.

### Redis embedding cache

`src/cache/client.py` wraps Redis (`localhost:6379`) with numpy serialization for embedding vectors. `NULL_RATE_COLUMNS` constant in `src/pipeline/runner.py` controls which columns get null-rate stats in `block_end` Kafka events. SQLite at `output/llm_cache.db` is the fallback when Redis is unavailable.

### GCS / BigQuery data flow

- Bronze: `gs://mip-bronze-2024/` (JSONL, partitioned by source + date)
- Silver: `gs://mip-silver-2024/` (Parquet, partitioned by domain + source)
- Gold CLI: `python -m src.pipeline.gold` (local Silver → local Gold Parquet)
- Gold BQ: `mip_gold.products` (BigQuery, schema auto-detected, append mode via Airflow)
### Checkpoint/resume is SQLite-backed

[src/pipeline/checkpoint/manager.py](src/pipeline/checkpoint/manager.py) stores run state in `checkpoints.db` (SHA256 of source file, schema version, completed chunks, plan YAML, corpus snapshots). The CLI `--resume` validates the SHA256 before rehydrating; `--force-fresh` clears all checkpoint rows. For **GCS sources** the source file doesn't exist on disk, so `src/pipeline/cli.py` `_create_gcs_checkpoint()` inserts directly into the SQLite with the URI's SHA256 instead of the file's.

### UC2 observability

[src/uc2_observability/](src/uc2_observability/) contains both the run-log persistence layer and the live Kafka/Prometheus emission path:

- `log_writer.py` — `RunLogWriter`: atomic JSON run logs to `output/run_logs/` after every run (success or partial). Called from `save_output_node`.
- `log_store.py` — `RunLogStore`: read-only query interface over the persisted logs.
- `rag_chatbot.py` — `ObservabilityChatbot`: structured retrieval + LLM synthesis over run history; returns `ChatResponse(answer, cited_run_ids, context_run_count)`.
- `metrics_exporter.py` — `MetricsExporter`: pushes 12 labelled Prometheus gauges to Pushgateway (`localhost:9091`) using an isolated `CollectorRegistry`; **never raises on network failure**.
- `metrics_collector.py` — legacy `MetricsCollector` kept alongside the newer `MetricsExporter`; both are called from `save_output_node`.
- `kafka_to_pg.py`, `chunker.py`, `anomaly_detector.py`, `mcp_server.py`, `streamlit_app.py` — downstream Kafka consumers and RAG-over-logs infrastructure.
- `anomaly_detection.py`, `dashboard.py` — still placeholder (`NotImplementedError`).

UC2 event emission from the graph (`run_started`, `run_completed`, `block_start`, `block_end`, `quarantine`, `dedup_cluster`) goes through `_emit_event` / `_UC2_AVAILABLE` / `_MetricsCollector` **re-exported from `src/models/llm.py`** — this is the one import shim every other file uses. All UC2 emits are wrapped in `try/except` and logged as warnings on failure; **pipeline runs must not be blocked by observability outages**.

`app.py` has a sidebar Mode radio (Pipeline / Observability). `grafana/docker-compose.yml` runs a local Prometheus + Pushgateway + Grafana stack; the full platform `docker-compose.yml` at the repo root adds Kafka, Airflow, Postgres, ChromaDB, Redis, and MLflow. Service endpoints live in [ENDPOINTS.md](ENDPOINTS.md).

### UC3 / UC4 are scaffolding only

[src/uc3_search/](src/uc3_search/) and [src/uc4_recommendations/](src/uc4_recommendations/) contain **placeholder classes that all raise `NotImplementedError`**. They are not wired into `demo.py`, `app.py`, the graph, or the CLI. Don't assume they work.

### Airflow orchestration

[airflow/dags/](airflow/dags/) contains the DAGs that schedule the data flow: bronze ingesters (`usda_dag`, `openfda_incremental_dag`, `off_incremental_dag`, `esci_dag`), `bronze_to_silver_dag`, `silver_to_gold_dag`, `bronze_to_bq_dag`, plus UC2 DAGs (`uc2_chunker_dag`, `uc2_anomaly_dag`). They call the CLI / `gold_pipeline` rather than importing the graph directly.

## Things to double-check before editing

- **Registry key determinism** — `FunctionRegistry.save()` preserves `used_count` on updates by design; if you rewrite the save logic, keep that preservation or the "pipeline remembered" telemetry gets reset every run.
- **Block `audit_entry()` signature** — every block extends `src/blocks/base.py:Block` and must return `{block, rows_in, rows_out, ...}` from `audit_entry()`. The UI's waterfall and `demo.py`'s trace both read those fields by name.
- **`run_step` vs `invoke`** — the Streamlit UI calls `run_step(step_name, state)` to execute one node; `demo.py` uses `graph.invoke()` to run the whole graph. State shape must remain compatible with both paths.
- **UC2 imports always go through `src.models.llm`** — never import `_emit_event` or `_MetricsCollector` directly from `src.uc2_observability`; the import guard in `llm.py` is what keeps things safe when UC2 deps are absent.
- **Don't touch `final_project/`** when working on the ETL pipeline — it's a fully separate project with its own dependencies and conventions, and its own CLAUDE.md is the authoritative guide for work in that tree.
- **Block `audit_entry()` signature** — every block extends [src/blocks/base.py](src/blocks/base.py):`Block` and must return `{block, rows_in, rows_out, ...}` from `audit_entry()`. The UI waterfall and `demo.py`'s trace both read those fields by name.
- **`run_step` vs `invoke`** — Streamlit calls `run_step(step_name, state)`; `demo.py`/CLI use `graph.invoke()`. State shape must remain compatible with both paths.
- **YAML cache writer coherence** — `plan_sequence_node` is where the full cacheable blob is written. If you add fields that Agent 1/2/3 produce, extend the `cacheable` dict there, or replayed runs will silently drop them.
- **UC2 emits are best-effort** — wrap new emits in `try/except` and log warnings; don't raise. Observability must not block the pipeline.
- **Safety boundary in enrichment** — `allergens`, `dietary_tags`, `is_organic` are S1-only. Never add them to S2/S3 output paths.

## Active Technologies
- Python 3.11 (Poetry) + LangGraph 0.4, pandas 2.2, PyYAML, pathlib (stdlib), importlib (stdlib) (016-kernel-domain-separation)
- `domain_packs/<domain>/` filesystem directory; `config/schemas/<domain>_schema.json` for domain schema; `src/blocks/generated/<domain>/` for YAML mapping files (016-kernel-domain-separation)
- Python 3.11 (Poetry) + FastAPI (already in stack via mcp_server.py), Uvicorn, Pydantic v2 (016-kernel-domain-separation)
- SQLite via CheckpointManager (run state); Postgres (observability queries); Redis + SQLite fallback (cache) (016-kernel-domain-separation)

- Python 3.11 (Poetry). pandas 2.2, LangGraph 0.4, LiteLLM 1.55, FAISS-CPU, sentence-transformers, rapidfuzz, pyarrow, redis-py, streamlit, structlog, prometheus_client, chromadb, networkx, mlxtend, rank-bm25.
- Redis at `localhost:6379` (SQLite fallback at `output/cache.db`).
- GCS buckets: `mip-bronze-2024` (JSONL), `mip-silver-2024` (Parquet), BigQuery `mip_gold.products`.
- Prometheus Pushgateway at `localhost:9091`; Grafana at `localhost:3000`.

## Recent Changes
- 016-kernel-domain-separation: Added Python 3.11 (Poetry) + LangGraph 0.4, pandas 2.2, PyYAML, pathlib (stdlib), importlib (stdlib)
