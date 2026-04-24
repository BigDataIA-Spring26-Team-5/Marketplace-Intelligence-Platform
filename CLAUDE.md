# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

```bash
# Install (Poetry, Python ^3.11)
poetry install

# API keys — see .env.example (ANTHROPIC_API_KEY, DEEPSEEK_API_KEY, GROQ_API_KEY)
cp .env.example .env

# Full platform stack (Kafka, Airflow, Postgres, Prometheus, Pushgateway, Grafana,
# ChromaDB, Redis, MLflow) — see docker-compose.yml at repo root
docker-compose -p mip up -d

# CLI demo — 3 pipeline passes (USDA → FDA → FDA replay), showcases YAML cache hits
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

# (One-time) Build FAISS KNN enrichment corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py
poetry run python scripts/build_corpus.py --limit 10000

# Tests
poetry run pytest
poetry run pytest -m "not integration"                          # skip GCS-dependent
poetry run pytest tests/uc2_observability/test_log_writer.py::test_name
poetry run pytest tests/unit/test_cache_client.py
poetry run pytest tests/integration/test_cache_pipeline.py

# Lint
cd src && ruff check .
```

`demo.py` expects `data/usda_fooddata_sample.csv` and `data/fda_recalls_sample.csv`. `data/` and `output/` are gitignored — place CSVs there before running.

## Architecture — load-bearing ideas

Three-agent LangGraph pipeline. Ingests heterogeneous food-product datasets (CSV local, JSONL from GCS Bronze), detects schema gaps, synthesizes declarative YAML transforms, produces unified catalog with DQ scores + cascading enrichment. README is the longer tour; below captures what is non-obvious from reading code.

### Graph is the control flow

[src/agents/graph.py](src/agents/graph.py) builds a `StateGraph` with 7 nodes:

```
load_source → analyze_schema → [critique_schema?] → check_registry → plan_sequence → run_pipeline → save_output
```

- **Agent 1** (`analyze_schema`, orchestrator): maps source → unified schema; emits `RENAME`/`CAST`/`FORMAT`/`DELETE`/`ADD`/`SPLIT`/`UNIFY`/`DERIVE` ops. Default: `deepseek/deepseek-chat`.
- **Agent 2** (`critique_schema`, critic): **off by default** — `--with-critic` or `state["with_critic"]` enables. Reasoning model (`anthropic/claude-sonnet-4-6`). `route_after_analyze_schema` skips this node when `cache_yaml_hit` is set (Redis returned complete cached mapping) or `with_critic` is false.
- **Agent 3** (`plan_sequence`): reorders block pool. **Reorder only — cannot add or remove blocks.** `plan_sequence_node` appends any block the LLM dropped back into the sequence (before `dq_score_post`) before returning.

`NODE_MAP` and `run_step(step_name, state)` are how [app.py](app.py) executes one node at a time with HITL gates between. New node → register in `NODE_MAP` too, else Streamlit wizard won't see it.

State flows through `PipelineState` ([src/agents/state.py](src/agents/state.py)) — a `TypedDict(total=False)`. Most fields are absent at most nodes; **never assume a key exists**.

### No runtime Python code generation — YAML-only is a constitutional constraint

Generated transforms live as YAML, not Python. `function_registry/registry.json` is vestigial (empty `[]`). When Agent 1 detects a gap, ops are serialized to `src/blocks/generated/<domain>/DYNAMIC_MAPPING_<source_stem>.yaml` via `src/blocks/mapping_io.py`. On later runs, [src/registry/block_registry.py](src/registry/block_registry.py) `_discover_generated_blocks()` loads each YAML as a `DynamicMappingBlock` ([src/blocks/dynamic_mapping.py](src/blocks/dynamic_mapping.py)) executing a fixed declarative action set (`set_null`, `type_cast`, `rename`, `coalesce`, `concat_columns`, `json_array_extract_multi`, `regex_replace`, ...). Do NOT reintroduce runtime-generated Python — `src/agents/sandbox.py` no longer exists; the YAML action set is the supported surface.

### Redis cache short-circuits Agents 1–3

[src/cache/client.py](src/cache/client.py) — Redis wrapper with SQLite fallback (`output/cache.db`, WAL) when Redis unavailable.

| Prefix | TTL | Contents |
|--------|-----|----------|
| `yaml`  | 30d | Complete cacheable mapping (column_mapping, operations, gaps, block_sequence, full YAML text) — keyed on schema fingerprint |
| `llm`   | 7d  | S3 enrichment LLM responses |
| `emb`   | 30d | sentence-transformers embeddings |
| `dedup` | 14d | Fuzzy dedup cluster keys |

A `yaml` cache hit sets `cache_yaml_hit` in state → `route_after_analyze_schema` skips Agent 2. `plan_sequence_node` writes the **full** cacheable blob (YAML text + planned sequence) — entry only complete after all 3 agents run. **Change what Agent 1/2/3 produces → update write site in `plan_sequence_node`** or cache hits replay stale state.

### Block sequence has a `"__generated__"` sentinel

[src/registry/block_registry.py](src/registry/block_registry.py) `get_default_sequence(domain, unified_schema, enable_enrichment)` returns ordered list with sentinel `"__generated__"`. [src/pipeline/runner.py](src/pipeline/runner.py) `PipelineRunner` replaces sentinel with the `DynamicMappingBlock` for current source at execution time. Two composite names expand at runtime: `dedup_stage` → `fuzzy_deduplicate, column_wise_merge, golden_record_select`; `enrich_stage` → `extract_allergens, llm_enrich`. Don't remove sentinel or reorder around it without understanding what gets injected where.

### Pipeline modes change the block sequence

`--mode` / `state["pipeline_mode"]`:

- **`full`** (default): `dq_score_pre → __generated__ → cleaning → dedup_stage → enrich_stage → dq_score_post`. Output: CSV to `output/`.
- **`silver`**: schema transform only via `get_silver_sequence()` — **no dedup, no enrichment**. Output: Parquet to `gs://mip-silver-2024/<source>/<YYYY/MM/DD>/`. `save_output_node` also updates watermark.
- **`gold`**: invoked via `src/pipeline/gold_pipeline.py` (separate entry point, not the graph). Reads all Silver Parquet for `source+date`, runs dedup + enrichment + DQ scoring, appends to BigQuery `mip_gold.products`.

### Column mapping happens before blocks run

`PipelineRunner.run()` applies `column_mapping` (source → unified names) **before** iterating block sequence. Every block reads unified names (`product_name`, `brand_name`, `ingredients`, ...) regardless of source. Adding a block that reads a raw source column = wrong layer; mapping step is the boundary.

### Chunked streaming is the default

`PipelineRunner.run_chunked()` drives via [src/utils/csv_stream.py](src/utils/csv_stream.py) `CsvStreamReader` at `DEFAULT_CHUNK_SIZE` (10K rows). `run_pipeline_node` always calls `run_chunked`, never `run` directly — `run()` exists for tests and small synthetic inputs.

### Enrichment is a cost cascade with a hard safety rule

[src/blocks/llm_enrich.py](src/blocks/llm_enrich.py) orchestrates three tiers:

1. **S1 deterministic** ([src/enrichment/deterministic.py](src/enrichment/deterministic.py)) — regex/keyword
2. **S2 KNN corpus** ([src/enrichment/embedding.py](src/enrichment/embedding.py)) — FAISS product-to-product similarity
3. **S3 RAG-LLM** ([src/enrichment/llm_tier.py](src/enrichment/llm_tier.py)) — LLM with top-3 S2 neighbors as context

**Hard safety rule: S2 and S3 touch only `primary_category`.** Safety fields `allergens`, `is_organic`, `dietary_tags` are extraction-only (S1 from product's own text) or null. Never inferred by KNN or LLM — false positives (e.g., "gluten-free" for a barley product) worse than nulls. `LLMEnrichBlock` post-run assertion warns if any S3-resolved row has safety field differing from post-S1 state. **If it fires, something upstream broke the invariant — fix it, don't silence the warning.**

S2's `_knn_neighbors` column is pipeline-internal JSON consumed only by S3. `LLMEnrichBlock` drops it before returning — don't write output code expecting it.

### Corpus persists across runs (feedback loop)

[src/enrichment/corpus.py](src/enrichment/corpus.py) manages persistent FAISS `IndexFlatIP` (inner product on L2-normalized vectors = cosine) at `corpus/faiss_index.bin` + `corpus/corpus_metadata.json`. Seeded by `scripts/build_corpus.py` (USDA FoodData Central) or bootstrapped in-run from S1-resolved rows if persistent corpus has fewer than `MIN_CORPUS_SIZE` (10) vectors. **Both S2 and S3 add resolved rows back into corpus** → later runs improve. `.bin` gitignored; `corpus_metadata.json` / `corpus_summary.json` committed. If `faiss-cpu` missing, S2 is skipped with warning, falls through to S3 — **don't treat missing FAISS as hard error**.

Thresholds: `VOTE_SIMILARITY_THRESHOLD=0.45`, `CONFIDENCE_THRESHOLD_CATEGORY=0.60`, `K_NEIGHBORS=5`.

### Domain schemas are the canonical target per-run

`config/schemas/<domain>_schema.json` is canonical target per domain (`nutrition`, `safety`, `pricing`, `retail`, `finance`, `manufacturing`). Domain is always operator-supplied via `--domain <domain>` (CLI) or `PipelineState["domain"]` (Streamlit) — never auto-inferred. **First run** for a new source: Agent 1 uses `FIRST_RUN_SCHEMA_PROMPT`, [src/schema/analyzer.py](src/schema/analyzer.py) `derive_unified_schema_from_source()` writes the domain schema — including auto-added enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns (`dq_score_pre`, `dq_score_post`, `dq_delta`). **Subsequent runs**: Agent 1 uses `SCHEMA_ANALYSIS_PROMPT` which excludes enrichment + computed columns from mappable set. After block execution, `_silver_normalize()` enforces exact domain schema column set/order, writes `output/silver/<domain>/<source_name>.parquet`. Gold rebuilt each run by concatenating all Silver parquets for domain → `output/gold/<domain>.parquet`. **`config/unified_schema.json` is retired — do not load it.**

Add a computed or enrichment column → extend both `derive_unified_schema_from_source()` **and** exclusion filter in `analyze_schema_node`, else LLM gets asked to map columns that don't come from source.

### LLM routing is centralized and multi-provider

[src/models/llm.py](src/models/llm.py) wraps LiteLLM with 5 getters, each env-overridable:

| Getter | Default | Env var |
|--------|---------|---------|
| `get_orchestrator_llm` (Agents 1, 3) | `deepseek/deepseek-chat` | `ORCHESTRATOR_LLM` |
| `get_critic_llm` (Agent 2) | `anthropic/claude-sonnet-4-6` | `CRITIC_LLM` |
| `get_codegen_llm` (legacy, unused by graph) | `deepseek/deepseek-chat` | `CODEGEN_LLM` |
| `get_enrichment_llm` (S3 RAG) | `claude-haiku-4-5-20251001` | `ENRICHMENT_LLM` |
| `get_observability_llm` (UC2 chatbot) | `groq/llama-3.1-8b-instant` | `OBSERVABILITY_LLM` |

Swap models here or via env, not at call sites. `call_llm_json()` has markdown-fence fallback for models wrapping JSON in ` ```json ... ``` `.

`src/models/llm.py` is also the **UC2 import gateway** — `_UC2_AVAILABLE`, `_emit_event`, `_MetricsCollector` are exported from there. All files import UC2 symbols from `src.models.llm`, never directly from `src.uc2_observability`. Guards against import failures when UC2 deps absent.

### UC2 observability layer (fully implemented)

`src/uc2_observability/`:

- `log_writer.py` — `RunLogWriter`: atomic JSON run logs to `output/run_logs/` after every run. Called from `save_output_node`.
- `log_store.py` — `RunLogStore`: read-only query interface (`load_all`, `filter`, `get_by_run_id`, `summary_stats`).
- `rag_chatbot.py` — `ObservabilityChatbot`: structured retrieval + LLM synthesis over run history. Returns `ChatResponse(answer, cited_run_ids, context_run_count)`.
- `metrics_exporter.py` — `MetricsExporter`: pushes 12 labelled Prometheus gauges to Pushgateway (`:9091`). Isolated `CollectorRegistry`; **never raises on network failure**.
- `metrics_collector.py` — in-process `MetricsCollector` called by `_emit_event` at each pipeline event.
- `anomaly_detector.py` — Isolation Forest on Prometheus metrics for last N runs per source; pushes `etl_anomaly_flag=1` to Pushgateway and writes to Postgres `anomaly_reports`. Called after each `run_completed` and on hourly schedule.
- `chunker.py` — reads new `audit_events` from Postgres since last cursor, embeds with `all-MiniLM-L6-v2`, upserts into ChromaDB `audit_corpus`. 5-minute sleep loop.
- `kafka_to_pg.py` — Kafka → Postgres consumer. Demuxes `pipeline.events` by `event_type` into `audit_events`, `block_trace`, `quarantine_rows`, `dedup_clusters`. Reconnects with exponential back-off.
- `mcp_server.py` — FastAPI on `:8001` with 7 MCP-style tool endpoints (Prometheus, Postgres, Redis; 15s TTL Prometheus, 30s Postgres).
- `streamlit_app.py` — standalone Streamlit observability UI (separate from `app.py` sidebar mode).
- `dashboard.py`, `anomaly_detection.py` — placeholder shims; real logic lives in files above.

`app.py` has sidebar Mode radio (Pipeline / Observability). Observability renders `_render_observability_page()` with multi-turn chat, refresh, cited run ID expanders.

UC2 event emission (`run_started`, `run_completed`, `block_start`, `block_end`, `quarantine`, `dedup_cluster`) goes through `_emit_event` / `_UC2_AVAILABLE` / `_MetricsCollector` re-exported from `src/models/llm.py`. All emits wrapped in `try/except` and logged as warnings on failure; **pipeline runs must not be blocked by observability outages.**

Postgres (UC2, `localhost:5432`, db `uc2`, user `mip`/`mip_pass`): `audit_events`, `block_trace`, `quarantine_rows`, `dedup_clusters`, `anomaly_reports`.

### Checkpoint/resume is SQLite-backed

[src/pipeline/checkpoint/manager.py](src/pipeline/checkpoint/manager.py) → `checkpoints.db` (SHA256 of source file, schema version, completed chunks, plan YAML, corpus snapshots). CLI `--resume` validates SHA256 before rehydrate; `--force-fresh` clears all rows. For **GCS sources** the source file doesn't exist on disk, so `src/pipeline/cli.py` `_create_gcs_checkpoint()` inserts into SQLite with URI's SHA256 instead.

### Airflow DAGs

`airflow/dags/` — production orchestration. DAGs mount `src/` and `config/` from repo root into Airflow container (see `docker-compose.yml` volumes). Daily chain (UTC):

| DAG | Schedule | What it does |
|---|---|---|
| `usda_incremental_dag` / `off_incremental_dag` / `openfda_incremental_dag` | 02:00–05:00 | Ingest source → GCS Bronze JSONL (`gs://mip-bronze-2024/`) |
| `bronze_to_bq_dag` | 03:00–06:00 | Bronze JSONL → BigQuery staging |
| `bronze_to_silver_dag` | 07:00 | Watermark-gated: new Bronze partitions → UC1 ETL → GCS Silver Parquet. Watermarks at `gs://mip-bronze-2024/_watermarks/{source}_silver_watermark.json`. |
| `silver_to_gold_dag` | 09:00 | ExternalTaskSensor waits for bronze_to_silver. Silver → dedup + enrichment → BigQuery `mip_gold.products` (append). |
| `uc2_anomaly_dag` | Hourly | Isolation Forest on UC1 Prometheus metrics; needs ≥5 completed runs per source. |
| `uc2_chunker_dag` | 5min | Postgres audit_events → ChromaDB embeddings. |
| `esci_dag` / `usda_dag` | Manual | ESCI ingestion / full USDA backfill. |

Airflow UI: `http://localhost:8080` (admin / admin).

### Kafka and GCS sink

Pipeline emits events to Kafka topic `pipeline.events`. `src/consumers/kafka_gcs_sink.py` replaces Kafka Connect S3 Sink for Bronze ingestion — `python -m src.consumers.kafka_gcs_sink --topic <topic> --prefix <prefix>` writes JSONL part files to GCS, flushing every `FLUSH_SIZE` records. `src/producers/` has `openfda_producer.py`, `off_producer.py`.

`NULL_RATE_COLUMNS` in `src/pipeline/runner.py` controls which columns get null-rate stats in `block_end` Kafka events. SQLite at `output/llm_cache.db` is Redis fallback.

### GCS / BigQuery data flow

- Bronze: `gs://mip-bronze-2024/` (JSONL, partitioned by source + date)
- Silver: `gs://mip-silver-2024/` (Parquet, partitioned by domain + source)
- Gold BQ: `mip_gold.products` (BigQuery, schema auto-detected, append via Airflow)

### UC3 / UC4

[src/uc3_search/](src/uc3_search/) (hybrid_search, indexer, evaluator) and [src/uc4_recommendations/](src/uc4_recommendations/) (association_rules, graph_store, recommender) exist as modules. Verify implementation state before assuming a function is wired into `demo.py`/`app.py`/graph/CLI — some surfaces may still be scaffolding.

## Things to double-check before editing

- **Block `audit_entry()` signature** — every block extends [src/blocks/base.py](src/blocks/base.py):`Block` and must return `{block, rows_in, rows_out, ...}` from `audit_entry()`. UI waterfall and `demo.py` trace read those fields by name.
- **`run_step` vs `invoke`** — Streamlit calls `run_step(step_name, state)`; `demo.py`/CLI use `graph.invoke()`. State shape must stay compatible with both paths.
- **YAML cache writer coherence** — `plan_sequence_node` writes the full cacheable blob. Add fields Agent 1/2/3 produce → extend `cacheable` dict there, or replayed runs silently drop them.
- **UC2 imports always go through `src.models.llm`** — never import `_emit_event`/`_MetricsCollector` directly from `src.uc2_observability`. Import guard in `llm.py` is what keeps things safe when UC2 deps absent.
- **UC2 emits are best-effort** — wrap new emits in `try/except`, log warnings, don't raise. Observability must not block pipeline.
- **Safety boundary in enrichment** — `allergens`, `dietary_tags`, `is_organic` are S1-only. Never route through S2/S3 output paths.
- **Registry key determinism** — `FunctionRegistry.save()` preserves `used_count` on updates by design; if you rewrite save logic, keep that preservation or "pipeline remembered" telemetry resets every run.
- **Don't touch `final_project/`** (if present) — fully separate project with own deps and its own CLAUDE.md.

## Active technologies

- Python 3.11 (Poetry). pandas 2.2, LangGraph 0.4, LiteLLM 1.55, FAISS-CPU, sentence-transformers, rapidfuzz, pyarrow, redis-py, streamlit, structlog, prometheus_client, chromadb, networkx, mlxtend, rank-bm25, kafka-python-ng, psycopg2-binary.
- Redis `localhost:6379` (SQLite fallback `output/cache.db`).
- GCS buckets: `mip-bronze-2024` (JSONL), `mip-silver-2024` (Parquet); BigQuery `mip_gold.products`.
- Prometheus Pushgateway `localhost:9091`; Grafana `localhost:3000`.
- Service endpoint cheat-sheet: [ENDPOINTS.md](ENDPOINTS.md).
