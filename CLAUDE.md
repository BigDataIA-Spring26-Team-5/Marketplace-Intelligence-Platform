# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

Two independent projects live side-by-side:

- **Repo root** — the schema-driven self-extending ETL pipeline (`demo.py`, `app.py`, `src/`, `config/`, `corpus/`, `function_registry/`). This is the primary project. `pyproject.toml` declares `name = "etl-pipeline"`.
- **`final_project/`** — a separate Kafka ingestion architecture (openFDA / USDA / OpenFoodFacts → Kafka → Snowflake). It has its own [CLAUDE.md](final_project/CLAUDE.md), its own `pyproject.toml`, and imports as `common.*` / `fda.*` / `usda.*` / `openfoodfacts.*`. Commands there must be run from inside `final_project/kafka_full_arch/`. **Nothing is shared between the two trees** — they have different dependencies, different Python versions, and different entry points. When working in `final_project/`, defer to its own CLAUDE.md.

The rest of this file is about the repo-root ETL pipeline.

## Common commands

```bash
# Install (Poetry, Python ^3.11)
poetry install

# API keys required — see .env.example (ANTHROPIC_API_KEY, DEEPSEEK_API_KEY, GROQ_API_KEY)
cp .env.example .env

# Full platform stack (Kafka, Airflow, Postgres, Prometheus, Pushgateway, Grafana,
# ChromaDB, Redis, MLflow) — see docker-compose.yml at repo root
docker-compose -p mip up -d

# CLI demo — runs 3 pipeline passes (USDA → FDA → FDA replay)
poetry run python demo.py

# Streamlit wizard UI with HITL approval gates
poetry run streamlit run app.py

# MCP observability server (FastAPI, Redis-cached, port 8001)
uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001

# (One-time) Build the KNN enrichment corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py
poetry run python scripts/build_corpus.py --limit 10000

# Tests
poetry run pytest
poetry run pytest tests/uc2_observability/test_log_writer.py::test_name
poetry run pytest tests/unit/test_cache_client.py
poetry run pytest tests/integration/test_cache_pipeline.py
```

`demo.py` expects `data/usda_fooddata_sample.csv` and `data/fda_recalls_sample.csv` to exist. The `data/` directory is **gitignored** — CSVs are not in the repo and must be placed there before running. Output CSVs land in `output/` (also gitignored).

## Architecture — the load-bearing ideas

This is a **two-agent LangGraph pipeline** that ingests heterogeneous food-product CSVs, auto-detects schema gaps, generates transformation code via LLM, and produces a unified catalog with DQ scores and cascading enrichment. The full node-by-node walkthrough is in [README.md](README.md); this section captures only what is non-obvious from reading the code.

### The graph is the control flow

[src/agents/graph.py](src/agents/graph.py) builds a `StateGraph` with 8 nodes: `load_source → analyze_schema → check_registry → {generate_code → validate_code → register_functions}* → run_pipeline → save_output`. State flows through `PipelineState` ([src/agents/state.py](src/agents/state.py)), a `TypedDict(total=False)` where fields are set incrementally — most fields are absent at most nodes, so never assume a key exists.

Two conditional edges gate the loop:
- `route_after_registry_check` — skips Agent 2 entirely if every gap has a registry hit.
- `route_after_validation` — loops back to `generate_code` up to `max_retries` (default 2) for functions that fail sandbox validation, then proceeds with whatever passed.

`graph.py` also exposes `run_step(step_name, state)` and a `NODE_MAP` — this is how [app.py](app.py) executes nodes one at a time with HITL gates in between. If you add a node, register it in `NODE_MAP` too or the Streamlit wizard won't see it.

### Self-extending function registry is the point

The defining feature is that **Agent 2 output is persisted**. When a schema gap is resolved, the generated function is saved to `function_registry/functions/{key}.py` and indexed in `function_registry/registry.json` with its `(source_type, target_type, tags)` signature. On subsequent runs, `check_registry_node` calls `FunctionRegistry.lookup()` and finds the match — **Agent 2 is never invoked for that gap again** (zero LLM cost). This is the "pipeline remembered" behavior Run 3 of the demo exists to showcase; don't break it by generating functions with non-deterministic keys or by mutating registry entries in ways that break lookups.

### Sandbox is security-critical

[src/agents/sandbox.py](src/agents/sandbox.py) runs LLM-generated code in a **subprocess with a 5-second timeout**, after a static-analysis pass (`is_code_safe`) that blocks `import os/sys/subprocess`, `open()`, `eval`, `exec`, `__import__`, `compile`, `globals`, `getattr`, etc. The `CODEGEN_PROMPT` in [src/agents/prompts.py](src/agents/prompts.py) also constrains allowed imports to `re, pandas, datetime, math, json`. If you loosen either of these, loosen both in lockstep, and never let generated code bypass the subprocess boundary — the validator is the only thing standing between LLM output and the user's machine.

### Block sequence has a `"__generated__"` sentinel

[src/registry/block_registry.py](src/registry/block_registry.py) `get_default_sequence(domain)` returns an ordered list of block names containing the sentinel string `"__generated__"`. [src/pipeline/runner.py](src/pipeline/runner.py) `PipelineRunner` treats this sentinel as an injection point: when it hits `"__generated__"`, it loads each generated/registry function via `FunctionRegistry.load_function()` (dynamic import by file path) and applies it to the target column — or, if the column doesn't exist yet, creates it by passing the whole row dict to the function. Don't remove the sentinel or reorder around it without understanding what is being injected where.

### Column mapping happens before blocks run

`PipelineRunner.run()` applies `column_mapping` (source → unified column names) **before** iterating the block sequence. That means every block reads unified column names (`product_name`, `brand_name`, `ingredients`, etc.), regardless of which source produced the DataFrame. If you're adding a block that reads a raw source column, you're doing it at the wrong layer — the mapping step is the boundary.

### Enrichment is a cost cascade with a hard safety rule

[src/blocks/llm_enrich.py](src/blocks/llm_enrich.py) orchestrates three strategies of increasing cost:
1. **S1 deterministic** ([src/enrichment/deterministic.py](src/enrichment/deterministic.py)) — regex/keyword extraction
2. **S2 KNN corpus** ([src/enrichment/embedding.py](src/enrichment/embedding.py)) — FAISS product-to-product similarity
3. **S3 RAG-LLM** ([src/enrichment/llm_tier.py](src/enrichment/llm_tier.py)) — LLM with top-3 S2 neighbors injected as RAG context

**Hard rule: S2 and S3 touch only `primary_category`.** The safety fields (`allergens`, `is_organic`, `dietary_tags`) are **extraction-only** — populated by S1 from the product's own text, or left null. They are never inferred by KNN similarity or the LLM, because dangerous false positives (e.g., confidently tagging a product "gluten-free" because neighbors are, when it actually contains barley) are worse than nulls. There's a post-run assertion in `LLMEnrichBlock` that logs a warning if any S3-resolved row has a safety field that differs from its post-S1 state — if that fires, something upstream broke the invariant; fix it rather than silencing the warning.

S2's `_knn_neighbors` column is a pipeline-internal JSON string consumed only by S3. `LLMEnrichBlock` drops it from the DataFrame before returning — don't write output code that expects it to be present.

### Corpus persists across runs

[src/enrichment/corpus.py](src/enrichment/corpus.py) manages a persistent FAISS `IndexFlatIP` (inner product on L2-normalized vectors = cosine similarity) at `corpus/faiss_index.bin` + `corpus/corpus_metadata.json`. It's seeded by `scripts/build_corpus.py` (from USDA FoodData Central) or bootstrapped in-run from S1-resolved rows if the persistent corpus has fewer than `MIN_CORPUS_SIZE` (10) vectors. Both S2 and S3 **add resolved rows back into the corpus as a feedback loop**, so later runs get better. The `.bin` file is gitignored; `corpus_metadata.json` and `corpus_summary.json` are committed. If `faiss-cpu` isn't installed, S2 is skipped with a logged warning and everything falls through to S3 — don't treat a missing FAISS as a hard error.

Key thresholds in `corpus.py`: `VOTE_SIMILARITY_THRESHOLD=0.45`, `CONFIDENCE_THRESHOLD_CATEGORY=0.60`, `K_NEIGHBORS=5`.

### Domain schemas are the canonical target per-run

`config/schemas/<domain>_schema.json` is the canonical target schema for each domain (`nutrition`, `safety`, `pricing`). The domain is always operator-supplied via `--domain <domain>` (CLI) or `PipelineState["domain"]` (Streamlit) — it is never auto-inferred. On the **first run** for a new source, Agent 1 uses `FIRST_RUN_SCHEMA_PROMPT` to derive unified column names, then [src/schema/analyzer.py](src/schema/analyzer.py) `derive_unified_schema_from_source()` writes to the correct domain schema file — including auto-added enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns (`dq_score_pre`, `dq_score_post`, `dq_delta`). On **subsequent runs**, Agent 1 uses `SCHEMA_ANALYSIS_PROMPT` which excludes enrichment and computed columns from the mappable set. After block execution, `_silver_normalize()` enforces exact domain schema column set/order and writes `output/silver/<domain>/<source_name>.parquet`. Gold output is rebuilt each run by concatenating all Silver parquets for the domain into `output/gold/<domain>.parquet`. `config/unified_schema.json` is retired — do not load it.

### LLM routing is centralized

[src/models/llm.py](src/models/llm.py) wraps [LiteLLM](https://github.com/BerriAI/litellm) with three getters (`get_orchestrator_llm`, `get_codegen_llm`, `get_enrichment_llm`) — all currently point to `deepseek/deepseek-chat`. `call_llm_json()` parses responses and has a markdown-fence fallback (` ```json ... ``` `) for models that wrap JSON. Swap models here, not at call sites. [config/litellm_config.yaml](config/litellm_config.yaml) exists for provider routing configuration.

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

### UC3/UC4 are scaffolding only

`src/uc3_search/`, `src/uc4_recommendations/` contain **placeholder classes that all raise `NotImplementedError`** (hybrid search, indexer, evaluator, recommender, association rules, graph store). They are not wired into `demo.py`, `app.py`, or the graph. Don't assume any of them work.

## Things to double-check before editing

- **Registry key determinism** — `FunctionRegistry.save()` preserves `used_count` on updates by design; if you rewrite the save logic, keep that preservation or the "pipeline remembered" telemetry gets reset every run.
- **Block `audit_entry()` signature** — every block extends `src/blocks/base.py:Block` and must return `{block, rows_in, rows_out, ...}` from `audit_entry()`. The UI's waterfall and `demo.py`'s trace both read those fields by name.
- **`run_step` vs `invoke`** — the Streamlit UI calls `run_step(step_name, state)` to execute one node; `demo.py` uses `graph.invoke()` to run the whole graph. State shape must remain compatible with both paths.
- **UC2 imports always go through `src.models.llm`** — never import `_emit_event` or `_MetricsCollector` directly from `src.uc2_observability`; the import guard in `llm.py` is what keeps things safe when UC2 deps are absent.
- **Don't touch `final_project/`** when working on the ETL pipeline — it's a fully separate project with its own dependencies and conventions, and its own CLAUDE.md is the authoritative guide for work in that tree.
