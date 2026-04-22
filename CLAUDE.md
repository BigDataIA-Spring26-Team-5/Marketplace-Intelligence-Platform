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

# CLI demo — runs 3 pipeline passes (USDA → FDA → FDA replay)
poetry run python demo.py

# Streamlit wizard UI with HITL approval gates
poetry run streamlit run app.py

# (One-time) Build the KNN enrichment corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py
poetry run python scripts/build_corpus.py --limit 10000

# Tests (pytest is declared; tests/ currently contains only __init__.py)
poetry run pytest
poetry run pytest tests/path/to/test_file.py::test_name
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

### Unified schema is auto-generated once, then reused

[config/unified_schema.json](config/unified_schema.json) is the canonical target schema. On the **first run** (when the file is absent or the orchestrator sees no existing schema), Agent 1 uses `FIRST_RUN_SCHEMA_PROMPT` to derive clean unified column names from the source DataFrame, then [src/schema/analyzer.py](src/schema/analyzer.py) `derive_unified_schema_from_source()` writes the JSON — including auto-added enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns (`dq_score_pre`, `dq_score_post`, `dq_delta`). On **subsequent runs**, Agent 1 uses `SCHEMA_ANALYSIS_PROMPT` which explicitly excludes enrichment and computed columns from the mappable set — those are not sourceable, they're produced by the pipeline. If you're adding new computed or enrichment columns, extend the `derive_unified_schema_from_source()` list **and** the exclusion filter in `analyze_schema_node`, otherwise the LLM will be asked to map columns that don't come from the source.

### LLM routing is centralized

[src/models/llm.py](src/models/llm.py) wraps [LiteLLM](https://github.com/BerriAI/litellm) with three getters (`get_orchestrator_llm`, `get_codegen_llm`, `get_enrichment_llm`) — all currently point to `deepseek/deepseek-chat`. `call_llm_json()` parses responses and has a markdown-fence fallback (` ```json ... ``` `) for models that wrap JSON. Swap models here, not at call sites. [config/litellm_config.yaml](config/litellm_config.yaml) exists for provider routing configuration.

### UC2 observability layer is implemented

`src/uc2_observability/` now contains working implementations:
- `log_writer.py` — `RunLogWriter`: writes atomic JSON run logs to `output/run_logs/` after every pipeline run (success or partial). Called from `save_output_node` in `src/agents/graph.py`.
- `log_store.py` — `RunLogStore`: read-only query interface (`load_all`, `filter`, `get_by_run_id`, `summary_stats`) over the persisted JSON logs.
- `rag_chatbot.py` — `ObservabilityChatbot`: structured retrieval + LLM synthesis (via `get_observability_llm()`) answering natural-language questions about run history. Returns `ChatResponse(answer, cited_run_ids, context_run_count)`.
- `metrics_exporter.py` — `MetricsExporter`: pushes 12 labelled Prometheus gauges to Pushgateway after each run. Uses isolated `CollectorRegistry`; never raises on network failure.
- `anomaly_detection.py`, `dashboard.py` — still placeholder (raise `NotImplementedError`).

`app.py` now has a sidebar Mode radio ("Pipeline" / "Observability"). Observability mode renders `_render_observability_page()` with multi-turn chat UI, refresh button, and cited run ID expanders.

`grafana/docker-compose.yml` starts a local Prometheus + Pushgateway + Grafana stack (`cd grafana && docker compose up -d`). Dashboard at `http://localhost:3000`.

### UC3/UC4 are scaffolding only

`src/uc3_search/`, `src/uc4_recommendations/` contain **placeholder classes that all raise `NotImplementedError`** (hybrid search, indexer, evaluator, recommender, association rules, graph store). They are not wired into `demo.py`, `app.py`, or the graph. Don't assume any of them work.

## Things to double-check before editing

- **Registry key determinism** — `FunctionRegistry.save()` preserves `used_count` on updates by design; if you rewrite the save logic, keep that preservation or the "pipeline remembered" telemetry gets reset every run.
- **Block `audit_entry()` signature** — every block extends `src/blocks/base.py:Block` and must return `{block, rows_in, rows_out, ...}` from `audit_entry()`. The UI's waterfall and `demo.py`'s trace both read those fields by name.
- **`run_step` vs `invoke`** — the Streamlit UI calls `run_step(step_name, state)` to execute one node; `demo.py` uses `graph.invoke()` to run the whole graph. State shape must remain compatible with both paths.
- **Don't touch `final_project/`** when working on the ETL pipeline — it's a fully separate project with its own dependencies and conventions, and its own CLAUDE.md is the authoritative guide for work in that tree.

## Active Technologies
- Python 3.11 + `redis-py` (new), `numpy` (existing, for embedding serialization), `hashlib` (stdlib), `argparse` (stdlib) (009-redis-cache-layer)
- Redis at `localhost:6379` (new); FAISS index (existing, unaffected) (009-redis-cache-layer)
- Python 3.11 + `redis-py` (existing cache), `litellm` (existing LLM routing), `pandas` (existing), `streamlit` (existing UI) (010-observability-rag-chatbot)
- Local JSON files in `output/run_logs/` (gitignored) (010-observability-rag-chatbot)
- Python 3.11 + `kafka-python`/`confluent-kafka` and `prometheus_client` imported indirectly via UC2 modules; `uuid`, `hashlib`, `time`, `datetime` (stdlib) added directly (010-uc1-uc2-integration)
- `NULL_RATE_COLUMNS` constant in `src/pipeline/runner.py` controls null-rate columns in block_end Kafka events (010-uc1-uc2-integration)
- UC2 import guard in `src/models/llm.py` — `_UC2_AVAILABLE`, `_emit_event`, `_MetricsCollector` exported from that module; all other files import UC2 symbols from there (010-uc1-uc2-integration)
- `prometheus_client` (push mode via `push_to_gateway` to Pushgateway at `localhost:9091`); `grafana/docker-compose.yml` Docker stack for UC2 Grafana dashboard (011-observability-rag-chatbot)
- `RunLogWriter`, `RunLogStore`, `ObservabilityChatbot`, `MetricsExporter` in `src/uc2_observability/` fully implemented (011-observability-rag-chatbot)
- Python 3.11 + `pyarrow` (schema validation), `rapidfuzz` (fuzzy dedup), `faiss-cpu` (batch KNN), `sentence-transformers` (embeddings), `sqlite3` (LLM cache fallback) (013-gold-layer-pipeline)
- GCS buckets `gs://mip-silver-2024/` (input) and `gs://mip-gold-2024/` (output); CLI at `python -m src.pipeline.gold` (013-gold-layer-pipeline)
- Python 3.11 + pandas 2.x, chromadb (HTTP client), sentence-transformers (`all-MiniLM-L6-v2`), rapidfuzz, faiss-cpu, redis-py, pyarrow (aqeel)
- GCS (Silver Parquet input), BigQuery (Gold output), ChromaDB at localhost:8000 (corpus), Redis at localhost:6379 (embedding + dedup cache) (aqeel)

## Recent Changes
- 009-redis-cache-layer: Added Python 3.11 + `redis-py` (new), `numpy` (existing, for embedding serialization), `hashlib` (stdlib), `argparse` (stdlib)
- 010-uc1-uc2-integration: Wired UC1 pipeline to emit Kafka events (block_start/block_end, run_started/run_completed, quarantine, dedup_cluster) and push Prometheus metrics via UC2 modules; added `_llm_call_counter` to `src/models/llm.py`; added `_run_id`/`_run_start_time` to PipelineState; added `last_clusters`/`last_dedup_rate` to FuzzyDeduplicateBlock
- 011-observability-rag-chatbot: Implemented UC2 log persistence (`RunLogWriter`), queryable log store (`RunLogStore`), RAG chatbot (`ObservabilityChatbot`), Prometheus Pushgateway exporter (`MetricsExporter`); added Observability mode to Streamlit wizard; added `grafana/` Docker Compose stack; `_run_start_time` now set in `load_source_node` via `time.monotonic()`
- 013-gold-layer-pipeline: Gold layer pipeline (Silver → Gold) with 4-stage architecture: Unify (read + validate Silver Parquet), Dedup (blocking + fuzzy + Union-Find), Enrichment (S1 deterministic → S2 KNN → S3 RAG-LLM), Output (Gold Parquet + run log). Safety boundary: allergens S1-only. SQLite cache fallback when Redis unavailable.
