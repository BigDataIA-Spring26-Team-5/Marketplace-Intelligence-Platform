# Schema-Driven Self-Extending ETL Pipeline

A two-agent, LangGraph-orchestrated ETL pipeline that ingests heterogeneous food product data sources, automatically detects schema gaps, generates transformation code via LLM, and produces a unified product catalog enriched with categories, allergens, dietary tags, and data quality scores.

---

## Table of Contents

- [Schema-Driven Self-Extending ETL Pipeline](#schema-driven-self-extending-etl-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Architecture Overview](#architecture-overview)
  - [Project Structure](#project-structure)
  - [Entry Point: `demo.py`](#entry-point-demopy)
    - [Execution Flow](#execution-flow)
  - [LangGraph Pipeline: `src/agents/graph.py`](#langgraph-pipeline-srcagentsgraphpy)
  - [Pipeline State: `src/agents/state.py`](#pipeline-state-srcagentsstatepy)
  - [Agent 1 — Orchestrator: `src/agents/orchestrator.py`](#agent-1--orchestrator-srcagentsorchestratorpy)
    - [`load_source_node(state)`](#load_source_nodestate)
    - [`analyze_schema_node(state)`](#analyze_schema_nodestate)
    - [`check_registry_node(state)`](#check_registry_nodestate)
  - [Agent 2 — Code Generator: `src/agents/code_generator.py`](#agent-2--code-generator-srcagentscode_generatorpy)
    - [`generate_code_node(state)`](#generate_code_nodestate)
    - [`validate_code_node(state)`](#validate_code_nodestate)
    - [`register_functions_node(state)`](#register_functions_nodestate)
  - [Sandbox Execution: `src/agents/sandbox.py`](#sandbox-execution-srcagentssandboxpy)
    - [`is_code_safe(code)`](#is_code_safecode)
    - [`execute_in_sandbox(function_code, function_name, sample_values, target_type, timeout=5)`](#execute_in_sandboxfunction_code-function_name-sample_values-target_type-timeout5)
  - [Prompt Templates: `src/agents/prompts.py`](#prompt-templates-srcagentspromptspy)
  - [Schema Analyzer: `src/schema/analyzer.py`](#schema-analyzer-srcschemaanalyzerpy)
  - [Block Registry: `src/registry/block_registry.py`](#block-registry-srcregistryblock_registrypy)
  - [Function Registry: `src/registry/function_registry.py`](#function-registry-srcregistryfunction_registrypy)
  - [Pipeline Runner: `src/pipeline/runner.py`](#pipeline-runner-srcpipelinerunnerpy)
  - [Transformation Blocks](#transformation-blocks)
    - [Data Cleaning Blocks](#data-cleaning-blocks)
    - [Feature Extraction Blocks](#feature-extraction-blocks)
    - [Deduplication Blocks](#deduplication-blocks)
  - [Enrichment Layer (3-Strategy)](#enrichment-layer-3-strategy)
    - [Strategy 1 — Deterministic Extraction: `src/enrichment/deterministic.py`](#strategy-1--deterministic-extraction-srcenrichmentdeterministicpy)
    - [Strategy 2 — KNN Corpus Search: `src/enrichment/embedding.py`](#strategy-2--knn-corpus-search-srcenrichmentembeddingpy)
    - [Strategy 3 — RAG-Augmented LLM: `src/enrichment/llm_tier.py`](#strategy-3--rag-augmented-llm-srcenrichmentllm_tierpy)
    - [Reference Corpus: `src/enrichment/corpus.py`](#reference-corpus-srcenrichmentcorpuspy)
    - [Enrichment Orchestrator Block: `src/blocks/llm_enrich.py`](#enrichment-orchestrator-block-srcblocksllm_enrichpy)
  - [LLM Routing: `src/models/llm.py`](#llm-routing-srcmodelsllmpy)
  - [Streamlit UI: `app.py`](#streamlit-ui-apppy)
  - [Data Quality Scoring](#data-quality-scoring)
  - [Configuration](#configuration)
    - [`config/unified_schema.json`](#configunified_schemajson)
    - [`.env`](#env)
    - [`pyproject.toml`](#pyprojecttoml)
  - [Setup \& Usage](#setup--usage)
  - [Demo Walkthrough](#demo-walkthrough)

---

## Architecture Overview

The pipeline follows a **two-agent architecture** built on [LangGraph](https://github.com/langchain-ai/langgraph):

```
                         +-------------------+
                         |   Data Source      |
                         |  (CSV file)        |
                         +--------+----------+
                                  |
                                  v
                    +-------------+-------------+
                    |  Agent 1 — Orchestrator    |
                    |  (Schema Analysis +        |
                    |   Gap Detection)           |
                    +-------------+-------------+
                                  |
                         +--------+--------+
                         | Function        |
                         | Registry Check  |
                         +--------+--------+
                                  |
                     +------------+------------+
                     |                         |
               All gaps covered          Gaps remain
                     |                         |
                     |            +------------+------------+
                     |            |  Agent 2 — Code Gen     |
                     |            |  (LLM generates         |
                     |            |   transform functions)   |
                     |            +------------+------------+
                     |                         |
                     |            +------------+------------+
                     |            |  Sandbox Validation     |
                     |            |  (static analysis +     |
                     |            |   subprocess execution) |
                     |            +------------+------------+
                     |                         |
                     |            +------------+------------+
                     |            |  Register Functions     |
                     |            +------------+------------+
                     |                         |
                     +------------+------------+
                                  |
                    +-------------+-------------+
                    |  Pipeline Execution        |
                    |  (13 transformation blocks  |
                    |   + generated functions)    |
                    +-------------+-------------+
                                  |
                    +-------------+-------------+
                    |  3-Strategy Enrichment     |
                    |  S1: Deterministic         |
                    |  S2: KNN Corpus            |
                    |  S3: RAG-LLM               |
                    +-------------+-------------+
                                  |
                    +-------------+-------------+
                    |  Post-Enrichment           |
                    |  Quarantine + Output       |
                    +----------------------------+
```

**Key design principles:**

1. **Self-extending**: When a new data source has columns the pipeline hasn't seen before, Agent 2 generates Python transform functions on-the-fly and saves them to a persistent registry. On subsequent runs with the same source, those functions are reloaded from the registry — zero LLM cost.
2. **Schema-driven**: A unified schema (`config/unified_schema.json`) defines the target output format. Every incoming source is diffed against this schema, and gaps are addressed automatically.
3. **Cascading enrichment**: Enrichment proceeds through three strategies of increasing cost. Cheap deterministic extraction handles safety-critical fields; KNN corpus search handles category via product-to-product comparison; the LLM is only called as a last resort with RAG context from real examples.
4. **Human-in-the-loop (HITL)**: The Streamlit UI exposes approval gates at schema mapping, code review, and quarantine stages.

---

## Project Structure

```
ETL/
+-- demo.py                          # CLI entry point — runs 3 pipeline passes
+-- app.py                           # Streamlit web UI with HITL approval gates
+-- pyproject.toml                   # Poetry dependencies
+-- .env.example                     # Required API keys template
+-- config/
|   +-- unified_schema.json          # Canonical output schema definition
|   +-- litellm_config.yaml          # LiteLLM provider routing config
+-- data/
|   +-- usda_fooddata_sample.csv     # USDA FoodData Central branded products (primary demo source)
|   +-- fda_recalls_sample.csv       # FDA recall notices (secondary demo source)
|   +-- open_food_facts_sample.csv   # Open Food Facts sample
|   +-- open_prices_sample.csv       # Open Prices sample
|   +-- usda_sample_raw.csv          # USDA raw (pre-mapping)
|   +-- fda_sample_raw.csv           # FDA raw (pre-mapping)
|   +-- off_bulk_sample.csv.gz       # Open Food Facts bulk sample (compressed)
|   +-- off_latest_delta.json.gz     # Open Food Facts delta feed (compressed)
|   +-- USDA/                        # Raw USDA download directory
|   +-- usda_raw/                    # USDA pre-processing staging
+-- function_registry/
|   +-- registry.json                # Index of all generated functions
|   +-- functions/                   # Stored .py transform functions
+-- output/                          # Pipeline output CSVs
+-- src/
|   +-- agents/
|   |   +-- graph.py                 # LangGraph StateGraph builder + step runner
|   |   +-- state.py                 # TypedDict pipeline state schema
|   |   +-- orchestrator.py          # Agent 1: schema analysis + registry check
|   |   +-- code_generator.py        # Agent 2: LLM code generation + validation
|   |   +-- sandbox.py               # Subprocess sandbox for code validation
|   |   +-- prompts.py               # All LLM prompt templates
|   +-- blocks/
|   |   +-- base.py                  # Abstract Block base class
|   |   +-- strip_whitespace.py      # Strip whitespace from string columns
|   |   +-- lowercase_brand.py       # Normalize brand names to lowercase
|   |   +-- remove_noise_words.py    # Remove legal suffixes (Inc, LLC, etc.)
|   |   +-- strip_punctuation.py     # Replace punctuation with spaces
|   |   +-- extract_quantity_column.py  # Extract sizes (oz, g, ml) into column
|   |   +-- keep_quantity_in_name.py # No-op for pricing domain
|   |   +-- extract_allergens.py     # FDA Big-9 allergen keyword scan
|   |   +-- fuzzy_deduplicate.py     # Blocking + rapidfuzz + union-find dedup
|   |   +-- column_wise_merge.py     # Best-value merge across clusters
|   |   +-- golden_record_select.py  # DQ-weighted golden record selection
|   |   +-- dq_score.py             # Pre/post data quality scoring
|   |   +-- llm_enrich.py           # 3-strategy enrichment orchestrator block
|   +-- enrichment/
|   |   +-- deterministic.py         # S1: regex/keyword extraction (safety fields)
|   |   +-- embedding.py             # S2: KNN corpus search via FAISS
|   |   +-- llm_tier.py             # S3: RAG-augmented LLM (primary_category only)
|   |   +-- corpus.py               # Persistent FAISS index + corpus feedback loop
|   +-- models/
|   |   +-- llm.py                   # LiteLLM wrapper for multi-provider routing
|   +-- pipeline/
|   |   +-- runner.py                # Sequential block executor with audit logging
|   +-- registry/
|   |   +-- block_registry.py        # Discovers and serves pre-built blocks
|   |   +-- function_registry.py     # Persistent store for generated functions
|   +-- schema/
|   |   +-- analyzer.py              # DataFrame profiler + schema diff logic
|   +-- ui/
|       +-- styles.py                # Global CSS for Streamlit dark theme
|       +-- components.py            # HTML renderers for tables, cards, charts
+-- tests/
```

---

## Entry Point: `demo.py`

`demo.py` is the CLI entry point that demonstrates the full pipeline lifecycle across three sequential runs:

| Run | Data Source | Purpose |
|-----|------------|---------|
| **Run 1** | `data/usda_fooddata_sample.csv` | First run — no unified schema exists. Agent 1 derives the schema from the USDA data, establishes column mappings, and saves the unified schema to `config/unified_schema.json`. Some column type gaps exist (USDA `object` columns need `string` conversions); Agent 2 generates 3 transform functions (`transform_product_name`, `transform_category`, `transform_data_source`) and registers them. The full block sequence executes including deduplication, enrichment, and DQ scoring. |
| **Run 2** | `data/fda_recalls_sample.csv` | Second run — unified schema now exists. Agent 1 diffs the FDA source against the unified schema, detects 3 registry hits from Run 1 and 1 new gap (`published_date` is `int64` rather than `string`). Agent 2 generates 1 function (`transform_published_date`) for the remaining gap, validates it in the sandbox, and registers it. |
| **Run 3** | `data/fda_recalls_sample.csv` | Replay run — identical source as Run 2. All 4 schema gaps now have registry hits. Agent 2 is never called. This demonstrates the "pipeline remembered" behavior — zero LLM cost for known transforms. |

### Execution Flow

1. `main()` validates that both CSV files exist in `data/`
2. Calls `run_pipeline(source_path, domain, run_label)` three times
3. Each call:
   - Imports and builds the LangGraph `StateGraph` via `src.agents.graph.build_graph()`
   - Invokes the graph with `{"source_path": ..., "domain": ...}`
   - Prints a results summary: row count, DQ scores (pre/post/delta), schema status, gap count, registry hits, and a block-by-block execution trace

---

## LangGraph Pipeline: `src/agents/graph.py`

This file defines the core state machine. It builds a `StateGraph` with 8 nodes and conditional routing edges:

```
load_source -> analyze_schema -> check_registry
                                      |
                              +-------+-------+
                              |               |
                        [has misses]    [all covered]
                              |               |
                        generate_code         |
                              |               |
                        validate_code         |
                              |               |
                     +--------+--------+      |
                     |                 |      |
                [all passed]    [retry < max] |
                     |                 |      |
                register_functions     |      |
                     |          (loop back)   |
                     +--------+--------+------+
                              |
                        run_pipeline
                              |
                        save_output -> END
```

**Node functions:**
- `load_source_node` — loads CSV into a DataFrame, profiles the schema (from `orchestrator.py`)
- `analyze_schema_node` — Agent 1 LLM call for schema analysis/gap detection (from `orchestrator.py`)
- `check_registry_node` — checks the function registry for existing transforms (from `orchestrator.py`)
- `generate_code_node` — Agent 2 LLM call to generate Python functions (from `code_generator.py`)
- `validate_code_node` — pass-through node for conditional routing inspection
- `register_functions_node` — persists validated functions to `function_registry/` (from `code_generator.py`)
- `run_pipeline_node` — executes the full block sequence via `PipelineRunner` (defined in `graph.py`)
- `save_output_node` — writes the final DataFrame to `output/{source_name}_unified.csv` (defined in `graph.py`)

**Routing logic:**
- `route_after_registry_check()`: if there are registry misses, routes to `generate_code`; otherwise skips straight to `run_pipeline`
- `route_after_validation()`: if all generated functions pass sandbox validation, routes to `register_functions`; if not and retries remain (max 2), loops back to `generate_code`; otherwise proceeds with partial results

**Step-by-step runner:** The file also exposes `run_step(step_name, state)` for the Streamlit UI to execute individual nodes with HITL gates in between, via the `NODE_MAP` dictionary.

---

## Pipeline State: `src/agents/state.py`

Defines `PipelineState` as a `TypedDict(total=False)` that flows through every LangGraph node. Fields are set incrementally — not every field is present at every node.

| Field Group | Fields | Set By |
|-------------|--------|--------|
| **Input** | `source_path`, `source_df`, `source_schema`, `domain` | `load_source_node` |
| **Schema Analysis** | `unified_schema`, `unified_schema_existed`, `gaps`, `column_mapping` | `analyze_schema_node` |
| **Registry** | `registry_hits`, `registry_misses` | `check_registry_node` |
| **Code Generation** | `generated_functions`, `retry_count`, `max_retries` | `generate_code_node` |
| **Execution** | `block_sequence`, `working_df`, `dq_score_pre`, `dq_score_post` | `run_pipeline_node` |
| **Enrichment** | `enrichment_stats` (`s1_extraction`, `s2_knn`, `s3_rag_llm`, `unresolved`) | `run_pipeline_node` (via `LLMEnrichBlock`) |
| **Quarantine** | `quarantined_df`, `quarantine_reasons` | `run_pipeline_node` |
| **Audit** | `audit_log`, `errors` | `run_pipeline_node` |

Also defines `GapItem` (a single schema gap between source and unified schema) and `GeneratedFunction` (a function generated by Agent 2 with its validation status and sample outputs).

---

## Agent 1 — Orchestrator: `src/agents/orchestrator.py`

Agent 1 handles schema intelligence. It exposes three LangGraph node functions:

### `load_source_node(state)`
- Reads the CSV at `state["source_path"]` into a pandas DataFrame
- Calls `profile_dataframe()` from `src/schema/analyzer.py` to compute per-column metadata (dtype, null rate, unique count, sample values)
- Returns `{source_df, source_schema}`

### `analyze_schema_node(state)`
- Loads the unified schema from `config/unified_schema.json` via `load_unified_schema()`
- **First run (no unified schema):** Sends the source schema to the LLM with `FIRST_RUN_SCHEMA_PROMPT`, asking it to derive clean unified column names. Then calls `derive_unified_schema_from_source()` to build and save the schema, including enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed columns (`dq_score_pre`, `dq_score_post`, `dq_delta`)
- **Subsequent runs (unified schema exists):** Filters out computed/enrichment columns, then sends both the source profile and the mappable unified schema to the LLM with `SCHEMA_ANALYSIS_PROMPT`. The LLM returns a column mapping and a list of gaps (type mismatches, format differences, semantic remappings)
- Returns `{unified_schema, unified_schema_existed, column_mapping, gaps}`

### `check_registry_node(state)`
- Iterates over each gap from the schema analysis
- For each gap, calls `FunctionRegistry.lookup(source_type, target_type, tags=[target_col])` to check if a matching transform function already exists
- Splits gaps into `registry_hits` (reusable — function file path found) and `registry_misses` (need Agent 2)
- Returns `{registry_hits, registry_misses, retry_count: 0, max_retries: 2}`

---

## Agent 2 — Code Generator: `src/agents/code_generator.py`

Agent 2 handles autonomous code generation for schema gaps that have no registry match. It exposes three node functions:

### `generate_code_node(state)`
- Iterates over `registry_misses` (or only failed functions on retry)
- For each gap, constructs a prompt using `CODEGEN_PROMPT` (or `CODEGEN_RETRY_PROMPT` on retry with the previous error and code)
- Calls the DeepSeek LLM via `call_llm()` to generate a self-contained Python function named `transform_{target_column}(value)`
- Strips markdown fences from the response via `_clean_code_response()`
- Validates the function in a subprocess sandbox via `execute_in_sandbox()`
- Returns `{generated_functions: [...], retry_count: N+1}`

### `validate_code_node(state)`
- Pass-through node — exists solely for the conditional edge to inspect `generated_functions` and decide whether to retry, register, or skip

### `register_functions_node(state)`
- For each function that passed validation, calls `FunctionRegistry.save()` which:
  - Writes the Python code to `function_registry/functions/{key}.py`
  - Adds/updates an entry in `function_registry/registry.json` with metadata (domain, source/target types, tags, timestamps, usage count)
- Returns the updated `generated_functions` list with `file_path` populated

---

## Sandbox Execution: `src/agents/sandbox.py`

Provides security-hardened execution of LLM-generated code:

### `is_code_safe(code)`
Static analysis pass that scans for 13 banned patterns including `import os`, `import sys`, `import subprocess`, `open(`, `eval(`, `exec(`, `__import__(`, `compile(`, `globals(`, and `getattr(`. Returns `(is_safe, reason)`.

### `execute_in_sandbox(function_code, function_name, sample_values, target_type, timeout=5)`
1. Runs the static safety check
2. Constructs a self-contained Python test script that imports the generated function, runs it against `sample_values`, and checks output types against `target_type`
3. Writes the script to a temporary file and executes it in a **subprocess** with a 5-second timeout
4. Parses the JSON output from the subprocess
5. Returns `{passed: bool, outputs: {input: output}, error: str | None}`

---

## Prompt Templates: `src/agents/prompts.py`

Four prompt templates used by the agents:

| Template | Used By | Purpose |
|----------|---------|---------|
| `SCHEMA_ANALYSIS_PROMPT` | Agent 1 (subsequent runs) | Diffs incoming source schema against the unified schema. Returns JSON with `column_mapping` and `gaps` (type mismatches, ADD/MAP actions). Explicitly excludes enrichment and computed columns. |
| `FIRST_RUN_SCHEMA_PROMPT` | Agent 1 (first run) | Derives clean unified column names from the first data source. Instructs the LLM to rename columns to standardized names, drop metadata/ID columns, and keep product-identity columns. |
| `CODEGEN_PROMPT` | Agent 2 (first attempt) | Generates a self-contained Python function `transform_{target_column}(value)` that converts `source_type` to `target_type`. Constrains allowed imports to `re, pandas, datetime, math, json`. |
| `CODEGEN_RETRY_PROMPT` | Agent 2 (retry after validation failure) | Provides the previous code and the error message, asking the LLM to fix the function. |

---

## Schema Analyzer: `src/schema/analyzer.py`

Provides utility functions for schema intelligence:

- **`profile_dataframe(df, sample_size=5)`**: Profiles every column — dtype, null rate, unique count, and up to 5 sample values. Used by `load_source_node` to build the source schema that Agent 1 analyzes.
- **`load_unified_schema()` / `save_unified_schema(schema)`**: Read/write the unified schema JSON at `config/unified_schema.json`.
- **`derive_unified_schema_from_source(df, column_mapping, domain)`**: On first run, builds the unified schema from the source DataFrame. Maps pandas dtypes to schema types (`string`, `integer`, `float`, `boolean`). Automatically adds enrichment columns (`allergens`, `primary_category`, `dietary_tags`, `is_organic`) and computed DQ columns. Sets `required: True` for columns with less than 50% null rate. Includes default DQ weights (completeness: 0.4, freshness: 0.35, ingredient_richness: 0.25).
- **`compute_schema_diff(source_profile, unified_schema)`**: Deterministic diff — exact name matches become `column_mapping`, unmatched unified columns become gaps with action `ADD`.

---

## Block Registry: `src/registry/block_registry.py`

The `BlockRegistry` class serves as the catalog of all pre-built transformation blocks. It instantiates 13 block singletons at import time and provides:

- **`get(name)`**: Returns a block instance by name. Raises `KeyError` if not found.
- **`list_blocks(domain=None)`**: Lists available block names, optionally filtered by domain compatibility.
- **`get_default_sequence(domain)`**: Returns the ordered list of block names for a given domain. The sequence includes a `"__generated__"` sentinel that marks where agent-generated transform functions are injected during execution.

**Default block sequence** (for `nutrition` domain):
1. `dq_score_pre` — compute baseline DQ score
2. `__generated__` — inject agent-generated/registry functions
3. `strip_whitespace` — clean string columns
4. `lowercase_brand` — normalize brand casing
5. `remove_noise_words` — strip legal suffixes
6. `strip_punctuation` — replace punctuation with spaces
7. `extract_quantity_column` — parse sizes into separate column (or `keep_quantity_in_name` for pricing)
8. `extract_allergens` — FDA Big-9 keyword scan (nutrition/safety only)
9. `fuzzy_deduplicate` — blocking + rapidfuzz clustering
10. `column_wise_merge` — best-value merge within clusters
11. `golden_record_select` — DQ-weighted record selection
12. `llm_enrich` — 3-strategy enrichment cascade (S1 extraction → S2 KNN → S3 RAG-LLM)
13. `dq_score_post` — compute final DQ score + delta

---

## Function Registry: `src/registry/function_registry.py`

The `FunctionRegistry` is the persistent store for LLM-generated transformation functions. It enables the "pipeline remembered" behavior — once a transform is generated and validated, it is never regenerated.

**Storage:**
- `function_registry/registry.json` — JSON array of entries, each with: `key`, `function_name`, `file` path, `created_for_domain`, `source_type`, `target_type`, `tags`, `used_count`, `last_used`, `validation_passed`, `created_at`
- `function_registry/functions/*.py` — the actual Python source files

**Currently registered functions (0):**

The registry is populated dynamically when the pipeline runs and encounters schema gaps. Functions are generated on first use and persisted for reuse on subsequent runs.

**Key methods:**
- **`lookup(source_type, target_type, tags)`**: Finds a registered function matching the type signature. First filters by exact source_type + target_type match, then ranks candidates by tag overlap. Returns the best match or `None`.
- **`save(key, function_name, function_code, metadata)`**: Writes the `.py` file, adds/updates the registry index entry. Preserves usage count on updates.
- **`load_function(file_path, function_name)`**: Dynamically loads a saved function via `importlib.util.spec_from_file_location()`.
- **`increment_usage(key)`**: Bumps `used_count` and updates `last_used` timestamp after a function is applied during pipeline execution.

---

## Pipeline Runner: `src/pipeline/runner.py`

The `PipelineRunner` class executes blocks in sequence on a DataFrame, producing an audit log of every step:

1. **Column mapping** is applied first — renames source columns to unified names
2. Iterates through the `block_sequence` list:
   - For regular blocks: looks up the block by name in the `BlockRegistry`, calls `block.run(df, config)`, and records an audit entry
   - For the `"__generated__"` sentinel: calls `_apply_generated()` which loads each generated/registry function via `FunctionRegistry.load_function()` and applies it. If the target column exists, applies the function to that column via `.apply()`. If the column doesn't exist, creates it by passing the entire row dict to the function.
3. After applying a function, calls `increment_usage()` to track registry utilization
4. Returns `(result_df, audit_log)` where each audit entry contains `{block, rows_in, rows_out, ...}`

---

## Transformation Blocks

All blocks extend `src/blocks/base.py:Block` (abstract base class requiring `run(df, config) -> DataFrame` and providing `audit_entry()` for observability).

### Data Cleaning Blocks

| Block | File | Description |
|-------|------|-------------|
| **StripWhitespaceBlock** | `strip_whitespace.py` | Strips leading/trailing whitespace from all `object`-type columns. Converts `"nan"` strings to `pd.NA`. |
| **LowercaseBrandBlock** | `lowercase_brand.py` | Lowercases the `brand_name` column. Handles missing columns gracefully. |
| **RemoveNoiseWordsBlock** | `remove_noise_words.py` | Removes legal suffixes (`Inc`, `LLC`, `Ltd`, `Corp`, `Co`, `Company`, `Corporation`, `Incorporated`, `Limited`) from `product_name` and `brand_name` using regex. |
| **StripPunctuationBlock** | `strip_punctuation.py` | Replaces non-alphanumeric characters (except spaces) with spaces in `product_name` and `brand_name`. Collapses multiple spaces. |

### Feature Extraction Blocks

| Block | File | Description |
|-------|------|-------------|
| **ExtractQuantityColumnBlock** | `extract_quantity_column.py` | Regex-extracts size/quantity patterns (e.g., `"16oz"`, `"500g"`, `"12 fl oz"`, `"6pk"`) from `product_name` into a new `sizes` column. Removes the quantity text from the product name. Supports units: oz, g, mg, kg, lb, ml, l, fl oz, gal, ct, count, pk, pack. Domain: `nutrition`. |
| **KeepQuantityInNameBlock** | `keep_quantity_in_name.py` | Intentional no-op for the `pricing` domain where quantity information should remain embedded in the product name. |
| **ExtractAllergensBlock** | `extract_allergens.py` | Scans the `ingredients` column for FDA Big-9 allergens (milk, egg, fish, shellfish, tree nut, peanut, wheat, soybean, sesame) using expanded regex patterns that catch common variants (e.g., `casein`, `whey` for milk; `albumin` for egg; `semolina`, `durum` for wheat). Produces a comma-separated `allergens` column. Domain: `nutrition`/`safety`. |

### Deduplication Blocks

| Block | File | Description |
|-------|------|-------------|
| **FuzzyDeduplicateBlock** | `fuzzy_deduplicate.py` | Three-phase deduplication: (1) **Blocking** — groups rows by first 3 characters of `product_name` to reduce pairwise comparisons. (2) **Scoring** — within each block, computes a weighted similarity score using `rapidfuzz.fuzz.token_sort_ratio` on product name (weight 0.5), brand name (weight 0.2), and combined text (weight 0.3). Default threshold: 85. (3) **Clustering** — uses a **Union-Find** (disjoint set) data structure with path compression and union by rank to form transitive closure clusters. Assigns `duplicate_group_id` and marks the first row per group as `canonical`. |
| **ColumnWiseMergeBlock** | `column_wise_merge.py` | Groups by `duplicate_group_id` and merges column-wise: for string columns, picks the longest non-null value; for numeric columns, picks the first non-null value. This ensures the most complete data survives deduplication. |
| **GoldenRecordSelectBlock** | `golden_record_select.py` | Selects one "golden record" per duplicate cluster using a weighted composite score: **completeness** (fraction of non-null columns, weight 0.4) + **freshness** (normalized `published_date`, weight 0.35) + **ingredient richness** (normalized ingredient text length, weight 0.25). The row with the highest score per group is selected. |

---

## Enrichment Layer (3-Strategy)

The enrichment layer fills in four enrichment columns — `primary_category`, `dietary_tags`, `is_organic`, and `allergens` — using a cascading 3-strategy architecture. A core design constraint governs safety fields: `allergens`, `is_organic`, and `dietary_tags` are **extraction-only** — they are populated from the product's own text or stay null. They are never inferred by KNN similarity or the LLM. This prevents dangerous false positives (e.g., a soup confidently tagged "gluten-free" because its neighbors are, when it actually contains barley).

The cascade is orchestrated by `src/blocks/llm_enrich.py:LLMEnrichBlock`. After each strategy, a `needs_enrichment` mask is recalculated. The `_knn_neighbors` column is a pipeline-internal column produced by S2 for consumption by S3; it is dropped from the DataFrame before output. Each strategy returns `(modified_df, updated_needs_enrichment_mask)`.

**Cost profile:**
- S1 (deterministic) — zero cost, pure regex
- S2 (KNN corpus) — one-time model load (~80MB), zero API calls; improves across runs as the corpus grows
- S3 (RAG-LLM) — LLM API call, only for rows still missing `primary_category` after S1+S2

---

### Strategy 1 — Deterministic Extraction: `src/enrichment/deterministic.py`

S1 handles all four enrichment columns. It is the **only** strategy that touches `allergens`, `is_organic`, and `dietary_tags`.

**`deterministic_enrich(df, enrich_cols, needs_enrichment)`**

**Primary category** — 18 keyword rules matched against `product_name`, `ingredients`, and `category`. First match wins.

| Pattern Keywords | Category |
|-----------------|----------|
| cereal, oat, granola, muesli | Breakfast Cereals |
| milk, cream, yogurt, cheese, butter, dairy | Dairy |
| chicken, beef, pork, turkey, meat, sausage, bacon | Meat & Poultry |
| fish, salmon, tuna, shrimp, seafood, cod | Seafood |
| bread, bagel, muffin, croissant, baguette, roll | Bakery |
| candy, chocolate, gummy, sweet, confection | Confectionery |
| chip, pretzel, popcorn, cracker, snack | Snacks |
| juice, soda, water, tea, coffee, beverage, drink | Beverages |
| sauce, ketchup, mustard, dressing, condiment, mayo | Condiments |
| frozen, ice cream, popsicle | Frozen Foods |
| fruit, apple, banana, berry, orange, grape | Fruits |
| vegetable, carrot, broccoli, spinach, lettuce, tomato | Vegetables |
| pasta, noodle, spaghetti, macaroni | Pasta & Grains |
| rice, quinoa, couscous | Pasta & Grains |
| soup, stew, broth, chili | Soups |
| baby, infant, toddler | Baby Food |
| organic | Organic |
| supplement, vitamin, mineral, protein powder | Supplements |

**Dietary tags** — 9 tag patterns matched **only** against `product_name` and dedicated label columns (`labels`, `dietary_tags_raw` if present). Ingredients are explicitly excluded: a dietary tag must be an explicit label claim on the product (e.g., "gluten-free" appears literally in the product name), not inferred from ingredient content.

| Pattern | Tag |
|---------|-----|
| gluten-free | gluten-free |
| vegan | vegan |
| vegetarian | vegetarian |
| kosher | kosher |
| halal | halal |
| sugar-free | sugar-free |
| low-fat | low-fat |
| non-gmo | non-gmo |
| keto | keto |

All matching tags are collected and comma-joined (a product can have multiple tags).

**Organic** — Matches "organic" or "usda organic" in any text column; sets `is_organic` to `True`/`False`.

If extraction fails for any safety field, the field stays null. S2 and S3 will not attempt to fill it.

---

### Strategy 2 — KNN Corpus Search: `src/enrichment/embedding.py`

S2 operates on `primary_category` only. It replaces the old label-string similarity approach with **product-to-product embedding comparison**: instead of asking "does '2% reduced fat homogenized milk' embed close to the word 'Dairy'?" (it doesn't), it asks "does this product embed close to other already-labeled dairy products?" (it does).

**`embedding_enrich(df, enrich_cols, needs_enrichment)`**

**Process:**
1. Loads the persistent FAISS corpus (`corpus/faiss_index.bin` + `corpus/corpus_metadata.json`) via `corpus.load_corpus()`
2. If the corpus is empty or too small (< 10 vectors), seeds it from rows already resolved by S1 in the current run via `corpus.build_seed_corpus(df)`
3. For each row where `primary_category` is still null:
   - Builds a query text from `product_name`, `brand_name`, `ingredients`, `category`
   - Retrieves the top-K (default 5) nearest neighbors from the FAISS index
   - Votes: neighbors with cosine similarity ≥ 0.45 contribute a weighted vote to their category
   - Computes confidence as the average similarity of winning-category votes
   - If confidence ≥ 0.60: assigns the category and adds the row to the corpus (feedback loop)
   - Else: stores the top-3 neighbors in `_knn_neighbors` (JSON string) for S3's RAG prompt and leaves the row unresolved
4. Saves the updated corpus to disk after processing

**FAISS index:** `IndexFlatIP` (inner product on L2-normalized vectors = cosine similarity). Persistent across pipeline runs — the corpus grows without depending on dedup cluster structure.

If `faiss-cpu` is not installed, S2 is skipped entirely with a logged warning and all unresolved rows flow directly to S3.

---

### Strategy 3 — RAG-Augmented LLM: `src/enrichment/llm_tier.py`

S3 operates on `primary_category` only. It is the fallback for rows that S2 could not confidently classify. Rather than cold inference from a sparse product row, S3 uses **RAG (Retrieval-Augmented Generation)**: it injects the top-3 nearest corpus neighbors from S2 into the LLM prompt, anchoring the model to real categorized examples.

**`llm_enrich(df, enrich_cols, needs_enrichment)`**

**Process:**
1. For each row still missing `primary_category`:
   a. Parses `_knn_neighbors` (JSON) to retrieve the top-3 neighbors found by S2
   b. Builds a RAG prompt listing the neighbors as examples ("these 3 similar products are all Dairy — what is this one?") followed by the product's own fields
   c. Calls the LLM via `call_llm_json()` with a system prompt that constrains output to a JSON `{"primary_category": "<category>"}` and allows `null` for uncertain cases
   d. If a non-null category is returned: assigns it and adds the row to the FAISS corpus (feedback loop)
2. Saves the updated corpus to disk after processing

**What S3 never does:** S3 never touches `allergens`, `is_organic`, or `dietary_tags`. These fields are not included in the prompt, not read from the LLM response, and not modified. If S1 did not extract them, they stay null.

**Temperature:** 0.1

---

### Reference Corpus: `src/enrichment/corpus.py`

The corpus module manages the persistent FAISS index shared by S2 and S3. It replaces the old cluster propagation tier with a mechanism that works on any dataset — not just those with dense duplicate clusters.

**Key functions:**

| Function | Purpose |
|----------|---------|
| `load_corpus()` | Load FAISS index + metadata from `corpus/` directory |
| `save_corpus(index, metadata)` | Persist index and metadata to disk |
| `build_seed_corpus(df)` | Seed the index from S1-resolved rows; skips if fewer than 10 labeled rows |
| `knn_search(row, index, metadata)` | Find K nearest neighbors; returns `(category, confidence, top3_neighbors)` |
| `add_to_corpus(row, category, index, metadata)` | Add a newly enriched row to the in-memory index (caller calls `save_corpus` after batch) |

**Thresholds:**

| Constant | Value | Meaning |
|----------|-------|---------|
| `VOTE_SIMILARITY_THRESHOLD` | 0.45 | Minimum similarity for a neighbor to cast a vote |
| `CONFIDENCE_THRESHOLD_CATEGORY` | 0.60 | Minimum average vote similarity to accept assignment without escalation |
| `K_NEIGHBORS` | 5 | Neighbors retrieved per query |
| `MIN_CORPUS_SIZE` | 10 | Minimum corpus size before KNN is attempted |

The `corpus/` directory is created automatically on first use.

---

### Enrichment Orchestrator Block: `src/blocks/llm_enrich.py`

The `LLMEnrichBlock` orchestrates the 3-strategy cascade:

1. Ensures all four enrichment columns exist (creates with `pd.NA` if missing)
2. Computes the initial `needs_enrichment` mask (rows with any null enrichment column)
3. Captures safety field values after S1 to enable the post-run assertion
4. Runs strategies in sequence, tracking resolution counts:
   ```
   S1 (deterministic extraction) -> S2 (KNN corpus) -> S3 (RAG-LLM)
   ```
5. Drops the `_knn_neighbors` pipeline-internal column before returning the DataFrame
6. Tags rows with `enriched_by_llm = True` only for S3-resolved rows (not S1 or S2)
7. Runs a safety assertion: logs a warning if any S3-resolved row has `allergens`, `is_organic`, or `dietary_tags` values that differ from their post-S1 state
8. Stores statistics in `self.last_enrichment_stats`:

| Key | Meaning |
|-----|---------|
| `s1_extraction` | Rows resolved by deterministic extraction |
| `s2_knn` | Rows resolved by KNN corpus search |
| `s3_rag_llm` | Rows resolved by RAG-augmented LLM |
| `unresolved` | Rows with at least one null enrichment column after all strategies |

These stats are read by `run_pipeline_node()` in `graph.py` and passed to the Streamlit UI for the enrichment breakdown visualization.

---

## LLM Routing: `src/models/llm.py`

All LLM calls are routed through [LiteLLM](https://github.com/BerriAI/litellm), which provides a unified interface across providers.

**Model assignments:**
| Role | Function | Model String |
|------|----------|-------------|
| Agent 1 (schema analysis) | `get_orchestrator_llm()` | `deepseek/deepseek-chat` |
| Agent 2 (code generation) | `get_codegen_llm()` | `deepseek/deepseek-chat` |
| S3 RAG-LLM enrichment | `get_enrichment_llm()` | `deepseek/deepseek-chat` |

**Wrapper functions:**
- **`call_llm(model, messages, temperature=0.0)`**: Returns the raw assistant message content string
- **`call_llm_json(model, messages, temperature=0.0)`**: Parses the response as JSON. Falls back to extracting JSON from markdown code blocks (`` ```json ... ``` `` or `` ``` ... ``` ``) if direct parsing fails

---

## Streamlit UI: `app.py`

A 5-step wizard UI with HITL (Human-in-the-Loop) approval gates:

| Step | Name | HITL Gate | Description |
|------|------|-----------|-------------|
| 0 | Select Source | - | File picker for CSVs in `data/`, domain selector, data preview |
| 1 | Schema Analysis | **Approve Mapping** | Displays source schema profile, schema delta table (source vs. unified), column mapping with MAP/ADD badges |
| 2 | Code Generation | **Approve & Register Functions** | Shows registry hit/miss results. For misses, displays generated code with syntax highlighting, PASS/FAIL badges, and sample I/O tables. Options: approve, regenerate failed, or skip |
| 3 | Pipeline Execution | - | Runs the full block sequence (auto-advances to results) |
| 4 | Results | **Accept Quarantine / Override** | DQ score cards (pre/post/delta), summary metrics, block execution waterfall, enrichment tier breakdown, quarantine table with override option, output preview, CSV download |

**UI components** (`src/ui/components.py`) render all data as raw HTML for full styling control with the dark-themed CSS defined in `src/ui/styles.py`.

---

## Data Quality Scoring

Computed by `src/blocks/dq_score.py` at two points in the pipeline: before enrichment (`dq_score_pre`) and after (`dq_score_post`).

**Formula:**
```
DQ Score = (Completeness * 0.4) + (Freshness * 0.35) + (Ingredient Richness * 0.25)
```

- **Completeness** (0.0 - 1.0): Fraction of non-null values across data columns (excludes computed/meta columns)
- **Freshness** (0.0 - 1.0): If `published_date` exists, normalized to 0-1 range based on min/max dates; otherwise defaults to 0.5
- **Ingredient Richness** (0.0 - 1.0): Length of `ingredients` text normalized against the maximum in the dataset

The `dq_delta` column captures the per-row improvement from the enrichment process.

---

## Configuration

### `config/unified_schema.json`
The canonical output schema defining all columns, their types, required status, and whether they are enrichment or computed columns. Auto-generated on first pipeline run and reused on subsequent runs.

### `.env`
Required API keys (see `.env.example`):
```
ANTHROPIC_API_KEY=sk-ant-...
DEEPSEEK_API_KEY=sk-...
GROQ_API_KEY=gsk_...
```

### `pyproject.toml`
Key dependencies:
- `pandas ^2.2` — data manipulation
- `langgraph ^0.4` — state machine orchestration
- `langchain-anthropic ^0.3` — LangChain Anthropic integration
- `litellm ^1.55` — multi-provider LLM routing
- `rapidfuzz ^3.9` — fuzzy string matching for deduplication
- `sentence-transformers ^3.0` — embedding model for S2 KNN corpus search
- `faiss-cpu ^1.8` — vector index for KNN corpus (product-to-product similarity)
- `streamlit ^1.56` — web UI

---

## Setup & Usage

```bash
# Install dependencies
poetry install

# Copy and fill in API keys
cp .env.example .env

# Run the CLI demo (3 pipeline passes)
poetry run python demo.py

# Run the Streamlit web UI
poetry run streamlit run app.py
```

---

## Demo Walkthrough

**Run 1 — USDA FoodData (`nutrition` domain):**
- No unified schema exists. Agent 1 calls the LLM to derive column mappings (e.g., `brand_owner` -> `brand_name`, `description` -> `product_name`)
- Unified schema is saved with 17 columns (9 mapped + 4 enrichment + 3 computed + `data_source`)
- 3 type-coercion gaps detected (`object` → `string` for `product_name`, `category`, `data_source`); Agent 2 generates and registers 3 functions: `transform_product_name`, `transform_category`, `transform_data_source`
- Full pipeline executes: DQ scoring, cleaning, quantity extraction, allergen scan, fuzzy dedup, merge, golden record selection, 3-strategy enrichment (S1 extraction seeds the FAISS corpus; S2 KNN resolves most remaining categories; S3 RAG-LLM handles stragglers), post-DQ scoring
- Output saved to `output/usda_fooddata_sample_unified.csv`

**Run 2 — FDA Recalls (`safety` domain):**
- Unified schema exists. Agent 1 diffs FDA columns against it — 3 registry hits from Run 1's functions, 1 new gap: `published_date` is `int64` (YYYYMMDD integer) but the unified schema expects `string`
- Agent 2 generates 1 function (`transform_published_date`), validates it in the sandbox, and registers it (4 total registered functions)
- Pipeline runs with both pre-built blocks and the newly generated function
- Output saved to `output/fda_recalls_sample_unified.csv`

**Run 3 — FDA Recalls (replay):**
- Same source as Run 2. Agent 1 detects the same 4 gaps
- Function registry now has 4 hits (3 from Run 1 + 1 from Run 2) — **zero LLM cost**
- Agent 2 is never called. Pipeline runs using cached transforms
- Demonstrates the self-extending "pipeline remembered" behavior
