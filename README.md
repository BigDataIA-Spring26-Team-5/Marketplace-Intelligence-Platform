# Schema-Driven Self-Extending ETL Pipeline

A three-agent, LangGraph-orchestrated ETL pipeline that ingests heterogeneous food product data sources, automatically detects schema gaps, resolves simple gaps declaratively via YAML mappings, generates Python transformation code via LLM for complex derivations, and produces a unified product catalog enriched with categories, allergens, dietary tags, and data quality scores.

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

The pipeline follows a **three-agent architecture** built on [LangGraph](https://github.com/langchain-ai/langgraph):

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
                    +-------------+-------------+
                    |  Registry Check +          |
                    |  YAML Mapping Build        |
                    |  (simple gaps → YAML;      |
                    |   DERIVE gaps → misses)    |
                    +-------------+-------------+
                                  |
                     +------------+------------+
                     |                         |
               No DERIVE gaps          DERIVE gaps remain
                     |                         |
                     |            +------------+------------+
                     |            |  Agent 2 — Code Gen     |
                     |            |  (LLM generates Python  |
                     |            |   blocks for DERIVE gaps)|
                     |            +------------+------------+
                     |                         |
                     |            +------------+------------+
                     |            |  Sandbox Validation     |
                     |            |  (static + subprocess)  |
                     |            +------------+------------+
                     |                         |
                     |            +------------+------------+
                     |            |  Register Blocks        |
                     |            +------------+------------+
                     |                         |
                     +------------+------------+
                                  |
                    +-------------+-------------+
                    |  Agent 3 — Sequence        |
                    |  Planner (LLM orders       |
                    |  block execution)          |
                    +-------------+-------------+
                                  |
                    +-------------+-------------+
                    |  Pipeline Execution        |
                    |  (pre-built blocks +       |
                    |   YAML mapping block +     |
                    |   generated DERIVE blocks) |
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

1. **Self-extending**: When a new data source has schema gaps, simple type/format operations are resolved declaratively via a generated YAML mapping file (zero LLM cost). Only complex derivations that require custom logic trigger LLM code generation. Both artifacts are persisted and reused on subsequent runs.
2. **Schema-driven**: A unified schema (`config/unified_schema.json`) defines the target output format. Every incoming source is diffed against this schema, and gaps are addressed automatically.
3. **Cascading enrichment**: Enrichment proceeds through three strategies of increasing cost. Cheap deterministic extraction handles safety-critical fields; KNN corpus search handles category via product-to-product comparison; the LLM is only called as a last resort with RAG context from real examples.
4. **Human-in-the-loop (HITL)**: The Streamlit UI exposes approval gates at schema mapping, code review, and quarantine stages. Missing columns can be individually accepted as null, given a default value, or excluded from schema requirements.

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
+-- src/blocks/generated/
|   +-- nutrition/
|   |   +-- DYNAMIC_MAPPING_*.yaml   # Declarative column ops (type_cast, set_null, format_transform)
|   |   +-- DERIVE_*.py              # Python Block classes for complex derivations (DERIVE gaps only)
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
|   |   +-- dynamic_mapping.py       # DynamicMappingBlock — YAML-driven declarative transforms
|   |   +-- mapping_io.py            # YAML read/write utilities + HITL decision merging
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
|   |   +-- block_registry.py        # Discovers and serves pre-built blocks; supports runtime registration
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
| **Run 1** | `data/usda_fooddata_sample.csv` | First run — Agent 1 diffs USDA columns against the unified schema, detects column mappings and type gaps. `check_registry_node` resolves all gaps as YAML `type_cast` operations and writes `DYNAMIC_MAPPING_usda_fooddata_sample.yaml`. Agent 2 is skipped. Full block sequence executes including deduplication, enrichment, and DQ scoring. |
| **Run 2** | `data/usda_sample_raw.csv` | Second source — different raw USDA format. Agent 1 detects its own set of gaps; `check_registry_node` writes a separate `DYNAMIC_MAPPING_usda_sample_raw.yaml`. Each source gets its own declarative mapping file. |
| **Replay** | Either source | Re-running with a known source: `BlockRegistry` auto-discovers the existing `DYNAMIC_MAPPING_` YAML, `check_registry_node` finds it as a registry hit — zero LLM cost. Demonstrates the "pipeline remembered" behavior. |

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
| **Input** | `source_path`, `source_df`, `source_schema`, `domain`, `enable_enrichment` | `load_source_node` |
| **Schema Analysis** | `unified_schema`, `unified_schema_existed`, `gaps`, `derivable_gaps`, `missing_columns`, `column_mapping`, `mapping_warnings`, `enrichment_columns_to_generate` | `analyze_schema_node` |
| **HITL** | `missing_column_decisions` | Streamlit UI (before `check_registry_node`) |
| **Registry** | `block_registry_hits`, `registry_misses`, `mapping_yaml_path` | `check_registry_node` |
| **Code Generation** | `generated_blocks`, `retry_count`, `max_retries` | `generate_code_node` |
| **Execution** | `block_sequence`, `sequence_reasoning`, `working_df`, `dq_score_pre`, `dq_score_post` | `plan_sequence_node`, `run_pipeline_node` |
| **Enrichment** | `enrichment_stats` (`s1_extraction`, `s2_knn`, `s3_rag_llm`, `unresolved`) | `run_pipeline_node` (via `LLMEnrichBlock`) |
| **Quarantine** | `quarantined_df`, `quarantine_reasons` | `run_pipeline_node` |
| **Audit** | `audit_log`, `errors` | `run_pipeline_node` |

Also defines `GapItem`, `DerivedGap`, `MissingColumn` (gap classification types), and `GeneratedBlock` (a Python Block generated by Agent 2 with its validation status).

---

## Agent 1 — Orchestrator: `src/agents/orchestrator.py`

Agent 1 handles schema intelligence. It exposes three LangGraph node functions:

### `load_source_node(state)`
- Reads the CSV at `state["source_path"]` into a pandas DataFrame
- Calls `profile_dataframe()` from `src/schema/analyzer.py` to compute per-column metadata (dtype, null rate, unique count, sample values)
- Returns `{source_df, source_schema}`

### `analyze_schema_node(state)`
- Loads the unified schema from `config/unified_schema.json` via `load_unified_schema()`; raises `FileNotFoundError` if absent (the unified schema must be defined before running the pipeline)
- Filters out computed/enrichment columns, then sends both the source profile and the mappable unified schema to the LLM with `SCHEMA_ANALYSIS_PROMPT`
- The LLM returns a richer classification than before:
  - `column_mapping` — direct renames (MAP)
  - `derivable_gaps` — gaps resolvable by transforming source data: actions `TYPE_CAST`, `FORMAT_TRANSFORM`, or `DERIVE`
  - `missing_columns` — unified schema columns with no source data and no derivation path
- A backward-compat `gaps` list (union of derivable_gaps + missing_columns) is also written to state
- Returns `{unified_schema, unified_schema_existed, column_mapping, gaps, derivable_gaps, missing_columns, enrichment_columns_to_generate, mapping_warnings}`

### `check_registry_node(state)`
Three-phase gap resolution:

**Phase A — Missing columns → YAML `set_null`**: For each `missing_column` not serviced by an enrichment block provider, adds a `set_null` operation (typed null column) to the pending YAML operations list.

**Phase B — Derivable gaps → registry or YAML**: For each `derivable_gap`, checks `BlockRegistry` for an enrichment provider or previously generated block. Gaps without a hit are routed by action type:
- `TYPE_CAST` / `FORMAT_TRANSFORM` → added to YAML operations as `type_cast` / `format_transform`
- `DERIVE` → added to `registry_misses` for Agent 2 code generation

**Phase C — Register DynamicMappingBlock**: If any YAML operations were collected, writes them to `src/blocks/generated/<domain>/DYNAMIC_MAPPING_<dataset>.yaml` and instantiates + registers a `DynamicMappingBlock`. HITL decisions from `missing_column_decisions` are merged in before writing.

Returns `{block_registry_hits, registry_misses, mapping_yaml_path, retry_count: 0, max_retries: 2}`

---

## Agent 2 — Code Generator: `src/agents/code_generator.py`

Agent 2 handles Python Block generation for **`DERIVE` gaps only** — transformations too complex to express declaratively. Simple `TYPE_CAST`, `FORMAT_TRANSFORM`, and `MISSING` gaps are handled upstream by `DynamicMappingBlock` via YAML. If `registry_misses` contains no `DERIVE` gaps, Agent 2 is skipped entirely.

### `generate_code_node(state)`
- Filters `registry_misses` to `DERIVE` gaps only; returns immediately if none remain
- For each DERIVE gap, constructs a prompt using `CODEGEN_PROMPT` (or `CODEGEN_RETRY_PROMPT` on retry with previous error and code)
- Calls the DeepSeek LLM via `call_llm()` to generate a Python `Block` subclass
- Strips markdown fences from the response via `_clean_code_response()`
- Validates inline via `_validate_block_code()` (syntax → safety → runtime)
- Returns `{generated_blocks: [...], retry_count: N+1}`

### `validate_code_node(state)`
- Pass-through node — exists solely for the conditional edge to inspect `generated_blocks` and decide whether to retry, register, or skip

### `register_blocks_node(state)`
- For each block that passed validation, writes the Python code to `src/blocks/generated/<domain>/<block_name>.py`
- Calls `BlockRegistry.instance().refresh()` to reload generated blocks via importlib discovery
- Returns the updated `generated_blocks` list with `file_path` populated

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
| `CODEGEN_PROMPT` | Agent 2 (first attempt) | Generates a Python `Block` subclass for a `DERIVE` gap. Includes the block template, gap details, safe NA patterns, and banned import constraints. Only invoked when `TYPE_CAST`/`FORMAT_TRANSFORM` YAML handling is insufficient. |
| `CODEGEN_RETRY_PROMPT` | Agent 2 (retry after validation failure) | Provides the previous code and the error message, asking the LLM to fix the block. |

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

## DynamicMappingBlock: `src/blocks/dynamic_mapping.py`

`DynamicMappingBlock` is a declarative, YAML-driven block that handles all simple schema operations — replacing what was previously done via LLM-generated Python for type casts, format transforms, and missing columns. It is instantiated and registered at runtime by `check_registry_node` after writing the YAML file.

**YAML format** (`src/blocks/generated/<domain>/DYNAMIC_MAPPING_<dataset>.yaml`):
```yaml
column_operations:
  - target: product_name
    type: string
    action: type_cast
    source: description
    source_type: object
  - target: published_date
    type: string
    action: format_transform
    source: date_int
    transform: to_string
  - target: serving_size
    type: float
    action: set_null
```

**Supported actions:**

| Action | Description |
|--------|-------------|
| `set_null` | Creates a typed null column (no source data) |
| `set_default` | Creates a column with a user-specified default value (HITL override) |
| `type_cast` | Converts source column to target type (string, integer, float, boolean) |
| `format_transform` | Applies named transforms: `to_string`, `parse_date`, `to_lowercase` |

YAML I/O is handled by `src/blocks/mapping_io.py`. HITL decisions from `missing_column_decisions` state field (set by the Streamlit UI before `check_registry_node`) are merged into operations via `merge_hitl_decisions()` before the file is written, allowing users to override `set_null` with `set_default` or mark a column as excluded from schema requirements.

---

## Pipeline Runner: `src/pipeline/runner.py`

The `PipelineRunner` class executes blocks in sequence on a DataFrame, producing an audit log of every step:

1. **Column mapping** is applied first — renames source columns to unified names
2. Expands the `__generated__` sentinel — replaced by all blocks in `BlockRegistry` whose names match generated prefixes (`DYNAMIC_MAPPING_`, `DERIVE_`, `TYPE_CONVERSION_`, etc.)
3. Iterates through the expanded `block_sequence` list — looks up each block by name in `BlockRegistry`, calls `block.run(df, config)`, and records an audit entry
4. Returns `(result_df, audit_log)` where each audit entry contains `{block, rows_in, rows_out, ...}`

The `DynamicMappingBlock` (registered at runtime, named `DYNAMIC_MAPPING_<domain>`) is picked up by the `__generated__` expansion and runs its YAML-defined column operations before the standard normalization blocks.

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
- Agent 1 diffs USDA columns against `config/unified_schema.json` — detects column mappings (e.g., `description` → `product_name`) and type gaps (`object` → `string`)
- `check_registry_node` classifies the type gaps as `TYPE_CAST` operations and writes them to `src/blocks/generated/nutrition/DYNAMIC_MAPPING_usda_fooddata_sample.yaml`; a `DynamicMappingBlock` is instantiated and registered — **no LLM code generation needed**
- Agent 2 is skipped (no DERIVE gaps)
- Full pipeline executes: DQ scoring, YAML mapping block (type casts), cleaning, quantity extraction, allergen scan, fuzzy dedup, merge, golden record selection, 3-strategy enrichment (S1 extraction seeds the FAISS corpus; S2 KNN resolves most remaining categories; S3 RAG-LLM handles stragglers), post-DQ scoring
- Output saved to `output/usda_fooddata_sample_unified.csv`

**Run 2 — USDA Raw (`nutrition` domain, different source):**
- Agent 1 diffs the raw USDA source against the unified schema — different column names and type gaps
- `check_registry_node` writes a new YAML file `DYNAMIC_MAPPING_usda_sample_raw.yaml` for this source's gaps
- Pipeline runs with its own `DynamicMappingBlock` — the YAML for Run 1's source is untouched

**Replay runs:**
- On subsequent runs with the same source, the YAML file already exists in `src/blocks/generated/`; `BlockRegistry` auto-discovers it, and `check_registry_node` finds the existing `DYNAMIC_MAPPING_` block as a registry hit — **zero LLM cost**
- Demonstrates the self-extending "pipeline remembered" behavior: declarative YAML for simple gaps, generated Python only for complex `DERIVE` gaps
