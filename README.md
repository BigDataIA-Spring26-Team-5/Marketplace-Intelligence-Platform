# Schema-Driven ETL Pipeline

Schema-driven ETL pipeline for heterogeneous food product datasets. Auto-detects schema gaps against a unified 14-column schema, generates YAML-based transforms via a 3-agent LangGraph flow, and executes them in a chunked streaming pipeline with multi-tier enrichment and DQ scoring.

## Architecture

No runtime code generation. Dataset-specific transforms persist as YAML under `src/blocks/generated/<domain>/` and replay on later runs.

### Pipeline Graph (`src/agents/graph.py`)

| Node | What it does |
|------|-------------|
| `load_source` | Auto-detect delimiter, adaptive sampling (~5K rows) |
| `analyze_schema` | Agent 1 maps source → unified schema; emits RENAME/CAST/FORMAT/DELETE/ADD/SPLIT/UNIFY/DERIVE ops |
| `critique_schema` | Agent 2 (reasoning model) validates ops against 7 deterministic rules |
| `check_registry` | HITL gate — human decides handling for missing columns |
| `plan_sequence` | Agent 3 reorders registered blocks (cannot add/remove) |
| `run_pipeline` | Executes block sequence, 10K rows/chunk |
| `save_output` | Persists clean + quarantined DataFrames |

**LLMs**: `deepseek/deepseek-chat` (Agents 1 & 3), `deepseek/deepseek-reasoner` (Agent 2) via LiteLLM.

### Blocks (`src/blocks/`)

- **13 static blocks**: cleaning (`strip_whitespace`, `lowercase_brand`, `remove_noise_words`), dedup (`fuzzy_deduplicate`, `golden_record_select`), enrichment (`extract_allergens`, `llm_enrich`), DQ scoring (`dq_score_pre`, `dq_score_post`)
- **Dynamic blocks**: generated from `src/blocks/generated/<domain>/DYNAMIC_MAPPING_*.yaml` — one file per source dataset

### Enrichment Tiers (`src/enrichment/`)

Cascading, three-tier for missing fields:

1. **S1 Deterministic** — rule-based for `allergens`, `dietary_tags`, `is_organic`
2. **S2 KNN** — FAISS + sentence-transformers for `primary_category`
3. **S3 LLM** — deepseek-chat + retrieved context for low-confidence categories

> **Safety boundary**: `allergens`, `dietary_tags`, `is_organic` are S1-only — never sent to probabilistic tiers.

### Core Behaviors

- **Schema-first**: every dataset compared to `config/unified_schema.json` before execution
- **DQ enforcement**: `dq_score_pre`/`dq_score_post` computed; rows failing required-field validation are quarantined
- **Checkpoint/resume**: SQLite-backed state at `checkpoints.db` via `src/pipeline/checkpoint/`
- **Audit trail**: every block logs `rows_in`/`rows_out`

### Observability (`src/uc2_observability/`)

- Dashboard: run history, DQ trends, cost tracking
- Anomaly detection on enrichment stats
- RAG chatbot over stored run logs (ChromaDB)

## Setup

**Requirements**: Python 3.11, Poetry

```bash
poetry install
cp .env.example .env   # fill in DEEPSEEK_API_KEY
```

**Key env vars** (`.env`):

```
DEEPSEEK_API_KEY=sk-...
FAISS_INDEX_PATH=corpus/faiss_index.bin
METADATA_PATH=corpus/metadata.json
CHROMA_DB_PATH=.specify/chroma_db
PIPELINE_RUN_TYPE=dev   # dev | demo | prod
```

**Docker services** (Kafka, Zookeeper, Airflow):

```bash
docker-compose up
```

## Running

```bash
# Streamlit HITL wizard
streamlit run app.py

# CLI
python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition
python -m src.pipeline.cli --source data/fda_recalls_sample.csv --resume
python -m src.pipeline.cli --source data/usda_sample_raw.csv --force-fresh

# Build FAISS corpus index
python scripts/build_corpus.py
```

## Tests & Lint

```bash
cd src && pytest && ruff check .
```

## Project Layout

```
src/
├── agents/          # LangGraph nodes + prompts + state
├── blocks/          # Static blocks + DynamicMappingBlock
│   └── generated/   # Per-domain/dataset YAML mappings
├── enrichment/      # S1 deterministic, S2 KNN, S3 LLM
├── models/          # LiteLLM model wrappers
├── pipeline/        # Runner, CLI, checkpoint manager
├── registry/        # Block registry
├── schema/          # Analyzer, sampler, schema models
├── ui/              # Streamlit components
├── uc2_observability/
├── uc3_search/
└── uc4_recommendations/

config/
├── unified_schema.json   # Master 14-column schema
└── litellm_config.yaml

specs/               # Feature specs (001–006)
```

## Contributor Notes

- Do not reintroduce runtime-generated Python transforms — YAML-only is a constitutional constraint.
- Changes to schema handling, enrichment safety, DQ scoring, or quarantine behavior must update the relevant spec/plan/tasks artifacts in the same PR.
- Keep architecture docs aligned with the three-agent, YAML-only flow.
