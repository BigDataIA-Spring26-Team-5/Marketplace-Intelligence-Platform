# ETL Platform Revamp — Domain-Pack Architecture

## Vision

Sell a **Schema-Adaptive ETL Platform** to Data Engineers. DE describes their domain once through a setup wizard. An agentic layer converts that description into executable artifacts. The pipeline runs with zero custom Python.

Target buyer: Data Engineer at a mid-to-large org with heterogeneous data sources, no time to write custom ETL, needs observability baked in.

---

## Core Problem with Current State

The kernel (schema detection, YAML transform engine, dedup, DQ scoring, UC2 observability) is genuinely domain-agnostic. But it is tangled with a food/nutrition reference implementation at every layer:

| Layer | Tangle |
|---|---|
| Agent prompts | Few-shot examples use food columns (`allergens`, `brand_name`, `ingredient_statement`) |
| Block registry | Nutrition/pricing branch logic hardcoded inline |
| Enrichment | `deterministic.py` regex rules are food-specific (FDA Big-9, USDA organic, dietary labels) |
| Gold pipeline | OpenFDA safety join always attempted |
| Airflow DAGs | USDA, OpenFDA, OpenFoodFacts hand-wired per source |
| Runner | `NULL_RATE_COLUMNS` hardcoded to food column names |

A new customer working on retail inventory or financial transactions must surgically remove food assumptions before the kernel is visible. That is not sellable.

---

## What Is the Kernel

Files that ship unchanged to every customer:

```
src/agents/
    graph.py                  — LangGraph orchestration (7-node pipeline)
    state.py                  — PipelineState TypedDict
    orchestrator.py           — Agent 1: schema analysis + YAML emission
    critic.py                 — Agent 2: gap validation (--with-critic)
    confidence.py             — confidence scoring for operations
    guardrails.py             — validation guardrails
    prompts.py                — prompt templates (examples injected at runtime from domain pack)

src/blocks/
    base.py                   — Block ABC
    strip_whitespace.py
    lowercase_brand.py
    remove_noise_words.py
    strip_punctuation.py
    fuzzy_deduplicate.py
    column_wise_merge.py
    golden_record_select.py
    dq_score.py
    llm_enrich.py             — S1→S2→S3 enrichment orchestrator
    dynamic_mapping.py        — declarative YAML action executor
    mapping_io.py             — YAML I/O

src/enrichment/
    corpus.py                 — FAISS KNN vector store
    embedding.py              — SentenceTransformer embedding pipeline
    llm_tier.py               — S3 RAG-LLM enrichment
    rate_limiter.py

src/pipeline/
    runner.py                 — chunked pipeline executor
    cli.py                    — primary CLI entry point
    gold_pipeline.py          — Silver → Gold aggregation

src/models/llm.py             — LiteLLM multi-provider router
src/cache/client.py           — Redis/SQLite cache layer
src/registry/block_registry.py — block discovery + sequence builder (mechanism only)
src/uc2_observability/        — entire directory (fully generic)
airflow/dags/
    bronze_to_silver_dag.py
    silver_to_gold_dag.py     (structure only)
    uc2_anomaly_dag.py
    uc2_chunker_dag.py
```

---

## What Is the Food-Demo (Reference Implementation)

Files that become `domain_packs/nutrition/` — not shipped as core, used as reference:

```
src/blocks/extract_allergens.py         — FDA Big-9 allergen keyword scan
src/blocks/extract_quantity_column.py   — serving size regex (oz, g, mg)
src/enrichment/deterministic.py         — CATEGORY_RULES, DIETARY_RULES, ORGANIC_PATTERN
config/schemas/nutrition_schema.json
config/schemas/safety_schema.json
airflow/dags/usda_dag.py
airflow/dags/openfda_incremental_dag.py
airflow/dags/off_incremental_dag.py
scripts/build_corpus.py                 — USDA FoodData Central hardcoded download
```

---

## Domain Pack — The New Primitive

A Domain Pack is a self-contained directory that configures the kernel for a specific vertical. No Python required from the DE.

### File Structure

```
domain_packs/
└── <domain_name>/
    ├── schema.json              — target unified schema: columns, types, DQ weights
    ├── enrichment_rules.yaml    — deterministic extraction rules (regex, keywords, flags)
    ├── prompt_examples.yaml     — few-shot column mapping examples injected into Agent 1/2
    ├── block_sequence.yaml      — ordered block list for this domain
    ├── custom_blocks/           — generated Python block classes (only if YAML insufficient)
    │   └── extract_<field>.py
    └── dag_config.yaml          — source connection (type, URL, auth, format, partition key)
```

### schema.json

```json
{
  "domain": "retail_inventory",
  "record_represents": "SKU",
  "columns": [
    { "name": "sku_id",         "type": "string",  "required": true,  "source": "mapped" },
    { "name": "product_name",   "type": "string",  "required": true,  "source": "mapped" },
    { "name": "category",       "type": "string",  "required": false, "source": "inferred" },
    { "name": "hazard_code",    "type": "string",  "required": false, "source": "extracted" },
    { "name": "dq_score_pre",   "type": "float",   "required": false, "source": "computed" },
    { "name": "dq_score_post",  "type": "float",   "required": false, "source": "computed" }
  ],
  "dq_weights": { "completeness": 0.4, "validity": 0.4, "consistency": 0.2 }
}
```

`source` field drives what Agent 1 can map vs. what enrichment must produce vs. what the runner computes. Agent 1 only sees `mapped` columns as mappable targets — same exclusion logic as current enrichment/computed column filter.

### enrichment_rules.yaml

```yaml
domain: retail_inventory
fields:
  - name: hazard_code
    strategy: deterministic
    patterns:
      - regex: "HAZ-\\d{4}"
        label: "hazmat"
      - keywords: ["flammable", "corrosive", "explosive"]
        label: "dangerous_goods"
  - name: category
    strategy: llm
    classification_classes:
      - Electronics
      - Apparel
      - Hardware
      - Consumables
    rag_context_field: product_name
```

`strategy: deterministic` → S1 only. `strategy: llm` → falls through S1→S2→S3. Safety boundary: deterministic fields never get LLM inference — same invariant, now config-driven instead of hardcoded.

### prompt_examples.yaml

```yaml
domain: retail_inventory
column_mapping_examples:
  - source_col: "SKU_Code"
    target_col: "sku_id"
    operation: RENAME
  - source_col: "ItemDesc"
    target_col: "product_name"
    operation: RENAME
  - source_col: "Qty_OnHand"
    target_col: "quantity"
    operation: CAST
    cast_to: integer
  - source_col: "UnitPrice"
    target_col: "price_usd"
    operation: CAST
    cast_to: float
```

Agent 1 loads these at runtime and injects into few-shot context. Replaces the hardcoded food examples in `prompts.py`.

### block_sequence.yaml

```yaml
domain: retail_inventory
sequence:
  - dq_score_pre
  - __generated__
  - strip_whitespace
  - remove_noise_words
  - fuzzy_deduplicate
  - column_wise_merge
  - golden_record_select
  - extract_hazard_code        # from custom_blocks/ if generated
  - llm_enrich
  - dq_score_post
```

Registry reads this file instead of inline Python branching. `__generated__` sentinel preserved — runner still injects DynamicMappingBlock at that position.

### dag_config.yaml

```yaml
domain: retail_inventory
source:
  type: rest_api
  url: "https://api.inventory-system.com/v2/products"
  auth:
    type: bearer
    env_var: INVENTORY_API_KEY
  format: json
  pagination:
    strategy: cursor
    cursor_field: next_cursor
  partition:
    field: updated_at
    granularity: daily
output:
  bronze_prefix: "gs://my-bucket/bronze/retail_inventory"
  silver_prefix:  "gs://my-bucket/silver/retail_inventory"
```

DAG factory reads this → generates Airflow DAG at scheduler startup. No new DAG Python file.

---

## Agentic Setup Layer

One agent with three tools that fires when DE clicks "Generate Domain Pack."

### Tool 1: `generate_schema`

- Input: domain name, record description, column specs (name + description + required/optional + extraction type)
- Process: LLM maps DE descriptions to typed schema columns, assigns `source` field (mapped/extracted/computed/inferred), sets DQ weights by column criticality
- Output: `domain_packs/<domain>/schema.json`

### Tool 2: `generate_enrichment_rules`

- Input: per-field extraction descriptions + 5-10 sample data rows
- Process:
  1. LLM generates candidate regex patterns from DE's keyword descriptions
  2. Validates patterns against sample rows
  3. Determines if deterministic coverage is sufficient or S3 LLM needed
  4. If S3: generates classification class list from DE's description
  5. If regex insufficient → generates `custom_blocks/extract_<field>.py` (minimal Block subclass)
- Output: `domain_packs/<domain>/enrichment_rules.yaml` + optional `custom_blocks/`
- Constraint: fields marked `strategy: deterministic` in output NEVER appear in S2/S3 paths — safety boundary preserved

### Tool 3: `generate_dag_config`

- Input: source type selection + connection details from wizard UI
- Process: validates connection (test API call or file read), infers partition field from sample response, selects pagination strategy
- Output: `domain_packs/<domain>/dag_config.yaml`

### Validation Step (before writing to disk)

Agent dry-runs generated artifacts on the DE's sample data:
1. Load sample CSV/JSON
2. Apply `schema.json` column mapping
3. Run `enrichment_rules.yaml` deterministic pass
4. Show block trace in UI (same waterfall as existing Streamlit pipeline page)
5. DE sees rows in → rows out, null rates, DQ score
6. If validation passes → write files. If not → show diff, allow regeneration of individual artifacts.

---

## Kernel Refactor Required

Before domain packs work, three kernel changes:

### 1. Dynamic Prompt Injection (`src/agents/prompts.py`)

```python
# Current: hardcoded food examples baked into prompt string
# Target: load from domain pack at node entry

def build_schema_analysis_prompt(domain: str) -> str:
    examples = load_prompt_examples(f"domain_packs/{domain}/prompt_examples.yaml")
    return SCHEMA_ANALYSIS_PROMPT_TEMPLATE.format(examples=render_examples(examples))
```

`analyze_schema_node` calls `build_schema_analysis_prompt(state["domain"])` instead of using the module-level constant.

### 2. Registry Reads `block_sequence.yaml` (`src/registry/block_registry.py`)

```python
# Current: inline if/elif branching on domain string
# Target:

def get_default_sequence(domain: str, ...) -> list[str]:
    pack_path = Path(f"domain_packs/{domain}/block_sequence.yaml")
    if pack_path.exists():
        return yaml.safe_load(pack_path.read_text())["sequence"]
    return FALLBACK_SEQUENCE  # generic cleaning only, no enrichment
```

Custom blocks in `domain_packs/<domain>/custom_blocks/` discovered at startup same way generated blocks are discovered from `src/blocks/generated/`.

### 3. Runner Column Names from Schema (`src/pipeline/runner.py`)

```python
# Current: NULL_RATE_COLUMNS = ["product_name", "brand_name", "ingredients", "primary_category"]
# Target:

def _get_null_rate_columns(domain: str) -> list[str]:
    schema = load_schema(f"domain_packs/{domain}/schema.json")
    return [col["name"] for col in schema["columns"] if col["required"]]
```

### 4. DAG Factory (`airflow/dags/dag_factory.py`)

```python
# New file. Scans domain_packs/*/dag_config.yaml at Airflow startup.
# For each config, registers a parameterized DAG (bronze ingest + bronze_to_silver trigger).
# Replaces usda_dag.py, openfda_incremental_dag.py, off_incremental_dag.py.
```

---

## Setup Wizard UI

New tab in `app.py` sidebar: **"Domain Setup"**. Five steps, Streamlit form pages.

```
Step 1: Domain Identity
        ├── Domain name (slug, no spaces)
        ├── Display name
        ├── What does one record represent? (free text → fed to Agent)
        └── Industry vertical (dropdown: retail, finance, healthcare, logistics, other)

Step 2: Target Schema Designer
        ├── Add columns: name, description, data type, required, source type
        ├── Upload sample CSV/JSON → auto-suggest columns via Agent
        └── Preview unified schema table

Step 3: Enrichment Definition
        ├── For each "extracted" column:
        │   ├── Describe what to extract (free text)
        │   ├── Provide example values
        │   └── Paste keywords/patterns if known
        ├── For each "inferred" column:
        │   ├── Describe classification task
        │   └── List possible classes
        └── Review safety boundary: confirm which fields are deterministic-only

Step 4: Source Connection
        ├── Source type: REST API | GCS | S3 | PostgreSQL | local CSV
        ├── Connection config (type-specific form)
        ├── Test connection button
        └── Preview: first 20 rows from source

Step 5: Review & Generate
        ├── Summary of all inputs
        ├── [Generate Domain Pack] button → fires agentic layer
        ├── Progress: schema → enrichment rules → dag config → validation dry-run
        ├── Block trace waterfall on sample data
        ├── Per-artifact diff view (editable inline)
        ├── [Regenerate <artifact>] buttons
        └── [Save Domain Pack] → writes files, registers with registry
```

---

## What DE Does After Setup

```bash
# 1. Complete wizard → domain pack written to domain_packs/<domain>/

# 2. Run pipeline
poetry run python -m src.pipeline.cli \
  --source my_data.csv \
  --domain my_domain

# 3. Or GCS source with Airflow (DAG auto-registered from dag_config.yaml)
#    No manual DAG file needed.

# 4. Monitor
poetry run streamlit run app.py
# → Observability tab: RAG chatbot over run history, anomaly alerts, Prometheus metrics
```

Zero custom Python written by DE. Domain Pack is the only artifact they own.

---

## Build Order

| Phase | Work | Days |
|---|---|---|
| 1 | Domain Pack file format spec + JSON/YAML schemas for all 5 artifact types | 1 |
| 2 | Kernel refactor: prompt injection, registry reads yaml, runner column names from schema | 2 |
| 3 | DAG factory: parameterized DAG from dag_config.yaml, replaces hand-written source DAGs | 1 |
| 4 | Nutrition domain pack: migrate existing food code into `domain_packs/nutrition/` as reference impl | 1 |
| 5 | Agentic setup agent: 3 tools (generate_schema, generate_enrichment_rules, generate_dag_config) | 2 |
| 6 | Validation loop: dry-run on sample data, block trace output, per-artifact regeneration | 1 |
| 7 | Streamlit wizard UI: 5-step flow wired to agentic layer | 2 |
| **Total** | | **~10 days** |

Phases 1-4 are pure kernel work — no UI, no new agent. Phases 5-7 build the product front door. Can demo after Phase 4 using CLI. Full wizard demo after Phase 7.

---

## Selling Points (Post-Revamp)

| Feature | What DE Gets |
|---|---|
| Schema-Adaptive Transform | No ETL code for new sources. Agent detects gaps, writes YAML, runs declarative transforms. |
| Domain Pack Wizard | Describe domain in plain English → get working pipeline config in minutes. |
| Multi-Tier Enrichment | Deterministic → KNN → LLM cascade. Safety boundary enforced by config, not trust. |
| Checkpoint + Resume | SHA256-gated resume. Safe on GCS sources. No re-processing on failure. |
| UC2 Observability | Kafka audit trail, Prometheus metrics, RAG chatbot over run history, anomaly detection. |
| Zero Lock-in | YAML artifacts are human-readable. DE can edit directly. No black box. |

---

## What This Is NOT

- Not a managed service. DE runs on their infra (Docker Compose or Kubernetes).
- Not an auto-ML platform. Enrichment LLM calls are configurable, not opaque.
- Not replacing Airflow. DAG factory generates standard Airflow DAGs — DE's existing orchestration stays.
- UC3 (search) and UC4 (recommendations) remain scaffolding. Not part of this revamp.
