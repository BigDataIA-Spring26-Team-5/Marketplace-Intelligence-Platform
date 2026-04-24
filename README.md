# Marketplace Intelligence Platform (MIP)

**Course:** DAMG 7245 — Big Data Systems and Intelligent Analytics · Spring 2026 · Group 5

MIP is a domain-agnostic data intelligence platform. Point it at any structured data source, supply a domain schema, and it automatically generates YAML transforms, runs a Bronze → Silver → Gold ETL pipeline, enriches missing fields, scores data quality, and makes every run observable and queryable.

The food catalog (USDA, OpenFoodFacts, OpenFDA, Amazon ESCI) is the **reference implementation** — five incompatible source schemas onboarded without a single line of hand-written ingestion code.

**Live:** `35.239.47.242` — see [Live Endpoints](#live-endpoints).

---

## Architecture

Every source flows through a **Domain Pack** (schema + block sequence) into a three-agent LangGraph pipeline, producing Silver Parquet per source. Gold runs in two stages: per-source dedup+enrich → BigQuery, then cross-source concat+dedup → GCS canonical Parquet.

```mermaid
flowchart TD
    subgraph DP["Domain Pack  (domain_packs/<domain>/)"]
        Schema[canonical schema\nconfig/schemas/domain_schema.json]
        BSeq[block_sequence.yaml\nsilver_sequence · gold_sequence]
        CB[custom_blocks/*.py\noptional domain extensions]
    end

    subgraph Sources["Data Sources (reference: food catalog)"]
        S1[USDA Branded]
        S2[USDA Foundation]
        S3[OpenFoodFacts]
        S4[OpenFDA Recalls]
        S5[Amazon ESCI]
    end

    subgraph Bronze["Bronze  gs://mip-bronze-2024/"]
        B[source/YYYY/MM/DD/  JSONL part files]
    end

    subgraph AgentETL["3-Agent LangGraph  (per source × domain)"]
        Cache[(Redis YAML Cache\nschema fingerprint · 30d TTL)]
        A1[Agent 1 – Orchestrator\nmap source cols → domain schema\nemit RENAME·CAST·FORMAT·DERIVE ops]
        A2[Agent 2 – Critic  opt-in\nvalidate ops · reject bad ones]
        A3[Agent 3 – Planner\nreorder block sequence\ncannot add or remove blocks]
        YAML[DYNAMIC_MAPPING_source.yaml\nsrc/blocks/generated/domain/]
    end

    subgraph Silver["Silver  gs://mip-silver-2024/  (per source+date)"]
        SP[N Parquet part files\nschema-conformed to domain]
    end

    subgraph PSourceGold["Per-Source Gold  run_gold_pipeline"]
        CatP[concat part files\npd.concat frames]
        Ded1[fuzzy dedup\ncolumn_wise_merge\ngolden_record_select]
        EnrA["Enrichment Cascade  ↓"]
        BQ[(BigQuery  mip_gold.products\nappend-mode per source)]
    end

    subgraph EnrFlow["Enrichment Agents  (Agentic Cascade)"]
        E1[S1 – Deterministic\nregex · keyword rules\nallergens · dietary_tags · is_organic]
        E2[S2 – KNN Agent\nFAISS IndexFlatIP cosine top-5\nprimary_category only]
        E3[S3 – LLM-RAG Agent\nClaude Haiku + top-3 neighbors\nprimary_category only]
        Corpus[(FAISS Corpus\ngrows each run\nS2+S3 write resolved rows back)]
        E1 --> E2 --> E3
        E2 <--> Corpus
        E3 --> Corpus
    end

    subgraph CrossGold["Cross-Source Gold  run_domain_gold_gcs  --domain-gcs"]
        BQR[read all domain sources from BQ\nnutrition: usda-branded + usda-foundation + off\nsafety: openfda    retail: esci]
        CatBQ[pd.concat all source frames]
        XDed[cross-source fuzzy dedup\nskipped for single-source domains]
        SJoin[LEFT JOIN OpenFDA Silver\nnutrition only\nrecall_class · recall_reason · allergen override]
        DQR[recompute dq_score_post + dq_delta\non merged canonical columns]
        GCS[(GCS  mip-gold-2024/domain/date/\nParquet in 500k-row chunks)]
    end

    Sources --> Bronze
    DP --> AgentETL
    Bronze --> AgentETL
    A1 <--> Cache
    Cache -- "hit: skip A1·A2·A3" --> YAML
    A1 --> A2 --> A3 --> YAML
    YAML --> Silver

    Silver --> CatP --> Ded1 --> EnrA --> BQ
    EnrA -. drives .-> EnrFlow

    BQ --> BQR --> CatBQ --> XDed --> SJoin --> DQR --> GCS

    GCS --> Obs[Observability\nPrometheus · Grafana · RAG]
    GCS --> Search[Hybrid Search\nBM25 + FAISS · 99k products]
    GCS --> Recs[Recommendations\nRules + Graph · 105 rules]
```

### Domain Pack structure

```
domain_packs/<domain>/
├── block_sequence.yaml      # silver_sequence · sequence · gold_sequence
└── custom_blocks/*.py       # optional domain-specific Block subclasses

config/schemas/<domain>_schema.json   # canonical column definitions
src/blocks/generated/<domain>/        # DYNAMIC_MAPPING_<source>.yaml  (auto-generated)
                                      # VALIDATION_PROFILE_<source>.json
```

Six domains shipped: `nutrition` · `safety` · `retail` · `pricing` · `finance` · `manufacturing`. Add a new domain → author a schema JSON + block_sequence.yaml, run the CLI once.

---

## Agentic Flows

### LangGraph — 3-agent schema analysis

```mermaid
stateDiagram-v2
    [*] --> load_source
    load_source --> analyze_schema
    analyze_schema --> critique_schema : --with-critic + cache miss
    analyze_schema --> check_registry  : cache hit or critic off
    critique_schema --> check_registry
    check_registry --> plan_sequence
    plan_sequence --> run_pipeline
    run_pipeline --> save_output
    save_output --> [*]
```

| Agent | Role | Model |
|---|---|---|
| **Orchestrator** | Maps source columns → domain schema; emits YAML transform ops; writes `DYNAMIC_MAPPING_<source>.yaml` on first run | `deepseek/deepseek-chat` |
| **Critic** *(opt-in `--with-critic`)* | Validates ops against 7 deterministic rules; rejects or amends bad ops | `claude-sonnet-4-6` |
| **Planner** | Reorders block sequence from `block_sequence.yaml`; dropped blocks auto-re-appended before `dq_score_post` | `deepseek/deepseek-chat` |

Redis caches the full YAML blob keyed on schema fingerprint (30-day TTL) — cache hit skips all three agents entirely.

### Enrichment — 3-tier agentic cascade

```mermaid
flowchart LR
    Row[unresolved row] --> E1

    subgraph E1["S1 — Deterministic"]
        R1[regex + keyword rules\non the row's own text]
    end

    subgraph E2["S2 — KNN Agent"]
        R2[embed product text\nFAISS cosine top-5 neighbors\nvote threshold 0.45]
    end

    subgraph E3["S3 — LLM-RAG Agent"]
        R3[Claude Haiku\ntop-3 S2 neighbors as context\nconfidence floor 0.60]
    end

    Corpus[(FAISS Corpus\npersists across runs)]

    E1 -- "unresolved primary_category" --> E2
    E2 -- "below confidence threshold" --> E3
    E2 -- "resolved rows" --> Corpus
    E3 -- "resolved rows" --> Corpus
    E3 --> Done[enriched row]
```

> **Hard safety rule.** `allergens`, `is_organic`, `dietary_tags` — S1 extraction only, never inferred by S2 or S3. A false-positive allergen label is a regulatory-grade mistake. `LLMEnrichBlock` has a post-run assertion tripwire — if it fires, fix the upstream cause, do not silence it.

---

## Prerequisites

- Python 3.11+, Poetry
- Docker + Docker Compose
- GCP: GCS buckets `mip-bronze-2024` / `mip-silver-2024`, BigQuery project `mip-platform-2024`
- API keys: `ANTHROPIC_API_KEY`, `DEEPSEEK_API_KEY`, `GROQ_API_KEY`

---

## Setup

```bash
git clone https://github.com/BigDataIA-Spring26-MIP/Marketplace-Intelligence-Platform.git
cd Marketplace-Intelligence-Platform

cp .env.example .env          # fill API keys + GOOGLE_APPLICATION_CREDENTIALS

poetry install

docker-compose -p mip up -d   # Kafka, Airflow, Postgres, Prometheus, Pushgateway,
                               # Grafana, ChromaDB, Redis, MLflow

# One-time: seed enrichment corpus from USDA FoodData Central
poetry run python scripts/build_corpus.py --limit 10000
```

---

## Running

### Demo (fastest way to see it work)

```bash
poetry run python demo.py
# Runs USDA → FDA → FDA replay; third pass shows Redis cache skipping all 3 agents
```

### CLI — run any source

```bash
# Local CSV
poetry run python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition

# GCS JSONL (silver mode — schema transform only, no enrichment)
poetry run python -m src.pipeline.cli \
    --source "gs://mip-bronze-2024/off/2026/04/22/*.jsonl" --mode silver

# Resume after failure
poetry run python -m src.pipeline.cli --source data/fda_recalls_sample.csv --domain safety --resume

# Enable Agent 2 critic
poetry run python -m src.pipeline.cli --source data/usda_fooddata_sample.csv --domain nutrition --with-critic
```

### Gold layer

```bash
poetry run python -m src.pipeline.gold_pipeline --source off --date 2026/04/21
# Reads all Silver Parquet for source+date → dedup + enrichment → BigQuery mip_gold.products
```

### Streamlit wizard

```bash
poetry run streamlit run app.py
# http://localhost:8501
# Sidebar tabs: Pipeline (HITL gates) | Observability (RAG chatbot) | MLflow | EDA
```

### Services

```bash
# MCP observability API
uvicorn src.uc2_observability.mcp_server:app --host 0.0.0.0 --port 8001
# Swagger: http://localhost:8001/docs

# REST API (pipeline + search + recommendations)
uvicorn src.api.main:app --host 0.0.0.0 --port 8002
# Swagger: http://localhost:8002/docs
```

### Pipeline modes

| Mode | What runs | Output |
|---|---|---|
| `full` (default) | DQ pre → YAML transforms → clean → dedup → enrich → DQ post | CSV to `output/` |
| `silver` | Schema transform only | Parquet to GCS |
| `gold` | Dedup + enrichment + DQ on Silver Parquet | Append to BigQuery |

### Tests

```bash
poetry run pytest
poetry run pytest -m "not integration"   # skip GCS-dependent tests
cd src && ruff check .
```

Coverage: **81.72%** across 920 tests, 43 test files.

---

## Live Endpoints

| Service | URL | Credentials |
|---|---|---|
| Streamlit App | http://35.239.47.242:8502 | — |
| Airflow | http://35.239.47.242:8080 | `admin` / `admin` |
| Grafana | http://35.239.47.242:3000 | `admin` / `mip_admin` |
| MLflow | http://35.239.47.242:5000 | — |
| Prometheus | http://35.239.47.242:9090 | — |
| MCP Server | http://35.239.47.242:8001/docs | — |
| REST API | http://35.239.47.242:8002/docs | — |
| ChromaDB | http://35.239.47.242:8000 | — |

---

## Repo Layout

```
src/
├── agents/              # LangGraph nodes, prompts, guardrails
├── blocks/generated/    # YAML transforms per domain (auto-created on first run)
├── cache/               # Redis + SQLite fallback
├── enrichment/          # S1 deterministic · S2 KNN · S3 LLM-RAG + FAISS corpus
├── models/              # LiteLLM wrappers (5 task getters)
├── pipeline/            # runner, CLI, checkpoint manager
├── uc2_observability/   # metrics, chunker, RAG chatbot, MCP server, MLflow bridge
├── uc3_search/          # BM25 + FAISS hybrid search
└── uc4_recommendations/ # association rules + graph recommender

airflow/dags/            # 9 DAGs: ingest → Bronze → Silver → Gold → anomaly + chunker
config/schemas/          # canonical target schemas (6 domains)
```

---

## Work Disclosure

> **WE ATTEST THAT WE HAVEN'T USED ANY OTHER STUDENTS' WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.**

| Member | Contribution | Share |
|---|---|---|
| **Bhavya Likhitha** | Three-agent LangGraph flow; YAML mapping I/O; Redis cache + SQLite fallback; chunked streaming runner; checkpoint/resume; MLflow integration; MCP server for Claude Desktop | **33.3%** |
| **Aqeel** | UC2 observability plane (Prometheus, anomaly detection, ChromaDB chunker, Kafka→Postgres, MCP FastAPI server); three-tier enrichment cascade with allergen safety boundary; all 9 Airflow DAGs | **33.3%** |
| **Deepika** | Domain schema design and registration; source bootstrap path; enrichment + DQ column extensions; hybrid search indexer and evaluator; association-rule and graph recommendation engine; project documentation | **33.3%** |

**AI tools used:** Claude Code (architecture, scaffolding, MCP server, Streamlit UI, debugging), OpenGPT (prompt engineering, Airflow templates), GitHub Codex (boilerplate, test stubs), DeepSeek Chat (data processing utilities, cache client). All AI-generated code was reviewed and tested. Safety-field boundary violations suggested by AI were rejected and replaced with explicit guards.
