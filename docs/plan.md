# Marketplace Intelligence Platform — Implementation Plan

## Current Status
- GCP VM running at `35.239.47.242`
- Docker containers up: Kafka, Zookeeper, Airflow
- Kafka topics created: `source.off.deltas`, `source.openfda.recalls`, `pipeline.events`, `pipeline.metrics`
- GCS buckets ready: `mip-bronze-2024`, `mip-silver-2024`, `mip-gold-2024`
- Workspace: `/home/bhavyalikhitha_bbl/bhavya-workspace`
- `.env` has all credentials (GCS, USDA API, DeepSeek, Groq)
- Claude Code + Codex installed on VM
- VS Code Remote SSH configured for all teammates

---

## Phase 1 — Data Ingestion (Bronze Layer)

### 1.1 USDA Ingestion (Airflow DAG)
- **File:** `airflow/dags/usda_dag.py`
- Pull from USDA FoodData Central API using `USDA_API_KEY`
- Endpoint: `https://api.nal.usda.gov/fdc/v1/foods/search`
- Write raw JSON responses to `gs://mip-bronze-2024/usda/`
- Schedule: monthly
- Output format: JSONL files partitioned by date

### 1.2 openFDA Ingestion (Kafka Producer)
- **File:** `src/producers/openfda_producer.py`
- Poll `https://api.fda.gov/food/enforcement.json`
- Produce to Kafka topic `source.openfda.recalls`
- Kafka Connect S3 Sink writes to `gs://mip-bronze-2024/openfda/`

### 1.3 Open Food Facts Ingestion (Kafka Producer)
- **File:** `src/producers/off_producer.py`
- Stream from HuggingFace `openfoodfacts/product-database` (no local download)
- Produce to Kafka topic `source.off.deltas`
- Kafka Connect S3 Sink writes to `gs://mip-bronze-2024/off/`

### 1.4 Amazon ESCI Ingestion (Airflow DAG)
- **File:** `airflow/dags/esci_dag.py`
- One-time load from HuggingFace `tasksource/esci`
- Write to `gs://mip-bronze-2024/esci/`
- Schedule: one-time

### 1.5 Kafka Connect S3 Sink
- Add to `docker-compose.yml` as a new service
- Config: reads from all Kafka topics → writes to GCS bronze buckets
- Uses HMAC credentials from `.env`

---

## Phase 2 — Stage A Pipeline (Per Source)

### 2.1 Agent 1 — Schema Gap Analysis
- **File:** `src/agents/gap_analyzer.py`
- Read bronze data schema + samples
- Compare against `config/unified_schema.json`
- LLM classifies each column: MAP / DROP / NEW / ADD
- Output: `operations[]` per source

### 2.2 HITL 1 — Mapping Approval
- **File:** `src/ui/hitl1.py` (Streamlit)
- Show Agent 1 mappings to user
- User approves/rejects per column

### 2.3 Agent 2 — Code Generation
- **File:** `src/agents/code_generator.py`
- LLM generates Python transform functions for gaps
- Run in Docker sandbox (network=none, 5s timeout)
- Validate: static scan → sandbox → types → nulls → pytest
- Save to function registry on approval

### 2.4 HITL 2 — Code Review
- **File:** `src/ui/hitl2.py` (Streamlit)
- Show generated code + sandbox output + pytest results
- User approves → saved to registry

### 2.5 Function Registry
- **File:** `function_registry/registry.json`
- Stores approved transform functions per source/domain
- Agent 2 checks registry before generating new code

### 2.6 Agent 3 — Profile-Driven Sequencer
- **File:** `src/agents/sequencer.py`
- Profile bronze data (nulls, types, domain)
- Rule table decides which preprocessing blocks to run
- No LLM at runtime — pure rule engine

### 2.7 Preprocessing Blocks
- **Files:** `src/blocks/`
- `normalize_text.py`
- `remove_noise_words.py`
- `extract_quantity_column.py`
- `extract_allergens.py`
- Output → `gs://mip-silver-2024/pending/<source>/`

---

## Phase 3 — Stage B Pipeline (Union)

### 3.1 Concat all 4 sources
- Read from `gs://mip-silver-2024/pending/`
- `pd.concat` → `union_df`

### 3.2 DQ Pre-score
- **File:** `src/blocks/dq_score.py` (exists)
- Score each row before enrichment

### 3.3 ID Reconciliation
- **File:** `src/blocks/id_reconcile.py`
- Barcode match: `OFF.code ↔ USDA.gtin_upc`
- Fuzzy fallback for non-barcode matches

### 3.4 Fuzzy Deduplication
- **File:** `src/blocks/fuzzy_deduplicate.py` (exists)
- Double blocking + RapidFuzz
- Union-find clustering

### 3.5 Collapse Clusters
- **File:** `src/blocks/collapse_cluster.py`
- Multi-source merge → one golden row per product
- `sources=[OFF, USDA]` provenance field

### 3.6 Recall Annotation
- **File:** `src/blocks/recall_annotate.py`
- Match openFDA rows onto catalog
- Flip `has_recall`, set `recall_class`, `recall_reason`

### 3.7 LLM Enrichment (4-tier cascade)
- **File:** `src/blocks/llm_enrich.py` (exists)
- S1: Deterministic rules (~60%)
- S2: FAISS KNN (~25%)
- S3: Cluster propagation (~10%)
- S4: LLM via Groq/DeepSeek (~5%)

### 3.8 DQ Post-score + Delta
- **File:** `src/blocks/dq_score.py`
- Score after enrichment, compute `dq_delta`

### 3.9 HITL 3 — Schema Validation
- **File:** `src/ui/hitl3.py` (Streamlit)
- Show rows failing schema contract
- Pass → `gs://mip-silver-2024/unified_catalog/latest.parquet`
- Fail → quarantine

---

## Phase 4 — Downstream (UC2/UC3/UC4)

### 4.1 UC2 — Observability Dashboard
- Reads `pipeline.events` and `pipeline.metrics` Kafka topics
- Streamlit dashboard showing pipeline run stats, DQ scores, audit log

### 4.2 UC3 — Search
- Build search index from `unified_catalog`
- Write to `gs://mip-gold-2024/search_index/`

### 4.3 UC4 — Recommendations
- Build recommendation model from `unified_catalog`
- Write to `gs://mip-gold-2024/recommendations/`

---

## Data Layer Summary
```
GCS bronze   → raw source data (USDA, OFF, openFDA, ESCI)
GCS silver   → Stage A buffers + unified_catalog.parquet
GCS gold     → search index + recommendation outputs
Snowflake    → final catalog table (UC3 + UC4 query layer)
```

### Snowflake Setup (Phase 4)
- Free trial: 30 days / $400 credits
- Load `unified_catalog.parquet` from GCS silver → Snowflake table
- UC3 search and UC4 recommendations query Snowflake directly
- Use Snowflake stage + COPY INTO to load from GCS

---

## File Structure
```
bhavya-workspace/
├── airflow/
│   └── dags/
│       ├── usda_dag.py          ← Phase 1.1
│       └── esci_dag.py          ← Phase 1.4
├── src/
│   ├── producers/
│   │   ├── off_producer.py      ← Phase 1.3
│   │   └── openfda_producer.py  ← Phase 1.2
│   ├── agents/
│   │   ├── gap_analyzer.py      ← Phase 2.1
│   │   ├── code_generator.py    ← Phase 2.3
│   │   └── sequencer.py         ← Phase 2.6
│   ├── blocks/                  ← Phase 2.7, 3.x (some exist)
│   └── ui/
│       ├── hitl1.py             ← Phase 2.2
│       ├── hitl2.py             ← Phase 2.4
│       └── hitl3.py             ← Phase 3.9
├── function_registry/           ← Phase 2.5
├── config/
│   └── unified_schema.json      ← exists
├── docker-compose.yml           ← add Kafka Connect S3 Sink
└── .env                         ← all credentials
```