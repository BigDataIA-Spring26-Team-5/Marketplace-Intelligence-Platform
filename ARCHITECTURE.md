# Architecture Diagrams

## Table of Contents
1. [DAG Workflow](#1-dag-workflow)
2. [Bronze → Silver → Gold Workflow](#2-bronze--silver--gold-workflow)
3. [Data Flow UC1 → UC2 → UC3 → UC4](#3-data-flow-uc1--uc2--uc3--uc4)
4. [Storage Diagram](#4-storage-diagram)
5. [Agentic Workflow](#5-agentic-workflow-langgraph)
6. [Logs Data Flow](#6-logs-data-flow--pipeline-runs--observability-dashboard)

---

## 1. DAG Workflow

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                        1. DAG WORKFLOW (Airflow, UTC)                          ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  02:00  ┌─────────────────────┐
         │ usda_incremental    │──────────────────────────────────────────────┐
         └─────────────────────┘                                              │
  04:00  ┌─────────────────────┐                                              │
         │ off_incremental     │──────────────────────────────────────────┐   │
         └─────────────────────┘                                          │   │
  05:00  ┌─────────────────────┐                                          │   │
         │ openfda_incremental │──────────────────────────────────────┐   │   │
         └─────────────────────┘                                      │   │   │
                                                                      ▼   ▼   ▼
  03:00  ┌─────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
  05:00  │bronze_to_bq │   │bronze_to_bq │   │ bronze_to_bq │   │  Kafka GCS   │
  06:00  │   (usda)    │   │   (off)     │   │  (openfda)   │   │  Sink JSONL  │
         └──────┬──────┘   └──────┬──────┘   └──────┬───────┘   └──────────────┘
                │                 │                  │
  07:00         └─────────────────┴──────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │     bronze_to_silver_dag     │
                    │  watermark → list partitions │
                    │  silver_off  silver_usda     │
                    │  silver_openfda  (parallel)  │
                    │  update watermark            │
                    └──────────────┬──────────────┘
                                   │  ExternalTaskSensor waits
  09:00                            ▼
                    ┌─────────────────────────────┐
                    │      silver_to_gold_dag      │
                    │  gold_off  gold_usda         │
                    │  gold_openfda  (parallel)    │
                    │  ──────────────────────────  │
                    │  gold_gcs_nutrition (fan-in) │
                    │  gold_gcs_safety    (fan-in) │
                    └─────────────────────────────┘

  Hourly ┌─────────────────────┐
         │  uc2_anomaly_dag    │  Isolation Forest on Prometheus metrics
         └─────────────────────┘
  5-min  ┌─────────────────────┐
         │  uc2_chunker_dag    │  audit_events → ChromaDB embeddings
         └─────────────────────┘
```

---

## 2. Bronze → Silver → Gold Workflow

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                      2. BRONZE → SILVER → GOLD WORKFLOW                        ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  SOURCES          BRONZE (GCS)              SILVER (GCS)           GOLD
  ──────────       ─────────────────────     ──────────────────     ──────────────
  USDA API    ──► usda/bulk/YYYY/MM/DD/  ──► nutrition/usda/    ─┐
  OFF  API    ──► off/YYYY/MM/DD/        ──► nutrition/off/     ─┼──► mip-gold-2024/
  openFDA API ──► openfda/YYYY/MM/DD/    ──► safety/openfda/   ─┘    domain/date/
                                                                │     (Parquet)
               ┌── Kafka producer                               │
               │   JSONL part files                             │         │
               │                          watermark gate:       │         ▼
               │                          _watermarks/*.json    │   BigQuery
               │                                                │   mip_gold.products
               │   bronze_to_bq_dag ──────────────────────────►│   (append)
               │   BigQuery staging                             │
               │                          UC1 ETL (silver mode):│
               │                          column_mapping        │
               │                          __generated__ block   │
               │                          _silver_normalize()   │
               │                          → enforce schema      │
               │                          → write Parquet       │
               │                                                │
               │                          Gold blocks:          │
               │                          dedup + enrichment    │
               │                          DQ scoring            │
               └────────────────────────────────────────────────┘
```

---

## 3. Data Flow UC1 → UC2 → UC3 → UC4

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                        3. DATA FLOW  UC1 → UC2 → UC3 → UC4                    ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  ┌───────────────────────────────────────────────────────────────────┐
  │  UC1 — ETL Pipeline                                               │
  │                                                                   │
  │  CLI / Streamlit / REST :8002                                     │
  │          │                                                        │
  │          ▼                                                        │
  │  LangGraph 7-node graph ──► PipelineRunner (10K-row chunks)       │
  │          │                          │                             │
  │          │                          ▼                             │
  │          │                  output/ CSV + Parquet                 │
  │          │                  GCS Silver / Gold                     │
  │          │                  BigQuery mip_gold.products            │
  │          │                                                        │
  └──────────┼────────────────────────────────────────────────────────┘
             │ _emit_event (try/except, non-blocking)
             │ MetricsExporter → Pushgateway
             │ RunLogWriter   → output/run_logs/
             ▼
  ┌───────────────────────────────────────────────────────────────────┐
  │  UC2 — Observability                                              │
  │                                                                   │
  │  Kafka pipeline.events                                            │
  │      │                                                            │
  │      ├──► kafka_to_pg ──► Postgres (audit_events, block_trace,    │
  │      │                             quarantine_rows, dedup_clusters)│
  │      │                       │                                    │
  │      │                       ▼                                    │
  │      │                   ChromaDB (embeddings via chunker)        │
  │      │                       │                                    │
  │      │                       ▼                                    │
  │      │                   ObservabilityChatbot (RAG)               │
  │      │                       │                                    │
  │      ├──► Prometheus ──► Grafana :3000                            │
  │      │         │                                                  │
  │      │         └──► AnomalyDetector (Isolation Forest)            │
  │      │                                                            │
  │      └──► MCP Server :8001 (7 tool endpoints)                     │
  │               │                                                   │
  │               └──► Streamlit app.py (Observability sidebar)       │
  └──────────────────────────────────┬────────────────────────────────┘
                                     │ product catalog
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
  ┌──────────────────────────┐   ┌──────────────────────────────┐
  │  UC3 — Search            │   │  UC4 — Recommendations       │
  │  src/uc3_search/         │   │  src/uc4_recommendations/     │
  │  🚧 NotImplementedError  │   │  🚧 NotImplementedError       │
  └──────────────────────────┘   └──────────────────────────────┘
```

---

## 4. Storage Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                           4. STORAGE DIAGRAM                                   ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  LOCAL / IN-PROCESS                                                          │
  │                                                                              │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────────────┐ │
  │  │ SQLite           │  │ SQLite           │  │ ChromaDB :8000             │ │
  │  │ output/cache.db  │  │ checkpoints.db   │  │ product_corpus collection  │ │
  │  │ Redis fallback   │  │ SHA256 + chunks  │  │ S2 KNN enrichment          │ │
  │  │ WAL-mode         │  │ resume state     │  │ audit_corpus collection    │ │
  │  └──────────────────┘  └──────────────────┘  │ UC2 log embeddings         │ │
  │                                               └────────────────────────────┘ │
  │                                                                              │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────────────┐ │
  │  │ output/run_logs/ │  │ output/*.csv     │  │ src/blocks/generated/      │ │
  │  │ atomic JSON      │  │ full-mode output │  │ DYNAMIC_MAPPING_*.yaml     │ │
  │  │ per run          │  │                  │  │ declarative transforms     │ │
  │  └──────────────────┘  └──────────────────┘  └────────────────────────────┘ │
  └──────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  DOCKER SERVICES                                                             │
  │                                                                              │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────────────┐ │
  │  │ Redis :6379      │  │ Postgres :5432   │  │ ChromaDB                   │ │
  │  │ yaml:  30 days   │  │ db=uc2           │  │ audit_corpus collection    │ │
  │  │ llm:    7 days   │  │ audit_events     │  │ MiniLM-L6-v2 embeddings    │ │
  │  │ emb:   30 days   │  │ block_trace      │  └────────────────────────────┘ │
  │  │ dedup: 14 days   │  │ quarantine_rows  │                                  │
  │  └──────────────────┘  │ dedup_clusters   │  ┌────────────────────────────┐ │
  │                         │ anomaly_reports  │  │ Prometheus + Pushgateway   │ │
  │  ┌──────────────────┐  └──────────────────┘  │ :9091  12 labelled gauges  │ │
  │  │ MLflow           │                         └────────────────────────────┘ │
  │  │ experiment track │  ┌──────────────────┐                                  │
  │  └──────────────────┘  │ Grafana :3000    │                                  │
  │                         │ Prometheus src   │                                  │
  │                         └──────────────────┘                                 │
  └──────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  GCS + BIGQUERY                                                              │
  │                                                                              │
  │  mip-bronze-2024/                mip-silver-2024/        mip-gold-2024/      │
  │  ├─ usda/bulk/YYYY/MM/DD/        ├─ nutrition/off/       ├─ nutrition/date/  │
  │  ├─ off/YYYY/MM/DD/              ├─ nutrition/usda/      └─ safety/date/     │
  │  ├─ openfda/YYYY/MM/DD/          └─ safety/openfda/                          │
  │  └─ _watermarks/*.json                                                       │
  │                                                                              │
  │  BigQuery                                                                    │
  │  ├─ staging tables (bronze load)                                             │
  │  └─ mip_gold.products (append, schema auto-detect)                           │
  └──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Agentic Workflow (LangGraph)

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          5. AGENTIC WORKFLOW (LangGraph)                       ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  [START]
     │
     ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  load_source                                                            │
  │  read CSV / GCS JSONL  ·  detect domain schema                         │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  analyze_schema  ── AGENT 1 ── claude-sonnet-4-5                        │
  │  RENAME · CAST · FORMAT · ADD · SPLIT · UNIFY · DERIVE ops              │
  │  emit column_mapping + operations → YAML                                │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                            ┌─────────┴──────────┐
                            │  Redis cache hit?   │
                            └─────────┬──────────┘
                      YES (skip 2+3)  │  NO
                            ┌─────────┴──────────┐
                            │    with_critic?     │
                            └─────────┬──────────┘
                          YES         │  NO
                           ▼          │
  ┌──────────────────────────┐        │
  │  critique_schema         │        │
  │  AGENT 2 (off by default)│        │
  │  claude-sonnet-4-6       │        │
  │  reasoning model review  │        │
  └──────────┬───────────────┘        │
             └────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  check_registry                                                         │
  │  load domain_packs/<domain>/block_sequence.yaml                         │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  plan_sequence  ── AGENT 3 ── claude-sonnet-4-5                         │
  │  reorder only (cannot add/remove)  ·  re-append dropped blocks          │
  │  write full cacheable blob → Redis (yaml + sequence)                    │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  run_pipeline  ·  PipelineRunner.run_chunked (10K rows/chunk)           │
  │                                                                         │
  │  apply column_mapping  →  expand block sequence:                        │
  │                                                                         │
  │  dq_score_pre                                                           │
  │      │                                                                  │
  │      ▼                                                                  │
  │  __generated__  ←── DynamicMappingBlock (YAML actions)                  │
  │      │              set_null · type_cast · rename · coalesce            │
  │      │              concat_columns · regex_replace · ...                │
  │      ▼                                                                  │
  │  cleaning                                                               │
  │      │                                                                  │
  │      ▼                                                                  │
  │  dedup_stage  →  fuzzy_deduplicate → column_wise_merge                  │
  │                  → golden_record_select                                 │
  │      │                                                                  │
  │      ▼                                                                  │
  │  <domain>__extract_allergens  (custom block)                            │
  │      │                                                                  │
  │      ▼                                                                  │
  │  llm_enrich                                                             │
  │      │  S1 deterministic  regex/keyword                                 │
  │      │      allergens · is_organic · dietary_tags  (S1 ONLY, safety)    │
  │      │  S2 KNN ChromaDB  cosine similarity                              │
  │      │      primary_category  only                                      │
  │      │  S3 RAG-LLM  Groq llama-3.3-70b  top-3 neighbors                │
  │      │      primary_category  only                                      │
  │      │                                                                  │
  │      ▼                                                                  │
  │  dq_score_post                                                          │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  save_output                                                            │
  │  CSV → output/  ·  Parquet → output/silver/<domain>/                   │
  │  RunLogWriter → output/run_logs/  ·  MetricsExporter → Pushgateway     │
  └───────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                    [END]
```

---

## 6. Logs Data Flow — Pipeline Runs → Observability Dashboard

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║              6. LOGS DATA FLOW — Pipeline Runs → Observability Dashboard       ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  PIPELINE RUN
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  PipelineRunner.run_chunked()                                            │
  │      │                                                                   │
  │      ├── run_started         ─┐                                          │
  │      ├── block_start/end      │  _emit_event()  try/except non-blocking  │
  │      │   rows_in/out          ├─────────────────────────────────────────►│
  │      │   null_rates           │                                          │
  │      ├── quarantine           │                                          │
  │      ├── dedup_cluster        │                                          │
  │      └── run_completed       ─┘                                          │
  └──────────────────────────────────────────────────────────────────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
  ┌──────────┐      ┌──────────────┐     ┌────────────────┐
  │  Kafka   │      │MetricsExport │     │ RunLogWriter   │
  │ pipeline │      │er → Push-   │     │ output/        │
  │ .events  │      │gateway:9091  │     │ run_logs/      │
  └────┬─────┘      └──────┬───────┘     │ *.json atomic  │
       │                   │             └───────┬────────┘
       ▼                   ▼                     │
  ┌─────────────┐   ┌─────────────┐             │
  │ kafka_to_pg │   │ Prometheus  │             │
  │ consumer    │   │ scrapes     │             │
  │             │   │ Pushgateway │             │
  │ audit_events│   └──────┬──────┘             │
  │ block_trace │          │                    │
  │ quarantine_ │          ├──────────────────► Grafana :3000
  │   rows      │          │                    (dq_score, row counts,
  │ dedup_      │          │                     anomaly flags)
  │   clusters  │          │
  └──────┬──────┘          │
         │                 ▼
         │          ┌────────────────────┐
         │          │ uc2_anomaly_dag    │
         │          │ Isolation Forest   │
         │          │ ≥5 runs/source     │
         │          │ → anomaly_reports  │
         │          │   (Postgres)       │
         │          │ → etl_anomaly_flag │
         │          │   (Pushgateway)    │
         │          └────────────────────┘
         │
         ▼
  ┌─────────────────────────────────────┐
  │  uc2_chunker_dag (every 5 min)      │
  │  new audit_events rows              │
  │  → MiniLM-L6-v2 embeddings          │
  │  → ChromaDB audit_corpus            │
  └──────────────────┬──────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  ObservabilityChatbot                                                   │
  │                                                                         │
  │  query                                                                  │
  │    │                                                                    │
  │    ├── structured retrieval ──► output/run_logs/*.json                  │
  │    │   (filter by source, status, date range)                           │
  │    │                                                                    │
  │    └── semantic retrieval   ──► ChromaDB audit_corpus                   │
  │        (MiniLM embeddings)                                              │
  │                │                                                        │
  │                ▼                                                        │
  │        Groq llama-3.1-8b-instant                                        │
  │                │                                                        │
  │                ▼                                                        │
  │        ChatResponse(answer, cited_run_ids, context_run_count)           │
  └──────────────────────────────────┬──────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
  ┌───────────────────────────┐      ┌──────────────────────────────────────┐
  │  MCP Server :8001         │      │  app.py Streamlit                    │
  │  7 tool endpoints         │      │  Observability sidebar               │
  │  Redis cache 15-30s       │      │  multi-turn chat UI                  │
  │  Prometheus + Postgres    │      │  cited run_id expanders              │
  └───────────────────────────┘      └──────────────────────────────────────┘
```
