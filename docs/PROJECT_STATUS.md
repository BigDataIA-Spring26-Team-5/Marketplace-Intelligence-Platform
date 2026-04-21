# MIP Platform — Project Status & Implementation Plan

**Last updated:** 2026-04-21  
**Team:** Bhavya Likhitha Bukka · Deepika Vaddadi · Aqeel Ryan  
**Course:** DAMG 7245 — Big Data and Intelligent Analytics, Spring 2026

---

## What Is Built

| Component | Status | Notes |
|-----------|--------|-------|
| Data ingestion — 4 sources | Done | GCS + BigQuery bronze, Kafka, Airflow DAGs |
| Bronze layer (GCS + BigQuery) | Done | USDA 432k, OFF 1M, openFDA 25k, ESCI 2M rows |
| Incremental load DAGs | Done | OFF, USDA, openFDA daily/monthly incremental |
| Bronze → BigQuery DAGs | Done | All 3 sources with GCS watermark pattern |
| Anomaly detection (Isolation Forest) | Done | Seeded, tested, Grafana dashboards live |
| MCP Server | Done | 8 tools, Redis cached, FastAPI on :8001 |
| RAG Chatbot | Done | Claude + mem0 persistent memory + ChromaDB |
| Streamlit dashboard | Done | Pipeline + Observability tabs on :8502 |
| Prometheus + Grafana | Done | Metrics, dashboards, alert rules on :9090/:3000 |
| UC2 anomaly → Postgres | Done | signal, score, details, ts schema |
| ChromaDB RAG chunker | Done | Audit events → embeddings → vector store |
| Auto-restart (systemd + Docker) | Done | All services survive VM reboots |
| UC3 base code | Done | indexer, hybrid_search (BM25+RRF+semantic), evaluator |
| UC4 base code | Done | association_rules, graph_store, recommender |

---

## Gaps — Not Built Yet

| Component | Priority | Notes |
|-----------|----------|-------|
| UC1 unified pipeline complete | Critical | Aqeel's work — in progress |
| Silver/Gold layer in BigQuery | Critical | Only bronze exists; UC3/UC4 need unified output |
| DSPy attribute extraction (Groq) | High | Proposal explicitly promises this — professor will ask |
| Root-cause diagnosis (DSPy) | High | "When anomaly detected, plain-English explanation" — proposed feature |
| Great Expectations validation | High | Proposed as data validation on every load — not implemented |
| UC3 full wiring | High | Code ready, needs silver layer + Instacart data |
| UC4 full wiring | High | Code ready, needs silver layer + Instacart data |
| Instacart transaction data | High | 37.3M rows needed for UC4 — not downloaded |
| MLflow experiment tracking | Medium | Proposed for DSPy prompt optimization tracking |
| LLM-as-Judge nDCG/MRR | Medium | Evaluator built, needs silver layer to run |
| GitHub Actions CI/CD | Medium | Listed in Week 3 deliverables |
| Demo video | Medium | Final deliverable |
| Neo4j → NetworkX | Low | Using NetworkX (proposal listed as acceptable fallback) |
| OpenSearch → rank_bm25 | Low | Proposal itself listed rank_bm25 as fallback — safe |
| Pathway → Prometheus | Low | Different approach, same monitoring outcome |

---

## Architecture Deviations (All Acceptable)

| Proposed | Built | Justification |
|----------|-------|---------------|
| AWS S3 | GCS | Same concept, different cloud — GCP already provisioned |
| Snowflake | BigQuery | Same analytical warehouse pattern |
| OpenSearch | rank_bm25 + ChromaDB | Proposal's own listed fallback |
| Neo4j | NetworkX | Sufficient for demo-scale graph traversal |
| Pathway | Prometheus + Pushgateway | Achieves same real-time monitoring outcome |

---

## Implementation Plan (Excluding UC1)

### Phase 1 — DSPy Attribute Extraction → Silver Layer
**Effort:** 2 days  
**Depends on:** Nothing (can start immediately)

**What:** Extract structured attributes (primary_category, allergens, dietary_tags, is_organic) from raw product descriptions using DSPy + Groq Llama 3.3. Output writes to `silver_raw.products` in BigQuery — this becomes the enriched catalog that UC3 and UC4 index. Also serves as the UC1 proxy until Aqeel's pipeline is complete.

**Silver table schema:**
```
product_id, product_name, brand_name, primary_category,
ingredients, allergens, dietary_tags, is_organic,
dq_score_post, data_source, extraction_confidence
```

**Files to create:**
- `src/intelligence/dspy_extractor.py` — DSPy `ChainOfThought` module, taxonomy-constrained, batched 10-20 products/call, Groq free tier
- `src/intelligence/taxonomy.py` — 50 product categories from USDA hierarchy, used as RAG context to constrain outputs (<1% hallucination)
- `airflow/dags/dspy_extraction_dag.py` — reads `bronze_raw.off` + `bronze_raw.usda_branded` in 5K chunks, writes to `silver_raw.products`

**Demo deliverable:** Side-by-side table — raw OFF product with `primary_category=null, allergens=null` vs DSPy-enriched row with structured fields.

---

### Phase 2 — DSPy Root-Cause Diagnosis
**Effort:** 0.5 day  
**Depends on:** Phase 1

**What:** When UC2 anomaly detector flags a run, DSPy generates a plain-English explanation cross-checked against `block_trace` and `anomaly_reports` data in Postgres.

**Files to create:**
- `src/intelligence/dspy_rootcause.py` — DSPy module: `{signal, score, metrics_dict}` → `{diagnosis, likely_cause, recommended_action}`

**Files to update:**
- `src/uc2_observability/anomaly_detector.py` — after `_insert_anomaly_report()`, call root-cause module and store `diagnosis` column

**Demo deliverable:** Grafana alert fires → chatbot asked "why did OFF null rate spike?" → DSPy diagnosis with evidence cited.

---

### Phase 3 — Great Expectations Validation
**Effort:** 1 day  
**Depends on:** Nothing (can start immediately, parallel to Phase 1)

**What:** Declarative data validation on every bronze ingestion load — validates schema, null rate thresholds, row count minimums, column type contracts. Halts pipeline on critical violations.

**Files to create:**
- `src/validation/ge_suite.py` — builds `ExpectationSuite` per source:
  - OFF: brand 70%+ coverage, product_name 100%, allergens 40%+
  - USDA: all required fields 95%+, dq_score_post 90%+
  - openFDA: recall date present, product description 95%+
- `airflow/dags/bronze_validation_dag.py` — runs GE suite after each bronze load, stores validation results to GCS + logs to Postgres `audit_events`

**Demo deliverable:** GE validation report — green checkmarks for USDA, orange warnings for OFF (expected data quality contrast).

---

### Phase 4 — Instacart Data + UC4 Full Wiring
**Effort:** 1.5 days  
**Depends on:** Phase 1 (for canonical product IDs)

**What:** Download Instacart 37.3M transaction dataset, load to BigQuery, wire UC4 recommender for real before/after lift comparison.

**Steps:**
1. Download from Kaggle: `orders.csv`, `order_products__prior.csv`, `products.csv`
2. Load to `bronze_raw.instacart_orders` and `bronze_raw.instacart_products`
3. Wire `AssociationRuleMiner` with real transaction data
4. Before baseline: raw fragmented product names as IDs
5. After enriched: canonical IDs from DSPy silver layer
6. Show 3-4x lift improvement in Streamlit

**Files to create:**
- `scripts/load_instacart_bq.py` — download + BigQuery load job

**Files to update:**
- `src/uc4_recommendations/recommender.py` — add `load_from_bigquery()` method

---

### Phase 5 — UC3 Full Wiring + LLM-as-Judge Eval
**Effort:** 1 day  
**Depends on:** Phase 1

**What:** Index DSPy silver catalog into ChromaDB + BM25, run LLM-as-Judge evaluation on 100 ESCI benchmark queries, add search UI to Streamlit.

**Steps:**
1. Call `ProductIndexer.build()` from `silver_raw.products`
2. Run `SearchEvaluator.run()` — before (bronze raw) vs after (DSPy silver)
3. Log nDCG@10 + MRR to MLflow
4. Add UC3 search tab to Streamlit

**Files to update:**
- `src/uc3_search/indexer.py` — add `load_from_bigquery(table)` method
- `src/uc2_observability/streamlit_app.py` — add Search tab

**KPI target:** nDCG@10 > 0.60 (from proposal)

---

### Phase 6 — MLflow Experiment Tracking
**Effort:** 0.5 day  
**Depends on:** Phase 1, Phase 5

**What:** Track DSPy extraction experiments (prompt versions, extraction accuracy per taxonomy category) and UC3 eval runs (nDCG/MRR per catalog version — before vs after enrichment).

**Files to create:**
- `src/intelligence/mlflow_tracker.py` — `log_extraction_run()`, `log_eval_run()`

**Infrastructure:**
- Add `mlflow` service to `docker-compose.uc2.yml` (port 5000, SQLite backend)
- Accessible at `http://35.239.47.242:5000`

---

### Phase 7 — Streamlit UC3 + UC4 Pages
**Effort:** 1 day  
**Depends on:** Phases 4, 5

**UC3 Search tab:**
- Search box → ranked results (product name, category, allergens, DQ score, match mode BM25/semantic/hybrid)
- Mode toggle: BM25 only / Semantic only / Hybrid
- nDCG/MRR scorecard panel: before enrichment vs after enrichment

**UC4 Recommendations tab:**
- Product name search → "Customers Also Bought" + "You Might Also Like" panels
- Before/after lift comparison bar chart
- Graph visualization (NetworkX + pyvis)

---

### Phase 8 — GitHub Actions CI/CD
**Effort:** 0.5 day  
**Depends on:** Nothing

**Files to create:**
- `.github/workflows/ci.yml` — on push to `main`:
  - `ruff` lint check
  - `pytest tests/` (non-integration tests)
  - `docker build` for MCP server image
  - Status badge in README

---

### Phase 9 — Demo Video + Final Documentation
**Effort:** 1 day  
**Depends on:** Everything

**Demo script (5 minutes):**
1. Show raw data fragmentation — same product, 4 different names across sources
2. DSPy extraction run — null fields become structured attributes
3. Great Expectations report — USDA clean, OFF warnings (expected)
4. UC3 search — "organic gluten-free cereal" → before (noisy) vs after (structured) results, nDCG improvement
5. UC4 recommendations — product lookup → also-bought + cross-category, lift before vs after
6. UC2 observability — anomaly detected, DSPy root-cause diagnosis, Grafana dashboard
7. RAG chatbot — "why did OFF null rate spike in run 06?" → cited evidence

**Documentation updates:**
- README architecture diagram
- ENDPOINTS.md updated with MLflow URL
- Per-component docstrings complete

---

## Execution Order

```
Phase 1 (DSPy extraction) ──────────────────────────┐
Phase 3 (Great Expectations) ─── parallel ──────────┤
Phase 8 (CI/CD) ─────────────── parallel ───────────┘
        │
        ▼ (Phase 1 done)
Phase 2 (Root-cause diagnosis)
Phase 4 (Instacart + UC4 wiring)
Phase 5 (UC3 wiring + eval)
        │
        ▼
Phase 6 (MLflow)
Phase 7 (Streamlit UC3 + UC4 pages)
        │
        ▼
Phase 9 (Demo video + docs)
```

## Total Estimated Effort: ~8.5 days

| Phase | Task | Days |
|-------|------|------|
| 1 | DSPy extraction → silver layer | 2.0 |
| 2 | DSPy root-cause diagnosis | 0.5 |
| 3 | Great Expectations | 1.0 |
| 4 | Instacart + UC4 | 1.5 |
| 5 | UC3 wiring + eval | 1.0 |
| 6 | MLflow | 0.5 |
| 7 | Streamlit UC3 + UC4 pages | 1.0 |
| 8 | CI/CD | 0.5 |
| 9 | Demo + docs | 1.0 |
| **Total** | | **9.0 days** |
