# EDA & Documentation — Execution Plan

Scope: build the EDA / statistics layer, DQ-scoring docs, Streamlit EDA page, architecture diagrams, README rewrite, and Codelabs write-up for the Marketplace Intelligence Platform final submission.

Deployment facts to anchor docs against:
- GCP VM `mip-vm`, static IP **35.239.47.242** (reserved as `mip-static-ip`)
- Public endpoints documented in `DEPLOYMENT.md`
- All compose services `restart: unless-stopped`; docker daemon systemd-enabled

---

## DQ scoring (what the docs must explain)

Formula (from `src/blocks/dq_score.py`):

```
dq_score = ( completeness * 0.40
           + freshness    * 0.35
           + ingredient_richness * 0.25 ) * 100
```

- **completeness** — fraction non-null over fixed reference column set (same pre → post for fair delta). Skips `dq_score_pre`, `dq_score_post`, `dq_delta`, `duplicate_group_id`, `canonical`, `enriched_by_llm`, `sizes`.
- **freshness** — `clip(1 − age_days / 730, 0, 1)` on `published_date`; fallback 0.5 when column missing.
- **ingredient_richness** — `len(ingredients) / max(len)` per batch.
- `dq_delta = dq_score_post − dq_score_pre` (enrichment lift).

---

## Phase 1 — Data foundation

### 1a. Inventory actual datasets
- Check GCS Bronze (`gs://mip-bronze-2024/`): which sources × dates exist
- Check GCS Silver (`gs://mip-silver-2024/`): which domains populated
- Check BigQuery `mip_gold.products`: row counts per source
- Check `output/run_logs/` for per-run telemetry
- Deliverable: `docs/data_inventory.md` — what exists vs what the code references

### 1b. `docs/DQ_SCORING.md`
- Formula (LaTeX)
- Worked example: 3 real rows, component math
- Weight justification (or flag as tunable)
- Pre vs post methodology (why fixed reference columns)
- Safety-column exclusion rationale (why `allergens` / `is_organic` / `dietary_tags` are S1-only)

---

## Phase 2 — EDA engine

### 2a. `src/eda/report.py` — reusable library
- `load_bronze(source, date) -> pd.DataFrame`
- `load_silver(source, date) -> pd.DataFrame`
- `load_gold(source) -> pd.DataFrame` (BQ or local parquet)
- `load_run_logs() -> pd.DataFrame` (from `output/run_logs/`)
- `compute_stats(bronze, silver, gold) -> EDAStats` dataclass:
  - shape, null%, schema diff, DQ dist, dedup stats, enrichment tier %, category dist

### 2b. `scripts/eda_full_report.py` — CLI driver
- Iterate all (source, date) pairs
- Dump `output/eda/<source>_<date>/{tables.csv, plots/*.png, summary.json}`
- Write rollup `output/eda/SUMMARY.md`

### 2c. `tests/unit/test_eda_report.py`
- Synthetic bronze / silver / gold frames exercise every stat function

---

## Phase 3 — Streamlit EDA page (demo surface)

### 3a. Sidebar option in `app.py`: `Mode = [Pipeline, Observability, EDA]`

### 3b. `src/eda/streamlit_page.py` — `_render_eda_page()`
- Source × date selector
- Tabs:
  - **Shape** — bronze/silver/gold row+col counts, quarantine/dedup deltas
  - **Schema diff** — kept / dropped / renamed / added columns
  - **Nulls** — side-by-side heatmap (plotly) bronze vs silver vs gold
  - **DQ scores** — histograms pre/post, delta boxplot, component breakdown
  - **Enrichment tiers** — S1 / S2 / S3 / unresolved bar
  - **Dedup** — cluster stats, top collisions
  - **Categories** — top-20 bar of `primary_category`
  - **Telemetry** — wallclock per block, token spend, cache hit %
  - **UC3 / UC4** — index size, rule count, top rules
- Data via `src/eda/report.py`; cache with `@st.cache_data(ttl=300)`

### 3c. Offline fallback
- If GCS unavailable, load `tests/fixtures/eda_sample/` so demo works offline

---

## Phase 4 — Docs & diagrams

### 4a. Architecture diagram
- Mermaid in README (renders on GitHub) + one PNG via draw.io for Codelabs
- Layers: Sources → Kafka → Bronze (GCS) → Airflow → 3-agent LangGraph → Silver (GCS) → Dedup+Enrich → Gold (BQ) → UC2/UC3/UC4 surfaces

### 4b. Data-flow diagram — single record bronze → gold

### 4c. Agent interaction diagram — LangGraph state machine

### 4d. Guardrails layer diagram — structural (`src/agents/guardrails.py`) + LLM safety (`src/agents/safety_guardrails.py`) choke points

---

## Phase 5 — README rewrite

Sections:
1. Problem statement (1 paragraph)
2. Architecture diagram
3. Data-flow diagram
4. Use cases (UC1 pipeline, UC2 observability, UC3 search, UC4 recs)
5. Quick start (poetry install, `.env`, `docker-compose up`, `demo.py`)
6. Deployed endpoints (link to `DEPLOYMENT.md`)
7. Data sources & domain schemas (table)
8. DQ scoring (link to `docs/DQ_SCORING.md`)
9. Three-agent LangGraph explainer
10. Guardrails layer
11. EDA highlights (link to `output/eda/SUMMARY.md` + 2–3 money plots)
12. Testing
13. Repo layout

Keep existing technical depth; add diagrams + EDA callouts + DEPLOYMENT.md link.

---

## Phase 6 — Codelabs document (`docs/codelabs.md`)

Steps:
1. Problem & Dataset tour (raw JSONL samples per source)
2. Architecture overview (diagram)
3. Bronze ingest (Kafka producer, GCS sink) + show a bronze row
4. Silver transform (three agents, YAML cache, guardrails) — before/after schema
5. DQ scoring (formula + real numbers from a run)
6. Gold dedup + enrichment cascade (S1→S2→S3) with real tier %
7. UC2 observability (Prometheus / Grafana / RAG chatbot screenshots)
8. UC3 hybrid search demo (query → results; 99,666 products indexed)
9. UC4 association rules demo (49,688 products, 105 rules)
10. Deployment on GCP (link to `DEPLOYMENT.md`)
11. Challenges (YAML cache coherence, safety invariant, Airflow pidfile, stale static IP, guardrails fail-closed vs fail-open, Kafka Connect stale IP, S2 corpus bootstrap)
12. Metrics & evaluation
    - Functional: DQ delta per source, dedup ratio, enrichment tier split, safety-invariant violations (target = 0)
    - Ops: pipeline wallclock, token cost/run, cache hit %, anomaly flags raised
    - Quality: UC4 rule lift distribution, UC3 top-k retrieval precision sample
13. What's next

---

## Execution order

| # | Task | ETA | Deliverable |
|---|---|---|---|
| 1 | Phase 1a — data inventory | ~15 min | `docs/data_inventory.md` |
| 2 | Phase 1b — DQ scoring doc | ~20 min | `docs/DQ_SCORING.md` |
| 3 | Phase 2a — EDA library | ~45 min | `src/eda/report.py` + tests |
| 4 | Phase 2b — CLI driver | ~20 min | `scripts/eda_full_report.py` |
| 5 | Run CLI on real data | ~15 min | `output/eda/SUMMARY.md` + artifacts |
| 6 | Phase 3 — Streamlit EDA page | ~45 min | `Mode=EDA` tab in `app.py` |
| 7 | Phase 4 — diagrams (mermaid) | ~30 min | embedded in README |
| 8 | Phase 5 — README rewrite | ~40 min | `README.md` |
| 9 | Phase 6 — Codelabs draft | ~60 min | `docs/codelabs.md` |

Total ≈ 5 hours. Steps 7 can run anytime; step 9 needs numbers from step 5.

Guardrails already implemented:
- Structural / hallucination checks — `src/agents/guardrails.py` (Agents 1/2/3 + S3 enrichment)
- LLM-based input-injection + PII-redaction — `src/agents/safety_guardrails.py`, wired into `src/uc2_observability/rag_chatbot.py`. Default model `groq/llama-3.1-8b-instant` (project uses Claude / Groq / DeepSeek; not OpenAI).
- Tests: `tests/unit/test_safety_guardrails.py` (10 pass).
