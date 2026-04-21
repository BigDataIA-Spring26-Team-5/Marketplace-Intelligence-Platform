# UC2 — Observability Layer

**Thesis:** watch UC1 in real time. Surface anomalies. Answer natural-language questions about pipeline state with cited evidence. Uses industry-standard metrics (Prometheus) + event log (Postgres) + RAG (ChromaDB) — three backends for three different data shapes.

---

## 1. The Whole Thing At A Glance

```
  ┌─────────────────────── UC1 PIPELINE ───────────────────────┐
  │                                                             │
  │   block runner        audit layer        metrics collector  │
  │       │                   │                     │           │
  └───────┼───────────────────┼─────────────────────┼───────────┘
          │                   │                     │
          │ block start/end   │ quarantine rows,    │ row_count,
          │ events            │ audit entries,      │ null_rate,
          │                   │ dedup stats         │ dq_score,
          │                   │                     │ llm_calls,
          │                   │                     │ cost_usd
          ▼                   ▼                     ▼
    ┌──────────────────────────────┐     ┌─────────────────────┐
    │          KAFKA                │     │ PROMETHEUS          │
    │  ┌────────────────────────┐  │     │    PUSHGATEWAY      │
    │  │ pipeline.events topic  │  │     │ (batch-job pattern) │
    │  └──────────┬─────────────┘  │     └──────────┬──────────┘
    │             │                │                │
    └─────────────┼────────────────┘                │ scrape
                  │                                 │
                  ▼                                 ▼
          ┌──────────────┐                 ┌────────────────┐
          │  POSTGRES    │                 │  PROMETHEUS    │
          │  event log   │                 │  time-series   │
          │  + chunker   │                 │  database      │
          └──────┬───────┘                 └────────┬───────┘
                 │                                  │
                 ▼                                  │
          ┌──────────────┐                          │
          │  CHROMA DB   │                          │
          │ RAG corpus   │                          │
          │ (audit log   │                          │
          │  chunks)     │                          │
          └──────┬───────┘                          │
                 │                                  │
                 │                                  │
                 │       ┌──────────────────────────┤
                 │       │                          │
                 │       ▼                          ▼
                 │  ┌─────────┐              ┌────────────┐
                 │  │ANOMALY  │              │  GRAFANA   │
                 │  │DETECTOR │              │  dashboard │
                 │  │         │              │  (panels)  │
                 │  │Isolation│              │            │
                 │  │ Forest  │              └──────┬─────┘
                 │  └────┬────┘                     │
                 │       │ writes back              │
                 │       │ anomaly_flag             │
                 │       │ metric to Prometheus     │
                 │       └──────────────────────────┤
                 │                                  │
                 ▼                                  ▼
          ┌─────────────────────────────────────────────┐
          │            STREAMLIT UI                     │
          │  ┌────────────────────┐ ┌────────────────┐  │
          │  │ Grafana panels     │ │  RAG chatbot   │  │
          │  │ embedded (iframe)  │ │  (right side)  │  │
          │  │                    │ │                │  │
          │  │ metrics + alerts   │ │  ChromaDB +    │  │
          │  │                    │ │  Claude + MCP  │  │
          │  └────────────────────┘ └────────────────┘  │
          └─────────────────────────────────────────────┘
```

---

## 2. Three Data Backends (Why Each One)

```
  ┌──────────────┬──────────────────────┬─────────────────────┐
  │ BACKEND      │ WHAT LIVES HERE      │ WHY                 │
  ├──────────────┼──────────────────────┼─────────────────────┤
  │ PROMETHEUS   │ numeric time-series  │ built for metrics.  │
  │ (+Pushgate-  │ metrics per run and  │ PromQL, alerting,   │
  │  way)        │ per source           │ Grafana integration │
  │              │                      │                     │
  │              │ row_count            │ Pushgateway lets    │
  │              │ null_rate_by_field   │ short-lived batch   │
  │              │ dq_score_mean        │ jobs push metrics   │
  │              │ dq_delta_mean        │ instead of being    │
  │              │ llm_calls_total      │ scraped             │
  │              │ cost_usd_per_run     │                     │
  │              │ cache_hit_rate       │                     │
  │              │ dedup_rate           │                     │
  │              │ s1/s2/s3/s4_count    │                     │
  │              │ anomaly_flag         │                     │
  ├──────────────┼──────────────────────┼─────────────────────┤
  │ POSTGRES     │ structured event log │ ChromaDB isn't a    │
  │              │ + per-row detail     │ relational store.   │
  │              │                      │ we need SQL for     │
  │              │ block execution trace│ exact row lookups:  │
  │              │   rows in/out per    │ "show me the 47     │
  │              │   block              │ quarantined rows    │
  │              │ quarantine log       │ from run X and why" │
  │              │   (row + reason)     │                     │
  │              │ dedup cluster log    │                     │
  │              │ audit log entries    │                     │
  │              │   (run start, block  │                     │
  │              │   transitions,       │                     │
  │              │   errors)            │                     │
  ├──────────────┼──────────────────────┼─────────────────────┤
  │ CHROMADB     │ vector-embedded      │ the RAG chatbot     │
  │              │ chunks of audit log, │ needs semantic      │
  │              │ anomaly reports, run │ retrieval over      │
  │              │ summaries            │ free-text logs.     │
  │              │                      │ Prometheus/Postgres │
  │              │                      │ can't do that.      │
  └──────────────┴──────────────────────┴─────────────────────┘
```

---

## 3. Metrics Catalog (what Prometheus stores)

```
  METRIC NAME                  TYPE      LABELS
  ─────────────────────────── ───────── ─────────────────────
  uc1_run_started_total       counter   source
  uc1_run_completed_total     counter   source, status
  uc1_rows_in                 gauge     source, block
  uc1_rows_out                gauge     source, block
  uc1_null_rate               gauge     source, field
  uc1_dq_score                histogram source, phase (pre/post)
  uc1_dq_delta                gauge     source
  uc1_dedup_rate              gauge     source
  uc1_duplicate_clusters      gauge     source
  uc1_enrich_calls            counter   source, tier (S1..S4)
  uc1_enrich_cache_hits       counter   source
  uc1_llm_cost_usd            counter   source, model
  uc1_llm_tokens_in           counter   source, model
  uc1_llm_tokens_out          counter   source, model
  uc1_quarantine_rows         gauge     source, reason
  uc1_schema_drift_columns    gauge     source, type (new/missing)
  uc1_block_duration_seconds  histogram source, block
  uc1_agent2_generations      counter   source, status
  uc1_registry_hits            counter   source
  uc1_anomaly_flag            gauge     source, signal
```

Every metric carries a `source` label so Grafana panels can slice by OFF / USDA / openFDA / ESCI.

---

## 4. Ingestion Paths (How Data Gets Into UC2)

### 4.1 Metrics path: UC1 → Pushgateway → Prometheus

```
   UC1 run completes
         │
         ▼
   ┌──────────────────────┐
   │ metrics_collector.py │    (hook in UC1 audit layer)
   │                      │
   │  for each metric:    │
   │    build Prometheus  │
   │    label set         │
   │    push to pushgw    │
   └──────────┬───────────┘
              │ HTTP POST
              ▼
   ┌──────────────────────┐
   │  Prometheus          │
   │  Pushgateway         │   holds last pushed value
   └──────────┬───────────┘   per (job, instance) key
              │
              │ scrape every 15s
              ▼
   ┌──────────────────────┐
   │  Prometheus Server   │   stores time-series
   └──────────────────────┘
```

### 4.2 Event path: UC1 → Kafka → Postgres → ChromaDB

```
   UC1 block runner                      audit layer
         │                                   │
         │ emit per block                    │ emit per row
         │ start/end event                   │ decision
         ▼                                   ▼
   ┌────────────────────────────────────────────┐
   │             pipeline.events topic          │
   └──────────────────────┬─────────────────────┘
                          │
                          ▼
                  ┌───────────────┐
                  │ kafka-to-pg   │   consumer service
                  │ consumer      │
                  └───────┬───────┘
                          │ INSERT
                          ▼
                  ┌───────────────┐
                  │   POSTGRES    │
                  │  event log    │   audit_events table
                  │  + quarantine │   quarantine_rows table
                  │  + dedup log  │   dedup_clusters table
                  │  + block trace│   block_trace table
                  └───────┬───────┘
                          │
                          │ periodic chunker job
                          │ (every 5 min)
                          ▼
                  ┌───────────────┐
                  │  embedder     │   bge-small or
                  │  service      │   all-MiniLM-L6-v2
                  └───────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
                  │   CHROMADB    │   vector collection
                  │  audit_corpus │   metadata = run_id,
                  └───────────────┘    source, timestamp
```

---

## 5. Dashboard (Grafana + Streamlit)

```
  ┌─────────────────────────────────────────────────────┐
  │  GRAFANA DASHBOARD (embedded as iframe in Streamlit)│
  │  ─────────────────────────────────────────────────  │
  │                                                     │
  │  Panel 1: DQ score trend (line chart)               │
  │           uc1_dq_score{phase="post"} over time,     │
  │           one line per source                       │
  │                                                     │
  │  Panel 2: Null rate heatmap                         │
  │           uc1_null_rate by (source, field)          │
  │                                                     │
  │  Panel 3: LLM cost per run (bar)                    │
  │           uc1_llm_cost_usd by source                │
  │                                                     │
  │  Panel 4: Enrichment tier breakdown (stacked bar)   │
  │           uc1_enrich_calls by tier — proves S4 is  │
  │           surgical, most rows handled by S1         │
  │                                                     │
  │  Panel 5: Anomaly flag timeline                     │
  │           uc1_anomaly_flag = 1 → red marker         │
  │                                                     │
  │  Panel 6: Block duration (histogram)                │
  │           uc1_block_duration_seconds_bucket         │
  │                                                     │
  │  Panel 7: Quarantine count per run                  │
  │           uc1_quarantine_rows by reason             │
  │                                                     │
  │  Panel 8: Source health table                       │
  │           last_ingestion, row_count vs baseline,    │
  │           null rates, schema drift count            │
  └─────────────────────────────────────────────────────┘
```

```
  ┌─────────────────────────────────────────────────────┐
  │  STREAMLIT SHELL                                    │
  │  ──────────────                                     │
  │                                                     │
  │  ┌─────────────────────────┐ ┌──────────────────┐   │
  │  │                         │ │                  │   │
  │  │   Grafana iframe        │ │  RAG chatbot     │   │
  │  │   (panels above)        │ │  (right panel)   │   │
  │  │                         │ │                  │   │
  │  │                         │ │  user question   │   │
  │  │                         │ │     ↓            │   │
  │  │                         │ │  Chroma retrieve │   │
  │  │                         │ │     ↓            │   │
  │  │                         │ │  Claude + MCP    │   │
  │  │                         │ │     ↓            │   │
  │  │                         │ │  cited answer    │   │
  │  │                         │ │                  │   │
  │  └─────────────────────────┘ └──────────────────┘   │
  └─────────────────────────────────────────────────────┘
```

---

## 6. Anomaly Detector (Isolation Forest)

```
  scheduled job (every run or every hour):
        │
        ▼
  ┌──────────────────────────┐
  │ query Prometheus PromQL  │   pull last N runs of:
  │                          │     • null_rate means
  │                          │     • row_count
  │                          │     • dq_score_mean
  │                          │     • dedup_rate
  │                          │     • llm_cost per row
  └────────────┬─────────────┘
               ▼
  ┌──────────────────────────┐
  │ build feature vector     │   one row per run
  │ (last N × M features)    │
  └────────────┬─────────────┘
               ▼
  ┌──────────────────────────┐
  │   Isolation Forest       │   sklearn
  │   .score_samples()       │
  └────────────┬─────────────┘
               ▼
       ┌───────┴──────┐
       │              │
    normal        outlier
       │              │
       │              ▼
       │       ┌─────────────────┐
       │       │ push anomaly    │
       │       │ metric to       │
       │       │ Pushgateway     │
       │       │                 │
       │       │ write report    │
       │       │ to Postgres     │
       │       │ → Chroma chunk  │
       │       └────────┬────────┘
       │                │
       └────────────────▼
           Grafana alert rule fires
           on uc1_anomaly_flag == 1
```

**Anomalies we explicitly watch for:**
```
  null rate spike:          2%   → 40% in one run
  row count deviation:      1000 → 847  (where did 153 go?)
  DQ distribution shift:    median dropped 12 pts after refresh
  LLM confidence drop:      mean conf < 0.6
  duplicate rate spike:     8%   → 31% (source sent repeats)
```

---

## 7. RAG Chatbot (ChromaDB + Claude + MCP)

```
   user question in Streamlit
         │
         ▼
   ┌──────────────────────┐
   │ embed question       │   same embedder
   └──────────┬───────────┘   as corpus (bge-small)
              ▼
   ┌──────────────────────┐
   │ ChromaDB vector      │   top-k chunks from
   │ search (k=5)         │   audit log / anomaly
   │                      │   reports / run history
   └──────────┬───────────┘
              ▼
   ┌──────────────────────┐
   │   CLAUDE             │   prompt includes:
   │   + MCP tool schemas │     - question
   │                      │     - retrieved chunks
   │                      │     - tool definitions
   └──────────┬───────────┘
              │ tool calls
              ▼
   ┌──────────────────────┐
   │  MCP SERVER          │
   │  (7 live tools)      │
   │  ──────────────      │
   │  get_run_metrics     │   Prometheus query
   │  get_block_trace     │   Postgres query
   │  get_source_stats    │   Prometheus query
   │  get_anomalies       │   Postgres query
   │  get_cost_report     │   Prometheus query
   │  get_quarantine      │   Postgres query
   │  get_dedup_stats     │   Postgres query
   └──────────┬───────────┘
              ▼
   ┌──────────────────────┐
   │ Claude answers with  │
   │ cited evidence       │
   │ (chunk ids + tool    │
   │  results inline)     │
   └──────────────────────┘
```

**Example questions the chatbot MUST answer:**
```
  "Why did the March 28 run produce fewer rows than March 21?"
  "Which block spiked null brand_owner values?"
  "How many rows were quarantined last run and why?"
  "What did S4 LLM enrich that rules couldn't handle?"
  "Is today's DQ distribution normal vs the last 5 runs?"
  "Show me the runs where duplicate rate was anomalous."
  "Which source contributed most to the enriched catalog?"
```

---

## 8. Execution Flow (Clear Steps)

### 8.1 UC1 run produces observability data

```
  1.  UC1 block runner fires first event
      └─▶ event: {type: "run_started", source, run_id, ts}
          published to Kafka pipeline.events

  2.  For each block, runner emits:
      └─▶ event: {type: "block_start", run_id, block, rows_in}
      └─▶ ...block executes...
      └─▶ event: {type: "block_end", run_id, block, rows_out,
                   null_rates, duration_ms}

  3.  Quarantine rows (HITL 3 failures) emitted as:
      └─▶ event: {type: "quarantine", run_id, row_hash, reason}

  4.  Dedup cluster decisions emitted as:
      └─▶ event: {type: "dedup_cluster", cluster_id, members,
                   canonical, merge_decisions}

  5.  On run completion, metrics_collector builds all metrics
      and pushes to Prometheus Pushgateway in one HTTP call.

  6.  metrics_collector also emits a final
      {type: "run_completed"} event to Kafka.
```

### 8.2 UC2 ingests the data

```
  7.  Prometheus server scrapes Pushgateway every 15s and
      stores time-series in its TSDB.

  8.  kafka-to-pg consumer subscribes to pipeline.events,
      demuxes by event type, INSERTs into the right Postgres
      table:
        audit_events, block_trace, quarantine_rows,
        dedup_clusters

  9.  Chunker job runs every 5 min:
      - SELECT new audit_events since last chunking
      - format each event as a readable text chunk
      - embed with bge-small
      - UPSERT into ChromaDB audit_corpus collection

  10. Anomaly detector runs after each run_completed event:
      - queries Prometheus for last N runs of key metrics
      - scores via Isolation Forest
      - if outlier, pushes uc1_anomaly_flag=1 to Pushgateway
        AND inserts an anomaly_report row in Postgres
        (which flows to Chroma via step 9)
```

### 8.3 UC2 serves queries

```
  11. Grafana dashboard queries Prometheus on page load
      and on auto-refresh (15s).

  12. User types question in Streamlit chatbot panel.

  13. Question is embedded → ChromaDB returns top-5 chunks.

  14. Claude receives: question + retrieved chunks +
      MCP tool definitions.

  15. Claude decides which tools to call (typically 1-2)
      → MCP server executes tool → returns result.

  16. Claude answers with cited chunks and tool results.
```

---

## 9. Hard Dependencies on UC1

```
  UC2 cannot exist unless UC1 commits to emitting:

  ┌─────────────────────────────┬──────────────────────┐
  │ UC1 CAPABILITY              │ UC2 FEATURE IT FEEDS │
  ├─────────────────────────────┼──────────────────────┤
  │ metrics_collector hook      │ Prometheus panels    │
  │ pipeline.events Kafka topic │ Postgres event log   │
  │   with block_start,         │ + Chroma chunks      │
  │   block_end, quarantine,    │                      │
  │   dedup_cluster,            │                      │
  │   run_completed events      │                      │
  │ audit log entries with      │ block-trace panel,   │
  │   rules-file hash,          │ reproducibility      │
  │   rows-in/out per block     │                      │
  │ quarantine rows with        │ quarantine panel     │
  │   reason tags               │                      │
  │ dedup cluster log with      │ dedup-stats panel    │
  │   canonical + members       │                      │
  │ run-level cost + token      │ cost tracker panel   │
  │   counts                    │                      │
  └─────────────────────────────┴──────────────────────┘

  ALL of the above are already on the UC1 commit list.
  → no blocker, they were added for this reason.
```

---

## 10. Commit vs Cut

```
  COMMITTED (must build, must demo)
  ┌───────────────────────────────────────┬──────────┐
  │ Prometheus server (Docker)            │ trivial  │
  │ Prometheus Pushgateway                │ trivial  │
  │ UC1 metrics_collector hook            │ small    │
  │ (produces 20+ metrics via prom client)│          │
  │ Postgres (Docker)                     │ trivial  │
  │ kafka-to-pg consumer service          │ small    │
  │ (demux events into tables)            │          │
  │ Postgres schema: audit_events,        │ small    │
  │ block_trace, quarantine_rows,         │          │
  │ dedup_clusters, anomaly_reports       │          │
  │ ChromaDB (Docker)                     │ trivial  │
  │ chunker job (postgres → chroma)       │ small    │
  │ bge-small embedder                    │ small    │
  │ Isolation Forest anomaly detector     │ medium   │
  │ Grafana (Docker) + dashboard JSON     │ small    │
  │ MCP server with 7 tools               │ medium   │
  │ Streamlit shell with Grafana iframe   │ small    │
  │ + chatbot right panel                 │          │
  │ Claude client + RAG loop              │ small    │
  └───────────────────────────────────────┴──────────┘

  CUT
  ┌───────────────────────────────────────┬──────────┐
  │ Pathway streaming framework           │ Kafka    │
  │                                       │ + Prom   │
  │                                       │ do this  │
  │ Email / Slack alerts                  │ out of   │
  │                                       │ scope    │
  │ Multi-pipeline monitoring             │ UC1 only │
  │ RPCA anomaly detection                │ Isolation│
  │                                       │ Forest   │
  │                                       │ is enough│
  │ Pager / on-call integration           │ not demo │
  │ Long-term metrics retention (>30 d)   │ scope    │
  └───────────────────────────────────────┴──────────┘
```

---

## 11. Demo Narrative (what the audience sees)

```
  Split screen Streamlit:

  LEFT  (Grafana panels)
    - DQ trend over last 4 runs goes up after UC1 enrichment
    - Enrichment tier breakdown: S1 huge, S2 medium,
      S3 small, S4 tiny bar → "LLM is surgical"
    - Null rate heatmap shows OFF had 50% nulls before UC1,
      5% after
    - Anomaly timeline clean except one red marker on OFF
      run 2 (we seeded a deliberate bad file)

  RIGHT (chatbot)
    User: "Why was OFF run 2 flagged as an anomaly?"
    Claude:
      - retrieves 3 chunks from ChromaDB: anomaly report,
        block_trace excerpt, quarantine rows
      - calls get_source_stats(source="OFF", run_id=2)
      - calls get_quarantine(run_id=2)
      - answers:
        "Run 2 was flagged because null_rate on brand_owner
         spiked from 2% to 31%. The quarantine table shows
         47 rows failed the brand_name contract. The block
         trace shows remove_noise_words returned 47 rows
         with empty brand_name because the source dump
         shipped with 'NULL' as a literal string instead
         of actual nulls. Evidence: [chunk-91], [chunk-93]."

  One-beat message:
    "UC1 runs. UC2 watches. When something's wrong,
     you ask the chatbot instead of grep-ing logs."
```

---

## 12. Sign-Off Checklist

```
  [ ] Prometheus + Pushgateway running in Docker
  [ ] UC1 metrics_collector pushes all 20+ metrics per run
  [ ] Prometheus scrapes Pushgateway every 15s (verified)
  [ ] Grafana dashboard renders all 8 panels
  [ ] Postgres schema created + kafka-to-pg consumer alive
  [ ] All pipeline.events event types land in right tables
  [ ] Chunker job embeds new events into ChromaDB every 5 min
  [ ] Isolation Forest runs after each run_completed event
  [ ] Anomaly flag metric visible in Grafana when triggered
  [ ] MCP server exposes 7 tools, each returns correct data
  [ ] RAG chatbot answers all 7 example questions with
      cited evidence
  [ ] Streamlit UI embeds Grafana panels cleanly
  [ ] Demo anomaly is reproducible (seeded bad file)
  [ ] Every box on diagram §1 maps to a real Docker service
      or Python module
```

---

## 13. Validation Story

```
  Q: "Why are you using Prometheus when the pipeline
      is batch?"
  A: Pushgateway pattern. UC1 pushes metrics at end of
     each run. Prometheus scrapes Pushgateway. Standard
     batch-job shape, used in production.

  Q: "Why three backends?"
  A: Different data shapes need different stores.
     Time-series → Prometheus (PromQL, Grafana).
     Structured event log → Postgres (SQL for exact
     lookups, cited evidence in chatbot).
     Free-text RAG corpus → ChromaDB (semantic search).
     No overlap. Each backend solves one problem.

  Q: "Why not just dump everything in Postgres?"
  A: Postgres is bad at high-cardinality time-series
     rollups. Prometheus owns that. Grafana expects
     Prometheus. This is the industry pattern.

  Q: "Why Isolation Forest and not a deep model?"
  A: Unsupervised. No labels. Small feature space
     (~10 metrics × N runs). Fast. Explainable when
     asked why a run was flagged. Deep models need
     training data we don't have.

  Q: "What did you test in UC2?"
  A: Seeded bad-file reproducer: a known-broken OFF
     file that triggers a null spike. Anomaly detector
     flags it. Chatbot answers the why. That's the
     end-to-end test.
```

---

## 14. File References

```
  revised.md                UC1 reference (this doc depends on it)
  Final_Project_Proposal    §5.4 architecture, §5.5 LLM,
                            §5.6 guardrails, §5.7 eval
  BIG-DATA FINAL prep.pdf   UC2 "planned vs built" section
  instructions.md           instructor grading bar
```
