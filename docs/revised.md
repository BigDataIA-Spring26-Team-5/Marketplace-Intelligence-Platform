# UC1 — Dynamic, Schema-Driven Pipeline

**Thesis:** one pipeline, any source, any industry, ONE unified catalog. Agents generate missing transforms and save them. Cross-source dedup is what makes it a catalog.

---

## 1. The Whole Thing At A Glance

```
     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
     │  USDA   │ │   OFF   │ │ openFDA │ │  ESCI   │
     │ (batch) │ │(stream) │ │(stream) │ │ (batch) │
     └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
          │           │           │           │
          ▼           ▼           ▼           ▼
     ┌─────────┐ ┌─────────────────────┐ ┌─────────┐
     │ Airflow │ │       KAFKA         │ │ Airflow │
     │  (pull) │ │  source.off.deltas  │ │ (once)  │
     │         │ │ source.openfda.*    │ │         │
     └────┬────┘ └──────────┬──────────┘ └────┬────┘
          │                 │                 │
          │         Kafka Connect S3 Sink     │
          └─────────────────┼─────────────────┘
                            ▼
                   ┌────────────────┐
                   │       S3       │  ← data lake
                   │  lake/bronze/  │    (contract)
                   └────────┬───────┘
                            │ S3 event → EventBridge
                            ▼
  ┌──────────────────── UC1 PIPELINE ────────────────────┐
  │                                                      │
  │  STAGE A (per source, dynamic)                       │
  │  ┌────┐  ┌──────┐  ┌────┐  ┌──────┐  ┌────┐         │
  │  │ A1 │→ │HITL 1│→ │ A2 │→ │HITL 2│→ │ A3 │→ prep   │
  │  │gap │  │ map  │  │code│  │review│  │rule│ blocks  │
  │  └────┘  └──────┘  └────┘  └──────┘  └────┘         │
  │                         │                            │
  │                         ▼                            │
  │                  Registry (persistent)               │
  │                                                      │
  │  ═══ all 4 sources converge in s3://silver/pending  │
  │                                                      │
  │  STAGE B (once on the union)                         │
  │  dq_pre → id_reconcile → fuzzy_dedup → collapse      │
  │        → recall_annotate → llm_enrich → dq_post      │
  │        → HITL 3 → s3://silver/unified_catalog        │
  │                                                      │
  └───────────────────────┬──────────────────────────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
       ┌───────┐      ┌───────┐      ┌───────┐
       │  UC2  │      │  UC3  │      │  UC4  │
       │ obs.  │      │search │      │ recs  │
       └───────┘      └───────┘      └───────┘

       UC2 also listens to Kafka `pipeline.events`
```

---

## 2. Data Sources (4)

```
┌──────────────┬──────────┬────────────────────────────────┐
│   SOURCE     │ TRANSPORT│   JOB IN UC1                   │
├──────────────┼──────────┼────────────────────────────────┤
│ USDA         │ Airflow  │ clean reference, seed S1+S2,   │
│              │ monthly  │ barcode-linked to OFF          │
├──────────────┼──────────┼────────────────────────────────┤
│ Open Food    │ Kafka    │ dirty primary catalog,         │
│ Facts        │ deltas + │ real dedup workload,           │
│              │ Airflow  │ main merge partner with USDA   │
│              │ daily    │                                │
├──────────────┼──────────┼────────────────────────────────┤
│ openFDA      │ Kafka    │ schema-gap stress test for A2, │
│ recalls      │ stream   │ becomes recall ANNOTATIONS     │
│              │          │ on matched catalog rows        │
├──────────────┼──────────┼────────────────────────────────┤
│ Amazon ESCI  │ Airflow  │ NON-FOOD catalog,              │
│              │ one-time │ proves "any industry" —        │
│              │          │ A3 skips food-specific blocks  │
└──────────────┴──────────┴────────────────────────────────┘

     CUT: Open Prices, Instacart, Pathway
```

---

## 3. Unified Schema (ONE table for all sources)

```
   ┌─────────────────┬──────────────┬─────────────────────┐
   │ COLUMN          │ TYPE         │ FROM                │
   ├─────────────────┼──────────────┼─────────────────────┤
   │ product_id      │ string       │ OFF/USDA/ESCI       │
   │ product_name    │ string       │ all                 │
   │ brand_name      │ string       │ all                 │
   │ ingredients     │ string       │ OFF, USDA           │
   │ primary_category│ string       │ enriched            │
   │ allergens       │ list[str]    │ OFF, USDA (S1 only) │
   │ dietary_tags    │ list[str]    │ OFF (S1 only)       │
   │ is_organic      │ bool         │ OFF, USDA (S1 only) │
   │ size_value      │ list[str]    │ OFF, USDA           │
   │ sources         │ list[str]    │ provenance ← KEY    │
   │ has_recall      │ bool         │ openFDA annotation  │
   │ recall_class    │ string       │ openFDA annotation  │
   │ recall_reason   │ string       │ openFDA annotation  │
   │ dq_score_pre    │ float        │ computed            │
   │ dq_score_post   │ float        │ computed            │
   │ dq_delta        │ float        │ post − pre          │
   └─────────────────┴──────────────┴─────────────────────┘
```

---

## 4. Ingestion Layer

```
                      CONTINUOUS                    BATCH
                          │                           │
   ┌──────────────────────┼───────────┐  ┌────────────┼────────────┐
   │                      │           │  │            │            │
   ▼                      ▼           │  ▼            ▼            │
┌──────┐             ┌─────────┐      │ ┌──────┐ ┌──────────┐      │
│ OFF  │────topic───▶│ KAFKA   │      │ │ USDA │ │   ESCI   │      │
│delta │             │         │      │ │      │ │          │      │
└──────┘             │ source. │      │ └──┬───┘ └─────┬────┘      │
┌──────┐             │ off.    │      │    │           │           │
│oFDA  │────topic───▶│ deltas  │      │    └─────┬─────┘           │
│recall│             │         │      │          │                 │
└──────┘             │ source. │      │          ▼                 │
                     │ openfda │      │    ┌──────────┐            │
                     └────┬────┘      │    │ AIRFLOW  │            │
                          │           │    │   DAG    │            │
                  Kafka   │           │    └────┬─────┘            │
                  Connect │           │         │                  │
                  S3 Sink │           │         │                  │
                          ▼           │         ▼                  │
                    ┌──────────────────────────────────────┐       │
                    │              S3 LAKE                 │       │
                    │  bronze/{usda,off,openfda,esci}/     │       │
                    └──────────────────┬───────────────────┘       │
                                       │                           │
                                       │ S3 event → EventBridge    │
                                       ▼                           │
                              ┌────────────────┐                   │
                              │  UC1 TRIGGER   │                   │
                              │  (Airflow DAG) │                   │
                              └────────────────┘                   │
                                                                    │
   ┌────────────────────────────────────────────────────────────────┘
   │
   └─▶ S3 LAKE ZONES
       bronze/  raw source data (one prefix per source)
       silver/  Stage-A buffers + Stage-B unified_catalog
       gold/    UC3 search index + UC4 recommendations
```

### Kafka topics

```
  ┌──────────────────────┬─────────────┬──────────────────┐
  │ TOPIC                │ PRODUCER    │ CONSUMER         │
  ├──────────────────────┼─────────────┼──────────────────┤
  │ source.off.deltas    │ OFF poller  │ S3 Sink          │
  │ source.openfda.*     │ FDA poller  │ S3 Sink + UC1    │
  │                      │             │ recall_annotate  │
  │ pipeline.events      │ UC1 runner  │ UC2 dashboard    │
  │ pipeline.metrics     │ UC1 audit   │ UC2 dashboard    │
  └──────────────────────┴─────────────┴──────────────────┘
```

---

## 5. Stage A — Dynamic Per-Source

Runs independently for every source. Agents figure out what to do.

```
  ┌──────────────────────────────────────────────────────┐
  │                   S3 bronze/<src>                    │
  └────────────────────────┬─────────────────────────────┘
                           │
                           ▼
    ┌────────────────────────────────────────────────────┐
    │  AGENT 1 — Gap Analysis                            │
    │  ───────────────────────                           │
    │  reads: schema + samples + nulls + registry        │
    │  LLM:   semantic column mapping                    │
    │  emits: MAP | DROP | NEW | ADD  per column         │
    └─────────────────────┬──────────────────────────────┘
                          ▼
    ┌────────────────────────────────────────────────────┐
    │  HITL 1  — user approves mapping                   │
    └─────────────────────┬──────────────────────────────┘
                          ▼
                  ┌───────┴────────┐
                  │                │
            [hit registry]    [miss]
                  │                │
                  ▼                ▼
          ┌─────────────┐  ┌─────────────────────────┐
          │ reuse saved │  │ AGENT 2 — Code Gen      │
          │   function  │  │ ──────────────────      │
          │   (free)    │  │  LLM → Python fn code   │
          └──────┬──────┘  │  ┌─────────────────────┐│
                 │         │  │ DOCKER SANDBOX      ││
                 │         │  │ --network none      ││
                 │         │  │ whitelisted libs    ││
                 │         │  │ 5s timeout          ││
                 │         │  └─────────────────────┘│
                 │         │  validation chain:      │
                 │         │  static scan → sandbox  │
                 │         │  → types → nulls →      │
                 │         │  PYTEST sample I/O      │
                 │         │  → self-correct (×2)    │
                 │         └──────────┬──────────────┘
                 │                    ▼
                 │         ┌──────────────────────┐
                 │         │ HITL 2  — code review│
                 │         └──────────┬───────────┘
                 │                    ▼
                 │         ┌──────────────────────┐
                 │         │ Function Registry    │
                 │         │  registry.json       │
                 │         │  functions/*.py      │
                 │         └──────────┬───────────┘
                 └────────────┬───────┘
                              ▼
    ┌────────────────────────────────────────────────────┐
    │  AGENT 3 — Profile-Driven Sequencer                │
    │  ──────────────────────────                        │
    │  one pandas pass → profile signals                 │
    │  rule table → block_sequence + skip_reasons        │
    │  NO LLM at runtime                                 │
    └─────────────────────┬──────────────────────────────┘
                          ▼
    ┌────────────────────────────────────────────────────┐
    │  Dynamic preprocessing (only the blocks A3 kept)   │
    │  normalize_text · remove_noise_words ·             │
    │  extract_quantity_column · extract_allergens       │
    └─────────────────────┬──────────────────────────────┘
                          ▼
                 s3://silver/pending/<src>/
```

### Agent 3 skip rules

```
   ┌──────────────────────────────────────┬──────────────┐
   │ SIGNAL                               │ SKIP         │
   ├──────────────────────────────────────┼──────────────┤
   │ text already clean                   │ normalize_   │
   │                                      │   text       │
   │ brand col has no legal suffixes      │ remove_noise │
   │ null_rate(product_name) > 50%        │ HALT + ALERT │
   │ domain != food                       │ allergens +  │
   │                                      │ quantity     │
   │ all enrich cols filled               │ llm_enrich   │
   └──────────────────────────────────────┴──────────────┘

   Demo beat:
     OFF  → runs everything (dirty)
     USDA → skips normalize_text, remove_noise_words (clean)
     ESCI → skips allergens, extract_quantity (non-food)
     Same code. Different sequences. Zero edits.
```

---

## 6. Stage B — Runs ONCE on the Union

The four per-source buffers concat into one dataframe. Stage B runs once. This is what turns 4 tables into 1 catalog.

```
  s3://silver/pending/usda  ┐
  s3://silver/pending/off   ├──▶ pd.concat ──▶ union_df
  s3://silver/pending/fda   │
  s3://silver/pending/esci  ┘

                                    │
                                    ▼
                           ┌──────────────────┐
                           │  dq_score_pre    │
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │  id_reconcile    │  barcode match
                           │                  │  (OFF.code ↔
                           │                  │   USDA.gtin_upc)
                           │                  │  fuzzy fallback
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │ fuzzy_deduplicate│  CROSS-SOURCE
                           │                  │  double blocking
                           │                  │  RapidFuzz 0.5/
                           │                  │  0.2/0.3 thr 85
                           │                  │  union-find
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │ collapse_cluster │  multi-source
                           │                  │  merge:
                           │                  │  - per-field pick
                           │                  │    from highest-DQ
                           │                  │  - sources=[...]
                           │                  │  - DQ golden row
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │ recall_annotate  │  openFDA rows
                           │                  │  fuzzy-match onto
                           │                  │  catalog → flip
                           │                  │  has_recall
                           │                  │  (unmatched dropped)
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │   llm_enrich     │  4-tier cascade
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │  dq_score_post   │
                           │  + dq_delta      │
                           └────────┬─────────┘
                                    ▼
                           ┌──────────────────┐
                           │  HITL 3          │  schema contract
                           │                  │  pass → output
                           │                  │  fail → quarantine
                           └────────┬─────────┘
                                    ▼
                  s3://silver/unified_catalog/latest.parquet
                         │
                         └─▶ UC2 / UC3 / UC4
```

---

## 7. Enrichment — 4-Tier Cascade

```
          rows needing enrichment
          (post-dedup canonical only)
                      │
                      ▼
       ┌─────────────────────────────┐
       │ S1  Deterministic rules     │  FREE   ~60%
       │     regex + FDA Big-9 +     │
       │     USDA category map       │
       └──────────────┬──────────────┘
                      │ unresolved
                      ▼
       ┌─────────────────────────────┐
       │ S2  FAISS Semantic KNN      │  CHEAP  ~25%
       │     sentence-transformers   │
       │     all-MiniLM-L6-v2        │
       │     k=5  thr 0.6            │
       │     seeded from USDA        │
       └──────────────┬──────────────┘
                      │ low confidence
                      ▼
       ┌─────────────────────────────┐
       │ S3  Cluster Propagation     │  FREE   ~10%
       │     2/3 members labeled →   │
       │     3rd inherits            │
       └──────────────┬──────────────┘
                      │ ambiguous
                      ▼
       ┌─────────────────────────────┐
       │ S4  RAG-Augmented LLM       │  PAID   ~5%
       │     Groq / Claude / DeepSeek│
       │     prompt: top-k neighbors │
       │     high-conf → back to S2  │
       └─────────────────────────────┘

   SAFETY: allergens, is_organic, dietary_tags
           NEVER cascade past S1.
           Only primary_category reaches S4.
```

---

## 8. HITL Gates

```
   ┌────────┬──────────────────┬────────────────────────┐
   │ GATE   │ WHERE            │ SHOWS                  │
   ├────────┼──────────────────┼────────────────────────┤
   │ HITL 1 │ after A1         │ MAP/DROP/NEW/ADD per   │
   │        │ (per source)     │ column                 │
   ├────────┼──────────────────┼────────────────────────┤
   │ HITL 2 │ after A2         │ code + sandbox out +   │
   │        │ (per generated   │ pytest + sample I/O    │
   │        │  function)       │                        │
   ├────────┼──────────────────┼────────────────────────┤
   │ HITL 3 │ after dq_post    │ rows failing schema    │
   │        │ (Stage B)        │ contract + reasons     │
   └────────┴──────────────────┴────────────────────────┘
```

---

## 9. Execution Flow (Runs In Order)

```
  RUN 1 — USDA ──────────────────────────────┐
    A1 map → HITL1 → A2 (no gaps) → A3 clean │
    → skip normalize_text + noise_words      │
    → silver/pending/usda                    │
    └─▶ establishes unified_schema.json      │
                                              │
  RUN 2 — OFF  ──────────────────────────────┤
    A1 map → HITL1 → A2 (small gaps) →       │
    HITL2 → registry → A3 dirty → run ALL    │
    → silver/pending/off                      │
                                              │
  RUN 3 — openFDA ───────────────────────────┤
    A1 detects 3 gaps → HITL1 → A2 gens 3   │
    fns in Docker sandbox → pytest passes →  │
    HITL2 → registry → A3 → silver/pending/  │
                                              │
  RUN 4 — ESCI ──────────────────────────────┤
    A1 map → HITL1 → A2 or registry hits →   │
    A3 non-food → skip allergens + quantity  │
    → silver/pending/esci                     │
                                              ▼
  STAGE B (once, on the union)  ────────────────
    concat → dq_pre → id_reconcile → fuzzy_
    dedup → collapse_cluster → recall_
    annotate → llm_enrich → dq_post → HITL3
    → silver/unified_catalog/latest.parquet

  RUN 5 — ESCI again  ───────────────────────
    Registry hits everywhere. A2 never called.
    "The pipeline remembered."
```

---

## 10. Commit vs Cut

```
  COMMITTED (must build, must demo)
  ┌─────────────────────────────────────┬──────────┐
  │ S3 lake (bronze/silver/gold)        │ trivial  │
  │ Kafka broker (single-node Docker)   │ small    │
  │ Kafka Connect S3 Sink               │ small    │
  │ 4 Kafka topics + 2 pollers          │ small    │
  │ Airflow DAGs (USDA, OFF, ESCI,      │ small    │
  │   UC1-trigger)                      │          │
  │ Agent 1 + HITL 1                    │ exists   │
  │ Agent 2 + Docker sandbox            │ medium   │
  │ Agent 2 pytest on sample I/O        │ small    │
  │ Agent 3 profile rule engine         │ sm-med   │
  │ Function registry + tests_passed    │ small    │
  │ normalize_text merged block         │ trivial  │
  │ remove_noise_words two-layer        │ small    │
  │ extract_quantity preserve sizes     │ small    │
  │ fuzzy_deduplicate double-blocking   │ small    │
  │ id_reconcile (barcode+fuzzy)        │ medium   │
  │ collapse_cluster multi-source merge │ medium   │
  │ recall_annotate block               │ small    │
  │ S1 rules enrichment                 │ exists   │
  │ S2 FAISS KNN                        │ medium   │
  │ S3 cluster propagation              │ small    │
  │ S4 LLM (fix Groq bug)               │ trivial  │
  │ HITL 3 schema validator             │ small    │
  │ Audit log for UC2                   │ small    │
  └─────────────────────────────────────┴──────────┘

  CUT
  ┌─────────────────────────────────────┬──────────┐
  │ Agent 3 LLM layer                   │ theater  │
  │ Pathway streaming                   │ redundant│
  │                                     │ w/ Kafka │
  │ Open Prices source                  │ scope    │
  │ Instacart source                    │ UC4 only │
  │ Neo4j knowledge graph               │ UC4 only │
  │ Cross-source anything beyond Stage B│ done here│
  └─────────────────────────────────────┴──────────┘
```

---

## 11. Demo Narrative (5 runs + 1 victory lap)

```
  1. USDA      clean run, establishes schema, no fireworks
  2. OFF       dirty → full preprocessing, DQ starts ~50%
  3. openFDA   ★ A2 generates 3 fns in Docker, pytest passes,
               saved to registry — "built its own transforms"
  4. ESCI      ★ non-food → A3 skips food blocks —
               "same code, different industry"
  5. STAGE B   ★ OFF+USDA Cheerios collapses to ONE row with
               sources=[OFF,USDA]. openFDA flips has_recall.
               ESCI stays separate. dq_delta visible.
  6. ESCI#2    registry hits everywhere, A2 never called —
               "the pipeline remembered"
```

---

## 12. Validation Story (Instructor Checklist)

```
  Q: "How deeply do you understand the data?"
  A: Named owner per source. Profile signals logged.
     Schema lists which source contributes which column.

  Q: "What tests did you write to verify LLM-generated code?"
  A: Static scan → Docker sandbox → type → nulls →
     PYTEST on sample I/O → HITL 2 review → registry.
     Every sample row becomes a parameterized test.

  Q: "Why were those tools truly necessary?"
     Kafka      ingestion transport + pipeline events → UC2
     S3         decouples ingestion from processing, the contract
     Airflow    scheduled batch + UC1 DAG trigger on S3 events
     FAISS      cheap tier between rules (~60%) and LLM (~5%)
     Docker     isolates LLM-generated code from host
     Groq/Claude pluggable, default Groq for cost

  Q: "How much of the architecture is actually implemented?"
  A: Every box on every diagram maps to a real file under src/.
     Not-built items named explicitly in §10.
```

---

## 13. Sign-Off Checklist

```
  [ ] 4 sources land in s3://bronze/
  [ ] Kafka topics live, S3 sink writing
  [ ] Airflow DAGs trigger UC1 on S3 events
  [ ] A3 profile signals logged per run
  [ ] A2 generated fns persist + pytest passes
  [ ] Docker sandbox blocks network + fs (hostile test)
  [ ] HITL 1/2/3 clickable in demo UI
  [ ] id_reconcile finds barcode matches OFF↔USDA
  [ ] collapse_cluster rows have sources=[OFF,USDA]
  [ ] recall_annotate flips has_recall on matches
  [ ] dq_delta non-zero on OFF
  [ ] S1/S2/S3/S4 call counts logged (S4 surgical)
  [ ] ONE unified_catalog.parquet output
  [ ] Each team member walks any source without notes
```

---

## 14. File References

```
  ARCHITECTURE.md           current agentic POC (to rewrite)
  dedup_demo.py + steps.md  demo POC source of truth
  Final_Project_Proposal    §5.1 sources, §5.4 arch, §5.5 LLM
  BIG-DATA FINAL prep.pdf   POC4 status, not-built list
  instructions.md           instructor grading bar
```
