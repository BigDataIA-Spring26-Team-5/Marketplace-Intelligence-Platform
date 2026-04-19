# ETL Pipeline — Agentic Workflow Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                              INPUT DATA SOURCES                                      ║
║                                                                                      ║
║  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────────────┐ ║
║  │  usda_fooddata      │  │  fda_recalls        │  │  openfoodfacts (12 GB)      │ ║
║  │  _sample.csv        │  │  _sample.csv        │  │  + synthetic_dataset_*.csv  │ ║
║  │  (nutrition domain) │  │  (safety domain)    │  │  + usda_raw/                │ ║
║  └──────────┬──────────┘  └──────────┬──────────┘  └─────────────┬───────────────┘ ║
╚═════════════╪══════════════════════════╪══════════════════════════╪════════════════╝
              └──────────────────────────┴──────────────────────────┘
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 0 — load_source_node                             [orchestrator.py]            ║
║                                                                                      ║
║   • Auto-detect CSV delimiter (comma / tab / pipe)                                  ║
║   • Recognize 25+ null sentinels  ("NA", "null", "unknown", …)                      ║
║   • Adaptive sampling: profile ~5K rows  (full data streamed later in Node 5)       ║
║                                                                                      ║
║   OUT: source_df, source_schema, source_sep, sampling_strategy                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 1 — analyze_schema_node          ┌─────────────────────────────────────┐     ║
║                                        │  AGENT 1  (Orchestrator LLM)        │     ║
║   • Diff source schema vs.             │  model: deepseek/deepseek-chat      │     ║
║     config/unified_schema.json (14 cols│  via LiteLLM  [src/models/llm.py]  │     ║
║                                        └─────────────────────────────────────┘     ║
║   8-Primitive Classification per column:                                            ║
║   ┌────────┐ ┌──────┐ ┌────────┐ ┌────────┐ ┌─────┐ ┌───────┐ ┌───────┐ ┌───────┐║
║   │ RENAME │ │ CAST │ │ FORMAT │ │ DELETE │ │ ADD │ │ SPLIT │ │ UNIFY │ │DERIVE ││║
║   └────────┘ └──────┘ └────────┘ └────────┘ └─────┘ └───────┘ └───────┘ └───────┘║
║                                                                                      ║
║   OUT: operations[], column_mapping, enrichment_columns_to_generate                 ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 2 — critique_schema_node         ┌─────────────────────────────────────┐     ║
║                                        │  AGENT 2  (Critic LLM)              │     ║
║   • Validates Agent 1 operations       │  model: deepseek/deepseek-reasoner  │     ║
║   • Applies 7 deterministic rules:     │  (fallback: deepseek-chat)          │     ║
║     R4: RENAME w/ incompatible type    └─────────────────────────────────────┘     ║
║         → CAST                                                                       ║
║     R6: Uncovered source cols → DELETE                                              ║
║     R7: Identity cols → normalize_before_dedup=true                                 ║
║     + 4 additional semantic rules                                                   ║
║                                                                                      ║
║   OUT: revised_operations, critique_notes (audit trail)                             ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 3 — check_registry_node                          [orchestrator.py]            ║
║                                                                                      ║
║   Block Registry scan  [src/registry/block_registry.py]                            ║
║   13 static blocks:                                                                  ║
║   ┌──────────────────────┬────────────────────────┬──────────────────────────────┐  ║
║   │ CLEANING             │ ENRICHMENT             │ DEDUP / QUALITY              │  ║
║   │ strip_whitespace     │ extract_allergens      │ fuzzy_deduplicate            │  ║
║   │ lowercase_brand      │ extract_quantity_col   │ column_wise_merge            │  ║
║   │ remove_noise_words   │ keep_quantity_in_name  │ golden_record_select         │  ║
║   │ strip_punctuation    │ llm_enrich             │ dq_score_pre / dq_score_post │  ║
║   └──────────────────────┴────────────────────────┴──────────────────────────────┘  ║
║                                                                                      ║
║   + Dynamic blocks discovered from:                                                  ║
║     src/blocks/generated/<domain>/DYNAMIC_MAPPING_*.yaml                            ║
║                                                                                      ║
║   HITL gate (Streamlit UI) ─── user approves missing-column decisions               ║
║                                                                                      ║
║   Generates/updates mapping YAML ──► src/blocks/generated/<domain>/                 ║
║                                                                                      ║
║   OUT: block_registry_hits, mapping_yaml_path, enrich_alias_ops                     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 4 — plan_sequence_node           ┌─────────────────────────────────────┐     ║
║                                        │  AGENT 3  (Orchestrator LLM)        │     ║
║   • Receives default block pool        │  model: deepseek/deepseek-chat      │     ║
║     (domain-specific)                  └─────────────────────────────────────┘     ║
║   • Reorders blocks for optimal                                                      ║
║     execution (cannot add/remove)                                                   ║
║   • Ensures DQ scoring at correct positions                                         ║
║                                                                                      ║
║   OUT: block_sequence[], sequence_reasoning                                         ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  NODE 5 — run_pipeline_node   (CHUNKED STREAMING — 10K rows/chunk)                 ║
║                                                [src/pipeline/runner.py]             ║
║                                                                                      ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │  For each chunk:                                                               │ ║
║  │                                                                                │ ║
║  │  column_mapping (RENAME ops)                                                   │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  DynamicMappingBlock  ◄── mapping YAML  (CAST / FORMAT / SPLIT / UNIFY /      │ ║
║  │         │                                DERIVE / ADD / DELETE ~30 handlers)  │ ║
║  │         ▼                                                                      │ ║
║  │  strip_whitespace ──► lowercase_brand ──► remove_noise_words                  │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  strip_punctuation ──► extract_quantity_column ──► keep_quantity_in_name       │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  fuzzy_deduplicate ──► column_wise_merge ──► golden_record_select              │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  dq_score_pre  (baseline quality score)                                        │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  ┌──────────────────────────────────────────────────────────────────────────┐ │ ║
║  │  │  LLMEnrichBlock  [src/blocks/llm_enrich.py]                             │ │ ║
║  │  │                                                                          │ │ ║
║  │  │   S1: Deterministic  ──► rule-based  (allergens, dietary_tags,          │ │ ║
║  │  │       [enrichment/deterministic.py]    is_organic)                      │ │ ║
║  │  │              │  (if null after S1)                                      │ │ ║
║  │  │              ▼                                                           │ │ ║
║  │  │   S2: KNN Embedding  ──► FAISS corpus search  (primary_category)        │ │ ║
║  │  │       [enrichment/embedding.py]   ◄── corpus/faiss_index.bin            │ │ ║
║  │  │              │  (if low confidence after S2)                            │ │ ║
║  │  │              ▼                                                           │ │ ║
║  │  │   S3: RAG-LLM  ──► LLM + retrieved context  (primary_category)         │ │ ║
║  │  │       [enrichment/llm_tier.py]   model: deepseek/deepseek-chat          │ │ ║
║  │  └──────────────────────────────────────────────────────────────────────────┘ │ ║
║  │         │                                                                      │ ║
║  │         ▼                                                                      │ ║
║  │  dq_score_post  (post-enrichment quality score + delta)                        │ ║
║  │                                                                                │ ║
║  │  Audit log: rows_in / rows_out per block                                      │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
║                                                                                      ║
║  Post-enrichment validation: quarantine rows with nulls in required fields          ║
║                                                                                      ║
║  OUT: working_df, quarantined_df, audit_log, dq_score_pre/post, enrichment_stats    ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
              ┌─────────────────────┐        ┌──────────────────────┐
              │  CLEAN DATA         │        │  QUARANTINED ROWS    │
              │  (working_df)       │        │  (quarantined_df)    │
              └──────────┬──────────┘        └──────────┬───────────┘
                         │                              │
                         ▼                              ▼
╔══════════════════════════════════╗      ┌─────────────────────────────┐
║  NODE 6 — save_output_node       ║      │  Quarantine log displayed   │
║                                  ║      │  in Streamlit UI            │
║  Writes to:                      ║      │  (row indices + reasons)    │
║  output/{dataset}_unified.csv    ║      └─────────────────────────────┘
╚══════════════════════════════════╝
                         │
                         ▼
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                              OUTPUT DESTINATIONS                                     ║
║                                                                                      ║
║  ┌────────────────────────┐  ┌────────────────────────┐  ┌──────────────────────┐  ║
║  │ /output/               │  │ /src/blocks/generated/ │  │ /corpus/             │  ║
║  │ {dataset}_unified.csv  │  │ <domain>/DYNAMIC_      │  │ faiss_index.bin      │  ║
║  │ (14-column schema)     │  │ MAPPING_*.yaml         │  │ corpus_metadata.json │  ║
║  │                        │  │ (reusable mappings)    │  │ (KNN embeddings)     │  ║
║  └────────────────────────┘  └────────────────────────┘  └──────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## Execution Modes

```
                         ┌─────────────────────────────────────┐
                         │            app.py (Streamlit)        │
                         │  5-step HITL Wizard                  │
                         │                                       │
                         │  Step 0: Source selection            │
                         │  Step 1: Schema analysis review      │
                         │  Step 2: Agent 2 corrections +       │
                         │          missing column decisions     │
                         │  Step 3: Pipeline execution          │
                         │  Step 4: Results + quarantine view   │
                         └─────────────────┬───────────────────┘
                                           │
                               ┌───────────┴───────────┐
                               ▼                       ▼
                     ┌──────────────────┐   ┌──────────────────┐
                     │  demo.py (CLI)   │   │ LangGraph DAG    │
                     │  3 sequential    │   │ [agents/graph.py]│
                     │  demo runs:      │   │                  │
                     │  - USDA          │   │  load_source     │
                     │  - FDA           │   │       │          │
                     │  - FDA replay    │   │  analyze_schema  │
                     └──────────────────┘   │       │          │
                                            │  critique_schema │
                                            │       │          │
                                            │  check_registry  │
                                            │       │          │
                                            │  plan_sequence   │
                                            │       │          │
                                            │  run_pipeline    │
                                            │       │          │
                                            │  save_output     │
                                            │       │          │
                                            │      END         │
                                            └──────────────────┘
```

---

## Agent + Model Summary

```
 ┌────────────────┬──────────────────────────────┬──────────────────────────────┐
 │ Agent          │ Role                         │ Model                        │
 ├────────────────┼──────────────────────────────┼──────────────────────────────┤
 │ Agent 1        │ Schema gap detection,        │ deepseek/deepseek-chat       │
 │ (Orchestrator) │ 8-primitive classification,  │ via LiteLLM                  │
 │                │ confidence scoring           │                              │
 ├────────────────┼──────────────────────────────┼──────────────────────────────┤
 │ Agent 2        │ Validate + correct Agent 1   │ deepseek/deepseek-reasoner   │
 │ (Critic)       │ ops, apply 7 rules           │ (fallback: deepseek-chat)    │
 ├────────────────┼──────────────────────────────┼──────────────────────────────┤
 │ Agent 3        │ Block sequence ordering      │ deepseek/deepseek-chat       │
 │ (Orchestrator) │ from available registry      │ via LiteLLM                  │
 ├────────────────┼──────────────────────────────┼──────────────────────────────┤
 │ LLMEnrich S3   │ RAG-augmented enrichment     │ deepseek/deepseek-chat       │
 │ (Enrichment)   │ for primary_category         │ via LiteLLM                  │
 └────────────────┴──────────────────────────────┴──────────────────────────────┘
```

---

## Key File Map

```
ETL/
├── app.py                          ← Streamlit HITL UI (5-step wizard)
├── demo.py                         ← CLI demo runner
├── config/
│   ├── unified_schema.json         ← Target 14-column schema + DQ weights
│   └── litellm_config.yaml         ← LLM provider routing
├── data/                           ← Input CSV sources
├── output/                         ← Unified CSV outputs
├── corpus/
│   ├── faiss_index.bin             ← KNN vector index
│   └── corpus_metadata.json
└── src/
    ├── agents/
    │   ├── graph.py                ← LangGraph DAG definition
    │   ├── orchestrator.py         ← Agent 1 (nodes 0, 1, 3)
    │   ├── critic.py               ← Agent 2 (node 2)
    │   ├── state.py                ← PipelineState TypedDict (100+ fields)
    │   └── prompts.py              ← LLM prompts
    ├── blocks/
    │   ├── base.py                 ← Block ABC
    │   ├── dynamic_mapping.py      ← YAML-driven ops (~30 handlers)
    │   ├── llm_enrich.py           ← 3-tier enrichment orchestrator
    │   ├── dq_score.py             ← Pre/post quality scoring
    │   ├── fuzzy_deduplicate.py
    │   ├── golden_record_select.py
    │   └── generated/              ← Auto-generated YAML mappings
    │       ├── nutrition/
    │       ├── safety/
    │       └── test/
    ├── enrichment/
    │   ├── deterministic.py        ← S1: rule-based
    │   ├── embedding.py            ← S2: KNN FAISS
    │   ├── llm_tier.py             ← S3: RAG-LLM
    │   └── corpus.py               ← FAISS index management
    ├── pipeline/
    │   ├── runner.py               ← Chunked block sequencing + audit log
    │   └── checkpoint/manager.py  ← Checkpoint save/resume
    ├── registry/
    │   └── block_registry.py       ← 13 static + dynamic blocks
    ├── schema/
    │   ├── analyzer.py             ← DataFrame profiling
    │   └── sampling.py             ← Adaptive sampling
    ├── models/
    │   └── llm.py                  ← LiteLLM wrapper
    └── ui/
        ├── components.py           ← Streamlit renderers
        └── styles.py               ← CSS styles
```
