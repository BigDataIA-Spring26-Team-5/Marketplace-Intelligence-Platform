# Implementation Plan: Gold Layer Pipeline

**Spec**: [spec.md](./spec.md)  
**Created**: 2026-04-21  
**Status**: Planning

---

## Technical Context

### Technologies

| Technology | Purpose | Status |
|------------|---------|--------|
| Python 3.11 | Runtime | Existing |
| pandas | DataFrame operations | Existing |
| pyarrow | Parquet read/write, schema validation | Existing |
| rapidfuzz | Fuzzy string matching (dedup) | Existing |
| faiss-cpu | Batch KNN similarity (S2) | Check install |
| sentence-transformers | Embedding model (S2) | Check install |
| redis-py | LLM cache (S3) | Optional (SQLite fallback) |
| google-cloud-storage | GCS read/write | Existing |
| LiteLLM | LLM routing (S3) | Existing |

### Dependencies

| Dependency | Status |
|------------|--------|
| Silver pipeline producing unified-schema Parquet | Required - must exist |
| Existing dedup blocks (FuzzyDeduplicateBlock, ColumnWiseMergeBlock, GoldenRecordSelectBlock) | Existing - reuse |
| GCS buckets (mip-silver-2024, mip-gold-2024) | Required - must exist |

### Unknowns

All resolved via `/speckit.clarify`:
1. ✅ Missing sources → Run without sr_legacy/survey
2. ✅ Threshold tuning → 10K sample first
3. ✅ Cache fallback → SQLite when Redis unavailable
4. ✅ dq_score_post → Add after Stage 3
5. ✅ Memory strategy → Eager load; lazy fallback if OOM

---

## Constitution Check

### I. Schema-First Gap Analysis
**Status**: ✅ COMPLIANT  
Gold validates Silver schema contract before processing. Schema mismatches abort pipeline. No gap analysis needed — Gold receives pre-unified data.

### II. Three-Agent Pipeline
**Status**: ⚪ NOT APPLICABLE  
Gold is a data transformation pipeline, not the schema-analysis agent flow. Existing dedup blocks are reused without modification.

### III. Declarative YAML Execution
**Status**: ⚪ NOT APPLICABLE  
Gold does not generate YAML mappings. It processes already-mapped Silver data through existing blocks.

### IV. Human Approval Gates
**Status**: ⚠️ NEEDS CONSIDERATION  
Spec does not define HITL gates for Gold. Recommendation: Add optional `--review-clusters` flag to pause and display top-N largest dedup clusters before commit. Not blocking for MVP.

### V. Cascading Enrichment with Safety Boundaries
**Status**: ✅ COMPLIANT  
- S1 → S2 → S3 order enforced
- `allergens` is S1-only (keyword extraction)
- `brand_name`, `serving_size`, `serving_size_unit` eligible for S2/S3
- Safety fields never LLM-enriched

### VI. Self-Extending Mapping Memory
**Status**: ⚪ NOT APPLICABLE  
Gold does not generate mappings.

### VII. Data Quality and Quarantine
**Status**: ✅ COMPLIANT  
- `dq_score_pre` comes from Silver
- `dq_score_post` computed after Stage 3
- Quarantine: rows failing required-field validation after enrichment are quarantined (implicit — need to confirm existing block behavior)

### VIII. Production Scale
**Status**: ✅ COMPLIANT  
- Volume: 1.25M rows expected, eager load with lazy fallback
- Batch-only LLM: S3 batches 10-20 records per call
- Checkpointing: Run log after each stage (can be extended)
- Batched enrichment: S2 uses batch FAISS, S3 uses grouped batching
- Max LLM calls: Configurable via `GOLD_MAX_LLM_CALLS`

---

## Gate Evaluation

| Gate | Result |
|------|--------|
| Constitution compliance | ✅ PASS (2 N/A, 1 consideration) |
| All unknowns resolved | ✅ PASS |
| Dependencies available | ✅ PASS (existing blocks, GCS) |

**Proceed to Phase 0.**

---

## Phase 0: Research

No NEEDS CLARIFICATION items remain. Research focuses on:
1. Existing dedup block interfaces
2. S2 batch FAISS patterns
3. SQLite cache schema

See [research.md](./research.md) for findings.

---

## Phase 1: Design

### Data Model
See [data-model.md](./data-model.md)

### Contracts
Gold exposes:
- CLI interface (documented in spec Section 9)
- Run log JSON schema (documented in spec Section 8)
- No external API contracts

### Quickstart
See [quickstart.md](./quickstart.md)

---

## File Structure

```
src/pipeline/gold/
├── __init__.py
├── __main__.py          # Entry point
├── cli.py               # Argument parsing
├── silver_reader.py     # Stage 1: Read + validate
├── schema_contract.py   # Schema definition
├── dedup.py             # Stage 2: Orchestrate dedup blocks
├── enrichment/
│   ├── __init__.py
│   ├── tier1_deterministic.py
│   ├── tier2_knn.py
│   ├── tier3_rag_llm.py
│   └── provenance.py
├── dq_score.py          # Post-enrichment DQ
├── writer.py            # Stage 4: Write Gold Parquet
└── run_log.py           # Run log generation
```

---

## Next Steps

1. `/speckit.tasks` to generate implementation tasks
2. Implement Stage 1 (silver_reader, schema_contract)
3. Implement Stage 2 (dedup orchestrator)
4. Implement Stage 3 (enrichment tiers)
5. Implement Stage 4 (writer, run_log)
6. Integration testing with sample data
