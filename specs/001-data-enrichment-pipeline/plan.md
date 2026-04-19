# Implementation Plan: Chunked CSV Processing

**Branch**: `[001-data-enrichment-pipeline]` | **Date**: 2026-04-18 | **Spec**: specs/001-data-enrichment-pipeline/spec.md

## Summary

Add chunked/streaming CSV processing to support arbitrarily large files without OOM. Uses pandas chunk reading with configurable batch sizes, integrates with existing CheckpointManager for resume capability between chunks, and updates both Schema Analysis and Enrichment to process incrementally.

## Technical Context

**Language/Version**: Python 3.11 (existing codebase)  
**Primary Dependencies**: pandas (existing), LiteLLM (existing), no new dependencies required  
**Storage**: Streaming CSV + SQLite checkpoint (existing)  
**Testing**: pytest (existing)  
**Target Platform**: Linux server (existing Streamlit app)  
**Project Type**: ETL pipeline enhancement (existing codebase)  
**Performance Goals**: Process any file size with bounded memory (~500MB max per chunk)  
**Constraints**: Memory is primary constraint, network for LLM calls  

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I. Schema-First Gap Analysis | ✅ Pass | Chunked processing preserves gap analysis workflow |
| II. Two-Agent Architecture | ✅ Pass | No change to agent count or roles |
| III. Declarative YAML-Driven | ✅ Pass | No change — YAML generation per chunk |
| IV. HITL Approval | ✅ Pass | HITL gates work per chunk |
| V. Cascading Enrichment | ✅ Pass | Enrichment processes chunks sequentially |
| VI. Self-Extending Memory | ✅ Pass | YAML caching per chunk |
| VII. Data Quality Scoring | ✅ Pass | DQ computed incrementally |

**Constitution Impact**: None — this enhancement augments existing functionality without violating principles.

## Project Structure

### Documentation (this feature)

```text
specs/001-data-enrichment-pipeline/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md         # Phase 1 output
├── quickstart.md         # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

The feature modifies existing files in `src/schema/` and adds streaming utilities:

```text
src/
├── pipeline/
│   ├── runner.py          # MODIFY: Add chunk iteration
│   └── checkpoint/
│       └── manager.py     # ENHANCE: Add chunk state tracking
├── schema/
│   ├── __init__.py        # MODIFY: Export streaming loader
│   └── analyzer.py        # MODIFY: Accept chunked input
├── utils/
│   └── csv_stream.py       # NEW: Streaming CSV loader
├── agents/
│   └── orchestrator.py    # MODIFY: Handle chunk state
```

### Key Entities (Enhanced)

| Entity | Changes |
|--------|---------|
| `CsvStreamReader` | NEW: Batched CSV reading with progress |
| `SchemaProfile` | MODIFY: Add chunk_index, total_chunks |
| `ChunkState` | NEW: Tracks per-chunk progress |
| `CheckpointManager` | ENHANCE: Chunk resume methods |
| `PipelineState` | MODIFY: Add chunk iteration state |

## Technical Design

### Chunking Strategy

1. **Chunk Size**: Configurable via `CHUNK_SIZE` env var (default: 10,000 rows)
2. **Memory Budget**: Each chunk loaded into pandas, processed, then optionally retained for final output
3. **Resume Points**:
   - Schema Analysis (once per source file - can reuse for any chunk)
   - Per-chunk: Transform → Enrich → DQ Score
4. **Final Output**: Aggregated from processed chunks or written incrementally

### Integration with Existing CheckpointManager

```python
class CheckpointManager:
    def save_chunk_state(self, run_id, chunk_index, stage, state):
        """Save progress at each pipeline stage."""
        
    def get_chunk_resume_state(self, run_id, chunk_index):
        """Get where to resume for a chunk."""
```

### Pipeline Flow (Chunked)

```
1. Load Source (stream) → Read CHUNK_SIZE rows
     ↓
2. Schema Analysis (once for entire file, not per chunk)
     ↓
3. For each chunk:
   a. Transform (YAML mapping)
   b. Enrich (S1→S2→S3)
   c. DQ Score
   d. Save checkpoint
     ↓
4. Aggregate results / write output
```

## Phase 0: Research

- **NEEDS CLARIFICATION**: What schema analysis applies to chunks vs full file?
  - Gap classification is per-full-file (needs representative sample)
  - Column statistics null_rate should be computed from full file (use header scan first)
- **Decision**: Schema analysis runs once on full file metadata (header + sample), transformations apply per chunk

## Phase 1: Design

### data-model.md additions

1. Add `ChunkState` entity to track per-chunk completion
2. Add `StreamingConfig` for chunk size and memory budgets

### quickstart.md

- Add "Large File Processing" section with --chunk-size flag

---

*Plan generated: 2026-04-18*
*Next: /speckit.tasks to generate implementation tasks*