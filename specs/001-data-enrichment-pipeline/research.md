# Research: Chunked CSV Processing

## Decision: Chunk-based Schema Analysis

**Question**: Should schema analysis run once per file or per chunk?

**Answer**: Run ONCE per source file (not per chunk)

**Rationale**:
- Column gap classification shouldn't differ between chunks - the schema is constant
- Null rates should be computed from sampling the full file before chunk processing (header row + statistical sample)
- Computing schema per-chunk would be wasteful and inconsistent

**Alternatives evaluated**:
1. Per-chunk analysis: ❌ Wasteful, inconsistent across chunks
2. Full-file-first, then chunk processing: ✅ Efficient, correct
3. Hybrid: Analyze full file header + first 10k rows, verify with sample from later chunks: ✅ For confidence, but defer unless issues seen

---

## Decision: Checkpoint Integration

**Question**: Where to resume if pipeline crashes mid-chunk?

**Answer**: Resume at chunk boundary after last successful chunk completion

**Rationale**:
- CheckpointManager already tracks chunk states
- Each chunk's transform + enrich + DQ should be atomic
- Schema analysis results cached and reused for all chunks

**Implementation**:
- Schema analysis only needs to run once (cache in BlockRegistry)
- Chunk processing saves checkpoint at end of each chunk
- If crash, resume from last completed chunk index