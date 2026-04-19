# Research: Checkpoint and Resume Capability

## Phase 0: Research & Decisions

### Decision 1: Atomic Checkpoint Write Strategy

**Chosen**: SQLite transaction with atomic rename

**Rationale**: Atomic rename (write to temp file, then os.rename()) is the standard SQLite pattern for safe writes. The rename operation is atomic on POSIX filesystems, ensuring no partial checkpoint file is ever visible to readers.

**Alternatives evaluated**:
- SQLite transaction with explicit COMMIT — relies on SQLite's built-in atomicity, but doesn't protect against crashes between COMMIT and fsync
- Write-ahead journal — SQLite default, but we need explicit temp-file approach to guarantee atomicity
- In-memory SQLite with periodic dump — adds complexity, not needed for this use case

### Decision 2: Checkpoint Schema Version Strategy

**Chosen**: Auto-increment integer in `.specify/requiredlimits.yaml`

**Rationale**: Simple, reliable, deterministic. Stored in checkpoint and compared on resume. Easy to test and debug.

**Alternatives evaluated**:
- Git commit hash — couples checkpoint to source control, adds complexity for CI/CD scenarios
- Timestamp — non-deterministic, hard to compare reliably

### Decision 3: Schema Change Handling on Resume

**Chosen**: Warn + force-fresh required

**Rationale**: Data integrity is paramount. Automatically resuming with a changed schema could produce silent data corruption. Operator must explicitly confirm they want to discard the checkpoint.

### Decision 4: Different Dataset Checkpoint Handling

**Chosen**: Checkpoint stores source dataset identifier, validates on resume

**Rationale**: Running a checkpoint against a different dataset would produce incorrect results. Automatic validation catches this before any processing.

**Implementation**: Store source file path/sha256 in checkpoint. On resume, validate against current source file.

### Decision 5: FAISS Index Serialization Strategy

**Chosen**: faiss.Index.write_index/read_index binary format

**Rationale**: Native FAISS serialization handles all index types correctly. Built into FAISS library.

**Alternatives evaluated**:
- pickle.dump/load — works but FAISS native format is more robust across versions

---

## Technical Notes

### Existing Codebase Integration Points

1. **PipelineRunner** (`src/pipeline/runner.py`):
   - Entry point for chunk execution
   - Already has audit_log tracking — checkpoint can persist this
   - Called from CLI/Streamlit entry points

2. **Corpus** (`src/enrichment/corpus.py`):
   - Already has `save_corpus()` and `load_corpus()` using FAISS native format
   - Index stored at `corpus/faiss_index.bin`
   - Metadata stored at `corpus/corpus_metadata.json`

3. **Chunk boundaries**:
   - Need to determine how chunks are defined in current pipeline
   - Likely: source file is split into chunks before processing
   - Checkpoint needs to know chunk boundaries

### What Needs Clarification (During Implementation)

- How does the current pipeline define chunk boundaries?
- What triggers the next chunk (file split, batch size, etc.)?
- Where is the transformation plan YAML stored/loaded?
- Is there an existing config file for pipeline settings?