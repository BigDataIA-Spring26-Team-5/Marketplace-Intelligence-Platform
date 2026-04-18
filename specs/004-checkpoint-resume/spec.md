# Feature Specification: Checkpoint and Resume Capability

**Feature Branch**: `[004-checkpoint-resume]`  
**Created**: 2026-04-18  
**Status**: Draft  
**Input**: User description: "Implement checkpointing and resume capability. Pipeline currently has no failure recovery — a crash at any node loses all progress. Requirements: - Save pipeline state to SQLite after each chunk completes - On restart, detect existing checkpoint and resume from last completed chunk - Checkpoint must store: completed chunks, transformation plan, enrichment corpus built so far - Zero new infrastructure — SQLite only - Must not change the 7-node pipeline structure"

## Clarifications

### Session 2026-04-18

- Q: Atomic checkpoint write strategy → A: SQLite transaction with atomic rename
- Q: Checkpoint schema version strategy → A: Auto-increment integer in config
- Q: Schema change handling on resume → A: Warn + force-fresh required
- Q: Different dataset checkpoint handling → A: Checkpoint stores source dataset identifier, validates on resume
- Q: FAISS index serialization strategy → A: faiss.Index.write_index/read_index binary format

### User Scenarios & Testing *(mandatory)*

### User Story 1 - Pipeline Resumes After Crash (Priority: P1)

A long-running ETL pipeline processes 50k+ records in chunks. Mid-way through execution, the process crashes due to infrastructure failure (machine restart, OOM, network timeout). When the operator restarts the pipeline, it automatically detects the existing checkpoint and resumes from the last successfully completed chunk, avoiding reprocessing of already-handled data.

**Why this priority**: Production-scale pipelines that process large datasets cannot afford to restart from scratch on every interruption. Without checkpointing, a 4-hour job that fails at hour 3 loses all progress.

**Independent Test**: Simulate a mid-chunk crash by killing the pipeline process, then restart and verify that only unprocessed chunks are handled.

**Acceptance Scenarios**:

1. **Given** a pipeline has completed 3 chunks of 5, **When** the process crashes, **Then** restarting the pipeline resumes from chunk 4
2. **Given** pipeline crash occurs during chunk 4 processing, **When** restarted, **Then** chunk 4 is reprocessed from the beginning (not resumed mid-chunk)
3. **Given** no checkpoint exists (first run), **When** pipeline starts, **Then** it runs normally without attempting to resume

---

### User Story 2 - Checkpoint Integrity Validation (Priority: P2)

Before resuming from a checkpoint, the operator wants to verify that the checkpoint is valid — the stored transformation plan, corpus, and chunk metadata are consistent and can be safely resumed.

**Why this priority**: Resuming from a corrupted checkpoint could produce incorrect data or silent failures. Operators need confidence that resuming is safe.

**Independent Test**: Create a checkpoint, artificially corrupt one field, and verify the system detects the corruption and offers to start fresh.

**Acceptance Scenarios**:

1. **Given** a valid checkpoint exists, **When** pipeline checks integrity, **Then** it proceeds with resume
2. **Given** checkpoint has missing/corrupted data, **When** pipeline validates, **Then** it warns operator and offers to start fresh
3. **Given** checkpoint schema version mismatches current pipeline version, **Then** it is treated as invalid and requires operator decision

---

### User Story 3 - Manual Resume Control (Priority: P3)

The operator wants explicit control over resume behavior — choosing to resume from a checkpoint, start fresh despite an existing checkpoint, or clear checkpoints before a fresh run.

**Why this priority**: Some scenarios require starting fresh (e.g., schema changes, debugging, testing). Operators should not be forced to manually delete checkpoints.

**Acceptance Scenarios**:

1. **Given** a checkpoint exists, **When** operator specifies "resume", **Then** pipeline resumes from checkpoint
2. **Given** a checkpoint exists, **When** operator specifies "force-fresh", **Then** pipeline starts from scratch and overwrites checkpoint
3. **Given** operator explicitly clears checkpoint, **Then** next run starts fresh without prompt

---

### Edge Cases

- Source schema changes: **Warn + force-fresh required** (operator must explicitly confirm)
- Checkpoint from different dataset: **Detected via source dataset identifier, validates on resume**
- Disk fills up during checkpoint write: **Atomic rename ensures no partial checkpoint**
- Transformation plan changes: **Treated as schema change, requires force-fresh**
- Checkpoint interacts with quarantine and DQ score tracking: **Preserved via ChunkState metadata**

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST save pipeline state to a SQLite database file after each chunk completes processing
- **FR-002**: System MUST detect existing checkpoints on pipeline startup and offer to resume
- **FR-003**: Checkpoint MUST store: completed chunk IDs, transformation plan YAML, and enrichment corpus state built so far
- **FR-004**: System MUST use SQLite only — no new database infrastructure permitted
- **FR-005**: Checkpoint save/load MUST NOT alter the 7-node pipeline structure or execution order
- **FR-006**: Resume MUST begin from the first incomplete chunk, skipping all marked-completed chunks
- **FR-007**: Checkpoint validation MUST occur before any processing begins on resume
- **FR-008**: Operator MUST be able to explicitly force a fresh run despite existing checkpoint

### Pipeline Governance Constraints *(mandatory when applicable)*

- Checkpoint does not affect unified schema — checkpoint stores applied schema gaps but does not modify schema contract
- Checkpoint stores the YAML transformation plan as-is; plan is revalidated on resume
- No new HITL gates introduced; checkpoint behavior is transparent to operatorsexcept at startup decision point
- Checkpoint preserves enrichment tier decisions — S1/S2/S3 results stored in corpus are reused on resume
- DQ scores for completed chunks are preserved — only incomplete chunks require recomputation

### Key Entities *(include if feature involves data)*

- **Checkpoint**: Persistent record containing pipeline run context, completed chunks list, transformation plan, corpus state, timestamp, and run configuration
- **ChunkState**: Metadata for each chunk including: chunk index, status (pending/completed/failed), record count, and optional DQ scores
- **CorpusSnapshot**: Serialized enrichment corpus state enabling fast resumption without rebuilding from source
- **TransformationPlan**: The YAML-generated operations list from the three-agent pipeline

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A pipeline processing 50k records that crashes at chunk 5 resumes and completes remaining chunks 6-10 without reprocessing completed chunks
- **SC-002**: Checkpoint write takes less than 5 seconds per chunk (outside of enrichment timing)
- **SC-003**: Resume detection adds less than 2 seconds to startup time when no checkpoint exists
- **SC-004**: Operator can force fresh start via CLI flag, completing a 50k run in same time as non-checkpoint run

## Assumptions

- SQLite is acceptable for production deployments (no external database required)
- Checkpoint corruption is rare but possible — validation catches obvious issues
- Transformation plan changes are rare — when schema changes, operator starts fresh
- The enrichment corpus (FAISS index) can be serialized/deserialized efficiently
- Pipeline config `.specify/requiredlimits.yaml` supplies `checkpoint_interval` already configured