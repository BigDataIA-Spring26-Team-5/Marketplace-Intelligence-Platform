# Implementation Plan: Checkpoint and Resume Capability

**Branch**: `[004-checkpoint-resume]` | **Date**: 2026-04-18 | **Spec**: spec.md
**Input**: Feature specification from `/specs/004-checkpoint-resume/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Persist pipeline state to SQLite after each chunk completes. On restart, detect existing checkpoint and resume from last completed chunk. Checkpoint stores: completed chunks, transformation plan YAML, enrichment corpus (FAISS index), dataset identifier, and schema version. Uses atomic rename for safe writes. Supports manual resume/force-fresh control.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: pandas, faiss, LiteLLM (lite-llm), sqlite3 (stdlib)  
**Storage**: SQLite (checkpoint.db), filesystem (corpus/faiss_index.bin)  
**Testing**: pytest  
**Target Platform**: Linux server  
**Project Type**: CLI/ETL pipeline  
**Performance Goals**: 50k+ records per run, <5s checkpoint write per chunk, <2s resume detection  
**Constraints**: Zero new infrastructure (SQLite only), must not alter 7-node pipeline structure  
**Scale/Scope**: Single large dataset (50k+ records) processed in chunks

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- Unified-schema impact is identified and aligned with `config/unified_schema.json` — **N/A** (checkpoint does not modify schema)
- Agent responsibilities remain within the current three-agent architecture — **N/A** (checkpoint is orthogonal to agents)
- Planned transformations use declarative YAML or existing blocks, not runtime-generated Python — **PASS** (uses SQLite, not code gen)
- HITL approval points and quarantine behavior are identified when the feature affects them — **PASS** (checkpoint preserves quarantine state via ChunkState)
- Enrichment changes preserve deterministic-only handling for safety fields — **N/A** (checkpoint serializes existing corpus, doesn't change enrichment logic)
- DQ scoring, generated mapping persistence, and documentation/runtime guidance updates are covered — **PASS** (DQ scores stored in checkpoint)

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
# [REMOVE IF UNUSED] Option 1: Single project (DEFAULT)
src/
├── models/
├── services/
├── cli/
└── lib/

tests/
├── contract/
├── integration/
└── unit/

# [REMOVE IF UNUSED] Option 2: Web application (when "frontend" + "backend" detected)
backend/
├── src/
│   ├── models/
│   ├── services/
│   └── api/
└── tests/

frontend/
├── src/
│   ├── components/
│   ├── pages/
│   └── services/
└── tests/

# [REMOVE IF UNUSED] Option 3: Mobile + API (when "iOS/Android" detected)
api/
└── [same as backend above]

ios/ or android/
└── [platform-specific structure: feature modules, UI flows, platform tests]
```

**Structure Decision**: Single project - extend `src/pipeline/` with checkpoint module

```text
src/
├── pipeline/
│   ├── runner.py        # Existing: executes blocks
│   ├── checkpoint.py   # NEW: checkpoint save/load/resume logic
│   └── __init__.py
└── enrichment/
    ├── corpus.py        # Existing: FAISS index management
    └── ...

tests/
├── unit/
│   └── test_checkpoint.py  # NEW
└── ...
```

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
