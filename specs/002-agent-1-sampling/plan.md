# Implementation Plan: Agent 1 Representative Sampling

**Branch**: `[002-agent-1-sampling]` | **Date**: 2026-04-17 | **Spec**: specs/002-agent-1-sampling/spec.md
**Input**: Feature specification from `/specs/002-agent-1-sampling/spec.md`

## Summary

Enhance Agent 1 (Orchestrator) to perform representative row sampling before schema gap analysis, preventing false gap detections caused by sparse or missing values in initial rows. The solution implements adaptive sampling that scales based on dataset size, plus confidence scoring for gap classifications to help HITL prioritize uncertain mappings.

## Technical Context

**Language/Version**: Python 3.11 (existing codebase)  
**Primary Dependencies**: pandas, LiteLLM (existing), no new dependencies required  
**Storage**: N/A (in-memory processing)  
**Testing**: pytest (existing)  
**Target Platform**: Linux server (existing Streamlit app)  
**Project Type**: ETL pipeline enhancement (existing codebase)  
**Performance Goals**: Sample size formula ensures token usage stays within 2x baseline  
**Constraints**: LLM token cost is primary constraint, not compute  
**Scale**: Enhancement to handle datasets from 100 to 1,000,000 rows

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I. Schema-First Gap Analysis | ✅ Pass | Enhanced sampling improves gap detection accuracy |
| II. Two-Agent Architecture | ✅ Pass | No change to agent count or roles |
| III. Declarative YAML-Driven | ✅ Pass | No change — sampling happens before YAML generation |
| IV. HITL Approval | ✅ Pass | Confidence scores enhance existing Gate 1 |
| V. Cascading Enrichment | ✅ Pass | No change to enrichment flow |
| VI. Self-Extending Memory | ✅ Pass | No change to YAML caching |
| VII. Data Quality Scoring | ✅ Pass | Sampling improves DQ detection |

**Constitution Impact**: None — this enhancement augments existing functionality without violating principles.

## Project Structure

### Documentation (this feature)

```text
specs/002-agent-1-sampling/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (if needed)
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

The feature modifies existing files in `src/schema/` and `src/agents/`:

```text
src/
├── schema/
│   └── analyzer.py      # MODIFY: Add representative sampling
├── agents/
│   ├── orchestrator.py # MODIFY: Pass sample metadata to LLM
│   └── state.py         # MODIFY: Add confidence_score to GapItem
├── blocks/
│   └── base.py         # No change
└── ui/
    └── components.py    # MODIFY: Display confidence scores
```

Tests go in `tests/` (existing test structure).

**Structure Decision**: Single Python project enhancement — adds sampling logic to existing schema analyzer without changing project structure.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | This is a focused enhancement to existing code |

## Phase 0: Research

### Unknowns to Resolve

1. **Sampling method for schema detection**: What statistical methods ensure representative sampling for categorical value detection?
2. **Confidence scoring for LLM**: How to extract/calculate confidence from LLM gap classifications?
3. **Adaptive sample size formula**: What formula balances accuracy vs token cost across dataset sizes?

### Research Tasks

- Task: "Research statistical sampling methods for schema profiling"
- Task: "Research LLM confidence scoring techniques"
- Task: "Research adaptive sampling formulas for data pipelines"