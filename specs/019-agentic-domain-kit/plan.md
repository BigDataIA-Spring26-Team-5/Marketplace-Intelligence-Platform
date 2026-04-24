# Implementation Plan: Agentic Domain Kit Builder

**Branch**: `019-agentic-domain-kit` | **Date**: 2026-04-24 | **Spec**: specs/019-agentic-domain-kit/spec.md

## Summary

Replace the existing single-shot LLM domain pack generator with two LangGraph graphs — one for sequential multi-step pack generation with auto-retry, one for block scaffold generation with syntax-fix retry — integrated into the existing Streamlit Domain Packs UI via the `run_step`-style HITL pattern. No changes to the main ETL pipeline graph or prompts.

## Technical Context

**Language/Version**: Python 3.11  
**Primary Dependencies**: LangGraph 0.4, LiteLLM 1.55, Streamlit, PyYAML, pandas 2.2 (already in stack)  
**Storage**: Local filesystem (`domain_packs/<domain>/`); no new DB  
**Testing**: pytest; unit tests mock `call_llm_json`; integration tests hit real LLM  
**Target Platform**: Linux (same VM as existing stack)  
**Project Type**: Library module + Streamlit UI  
**Performance Goals**: Pack generation completes in <60s (3 sequential LLM calls + validation)  
**Constraints**: No new dependencies; no edits to `src/agents/graph.py` or `src/agents/prompts.py`  
**Scale/Scope**: UI feature; 4 fixture CSVs as canonical test inputs

## Constitution Check

- **Domain-schema impact**: None. This feature generates domain packs; it does not process data through `config/schemas/<domain>_schema.json`. No schema file is created or modified by this feature (domain schemas are created on first pipeline run). ✅
- **Agent responsibilities**: Two new graphs (`DomainKitGraph`, `ScaffoldGraph`) are fully separate from the three-agent ETL pipeline. Agent 1/2/3 roles unchanged. ✅
- **Declarative YAML / no runtime code gen**: Generated code is the scaffold Python file — this is source code for human review and approval before being saved, not runtime-generated transformation logic. No `DynamicMappingBlock` is generated. ✅
- **HITL approval points**: FR-3 mandates explicit approval before any file write. `commit_to_disk` and `save_to_custom_blocks` nodes are gated by Streamlit Approve button. ✅
- **Enrichment safety boundaries**: This feature does not touch enrichment execution. The prompts for `generate_enrichment_rules` must instruct the LLM to mark safety fields as deterministic-only — covered in `domain_kit_prompts.py`. ✅
- **New domain — zero `src/` edits**: The feature itself generates domain packs; the generated pack's presence in `domain_packs/<domain>/` is sufficient for the pipeline to use it. ✅
- **SC-002**: Not directly impacted by this feature. Existing test coverage maintained. ✅
- **DQ scoring / mapping persistence / docs**: Not impacted — this feature generates domain pack files, not pipeline run artifacts. quickstart.md updated. ✅

## Project Structure

### Documentation (this feature)

```text
specs/019-agentic-domain-kit/
├── plan.md                        # This file
├── research.md                    # Phase 0 — decisions + rationale
├── data-model.md                  # State types, node contracts, routing
├── quickstart.md                  # Dev flow, test commands, constraints
├── contracts/
│   └── domain_kit_graph.py.contract.md
└── tasks.md                       # Phase 2 output (speckit.tasks — not yet)
```

### Source Code

```text
src/agents/
├── domain_kit_graph.py    # NEW — DomainKitGraph, ScaffoldGraph, validator, run_*_step()
├── domain_kit_prompts.py  # NEW — all domain-agnostic prompts
├── graph.py               # UNCHANGED
└── prompts.py             # UNCHANGED

src/ui/
├── kit_generator.py       # REPLACE — rewire to run_kit_step()
├── block_scaffolder.py    # REPLACE — rewire to run_scaffold_step()
└── domain_kits.py         # EXTEND — rewire tabs, extend Preview validator

app.py                     # FIX — _mode_override sentinel for post-commit navigation

tests/
├── unit/
│   ├── test_domain_kit_validator.py   # NEW — deterministic validator checks
│   └── test_domain_kit_graph.py       # NEW — node functions with mocked LLM
└── integration/
    └── test_domain_kit_generation.py  # NEW — fixture CSV → pack generation
```

**Structure Decision**: Single-project layout. All new code goes into existing `src/agents/` and `src/ui/` packages; no new package directories.

## Complexity Tracking

No constitution violations. No complexity justification required.
