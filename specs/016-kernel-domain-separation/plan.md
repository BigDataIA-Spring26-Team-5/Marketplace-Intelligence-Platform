# Implementation Plan: Kernel / Domain Separation

**Branch**: `016-kernel-domain-separation` | **Date**: 2026-04-24 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/016-kernel-domain-separation/spec.md`

## Summary

Decouple food/nutrition-specific logic from the generic ETL kernel so that any new domain can be onboarded by dropping a `domain_packs/<domain>/` directory — without editing any file under `src/`. The work has four concrete changes: (1) registry reads block sequence from `block_sequence.yaml` instead of inline Python branching; (2) runner derives null-rate columns from `schema.json` instead of a hardcoded constant; (3) Agent 1's prompts load few-shot examples from `prompt_examples.yaml` at node entry; (4) food-specific blocks and rules physically move to `domain_packs/nutrition/`. The kernel itself does not change its seven-node graph, YAML-only transform constraint, or safety field boundary.

## Technical Context

**Language/Version**: Python 3.11 (Poetry)
**Primary Dependencies**: LangGraph 0.4, pandas 2.2, PyYAML, pathlib (stdlib), importlib (stdlib)
**Storage**: `domain_packs/<domain>/` filesystem directory; `config/schemas/<domain>_schema.json` for domain schema; `src/blocks/generated/<domain>/` for YAML mapping files
**Testing**: pytest (`poetry run pytest`); existing nutrition pipeline tests serve as regression baseline
**Target Platform**: Linux server (Fedora), local dev + Docker Compose stack
**Project Type**: Library / CLI pipeline
**Performance Goals**: Registry initialization overhead < 10% vs. current baseline; pipeline throughput unchanged (10K rows/chunk default)
**Constraints**: Zero breaking changes to existing nutrition/safety/pricing runs; YAML cache keys must remain valid (schema fingerprint-based); no new Poetry dependencies for this phase
**Scale/Scope**: Three existing domains (nutrition, safety, pricing) migrated; one new `domain_packs/nutrition/` reference implementation produced

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Schema-First Gap Analysis | ✅ Pass | Domain schema path (`config/schemas/<domain>_schema.json`) unchanged; Agent 1 gap analysis unaffected |
| II. Three-Agent Pipeline | ✅ Pass | No changes to graph nodes, agent responsibilities, or sequence |
| III. Declarative YAML Execution Only | ✅ Pass | Domain packs contain YAML/JSON only; custom_blocks/ Python is allowed Block subclasses, not runtime codegen |
| IV. Human Approval Gates | ✅ Pass | No changes to Gate 1 (schema mapping review) or Gate 2 (quarantine review) |
| V. Cascading Enrichment with Safety Boundaries | ✅ Pass | Moving nutrition enrichment_rules.yaml to domain pack does not change the S1/S2/S3 dispatch logic; safety field boundary enforced by `strategy: deterministic` in config |
| VI. Self-Extending Mapping Memory | ✅ Pass | Generated YAML paths (`src/blocks/generated/<domain>/`) unchanged |
| VII. DQ and Quarantine | ✅ Pass | `dq_score_pre`/`dq_score_post` sentinel names preserved; DQ column set now derived from schema required fields rather than hardcoded |
| VIII. Production Scale | ✅ Pass | Chunked runner, batched enrichment, checkpointing all untouched |
| IX. Domain-Scoped Schemas | ✅ Pass | `config/schemas/<domain>_schema.json` is the schema contract; domain pack `schema.json` is the enrichment/block configuration contract — distinct artifacts with distinct roles |

**Post-Phase 1 re-check**: Required. Verify `_DQ_COLS` constant in runner.py (separate from `NULL_RATE_COLUMNS`) is also domain-driven or is acceptable to leave as-is given its internal-only use.

## Project Structure

### Documentation (this feature)

```text
specs/016-kernel-domain-separation/
├── plan.md              ← this file
├── research.md          ← Phase 0 output
├── data-model.md        ← Phase 1 output
├── contracts/           ← Phase 1 output
└── tasks.md             ← Phase 2 output (/speckit.tasks)
```

### Source Code Changes

```text
# New: domain pack directory (created by this feature)
domain_packs/
└── nutrition/
    ├── schema.json                  ← maps to config/schemas/nutrition_schema.json fields
    ├── enrichment_rules.yaml        ← migrated from src/enrichment/deterministic.py rules
    ├── prompt_examples.yaml         ← food few-shot examples extracted from src/agents/prompts.py
    ├── block_sequence.yaml          ← nutrition block sequence extracted from block_registry.py
    ├── dag_config.yaml              ← nutrition DAG parameters (sources, auth, prefixes)
    └── custom_blocks/
        ├── extract_allergens.py     ← moved from src/blocks/extract_allergens.py
        ├── extract_quantity_column.py ← moved from src/blocks/extract_quantity_column.py
        └── keep_quantity_in_name.py ← moved from src/blocks/keep_quantity_in_name.py

# Modified: kernel files
src/registry/block_registry.py      ← remove food imports/sequences; add domain pack YAML loader + custom_blocks discovery
src/pipeline/runner.py              ← replace NULL_RATE_COLUMNS constant with _get_null_rate_columns(domain)
src/agents/prompts.py               ← extract food few-shot examples; add load_prompt_examples() + build_schema_analysis_prompt(domain)
src/enrichment/deterministic.py     ← make generic rule executor; food rules move to nutrition domain pack

# Removed from kernel (after validation):
src/blocks/extract_allergens.py     ← moved to domain_packs/nutrition/custom_blocks/
src/blocks/extract_quantity_column.py ← moved to domain_packs/nutrition/custom_blocks/
src/blocks/keep_quantity_in_name.py ← moved to domain_packs/nutrition/custom_blocks/

# Tests
tests/unit/test_block_registry.py   ← update: nutrition sequence via domain pack, not hardcoded
tests/unit/test_runner.py           ← update: null rate columns from schema
tests/integration/                  ← existing nutrition pipeline integration tests (must pass unchanged)
```

## Complexity Tracking

No constitution violations. No complexity justification required.
