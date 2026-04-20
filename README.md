# Schema-Driven ETL Pipeline

This project implements a schema-driven ETL pipeline for heterogeneous food
product datasets. The current architecture is a declarative, YAML-first flow:
Agent 1 analyzes schema gaps, Agent 2 critiques the proposed operations, Agent 3
plans block order, and execution runs through registered blocks plus a generated
`DynamicMappingBlock`.

## Current Architecture

The repository does not use runtime code generation for transformations.
Dataset-specific transformations are persisted as YAML under
`src/blocks/generated/<domain>/` and replayed on later runs.

Pipeline graph order:
1. `load_source`
2. `analyze_schema`
3. `critique_schema`
4. `check_registry`
5. `plan_sequence`
6. `run_pipeline`
7. `save_output`

Agent responsibilities:
- **Agent 1**: schema analysis against `config/unified_schema.json`
- **Agent 2**: reasoning-model critique of Agent 1 operations
- **Agent 3**: block-sequence planning from the available registry pool

## Core Behaviors

- **Schema-first planning**: every dataset is compared with the unified schema
  before execution.
- **Declarative transforms**: schema operations execute through YAML consumed by
  `src/blocks/dynamic_mapping.py`.
- **Human approval gates**: the Streamlit UI exposes mapping review and
  quarantine review.
- **Cascading enrichment**: deterministic extraction runs first, then KNN corpus
  lookup, then RAG-assisted LLM categorization for `primary_category`.
- **Safety boundaries**: `allergens`, `dietary_tags`, and `is_organic` are
  deterministic-only enrichment fields and are not sent to probabilistic tiers.
- **DQ enforcement**: the pipeline computes `dq_score_pre` and `dq_score_post`
  and quarantines rows that still fail required-field validation.

## Project Layout

```text
src/
├── agents/
├── blocks/
├── enrichment/
├── models/
├── pipeline/
├── registry/
├── schema/
└── ui/

tests/
```

Generated mapping artifacts live under `src/blocks/generated/`.

## Development

Repository guidance:
- Python 3.11
- `pandas` for DataFrame work
- LiteLLM-backed model access
- LangGraph for orchestration
- Streamlit for interactive review

Validation command:

```bash
cd src && pytest && ruff check .
```

## Notes For Contributors

- Keep architecture docs aligned with the current three-agent, YAML-only flow.
- Do not reintroduce runtime-generated Python transforms without a constitution
  amendment.
- If a change affects schema handling, enrichment safety, DQ scoring, or
  quarantine behavior, update the spec/plan/tasks artifacts as part of the same
  change.
