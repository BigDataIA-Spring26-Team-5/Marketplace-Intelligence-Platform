# Quickstart: Agentic Domain Kit Builder

## What changes

| New / Modified | Path | Role |
|---|---|---|
| NEW | `src/agents/domain_kit_graph.py` | Two LangGraph graphs + state types + validator |
| NEW | `src/agents/domain_kit_prompts.py` | All domain-agnostic prompts for kit + scaffold agents |
| MODIFIED | `src/ui/kit_generator.py` | Replace single-shot LLM with `run_kit_step` calls |
| MODIFIED | `src/ui/block_scaffolder.py` | Replace single-shot LLM with `run_scaffold_step` calls |
| MODIFIED | `src/ui/domain_kits.py` | Rewire Generate/Scaffold tabs; extend Preview validator |
| MODIFIED | `app.py` | Fix "Run Pipeline" post-commit navigation bug (FR-8) |

Nothing in `src/agents/graph.py`, `src/agents/prompts.py`, or any domain pack is touched.

---

## Local dev flow

```bash
# Install (no new deps — LangGraph, litellm, streamlit already present)
poetry install

# Run the UI
poetry run streamlit run app.py

# Navigate: Domain Packs → Generate Pack tab
# Select a fixture preset (e.g. pharma_sample.csv)
# Click "Generate Domain Kit" → watch step-by-step progress
# Review + edit YAML in text areas → click "Approve & Save"

# Validate an existing pack
# Navigate: Domain Packs → Preview/Validate tab
# Select domain → upload source CSV → click "Run Validation"
# (CSV upload is required — Validate button disabled without it)
```

---

## Testing

```bash
# Unit: validator function — all 5 deterministic checks
poetry run pytest tests/unit/test_domain_kit_validator.py -v

# Unit: graph node functions — mocked call_llm_json, no LLM required
poetry run pytest tests/unit/test_domain_kit_graph.py -v

# Unit: re-export surface for kit_generator.py and block_scaffolder.py
poetry run pytest tests/unit/test_kit_generator.py tests/unit/test_block_scaffolder.py -v

# Integration: full DomainKitGraph against 4 fixture CSVs — mocked HITL, no real LLM
poetry run pytest tests/integration/test_domain_kit_generation.py -v

# Run all 019 tests at once
poetry run pytest tests/unit/test_domain_kit_validator.py tests/unit/test_domain_kit_graph.py tests/integration/test_domain_kit_generation.py -v
```

### Test counts

| Suite | File | Tests | LLM required |
|---|---|---|---|
| Validator | `tests/unit/test_domain_kit_validator.py` | 12 | No |
| Graph nodes | `tests/unit/test_domain_kit_graph.py` | 27 | No (mocked) |
| Re-export adapters | `tests/unit/test_kit_generator.py`, `test_block_scaffolder.py` | 7 | No |
| Integration | `tests/integration/test_domain_kit_generation.py` | 12 | No (mocked) |

---

## Key prompt rules (enforced in domain_kit_prompts.py)

1. No hardcoded field names from any domain (no `"allergens"`, `"primary_category"`, etc.).
2. Nutrition pack YAMLs may appear as structural few-shot examples — field names inside are illustrative only.
3. Every `enrichment_rules.yaml` prompt must include: "Structured columns already present in the CSV are RENAME candidates for prompt_examples — do NOT add them as extraction fields in enrichment_rules."
4. Every `block_sequence.yaml` prompt must receive the `enrichment_fields` list and must include: "Do NOT add custom block references for fields already listed in enrichment_fields — those are handled automatically by the enrichment layer."

---

## Architecture constraints to preserve

- `commit_to_disk` is the only node that writes to `domain_packs/`. Every other node is read-only.
- Retry routing is via LangGraph conditional edges — no while-loops inside nodes.
- `validate_enrichment_rules_yaml()` is the single source of truth for structural checks — imported by both the graph node and the Preview tab.
- The `run_kit_step` / `run_scaffold_step` functions mirror the `run_step(step_name, state)` signature in `graph.py` exactly.
