# Data Model: Kernel / Domain Separation

**Date**: 2026-04-24
**Feature**: 016-kernel-domain-separation

---

## Entities

### DomainPack

A self-contained directory that configures the kernel for one domain vertical.

```
domain_packs/<domain>/
├── schema.json            ← wizard config (NOT the runtime schema contract)
├── enrichment_rules.yaml  ← deterministic + LLM field rules
├── prompt_examples.yaml   ← Agent 1 few-shot examples
├── block_sequence.yaml    ← ordered block list for this domain
├── dag_config.yaml        ← source connection parameters
└── custom_blocks/         ← optional Block subclasses
    └── <block_name>.py
```

**Invariants**:
- `block_sequence.yaml` MUST be present for any domain that uses enrichment. Absent → `FALLBACK_SEQUENCE`.
- `prompt_examples.yaml` MUST be present for Agent 1 to produce accurate mappings. Absent → generic examples.
- `custom_blocks/` MAY be empty. Empty directory is valid.
- `schema.json` in the domain pack is an operator-facing config artifact. The runtime schema contract is `config/schemas/<domain>_schema.json`.

---

### block_sequence.yaml

```yaml
domain: <string>          # must match the domain slug
sequence:
  - dq_score_pre          # kernel block
  - __generated__         # sentinel: replaced by DynamicMappingBlock at runtime
  - strip_whitespace      # kernel block
  - ...
  - dedup_stage           # composite: expands to fuzzy_deduplicate, column_wise_merge, golden_record_select
  - enrich_stage          # composite: expands to domain custom_blocks + llm_enrich
  - dq_score_post         # kernel block
```

**Validation rules**:
- Each name must resolve to a kernel block, a stage, the `__generated__` sentinel, or a `custom_blocks/` block.
- Unresolvable name at startup → `BlockNotFoundError` (logged, init aborted).
- `__generated__` MUST appear exactly once.
- `dq_score_pre` MUST precede `__generated__`.
- `dq_score_post` MUST be last.

---

### enrichment_rules.yaml

```yaml
domain: <string>
fields:
  - name: <column_name>
    strategy: deterministic | llm
    # if strategy == deterministic:
    patterns:
      - regex: <pattern>
        label: <string>
      - keywords: [<word>, ...]
        label: <string>
    # if strategy == llm:
    classification_classes: [<class>, ...]
    rag_context_field: <mapped_column_name>
```

**Invariants**:
- `strategy: deterministic` fields are S1-only. The `EnrichmentRulesLoader` MUST NOT place them in the S2/S3 dispatch path.
- Safety fields (`allergens`, `dietary_tags`, `is_organic`) MUST use `strategy: deterministic`. This is enforced by `LLMEnrichBlock` post-run assertion — same assertion that exists today.

---

### prompt_examples.yaml

```yaml
domain: <string>
column_mapping_examples:
  - source_col: <string>    # raw source column name or pattern
    target_col: <string>    # unified schema column name
    operation: RENAME | CAST | FORMAT | DELETE | ADD | SPLIT | UNIFY | DERIVE
    cast_to: <type>         # only when operation == CAST
```

**Used by**: `build_schema_analysis_prompt(domain)` in `src/agents/prompts.py`. Injected into Agent 1's few-shot context block. Not used by Agent 2 or Agent 3.

---

### EnrichmentRulesLoader (new module: `src/enrichment/rules_loader.py`)

Loads `domain_packs/<domain>/enrichment_rules.yaml` and returns structured rule objects compatible with `deterministic_enrich()`.

**Fields**:
- `domain: str`
- `deterministic_fields: list[FieldRule]` — fields with `strategy: deterministic`
- `llm_fields: list[FieldRule]` — fields with `strategy: llm`

**FieldRule**:
- `name: str` — target column name
- `patterns: list[PatternRule]` — compiled `re.Pattern` objects + labels (for deterministic)
- `classification_classes: list[str]` — class labels (for LLM)
- `rag_context_field: str | None` — source column for LLM context

---

### PipelineRunner (modified)

Added field: `domain: str`

**Changed methods**:

| Method | Before | After |
|--------|--------|-------|
| `__init__` | `(self, block_registry)` | `(self, block_registry, domain: str)` |
| `_get_null_rate_columns` | does not exist; `NULL_RATE_COLUMNS` constant used inline | new function; calls `get_domain_schema(domain)` and returns `required` columns |
| `_compute_block_dq` | uses hardcoded `_DQ_COLS` | calls `_get_null_rate_columns()` — same derived list |

---

### BlockRegistry (modified)

**Changed methods**:

| Method | Before | After |
|--------|--------|-------|
| `__init__` | imports food blocks at module level | food block imports removed; domain custom_blocks loaded via `_discover_domain_custom_blocks()` |
| `get_default_sequence` | inline `if domain == "nutrition"` / `elif domain == "pricing"` | reads `domain_packs/<domain>/block_sequence.yaml`; falls back to `FALLBACK_SEQUENCE` |
| `get_silver_sequence` | inline domain branching | same pattern as `get_default_sequence` |
| `get_gold_sequence` | inline domain branching | same pattern |
| `_discover_domain_custom_blocks` | does not exist | new; scans `domain_packs/*/custom_blocks/*.py` using `importlib.util` |

**FALLBACK_SEQUENCE** (kernel-only, no enrichment):
```python
["dq_score_pre", "__generated__", "strip_whitespace", "remove_noise_words", "dq_score_post"]
```

---

## State Transitions

None. This feature does not change `PipelineState` fields, graph node count, or node order.

## Validation Rules

1. **At registry init**: every name in `block_sequence.yaml` must resolve → `BlockNotFoundError` if not.
2. **At prompt load**: if `prompt_examples.yaml` absent → use generic examples; log warning at INFO level.
3. **At run time**: if `get_domain_schema(domain)` raises `FileNotFoundError` → propagate; pipeline cannot run without a schema contract.
4. **Post-enrichment assertion** (existing): if any S3-resolved row has a safety field value different from post-S1 state → log warning. Unchanged by this feature.
