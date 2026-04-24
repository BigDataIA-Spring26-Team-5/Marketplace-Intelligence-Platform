# Domain Pack Contract

**Version**: 1.0.0
**Feature**: 016-kernel-domain-separation

This document defines the interface contract between the kernel and a domain pack. Any directory placed at `domain_packs/<domain>/` and conforming to this contract will be recognized and loaded by the kernel at startup.

---

## Directory Layout

```
domain_packs/<domain>/
├── block_sequence.yaml    REQUIRED for non-trivial domains
├── enrichment_rules.yaml  REQUIRED if any column uses strategy: llm or deterministic
├── prompt_examples.yaml   REQUIRED for accurate Agent 1 column mapping
├── dag_config.yaml        OPTIONAL (DAG factory, Phase 3)
├── schema.json            OPTIONAL (wizard artifact, not read by kernel at runtime)
└── custom_blocks/         OPTIONAL
    └── *.py               Block subclasses following the Block ABC contract
```

---

## block_sequence.yaml Schema

```yaml
# Required fields
domain: string              # Must match the directory name exactly
sequence: list[string]      # Ordered block names

# Valid sequence entries:
# - Any key in BlockRegistry.blocks (kernel blocks)
# - "__generated__"  (sentinel, exactly once)
# - "dedup_stage"    (composite: fuzzy_deduplicate, column_wise_merge, golden_record_select)
# - "enrich_stage"   (composite: expands to domain custom_blocks enrichment + llm_enrich)
# - "<domain>__<name>"  (custom block from custom_blocks/)

# Ordering invariants (enforced at init):
# 1. dq_score_pre must precede __generated__
# 2. __generated__ must appear exactly once
# 3. dq_score_post must be last
```

---

## enrichment_rules.yaml Schema

```yaml
domain: string

fields:
  - name: string                     # target column name in the domain schema
    strategy: deterministic | llm

    # When strategy == deterministic (S1 only — never passed to S2/S3):
    patterns:
      - regex: string                # Python re-compatible regex
        label: string                # value to assign when matched
      - keywords: list[string]       # matched as word boundary patterns
        label: string

    # When strategy == llm (S1 → S2 → S3 cascade):
    classification_classes: list[string]   # possible output values
    rag_context_field: string | null       # which mapped column the LLM reads
```

**Safety invariant**: Fields with `strategy: deterministic` are never passed to S2 or S3 inference paths. `EnrichmentRulesLoader` splits fields into `deterministic_fields` and `llm_fields` before dispatch.

---

## prompt_examples.yaml Schema

```yaml
domain: string

column_mapping_examples:
  - source_col: string       # raw source column name
    target_col: string       # unified schema target column name
    operation: string        # one of: RENAME | CAST | FORMAT | DELETE | ADD | SPLIT | UNIFY | DERIVE
    cast_to: string          # only required when operation == CAST
                             # values: string | integer | float | boolean | date | timestamp
```

---

## Custom Block Contract

Files in `custom_blocks/` must:

1. Define exactly one class that inherits from `src.blocks.base.Block`
2. The class `name` attribute must be `"<domain>__<descriptive_name>"`
3. Implement `transform(self, df: pd.DataFrame) -> pd.DataFrame`
4. Implement `audit_entry(self) -> dict` returning `{block, rows_in, rows_out, ...}`
5. Must not import from `src.enrichment.deterministic` food-specific constants (those are being removed)
6. Must not perform runtime code generation

---

## Kernel Guarantees to Domain Packs

- `__generated__` sentinel is always replaced by the appropriate `DynamicMappingBlock` at runtime before blocks execute.
- `dedup_stage` and `enrich_stage` composites always expand in the documented order.
- Domain custom blocks receive a pandas DataFrame with unified column names (post-column-mapping step).
- `domain` string passed to `PipelineRunner` matches the directory name under `domain_packs/`.

---

## Backward Compatibility

Existing domains without a `domain_packs/<domain>/` directory will receive `FALLBACK_SEQUENCE`:
```
["dq_score_pre", "__generated__", "strip_whitespace", "remove_noise_words", "dq_score_post"]
```
This is a safe degradation — cleaning-only, no enrichment. A warning is logged.
