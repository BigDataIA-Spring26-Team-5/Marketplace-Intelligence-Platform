# Data Model: Agentic Domain Kit Builder

## State Schemas

### `DomainKitState` (TypedDict, total=False)
LangGraph state for the domain pack generation agent (`DomainKitGraph`).

| Field | Type | Set by node | Description |
|---|---|---|---|
| `domain_name` | `str` | caller | Target domain slug (e.g. `"pharma"`) |
| `description` | `str` | caller | Plain-language domain description |
| `csv_content` | `str` | caller | Raw CSV text (sample file) |
| `csv_headers` | `list[str]` | `analyze_csv` | Parsed column names from CSV header row |
| `csv_sample_table` | `str` | `analyze_csv` | Markdown table of first 5 rows |
| `enrichment_rules_yaml` | `str` | `generate_enrichment_rules` / `revise_enrichment_rules` | YAML text of enrichment_rules.yaml |
| `enrichment_fields` | `list[str]` | `validate_enrichment_rules` | Field names extracted from `enrichment_rules_yaml` for downstream context |
| `validation_errors` | `list[str]` | `validate_enrichment_rules` | Structural error messages (empty = valid) |
| `retry_count` | `int` | `validate_enrichment_rules` | Number of auto-retries attempted (0–2) |
| `prompt_examples_yaml` | `str` | `generate_prompt_examples` | YAML text of prompt_examples.yaml |
| `block_sequence_yaml` | `str` | `generate_block_sequence` | YAML text of block_sequence.yaml |
| `pending_review` | `bool` | `hitl_review` | True = waiting for user approval |
| `user_edits` | `dict[str, str]` | Streamlit UI | Map of filename → user-edited YAML text |
| `committed` | `bool` | `commit_to_disk` | True after files written to `domain_packs/<domain>/` |
| `error` | `str` | any node | Last error message if a node fails |

---

### `ScaffoldState` (TypedDict, total=False)
LangGraph state for the block scaffold agent (`ScaffoldGraph`).

| Field | Type | Set by node | Description |
|---|---|---|---|
| `domain_name` | `str` | caller | Target domain slug |
| `extraction_description` | `str` | caller | Plain-language description of extraction logic |
| `scaffold_source` | `str` | `generate_scaffold` / `fix_scaffold` | Python source text of the Block subclass |
| `syntax_valid` | `bool` | `validate_syntax` | `ast.parse()` succeeded |
| `syntax_error` | `str` | `validate_syntax` | Error message from `ast.parse()` if invalid |
| `retry_count` | `int` | `validate_syntax` | Number of auto-fix attempts (0–2) |
| `pending_review` | `bool` | `hitl_review` | True = waiting for user approval |
| `user_source` | `str` | Streamlit UI | User-edited Python source |
| `committed` | `bool` | `save_to_custom_blocks` | True after file written |
| `error` | `str` | any node | Last error message |

---

## `ValidationIssue` (dataclass or TypedDict)

```python
class ValidationIssue(TypedDict):
    level: str   # "error" | "warning"
    check: str   # short identifier e.g. "missing_generated_sentinel"
    message: str # human-readable description
```

Produced by `validate_enrichment_rules_yaml()` and the Preview validator. Never persisted — computed on demand.

---

## Graph Node Contracts

### DomainKitGraph nodes

| Node | Reads from state | Writes to state |
|---|---|---|
| `analyze_csv` | `csv_content` | `csv_headers`, `csv_sample_table` |
| `generate_enrichment_rules` | `domain_name`, `description`, `csv_headers`, `csv_sample_table`, `validation_errors` (on retry) | `enrichment_rules_yaml` |
| `validate_enrichment_rules` | `enrichment_rules_yaml`, `csv_headers` | `enrichment_fields`, `validation_errors`, `retry_count` |
| `revise_enrichment_rules` | `enrichment_rules_yaml`, `validation_errors`, `domain_name`, `description` | `enrichment_rules_yaml` |
| `generate_prompt_examples` | `csv_headers`, `enrichment_fields`, `domain_name`, `description` | `prompt_examples_yaml` |
| `generate_block_sequence` | `enrichment_fields`, `domain_name`, `description` | `block_sequence_yaml` |
| `hitl_review` | all yaml fields | `pending_review=True` |
| `commit_to_disk` | `domain_name`, `user_edits` (or yaml fields if no edits) | `committed=True` |

### ScaffoldGraph nodes

| Node | Reads from state | Writes to state |
|---|---|---|
| `generate_scaffold` | `domain_name`, `extraction_description`, `syntax_error` (on retry) | `scaffold_source` |
| `validate_syntax` | `scaffold_source` | `syntax_valid`, `syntax_error`, `retry_count` |
| `fix_scaffold` | `scaffold_source`, `syntax_error` | `scaffold_source` |
| `hitl_review` | `scaffold_source`, `syntax_valid` | `pending_review=True` |
| `save_to_custom_blocks` | `domain_name`, `user_source` (or `scaffold_source`) | `committed=True` |

---

## Filesystem Artifacts

All written only after explicit user approval (FR-3):

```
domain_packs/<domain>/
├── enrichment_rules.yaml     # from DomainKitState.user_edits or enrichment_rules_yaml
├── prompt_examples.yaml      # from DomainKitState.user_edits or prompt_examples_yaml
├── block_sequence.yaml       # from DomainKitState.user_edits or block_sequence_yaml
└── custom_blocks/
    └── <block_name>.py       # from ScaffoldState.user_source or scaffold_source
```

The `custom_blocks/` directory is created by `BlockRegistry` auto-discovery; the scaffold commit just writes the `.py` file.

---

## Conditional Routing Logic

### DomainKitGraph

```
analyze_csv
  → generate_enrichment_rules
    → validate_enrichment_rules
      → [if errors and retry_count < 2] revise_enrichment_rules → validate_enrichment_rules
      → [if no errors OR retry_count >= 2] generate_prompt_examples
        → generate_block_sequence
          → hitl_review
            → commit_to_disk
```

### ScaffoldGraph

```
generate_scaffold
  → validate_syntax
    → [if invalid and retry_count < 2] fix_scaffold → validate_syntax
    → [if valid OR retry_count >= 2] hitl_review
      → save_to_custom_blocks
```
