# Data Model: Domain Kit UI Builder

**Phase**: 1 — Design  
**Branch**: `018-domain-kit-ui-builder`  
**Date**: 2026-04-24

---

## Entities

### DomainKit (runtime / display only — not persisted)

Derived at panel load by scanning `domain_packs/` on the VM filesystem.

```
DomainKit
├── domain_name: str                   # directory name under domain_packs/
├── type: Literal["built-in", "user-created"]
│        # "built-in" ↔ git ls-files domain_packs/<domain>/ is non-empty
├── file_manifest: list[str]           # files present under domain_packs/<domain>/
├── created_at: datetime               # filesystem mtime of domain_packs/<domain>/
├── enrichment_fields: list[str]       # EnrichmentRulesLoader.enrichment_column_names
└── safety_fields: list[str]           # EnrichmentRulesLoader.safety_field_names()
```

**No database table.** Derived fresh on each panel render from filesystem + EnrichmentRulesLoader.

---

### KitGenerationSession (Streamlit session state — ephemeral)

Lives in `st.session_state["kit_gen"]`. Cleared on page reload.

```
KitGenerationSession
├── domain_name: str
├── description: str
├── csv_filename: str
├── csv_content: str                   # raw CSV bytes decoded as UTF-8
├── generated_files: dict[str, str]    # filename → YAML string
│     keys: "enrichment_rules.yaml", "prompt_examples.yaml", "block_sequence.yaml"
├── validation_errors: list[str]       # per-file parse/structural errors
└── committed: bool                    # True after files written to domain_packs/
```

**Validation state transitions**:
```
EMPTY → GENERATING → GENERATED → VALIDATING → VALID / INVALID → COMMITTED
```
- `VALID` → Commit button enabled
- `INVALID` → Commit button disabled, errors displayed inline
- `COMMITTED` → Read-only view, "Run Pipeline" deep-link

---

### BlockScaffold (Streamlit session state — ephemeral)

Lives in `st.session_state["scaffold"]`.

```
BlockScaffold
├── domain_name: str
├── block_name: str                    # snake_case, derived from extraction description
├── extraction_description: str
├── file_content: str                  # generated Python source code
├── syntax_valid: bool                 # ast.parse() succeeded
└── security_acknowledged: bool        # user checked the security notice checkbox
```

Download enabled only when `syntax_valid AND security_acknowledged`.

---

### KitAuditEntry (append-only log — persisted to filesystem)

Written to `output/kit_audit.jsonl` (one JSON object per line). Survives app restarts.

```
KitAuditEntry
├── domain_name: str
├── action: Literal["generate", "commit", "delete"]
├── timestamp: str                     # ISO-8601 UTC
├── outcome: Literal["success", "failure", "pending"]
└── detail: str                        # error message or file list on success
```

No reads by the pipeline. Read only by the Manage Kits panel (last 50 entries displayed).

---

## State Transitions

### Kit Lifecycle

```
[no kit]
    │ user fills wizard + clicks Generate
    ▼
[generated — under review]
    │ user edits YAML in text_areas
    │ clicks Validate (or auto-validate on edit)
    ▼
[validated]
    │ user clicks Commit
    ▼
[committed — domain_packs/<domain>/ written]
    │ user runs pipeline (deep-link to Pipeline mode)
    ▼
[pipeline output in output/<domain>/]

[committed]
    │ user deletes from Manage Kits panel
    ▼
[no kit]
```

### Block Scaffold Lifecycle

```
[no scaffold]
    │ user types description + clicks Generate Block
    ▼
[generated — displayed in code viewer]
    │ user checks security acknowledgment checkbox
    ▼
[acknowledged — download button enabled]
    │ user downloads .py, places in custom_blocks/ manually
    ▼
[effective on next Streamlit session load]
```

---

## `enrichment_rules.yaml` Schema (extended for new domains)

```yaml
# Required top-level
domain: <str>          # must be valid Python identifier (lowercase + underscores)

# Optional top-level (new — backward-compatible)
text_columns:          # list[str]; fallback: ["product_name", "ingredients", "category"]
  - <column_name>

# Required
fields:                # list[FieldRule]
  - name: <str>        # output column name
    strategy: deterministic | llm
    output_type: single | multi | boolean

    # For deterministic fields (S1 only — safety fields)
    patterns:          # list[PatternRule]
      - regex: <str>   # Python re pattern (case-insensitive)
        label: <str>   # output value / tag

    # For llm fields only (S3 classification)
    classification_classes:   # list[str]
      - <category_name>
    rag_context_field: <str>  # column fed to RAG prompt
```

**Validation rules enforced at commit time**:
- `domain` must match `[a-z][a-z0-9_]*`
- At least one field must be declared
- Fields with `strategy: llm` must have `classification_classes` non-empty
- Fields with `strategy: llm` MUST NOT be listed in `safety_field_names()` — they are different concepts: `deterministic` = safety, `llm` = probabilistic

---

## `block_sequence.yaml` Generated Template

```yaml
sequence:
  - dq_score_pre
  - __generated__
  - cleaning
  - dedup_stage
  - llm_enrich
  - dq_score_post

silver_sequence:
  - __generated__
  - cleaning
```

`__generated__` sentinel is always preserved. Custom blocks referenced in `sequence` are left as comments in generated output, prompting the user to add their scaffold file name.

---

## `prompt_examples.yaml` Generated Structure

```yaml
examples:
  - description: "Rename source column to unified name"
    primitive: RENAME
    source_column: <src_col>
    target_column: <unified_col>
  - description: "Cast to correct type"
    primitive: CAST
    source_column: <col>
    target_type: string | float | int | date
  - description: "Format / normalize value"
    primitive: FORMAT
    source_column: <col>
    action: value_map | parse_date | regex_replace
```

Minimum 3 examples generated from sample CSV headers. Enrichment and computed columns excluded (`dq_score_*`, `primary_category`, safety field names).

---

## Filesystem Layout (per committed kit)

```
domain_packs/<domain_name>/
├── enrichment_rules.yaml       # required — generated or user-provided
├── block_sequence.yaml         # required — generated from template
├── prompt_examples.yaml        # generated — can be empty examples list
└── custom_blocks/              # optional — user places scaffold .py files here
    └── extract_<concept>.py    # user-downloaded scaffold
```

`config/schemas/<domain_name>_schema.json` is NOT written at commit time. It is created automatically on the first pipeline run via `derive_unified_schema_from_source()` — existing behavior unchanged.

---

## EnrichmentRulesLoader Extended Interface

```python
class EnrichmentRulesLoader:
    # Existing (unchanged)
    all_fields: list[FieldRule]
    deterministic_fields: list[FieldRule]
    llm_fields: list[FieldRule]
    s1_fields: list[FieldRule]
    def safety_field_names(self) -> list[str]: ...

    # New properties (added by this feature)
    @property
    def enrichment_column_names(self) -> list[str]:
        """All field names in declaration order. Replaces ENRICHMENT_COLUMNS."""
        return [f.name for f in self.all_fields]

    @property
    def llm_categories_string(self) -> str:
        """Comma-separated classification classes from first LLM field.
        Replaces CATEGORIES in llm_tier.py."""
        for f in self.llm_fields:
            if f.classification_classes:
                return ", ".join(f.classification_classes)
        return ""

    @property
    def text_columns(self) -> list[str]:
        """Text columns for deterministic pattern matching.
        Reads from YAML text_columns key; fallback to food defaults."""
        return self._raw.get("text_columns", ["product_name", "ingredients", "category"])

    @property
    def llm_rag_context_field(self) -> str | None:
        """RAG context column name from first LLM field."""
        for f in self.llm_fields:
            if f.rag_context_field:
                return f.rag_context_field
        return None
```
