"""Domain-agnostic prompts for the Agentic Domain Kit Builder.

Invariants enforced across all prompts:
- No hardcoded field names from any specific domain (no allergens, primary_category, etc.)
- Structured CSV columns are RENAME candidates in prompt_examples, NOT extraction targets
  in enrichment_rules
- block_sequence must not reference custom blocks for fields handled by enrichment_rules
- Nutrition pack appears as a structural illustration only; field names are masked
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Structural reference shown to LLM for format guidance (domain-agnostic)
# ---------------------------------------------------------------------------

_ENRICHMENT_RULES_FORMAT = """\
domain: <domain_name>

text_columns: [<source_col_1>, <source_col_2>]   # columns to scan for extraction

fields:
  # Deterministic (safety) fields — regex extraction from text, never touched by LLM/KNN
  - name: <safety_field_name>
    strategy: deterministic
    output_type: multi          # or "single"
    patterns:
      - regex: "\\b(pattern1|pattern2)\\b"
        label: label_value
      - regex: "\\b(pattern3)\\b"
        label: label_value2

  # LLM field — probabilistic categorisation via S3 RAG (primary_category equivalent)
  - name: <category_field_name>
    strategy: llm
    output_type: single
    classification_classes:
      - Category A
      - Category B
      - Category C
      - Other
    rag_context_field: <text_column_to_use_as_context>
"""

_BLOCK_SEQUENCE_FORMAT = """\
domain: <domain_name>

sequence:
  - dq_score_pre           # ALWAYS first — pre-enrichment DQ score
  - __generated__          # REQUIRED sentinel — replaced at runtime with DynamicMappingBlock
  - strip_whitespace       # built-in cleaning block
  - lowercase_brand        # built-in cleaning block
  - <domain>__extract_<field>   # custom block for domain-specific extraction (if needed)
  - llm_enrich             # enriches columns defined in enrichment_rules.yaml
  - dq_score_post          # ALWAYS last — post-enrichment DQ score
"""

_PROMPT_EXAMPLES_FORMAT = """\
# Domain: clinical_records (EXAMPLE — replace with actual domain)
# Source CSV headers: PatientID, DischargeDate, DiagnosisText, MedicationList, FacilityCode
# Derived unified names (snake_case of the same concepts): patient_id, discharge_date, diagnosis_text, medications, facility_code

domain: clinical_records

column_mapping_examples:
  # PatientID -> canonical snake_case name for the same column
  - source_col: PatientID
    target_col: patient_id
    operation: RENAME
  # alternate name variant that means the same thing
  - source_col: MRN
    target_col: patient_id
    operation: RENAME

  # DischargeDate -> snake_case canonical name
  - source_col: DischargeDate
    target_col: discharge_date
    operation: RENAME
  # date format normalisation variant
  - source_col: discharge_date_raw
    target_col: discharge_date
    operation: FORMAT

  # Columns with no unified equivalent -> DELETE
  - source_col: internal_row_id
    target_col: null
    operation: DELETE
"""

# ---------------------------------------------------------------------------
# Prompt: generate_enrichment_rules
# ---------------------------------------------------------------------------

def build_enrichment_rules_prompt(
    domain_name: str,
    description: str,
    csv_headers: list[str],
    sample_table: str,
) -> str:
    return f"""\
You are generating an enrichment_rules.yaml file for an ETL pipeline domain pack.

## Domain
Name: {domain_name}
Description: {description}

## Source CSV Columns
Headers: {csv_headers}

Sample data:
{sample_table}

## Your Task
Generate a valid enrichment_rules.yaml for this domain.

## Rules — READ CAREFULLY

1. `text_columns` lists source CSV columns that contain free-text suitable for regex extraction.
   - Only include columns with unstructured text (descriptions, notes, narratives).
   - Do NOT include columns that are already structured identifiers, codes, or dates.

2. `fields` defines what to extract or classify:
   - Use `strategy: deterministic` for facts that can be reliably detected with regex patterns
     (e.g. safety-relevant labels, boolean flags, coded values in free text).
   - Use `strategy: llm` for one categorical classification field where LLM inference adds value.
   - Only one `strategy: llm` field per domain is typical.

3. CRITICAL — DO NOT add a field to enrichment_rules for a column that already exists as a
   structured value in the CSV (e.g. if the CSV has a `classification` column with clean values,
   do not create an extraction field for it — it belongs in prompt_examples as a RENAME).

4. Deterministic fields are safety-class fields. They are extraction-only — the LLM/KNN
   enrichment layer NEVER modifies them. Only add them if the text genuinely requires extraction.

5. Do not use field names from any other domain. Invent names appropriate for {domain_name}.

## Format Reference (structural only — do not copy field names)
```yaml
{_ENRICHMENT_RULES_FORMAT}
```

Return ONLY a JSON object with this structure:
{{"yaml": "<complete enrichment_rules.yaml content as a string>"}}

The yaml value must be valid YAML. Use \\n for newlines inside the JSON string value.
"""


# ---------------------------------------------------------------------------
# Prompt: revise_enrichment_rules (retry with validation errors)
# ---------------------------------------------------------------------------

def build_enrichment_rules_fix_prompt(
    domain_name: str,
    description: str,
    csv_headers: list[str],
    previous_yaml: str,
    validation_errors: list[str],
) -> str:
    errors_block = "\n".join(f"  - {e}" for e in validation_errors)
    return f"""\
You previously generated an enrichment_rules.yaml for domain "{domain_name}" that failed validation.

## Validation Errors Found
{errors_block}

## Previous (broken) YAML
```yaml
{previous_yaml}
```

## Domain Context
Name: {domain_name}
Description: {description}
CSV Headers: {csv_headers}

## Fix Instructions
- Correct every validation error listed above.
- Do not add new fields or change the overall structure unless required by the fix.
- Preserve valid fields from the previous YAML where possible.
- Remember: structured CSV columns are RENAME candidates, not extraction fields.

Return ONLY a JSON object:
{{"yaml": "<corrected enrichment_rules.yaml content as a string>"}}
"""


# ---------------------------------------------------------------------------
# Prompt: generate_prompt_examples
# ---------------------------------------------------------------------------

def build_prompt_examples_prompt(
    domain_name: str,
    description: str,
    csv_headers: list[str],
    enrichment_fields: list[str],
    sample_table: str,
) -> str:
    # Derive canonical target names: snake_case versions of the actual CSV headers
    canonical_targets = [h.lower().replace(" ", "_").replace("-", "_") for h in csv_headers]
    header_to_canonical = dict(zip(csv_headers, canonical_targets))
    canonical_list = "\n".join(
        f"  {src} -> {tgt}" for src, tgt in header_to_canonical.items()
    )
    return f"""\
You are generating a prompt_examples.yaml file for an ETL pipeline domain pack.

## Domain
Name: {domain_name}
Description: {description}

## Source CSV Headers (THESE ARE THE ACTUAL COLUMNS IN THIS DOMAIN'S DATA)
{csv_headers}

## Sample Data
{sample_table}

## CRITICAL — Target Column Name Rules

The target column names MUST be derived from the actual CSV headers above — NOT from any
other domain's schema. The unified column names for this domain are the snake_case versions
of the CSV headers:

{canonical_list}

DO NOT use target column names like `ingredients`, `brand_name`, `data_source`, `product_name`,
`allergens`, or any other name that does not correspond to a column in THIS domain's CSV.
Those names belong to other domains. Using them here would corrupt the schema mapping.

## Enrichment Fields (produced by enrichment layer — NOT sourced from CSV)
Do NOT use these as target_col values in RENAME mappings:
{enrichment_fields}

## Your Task
Generate column_mapping_examples that teach Agent 1 how to map raw source column name variants
to the canonical unified names defined above.

## Rules

1. For each CSV header, the canonical target_col is its snake_case version (shown in the
   mapping table above). Use that exact name as target_col.

2. Provide 1-3 example source name variants per target — covering realistic alternate spellings,
   PascalCase versions, abbreviations, or legacy column names a source system might use.
   Example: if the CSV has `discharge_date`, realistic variants are `DischargeDate`,
   `date_of_discharge`, `discharge_dt`.

3. DO NOT include identity mappings where source_col == target_col. Only include non-trivial
   variants (alternate names that differ from the canonical target).

4. Operations:
   - RENAME: source column maps directly to a unified column name
   - FORMAT: column exists but needs reformatting (dates, units)
   - CAST: type conversion needed
   - DELETE: column should be dropped entirely (no unified equivalent)

5. Add 2-3 DELETE examples for columns commonly seen in this domain that should be dropped
   (e.g. internal IDs, row timestamps, audit columns).

## Format Reference (read the comments — the pattern is variant→canonical, not identity)
```yaml
{_PROMPT_EXAMPLES_FORMAT}
```

Return ONLY a JSON object:
{{"yaml": "<complete prompt_examples.yaml content as a string>"}}
"""


# ---------------------------------------------------------------------------
# Prompt: generate_block_sequence
# ---------------------------------------------------------------------------

def build_block_sequence_prompt(
    domain_name: str,
    description: str,
    enrichment_fields: list[str],
) -> str:
    return f"""\
You are generating a block_sequence.yaml file for an ETL pipeline domain pack.

## Domain
Name: {domain_name}
Description: {description}

## Enrichment Fields (already handled by the enrichment layer — llm_enrich block)
These fields are produced automatically by the enrichment layer:
{enrichment_fields}

## Your Task
Generate a block_sequence.yaml defining the ordered execution plan for this domain.

## Rules — READ CAREFULLY

1. `dq_score_pre` MUST be the first block. `dq_score_post` MUST be the last block.

2. `__generated__` MUST appear immediately after `dq_score_pre`. It is a required sentinel
   that gets replaced at runtime with the DynamicMappingBlock for schema transformation.

3. Standard cleaning blocks (place after __generated__, before custom blocks):
   - `strip_whitespace` — trim whitespace from string columns
   - `lowercase_brand` — normalise brand name capitalisation
   - `remove_noise_words` — strip filler words
   - `strip_punctuation` — clean punctuation
   (Include only the ones relevant to this domain.)

4. `llm_enrich` is the block that executes all enrichment_rules.yaml fields automatically.
   Include it before dedup blocks if dedup is needed, else before dq_score_post.

5. CRITICAL — DO NOT add custom block references (e.g. {domain_name}__extract_<field>) for
   any field already listed in the Enrichment Fields above. Those are handled by llm_enrich.
   Only add a custom block if the domain requires extraction logic that cannot be expressed
   as regex patterns in enrichment_rules.yaml.

6. If you add a custom block, use the naming convention: {domain_name}__<block_name>

7. Keep the sequence minimal. Avoid phantom custom blocks.

## Format Reference
```yaml
{_BLOCK_SEQUENCE_FORMAT}
```

Return ONLY a JSON object:
{{"yaml": "<complete block_sequence.yaml content as a string>"}}
"""


# ---------------------------------------------------------------------------
# Prompt: generate_scaffold (Python Block subclass)
# ---------------------------------------------------------------------------

_BLOCK_BASE_CONTRACT = """\
class Block:
    name: str = "unnamed"          # snake_case, follow convention: <domain>__<block_name>
    domain: str = ""               # domain this block belongs to
    description: str = ""          # one-line description
    inputs: list[str] = []         # column names this block reads
    outputs: list[str] = []        # column names this block writes

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        \"\"\"Transform df and return the modified copy.\"\"\"
        raise NotImplementedError

    def audit_entry(self) -> dict:
        \"\"\"Return {block, rows_in, rows_out, ...} — called after run().\"\"\"
        return {"block": self.name, "rows_in": 0, "rows_out": 0}
"""

_BLOCK_SCAFFOLD_EXAMPLE = """\
import logging
import re
import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)


class ExtractCodesBlock(Block):
    name = "example__extract_codes"
    domain = "example"
    description = "Extract structured codes from free-text field"
    inputs = ["description_column"]
    outputs = ["extracted_codes"]

    _PATTERN = re.compile(r"\\b[A-Z]\\d{2,3}(\\.\\d{1,2})?\\b", re.I)

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        if "description_column" not in df.columns:
            df["extracted_codes"] = pd.NA
            return df
        df["extracted_codes"] = df["description_column"].apply(self._extract)
        return df

    def _extract(self, text: object) -> str | None:
        if not isinstance(text, str) or not text.strip():
            return None
        matches = self._PATTERN.findall(text)
        return ", ".join(m[0] if isinstance(m, tuple) else m for m in matches) or None
"""


def build_scaffold_generate_prompt(
    domain_name: str,
    extraction_description: str,
) -> str:
    return f"""\
You are generating a Python Block subclass for an ETL pipeline.

## Domain
{domain_name}

## What to Extract
{extraction_description}

## Block Base Class Contract
```python
{_BLOCK_BASE_CONTRACT}
```

## Example Block (structural reference — do not copy the domain or field names)
```python
{_BLOCK_SCAFFOLD_EXAMPLE}
```

## Requirements
1. Class name: use PascalCase, e.g. Extract{domain_name.title().replace("_", "")}Block
2. `name` attribute: follow convention `{domain_name}__<block_name>` (snake_case)
3. `domain` attribute: set to "{domain_name}"
4. `inputs` and `outputs` must list the actual DataFrame column names used
5. `run()` must return `df.copy()` with the extracted column added
6. Handle missing or null input columns gracefully (return pd.NA for the output column)
7. Add a logger with `logger = logging.getLogger(__name__)`
8. No hardcoded field names from other domains

Return ONLY a JSON object:
{{"source": "<complete Python source as a string>"}}

The source value must be valid Python. Use \\n for newlines inside the JSON string value.
"""


# ---------------------------------------------------------------------------
# Prompt: fix_scaffold (syntax error retry)
# ---------------------------------------------------------------------------

def build_scaffold_fix_prompt(
    domain_name: str,
    extraction_description: str,
    broken_source: str,
    syntax_error: str,
) -> str:
    return f"""\
You previously generated a Python Block subclass for domain "{domain_name}" that has a syntax error.

## Syntax Error
{syntax_error}

## Broken Source
```python
{broken_source}
```

## Original Task
{extraction_description}

## Fix Instructions
- Correct the syntax error.
- Do not change the logic unless required to fix the syntax.
- Preserve all imports, class structure, and method signatures.

Return ONLY a JSON object:
{{"source": "<corrected Python source as a string>"}}
"""
