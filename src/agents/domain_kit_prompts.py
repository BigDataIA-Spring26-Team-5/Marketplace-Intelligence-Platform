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
domain: <domain_name>

column_mapping_examples:
  - source_col: <raw_column_name>
    target_col: <unified_column_name>
    operation: RENAME
  - source_col: <other_raw_name>
    target_col: <same_unified_name>
    operation: RENAME
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
    return f"""\
You are generating a prompt_examples.yaml file for an ETL pipeline domain pack.

## Domain
Name: {domain_name}
Description: {description}

## Source CSV Headers
{csv_headers}

## Sample Data
{sample_table}

## Enrichment Fields (handled automatically — DO NOT map these as RENAME targets from CSV)
These field names are produced by the enrichment layer, not sourced from the CSV:
{enrichment_fields}

## Your Task
Generate column_mapping_examples for this domain. These examples teach Agent 1 how to map
raw CSV column names to the unified schema column names.

## Rules

1. For each CSV header, provide 1-3 mapping examples showing realistic source name variants
   and the correct unified target name + operation.

2. Operations:
   - RENAME: column exists in CSV and maps directly to a unified column name
   - FORMAT: column exists but needs reformatting (dates, units, etc.)
   - CAST: type conversion needed (string → numeric, etc.)
   - DELETE: column should be dropped (no unified equivalent)

3. CRITICAL — DO NOT create RENAME mappings that target any of the enrichment_fields listed
   above. Those are produced by the enrichment layer, not by column mapping.

4. Structured CSV columns (codes, identifiers, flags) should appear as RENAME entries here —
   NOT as extraction targets in enrichment_rules.yaml.

5. Provide 8–15 realistic examples that cover the likely naming variations for this domain.

## Format Reference
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
