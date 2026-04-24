"""Generate domain pack YAML files from a sample CSV using the orchestrator LLM."""

from __future__ import annotations

import csv
import io
import logging
from pathlib import Path

import yaml

from src.models.llm import call_llm_json, get_orchestrator_llm

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"


def _load_nutrition_example() -> str:
    """Return nutrition enrichment_rules.yaml content as a few-shot example."""
    p = DOMAIN_PACKS_DIR / "nutrition" / "enrichment_rules.yaml"
    try:
        return p.read_text()
    except Exception:
        return ""


def _csv_to_markdown_table(csv_content: str, max_rows: int = 5) -> tuple[list[str], str]:
    """Parse CSV and return (headers, markdown_table_string)."""
    reader = csv.reader(io.StringIO(csv_content))
    rows = list(reader)
    if not rows:
        return [], ""
    headers = rows[0]
    data_rows = rows[1:max_rows + 1]
    sep = " | ".join(["---"] * len(headers))
    header_line = " | ".join(headers)
    lines = [f"| {header_line} |", f"| {sep} |"]
    for row in data_rows:
        # pad/truncate to header length
        padded = (row + [""] * len(headers))[: len(headers)]
        lines.append("| " + " | ".join(str(v)[:80] for v in padded) + " |")
    return headers, "\n".join(lines)


def generate_domain_kit(
    domain_name: str,
    description: str,
    csv_content: str,
) -> dict[str, str]:
    """Generate enrichment_rules.yaml, prompt_examples.yaml, block_sequence.yaml for a new domain.

    Returns dict mapping filename → YAML string.
    On LLM or parse failure, the affected file's value is '{"error": "<message>"}'.
    """
    headers, sample_table = _csv_to_markdown_table(csv_content)
    nutrition_example = _load_nutrition_example()

    prompt = f"""You are a domain pack generator for an ETL pipeline.

A domain pack consists of three YAML files that configure the pipeline for a new data domain.
Generate all three files for the domain described below.

## Domain
Name: {domain_name}
Description: {description}

## Sample CSV Columns and Data
Headers: {headers}

{sample_table}

## Few-Shot Example: nutrition enrichment_rules.yaml
```yaml
{nutrition_example}
```

## Block Sequence Template (use this as the base for block_sequence.yaml)
```yaml
domain: {domain_name}
sequence:
  - dq_score_pre
  - __generated__
  - strip_whitespace
  - lowercase_brand
  - <add domain-specific extraction blocks here, e.g. {domain_name}__extract_<field>>
  - llm_enrich
  - dq_score_post
```

## Generation Rules

### enrichment_rules.yaml
- Must have top-level `domain: {domain_name}` key
- Must have `fields:` list with at least 1 deterministic field (strategy: deterministic) and 1 LLM field (strategy: llm)
- Deterministic fields are safety fields — extraction only, never inferred by LLM
- LLM field must have `classification_classes:` list with domain-appropriate categories
- Include `text_columns:` list (top-level) naming the source text columns for extraction
- Pattern regex must be valid Python regex syntax

### prompt_examples.yaml
- Must have top-level `column_mapping_examples:` list
- Each entry: source_col, target_col, operation (RENAME/CAST/FORMAT/DELETE)
- Provide 8-15 examples specific to the domain's column naming conventions

### block_sequence.yaml
- Must have top-level `domain: {domain_name}` key
- Must have `sequence:` list
- MUST include `__generated__` sentinel (required — replaced at runtime with mapping block)
- Must include `dq_score_pre` first and `dq_score_post` last
- Add a domain-specific custom block name for extraction (e.g. {domain_name}__extract_<primary_field>)

Return ONLY a JSON object with exactly these three keys:
{{
  "enrichment_rules": "<full YAML content as a string>",
  "prompt_examples": "<full YAML content as a string>",
  "block_sequence": "<full YAML content as a string>"
}}
"""

    result: dict[str, str] = {}

    try:
        llm_response = call_llm_json(
            model=get_orchestrator_llm(),
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        logger.error("Kit generator LLM call failed: %s", exc)
        err = str(exc)
        return {
            "enrichment_rules.yaml": f'{{"error": "{err}"}}',
            "prompt_examples.yaml": f'{{"error": "{err}"}}',
            "block_sequence.yaml": f'{{"error": "{err}"}}',
        }

    for key, filename in [
        ("enrichment_rules", "enrichment_rules.yaml"),
        ("prompt_examples", "prompt_examples.yaml"),
        ("block_sequence", "block_sequence.yaml"),
    ]:
        raw = llm_response.get(key, "") if isinstance(llm_response, dict) else ""
        if not raw:
            result[filename] = '{"error": "LLM returned empty value for this file"}'
            continue
        try:
            yaml.safe_load(raw)
            result[filename] = raw
        except yaml.YAMLError as exc:
            logger.warning("YAML parse error for %s: %s", filename, exc)
            result[filename] = f'{{"error": "YAML parse error: {exc}"}}'

    return result
