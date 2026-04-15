"""Prompt templates for Agent 1 (Orchestrator) and Agent 2 (Code Generator)."""

SCHEMA_ANALYSIS_PROMPT = """You are a schema analysis agent for a data enrichment pipeline.

You are given:
1. An incoming data source's schema (column names, types, sample values, null rates)
2. A unified output schema that all data sources must conform to

Your task: For each column in the unified schema, determine how to map it from the incoming source.

## Incoming Source Schema
{source_schema}

## Unified Output Schema
{unified_schema}

## Semantic Mapping Examples
Map source columns to unified columns based on SEMANTIC meaning, not just name:
- "product_description" → "product_name" (both describe product name)
- "item_name" → "product_name"
- "name" → "product_name"
- "recalling_firm" → "brand_owner" (firm that owns/recalls the product)
- "manufacturer" → "brand_owner"
- "brand" → "brand_name"
- "category" → "category" (same meaning)
- "product_type" → "category" (type is a category)
- "recall_initiation_date" → "published_date" (date field)
- "report_date" → "published_date" (date field)
- "code_info" → "data_source" (code/source info)
- "event_id" → "data_source" (ID as source reference)

## Instructions
For each unified schema column (excluding "computed" and "enrichment" columns), classify:
- MAP: A source column maps semantically. Provide source_column. Include in column_mapping.
- GAP: The unified column has a source equivalent but needs transformation (type change, format change). Include in gaps list with source_column set.

Return ONLY a JSON object with this exact structure:
{{
  "column_mapping": {{
    "source_col_name": "unified_col_name",
    ...
  }},
  "gaps": [
    {{
      "target_column": "unified_col_name",
      "target_type": "string",
      "source_column": "source_col_name",
      "source_type": "source_type",
      "action": "GAP",
      "sample_values": ["val1", "val2"]
    }}
  ]
}}

For enrichment/computed columns (allergens, primary_category, dietary_tags, is_organic, dq_score_*, dq_delta), skip entirely — they are handled downstream."""


FIRST_RUN_SCHEMA_PROMPT = """You are a schema analysis agent. This is the FIRST data source for this pipeline.
There is no unified schema yet — you must derive one.

## Incoming Source Schema
{source_schema}

## Domain: {domain}

## Instructions
Analyze the source columns and create a column mapping from source names to clean unified names.

Rules:
- Rename columns to clean, standardized names (e.g., "brand_owner" -> "brand_name", "description" -> "product_name")
- Drop columns that are metadata/IDs not useful for the product catalog (e.g., "fdc_id", "gtin_upc")
- Keep columns relevant to product identity: name, brand, category, ingredients, serving info
- NEVER map nutrient/nutrition measurement columns (e.g., "foodNutrients", "nutrients") to "ingredients". Nutrient arrays contain lab measurements (Protein, Fat, Vitamins), not ingredient lists. If no true ingredients text column exists, leave "ingredients" unmapped.

Return ONLY a JSON object:
{{
  "column_mapping": {{
    "source_col": "unified_col_name",
    ...
  }},
  "dropped_columns": ["col1", "col2"],
  "gaps": [
    // Leave empty on first run unless a source column requires type coercion to fit the unified name
  ]
}}"""


SEQUENCE_PLANNING_PROMPT = """You are a pipeline sequence planner for a data enrichment ETL system.

You are given a set of pipeline blocks that MUST ALL run. Your task is to determine the optimal execution order.

## Domain
{domain}

## Source Schema (column names and types)
{source_schema}

## Schema Gaps and Registry Results
{gap_summary}

## Available Blocks (all must appear exactly once in your output)
{blocks_metadata}

## Ordering Rules
- dq_score_pre MUST be first
- dq_score_post MUST be last
- Normalization blocks (strip_whitespace, lowercase_brand, remove_noise_words, strip_punctuation) must run before deduplication
- extract_allergens must run before llm_enrich
- Deduplication blocks (fuzzy_deduplicate, column_wise_merge, golden_record_select) must run after normalization
- llm_enrich must run after deduplication
- __generated__ (dynamically generated schema transformation blocks) should run after dq_score_pre but before normalization blocks
- Use stage names: "dedup_stage" expands to [fuzzy_deduplicate, column_wise_merge, golden_record_select]
- Use stage names: "enrich_stage" expands to [extract_allergens, llm_enrich]

## Stage Expansion
- dedup_stage = ["fuzzy_deduplicate", "column_wise_merge", "golden_record_select"]
- enrich_stage = ["extract_allergens", "llm_enrich"]

Return ONLY a JSON object with this exact structure:
{{
  "block_sequence": ["block_name_1", "block_name_2", ...],
  "reasoning": "One sentence explaining the key ordering decision made"
}}

Include every block from the input list exactly once. Do not add or remove any blocks.
You may use stage names (dedup_stage, enrich_stage) or expand them — either is valid."""


CODEGEN_PROMPT = """You are a code generation agent. Generate a Python Block class for schema transformation.

## Gap to fill
- Target column: {target_column}
- Target type: {target_type}
- Source column: {source_column}
- Source type: {source_type}
- Sample source values: {sample_values}
- Domain: {domain}
- Dataset name: {dataset_name}

## Block Template to Follow
```python
import pandas as pd
from src.blocks.base import Block


class {block_name}Block(Block):
    name = "{block_name}"
    domain = "{domain}"
    description = "Auto-generated: {description}"
    inputs = {input_cols}
    outputs = {output_cols}
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # TODO: Implement transformation logic based on gap type
        return df
```

## Gap Types (choose appropriate transformation):
1. TYPE_CONVERSION: Cast source column to target type (e.g., object -> string, int64 -> date string)
2. COLUMN_RENAME: Rename source column to target column name
3. COLUMN_DROP: Drop a column not in output schema
4. COLUMN_CREATE: Create new column with default/null value
5. FORMAT_TRANSFORM: Transform format (e.g., date parsing, number parsing)

## Safe NA Patterns (required — these prevent runtime TypeErrors)
- Float/int COLUMN_CREATE: `df['col'] = float('nan')` — NEVER `pd.NA` then `.astype('float64')`
- Float/int TYPE_CONVERSION: `pd.to_numeric(df['src'], errors='coerce')` — NEVER `.astype('float64')` directly
- String cast: `.astype(str)` is safe
- Boolean COLUMN_CREATE: `df['col'] = None`

## Constraints
- Block must inherit from src.blocks.base.Block
- Must implement run(self, df, config=None) -> pd.DataFrame
- Use df.copy() to avoid modifying original
- Handle None/NA values gracefully
- Do NOT use: os, sys, subprocess, open, eval, exec, __import__

## Naming Convention
Block name format: {{Action}}_{{TargetColumn}}_{{DatasetName}}
Example: CastToString_product_name_acme_data

## Return ONLY the Python Block class code, nothing else. No markdown, no explanation."""


CODEGEN_RETRY_PROMPT = """The previous Block class failed validation.

## Error
{error}

## Previous code
{previous_code}

## Original requirements
- Target column: {target_column}
- Target type: {target_type}
- Source column: {source_column}
- Source type: {source_type}
- Sample values: {sample_values}
- Domain: {domain}

## Fix the Block class. Return ONLY the corrected Python Block class code."""
