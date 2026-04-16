"""Prompt templates for Agent 1 (Orchestrator) and Agent 3 (Sequence Planner)."""

SCHEMA_ANALYSIS_PROMPT = """You are a schema analysis agent for a data enrichment pipeline.

You are given:
1. An incoming data source's schema — column names, types, null rates, sample values, AND structural metadata (detected_structure, inferred_keys, inferred_value_types, parsed_sample).
2. Optional dataset-level metadata (__meta__) with numeric_columns, structured_columns, candidate_unify_groups.
3. A unified output schema that all data sources must conform to.

Your task: For each column in the unified schema, determine how to map it from the incoming source using the 8-primitive taxonomy below.

## Incoming Source Schema
{source_schema}

## Dataset Metadata
{source_meta}

## Unified Output Schema
{unified_schema}

## Semantic Mapping Examples
Map source columns to unified columns based on SEMANTIC meaning, not just name:
- "product_description" / "item_name" / "name" → "product_name"
- "recalling_firm" / "manufacturer" → "brand_owner"
- "brand" → "brand_name"
- "product_type" → "category"
- "recall_initiation_date" / "report_date" → "published_date"
- "event_id" / "code_info" → "data_source"

## Special Cases

### Data source columns
When a source column exists but its values represent INTERNAL SYSTEM CODES
(e.g., "GDSN", "LI", "API", submission system identifiers) rather than the
semantic meaning of the unified schema column, prefer:
  - primitive: ADD
  - action: set_default
  - default_value: "<dataset_name>" (e.g., "USDA", "FDA", "OpenFoodFacts")

Do NOT map these as RENAME — the source values are metadata, not the data provider.

### Value-variant columns (units, codes, categories)
When sample_values show variant spellings that should map to canonical values
(e.g., "GRM"/"g"/"gram" → "g", or "USA"/"US"/"United States" → "US"):
  - primitive: FORMAT
  - action: value_map
  - mapping: {{<variant>: <canonical>, ...}}  (generate from sample_values)

Analyze sample_values to detect variants and build the mapping dict.
Unmapped values pass through unchanged.

### Identity-bearing columns
Columns that identify the product (product_name, brand_owner, brand_name) require
normalization before deduplication. Add annotation:
  "normalize_before_dedup": true
This signals the sequence planner to apply strip_whitespace/lowercase before dedup.

## 8-Primitive Taxonomy

Use EXACTLY these primitive names. Each unified column must appear in exactly one output list.

| Primitive | When to use | Required fields |
|-----------|-------------|-----------------|
| RENAME    | Source col maps semantically, same data, no type change needed | source_column, target_column |
| CAST      | Source col maps semantically but needs type conversion (e.g. int64→string) | source_column, target_column, target_type, source_type, action (one of: type_cast) |
| FORMAT    | Source col needs value transformation (date parsing, case change, regex, value mapping, etc.) | source_column, target_column, target_type, action (one of: parse_date, to_lowercase, to_uppercase, strip_whitespace, regex_replace, regex_extract, truncate_string, pad_string, value_map, format_transform), optional: mapping dict for value_map |
| DELETE    | Source col has no place in unified schema — drop it | source_column |
| ADD       | No source data exists — create null or constant column | target_column, target_type, action (one of: set_null, set_default), optional: default_value |
| SPLIT     | 1 source col → N target cols (JSON array, delimited string) | source_column, action (one of: json_array_extract_multi, split_column, xml_extract), target_columns dict |
| UNIFY     | N source cols → 1 target col (first-non-null, concatenate, template) | sources list, target_column, action (one of: coalesce, concat_columns, string_template) |
| DERIVE    | Complex extraction or conditional logic (keyword→bool, JSON field extract, arithmetic) | source_column (or sources list), target_column, target_type, action (one of: extract_json_field, conditional_map, expression, contains_flag) |

## SPLIT action details
For json_array_extract_multi, target_columns is a dict:
  "target_columns": {{
    "col_name": {{"key": "field_in_array", "filter": {{"name": "Energy"}}, "type": "float"}},
    "col_name2": {{"key": "other_field", "join_all": true, "type": "string"}}
  }}

## Important rules
- RENAME goes in column_mapping (not operations[]).
- Everything else goes in operations[].
- If a unified column truly has no source data and cannot be derived → put it in unresolvable[] with a reason and fallback: "set_null".
- Do NOT classify as unresolvable if any source column could produce the data with a SPLIT/DERIVE action.
- NEVER map nutrient arrays (foodNutrients) to "ingredients" — nutrients are lab measurements, not ingredient lists.
- For enrichment/computed columns (allergens, primary_category, dietary_tags, is_organic, dq_score_*, dq_delta) — skip entirely.

## Return ONLY a JSON object with this exact structure:
{{
  "column_mapping": {{
    "source_col": "unified_col",
    ...
  }},
  "operations": [
    {{
      "primitive": "CAST",
      "source_column": "fdcId",
      "target_column": "data_source",
      "target_type": "string",
      "source_type": "int64",
      "action": "type_cast"
    }},
    {{
      "primitive": "ADD",
      "target_column": "brand_name",
      "target_type": "string",
      "action": "set_null"
    }},
    {{
      "primitive": "ADD",
      "target_column": "data_source",
      "target_type": "string",
      "action": "set_default",
      "default_value": "USDA"
    }},
    {{
      "primitive": "FORMAT",
      "source_column": "serving_size_unit",
      "target_column": "serving_size_unit",
      "target_type": "string",
      "action": "value_map",
      "mapping": {{"grm": "g", "gram": "g", "mlt": "ml", "mg": "mg"}}
    }},
    {{
      "primitive": "FORMAT",
      "source_column": "brand_owner",
      "target_column": "brand_owner",
      "target_type": "string",
      "action": "strip_whitespace",
      "normalize_before_dedup": true
    }},
    {{
      "primitive": "SPLIT",
      "source_column": "foodNutrients",
      "action": "json_array_extract_multi",
      "target_columns": {{
        "serving_size": {{"key": "amount", "filter": {{"name": "Energy"}}, "type": "float"}},
        "serving_size_unit": {{"key": "unitName", "filter": {{"name": "Energy"}}, "type": "string"}}
      }}
    }},
    {{
      "primitive": "DELETE",
      "source_column": "gtinUpc"
    }}
  ],
  "unresolvable": [
    {{
      "target_column": "ingredients",
      "reason": "No ingredient text exists in source — nutrient array is not a substitute",
      "fallback": "set_null"
    }}
  ]
}}\
"""


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


CRITIC_PROMPT = """You are a senior data engineer reviewing a junior engineer's schema gap analysis.

You will be given:
1. The source data profile (column by column, with structure detection and sample values)
2. The unified target schema
3. The gap analysis operations list produced by the junior engineer

Your job is to find errors, omissions, and poor classifications, then return a corrected operations list.

## Source Data Profile
{source_profile}

## Dataset Metadata
{source_meta}

## Unified Target Schema
{unified_schema}

## Junior Engineer's Operations List
{operations}

## Verification Checklist

Work through each rule below for EVERY operation. Correct any violations.

### Rule 1 — value_map completeness
For every operation with action `value_map`: inspect the source column's `sample_values` in the profile. Every distinct value in `sample_values` must appear as a key in the mapping dict. Additionally use world knowledge to add other likely variants not present in the sample. For example if sample shows `["GRM", "g", "MG"]` the mapping must include at minimum all common abbreviations and spellings for those unit families. If any sample value is missing from the mapping keys, add it. This is a mandatory correction — never leave a sampled value unmapped.

### Rule 2 — semantic override detection
For every operation classified as `RENAME` or FORMAT passthrough: inspect whether the source column's sample values are internal system codes, submission channel identifiers, technical keys, or numeric IDs rather than the human-readable semantic value the unified schema column represents. Examples of system codes: `"GDSN"`, `"LI"`, `"API"`, `"BATCH"`, `"SRC_01"`. If yes, reclassify as `ADD` with action `set_default`. The default value must be the data provider name inferred from context (file path, dataset name, domain). If you cannot determine the provider name with confidence, flag it in `critique_notes` and set the default to `"UNKNOWN"` rather than leaving the system code.

### Rule 3 — structural column under-classification
For every source column where `detected_structure` in the profile is `json_array`, `json_object`, `composite`, `delimited`, or `xml`: verify that the operation assigned to it reflects the structure. A `json_array` column classified as simple `FORMAT` or `RENAME` is almost certainly wrong — it should be `SPLIT` with `json_array_extract_multi` or `DERIVE` with `extract_json_field`. If a structured column has been under-classified, correct it and specify the appropriate target columns based on `inferred_keys` in the profile.

### Rule 4 — type mismatch on RENAME
For every `RENAME` operation: compare the source column's `dtype` in the profile against the target column's `type` in the unified schema. If they differ (e.g. source is `object`, target is `float`), reclassify as `CAST` and add the appropriate `target_type`. A RENAME should only remain a RENAME if types are already compatible.

### Rule 5 — derivable ADD operations
For every `ADD set_null` operation: scan all source columns in the profile and determine whether any source column could reasonably produce the target column's value through extraction, derivation, or transformation. If yes, reclassify from `ADD set_null` to the appropriate `DERIVE` or `FORMAT` operation and explain the derivation in `critique_notes`. Only leave it as `set_null` if no source data path exists.

### Rule 6 — DELETE completeness
Compare the full list of source columns in the profile against the columns consumed by operations (as `source_column`). Any source column that appears in neither the unified schema nor any operation's `source_column` field is an implicit DELETE that Agent 1 missed. Add explicit `DELETE drop_column` operations for each.

### Rule 7 — normalize_before_dedup annotation
For every operation targeting a column that is identity-bearing — meaning it will be used as a similarity signal during deduplication (product name, brand, title, identifier-like columns) — ensure the operation has `normalize_before_dedup: true`. If Agent 1 omitted this annotation on identity columns, add it.

## Output Format

Return ONLY a JSON object with this exact structure:
{{
  "revised_operations": [ ...complete corrected operations list... ],
  "critique_notes": [
    {{
      "rule": "Rule N — rule name",
      "column": "column_name",
      "original": "what Agent 1 had",
      "correction": "what you changed and why"
    }}
  ]
}}
Do not include any text outside the JSON object."""


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


