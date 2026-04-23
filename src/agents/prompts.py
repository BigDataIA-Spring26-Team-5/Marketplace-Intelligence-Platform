"""Prompt templates for Agent 1 (Orchestrator) and Agent 3 (Sequence Planner)."""

SCHEMA_ANALYSIS_PROMPT = """You are a schema analysis agent for a data enrichment pipeline.

You are given:
1. An incoming data source's schema — column names, types, null rates, sample values, AND structural metadata (detected_structure, inferred_keys, inferred_value_types, parsed_sample).
2. Optional dataset-level metadata (__meta__) with numeric_columns, structured_columns, candidate_unify_groups.
3. A domain output schema that all data sources must conform to.

Your task: For each column in the domain schema (`config/schemas/<domain>_schema.json`), determine how to map it from the incoming source using the 8-primitive taxonomy below.

## Incoming Source Schema
{source_schema}

## Dataset Metadata
{source_meta}

## Domain Output Schema
{unified_schema}

## Semantic Mapping Examples
Map source columns to unified columns based on SEMANTIC meaning, not just name:
- "product_description" / "item_name" / "name" → "product_name"
- "recalling_firm" / "manufacturer" → "brand_owner"
- "brand" → "brand_name"
- "product_type" → "category"
- "recall_initiation_date" / "report_date" → "published_date"
- "event_id" / "code_info" → "data_source"
- "brand_owner" / "brandOwner" / "manufacturer" → "brand_owner"
- "brand_name" / "brandName" / "brand" → "brand_name"
- "serving_size" / "servingSize" → "serving_size"
- "serving_size_unit" / "servingSizeUnit" → "serving_size_unit"
- "ingredient_statement" / "ingredientStatement" / "ingredients" → "ingredients"

Note: Source columns may use camelCase (brandOwner), snake_case (brand_owner), or PascalCase — treat these as semantically equivalent and map to the corresponding snake_case unified column.

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
This signals the sequence planner that fixed cleaning blocks (whitespace, lowercase) will run before dedup.

### Unix timestamp columns
When a source column contains Unix epoch timestamps (int64 values like 1617293847) — typically
column names ending in `_t`, `_at`, `_timestamp`, `_modified`, `_created`:
  - primitive: FORMAT
  - action: parse_date
  - format: unix_timestamp   ← use exactly this string, not "timestamp_to_date" or any other variant

Do NOT use `format_transform` for these columns.

Example:
  source: last_modified_t, sample_values: [1617293847, 1609459200]
  → primitive: FORMAT, action: parse_date, format: unix_timestamp

### Composite value columns (number + unit in same cell)
When a source column contains values that embed both a numeric quantity AND a unit string
(e.g., "30g", "1.5 oz", "500 ml", "1 TEA BAG (1.5g)"), and the unified schema needs both a
numeric `serving_size` column AND a `serving_size_unit` column:

Emit TWO separate FORMAT operations in this order:

1. First op — extract the unit (MUST include keep_source: true):
   {{
     "primitive": "FORMAT",
     "source_column": "serving_size",
     "target_column": "serving_size_unit",
     "target_type": "string",
     "action": "regex_extract",
     "pattern": "(g|oz|ml|l|kg|lb|mg)\\b",
     "keep_source": true
   }}

2. Second op — extract the number (source is still intact because of keep_source):
   {{
     "primitive": "FORMAT",
     "source_column": "serving_size",
     "target_column": "serving_size",
     "target_type": "float",
     "action": "regex_extract",
     "pattern": "([0-9]+(?:\\.[0-9]+)?)"
   }}

IMPORTANT ordering rules:
- Unit extraction MUST come before numeric extraction in the operations[] list.
- The first op MUST have `keep_source: true` — without it, the source column is dropped and the
  second op has nothing to read from.
- When source_column == target_column (the numeric op above), keep_source is not needed.
- Optionally follow with a FORMAT + value_map to normalize unit variants (ONZ→oz, GRM→g, etc.).

## 8-Primitive Taxonomy

Use EXACTLY these primitive names. Each unified column must appear in exactly one output list.

| Primitive | When to use | Required fields |
|-----------|-------------|-----------------|
| RENAME       | Source col maps semantically, same data, no type change needed. Use for ALL plain-string copies — product names, ingredient text, descriptions, brand text — even when renaming to a different target name. | source_column, target_column |
| CAST         | Source col maps semantically but needs type conversion (e.g. int64→string) | source_column, target_column, target_type, source_type, action (one of: type_cast) |
| FORMAT       | Source col needs value transformation (date parsing, regex, value mapping, etc.) | source_column, target_column, target_type, action (one of: parse_date, regex_replace, regex_extract, truncate_string, pad_string, value_map, format_transform), optional: mapping dict for value_map |
| DELETE       | Source col has no place in unified schema — drop it | source_column |
| ADD          | No source data exists — create null or constant column | target_column, target_type, action (one of: set_null, set_default), optional: default_value |
| SPLIT        | 1 source col → N target cols (JSON array, delimited string) | source_column, action (one of: json_array_extract_multi, split_column, xml_extract), target_columns dict |
| UNIFY        | N source cols → 1 target col (first-non-null, concatenate, template) | sources list, target_column, action (one of: coalesce, concat_columns, string_template) |
| DERIVE       | Complex extraction or conditional logic (keyword→bool, JSON field extract, arithmetic) | source_column (or sources list), target_column, target_type, action (one of: extract_json_field, conditional_map, expression, contains_flag) |
| ENRICH_ALIAS | Required col has no source data, but a semantically equivalent enrichment col (enrichment: true) will fill it downstream | target_column, source_enrichment |

**CRITICAL — extract_json_field vs RENAME:**
- Use `DERIVE` + `extract_json_field` ONLY when `detected_structure` in the source profile is
  `json_object` or `json_array`. This action calls `json.loads()` on each cell — plain text returns null.
- For ANY plain-text column (detected_structure = string, plain, or absent) that maps to a unified
  column: use `RENAME` regardless of whether source_col == target_col or not.
- Columns like `ingredients_text`, `product_name`, `description`, `brand` — these are plain text.
  Always RENAME, never DERIVE + extract_json_field.

## SPLIT action details
For json_array_extract_multi, target_columns is a dict:
  "target_columns": {{
    "col_name": {{"key": "field_in_array", "filter": {{"name": "Energy"}}, "type": "float"}},
    "col_name2": {{"key": "other_field", "join_all": true, "type": "string"}}
  }}

## Schema-Defined Enrichment Aliases

Some unified schema columns have an `enrichment_alias` field that explicitly declares which enrichment column will fill them downstream. These are **mandatory ENRICH_ALIAS** — do not use ADD, RENAME, or any other primitive for these columns.

Scan unified schema for columns with `"enrichment_alias"`: any such column MUST be output as `ENRICH_ALIAS` with `source_enrichment` set to the value of `enrichment_alias`. Do not attempt to map source data to these columns.

Additionally, for any required column with no source data path, check if an enrichment column (`"enrichment": true`) covers the same concept → output ENRICH_ALIAS.

## Important rules
- RENAME goes in column_mapping (not operations[]).
- Everything else goes in operations[].
- Do NOT generate these FORMAT actions (handled by fixed cleaning blocks post-transformation):
  - strip_whitespace
  - to_lowercase
  - to_uppercase
  These run automatically on ALL datasets after DynamicMappingBlock. Only use FORMAT for dataset-specific transformations like parse_date, value_map, regex_replace.
- If a unified column truly has no source data and cannot be derived → put it in unresolvable[] with a reason and fallback: "set_null".
- Do NOT classify as unresolvable if any source column could produce the data with a SPLIT/DERIVE action.
- NEVER map nutrient arrays (foodNutrients) to "ingredients" — nutrients are lab measurements, not ingredient lists.
- Columns marked `"enrichment": true` will be filled by downstream enrichment blocks — do NOT map source columns to them and do NOT include them in operations[].
- Columns marked `"computed": true` (dq_score_*, dq_delta) — skip entirely.
- NEVER use DERIVE + extract_json_field for plain-text columns. If detected_structure is not
  json_object or json_array, use RENAME instead. extract_json_field silently produces 100% null
  for plain strings because it calls json.loads() internally.
- For FORMAT + parse_date on Unix timestamps, always set format: unix_timestamp (not
  "timestamp_to_date" or any other string). This is the only sentinel the mapper recognizes.
- When emitting multiple FORMAT ops on the same source_column that produce different targets,
  all ops except the last must include keep_source: true to prevent premature column deletion.
- For a required non-enrichment column with no source data: if an enrichment column (`"enrichment": true`) semantically covers the same concept (same meaning, compatible type), output ENRICH_ALIAS instead of ADD set_null. Required fields: target_column (the required col), source_enrichment (the enrichment col). Example: `category` (required) has no source data but `primary_category` (enrichment) represents the same concept → ENRICH_ALIAS.
- Only use ENRICH_ALIAS when you are confident the enrichment column will produce the same data the required column needs. Otherwise use ADD set_null.

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
    }},
    {{
      "primitive": "ENRICH_ALIAS",
      "target_column": "category",
      "source_enrichment": "primary_category"
    }}
  ],
  "unresolvable": [
    {{
      "target_column": "ingredients",
      "reason": "No ingredient text exists in source — nutrient array is not a substitute",
      "fallback": "set_null"
    }}
  ]
}}

Note: ENRICH_ALIAS example (use when a required col is semantically covered by an enrichment col):
{{
  "primitive": "ENRICH_ALIAS",
  "target_column": "category",
  "source_enrichment": "primary_category"
}}
ENRICH_ALIAS goes in operations[], not unresolvable[].
"""


FIRST_RUN_SCHEMA_PROMPT = """You are a schema analysis agent. This is the FIRST data source for this pipeline.
There is no domain schema yet for this source — you must derive one.

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
3. The column_mapping already decided (RENAME operations — these columns are already resolved)
4. The gap analysis operations list (everything else: CAST, FORMAT, ADD, SPLIT, UNIFY, DERIVE, DELETE, ENRICH_ALIAS)

Your job is to find errors in the 4 areas below. Do NOT touch what is already correct.

## Source Data Profile
{source_profile}

## Dataset Metadata
{source_meta}

## Unified Target Schema
{unified_schema}

## Already-Resolved Column Mapping (RENAME operations — DO NOT re-add these)
{column_mapping}

The target columns in column_mapping are already handled. Do NOT add operations for them. Do NOT flag them as missing or derivable.

## Junior Engineer's Operations List
{operations}

## Verification Rules

Apply ONLY these 4 rules. Do not invent others.

### Rule 1 — value_map completeness
For every operation with action `value_map`: inspect the source column's `sample_values` in the profile. Every distinct value in `sample_values` must appear as a key in the mapping dict. Use world knowledge to add other likely variants not in the sample (e.g., if sample shows `["GRM", "g", "MG"]`, add all common abbreviations for those unit families). This is mandatory — never leave a sampled value unmapped.

### Rule 2 — semantic override detection
For every RENAME or FORMAT passthrough in operations[]: inspect whether sample values are internal system codes, submission channel identifiers, technical keys, or numeric IDs rather than the human-readable semantic value the unified schema column represents. Examples: `"GDSN"`, `"LI"`, `"API"`, `"BATCH"`, `"SRC_01"`. If yes, reclassify as `ADD set_default`. Default value = data provider name from context (file path, dataset name, domain). If uncertain, set default to `"UNKNOWN"` and flag in critique_notes.

### Rule 3 — structural column under-classification
For every source column where `detected_structure` in the profile is `json_array`, `json_object`, `composite`, `delimited`, or `xml`: verify the operation reflects the structure. A `json_array` column classified as simple RENAME or FORMAT is wrong — it should be SPLIT with `json_array_extract_multi` or DERIVE with `extract_json_field`. If under-classified, correct it using `inferred_keys` from the profile.

### Rule 5 — derivable ADD operations
For every `ADD set_null` in operations[]: check whether any source column that is NOT already in column_mapping could produce the target value via extraction, derivation, or transformation. If a genuine unused source path exists, reclassify to DERIVE or FORMAT. Do NOT reclassify if the source column is already in column_mapping (it's already handled as a RENAME).

## Output Format

Return ONLY a JSON object:
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

Your job is to select which optional blocks to run and determine the optimal execution order based on the source data characteristics.

## Domain
{domain}

## Source Schema (column names and types)
{source_schema}

## Schema Gaps and Registry Results
{gap_summary}

## Mandatory Blocks (MUST always appear in output, in correct position)
{mandatory_blocks}

## Optional Blocks (include ONLY if beneficial for this source)
{optional_blocks}

## Selection Rules for Optional Blocks
- strip_whitespace: include if any string columns exist (almost always yes)
- lowercase_brand: include if brand_name or brand_owner columns are present and non-trivially populated
- remove_noise_words: include if product_name likely contains legal suffixes, language filler words, or marketing noise (e.g. OpenFoodFacts multilingual data). Skip for clean structured sources like USDA.
- strip_punctuation: include if product_name or ingredients contain excessive punctuation noise. Skip for clean structured sources.
- extract_quantity_column: include if product_name likely embeds quantity/weight info (e.g. "Coca Cola 330ml"). Skip if product names are plain descriptors.
- enrich_stage: include if the domain has enrichment columns (allergens, primary_category, dietary_tags, is_organic) and the source has ingredients or product text to enrich from.

## Ordering Rules
- Mandatory blocks keep their fixed positions (dq_score_pre first, __generated__ second, schema_enforce or dq_score_post last)
- Normalization blocks (strip_whitespace, lowercase_brand, remove_noise_words, strip_punctuation) run before deduplication
- extract_allergens runs before llm_enrich
- Deduplication (dedup_stage) runs after all normalization
- enrich_stage runs after dedup_stage
- Stage names: "dedup_stage" = [fuzzy_deduplicate, column_wise_merge, golden_record_select]
- Stage names: "enrich_stage" = [extract_allergens, llm_enrich]

Return ONLY a JSON object:
{{
  "block_sequence": ["block_name_1", "block_name_2", ...],
  "reasoning": "One sentence explaining key selection and ordering decisions",
  "skipped_blocks": {{"block_name": "reason skipped"}}
}}

Include all mandatory blocks. Include only the optional blocks that add value for this specific source."""


