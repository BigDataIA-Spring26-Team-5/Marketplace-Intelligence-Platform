# Data Model: Domain-Scoped Schemas, Silver Normalization, and Gold Concatenation

**Feature**: 015-domain-scoped-schemas
**Date**: 2026-04-22

---

## Domain Schema File

**Location**: `config/schemas/<domain>_schema.json`
**One file per domain**: `nutrition_schema.json`, `safety_schema.json`, `pricing_schema.json`

### JSON Structure (unchanged from unified_schema.json)

```json
{
  "columns": {
    "<col_name>": {
      "type": "string|float|integer|boolean",
      "required": true|false,
      "computed": true|false,
      "enrichment": true|false,
      "enrichment_alias": "<target_col>"
    }
  },
  "dq_weights": {
    "completeness": 0.4,
    "freshness": 0.35,
    "ingredient_richness": 0.25
  }
}
```

### Pydantic Model (unchanged — `src/schema/models.py`)

`ColumnSpec` fields already in use:
- `type: Literal["string", "float", "integer", "boolean"]`
- `required: bool = False`
- `enrichment: bool = False`
- `computed: bool = False`
- `enrichment_alias: Optional[str] = None`
- `model_config = {"extra": "allow"}` — tolerates future fields (e.g., `default_value`) in JSON without parse errors

No model changes required for this feature.

### Column Sets by Domain

**`nutrition_schema.json`** — 16 columns (full set, same as current unified_schema.json):

| Column | Type | Required | Enrichment | Computed |
|--------|------|----------|------------|---------|
| `product_name` | string | ✓ | | |
| `brand_owner` | string | ✓ | | |
| `brand_name` | string | ✓ | | |
| `ingredients` | string | ✓ | | |
| `category` | string | | | |
| `serving_size` | float | | | |
| `serving_size_unit` | string | | | |
| `published_date` | string | ✓ | | |
| `data_source` | string | ✓ | | |
| `allergens` | string | | ✓ | |
| `primary_category` | string | | ✓ | |
| `dietary_tags` | string | | ✓ | |
| `is_organic` | boolean | | ✓ | |
| `dq_score_pre` | float | ✓ | | ✓ |
| `dq_score_post` | float | ✓ | | ✓ |
| `dq_delta` | float | ✓ | | ✓ |

**`safety_schema.json`** — identical to `nutrition_schema.json` (domain-specific divergence is out of scope).

**`pricing_schema.json`** — 12 columns (enrichment columns removed):

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `product_name` | string | ✓ | |
| `brand_owner` | string | ✓ | |
| `brand_name` | string | ✓ | |
| `ingredients` | string | ✓ | |
| `category` | string | | |
| `serving_size` | float | | |
| `serving_size_unit` | string | | |
| `published_date` | string | ✓ | |
| `data_source` | string | ✓ | |
| `dq_score_pre` | float | ✓ | computed |
| `dq_score_post` | float | ✓ | computed |
| `dq_delta` | float | ✓ | computed |

---

## Schema Loader (`src/schema/analyzer.py`)

### Before → After

| Before | After |
|--------|-------|
| `UNIFIED_SCHEMA_PATH = CONFIG_DIR / "unified_schema.json"` | `SCHEMAS_DIR = CONFIG_DIR / "schemas"` |
| `_schema_cache: UnifiedSchema \| None = None` | `_schema_cache: dict[str, UnifiedSchema] = {}` |
| `def get_unified_schema() -> UnifiedSchema` | `def get_domain_schema(domain: str = "nutrition") -> UnifiedSchema` |
| `def save_unified_schema(schema)` | `def save_domain_schema(schema, domain: str)` |

### `get_domain_schema` contract

```python
def get_domain_schema(domain: str = "nutrition") -> UnifiedSchema:
    """
    Load config/schemas/<domain>_schema.json.
    Raises FileNotFoundError naming the missing file if absent.
    Caches per domain — subsequent calls for same domain return cached instance.
    """
```

- Cache key: `domain` string
- Cache miss: load from `SCHEMAS_DIR / f"{domain}_schema.json"`
- FileNotFoundError message: `"config/schemas/{domain}_schema.json not found. Create it or pass a valid domain."`
- Returns: `UnifiedSchema` (existing Pydantic model, no changes needed)

### `_reset_schema_cache` update

```python
def _reset_schema_cache(domain: str | None = None) -> None:
    """Clear cache. Pass domain to clear one entry, or None to clear all."""
    if domain is None:
        _schema_cache.clear()
    else:
        _schema_cache.pop(domain, None)
```

---

## Silver Normalization

**Location**: `_silver_normalize()` private function in `src/agents/graph.py`, called from `run_pipeline_node`

### Function contract

```python
def _silver_normalize(
    df: pd.DataFrame,
    domain_schema: UnifiedSchema,
    dq_weights: dict,
) -> pd.DataFrame:
    """
    Enforce domain schema column set and order post-block-sequence.
    Adds null-filled columns for any schema column absent from df.
    Recomputes dq_score_pre if any required column was null-filled.
    Drops columns not in domain schema.
    Returns df with exactly domain_schema.columns keys in declaration order.
    """
```

### Algorithm

```
canonical_cols = list(domain_schema.columns.keys())
added_required = []

for col in canonical_cols:
    if col not in df.columns:
        df[col] = pd.NA
        if domain_schema.columns[col].required and not domain_schema.columns[col].computed:
            added_required.append(col)

if added_required:
    from src.blocks.dq_score import compute_dq_score
    df["dq_score_pre"] = compute_dq_score(df, dq_weights)

# Drop extra columns, reorder
return df[canonical_cols]
```

### Invariants
- `dq_score_pre` and `dq_score_post` are in `canonical_cols` — they are not stripped
- Recomputation only triggers when a required, non-computed column was null-filled
- Enrichment columns (`allergens`, `primary_category`, etc.) are always in `canonical_cols` for nutrition/safety; if blocks already populated them, values are preserved

---

## Silver Parquet Store

**Location**: `output/silver/<domain>/<source_name>.parquet`

| Attribute | Value |
|-----------|-------|
| Directory | `output/silver/<domain>/` — created by `save_output_node` if absent |
| File name | `<source_name>.parquet` where `source_name = Path(source_path).stem` |
| Write mode | Overwrite — same source name always overwrites |
| Schema | Exactly the domain schema column set (enforced by Silver normalization) |
| Lifecycle | Accumulates across runs. Deletion is a separate cleanup concern (out of scope). |

---

## Gold Output

**Location**: `output/gold/<domain>.parquet`

| Attribute | Value |
|-----------|-------|
| Directory | `output/gold/` — created by `save_output_node` if absent |
| File name | `<domain>.parquet` |
| Write mode | Overwrite each run (full rebuild from Silver Parquet Store) |
| Source | All `*.parquet` files under `output/silver/<domain>/` |
| Schema | Identical to domain schema (Silver normalization guarantees uniformity) |
| Empty guard | If `output/silver/<domain>/` has no parquet files, Gold write is skipped with WARNING |

### Gold concatenation algorithm

```
silver_dir = OUTPUT_DIR / "silver" / domain
silver_files = sorted(silver_dir.glob("*.parquet"))

if not silver_files:
    logger.warning("No Silver parquet files found for domain '%s' — Gold write skipped", domain)
    return

gold_df = pd.concat([pd.read_parquet(p) for p in silver_files], ignore_index=True)
gold_dir = OUTPUT_DIR / "gold"
gold_dir.mkdir(parents=True, exist_ok=True)
gold_df.to_parquet(gold_dir / f"{domain}.parquet", index=False)
logger.info("Gold output: %d rows → output/gold/%s.parquet", len(gold_df), domain)
```

---

## State Shape Changes

### `PipelineState` — no new fields required

`silver_output_path` and `gold_output_path` are recorded in the run log (via `save_output_node` return dict) but not stored in PipelineState. Existing `output_path` field in state is sufficient for the run log.

---

## Prompt Text Changes (`src/agents/prompts.py`)

| Location | Current text | Updated text |
|----------|-------------|-------------|
| `SCHEMA_ANALYSIS_PROMPT` header | `## Unified Output Schema` | `## Domain Output Schema` |
| `SCHEMA_ANALYSIS_PROMPT` description | "the unified schema" | "the domain schema (`config/schemas/<domain>_schema.json`)" |
| `FIRST_RUN_SCHEMA_PROMPT` | "There is no unified schema yet" | "There is no domain schema yet for this source" |

The `{unified_schema}` Python format placeholder **name** is unchanged (it's just a variable name injected at call time). Only human-readable text strings are updated.
