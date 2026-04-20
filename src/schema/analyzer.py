"""Schema analysis utilities — profile DataFrames and diff against unified schema."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.schema.models import ColumnSpec, UnifiedSchema

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"
UNIFIED_SCHEMA_PATH = CONFIG_DIR / "unified_schema.json"

# Lazy singleton — loaded once per process, reset via _reset_schema_cache() in tests.
_schema_cache: UnifiedSchema | None = None


def get_unified_schema() -> UnifiedSchema:
    """Return the unified schema, loading and caching on first call.

    Raises FileNotFoundError if config/unified_schema.json is absent.
    """
    global _schema_cache
    if _schema_cache is None:
        if not UNIFIED_SCHEMA_PATH.exists():
            raise FileNotFoundError(
                "config/unified_schema.json not found. "
                "The unified schema is the gold-standard target format and must be "
                "defined before running the pipeline."
            )
        with open(UNIFIED_SCHEMA_PATH) as f:
            _schema_cache = UnifiedSchema.model_validate(json.load(f))
    return _schema_cache


def _reset_schema_cache() -> None:
    """Reset the schema cache. For use in tests only."""
    global _schema_cache
    _schema_cache = None

# Minimum fraction of non-null rows that must successfully parse for a
# structural detection to be accepted (avoids false positives on mostly-scalar cols).
_PARSE_THRESHOLD = 0.6


def _try_parse_json(value: str) -> Any | None:
    """Try to parse a string as JSON. Returns parsed value or None."""
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        pass
    # Python single-quote dicts/lists (e.g. from repr())
    try:
        result = ast.literal_eval(value)
        if isinstance(result, (dict, list)):
            return result
    except (ValueError, SyntaxError, TypeError):
        pass
    return None


def _detect_structure(series: pd.Series) -> str:
    """
    Detect the structural type of a string column by sampling non-null values.

    Returns one of: scalar | json_array | json_object | delimited | composite | xml
    """
    non_null = series.dropna().astype(str)
    if len(non_null) == 0:
        return "scalar"

    sample = non_null.head(20).tolist()
    total = len(sample)

    # JSON array / object
    json_array_count = 0
    json_object_count = 0
    for v in sample:
        parsed = _try_parse_json(v)
        if isinstance(parsed, list):
            json_array_count += 1
        elif isinstance(parsed, dict):
            json_object_count += 1

    if json_array_count / total >= _PARSE_THRESHOLD:
        return "json_array"
    if json_object_count / total >= _PARSE_THRESHOLD:
        return "json_object"

    # XML
    xml_count = sum(1 for v in sample if re.search(r"<\w+[\s>]", v))
    if xml_count / total >= _PARSE_THRESHOLD:
        return "xml"

    # Delimited: 3+ consistent delimiter-separated tokens in majority of values
    for delim in ("|", ";", "\t"):
        delim_count = sum(1 for v in sample if v.count(delim) >= 2)
        if delim_count / total >= _PARSE_THRESHOLD:
            return "delimited"

    # Composite: "value unit" pattern — number followed by non-numeric word
    composite_count = sum(
        1 for v in sample if re.match(r"^\d+(\.\d+)?\s+[a-zA-Z]+", v.strip())
    )
    if composite_count / total >= _PARSE_THRESHOLD:
        return "composite"

    return "scalar"


def _parse_json_samples(series: pd.Series, n: int = 3) -> list[Any]:
    """Return up to n parsed JSON values from a series."""
    parsed = []
    for v in series.dropna().astype(str):
        result = _try_parse_json(v)
        if result is not None:
            parsed.append(result)
            if len(parsed) >= n:
                break
    return parsed


def _infer_keys_and_types(
    series: pd.Series,
    structure: str,
) -> tuple[list[str], dict[str, str]]:
    """
    For json_array / json_object columns, collect keys and infer per-key value types.

    Returns:
        inferred_keys: sorted list of keys seen across sampled rows
        inferred_value_types: {key: dtype_name}
    """
    if structure not in ("json_array", "json_object"):
        return [], {}

    key_type_samples: dict[str, list] = {}
    sample_size = min(50, len(series.dropna()))

    for v in series.dropna().astype(str).head(sample_size):
        parsed = _try_parse_json(v)
        if parsed is None:
            continue

        # Normalise: list-of-dicts → iterate items; plain dict → single item
        items: list[dict] = []
        if isinstance(parsed, list):
            items = [x for x in parsed if isinstance(x, dict)]
        elif isinstance(parsed, dict):
            items = [parsed]

        for item in items:
            for k, val in item.items():
                if k not in key_type_samples:
                    key_type_samples[k] = []
                key_type_samples[k].append(val)

    if not key_type_samples:
        return [], {}

    inferred_keys = sorted(key_type_samples.keys())
    inferred_value_types: dict[str, str] = {}
    for k, vals in key_type_samples.items():
        non_none = [v for v in vals if v is not None]
        if not non_none:
            inferred_value_types[k] = "null"
        elif all(isinstance(v, bool) for v in non_none):
            inferred_value_types[k] = "boolean"
        elif all(isinstance(v, int) for v in non_none):
            inferred_value_types[k] = "integer"
        elif all(isinstance(v, (int, float)) for v in non_none):
            inferred_value_types[k] = "float"
        elif all(isinstance(v, str) for v in non_none):
            inferred_value_types[k] = "string"
        else:
            inferred_value_types[k] = "mixed"

    return inferred_keys, inferred_value_types


def _count_components(series: pd.Series, structure: str) -> int:
    """Estimate typical sub-component count for SPLIT candidates."""
    if structure == "json_array":
        counts = []
        for v in series.dropna().astype(str).head(20):
            parsed = _try_parse_json(v)
            if isinstance(parsed, list):
                counts.append(len(parsed))
        return int(sum(counts) / len(counts)) if counts else 0

    if structure == "json_object":
        counts = []
        for v in series.dropna().astype(str).head(20):
            parsed = _try_parse_json(v)
            if isinstance(parsed, dict):
                counts.append(len(parsed))
        return int(sum(counts) / len(counts)) if counts else 0

    if structure == "delimited":
        for delim in ("|", ";", "\t"):
            sample = series.dropna().astype(str).head(20)
            counts = [v.count(delim) + 1 for v in sample if v.count(delim) >= 2]
            if counts:
                return int(sum(counts) / len(counts))

    if structure == "composite":
        return 2  # typically "value unit"

    return 1


def _candidate_unify_groups(profile: dict) -> list[list[str]]:
    """
    Find columns that look like they belong together (UNIFY candidates).

    Heuristics:
    - Columns sharing a common prefix with a numeric/positional suffix
      (e.g., addr_1, addr_2, addr_3)
    - Pairs where one is a numeric amount and the other is a unit string
      with a shared stem (e.g., net_weight, net_weight_unit)
    """
    cols = list(profile.keys())
    groups: list[list[str]] = []

    # Numeric suffix groups
    stem_map: dict[str, list[str]] = {}
    for col in cols:
        m = re.match(r"^(.+?)_?(\d+)$", col)
        if m:
            stem = m.group(1)
            stem_map.setdefault(stem, []).append(col)
    for stem, members in stem_map.items():
        if len(members) >= 2:
            groups.append(sorted(members))

    # Amount + unit pairs (e.g., serving_size + serving_size_unit)
    for col in cols:
        unit_col = col + "_unit"
        if unit_col in profile:
            groups.append([col, unit_col])

    # Deduplicate (simple — remove exact duplicates)
    seen: set[tuple] = set()
    unique_groups = []
    for g in groups:
        key = tuple(sorted(g))
        if key not in seen:
            seen.add(key)
            unique_groups.append(g)

    return unique_groups


def profile_dataframe(df: pd.DataFrame, sample_size: int = 5) -> dict:
    """
    Profile a DataFrame's schema with structural detection.

    Returns a dict with per-column metadata plus a ``__meta__`` root block.

    Per-column fields:
        dtype               pandas dtype string
        null_rate           fraction of null values [0.0–1.0]
        unique_count        number of distinct non-null values
        sample_values       list of up to sample_size raw string values
        is_numeric          True if column holds numeric data
        detected_structure  scalar | json_array | json_object | delimited | composite | xml
        parsed_sample       list of parsed (non-string) representations (for structured cols)
        inferred_keys       list of keys found in sampled rows (json_array / json_object only)
        inferred_value_types  {key: dtype} per key (json_array / json_object only)
        component_count     typical number of sub-values (for SPLIT candidates)

    Root ``__meta__`` block:
        row_count               total rows in the DataFrame
        numeric_columns         list of numeric column names
        structured_columns      list of non-scalar column names
        candidate_unify_groups  groups of columns that look like they belong together
    """
    profile: dict = {}
    numeric_columns: list[str] = []
    structured_columns: list[str] = []

    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        samples = non_null.head(sample_size).astype(str).tolist() if len(non_null) > 0 else []

        dtype_str = str(series.dtype)
        is_numeric = pd.api.types.is_numeric_dtype(series)

        # Structural detection only for object/string columns
        if is_numeric or dtype_str == "bool":
            structure = "scalar"
        else:
            structure = _detect_structure(series)

        # Parsed sample (only for structured cols — keeps profile compact)
        parsed_sample: list[Any] = []
        inferred_keys: list[str] = []
        inferred_value_types: dict[str, str] = {}

        if structure in ("json_array", "json_object"):
            parsed_sample = _parse_json_samples(series, n=3)
            inferred_keys, inferred_value_types = _infer_keys_and_types(series, structure)

        component_count = _count_components(series, structure)

        entry: dict[str, Any] = {
            "dtype": dtype_str,
            "null_rate": round(float(series.isna().mean()), 4),
            "unique_count": int(series.nunique()),
            "sample_values": samples,
            "is_numeric": is_numeric,
            "detected_structure": structure,
            "component_count": component_count,
        }

        if parsed_sample:
            entry["parsed_sample"] = parsed_sample
        if inferred_keys:
            entry["inferred_keys"] = inferred_keys
        if inferred_value_types:
            entry["inferred_value_types"] = inferred_value_types

        profile[col] = entry

        if is_numeric:
            numeric_columns.append(col)
        if structure != "scalar":
            structured_columns.append(col)

    profile["__meta__"] = {
        "row_count": len(df),
        "numeric_columns": numeric_columns,
        "structured_columns": structured_columns,
        "candidate_unify_groups": _candidate_unify_groups(profile),
    }

    return profile


def save_unified_schema(schema: UnifiedSchema) -> None:
    """Save unified schema to config."""
    UNIFIED_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(UNIFIED_SCHEMA_PATH, "w") as f:
        json.dump(schema.model_dump(), f, indent=2)


def derive_unified_schema_from_source(
    df: pd.DataFrame,
    column_mapping: dict[str, str],
    domain: str,
) -> dict:
    """
    Derive a unified schema from the first data source.

    column_mapping: {source_col -> unified_col}
    Returns a schema dict compatible with config/unified_schema.json format.
    """
    columns = {}
    for source_col, unified_col in column_mapping.items():
        dtype = str(df[source_col].dtype)
        # Map pandas dtypes to schema types
        if "int" in dtype:
            schema_type = "integer"
        elif "float" in dtype:
            schema_type = "float"
        elif "bool" in dtype:
            schema_type = "boolean"
        else:
            schema_type = "string"

        null_rate = float(df[source_col].isna().mean())
        columns[unified_col] = {
            "type": schema_type,
            "required": null_rate < 0.5,
        }

    # Add standard enrichment columns
    for enrich_col in ["allergens", "primary_category", "dietary_tags", "is_organic"]:
        if enrich_col not in columns:
            columns[enrich_col] = {
                "type": "boolean" if enrich_col == "is_organic" else "string",
                "required": False,
                "enrichment": True,
            }

    # Add computed columns
    for computed_col in ["dq_score_pre", "dq_score_post", "dq_delta"]:
        columns[computed_col] = {
            "type": "float",
            "required": True,
            "computed": True,
        }

    return UnifiedSchema.model_validate({
        "columns": columns,
        "dq_weights": {
            "completeness": 0.4,
            "freshness": 0.35,
            "ingredient_richness": 0.25,
        },
    })


def compute_schema_diff(
    source_profile: dict,
    unified_schema: UnifiedSchema,
) -> tuple[dict, list[dict]]:
    """
    Compute the diff between a source profile and the unified schema.

    Returns:
        column_mapping: {source_col -> unified_col} for direct matches
        gaps: list of gap dicts for columns that need transformation
    """
    mappable_cols = unified_schema.mappable_columns

    column_mapping = {}
    gaps = []

    source_cols_remaining = set(k for k in source_profile.keys() if k != "__meta__")
    target_cols_remaining = set(mappable_cols.keys())

    for src_col in list(source_cols_remaining):
        if src_col in target_cols_remaining:
            column_mapping[src_col] = src_col
            source_cols_remaining.discard(src_col)
            target_cols_remaining.discard(src_col)

    for target_col in target_cols_remaining:
        target_spec = mappable_cols[target_col]
        gaps.append({
            "target_column": target_col,
            "target_type": target_spec.type,
            "source_column": None,
            "source_type": None,
            "action": "ADD",
            "sample_values": [],
        })

    return column_mapping, gaps
