"""DynamicMappingBlock — declarative YAML-driven column operations.

Handles all schema gap types declaratively. Operations are defined in a YAML file and
executed deterministically with correct null handling.

Supported actions:
  Scalar creation:  set_null, set_default
  Type ops:         type_cast, rename, drop_column
  Format ops:       format_transform, parse_date, to_lowercase, to_uppercase,
                    strip_whitespace, regex_replace, regex_extract,
                    truncate_string, pad_string, value_map
  Split ops:        json_array_extract_multi, split_column, xml_extract
  Unify ops:        coalesce, concat_columns, string_template
  Derive ops:       extract_json_field, conditional_map, expression, contains_flag
"""

from __future__ import annotations

import ast
import json
import logging
import re
from typing import Any

import pandas as pd

from src.blocks.base import Block
from src.blocks.mapping_io import read_mapping_yaml

logger = logging.getLogger(__name__)

# Pandas nullable dtype mapping for set_null operations.
# Using nullable dtypes ensures proper NA semantics (no "None" strings).
_NULL_DTYPE_MAP = {
    "float": "Float64",
    "integer": "Int64",
    "int": "Int64",
    "boolean": "boolean",
    "bool": "boolean",
    "string": "string",
    "str": "string",
}


def _try_parse(value: str) -> Any:
    """Try JSON then ast.literal_eval for Python repr strings."""
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        pass
    try:
        result = ast.literal_eval(value)
        if isinstance(result, (dict, list)):
            return result
    except (ValueError, SyntaxError, TypeError):
        pass
    return None


class DynamicMappingBlock(Block):
    """Declarative YAML-driven column operations.

    Reads a YAML mapping file and applies each operation in sequence.

    The block name starts with ``DYNAMIC_MAPPING_`` so that
    ``PipelineRunner._expand_sequence`` picks it up via prefix matching
    when expanding the ``__generated__`` sentinel.
    """

    def __init__(self, domain: str, yaml_path: str) -> None:
        self._yaml_path = yaml_path
        self._operations = read_mapping_yaml(yaml_path)
        self.name = f"DYNAMIC_MAPPING_{domain}"
        self.domain = domain
        self.description = f"Declarative column operations from {yaml_path}"
        self.inputs = [
            op["source"]
            for op in self._operations
            if op.get("source") and isinstance(op.get("source"), str)
        ]
        self.outputs = [
            op["target"]
            for op in self._operations
            if op.get("target")
        ]

    @property
    def operations(self) -> list[dict[str, Any]]:
        return list(self._operations)

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        for op in self._operations:
            action = op["action"]
            handler = _ACTION_HANDLERS.get(action)
            if handler is None:
                logger.warning(f"Unknown action '{action}' for target '{op.get('target', '?')}' — skipping")
                continue
            try:
                df = handler(df, op)
            except Exception as exc:
                logger.error(f"Action '{action}' on '{op.get('target', '?')}' failed: {exc}")
        return df


# ── Scalar creation ───────────────────────────────────────────────────


def _handle_set_null(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Create a column filled with proper typed null values."""
    target = op["target"]
    col_type = op.get("type", "string")
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    df[target] = pd.array([pd.NA] * len(df), dtype=dtype)
    logger.debug(f"set_null: created '{target}' as {dtype} (all NA)")
    return df


def _handle_set_default(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Create a column with a user-specified default value."""
    target = op["target"]
    col_type = op.get("type", "string")
    default = op.get("default_value")

    if default is None:
        return _handle_set_null(df, op)

    value = _cast_value(default, col_type)
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    df[target] = pd.array([value] * len(df), dtype=dtype)
    logger.debug(f"set_default: created '{target}' = {value!r}")
    return df


# ── Type ops ──────────────────────────────────────────────────────────


def _handle_type_cast(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Convert an existing source column to the target type."""
    source = op["source"]
    target = op["target"]
    col_type = op.get("type", "string")

    if source not in df.columns:
        logger.warning(f"type_cast: source column '{source}' not found — falling back to set_null")
        return _handle_set_null(df, op)

    if col_type in ("float", "integer", "int"):
        df[target] = pd.to_numeric(df[source], errors="coerce")
        if col_type in ("integer", "int"):
            df[target] = df[target].astype("Int64")
    elif col_type in ("string", "str"):
        df[target] = df[source].astype("string")
    elif col_type in ("boolean", "bool"):
        df[target] = (
            df[source]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "1": True, "yes": True,
                   "false": False, "0": False, "no": False})
            .astype("boolean")
        )
    else:
        df[target] = df[source].astype("string")

    if source != target and source in df.columns:
        df = df.drop(columns=[source])

    logger.debug(f"type_cast: '{source}' → '{target}' as {col_type}")
    return df


def _handle_rename(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Rename a source column to target name."""
    source = op["source"]
    target = op["target"]
    if source not in df.columns:
        logger.warning(f"rename: source '{source}' not found — skipping")
        return df
    df = df.rename(columns={source: target})
    logger.debug(f"rename: '{source}' → '{target}'")
    return df


def _handle_drop_column(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Drop a source column that has no place in the unified schema."""
    source = op.get("source") or op.get("target")
    if source and source in df.columns:
        df = df.drop(columns=[source])
        logger.debug(f"drop_column: dropped '{source}'")
    return df


# ── Format ops ────────────────────────────────────────────────────────


def _handle_format_transform(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Apply a named format transformation (legacy dispatcher)."""
    transform = op.get("transform", "to_string")
    # Dispatch to dedicated handlers by transform name
    transform_map = {
        "to_string": _fmt_to_string,
        "parse_date": _fmt_parse_date,
        "to_lowercase": _fmt_to_lowercase,
        "to_uppercase": _fmt_to_uppercase,
        "strip_whitespace": _fmt_strip_whitespace,
    }
    handler = transform_map.get(transform)
    if handler:
        return handler(df, op)
    logger.warning(f"Unknown format_transform '{transform}' — copying column as-is")
    source = op.get("source", op["target"])
    target = op["target"]
    if source in df.columns:
        df[target] = df[source]
        if source != target and source in df.columns:
            df = df.drop(columns=[source])
    return df


def _fmt_to_string(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    source = op.get("source", op["target"])
    target = op["target"]
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string")
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _fmt_parse_date(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    source = op.get("source", op["target"])
    target = op["target"]
    fmt = op.get("format")  # optional strptime format
    if source not in df.columns:
        return _handle_set_null(df, op)
    if fmt == "unix_timestamp":
        df[target] = pd.to_datetime(
            pd.to_numeric(df[source], errors="coerce"), unit="s", errors="coerce"
        )
    else:
        df[target] = pd.to_datetime(df[source], format=fmt, errors="coerce")
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _fmt_to_lowercase(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    source = op.get("source", op["target"])
    target = op["target"]
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string").str.lower()
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _fmt_to_uppercase(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    source = op.get("source", op["target"])
    target = op["target"]
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string").str.upper()
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _fmt_strip_whitespace(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    source = op.get("source", op["target"])
    target = op["target"]
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string").str.strip()
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _handle_parse_date(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    return _fmt_parse_date(df, op)


def _handle_to_lowercase(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    return _fmt_to_lowercase(df, op)


def _handle_to_uppercase(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    return _fmt_to_uppercase(df, op)


def _handle_strip_whitespace(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    return _fmt_strip_whitespace(df, op)


def _handle_regex_replace(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Replace regex pattern with a replacement string."""
    source = op.get("source", op["target"])
    target = op["target"]
    pattern = op.get("pattern", "")
    replacement = op.get("replacement", "")
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string").str.replace(pattern, replacement, regex=True)
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _handle_regex_extract(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Extract first regex group from source column."""
    source = op.get("source", op["target"])
    target = op["target"]
    pattern = op.get("pattern", "")
    if source not in df.columns:
        return _handle_set_null(df, op)
    # Don't double-wrap: if pattern already has a capture group, use it as-is
    has_group = bool(re.search(r'(?<!\\)\((?!\?)', pattern))
    extract_pattern = pattern if has_group else f"({pattern})"
    extracted = df[source].astype("string").str.extract(extract_pattern, expand=False)
    col_type = op.get("type", "string")
    if col_type in ("float", "float64", "Float64"):
        df[target] = pd.to_numeric(extracted, errors="coerce")
    elif col_type in ("integer", "int", "int64", "Int64"):
        df[target] = pd.to_numeric(extracted, errors="coerce").astype("Int64")
    else:
        df[target] = extracted
    if source != target and source in df.columns and not op.get("keep_source", False):
        df = df.drop(columns=[source])
    return df


def _handle_truncate_string(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Truncate string to max_length characters."""
    source = op.get("source", op["target"])
    target = op["target"]
    max_len = int(op.get("max_length", 255))
    if source not in df.columns:
        return _handle_set_null(df, op)
    df[target] = df[source].astype("string").str[:max_len]
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _handle_pad_string(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Pad string to min_length with fill_char (default '0'), side='left'|'right'."""
    source = op.get("source", op["target"])
    target = op["target"]
    min_len = int(op.get("min_length", 0))
    fill_char = str(op.get("fill_char", "0"))[:1] or "0"
    side = op.get("side", "left")
    if source not in df.columns:
        return _handle_set_null(df, op)
    s = df[source].astype("string")
    if side == "left":
        df[target] = s.str.zfill(min_len) if fill_char == "0" else s.str.rjust(min_len, fill_char)
    else:
        df[target] = s.str.ljust(min_len, fill_char)
    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    return df


def _handle_value_map(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Map source values to target values via explicit mapping dict.

    Unmapped values pass through unchanged by default.

    op parameters:
        source: str
        target: str
        mapping: dict — {source_value: target_value}
        default: optional fallback (if None, unmapped pass through)
        type: target type
    """
    source = op.get("source", op["target"])
    target = op["target"]
    mapping = op.get("mapping", {})
    default = op.get("default")
    col_type = op.get("type", "string")

    if source not in df.columns:
        return _handle_set_null(df, op)

    lower_map = {str(k).lower(): v for k, v in mapping.items()}

    def apply_map(v):
        if pd.isna(v):
            return default if default is not None else pd.NA
        key = str(v).strip().lower()
        if key in lower_map:
            return lower_map[key]
        return default if default is not None else v

    df[target] = df[source].apply(apply_map)
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    df[target] = df[target].astype(dtype)

    if source != target and source in df.columns:
        df = df.drop(columns=[source])
    logger.debug(f"value_map: '{source}' → '{target}' with {len(mapping)} mappings")
    return df


# ── Split ops ─────────────────────────────────────────────────────────


def _handle_json_array_extract_multi(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Extract multiple fields from a JSON array column into separate target columns.

    op parameters:
        source: str — source column containing JSON array of objects
        target_columns: dict — {target_col_name: {key, filter?, join_all?}}
            key:       the field name to extract from each array element
            filter:    optional {field: value} condition — only match items where field==value
            join_all:  if True, join all matched values as comma-separated string
                       if False (default), take first matched value
    """
    source = op["source"]
    target_columns: dict = op.get("target_columns", {})

    if source not in df.columns:
        logger.warning(f"json_array_extract_multi: source '{source}' not found")
        for col_name, spec in target_columns.items():
            col_type = spec.get("type", "string")
            df[col_name] = pd.array([pd.NA] * len(df), dtype=_NULL_DTYPE_MAP.get(col_type, "string"))
        return df

    # Pre-parse the source column once
    parsed_col = df[source].apply(lambda v: _try_parse(str(v)) if pd.notna(v) else None)

    for col_name, spec in target_columns.items():
        key = spec.get("key")
        filt = spec.get("filter")  # {field: value} or None
        join_all = spec.get("join_all", False)
        col_type = spec.get("type", "string")

        def extract_value(items, key=key, filt=filt, join_all=join_all):
            if not isinstance(items, list):
                return None
            matched = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if filt:
                    filt_field, filt_val = next(iter(filt.items()))
                    if str(item.get(filt_field, "")).lower() != str(filt_val).lower():
                        continue
                if key in item:
                    matched.append(item[key])
            if not matched:
                return None
            if join_all:
                return ", ".join(str(v) for v in matched)
            return matched[0]

        raw = parsed_col.apply(extract_value)

        dtype = _NULL_DTYPE_MAP.get(col_type, "string")
        if col_type in ("float", "integer", "int"):
            df[col_name] = pd.to_numeric(raw, errors="coerce")
            if col_type in ("integer", "int"):
                df[col_name] = df[col_name].astype("Int64")
        else:
            df[col_name] = raw.astype("string")

        logger.debug(f"json_array_extract_multi: extracted '{key}' → '{col_name}'")

    return df


def _handle_split_column(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Split a delimited string column into N named target columns.

    op parameters:
        source: str
        delimiter: str (default ',')
        column_names: list[str] — names for the resulting split parts
        strip: bool — strip whitespace from parts (default True)
    """
    source = op["source"]
    delimiter = op.get("delimiter", ",")
    column_names: list[str] = op.get("column_names", [])
    strip = op.get("strip", True)

    if source not in df.columns or not column_names:
        return df

    split_df = df[source].astype("string").str.split(delimiter, expand=True)
    for i, col_name in enumerate(column_names):
        if i < split_df.shape[1]:
            col = split_df[i]
            if strip:
                col = col.str.strip()
            df[col_name] = col
        else:
            df[col_name] = pd.array([pd.NA] * len(df), dtype="string")
    return df


def _handle_xml_extract(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Extract a field from an XML column using a simple tag search.

    op parameters:
        source: str
        tag: str — XML tag name to extract (first occurrence)
        target: str
    """
    source = op["source"]
    target = op["target"]
    tag = op.get("tag", "")

    if source not in df.columns or not tag:
        return _handle_set_null(df, op)

    pattern = rf"<{re.escape(tag)}[^>]*>(.*?)</{re.escape(tag)}>"

    def extract_tag(v):
        if pd.isna(v):
            return pd.NA
        m = re.search(pattern, str(v), re.DOTALL)
        return m.group(1).strip() if m else pd.NA

    df[target] = df[source].apply(extract_tag).astype("string")
    return df


# ── Unify ops ─────────────────────────────────────────────────────────


def _handle_coalesce(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Return first non-null value across a list of source columns."""
    sources: list[str] = op.get("sources", [])
    target = op["target"]
    col_type = op.get("type", "string")

    result = pd.Series([pd.NA] * len(df), index=df.index, dtype=object)
    for src in sources:
        if src in df.columns:
            mask = result.isna()
            result[mask] = df.loc[mask, src]

    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    try:
        df[target] = result.astype(dtype)
    except Exception:
        df[target] = result.astype("string")
    logger.debug(f"coalesce: {sources} → '{target}'")
    return df


def _handle_concat_columns(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Concatenate multiple source columns with a separator."""
    sources: list[str] = op.get("sources", [])
    target = op["target"]
    separator = op.get("separator", " ")
    exclude_nulls = op.get("exclude_nulls", True)

    valid_sources = [s for s in sources if s in df.columns]
    if not valid_sources:
        return _handle_set_null(df, op)

    def concat_row(row):
        parts = [str(row[s]) for s in valid_sources if not pd.isna(row[s])] if exclude_nulls else \
                [str(row[s]) if not pd.isna(row[s]) else "" for s in valid_sources]
        return separator.join(parts) if parts else pd.NA

    df[target] = df.apply(concat_row, axis=1).astype("string")
    logger.debug(f"concat_columns: {valid_sources} → '{target}'")
    return df


def _handle_string_template(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Fill a target column using a format string referencing other columns.

    op parameters:
        template: str — e.g. "{first_name} {last_name}"
        target: str
    """
    template: str = op.get("template", "")
    target = op["target"]

    if not template:
        return _handle_set_null(df, op)

    def apply_template(row):
        try:
            return template.format(**{col: (str(row[col]) if not pd.isna(row[col]) else "") for col in df.columns if col in template})
        except (KeyError, ValueError):
            return pd.NA

    df[target] = df.apply(apply_template, axis=1).astype("string")
    return df


# ── Derive ops ────────────────────────────────────────────────────────


def _handle_extract_json_field(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Extract a single field from a JSON array or object with optional filter.

    op parameters:
        source: str
        key: str — field to extract
        filter: optional {field: value} to match items in a json_array
        target: str
        type: str — target type
    """
    source = op["source"]
    target = op["target"]
    key = op.get("key", "")
    filt: dict | None = op.get("filter")
    col_type = op.get("type", "string")

    if source not in df.columns:
        return _handle_set_null(df, op)

    def extract(v):
        if pd.isna(v):
            return None
        parsed = _try_parse(str(v))
        if parsed is None:
            return str(v)  # plain string fallback — return raw value for non-JSON rows
        if isinstance(parsed, dict):
            return parsed.get(key)
        if isinstance(parsed, list):
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                if filt:
                    fk, fv = next(iter(filt.items()))
                    if str(item.get(fk, "")).lower() != str(fv).lower():
                        continue
                if key in item:
                    return item[key]
        return None

    raw = df[source].apply(extract)
    if col_type in ("float", "integer", "int"):
        df[target] = pd.to_numeric(raw, errors="coerce")
        if col_type in ("integer", "int"):
            df[target] = df[target].astype("Int64")
    else:
        df[target] = raw.astype("string")

    if source != target and source in df.columns:
        df = df.drop(columns=[source])

    logger.debug(f"extract_json_field: '{source}'[{key}] → '{target}'")
    return df


def _handle_conditional_map(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Map source column values to target values via keyword lookup.

    op parameters:
        source: str
        target: str
        mapping: dict — {keyword: result_value}  (case-insensitive substring match)
        default: optional fallback value (default: null)
        type: target type
    """
    source = op["source"]
    target = op["target"]
    mapping: dict = op.get("mapping", {})
    default = op.get("default")
    col_type = op.get("type", "string")

    if source not in df.columns or not mapping:
        return _handle_set_null(df, op)

    lower_map = {k.lower(): v for k, v in mapping.items()}

    def lookup(v):
        if pd.isna(v):
            return default
        s = str(v).lower()
        for kw, result in lower_map.items():
            if kw in s:
                return result
        return default

    raw = df[source].apply(lookup)
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    try:
        df[target] = raw.astype(dtype)
    except Exception:
        df[target] = raw.astype("string")

    logger.debug(f"conditional_map: '{source}' → '{target}'")
    return df


def _handle_expression(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Arithmetic expression across numeric columns.

    op parameters:
        expression: str — e.g. "col_a * col_b / 100"
        target: str
        type: target type (default 'float')

    Only column names and basic arithmetic operators are allowed (no eval of
    arbitrary Python). Uses pandas eval() which is safe by design.
    """
    expr: str = op.get("expression", "")
    target = op["target"]
    col_type = op.get("type", "float")

    if not expr:
        return _handle_set_null(df, op)

    try:
        result = df.eval(expr)
        if col_type in ("integer", "int"):
            df[target] = pd.to_numeric(result, errors="coerce").astype("Int64")
        else:
            df[target] = pd.to_numeric(result, errors="coerce")
    except Exception as e:
        logger.warning(f"expression '{expr}' failed: {e} — falling back to set_null")
        return _handle_set_null(df, op)

    logger.debug(f"expression: '{expr}' → '{target}'")
    return df


def _handle_contains_flag(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """
    Boolean: True if source column string contains any of the keywords.

    op parameters:
        source: str
        target: str
        keywords: list[str] — case-insensitive substring match
    """
    source = op["source"]
    target = op["target"]
    keywords: list[str] = [k.lower() for k in op.get("keywords", [])]

    if source not in df.columns or not keywords:
        return _handle_set_null(df, op)

    pattern = "|".join(re.escape(k) for k in keywords)
    df[target] = (
        df[source]
        .astype("string")
        .str.lower()
        .str.contains(pattern, na=False)
        .astype("boolean")
    )
    logger.debug(f"contains_flag: '{source}' contains {keywords} → '{target}'")
    return df


# ── Helpers ───────────────────────────────────────────────────────────


def _cast_value(value: Any, col_type: str) -> Any:
    """Cast a raw default value to the appropriate Python type."""
    if col_type in ("float",):
        return float(value)
    if col_type in ("integer", "int"):
        return int(value)
    if col_type in ("boolean", "bool"):
        return str(value).lower() in ("true", "1", "yes")
    return str(value)


# ── Action handler registry ───────────────────────────────────────────

_ACTION_HANDLERS: dict[str, Any] = {
    # Scalar creation
    "set_null": _handle_set_null,
    "set_default": _handle_set_default,
    # Type ops
    "type_cast": _handle_type_cast,
    "rename": _handle_rename,
    "drop_column": _handle_drop_column,
    # Format ops (legacy dispatcher + direct names)
    "format_transform": _handle_format_transform,
    "parse_date": _handle_parse_date,
    "to_lowercase": _handle_to_lowercase,
    "to_uppercase": _handle_to_uppercase,
    "strip_whitespace": _handle_strip_whitespace,
    "regex_replace": _handle_regex_replace,
    "regex_extract": _handle_regex_extract,
    "truncate_string": _handle_truncate_string,
    "pad_string": _handle_pad_string,
    "value_map": _handle_value_map,
    # Split ops
    "json_array_extract_multi": _handle_json_array_extract_multi,
    "split_column": _handle_split_column,
    "xml_extract": _handle_xml_extract,
    # Unify ops
    "coalesce": _handle_coalesce,
    "concat_columns": _handle_concat_columns,
    "string_template": _handle_string_template,
    # Derive ops
    "extract_json_field": _handle_extract_json_field,
    "conditional_map": _handle_conditional_map,
    "expression": _handle_expression,
    "contains_flag": _handle_contains_flag,
}
