"""SchemaEnforceBlock — enforce unified schema as final silver step.

Reads column specs and types directly from the UnifiedSchema Pydantic model
so that any change to config/unified_schema.json is automatically reflected
without touching this file.

Silver scope: all unified schema columns EXCEPT dq_score_post and dq_delta
(those are computed in the gold layer).
"""

from __future__ import annotations

import logging

import pandas as pd

from src.blocks.base import Block

logger = logging.getLogger(__name__)

# Gold-only computed columns — never written to silver
_GOLD_ONLY = {"dq_score_post", "dq_delta"}

# Pydantic ColumnSpec.type → pandas nullable dtype
_TYPE_TO_DTYPE: dict[str, str] = {
    "string":  "string",
    "float":   "Float64",
    "integer": "Int64",
    "boolean": "boolean",
}


def _silver_columns_from_schema(unified_schema) -> list[tuple[str, str]]:
    """Return [(col_name, pandas_dtype)] for all silver-layer columns."""
    result = []
    for col, spec in unified_schema.columns.items():
        if col in _GOLD_ONLY:
            continue
        dtype = _TYPE_TO_DTYPE.get(spec.type, "string")
        result.append((col, dtype))
    return result


class SchemaEnforceBlock(Block):
    name = "schema_enforce"
    domain = "all"
    description = (
        "Drop extra columns, fill missing unified schema columns with typed nulls, "
        "and cast existing columns to their declared types. Driven by UnifiedSchema Pydantic model."
    )
    inputs = ["all columns"]
    outputs = []  # dynamic — set at run time from schema

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        from src.schema.analyzer import get_unified_schema

        unified_schema = (config or {}).get("unified_schema") or get_unified_schema()
        silver_cols = _silver_columns_from_schema(unified_schema)
        col_names = [c for c, _ in silver_cols]

        df = df.copy()

        # Drop columns not in unified schema
        extra = [c for c in df.columns if c not in col_names]
        if extra:
            df = df.drop(columns=extra)
            logger.info("schema_enforce: dropped %d extra column(s): %s", len(extra), extra)

        # Add missing columns as typed nulls; cast existing to declared type
        for col, dtype in silver_cols:
            if col not in df.columns:
                df[col] = pd.array([pd.NA] * len(df), dtype=dtype)
                logger.info("schema_enforce: added missing column '%s' as %s", col, dtype)
            else:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as exc:
                    logger.warning(
                        "schema_enforce: could not cast '%s' to %s — leaving as-is (%s)",
                        col, dtype, exc,
                    )

        # Return columns in schema-defined order
        return df[col_names]
