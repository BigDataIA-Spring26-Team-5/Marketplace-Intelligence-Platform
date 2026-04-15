"""Column-wise merge across duplicate clusters — best value per field."""

from __future__ import annotations

import logging

import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)


class ColumnWiseMergeBlock(Block):
    name = "column_wise_merge"
    domain = "all"
    description = "Merge duplicate clusters column-wise, picking the most complete value per field"
    inputs = ["duplicate_group_id", "all columns"]
    outputs = ["merged rows (one per cluster)"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        if "duplicate_group_id" not in df.columns:
            return df

        df = df.copy()

        def pick_best(series: pd.Series) -> object:
            """Pick the most complete (longest non-null) value from a group."""
            non_null = series.dropna()
            if non_null.empty:
                return pd.NA
            # For strings, prefer the longest
            if series.dtype == object:
                str_vals = non_null.astype(str)
                return str_vals.loc[str_vals.str.len().idxmax()]
            # For numerics, prefer the first non-null
            return non_null.iloc[0]

        # Group by duplicate cluster and merge column-wise
        merged = df.groupby("duplicate_group_id", as_index=False).agg(
            {col: pick_best for col in df.columns if col != "duplicate_group_id"}
        )
        merged.attrs = df.attrs.copy()
        logger.info(f"Column-wise merge: {len(df)} rows → {len(merged)} merged rows")
        return merged
