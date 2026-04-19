"""Select canonical golden record per cluster using DQ composite score."""

from __future__ import annotations

import pandas as pd
from src.blocks.base import Block


class GoldenRecordSelectBlock(Block):
    name = "golden_record_select"
    domain = "all"
    description = "Select the best row per duplicate cluster using a composite DQ score"
    inputs = ["duplicate_group_id", "published_date", "ingredients"]
    outputs = ["golden records (one per cluster)"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        if "duplicate_group_id" not in df.columns:
            return df

        config = config or {}
        completeness_w = config.get("completeness_weight", 0.4)
        freshness_w = config.get("freshness_weight", 0.35)
        richness_w = config.get("richness_weight", 0.25)

        df = df.copy()

        # Completeness: fraction of non-null columns
        non_computed = [
            c for c in df.columns
            if c not in ("dq_score_pre", "dq_score_post", "dq_delta",
                         "duplicate_group_id", "canonical")
        ]
        df["_completeness"] = df[non_computed].notna().mean(axis=1)

        # Freshness: if published_date exists, normalize to 0-1
        if "published_date" in df.columns:
            dates = pd.to_datetime(df["published_date"], errors="coerce")
            if dates.notna().any():
                min_d, max_d = dates.min(), dates.max()
                if min_d != max_d:
                    df["_freshness"] = (dates - min_d) / (max_d - min_d)
                else:
                    df["_freshness"] = 1.0
            else:
                df["_freshness"] = 0.5
        else:
            df["_freshness"] = 0.5

        # Ingredient richness: length of ingredients field normalized
        if "ingredients" in df.columns:
            lengths = df["ingredients"].fillna("").astype(str).str.len()
            max_len = lengths.max()
            df["_richness"] = lengths / max_len if max_len > 0 else 0
        else:
            df["_richness"] = 0

        df["_golden_score"] = (
            df["_completeness"] * completeness_w
            + df["_freshness"] * freshness_w
            + df["_richness"] * richness_w
        )

        # Select the row with highest golden score per group
        best_idx = df.groupby("duplicate_group_id")["_golden_score"].idxmax()
        result = df.loc[best_idx].copy()

        # Cleanup temp columns
        result.drop(columns=["_completeness", "_freshness", "_richness", "_golden_score"],
                     inplace=True, errors="ignore")
        return result.reset_index(drop=True)
