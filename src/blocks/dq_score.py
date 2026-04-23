"""Data quality scoring — pre and post enrichment."""

from __future__ import annotations

import logging

import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)

# Columns that are never data quality indicators — always excluded from completeness
_SKIP_ALWAYS = {
    "dq_score_pre", "dq_score_post", "dq_delta",
    "duplicate_group_id", "canonical",
    "enriched_by_llm",  # pipeline-internal enrichment tag
    "sizes",            # extracted from product_name, not a source-level completeness indicator
}


def compute_dq_score(
    df: pd.DataFrame,
    weights: dict | None = None,
    reference_columns: list[str] | None = None,
) -> pd.Series:
    """
    Compute a DQ score per row.

    Score = Completeness * w1 + Freshness * w2 + IngredientRichness * w3

    reference_columns: if provided, compute completeness over this fixed column set
    (used by dq_score_post to match the column set from dq_score_pre for a fair delta).
    """
    weights = weights or {"completeness": 0.4, "freshness": 0.35, "ingredient_richness": 0.25}

    if reference_columns is not None:
        # Fixed column set for fair pre/post comparison
        data_cols = [c for c in reference_columns if c in df.columns and c not in _SKIP_ALWAYS]
    else:
        data_cols = [c for c in df.columns if c not in _SKIP_ALWAYS]

    # Completeness: fraction of non-null values
    if not data_cols:
        completeness = pd.Series(0.0, index=df.index)
    else:
        completeness = df[data_cols].notna().mean(axis=1)

    # Freshness: absolute age from today (0 = 2+ years old, 1 = today)
    if "published_date" in df.columns:
        dates = pd.to_datetime(df["published_date"], errors="coerce")
        if dates.notna().any():
            today = pd.Timestamp("today", tz=None).normalize()
            age_days = (today - dates.dt.tz_localize(None)).dt.days.clip(lower=0)
            freshness = (1 - (age_days / 730)).clip(0, 1).fillna(0.5)
        else:
            freshness = 0.5
    else:
        freshness = 0.5

    # Ingredient richness
    if "ingredients" in df.columns:
        lengths = df["ingredients"].fillna("").astype(str).str.len()
        max_len = lengths.max()
        richness = lengths / max_len if max_len > 0 else pd.Series(0.0, index=df.index)
    else:
        richness = pd.Series(0.0, index=df.index)

    score = (
        completeness * weights["completeness"]
        + freshness * weights["freshness"]
        + richness * weights["ingredient_richness"]
    )
    return (score * 100).round(2)


class DQScorePreBlock(Block):
    name = "dq_score_pre"
    domain = "all"
    description = "Compute initial per-row data quality score before enrichment"
    inputs = ["all columns"]
    outputs = ["dq_score_pre"]

    # Class-level stash: survives pd.concat / reset_index / merge that drop df.attrs
    _last_reference_columns: list[str] = []

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        weights = (config or {}).get("dq_weights")
        df["dq_score_pre"] = compute_dq_score(df, weights)
        ref_cols = [c for c in df.columns if c not in _SKIP_ALWAYS]
        df.attrs["dq_reference_columns"] = ref_cols
        DQScorePreBlock._last_reference_columns = ref_cols
        mean_score = df["dq_score_pre"].mean()
        logger.info(f"DQ Score (pre): mean={mean_score:.1f}%, min={df['dq_score_pre'].min():.1f}%, max={df['dq_score_pre'].max():.1f}%, cols={len(ref_cols)}")
        return df


class DQScorePostBlock(Block):
    name = "dq_score_post"
    domain = "all"
    description = "Compute final per-row data quality score after enrichment and compute dq_delta"
    inputs = ["all columns", "dq_score_pre"]
    outputs = ["dq_score_post", "dq_delta"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        weights = (config or {}).get("dq_weights")
        reference_columns = df.attrs.get("dq_reference_columns") or DQScorePreBlock._last_reference_columns or None
        attrs_lost = not df.attrs.get("dq_reference_columns") and bool(DQScorePreBlock._last_reference_columns)
        if attrs_lost:
            logger.warning("dq_score_post: df.attrs lost between pre/post — falling back to class-level reference columns")
        df["dq_score_post"] = compute_dq_score(df, weights, reference_columns=reference_columns)
        if "dq_score_pre" in df.columns:
            df["dq_delta"] = (df["dq_score_post"] - df["dq_score_pre"]).round(2)
            mean_delta = df["dq_delta"].mean()
            ref_count = len(reference_columns) if reference_columns else len(df.columns)
            logger.info(f"DQ Score (post): mean={df['dq_score_post'].mean():.1f}%, delta={mean_delta:+.1f}%, cols={ref_count}")
        return df
