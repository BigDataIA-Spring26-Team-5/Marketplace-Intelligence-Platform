"""Tier 1 / Strategy 1: Deterministic enrichment — generic rule executor.

Accepts compiled rule objects from EnrichmentRulesLoader. Food-domain rules
(CATEGORY_RULES, DIETARY_RULES, ORGANIC_PATTERN) have moved to
domain_packs/nutrition/enrichment_rules.yaml.

dietary_tags, allergens, and is_organic are extraction-only fields.
They are never passed to Strategy 2 (KNN) or Strategy 3 (LLM).
If extraction fails here, the field stays null.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def deterministic_enrich(
    df: pd.DataFrame,
    enrich_cols: list[str],
    needs_enrichment: "pd.Series",
    rules: list | None = None,
) -> "tuple[pd.DataFrame, pd.Series, dict]":
    """Apply rule-based enrichment using compiled FieldRule objects.

    Args:
        df: Input DataFrame.
        enrich_cols: Columns to attempt enrichment on.
        needs_enrichment: Boolean Series — True = row still needs enrichment.
        rules: List of FieldRule from EnrichmentRulesLoader.s1_fields.
               When None, performs a no-op (returns df unchanged).

    Returns:
        (modified_df, updated_needs_enrichment_mask, stats).
    """
    if not rules:
        return df, needs_enrichment, {"resolved": 0}

    text_cols = ["product_name", "ingredients", "category"]
    existing_text_cols = [c for c in text_cols if c in df.columns]

    if not existing_text_cols:
        return df, needs_enrichment, {"resolved": 0}

    before_count = int(needs_enrichment.sum())
    combined_text = df[existing_text_cols].fillna("").astype(str).agg(" ".join, axis=1)

    for rule in rules:
        if rule.name not in enrich_cols or not rule.patterns:
            continue

        if rule.name not in df.columns:
            df[rule.name] = pd.NA

        output_type = getattr(rule, "output_type", "single")

        if output_type == "boolean":
            mask = needs_enrichment & df[rule.name].isna()
            for idx in df.index[mask]:
                text = combined_text.loc[idx]
                matched = any(p.pattern.search(text) for p in rule.patterns)
                df.at[idx, rule.name] = matched

        elif output_type == "multi":
            # dietary_tags-style: scan product_name + dedicated label columns only
            label_cols = ["product_name"]
            for opt_col in ["labels", f"{rule.name}_raw"]:
                if opt_col in df.columns:
                    label_cols.append(opt_col)
            label_text = df[label_cols].fillna("").astype(str).agg(" ".join, axis=1)

            mask = needs_enrichment & df[rule.name].isna()
            for idx in df.index[mask]:
                text = label_text.loc[idx]
                matched_labels = []
                for p in rule.patterns:
                    if p.pattern.search(text):
                        matched_labels.append(p.label)
                if matched_labels:
                    df.at[idx, rule.name] = ", ".join(matched_labels)
                else:
                    df.at[idx, rule.name] = ""

        else:  # single — first-match wins (category-style)
            mask = needs_enrichment & df[rule.name].isna()
            for idx in df.index[mask]:
                text = combined_text.loc[idx]
                for p in rule.patterns:
                    if p.pattern.search(text):
                        df.at[idx, rule.name] = p.label
                        break

    needs_enrichment = df[enrich_cols].isna().any(axis=1)
    resolved = before_count - int(needs_enrichment.sum())
    return df, needs_enrichment, {"resolved": resolved}
