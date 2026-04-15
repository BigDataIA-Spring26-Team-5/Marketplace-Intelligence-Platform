"""Tier 1 / Strategy 1: Deterministic enrichment — regex, keyword scan, rule-based classification.

dietary_tags, allergens, and is_organic are extraction-only fields.
They are never passed to Strategy 2 (KNN) or Strategy 3 (LLM).
If extraction fails here, the field stays null.
"""

from __future__ import annotations

import re

import pandas as pd

# Category classification rules based on keywords
CATEGORY_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(cereal|oat|granola|muesli)\b", re.I), "Breakfast Cereals"),
    (re.compile(r"\b(milk|cream|yogurt|cheese|butter|dairy)\b", re.I), "Dairy"),
    (re.compile(r"\b(chicken|beef|pork|turkey|meat|sausage|bacon)\b", re.I), "Meat & Poultry"),
    (re.compile(r"\b(fish|salmon|tuna|shrimp|seafood|cod)\b", re.I), "Seafood"),
    (re.compile(r"\b(bread|bagel|muffin|croissant|baguette|roll)\b", re.I), "Bakery"),
    (re.compile(r"\b(candy|chocolate|gummy|sweet|confection)\b", re.I), "Confectionery"),
    (re.compile(r"\b(chip|pretzel|popcorn|cracker|snack)\b", re.I), "Snacks"),
    (re.compile(r"\b(juice|soda|water|tea|coffee|beverage|drink)\b", re.I), "Beverages"),
    (re.compile(r"\b(sauce|ketchup|mustard|dressing|condiment|mayo)\b", re.I), "Condiments"),
    (re.compile(r"\b(frozen|ice cream|popsicle)\b", re.I), "Frozen Foods"),
    (re.compile(r"\b(fruit|apple|banana|berry|orange|grape)\b", re.I), "Fruits"),
    (re.compile(r"\b(vegetable|carrot|broccoli|spinach|lettuce|tomato)\b", re.I), "Vegetables"),
    (re.compile(r"\b(pasta|noodle|spaghetti|macaroni)\b", re.I), "Pasta & Grains"),
    (re.compile(r"\b(rice|quinoa|couscous)\b", re.I), "Pasta & Grains"),
    (re.compile(r"\b(soup|stew|broth|chili)\b", re.I), "Soups"),
    (re.compile(r"\b(baby|infant|toddler)\b", re.I), "Baby Food"),
    (re.compile(r"\b(organic)\b", re.I), "Organic"),
    (re.compile(r"\b(supplement|vitamin|mineral|protein powder)\b", re.I), "Supplements"),
]

# Dietary tags rules — matched only against explicit label claims in product_name
# and dedicated label columns. Ingredients are NOT scanned for dietary tags.
DIETARY_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(gluten[\s-]?free)\b", re.I), "gluten-free"),
    (re.compile(r"\b(vegan)\b", re.I), "vegan"),
    (re.compile(r"\b(vegetarian)\b", re.I), "vegetarian"),
    (re.compile(r"\b(kosher)\b", re.I), "kosher"),
    (re.compile(r"\b(halal)\b", re.I), "halal"),
    (re.compile(r"\b(sugar[\s-]?free)\b", re.I), "sugar-free"),
    (re.compile(r"\b(low[\s-]?fat)\b", re.I), "low-fat"),
    (re.compile(r"\b(non[\s-]?gmo)\b", re.I), "non-gmo"),
    (re.compile(r"\b(keto)\b", re.I), "keto"),
]

ORGANIC_PATTERN = re.compile(r"\b(organic|usda\s+organic)\b", re.I)


def deterministic_enrich(
    df: pd.DataFrame,
    enrich_cols: list[str],
    needs_enrichment: pd.Series,
) -> tuple[pd.DataFrame, pd.Series, dict]:
    """
    Apply rule-based enrichment.

    Returns (modified_df, updated_needs_enrichment_mask, stats).
    """
    # Build a text field to scan against (for primary_category, is_organic, allergens)
    text_cols = ["product_name", "ingredients", "category"]
    existing_text_cols = [c for c in text_cols if c in df.columns]

    if not existing_text_cols:
        return df, needs_enrichment, {"resolved": 0}

    before_count = int(needs_enrichment.sum())

    combined_text = df[existing_text_cols].fillna("").astype(str).agg(" ".join, axis=1)

    # Primary category
    if "primary_category" in enrich_cols:
        mask = needs_enrichment & df["primary_category"].isna()
        for idx in df.index[mask]:
            text = combined_text.loc[idx]
            for pattern, category in CATEGORY_RULES:
                if pattern.search(text):
                    df.at[idx, "primary_category"] = category
                    break

    # Dietary tags — only scan product_name and dedicated label columns.
    # Ingredients are NOT scanned: dietary tags must be explicit label claims,
    # not inferred from ingredient content.
    if "dietary_tags" in enrich_cols:
        label_cols = ["product_name"]
        for opt_col in ["labels", "dietary_tags_raw"]:
            if opt_col in df.columns:
                label_cols.append(opt_col)
        label_text = df[label_cols].fillna("").astype(str).agg(" ".join, axis=1)

        mask = needs_enrichment & df["dietary_tags"].isna()
        for idx in df.index[mask]:
            text = label_text.loc[idx]
            tags = []
            for pattern, tag in DIETARY_RULES:
                if pattern.search(text):
                    tags.append(tag)
            if tags:
                df.at[idx, "dietary_tags"] = ", ".join(tags)
            else:
                df.at[idx, "dietary_tags"] = ""

    # is_organic
    if "is_organic" in enrich_cols:
        mask = needs_enrichment & df["is_organic"].isna()
        for idx in df.index[mask]:
            text = combined_text.loc[idx]
            df.at[idx, "is_organic"] = bool(ORGANIC_PATTERN.search(text))

    # Recalculate needs_enrichment
    needs_enrichment = df[enrich_cols].isna().any(axis=1)
    resolved = before_count - int(needs_enrichment.sum())
    return df, needs_enrichment, {"resolved": resolved}
