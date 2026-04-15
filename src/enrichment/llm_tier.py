"""Strategy 3: RAG-augmented LLM — primary_category assignment only.

Uses top-3 KNN neighbors from Strategy 2 as RAG context injected into the
LLM prompt. The model does analogy completion anchored to real examples rather
than cold inference from a sparse row. This dramatically reduces hallucination.

allergens, is_organic, and dietary_tags are NEVER passed to this strategy.
Those fields are handled exclusively by Strategy 1 (deterministic extraction).
If S1 extraction fails, those fields stay null — they are not inferred here.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

from src.models.llm import call_llm_json, get_enrichment_llm
from src.enrichment.corpus import add_to_corpus, load_corpus, save_corpus

logger = logging.getLogger(__name__)

CATEGORIES = (
    "Breakfast Cereals, Dairy, Meat & Poultry, Seafood, Bakery, "
    "Confectionery, Snacks, Beverages, Condiments, Frozen Foods, Fruits, "
    "Vegetables, Pasta & Grains, Soups, Baby Food, Supplements, "
    "Canned Foods, Deli, Pet Food, Other"
)

SYSTEM_PROMPT = (
    "You are a product categorization assistant. Assign exactly one category "
    "from the list below. Return ONLY a JSON object with one key: "
    '{{"primary_category": "<category>"}}. If you cannot determine the category '
    'with confidence, return {{"primary_category": null}}.\n\n'
    f"CATEGORIES: {CATEGORIES}"
)


def _build_rag_prompt(row: pd.Series, neighbors: list[dict]) -> str:
    """Build a RAG-augmented prompt for primary_category assignment."""
    lines = []

    if neighbors:
        lines.append("Similar products already categorized:")
        for n in neighbors:
            sim = n.get("similarity", 0.0)
            pname = n.get("product_name", "")
            cat = n.get("category", "")
            lines.append(f"  - {pname} → {cat} (similarity: {sim:.2f})")
        lines.append("")

    lines.append("Product to categorize:")
    for field, label in [
        ("product_name", "Name"),
        ("brand_name", "Brand"),
        ("ingredients", "Ingredients"),
        ("category", "Source category"),
    ]:
        val = row.get(field)
        if pd.notna(val) and str(val).strip():
            lines.append(f"  {label}: {val}")

    lines.append("")
    lines.append("What is the primary_category?")
    return "\n".join(lines)


def llm_enrich(
    df: pd.DataFrame,
    enrich_cols: list[str],
    needs_enrichment: pd.Series,
) -> tuple[pd.DataFrame, pd.Series, dict]:
    """
    Call LLM with RAG context for rows where primary_category is still null.

    Only operates on primary_category. allergens, is_organic, and dietary_tags
    are never sent to the LLM — they are extraction-only fields handled by S1.

    Returns (modified_df, updated_needs_enrichment_mask, stats).
    """
    if "primary_category" not in enrich_cols:
        return df, needs_enrichment, {"resolved": 0}

    mask = needs_enrichment & df["primary_category"].isna()
    if not mask.any():
        return df, needs_enrichment, {"resolved": 0}

    model = get_enrichment_llm()
    rows_to_enrich = df.index[mask].tolist()
    logger.info(f"S3 RAG-LLM: {len(rows_to_enrich)} rows need primary_category")

    # Load corpus for feedback loop additions
    index, metadata = load_corpus()
    corpus_updated = False
    resolved = 0

    for idx in rows_to_enrich:
        row = df.loc[idx]

        # Parse KNN neighbors from S2 if available
        neighbors = []
        raw_neighbors = row.get("_knn_neighbors")
        if pd.notna(raw_neighbors) and raw_neighbors:
            try:
                neighbors = json.loads(raw_neighbors)
            except (json.JSONDecodeError, TypeError):
                neighbors = []

        prompt = _build_rag_prompt(row, neighbors)

        try:
            result = call_llm_json(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )

            category = result.get("primary_category") if isinstance(result, dict) else None
            if category is not None:
                df.at[idx, "primary_category"] = category
                resolved += 1
                # Corpus feedback loop: add high-confidence S3 results
                if index is not None:
                    add_to_corpus(row, category, index, metadata)
                    corpus_updated = True

        except Exception as e:
            logger.warning(f"S3 RAG-LLM: enrichment failed for row {idx}: {e}")

    if corpus_updated:
        try:
            save_corpus(index, metadata)
        except Exception as e:
            logger.warning(f"S3 RAG-LLM: could not save corpus: {e}")

    needs_enrichment = df[enrich_cols].isna().any(axis=1)
    return df, needs_enrichment, {"resolved": resolved}
