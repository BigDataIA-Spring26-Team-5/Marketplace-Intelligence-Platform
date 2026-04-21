"""Strategy 2: KNN corpus search — product-to-product embedding comparison.

Replaces the old label-string similarity approach. Instead of comparing row
text against category label words like "Dairy", this strategy compares the
row's embedding against embeddings of already-labeled product rows stored in
a persistent FAISS index (the reference corpus).
"""

from __future__ import annotations

import json
import logging

import pandas as pd

from src.enrichment.corpus import (
    CONFIDENCE_THRESHOLD_CATEGORY,
    add_to_corpus,
    build_seed_corpus,
    knn_search_batch,
    load_corpus,
    save_corpus,
)

logger = logging.getLogger(__name__)


def embedding_enrich(
    df: pd.DataFrame,
    enrich_cols: list[str],
    needs_enrichment: pd.Series,
    cache_client=None,
) -> tuple[pd.DataFrame, pd.Series, dict]:
    """
    Use KNN corpus search to assign primary_category to unmatched rows.

    For each row still needing enrichment, queries the FAISS corpus of
    labeled product embeddings. If the top-K neighbors vote a category with
    sufficient confidence, assigns it and adds the row to the corpus.

    Stores top-3 neighbors in a pipeline-internal "_knn_neighbors" column
    (JSON string) for use by Strategy 3's RAG prompt. This column must be
    dropped by the orchestrator before final output.

    Returns (modified_df, updated_needs_enrichment_mask, stats).
    """
    if "primary_category" not in enrich_cols:
        return df, needs_enrichment, {"resolved": 0}

    mask = needs_enrichment & df["primary_category"].isna()
    if not mask.any():
        return df, needs_enrichment, {"resolved": 0}

    # Ensure the internal neighbor column exists
    if "_knn_neighbors" not in df.columns:
        df["_knn_neighbors"] = None

    # Load or seed the corpus
    index, metadata = load_corpus()

    if index is None or index.ntotal < 10:
        logger.info("S2 KNN: corpus empty or too small, seeding from S1-resolved rows")
        try:
            build_seed_corpus(df)
            index, metadata = load_corpus()
        except ImportError:
            logger.warning("S2 KNN: faiss not installed, skipping Strategy 2")
            return df, needs_enrichment, {"resolved": 0}
        except Exception as e:
            logger.warning(f"S2 KNN: corpus seed failed: {e}")
            return df, needs_enrichment, {"resolved": 0}

    if index is None or index.ntotal < 10:
        logger.info("S2 KNN: corpus still too small after seeding, skipping to S3")
        return df, needs_enrichment, {"resolved": 0}

    resolved = 0
    unresolved_indices = list(df.index[mask])
    unresolved_rows = [df.loc[idx] for idx in unresolved_indices]

    try:
        batch_results = knn_search_batch(unresolved_rows, index, metadata, cache_client=cache_client)
    except ImportError:
        logger.warning("S2 KNN: faiss not installed, skipping Strategy 2")
        needs_enrichment = df[enrich_cols].isna().any(axis=1)
        return df, needs_enrichment, {"resolved": 0}
    except Exception as e:
        logger.warning(f"S2 KNN: batch search failed: {e}")
        batch_results = [(None, 0.0, []) for _ in unresolved_indices]

    for idx, row, (category, confidence, neighbors) in zip(unresolved_indices, unresolved_rows, batch_results):
        df.at[idx, "_knn_neighbors"] = json.dumps(neighbors)
        if category is not None and confidence >= CONFIDENCE_THRESHOLD_CATEGORY:
            df.at[idx, "primary_category"] = category
            add_to_corpus(row, category, index, metadata)
            resolved += 1

    if resolved > 0:
        try:
            save_corpus(index, metadata)
        except Exception as e:
            logger.warning(f"S2 KNN: could not save corpus: {e}")

    logger.info(f"S2 KNN: resolved {resolved} rows")
    needs_enrichment = df[enrich_cols].isna().any(axis=1)
    return df, needs_enrichment, {"resolved": resolved}
