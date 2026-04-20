"""
Reference corpus for KNN-based primary_category enrichment.

Manages a persistent FAISS index of (embedding, category) pairs.
The corpus is seeded on first use and grows as new rows are enriched
with high-confidence category assignments.

Index is stored at: corpus/faiss_index.bin
Metadata is stored at: corpus/corpus_metadata.json
(both relative to project root, created automatically)
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CORPUS_DIR = Path("corpus")
INDEX_PATH = CORPUS_DIR / "faiss_index.bin"
META_PATH = CORPUS_DIR / "corpus_metadata.json"

# Minimum similarity for a KNN result to count as a vote
VOTE_SIMILARITY_THRESHOLD = 0.45

# Minimum average similarity of top-K neighbors to accept without escalation
CONFIDENCE_THRESHOLD_CATEGORY = 0.60

# Number of neighbors to retrieve
K_NEIGHBORS = 5

# Minimum corpus size before KNN is attempted (below this, skip to S3)
MIN_CORPUS_SIZE = 10


def _get_model():
    """Lazy-load sentence transformer. Cached globally."""
    global _MODEL
    try:
        if _MODEL is None:
            from sentence_transformers import SentenceTransformer
            _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    except Exception as e:
        logger.error(f"Failed to load sentence transformer: {e}")
        _MODEL = None
    return _MODEL

_MODEL = None


def _build_row_text(row: pd.Series) -> str:
    """Build a single query string from available product fields."""
    parts = []
    for col in ["product_name", "brand_name", "ingredients", "category"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    return " ".join(parts)


def load_corpus() -> tuple[Optional[object], list[dict]]:
    """
    Load the FAISS index and metadata from disk.
    Returns (index, metadata_list) or (None, []) if not found.
    metadata_list entries: {"category": str, "product_name": str}
    """
    try:
        import faiss
        if INDEX_PATH.exists() and META_PATH.exists():
            index = faiss.read_index(str(INDEX_PATH))
            with open(META_PATH) as f:
                metadata = json.load(f)
            logger.info(f"Loaded corpus: {index.ntotal} vectors")
            return index, metadata
    except Exception as e:
        logger.warning(f"Could not load corpus: {e}")
    return None, []


def save_corpus(index, metadata: list[dict]) -> None:
    """Persist the FAISS index and metadata to disk."""
    try:
        import faiss
        CORPUS_DIR.mkdir(exist_ok=True)
        faiss.write_index(index, str(INDEX_PATH))
        with open(META_PATH, "w") as f:
            json.dump(metadata, f)
        logger.info(f"Saved corpus: {index.ntotal} vectors")
    except Exception as e:
        logger.warning(f"Could not save corpus: {e}")


def build_seed_corpus(df: pd.DataFrame) -> None:
    """
    Seed the corpus from rows in df that already have primary_category.
    Called once when the corpus is empty. Uses S1-resolved rows as the seed.
    If fewer than MIN_CORPUS_SIZE rows have a category, logs a warning and skips.
    """
    import faiss

    model = _get_model()
    if model is None:
        return

    labeled = df[df["primary_category"].notna()].copy()
    if len(labeled) < MIN_CORPUS_SIZE:
        logger.warning(
            f"Corpus seed skipped: only {len(labeled)} labeled rows "
            f"(need {MIN_CORPUS_SIZE})"
        )
        return

    texts = labeled.apply(_build_row_text, axis=1).tolist()
    categories = labeled["primary_category"].tolist()
    product_names = labeled.get("product_name", pd.Series([""] * len(labeled))).tolist()

    embeddings = model.encode(texts, batch_size=64, show_progress_bar=False)
    embeddings = embeddings.astype(np.float32)

    # L2-normalize for cosine similarity via inner product
    faiss.normalize_L2(embeddings)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)  # Inner product on normalized = cosine
    index.add(embeddings)

    metadata = [
        {"category": cat, "product_name": pn}
        for cat, pn in zip(categories, product_names)
    ]

    save_corpus(index, metadata)
    logger.info(f"Corpus seeded with {len(metadata)} labeled rows")


def _score_from_search_result(
    similarities: np.ndarray,
    indices: np.ndarray,
    metadata: list[dict],
) -> tuple[Optional[str], float, list[dict]]:
    """Shared scoring logic: votes neighbors, returns (category, confidence, top-3)."""
    neighbors = []
    for sim, idx in zip(similarities, indices):
        if idx < 0:
            continue
        neighbors.append({
            "category": metadata[idx]["category"],
            "product_name": metadata[idx]["product_name"],
            "similarity": float(sim),
        })

    votes: dict[str, float] = {}
    for n in neighbors:
        if n["similarity"] >= VOTE_SIMILARITY_THRESHOLD:
            votes[n["category"]] = votes.get(n["category"], 0.0) + n["similarity"]

    if not votes:
        return None, 0.0, neighbors[:3]

    best_category = max(votes, key=lambda c: votes[c])
    winner_sims = [
        n["similarity"]
        for n in neighbors
        if n["category"] == best_category
        and n["similarity"] >= VOTE_SIMILARITY_THRESHOLD
    ]
    confidence = sum(winner_sims) / len(winner_sims) if winner_sims else 0.0

    if confidence < CONFIDENCE_THRESHOLD_CATEGORY:
        return None, confidence, neighbors[:3]

    return best_category, confidence, neighbors[:3]


def knn_search(
    row: pd.Series,
    index,
    metadata: list[dict],
    k: int = K_NEIGHBORS,
) -> tuple[Optional[str], float, list[dict]]:
    """
    Find K nearest neighbors in the corpus for the given row.

    Returns:
        (category, confidence, neighbors) where:
        - category: the majority-voted category string, or None if below threshold
        - confidence: average cosine similarity of votes that passed VOTE_SIMILARITY_THRESHOLD
        - neighbors: list of {"category", "product_name", "similarity"} dicts
                     (top-3 for use in S3 RAG prompt)
    """
    model = _get_model()
    if model is None or index is None or index.ntotal < MIN_CORPUS_SIZE:
        return None, 0.0, []

    import faiss

    text = _build_row_text(row)
    if not text.strip():
        return None, 0.0, []

    embedding = model.encode([text], show_progress_bar=False).astype(np.float32)
    faiss.normalize_L2(embedding)

    k_actual = min(k, index.ntotal)
    similarities, indices = index.search(embedding, k_actual)

    return _score_from_search_result(similarities[0], indices[0], metadata)


def knn_search_batch(
    rows: list[pd.Series],
    index,
    metadata: list[dict],
    k: int = K_NEIGHBORS,
) -> list[tuple[Optional[str], float, list[dict]]]:
    """
    Batch KNN search: one model.encode() call for all rows.

    Returns list of (category, confidence, neighbors) tuples, one per input row.
    Rows with empty text return (None, 0.0, []).
    """
    model = _get_model()
    if model is None or index is None or index.ntotal < MIN_CORPUS_SIZE:
        return [(None, 0.0, []) for _ in rows]

    import faiss

    texts = [_build_row_text(row) for row in rows]
    valid_mask = [bool(t.strip()) for t in texts]

    if not any(valid_mask):
        return [(None, 0.0, []) for _ in rows]

    valid_texts = [t for t, v in zip(texts, valid_mask) if v]
    embeddings = model.encode(valid_texts, batch_size=64, show_progress_bar=False).astype(np.float32)
    faiss.normalize_L2(embeddings)

    k_actual = min(k, index.ntotal)
    all_similarities, all_indices = index.search(embeddings, k_actual)

    results: list[tuple[Optional[str], float, list[dict]]] = []
    valid_iter = iter(zip(all_similarities, all_indices))
    for is_valid in valid_mask:
        if is_valid:
            sims, idxs = next(valid_iter)
            results.append(_score_from_search_result(sims, idxs, metadata))
        else:
            results.append((None, 0.0, []))

    return results


def add_to_corpus(
    row: pd.Series,
    category: str,
    index,
    metadata: list[dict],
) -> None:
    """
    Add a newly enriched row to the in-memory FAISS index and metadata list.
    The caller is responsible for calling save_corpus() after a batch.
    Modifies index and metadata in place.
    """
    model = _get_model()
    if model is None or index is None:
        return

    import faiss

    text = _build_row_text(row)
    if not text.strip():
        return

    embedding = model.encode([text], show_progress_bar=False).astype(np.float32)
    faiss.normalize_L2(embedding)
    index.add(embedding)
    metadata.append({
        "category": category,
        "product_name": str(row.get("product_name", "")),
    })
