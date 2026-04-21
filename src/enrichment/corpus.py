"""
Reference corpus for KNN-based primary_category enrichment.

Backed by ChromaDB (HTTP client at localhost:8000). Persists automatically —
no file-based index management. Public API unchanged from FAISS version so
embedding.py and llm_tier.py call sites need no modification.
"""

import hashlib
import json
import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Minimum similarity for a KNN result to count as a vote
VOTE_SIMILARITY_THRESHOLD = 0.45

# Minimum average similarity of top-K neighbors to accept without escalation
CONFIDENCE_THRESHOLD_CATEGORY = 0.60

# Number of neighbors to retrieve
K_NEIGHBORS = 5

# Minimum corpus size before KNN is attempted (below this, skip to S3)
MIN_CORPUS_SIZE = 10

_COLLECTION_NAME = "product_corpus"

_MODEL = None
_MODEL_NAME = "all-MiniLM-L6-v2"


def _get_model():
    """Lazy-load sentence transformer. Cached globally."""
    global _MODEL
    try:
        if _MODEL is None:
            from sentence_transformers import SentenceTransformer
            _MODEL = SentenceTransformer(_MODEL_NAME)
    except Exception as e:
        logger.error(f"Failed to load sentence transformer: {e}")
        _MODEL = None
    return _MODEL


def _get_collection():
    """Get or create the ChromaDB product corpus collection."""
    import chromadb
    client = chromadb.HttpClient(
        host=os.environ.get("CHROMA_HOST", "localhost"),
        port=int(os.environ.get("CHROMA_PORT", "8000")),
    )
    return client.get_or_create_collection(
        _COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def _compute_embedding_key(model_name: str, text: str) -> str:
    """SHA-256-16 of (model_name, text) — for Redis embedding cache."""
    raw = json.dumps({"model": model_name, "text": text})
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _make_vector_id(text: str, category: str) -> str:
    """Stable ID for a (text, category) pair — enables upsert deduplication."""
    return hashlib.sha256(f"{text.strip()}{category}".encode()).hexdigest()[:16]


def _build_row_text(row: pd.Series) -> str:
    """Build a single query string from available product fields."""
    parts = []
    for col in ["product_name", "brand_name", "ingredients", "category"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    return " ".join(parts)


def _score_from_neighbors(
    neighbors: list[dict],
) -> tuple[Optional[str], float, list[dict]]:
    """
    Shared scoring logic: vote over neighbors, return (category, confidence, top-3).
    neighbors: list of {"category", "product_name", "similarity"}
    """
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
        if n["category"] == best_category and n["similarity"] >= VOTE_SIMILARITY_THRESHOLD
    ]
    confidence = sum(winner_sims) / len(winner_sims) if winner_sims else 0.0

    if confidence < CONFIDENCE_THRESHOLD_CATEGORY:
        return None, confidence, neighbors[:3]

    return best_category, confidence, neighbors[:3]


def load_corpus() -> tuple[Optional[object], list[dict]]:
    """
    Load the ChromaDB product corpus collection.
    Returns (collection, []) — metadata list is unused (ChromaDB stores its own).
    Returns (None, []) if ChromaDB is unreachable.
    """
    try:
        collection = _get_collection()
        n = collection.count()
        logger.info(f"Loaded corpus: {n} vectors")
        return collection, []
    except Exception as e:
        logger.warning(f"Could not connect to ChromaDB corpus: {e}")
        return None, []


def save_corpus(index, metadata: list[dict]) -> None:
    """No-op — ChromaDB auto-persists. Kept for API compatibility."""
    if index is not None:
        try:
            logger.info(f"Saved corpus: {index.count()} vectors")
        except Exception:
            pass


def build_seed_corpus(df: pd.DataFrame) -> None:
    """
    Seed the corpus from rows in df that already have primary_category.
    Called once when the corpus is empty. Uses S1-resolved rows as the seed.
    """
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

    try:
        collection = _get_collection()
    except Exception as e:
        logger.warning(f"ChromaDB unavailable for corpus seed: {e}")
        return

    texts = labeled.apply(_build_row_text, axis=1).tolist()
    categories = labeled["primary_category"].tolist()
    product_names = labeled.get("product_name", pd.Series([""] * len(labeled))).tolist()

    embeddings = model.encode(texts, batch_size=64, show_progress_bar=False).astype(np.float32)

    ids = [_make_vector_id(t, c) for t, c in zip(texts, categories)]
    metadatas = [
        {"category": cat, "product_name": pn}
        for cat, pn in zip(categories, product_names)
    ]

    # Upsert in chunks to avoid payload size limits
    chunk = 500
    for i in range(0, len(ids), chunk):
        collection.upsert(
            embeddings=embeddings[i:i + chunk].tolist(),
            metadatas=metadatas[i:i + chunk],
            ids=ids[i:i + chunk],
        )

    logger.info(f"Corpus seeded with {len(ids)} labeled rows")


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
        - category: majority-voted category or None if below threshold
        - confidence: average cosine similarity of votes
        - neighbors: list of {"category", "product_name", "similarity"} (top-3)
    """
    model = _get_model()
    if model is None or index is None or index.count() < MIN_CORPUS_SIZE:
        return None, 0.0, []

    text = _build_row_text(row)
    if not text.strip():
        return None, 0.0, []

    embedding = model.encode([text], show_progress_bar=False).astype(np.float32)
    k_actual = min(k, index.count())

    try:
        results = index.query(
            query_embeddings=[embedding[0].tolist()],
            n_results=k_actual,
        )
    except Exception as e:
        logger.warning(f"ChromaDB query failed: {e}")
        return None, 0.0, []

    metadatas_list = results["metadatas"][0]
    distances = results["distances"][0]
    neighbors = [
        {
            "category": m["category"],
            "product_name": m["product_name"],
            "similarity": float(1.0 - d),
        }
        for m, d in zip(metadatas_list, distances)
    ]
    return _score_from_neighbors(neighbors)


def knn_search_batch(
    rows: list[pd.Series],
    index,
    metadata: list[dict],
    k: int = K_NEIGHBORS,
    cache_client=None,
) -> list[tuple[Optional[str], float, list[dict]]]:
    """
    Batch KNN search: one model.encode() call for all rows.

    Returns list of (category, confidence, neighbors) tuples, one per input row.
    Rows with empty text return (None, 0.0, []).
    Embedding results are cached in Redis if cache_client is provided.
    """
    model = _get_model()
    if model is None or index is None or index.count() < MIN_CORPUS_SIZE:
        return [(None, 0.0, []) for _ in rows]

    from src.cache.client import CACHE_TTL_EMB

    texts = [_build_row_text(row) for row in rows]
    valid_mask = [bool(t.strip()) for t in texts]

    if not any(valid_mask):
        return [(None, 0.0, []) for _ in rows]

    valid_texts = [t for t, v in zip(texts, valid_mask) if v]

    # Split into cached and uncached embeddings
    embedding_dim = model.get_sentence_embedding_dimension()
    cached_embeddings: dict[int, np.ndarray] = {}
    uncached_positions: list[int] = []
    uncached_texts: list[str] = []

    if cache_client is not None:
        for i, text in enumerate(valid_texts):
            key = _compute_embedding_key(_MODEL_NAME, text)
            raw = cache_client.get("emb", key)
            if raw is not None:
                try:
                    vec = np.frombuffer(raw, dtype=np.float32).reshape(embedding_dim)
                    cached_embeddings[i] = vec
                except Exception:
                    uncached_positions.append(i)
                    uncached_texts.append(text)
            else:
                uncached_positions.append(i)
                uncached_texts.append(text)
    else:
        uncached_positions = list(range(len(valid_texts)))
        uncached_texts = valid_texts

    # Encode only uncached texts
    if uncached_texts:
        fresh_embeddings = model.encode(uncached_texts, batch_size=64, show_progress_bar=False).astype(np.float32)
        for pos, text, vec in zip(uncached_positions, uncached_texts, fresh_embeddings):
            cached_embeddings[pos] = vec
            if cache_client is not None:
                try:
                    key = _compute_embedding_key(_MODEL_NAME, text)
                    cache_client.set("emb", key, vec.tobytes(), ttl=CACHE_TTL_EMB)
                except Exception:
                    pass

    if uncached_texts:
        logger.debug(
            f"Embedding cache: {len(cached_embeddings) - len(uncached_texts)} hits, "
            f"{len(uncached_texts)} new encodings"
        )

    embeddings = np.stack([cached_embeddings[i] for i in range(len(valid_texts))]).astype(np.float32)

    k_actual = min(k, index.count())

    try:
        batch_results = index.query(
            query_embeddings=embeddings.tolist(),
            n_results=k_actual,
        )
    except Exception as e:
        logger.warning(f"ChromaDB batch query failed: {e}")
        return [(None, 0.0, []) for _ in rows]

    results: list[tuple[Optional[str], float, list[dict]]] = []
    valid_iter = iter(range(len(valid_texts)))
    vi = 0
    for is_valid in valid_mask:
        if is_valid:
            metadatas_list = batch_results["metadatas"][vi]
            distances = batch_results["distances"][vi]
            neighbors = [
                {
                    "category": m["category"],
                    "product_name": m["product_name"],
                    "similarity": float(1.0 - d),
                }
                for m, d in zip(metadatas_list, distances)
            ]
            results.append(_score_from_neighbors(neighbors))
            vi += 1
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
    Add a newly enriched row to the ChromaDB corpus.
    Upsert-safe: duplicate (text, category) pairs are silently ignored.
    `metadata` param is unused (ChromaDB stores its own) — kept for API compat.
    """
    model = _get_model()
    if model is None or index is None:
        return

    text = _build_row_text(row)
    if not text.strip():
        return

    embedding = model.encode([text], show_progress_bar=False).astype(np.float32)
    vector_id = _make_vector_id(text, category)

    try:
        index.upsert(
            embeddings=[embedding[0].tolist()],
            metadatas=[{"category": category, "product_name": str(row.get("product_name", ""))}],
            ids=[vector_id],
        )
    except Exception as e:
        logger.warning(f"ChromaDB add_to_corpus failed: {e}")
