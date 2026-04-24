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
import time
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

CORPUS_TTL_DAYS = int(os.environ.get("CORPUS_TTL_DAYS", "90"))
MAX_CORPUS_SIZE = int(os.environ.get("MAX_CORPUS_SIZE", "500000"))
CHROMA_QUERY_CHUNK_SIZE = int(os.environ.get("CHROMA_QUERY_CHUNK_SIZE", "500"))

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


def _get_collection(collection_name: str | None = None):
    """Get or create the ChromaDB product corpus collection."""
    import chromadb
    client = chromadb.HttpClient(
        host=os.environ.get("CHROMA_HOST", "localhost"),
        port=int(os.environ.get("CHROMA_PORT", "8000")),
    )
    return client.get_or_create_collection(
        collection_name or _COLLECTION_NAME,
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


def evict_corpus(collection) -> None:
    """
    Two-tier eviction: TTL-first (remove vectors unseen > CORPUS_TTL_DAYS),
    then size cap (delete oldest last_seen until count <= MAX_CORPUS_SIZE).
    Best-effort — logs WARNING on any ChromaDB failure and returns.
    """
    try:
        cutoff = time.time() - (CORPUS_TTL_DAYS * 86400)
        try:
            stale = collection.get(where={"last_seen": {"$lt": cutoff}}, include=["metadatas"])
            stale_ids = stale["ids"]
            if stale_ids:
                for i in range(0, len(stale_ids), 500):
                    collection.delete(ids=stale_ids[i:i + 500])
                logger.info("Corpus eviction: deleted %d stale vectors (TTL %d days)", len(stale_ids), CORPUS_TTL_DAYS)
        except Exception as e:
            logger.warning("Corpus TTL eviction query failed: %s", e)

        if collection.count() > MAX_CORPUS_SIZE:
            try:
                all_items = collection.get(include=["metadatas"])
                ids_and_ts = [
                    (vid, m.get("last_seen", ""))
                    for vid, m in zip(all_items["ids"], all_items["metadatas"])
                ]
                ids_and_ts.sort(key=lambda x: float(x[1]) if isinstance(x[1], (int, float)) else 0.0)
                excess = collection.count() - MAX_CORPUS_SIZE
                to_delete = [vid for vid, _ in ids_and_ts[:excess]]
                for i in range(0, len(to_delete), 500):
                    collection.delete(ids=to_delete[i:i + 500])
                logger.info("Corpus eviction: deleted %d vectors (size cap %d)", len(to_delete), MAX_CORPUS_SIZE)
            except Exception as e:
                logger.warning("Corpus size-cap eviction failed: %s", e)
    except Exception as e:
        logger.warning("evict_corpus failed: %s", e)


def load_corpus(collection_name: str | None = None) -> tuple[Optional[object], list[dict]]:
    """
    Load the ChromaDB product corpus collection.
    Returns (collection, []) — metadata list is unused (ChromaDB stores its own).
    Returns (None, []) if ChromaDB is unreachable.
    """
    try:
        collection = _get_collection(collection_name)
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


def augment_from_df(
    df: pd.DataFrame,
    collection,
    unresolved_count: int,
    force_ratio_threshold: float = 0.25,
) -> int:
    """
    Upsert S1-resolved rows into the corpus if corpus_size/unresolved_count < force_ratio_threshold.
    Returns number of vectors upserted.
    """
    try:
        corpus_size = collection.count()
        if unresolved_count > 0 and corpus_size / unresolved_count >= force_ratio_threshold:
            logger.debug(
                "S2 KNN: corpus ratio %.3f >= threshold %.2f, skipping augmentation",
                corpus_size / unresolved_count,
                force_ratio_threshold,
            )
            return 0

        labeled = df[df["primary_category"].notna()].copy()
        if labeled.empty:
            logger.warning("augment_from_df: no S1-resolved rows available to augment corpus")
            return 0

        model = _get_model()
        if model is None:
            return 0

        logger.info(
            "S2 KNN: corpus too sparse (%d vectors / %d queries = %.3f < threshold %.2f). "
            "Augmenting from %d S1-resolved rows...",
            corpus_size, unresolved_count,
            corpus_size / unresolved_count if unresolved_count else 0,
            force_ratio_threshold, len(labeled),
        )

        texts = labeled.apply(_build_row_text, axis=1).tolist()
        categories = labeled["primary_category"].tolist()
        product_names = labeled.get("product_name", pd.Series([""] * len(labeled))).tolist()

        embeddings = model.encode(texts, batch_size=64, show_progress_bar=False).astype(np.float32)
        now_ts = time.time()
        ids = [_make_vector_id(t, c) for t, c in zip(texts, categories)]
        metadatas = [
            {"category": cat, "product_name": str(pn), "last_seen": now_ts}
            for cat, pn in zip(categories, product_names)
        ]

        for i in range(0, len(ids), 500):
            collection.upsert(
                embeddings=embeddings[i:i + 500].tolist(),
                metadatas=metadatas[i:i + 500],
                ids=ids[i:i + 500],
            )

        after = collection.count()
        upserted = len(ids)
        logger.info(
            "S2 KNN: corpus augmented %d → %d vectors (upserted %d)",
            corpus_size, after, upserted,
        )
        return upserted
    except Exception as e:
        logger.warning("augment_from_df failed: %s", e)
        return 0


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
    now_ts = time.time()
    metadatas = [
        {"category": cat, "product_name": pn, "last_seen": now_ts}
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

    all_metadatas: list[list] = []
    all_distances: list[list] = []
    total_chunks = (len(embeddings) + CHROMA_QUERY_CHUNK_SIZE - 1) // CHROMA_QUERY_CHUNK_SIZE
    for chunk_num, chunk_start in enumerate(range(0, len(embeddings), CHROMA_QUERY_CHUNK_SIZE), 1):
        chunk_emb = embeddings[chunk_start:chunk_start + CHROMA_QUERY_CHUNK_SIZE]
        try:
            chunk_res = index.query(
                query_embeddings=chunk_emb.tolist(),
                n_results=k_actual,
            )
            all_metadatas.extend(chunk_res["metadatas"])
            all_distances.extend(chunk_res["distances"])
        except Exception as e:
            logger.warning("ChromaDB batch query chunk %d/%d failed: %s", chunk_num, total_chunks, e)
            all_metadatas.extend([[] for _ in range(len(chunk_emb))])
            all_distances.extend([[] for _ in range(len(chunk_emb))])
        if chunk_num % 10 == 0:
            logger.info(
                "S2 KNN: queried chunk %d/%d (%d/%d rows)",
                chunk_num, total_chunks,
                min(chunk_start + CHROMA_QUERY_CHUNK_SIZE, len(embeddings)),
                len(embeddings),
            )

    batch_results = {"metadatas": all_metadatas, "distances": all_distances}

    results: list[tuple[Optional[str], float, list[dict]]] = []
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
            metadatas=[{"category": category, "product_name": str(row.get("product_name", "")), "last_seen": time.time()}],
            ids=[vector_id],
        )
    except Exception as e:
        logger.warning(f"ChromaDB add_to_corpus failed: {e}")
