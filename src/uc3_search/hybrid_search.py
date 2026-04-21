"""
UC3 Hybrid Search — BM25 + Semantic with Reciprocal Rank Fusion

Flow:
  query → BM25 (rank_bm25, top-50) ─┐
                                      ├─ RRF → top-k unified ranking
  query → Semantic (ChromaDB, top-50)─┘

RRF score: sum(1 / (k + rank_i))  where k=60 (standard)
"""

from __future__ import annotations

import logging
import os
from typing import Any

import chromadb

from src.uc3_search.indexer import (
    CHROMA_HOST,
    CHROMA_PORT,
    COLLECTION_NAME,
    ProductIndexer,
    _tokenize,
)

logger = logging.getLogger(__name__)

RRF_K        = 60
CANDIDATE_N  = 50   # candidates pulled from each retriever before fusion


class HybridSearch:
    """
    BM25 + Semantic hybrid search with Reciprocal Rank Fusion.

    Usage:
        hs = HybridSearch()
        results = hs.search("organic gluten-free cereal", top_k=10)
    """

    def __init__(self):
        self._indexer = ProductIndexer()
        self._chroma  = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        self._bm25    = None
        self._bm25_ids  = None
        self._bm25_docs = None
        self._load_bm25()

    # ── public API ─────────────────────────────────────────────────────────────

    def search(self, query: str, top_k: int = 10, mode: str = "hybrid") -> list[dict]:
        """
        Execute search and return ranked results.

        mode: "hybrid" | "bm25" | "semantic"
        Each result: {product_name, brand_name, primary_category, allergens,
                      dietary_tags, is_organic, dq_score_post, data_source,
                      score, rank, mode}
        """
        if mode == "bm25":
            return self.bm25_search(query, top_k)
        if mode == "semantic":
            return self.semantic_search(query, top_k)

        bm25_hits     = self.bm25_search(query, CANDIDATE_N)
        semantic_hits = self.semantic_search(query, CANDIDATE_N)
        return self.reciprocal_rank_fusion(bm25_hits, semantic_hits, top_k)

    def bm25_search(self, query: str, top_k: int = 50) -> list[dict]:
        """BM25 keyword search over indexed product text."""
        if self._bm25 is None:
            logger.warning("BM25 index not loaded — returning empty")
            return []

        tokens = _tokenize(query)
        scores = self._bm25.get_scores(tokens)

        indexed = sorted(
            enumerate(scores), key=lambda x: x[1], reverse=True
        )[:top_k]

        results = []
        for rank, (idx, score) in enumerate(indexed, start=1):
            doc = dict(self._bm25_docs[idx])
            doc["score"] = float(score)
            doc["rank"]  = rank
            doc["mode"]  = "bm25"
            results.append(doc)
        return results

    def semantic_search(self, query: str, top_k: int = 50) -> list[dict]:
        """Dense vector similarity search via ChromaDB."""
        try:
            col = self._chroma.get_collection(COLLECTION_NAME)
            res = col.query(query_texts=[query], n_results=min(top_k, col.count()))
        except Exception as exc:
            logger.warning("ChromaDB query failed: %s", exc)
            return []

        results = []
        metas    = res.get("metadatas", [[]])[0]
        distances = res.get("distances", [[]])[0]

        for rank, (meta, dist) in enumerate(zip(metas, distances), start=1):
            doc = dict(meta)
            doc["score"] = float(1 - dist)   # cosine similarity
            doc["rank"]  = rank
            doc["mode"]  = "semantic"
            results.append(doc)
        return results

    def reciprocal_rank_fusion(
        self,
        bm25_results: list[dict],
        semantic_results: list[dict],
        top_k: int = 10,
    ) -> list[dict]:
        """
        Merge two ranked lists using Reciprocal Rank Fusion.
        RRF score = Σ 1/(k + rank_i)
        """
        rrf_scores: dict[str, float] = {}
        doc_index:  dict[str, dict]  = {}

        for rank, doc in enumerate(bm25_results, start=1):
            key = self._doc_key(doc)
            rrf_scores[key] = rrf_scores.get(key, 0) + 1 / (RRF_K + rank)
            doc_index[key]  = doc

        for rank, doc in enumerate(semantic_results, start=1):
            key = self._doc_key(doc)
            rrf_scores[key] = rrf_scores.get(key, 0) + 1 / (RRF_K + rank)
            if key not in doc_index:
                doc_index[key] = doc

        ranked = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

        results = []
        for final_rank, (key, rrf_score) in enumerate(ranked, start=1):
            doc = dict(doc_index[key])
            doc["score"] = round(rrf_score, 6)
            doc["rank"]  = final_rank
            doc["mode"]  = "hybrid"
            results.append(doc)
        return results

    def is_ready(self) -> bool:
        """Returns True if both indexes are available."""
        try:
            col   = self._chroma.get_collection(COLLECTION_NAME)
            chroma_ok = col.count() > 0
        except Exception:
            chroma_ok = False
        return chroma_ok and self._bm25 is not None

    # ── internals ──────────────────────────────────────────────────────────────

    def _load_bm25(self) -> None:
        try:
            bm25, ids, docs = self._indexer.load_bm25()
            self._bm25      = bm25
            self._bm25_ids  = ids
            self._bm25_docs = docs
        except FileNotFoundError:
            logger.info("BM25 index not built yet — run ProductIndexer.build() first")
        except Exception as exc:
            logger.warning("BM25 load failed: %s", exc)

    @staticmethod
    def _doc_key(doc: dict) -> str:
        return f"{doc.get('product_name','')}::{doc.get('brand_name','')}"
