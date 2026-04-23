"""
UC3 Hybrid Search — Product Indexer

Reads UC1 unified output (DataFrame or BigQuery table) and builds:
  - ChromaDB collection  → dense vector search (semantic)
  - Pickled BM25 index   → keyword search

UC1 unified schema columns used:
    product_name, brand_name, primary_category, ingredients,
    allergens, dietary_tags, is_organic, dq_score_post, data_source
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from pathlib import Path
from typing import Any

import chromadb
import pandas as pd

logger = logging.getLogger(__name__)

CHROMA_HOST       = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT       = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME   = "uc3_products"
BM25_INDEX_PATH   = Path("/tmp/uc3_bm25_index.pkl")


def _build_text(row: dict) -> str:
    """Concatenate searchable fields into one text blob."""
    parts = [
        row.get("product_name") or "",
        row.get("brand_name") or "",
        row.get("primary_category") or "",
        row.get("ingredients") or "",
        row.get("dietary_tags") or "",
        row.get("allergens") or "",
        row.get("recall_reason") or "",
    ]
    return " ".join(p for p in parts if p).lower()


def _tokenize(text: str) -> list[str]:
    import re
    return re.findall(r"\w+", text.lower())


class ProductIndexer:
    """
    Builds and manages the UC3 search indexes.

    Usage (called once after UC1 produces its output):
        indexer = ProductIndexer()
        n = indexer.build(enriched_df)
        print(f"Indexed {n} products")
    """

    def __init__(self):
        self._chroma = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)

    # ── public API ─────────────────────────────────────────────────────────────

    def build(self, df: pd.DataFrame, batch_size: int = 500) -> int:
        """
        Index the full enriched catalog.  Deletes and recreates both indexes.
        Returns number of products indexed.
        """
        df = self._clean(df)
        n = self._build_chroma(df, batch_size)
        self._build_bm25(df)
        logger.info("UC3 index built: %d products", n)
        return n

    def collection(self) -> chromadb.Collection:
        return self._chroma.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    def load_bm25(self):
        """Load pickled BM25 index. Returns (bm25, doc_ids) tuple."""
        if not BM25_INDEX_PATH.exists():
            raise FileNotFoundError("BM25 index not found — run build() first")
        with open(BM25_INDEX_PATH, "rb") as f:
            return pickle.load(f)

    def stats(self) -> dict:
        try:
            col = self.collection()
            chroma_count = col.count()
        except Exception:
            chroma_count = 0
        bm25_exists = BM25_INDEX_PATH.exists()
        return {"chroma_docs": chroma_count, "bm25_index": bm25_exists}

    # ── internals ──────────────────────────────────────────────────────────────

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.dropna(subset=["product_name"])
        df = df.drop_duplicates(subset=["product_name", "brand_name"])
        df["_doc_id"] = df.index.astype(str)
        return df.reset_index(drop=True)

    def _build_chroma(self, df: pd.DataFrame, batch_size: int) -> int:
        try:
            self._chroma.delete_collection(COLLECTION_NAME)
        except Exception:
            pass
        col = self._chroma.create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

        ids, documents, metadatas = [], [], []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            ids.append(str(row_dict["_doc_id"]))
            documents.append(_build_text(row_dict))
            metadatas.append({
                "product_name":     str(row_dict.get("product_name") or ""),
                "brand_name":       str(row_dict.get("brand_name") or ""),
                "primary_category": str(row_dict.get("primary_category") or ""),
                "allergens":        str(row_dict.get("allergens") or ""),
                "dietary_tags":     str(row_dict.get("dietary_tags") or ""),
                "is_organic":       str(row_dict.get("is_organic") or ""),
                "dq_score_post":    float(row_dict.get("dq_score_post") or 0.0),
                "data_source":      str(row_dict.get("data_source") or ""),
                "is_recalled":      str(row_dict.get("is_recalled") or "False"),
                "recall_class":     str(row_dict.get("recall_class") or ""),
            })

            if len(ids) >= batch_size:
                col.add(ids=ids, documents=documents, metadatas=metadatas)
                ids, documents, metadatas = [], [], []

        if ids:
            col.add(ids=ids, documents=documents, metadatas=metadatas)

        return col.count()

    def _build_bm25(self, df: pd.DataFrame) -> None:
        try:
            from rank_bm25 import BM25Okapi
        except ImportError:
            logger.warning("rank_bm25 not installed — BM25 index skipped")
            return

        texts  = [_build_text(row.to_dict()) for _, row in df.iterrows()]
        corpus = [_tokenize(t) for t in texts]
        doc_ids = df["_doc_id"].tolist()

        bm25 = BM25Okapi(corpus)
        with open(BM25_INDEX_PATH, "wb") as f:
            pickle.dump((bm25, doc_ids, df.to_dict("records")), f)
        logger.info("BM25 index saved to %s", BM25_INDEX_PATH)
