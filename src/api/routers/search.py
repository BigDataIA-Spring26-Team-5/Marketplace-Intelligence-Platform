"""
Search router — /v1/search/*

Hybrid product search backed by UC3 HybridSearch.
Returns 503 when the search index is not built.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from src.api.dependencies import get_hybrid_search
from src.api.models.search import SearchHit, SearchRequest, SearchResult

logger = logging.getLogger(__name__)
router = APIRouter()

_NOT_READY_DETAIL = (
    "Search index not ready. "
    "Run: poetry run python scripts/build_corpus.py"
)


@router.post("/query", response_model=SearchResult)
def search_products(body: SearchRequest):
    search = get_hybrid_search()

    if not search.is_ready():
        raise HTTPException(
            status_code=503,
            detail={"error": "service_unavailable", "detail": _NOT_READY_DETAIL},
        )

    try:
        raw = search.search(query=body.query, top_k=body.top_k, mode=body.mode)
    except Exception as exc:
        logger.exception("Search failed: %s", exc)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "detail": str(exc)})

    # Apply optional post-filters
    if body.domain:
        raw = [r for r in raw if r.get("data_source", "").startswith(body.domain)]
    if body.category:
        raw = [r for r in raw if r.get("primary_category") == body.category]

    hits = [
        SearchHit(
            product_name=r.get("product_name", ""),
            brand_name=r.get("brand_name"),
            primary_category=r.get("primary_category"),
            data_source=r.get("data_source"),
            is_recalled=r.get("is_recalled"),
            recall_class=r.get("recall_class"),
            score=float(r.get("score", 0)),
            rank=int(r.get("rank", i + 1)),
        )
        for i, r in enumerate(raw)
    ]

    return SearchResult(
        query=body.query,
        mode=body.mode,
        total=len(hits),
        index_ready=True,
        results=hits,
    )


@router.get("/status")
def search_status():
    search = get_hybrid_search()
    return {"ready": search.is_ready(), "backend": "hybrid"}
