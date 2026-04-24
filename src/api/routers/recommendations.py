"""
Recommendations router — /v1/recommendations/*

Also-bought and you-might-like backed by UC4 ProductRecommender.
Returns 503 when the recommendation graph has not been built.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from src.api.dependencies import get_recommender
from src.api.models.recommendations import RecHit, RecommendationResult

logger = logging.getLogger(__name__)
router = APIRouter()

_NOT_READY_DETAIL = (
    "Recommendation graph not ready. "
    "Call ProductRecommender.build() with enriched product data first."
)


def _not_ready():
    raise HTTPException(
        status_code=503,
        detail={"error": "service_unavailable", "detail": _NOT_READY_DETAIL},
    )


def _not_found(product_id: str):
    raise HTTPException(
        status_code=404,
        detail={"error": "not_found", "detail": f"product_id '{product_id}' not found in recommendation graph"},
    )


@router.get("/{product_id}/also-bought", response_model=RecommendationResult)
def also_bought(
    product_id: str,
    top_k: int = Query(5, ge=1, le=20),
):
    rec = get_recommender()
    if not rec.is_ready():
        _not_ready()

    try:
        results = rec.also_bought(product_id, top_k=top_k)
    except Exception as exc:
        logger.exception("also_bought failed: %s", exc)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "detail": str(exc)})

    if not results:
        _not_found(product_id)

    hits = [
        RecHit(
            product_id=r.get("product_id", ""),
            product_name=r.get("product_name"),
            primary_category=r.get("primary_category"),
            score=float(r.get("confidence", r.get("score", 0))),
            rank=i + 1,
            extra={"lift": r.get("lift")} if r.get("lift") is not None else {},
        )
        for i, r in enumerate(results)
    ]

    return RecommendationResult(
        product_id=product_id,
        rec_type="also_bought",
        top_k=top_k,
        graph_ready=True,
        results=hits,
    )


@router.get("/{product_id}/you-might-like", response_model=RecommendationResult)
def you_might_like(
    product_id: str,
    top_k: int = Query(5, ge=1, le=20),
):
    rec = get_recommender()
    if not rec.is_ready():
        _not_ready()

    try:
        results = rec.you_might_like(product_id, top_k=top_k)
    except Exception as exc:
        logger.exception("you_might_like failed: %s", exc)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "detail": str(exc)})

    if not results:
        _not_found(product_id)

    hits = [
        RecHit(
            product_id=r.get("product_id", ""),
            product_name=r.get("product_name"),
            primary_category=r.get("primary_category"),
            score=float(r.get("affinity_score", r.get("score", 0))),
            rank=i + 1,
            extra={"hops": r.get("hops")} if r.get("hops") is not None else {},
        )
        for i, r in enumerate(results)
    ]

    return RecommendationResult(
        product_id=product_id,
        rec_type="you_might_like",
        top_k=top_k,
        graph_ready=True,
        results=hits,
    )


@router.get("/status")
def recommendations_status():
    rec = get_recommender()
    if rec.is_ready():
        return {"ready": True, **rec.stats()}
    return {"ready": False, "products": 0, "rules": 0, "graph_edges": 0}
