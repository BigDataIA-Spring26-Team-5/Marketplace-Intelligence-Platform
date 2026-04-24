from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class RecHit(BaseModel):
    product_id: str
    product_name: str | None = None
    primary_category: str | None = None
    score: float
    rank: int
    extra: dict[str, Any] = {}


class RecommendationResult(BaseModel):
    product_id: str
    rec_type: str  # also_bought | you_might_like
    top_k: int
    graph_ready: bool
    results: list[RecHit]
