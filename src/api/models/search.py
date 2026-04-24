from __future__ import annotations

from pydantic import BaseModel, field_validator


class SearchRequest(BaseModel):
    query: str
    domain: str | None = None
    category: str | None = None
    top_k: int = 10
    mode: str = "hybrid"  # hybrid | bm25 | semantic

    @field_validator("query")
    @classmethod
    def query_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("query must not be empty")
        return v

    @field_validator("top_k")
    @classmethod
    def top_k_range(cls, v: int) -> int:
        if v < 1 or v > 100:
            raise ValueError("top_k must be between 1 and 100")
        return v


class SearchHit(BaseModel):
    product_name: str
    brand_name: str | None = None
    primary_category: str | None = None
    data_source: str | None = None
    is_recalled: bool | None = None
    recall_class: str | None = None
    score: float
    rank: int


class SearchResult(BaseModel):
    query: str
    mode: str
    total: int
    index_ready: bool
    results: list[SearchHit]
