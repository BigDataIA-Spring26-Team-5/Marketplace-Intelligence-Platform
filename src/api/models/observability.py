from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class RunSummary(BaseModel):
    run_id: str
    source: str
    domain: str
    status: str
    dq_score_pre: float | None = None
    dq_score_post: float | None = None
    started_at: datetime
    completed_at: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None


class RunListResponse(BaseModel):
    runs: list[RunSummary]
    total: int
    page: int
    page_size: int


class BlockTraceEntry(BaseModel):
    block: str
    rows_in: int
    rows_out: int
    started_at: datetime | None = None
    duration_ms: float | None = None


class BlockTrace(BaseModel):
    run_id: str
    blocks: list[BlockTraceEntry]


class AnomalyRecord(BaseModel):
    source: str
    anomaly_score: float
    flagged_at: datetime
    metrics: dict[str, Any] = {}


class QuarantineRecord(BaseModel):
    run_id: str
    row_index: int | None = None
    reason: str
    fields: dict[str, Any] = {}


class SourceCost(BaseModel):
    source: str
    model_tier: str
    tokens_used: int
    requests: int


class CostReport(BaseModel):
    period_start: datetime | None = None
    period_end: datetime | None = None
    by_source: list[SourceCost] = []
    total_tokens: int = 0
    estimated_usd: float | None = None


class DedupStats(BaseModel):
    run_id: str | None = None
    source: str | None = None
    clusters: int = 0
    merged_rows: int = 0
    dedup_rate: float = 0.0
