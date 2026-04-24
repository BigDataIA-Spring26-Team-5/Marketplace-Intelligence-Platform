from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class RunRequest(BaseModel):
    source_path: str
    domain: str
    pipeline_mode: str = "full"
    with_critic: bool = False
    force_fresh: bool = False
    no_cache: bool = False
    chunk_size: int = 10000
    source_name: str | None = None


class RunStatus(BaseModel):
    run_id: str
    status: str  # pending | running | completed | failed | cancelled
    stage: str | None = None
    chunk_index: int | None = None
    started_at: datetime
    updated_at: datetime
    error: str | None = None


class BlockAuditEntry(BaseModel):
    block: str
    rows_in: int
    rows_out: int
    duration_ms: float | None = None
    extra: dict[str, Any] = {}


class RunResult(BaseModel):
    run_id: str
    status: str
    output_path: str | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_quarantined: int | None = None
    dq_score_pre: float | None = None
    dq_score_post: float | None = None
    dq_delta: float | None = None
    block_audit: list[BlockAuditEntry] = []
    completed_at: datetime | None = None


class ResumeRequest(BaseModel):
    pass
