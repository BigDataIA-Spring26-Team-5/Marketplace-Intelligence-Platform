"""
Pipeline router — /v1/pipeline/*

Submit, poll, and retrieve pipeline runs asynchronously.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from src.api.dependencies import get_run_store, get_semaphore
from src.api.models.pipeline import (
    BlockAuditEntry,
    ResumeRequest,
    RunRequest,
    RunResult,
    RunStatus,
)

logger = logging.getLogger(__name__)
router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

_PIPELINE_RATE_LIMIT = os.getenv("PIPELINE_RATE_LIMIT", "10/minute")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_status(row: dict) -> RunStatus:
    return RunStatus(
        run_id=row["run_id"],
        status=row["status"],
        stage=row.get("stage"),
        chunk_index=row.get("chunk_index"),
        started_at=datetime.fromisoformat(row["started_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        error=row.get("error"),
    )


def _build_state(req: RunRequest, run_id: str) -> dict:
    return {
        "source_path": req.source_path,
        "resolved_source_name": req.source_name,
        "domain": req.domain,
        "missing_column_decisions": {},
        "chunk_size": req.chunk_size,
        "with_critic": req.with_critic,
        "pipeline_mode": req.pipeline_mode,
    }


def _run_graph(run_id: str, req: RunRequest) -> None:
    """Execute the pipeline graph as a background task."""
    from src.agents.graph import build_graph
    from src.api.dependencies import get_run_store, get_semaphore

    store = get_run_store()
    sem = get_semaphore()
    try:
        store.set_running(run_id, stage="load_source")
        graph = build_graph()
        state = _build_state(req, run_id)
        result = graph.invoke(state)

        # Extract audit log from result
        audit_log = []
        for entry in result.get("audit_log", []):
            if isinstance(entry, dict):
                audit_log.append(entry)

        # Derive output path from state
        output_path = result.get("output_path") or result.get("silver_output_path")

        working_df = result.get("working_df")
        rows_in = result.get("rows_in") or (len(working_df) if working_df is not None else None)
        rows_out = result.get("rows_out") or (len(working_df) if working_df is not None else None)

        store.set_completed(run_id, {
            "output_path": str(output_path) if output_path else None,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": result.get("quarantine_count"),
            "dq_score_pre": result.get("dq_score_pre"),
            "dq_score_post": result.get("dq_score_post"),
            "dq_delta": result.get("dq_delta"),
            "block_audit": audit_log,
        })
    except Exception as exc:
        logger.exception("Pipeline run %s failed: %s", run_id, exc)
        store.set_failed(run_id, str(exc))
    finally:
        # Release semaphore slot
        try:
            sem.release()
        except Exception:
            pass


# ── POST /v1/pipeline/runs ────────────────────────────────────────────────────

@router.post("/runs", status_code=202, response_model=RunStatus)
@limiter.limit(_PIPELINE_RATE_LIMIT)
async def submit_run(request: Request, body: RunRequest, background_tasks: BackgroundTasks):
    store = get_run_store()
    sem = get_semaphore()

    # Same-source conflict check
    active_sources = store.get_active_sources()
    if body.source_path in active_sources:
        # Find existing run_id
        raise HTTPException(
            status_code=409,
            detail={
                "error": "conflict",
                "detail": f"A run is already in progress for source '{body.source_path}'",
            },
        )

    # Concurrent run cap
    if not sem._value:  # no permits available
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": "60"},
            detail={
                "error": "too_many_requests",
                "detail": f"Maximum concurrent runs ({os.getenv('MAX_CONCURRENT_RUNS', '2')}) reached. Retry after 60s.",
            },
        )

    # Acquire semaphore slot (non-blocking since we checked above)
    await sem.acquire()

    # Validate source exists for local paths
    from src.pipeline.cli import is_gcs_uri
    if not is_gcs_uri(body.source_path):
        if not Path(body.source_path).exists():
            sem.release()
            raise HTTPException(
                status_code=422,
                detail={"error": "validation_error", "detail": f"Source file not found: {body.source_path}"},
            )

    # Create run record
    from src.pipeline.checkpoint.manager import CheckpointManager
    from src.pipeline.cli import _create_gcs_checkpoint, is_gcs_uri

    checkpoint_mgr = CheckpointManager()
    if body.force_fresh:
        checkpoint_mgr.force_fresh()

    if is_gcs_uri(body.source_path):
        run_id = _create_gcs_checkpoint(checkpoint_mgr, body.source_path)
    else:
        run_id = checkpoint_mgr.create(
            source_file=Path(body.source_path),
            block_sequence=[],
            config={},
        )

    source_label = body.source_name or Path(body.source_path).stem
    store.create(run_id, body.source_path, body.domain)

    background_tasks.add_task(_run_graph, run_id, body)

    row = store.get(run_id)
    return _row_to_status(row)


# ── GET /v1/pipeline/runs/{run_id}/status ────────────────────────────────────

@router.get("/runs/{run_id}/status", response_model=RunStatus)
def get_run_status(run_id: str):
    store = get_run_store()
    row = store.get(run_id)
    if not row:
        raise HTTPException(status_code=404, detail={"error": "not_found", "detail": f"Run '{run_id}' not found"})
    return _row_to_status(row)


# ── GET /v1/pipeline/runs/{run_id}/result ────────────────────────────────────

@router.get("/runs/{run_id}/result", response_model=RunResult)
def get_run_result(run_id: str):
    import json as _json

    store = get_run_store()
    row = store.get(run_id)
    if not row:
        raise HTTPException(status_code=404, detail={"error": "not_found", "detail": f"Run '{run_id}' not found"})
    if row["status"] != "completed":
        raise HTTPException(
            status_code=409,
            detail={"error": "run_not_complete", "detail": f"Run status is '{row['status']}', not 'completed'", "run_id": run_id},
        )

    audit_raw = row.get("audit_json") or "[]"
    try:
        audit_list = _json.loads(audit_raw)
    except Exception:
        audit_list = []

    block_audit = [
        BlockAuditEntry(
            block=e.get("block", ""),
            rows_in=e.get("rows_in", 0),
            rows_out=e.get("rows_out", 0),
            duration_ms=e.get("duration_ms"),
            extra={k: v for k, v in e.items() if k not in ("block", "rows_in", "rows_out", "duration_ms")},
        )
        for e in audit_list
        if isinstance(e, dict)
    ]

    return RunResult(
        run_id=run_id,
        status="completed",
        output_path=row.get("output_path"),
        rows_in=row.get("rows_in"),
        rows_out=row.get("rows_out"),
        rows_quarantined=row.get("rows_quarantined"),
        dq_score_pre=row.get("dq_score_pre"),
        dq_score_post=row.get("dq_score_post"),
        dq_delta=row.get("dq_delta"),
        block_audit=block_audit,
        completed_at=datetime.fromisoformat(row["completed_at"]) if row.get("completed_at") else None,
    )


# ── POST /v1/pipeline/runs/{run_id}/resume ───────────────────────────────────

@router.post("/runs/{run_id}/resume", status_code=202, response_model=RunStatus)
async def resume_run(run_id: str, body: ResumeRequest, background_tasks: BackgroundTasks):
    store = get_run_store()
    sem = get_semaphore()

    row = store.get(run_id)
    if not row:
        raise HTTPException(status_code=404, detail={"error": "not_found", "detail": f"Run '{run_id}' not found"})
    if row["status"] != "failed":
        raise HTTPException(
            status_code=409,
            detail={"error": "conflict", "detail": f"Run '{run_id}' has status '{row['status']}'; can only resume failed runs"},
        )

    if not sem._value:
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": "60"},
            detail={"error": "too_many_requests", "detail": "Maximum concurrent runs reached. Retry after 60s."},
        )

    await sem.acquire()

    # Rebuild RunRequest from stored run data
    req = RunRequest(source_path=row["source"], domain=row["domain"])
    store.set_running(run_id, stage="load_source")
    background_tasks.add_task(_run_graph, run_id, req)

    row = store.get(run_id)
    return _row_to_status(row)


# ── POST /v1/pipeline/runs/{run_id}/cancel ───────────────────────────────────

@router.post("/runs/{run_id}/cancel", status_code=501)
def cancel_run(run_id: str):
    raise HTTPException(
        status_code=501,
        detail={"error": "not_implemented", "detail": "Run cancellation is not supported in this version"},
    )
