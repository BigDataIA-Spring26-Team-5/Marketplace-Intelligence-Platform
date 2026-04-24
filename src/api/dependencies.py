"""
Shared singletons and run-status store for the REST API layer.

RunStore tracks per-run lifecycle state (pending/running/completed/failed)
in a lightweight SQLite DB separate from CheckpointManager's chunk-level
state. CheckpointManager continues to own chunk resume logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

logger = logging.getLogger(__name__)

_RUN_DB_PATH = Path("output/api_runs.db")

# ── Run status store ─────────────────────────────────────────────────────────

_CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS api_runs (
    run_id      TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    domain      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    stage       TEXT,
    chunk_index INTEGER,
    error       TEXT,
    audit_json  TEXT,
    output_path TEXT,
    rows_in     INTEGER,
    rows_out    INTEGER,
    rows_quarantined INTEGER,
    dq_score_pre REAL,
    dq_score_post REAL,
    dq_delta    REAL,
    started_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    completed_at TEXT
)
"""


class RunStore:
    def __init__(self, db_path: Path = _RUN_DB_PATH) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = str(db_path)
        with self._conn() as conn:
            conn.execute(_CREATE_RUNS_TABLE)
            conn.commit()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def create(self, run_id: str, source: str, domain: str) -> None:
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO api_runs (run_id, source, domain, status, started_at, updated_at)
                   VALUES (?, ?, ?, 'pending', ?, ?)""",
                (run_id, source, domain, now, now),
            )
            conn.commit()

    def set_running(self, run_id: str, stage: str | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_runs SET status='running', stage=?, updated_at=? WHERE run_id=?",
                (stage, self._now(), run_id),
            )
            conn.commit()

    def set_stage(self, run_id: str, stage: str, chunk_index: int | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_runs SET stage=?, chunk_index=?, updated_at=? WHERE run_id=?",
                (stage, chunk_index, self._now(), run_id),
            )
            conn.commit()

    def set_completed(self, run_id: str, result: dict[str, Any]) -> None:
        now = self._now()
        with self._conn() as conn:
            conn.execute(
                """UPDATE api_runs SET
                   status='completed', stage='save_output',
                   output_path=?, rows_in=?, rows_out=?, rows_quarantined=?,
                   dq_score_pre=?, dq_score_post=?, dq_delta=?,
                   audit_json=?, updated_at=?, completed_at=?
                   WHERE run_id=?""",
                (
                    result.get("output_path"),
                    result.get("rows_in"),
                    result.get("rows_out"),
                    result.get("rows_quarantined"),
                    result.get("dq_score_pre"),
                    result.get("dq_score_post"),
                    result.get("dq_delta"),
                    json.dumps(result.get("block_audit", [])),
                    now,
                    now,
                    run_id,
                ),
            )
            conn.commit()

    def set_failed(self, run_id: str, error: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE api_runs SET status='failed', error=?, updated_at=? WHERE run_id=?",
                (error, self._now(), run_id),
            )
            conn.commit()

    def get(self, run_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM api_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_active_sources(self) -> list[str]:
        """Return source paths with status='running' or 'pending'."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT source FROM api_runs WHERE status IN ('running','pending')"
            ).fetchall()
            return [r["source"] for r in rows]

    def count_active(self) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as n FROM api_runs WHERE status IN ('running','pending')"
            ).fetchone()
            return row["n"] if row else 0

    def cleanup_orphans(self) -> int:
        """Mark 'running'/'pending' records as failed on server restart."""
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE api_runs SET status='failed', error='server_restart',
                   updated_at=? WHERE status IN ('running','pending')""",
                (self._now(),),
            )
            conn.commit()
            return cursor.rowcount


# ── Singletons ────────────────────────────────────────────────────────────────

_run_store: RunStore | None = None
_semaphore: asyncio.Semaphore | None = None


def get_run_store() -> RunStore:
    global _run_store
    if _run_store is None:
        _run_store = RunStore()
    return _run_store


def get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        max_concurrent = int(os.getenv("MAX_CONCURRENT_RUNS", "2"))
        _semaphore = asyncio.Semaphore(max_concurrent)
    return _semaphore


def get_cache_client():
    """Return CacheClient singleton (lazy import to avoid heavy startup cost)."""
    from src.cache.client import CacheClient
    if not hasattr(get_cache_client, "_instance"):
        get_cache_client._instance = CacheClient()
    return get_cache_client._instance


def get_hybrid_search():
    """Return HybridSearch singleton."""
    from src.uc3_search.hybrid_search import HybridSearch
    if not hasattr(get_hybrid_search, "_instance"):
        get_hybrid_search._instance = HybridSearch()
    return get_hybrid_search._instance


def get_recommender():
    """Return ProductRecommender singleton."""
    from src.uc4_recommendations.recommender import ProductRecommender
    if not hasattr(get_recommender, "_instance"):
        get_recommender._instance = ProductRecommender()
    return get_recommender._instance
