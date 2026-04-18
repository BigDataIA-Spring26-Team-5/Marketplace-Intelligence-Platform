"""Checkpoint manager for pipeline state persistence and resume."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

CHECKPOINT_DIR = Path("checkpoint")
CHECKPOINT_DB = CHECKPOINT_DIR / "checkpoint.db"


def _load_config() -> dict:
    """Load configuration from requiredlimits.yaml."""
    config_path = Path(".specify/requiredlimits.yaml")
    config = {"checkpoint_schema_version": 1}
    if config_path.exists():
        try:
            import yaml
            with open(config_path) as f:
                loaded = yaml.safe_load(f)
                if loaded:
                    config.update(loaded)
        except Exception as e:
            logger.warning(f"Could not load config: {e}")
    return config


def _get_schema_version() -> int:
    """Get current checkpoint schema version."""
    return _load_config().get("checkpoint_schema_version", 1)


def _compute_file_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _init_database(conn: sqlite3.Connection) -> None:
    """Initialize database schema."""
    schema_path = Path(__file__).parent / "schema.sql"
    with open(schema_path) as f:
        conn.executescript(f.read())


class CheckpointManager:
    """Manages checkpoint creation, loading, and validation."""

    def __init__(self, checkpoint_dir: Path = CHECKPOINT_DIR):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.db_path = self.checkpoint_dir / "checkpoint.db"
        self._ensure_db()

    def _ensure_db(self) -> None:
        """Ensure database exists with schema."""
        conn = self._get_connection()
        try:
            _init_database(conn)
        finally:
            conn.close()

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _atomic_write(self, temp_data: dict) -> None:
        """Write checkpoint data atomically using temp file + rename."""
        temp_fd, temp_path = tempfile.mkstemp(
            suffix=".json",
            dir=self.checkpoint_dir,
        )
        try:
            with os.fdopen(temp_fd, "w") as f:
                json.dump(temp_data, f)
            os.rename(temp_path, str(self.checkpoint_dir / "checkpoint_state.json"))
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def create(
        self,
        source_file: Path,
        block_sequence: list[str],
        config: dict,
    ) -> str:
        """
        Create a new checkpoint.

        Returns:
            run_id: UUID for this checkpoint
        """
        run_id = str(uuid.uuid4())
        schema_version = _get_schema_version()
        source_sha256 = _compute_file_sha256(source_file)

        conn = self._get_connection()
        try:
            conn.execute(
                """INSERT INTO checkpoints
                   (run_id, source_file, source_sha256, schema_version, created_at, resume_state)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    str(source_file),
                    source_sha256,
                    schema_version,
                    datetime.now(timezone.utc).isoformat(),
                    "none",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        logger.info(f"Created checkpoint: run_id={run_id}")
        return run_id

    def save_checkpoint(
        self,
        run_id: str,
        chunk_index: int,
        chunk_data: dict,
        plan_yaml: str,
        corpus_index_path: Optional[Path] = None,
        corpus_metadata_path: Optional[Path] = None,
    ) -> None:
        """
        Save checkpoint state after a chunk completes.

        Args:
            run_id: The checkpoint run identifier
            chunk_index: Index of the completed chunk
            chunk_data: Dict with record_count, dq_score_pre, dq_score_post, etc.
            plan_yaml: YAML transformation plan content
            corpus_index_path: Path to FAISS index file
            corpus_metadata_path: Path to corpus metadata JSON
        """
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT id FROM checkpoints WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Checkpoint not found: {run_id}")
            checkpoint_id = row["id"]

            conn.execute(
                """INSERT INTO chunk_states
                   (checkpoint_id, chunk_index, status, record_count,
                    dq_score_pre, dq_score_post, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    checkpoint_id,
                    chunk_index,
                    "completed",
                    chunk_data.get("record_count", 0),
                    chunk_data.get("dq_score_pre"),
                    chunk_data.get("dq_score_post"),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

            existing_plan = conn.execute(
                "SELECT id FROM transformation_plans WHERE checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()
            if not existing_plan:
                import hashlib

                plan_md5 = hashlib.md5(plan_yaml.encode()).hexdigest()
                conn.execute(
                    """INSERT INTO transformation_plans
                       (checkpoint_id, plan_yaml, plan_md5)
                       VALUES (?, ?, ?)""",
                    (checkpoint_id, plan_yaml, plan_md5),
                )

            if corpus_index_path and corpus_index_path.exists():
                existing_corpus = conn.execute(
                    "SELECT id FROM corpus_snapshots WHERE checkpoint_id = ?",
                    (checkpoint_id,),
                ).fetchone()
                if not existing_corpus:
                    import faiss

                    try:
                        index = faiss.read_index(str(corpus_index_path))
                        vector_count = index.ntotal
                    except Exception:
                        vector_count = 0

                    conn.execute(
                        """INSERT INTO corpus_snapshots
                           (checkpoint_id, index_path, metadata_path, vector_count)
                           VALUES (?, ?, ?, ?)""",
                        (
                            checkpoint_id,
                            str(corpus_index_path),
                            str(corpus_metadata_path) if corpus_metadata_path else None,
                            vector_count,
                        ),
                    )

            conn.commit()
            logger.info(f"Saved checkpoint state for chunk {chunk_index}")

        finally:
            conn.close()

    def load_checkpoint(self, run_id: str) -> Optional[dict]:
        """
        Load checkpoint data by run_id.

        Returns:
            dict with checkpoint data or None if not found
        """
        conn = self._get_connection()
        try:
            checkpoint = conn.execute(
                "SELECT * FROM checkpoints WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not checkpoint:
                return None

            chunks = conn.execute(
                "SELECT * FROM chunk_states WHERE checkpoint_id = ? ORDER BY chunk_index",
                (checkpoint["id"],),
            ).fetchall()

            plan = conn.execute(
                "SELECT * FROM transformation_plans WHERE checkpoint_id = ?",
                (checkpoint["id"],),
            ).fetchone()

            corpus = conn.execute(
                "SELECT * FROM corpus_snapshots WHERE checkpoint_id = ?",
                (checkpoint["id"],),
            ).fetchone()

            return {
                "run_id": checkpoint["run_id"],
                "source_file": checkpoint["source_file"],
                "source_sha256": checkpoint["source_sha256"],
                "schema_version": checkpoint["schema_version"],
                "created_at": checkpoint["created_at"],
                "resume_state": checkpoint["resume_state"],
                "chunks": [dict(c) for c in chunks],
                "plan": dict(plan) if plan else None,
                "corpus": dict(corpus) if corpus else None,
            }
        finally:
            conn.close()

    def get_resume_state(self) -> Optional[dict]:
        """
        Detect and return checkpoint to resume from.

        Returns:
            dict with checkpoint data if valid checkpoint exists, None otherwise
        """
        conn = self._get_connection()
        try:
            latest = conn.execute(
                "SELECT run_id FROM checkpoints ORDER BY created_at DESC LIMIT 1",
            ).fetchone()
            if not latest:
                return None

            checkpoint = self.load_checkpoint(latest["run_id"])
            if checkpoint:
                conn.execute(
                    "UPDATE checkpoints SET resume_state = 'resume' WHERE run_id = ?",
                    (checkpoint["run_id"],),
                )
                conn.commit()
            return checkpoint

        finally:
            conn.close()

    def validate_checkpoint(self, source_file: Path) -> tuple[bool, str]:
        """
        Validate checkpoint integrity.

        Args:
            source_file: Current source file to validate against

        Returns:
            (is_valid, message)
        """
        checkpoint = self.get_resume_state()
        if not checkpoint:
            return True, "No checkpoint to validate"

        current_sha256 = _compute_file_sha256(source_file)
        if checkpoint["source_sha256"] != current_sha256:
            return False, "Source file has changed since checkpoint"

        schema_version = _get_schema_version()
        if checkpoint["schema_version"] != schema_version:
            return False, f"Schema version mismatch: checkpoint={checkpoint['schema_version']}, current={schema_version}"

        return True, "Checkpoint valid"

    def force_fresh(self) -> None:
        """Clear all checkpoint data for a fresh run."""
        conn = self._get_connection()
        try:
            conn.execute("DELETE FROM corpus_snapshots")
            conn.execute("DELETE FROM transformation_plans")
            conn.execute("DELETE FROM chunk_states")
            conn.execute("DELETE FROM checkpoints")
            conn.commit()
            logger.info("Cleared all checkpoint data")
        finally:
            conn.close()

    def clear_checkpoint(self, run_id: Optional[str] = None) -> None:
        """
        Clear specific checkpoint or all checkpoints.

        Args:
            run_id: Specific run to clear, or None to clear all
        """
        conn = self._get_connection()
        try:
            if run_id:
                conn.execute("DELETE FROM checkpoints WHERE run_id = ?", (run_id,))
            else:
                conn.execute("DELETE FROM corpus_snapshots")
                conn.execute("DELETE FROM transformation_plans")
                conn.execute("DELETE FROM chunk_states")
                conn.execute("DELETE FROM checkpoints")
            conn.commit()
            logger.info(f"Cleared checkpoint(s): {run_id or 'all'}")
        finally:
            conn.close()

    def save_chunk_stage(
        self,
        run_id: str,
        chunk_index: int,
        stage: str,
        state: Optional[dict] = None,
    ) -> None:
        """
        Save checkpoint at a specific pipeline stage for a chunk.

        Args:
            run_id: The checkpoint run identifier
            chunk_index: Index of the chunk
            stage: Current stage (transform, enrich, dq, complete)
            state: Optional state dict to store
        """
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT id FROM checkpoints WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Checkpoint not found: {run_id}")
            checkpoint_id = row["id"]

            existing = conn.execute(
                """SELECT id FROM chunk_states 
                   WHERE checkpoint_id = ? AND chunk_index = ?""",
                (checkpoint_id, chunk_index),
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE chunk_states 
                       SET status = ?, dq_score_pre = ?, dq_score_post = ?, completed_at = ?
                       WHERE checkpoint_id = ? AND chunk_index = ?""",
                    (
                        stage,
                        state.get("dq_score_pre") if state else None,
                        state.get("dq_score_post") if state else None,
                        datetime.now(timezone.utc).isoformat() if stage == "complete" else None,
                        checkpoint_id,
                        chunk_index,
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO chunk_states
                       (checkpoint_id, chunk_index, status, record_count,
                        dq_score_pre, dq_score_post, completed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        checkpoint_id,
                        chunk_index,
                        stage,
                        state.get("record_count", 0) if state else 0,
                        state.get("dq_score_pre") if state else None,
                        state.get("dq_score_post") if state else None,
                        datetime.now(timezone.utc).isoformat() if stage == "complete" else None,
                    ),
                )
            conn.commit()
            logger.info(f"Saved chunk stage: run_id={run_id}, chunk={chunk_index}, stage={stage}")
        finally:
            conn.close()

    def get_chunk_resume_index(self, run_id: str) -> int:
        """
        Get the chunk index to resume from.

        Args:
            run_id: The checkpoint run identifier

        Returns:
            Chunk index to resume from (0 if no checkpoint)
        """
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT id FROM checkpoints WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                return 0
            checkpoint_id = row["id"]

            completed = conn.execute(
                """SELECT MAX(chunk_index) as max_chunk 
                   FROM chunk_states 
                   WHERE checkpoint_id = ? AND status = 'completed'""",
                (checkpoint_id,),
            ).fetchone()

            if completed and completed["max_chunk"] is not None:
                return completed["max_chunk"] + 1
            return 0
        finally:
            conn.close()

    def get_latest_run_id(self) -> Optional[str]:
        """Get the most recent run_id."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT run_id FROM checkpoints ORDER BY created_at DESC LIMIT 1",
            ).fetchone()
            return row["run_id"] if row else None
        finally:
            conn.close()