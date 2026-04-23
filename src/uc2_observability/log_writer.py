"""RunLogWriter: persist a structured JSON run log after each pipeline execution."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.agents.state import PipelineState

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class RunLogWriter:
    def __init__(self, log_dir: Path = PROJECT_ROOT / "output" / "run_logs"):
        self.log_dir = Path(log_dir)

    def _extract_record(
        self,
        state: "PipelineState",
        status: str,
        error: str | None = None,
        start_time: float | None = None,
    ) -> dict:
        source_path = state.get("source_path", "unknown")
        source_name = (
            state.get("resolved_source_name")
            or (Path(source_path).stem if source_path != "unknown" else "unknown")
        )
        # GCS glob paths produce stem="*"; fall back to parent dir name
        if source_name == "*":
            source_name = Path(source_path.replace("*", "")).parent.name or "unknown"

        run_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        duration_seconds: float | None = None
        if start_time is not None:
            duration_seconds = round(time.monotonic() - start_time, 3)

        source_df = state.get("source_df")
        working_df = state.get("working_df")
        quarantined_df = state.get("quarantined_df")

        rows_in = int(len(source_df)) if source_df is not None else None
        rows_out = int(len(working_df)) if working_df is not None else None
        rows_quarantined = int(len(quarantined_df)) if quarantined_df is not None else None

        dq_pre = state.get("dq_score_pre")
        dq_post = state.get("dq_score_post")
        dq_delta: float | None = None
        if dq_pre is not None and dq_post is not None:
            dq_delta = round(float(dq_post) - float(dq_pre), 4)

        cache_stats: dict = {}
        cache_client = state.get("cache_client")
        if cache_client is not None:
            try:
                cache_stats = cache_client.get_stats().summary()
            except Exception:
                pass

        operations = state.get("revised_operations", state.get("operations", []))

        record: dict = {
            "run_id": run_id,
            "timestamp": timestamp,
            "source_path": source_path,
            "source_name": source_name,
            "domain": state.get("domain", "unknown"),
            "status": status,
            "error": error,
            "duration_seconds": duration_seconds,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": rows_quarantined,
            "dq_score_pre": float(dq_pre) if dq_pre is not None else None,
            "dq_score_post": float(dq_post) if dq_post is not None else None,
            "dq_delta": dq_delta,
            "enrichment_stats": state.get("enrichment_stats", {}),
            "block_sequence": state.get("block_sequence", []),
            "sequence_reasoning": state.get("sequence_reasoning", ""),
            "skipped_blocks": state.get("skipped_blocks", {}),
            "audit_log": state.get("audit_log", []),
            "column_mapping": state.get("column_mapping", {}),
            "operations": list(operations) if operations else [],
            "critique_notes": state.get("critique_notes", []),
            "quarantine_reasons": state.get("quarantine_reasons", []),
            "cache_stats": cache_stats,
            "registry_hits": state.get("block_registry_hits", {}),
            "schema_fingerprint": state.get("_schema_fingerprint"),
        }
        return record

    def save(
        self,
        state: "PipelineState",
        status: str,
        error: str | None = None,
        start_time: float | None = None,
    ) -> Path | None:
        """Write run log atomically. Returns path on success, None on failure."""
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            record = self._extract_record(state, status, error, start_time)

            ts_compact = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            run_id_short = record["run_id"].replace("-", "")[:8]
            filename = f"run_{ts_compact}_{run_id_short}.json"
            target = self.log_dir / filename
            tmp = target.with_suffix(".tmp")

            tmp.write_text(json.dumps(record, indent=2, default=str), encoding="utf-8")
            tmp.rename(target)
            logger.info(f"Run log written: {target}")
            return target
        except Exception as exc:
            logger.warning(f"RunLogWriter.save failed: {exc}")
            return None
