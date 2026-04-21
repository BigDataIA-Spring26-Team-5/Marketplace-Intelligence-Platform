"""RunLogStore: queryable read-only access to persisted run logs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class RunLogStore:
    def __init__(self, log_dir: Path = PROJECT_ROOT / "output" / "run_logs"):
        self.log_dir = Path(log_dir)

    def load_all(self) -> list[dict]:
        """All run logs sorted by timestamp ASC. Skips corrupt files."""
        if not self.log_dir.exists():
            return []
        logs: list[dict] = []
        for path in self.log_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                logs.append(data)
            except Exception as exc:
                logger.warning(f"Skipping corrupt log {path.name}: {exc}")
        logs.sort(key=lambda r: r.get("timestamp", ""))
        return logs

    def get_by_run_id(self, run_id: str) -> dict | None:
        for log in self.load_all():
            if log.get("run_id") == run_id:
                return log
        return None

    def filter(
        self,
        source_name: str | None = None,
        status: str | None = None,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """AND-filter logs, return sorted timestamp DESC."""
        results = self.load_all()
        if source_name is not None:
            results = [r for r in results if r.get("source_name") == source_name]
        if status is not None:
            results = [r for r in results if r.get("status") == status]
        if since is not None:
            since_iso = since.isoformat() if isinstance(since, datetime) else str(since)
            results = [r for r in results if r.get("timestamp", "") >= since_iso]
        results.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
        if limit is not None:
            results = results[:limit]
        return results

    def summary_stats(self) -> dict:
        logs = self.load_all()
        total = len(logs)
        success = sum(1 for r in logs if r.get("status") == "success")
        partial = sum(1 for r in logs if r.get("status") == "partial")
        failed = sum(1 for r in logs if r.get("status") == "failed")

        deltas = [r["dq_delta"] for r in logs if r.get("dq_delta") is not None]
        avg_dq_delta = round(sum(deltas) / len(deltas), 4) if deltas else None

        durations = [r["duration_seconds"] for r in logs if r.get("duration_seconds") is not None]
        avg_duration = round(sum(durations) / len(durations), 3) if durations else None

        sources = sorted({r["source_name"] for r in logs if r.get("source_name")})

        return {
            "total_runs": total,
            "success_count": success,
            "partial_count": partial,
            "failed_count": failed,
            "avg_dq_delta": avg_dq_delta,
            "avg_duration_seconds": avg_duration,
            "sources_seen": sources,
        }
