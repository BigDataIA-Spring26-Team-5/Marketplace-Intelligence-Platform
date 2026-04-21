"""Tests for RunLogStore."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.uc2_observability.log_store import RunLogStore


def _write_log(log_dir: Path, run_id: str, timestamp: str, source_name: str = "usda",
               status: str = "success", dq_delta: float | None = 0.1,
               duration: float | None = 30.0) -> Path:
    data = {
        "run_id": run_id,
        "timestamp": timestamp,
        "source_name": source_name,
        "status": status,
        "dq_delta": dq_delta,
        "duration_seconds": duration,
    }
    p = log_dir / f"run_{run_id[:8]}.json"
    p.write_text(json.dumps(data))
    return p


class TestLoadAll:
    def test_empty_dir(self, tmp_path: Path) -> None:
        store = RunLogStore(log_dir=tmp_path)
        assert store.load_all() == []

    def test_missing_dir(self, tmp_path: Path) -> None:
        store = RunLogStore(log_dir=tmp_path / "nonexistent")
        assert store.load_all() == []

    def test_single_file(self, tmp_path: Path) -> None:
        _write_log(tmp_path, "aaaa-0001", "2026-04-21T10:00:00+00:00")
        store = RunLogStore(log_dir=tmp_path)
        logs = store.load_all()
        assert len(logs) == 1
        assert logs[0]["run_id"] == "aaaa-0001"

    def test_three_files_sorted_asc(self, tmp_path: Path) -> None:
        _write_log(tmp_path, "ccc3", "2026-04-21T12:00:00")
        _write_log(tmp_path, "aaa1", "2026-04-21T10:00:00")
        _write_log(tmp_path, "bbb2", "2026-04-21T11:00:00")
        store = RunLogStore(log_dir=tmp_path)
        logs = store.load_all()
        assert len(logs) == 3
        assert logs[0]["run_id"] == "aaa1"
        assert logs[1]["run_id"] == "bbb2"
        assert logs[2]["run_id"] == "ccc3"

    def test_skips_corrupt_json(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _write_log(tmp_path, "good1", "2026-04-21T10:00:00")
        (tmp_path / "corrupt.json").write_text("NOT JSON {{{")
        import logging
        store = RunLogStore(log_dir=tmp_path)
        with caplog.at_level(logging.WARNING):
            logs = store.load_all()
        assert len(logs) == 1
        assert logs[0]["run_id"] == "good1"


class TestFilter:
    def _setup(self, tmp_path: Path) -> RunLogStore:
        _write_log(tmp_path, "r001", "2026-04-20T10:00:00", source_name="usda", status="success")
        _write_log(tmp_path, "r002", "2026-04-21T10:00:00", source_name="fda", status="partial")
        _write_log(tmp_path, "r003", "2026-04-22T10:00:00", source_name="usda", status="failed")
        return RunLogStore(log_dir=tmp_path)

    def test_filter_by_source(self, tmp_path: Path) -> None:
        store = self._setup(tmp_path)
        results = store.filter(source_name="usda")
        assert len(results) == 2
        assert all(r["source_name"] == "usda" for r in results)

    def test_filter_by_status(self, tmp_path: Path) -> None:
        store = self._setup(tmp_path)
        results = store.filter(status="partial")
        assert len(results) == 1
        assert results[0]["run_id"] == "r002"

    def test_filter_by_since(self, tmp_path: Path) -> None:
        store = self._setup(tmp_path)
        since = datetime(2026, 4, 21, tzinfo=timezone.utc)
        results = store.filter(since=since)
        assert len(results) == 2
        ids = {r["run_id"] for r in results}
        assert "r001" not in ids

    def test_filter_with_limit(self, tmp_path: Path) -> None:
        store = self._setup(tmp_path)
        results = store.filter(limit=1)
        assert len(results) == 1

    def test_filter_sorted_desc(self, tmp_path: Path) -> None:
        store = self._setup(tmp_path)
        results = store.filter()
        assert results[0]["timestamp"] >= results[-1]["timestamp"]


class TestSummaryStats:
    def test_empty(self, tmp_path: Path) -> None:
        store = RunLogStore(log_dir=tmp_path)
        stats = store.summary_stats()
        assert stats["total_runs"] == 0
        assert stats["avg_dq_delta"] is None
        assert stats["avg_duration_seconds"] is None
        assert stats["sources_seen"] == []

    def test_with_fixture_data(self, tmp_path: Path) -> None:
        _write_log(tmp_path, "r001", "2026-04-20T10:00:00", source_name="usda", status="success",
                   dq_delta=0.1, duration=30.0)
        _write_log(tmp_path, "r002", "2026-04-21T10:00:00", source_name="fda", status="partial",
                   dq_delta=0.2, duration=50.0)
        _write_log(tmp_path, "r003", "2026-04-22T10:00:00", source_name="usda", status="failed",
                   dq_delta=None, duration=None)
        store = RunLogStore(log_dir=tmp_path)
        stats = store.summary_stats()
        assert stats["total_runs"] == 3
        assert stats["success_count"] == 1
        assert stats["partial_count"] == 1
        assert stats["failed_count"] == 1
        assert stats["avg_dq_delta"] == pytest.approx(0.15)
        assert stats["avg_duration_seconds"] == pytest.approx(40.0)
        assert set(stats["sources_seen"]) == {"usda", "fda"}
