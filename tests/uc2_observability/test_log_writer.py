"""Tests for RunLogWriter."""

from __future__ import annotations

import json
import stat
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.uc2_observability.log_writer import RunLogWriter


def _full_state() -> dict:
    source_df = MagicMock()
    source_df.__len__ = lambda self: 1000
    working_df = MagicMock()
    working_df.__len__ = lambda self: 987
    quarantined_df = MagicMock()
    quarantined_df.__len__ = lambda self: 13

    return {
        "source_path": "data/usda_fooddata_sample.csv",
        "domain": "nutrition",
        "source_df": source_df,
        "working_df": working_df,
        "quarantined_df": quarantined_df,
        "dq_score_pre": 0.82,
        "dq_score_post": 0.91,
        "enrichment_stats": {"deterministic": 200, "embedding": 50, "llm": 30, "unresolved": 20},
        "block_sequence": ["column_mapping", "dq_score_pre", "normalize_text"],
        "audit_log": [{"block": "column_mapping", "rows_in": 1000, "rows_out": 1000}],
        "column_mapping": {"fdc_id": "product_id"},
        "operations": [{"type": "RENAME", "source": "fdc_id", "target": "product_id"}],
        "critique_notes": [],
        "quarantine_reasons": [{"row_idx": 42, "missing_fields": ["product_name"], "reason": "null"}],
        "block_registry_hits": {"allergens": "extract_allergens"},
        "_schema_fingerprint": "abc123",
    }


def _minimal_state() -> dict:
    return {"source_path": "data/usda_fooddata_sample.csv"}


class TestExtractRecord:
    def test_full_state(self) -> None:
        writer = RunLogWriter()
        record = writer._extract_record(_full_state(), "success", start_time=time.monotonic() - 1.5)

        assert record["status"] == "success"
        assert record["source_path"] == "data/usda_fooddata_sample.csv"
        assert record["source_name"] == "usda_fooddata_sample"
        assert record["domain"] == "nutrition"
        assert record["rows_in"] == 1000
        assert record["rows_out"] == 987
        assert record["rows_quarantined"] == 13
        assert record["dq_score_pre"] == pytest.approx(0.82)
        assert record["dq_score_post"] == pytest.approx(0.91)
        assert record["dq_delta"] == pytest.approx(0.09, abs=1e-3)
        assert record["duration_seconds"] is not None
        assert record["duration_seconds"] > 0
        assert record["run_id"] is not None
        assert record["timestamp"] is not None
        assert record["error"] is None

    def test_minimal_state(self) -> None:
        writer = RunLogWriter()
        record = writer._extract_record(_minimal_state(), "failed", error="load error")

        assert record["source_path"] == "data/usda_fooddata_sample.csv"
        assert record["source_name"] == "usda_fooddata_sample"
        assert record["status"] == "failed"
        assert record["error"] == "load error"
        assert record["rows_in"] is None
        assert record["rows_out"] is None
        assert record["dq_score_pre"] is None
        assert record["dq_delta"] is None
        assert record["run_id"] is not None


class TestSave:
    def test_writes_valid_json(self, tmp_path: Path) -> None:
        writer = RunLogWriter(log_dir=tmp_path)
        result = writer.save(_full_state(), "success", start_time=time.monotonic() - 2.0)

        assert result is not None
        assert result.exists()
        data = json.loads(result.read_text())
        assert data["status"] == "success"
        assert "run_id" in data
        assert "timestamp" in data
        assert data["source_path"] == "data/usda_fooddata_sample.csv"

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="chmod read-only is not enforced on Windows NTFS; cannot simulate unwritable dir cross-platform",
    )
    def test_returns_none_on_unwritable_dir(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        ro_dir = tmp_path / "readonly"
        ro_dir.mkdir()
        ro_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)  # no write
        writer = RunLogWriter(log_dir=ro_dir / "logs")
        import logging
        with caplog.at_level(logging.WARNING):
            result = writer.save(_minimal_state(), "failed", error="oops")
        assert result is None
        assert any("RunLogWriter" in m or "save" in m.lower() for m in caplog.messages)
