"""Unit tests for CheckpointManager."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.checkpoint.manager import (
    CheckpointManager,
    _compute_file_sha256,
    _get_schema_version,
    _load_config,
)


@pytest.fixture
def source_file(tmp_path: Path) -> Path:
    p = tmp_path / "src.csv"
    p.write_text("col_a,col_b\n1,2\n3,4\n")
    return p


@pytest.fixture
def mgr(tmp_path: Path) -> CheckpointManager:
    return CheckpointManager(checkpoint_dir=tmp_path / "ckpt")


class TestHelpers:
    def test_load_config_defaults_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = _load_config()
        assert "checkpoint_schema_version" in cfg

    def test_get_schema_version_returns_int(self):
        v = _get_schema_version()
        assert isinstance(v, int)

    def test_compute_file_sha256_deterministic(self, source_file):
        h1 = _compute_file_sha256(source_file)
        h2 = _compute_file_sha256(source_file)
        assert h1 == h2
        assert len(h1) == 64


class TestCheckpointCRUD:
    def test_init_creates_dir_and_db(self, tmp_path):
        d = tmp_path / "x"
        mgr = CheckpointManager(checkpoint_dir=d)
        assert d.exists()
        assert mgr.db_path.exists()

    def test_create_returns_run_id(self, mgr, source_file):
        rid = mgr.create(source_file, ["b1", "b2"], {})
        assert isinstance(rid, str)
        assert len(rid) > 0

    def test_load_checkpoint_missing_returns_none(self, mgr):
        assert mgr.load_checkpoint("no-such-run") is None

    def test_load_checkpoint_roundtrip(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        loaded = mgr.load_checkpoint(rid)
        assert loaded is not None
        assert loaded["run_id"] == rid
        assert loaded["chunks"] == []
        assert loaded["plan"] is None
        assert loaded["corpus"] is None

    def test_save_checkpoint_persists_chunk_and_plan(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.save_checkpoint(
            run_id=rid,
            chunk_index=0,
            chunk_data={"record_count": 5, "dq_score_pre": 0.1, "dq_score_post": 0.9},
            plan_yaml="block_sequence: [a, b]",
        )
        loaded = mgr.load_checkpoint(rid)
        assert len(loaded["chunks"]) == 1
        assert loaded["chunks"][0]["record_count"] == 5
        assert loaded["plan"] is not None
        assert "block_sequence" in loaded["plan"]["plan_yaml"]

    def test_save_checkpoint_invalid_run_id_raises(self, mgr):
        with pytest.raises(ValueError):
            mgr.save_checkpoint(
                run_id="nope",
                chunk_index=0,
                chunk_data={},
                plan_yaml="",
            )

    def test_save_checkpoint_plan_not_duplicated(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.save_checkpoint(rid, 0, {"record_count": 1}, "plan-a")
        mgr.save_checkpoint(rid, 1, {"record_count": 2}, "plan-b")
        loaded = mgr.load_checkpoint(rid)
        assert loaded["plan"]["plan_yaml"] == "plan-a"  # first wins

    def test_save_checkpoint_with_corpus_path(self, mgr, source_file, tmp_path):
        rid = mgr.create(source_file, [], {})
        idx = tmp_path / "idx.bin"
        idx.write_bytes(b"fake-index")
        meta = tmp_path / "meta.json"
        meta.write_text("{}")
        with patch("faiss.read_index") as m:
            m.return_value = MagicMock(ntotal=42)
            mgr.save_checkpoint(rid, 0, {}, "plan", idx, meta)
        loaded = mgr.load_checkpoint(rid)
        assert loaded["corpus"]["vector_count"] == 42

    def test_save_checkpoint_corpus_read_failure_sets_zero(self, mgr, source_file, tmp_path):
        rid = mgr.create(source_file, [], {})
        idx = tmp_path / "idx.bin"
        idx.write_bytes(b"bogus")
        with patch("faiss.read_index", side_effect=RuntimeError("bad")):
            mgr.save_checkpoint(rid, 0, {}, "plan", idx, None)
        loaded = mgr.load_checkpoint(rid)
        assert loaded["corpus"]["vector_count"] == 0


class TestResumeAndValidation:
    def test_get_resume_state_empty(self, mgr):
        assert mgr.get_resume_state() is None

    def test_get_resume_state_marks_resume(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        state = mgr.get_resume_state()
        assert state is not None
        assert state["run_id"] == rid

    def test_validate_no_checkpoint_is_true(self, mgr, source_file):
        ok, msg = mgr.validate_checkpoint(source_file)
        assert ok is True

    def test_validate_matching_sha256(self, mgr, source_file):
        mgr.create(source_file, [], {})
        ok, msg = mgr.validate_checkpoint(source_file)
        assert ok is True

    def test_validate_changed_source(self, mgr, source_file, tmp_path):
        mgr.create(source_file, [], {})
        other = tmp_path / "other.csv"
        other.write_text("different,data\n9,9\n")
        ok, msg = mgr.validate_checkpoint(other)
        assert ok is False
        assert "changed" in msg.lower()

    def test_validate_schema_mismatch(self, mgr, source_file):
        mgr.create(source_file, [], {})
        with patch("src.pipeline.checkpoint.manager._get_schema_version", return_value=999):
            ok, msg = mgr.validate_checkpoint(source_file)
        assert ok is False
        assert "schema" in msg.lower()


class TestClearAndFresh:
    def test_force_fresh_clears_all(self, mgr, source_file):
        mgr.create(source_file, [], {})
        mgr.force_fresh()
        assert mgr.get_resume_state() is None

    def test_clear_checkpoint_specific(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.clear_checkpoint(rid)
        assert mgr.load_checkpoint(rid) is None

    def test_clear_checkpoint_all(self, mgr, source_file):
        mgr.create(source_file, [], {})
        mgr.clear_checkpoint(None)
        assert mgr.get_resume_state() is None


class TestChunkStages:
    def test_save_chunk_stage_insert_then_update(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.save_chunk_stage(rid, 0, "in_progress", {"record_count": 3})
        mgr.save_chunk_stage(rid, 0, "completed", {"dq_score_post": 0.8})
        loaded = mgr.load_checkpoint(rid)
        assert len(loaded["chunks"]) == 1
        assert loaded["chunks"][0]["status"] == "completed"

    def test_save_chunk_stage_invalid_raises(self, mgr):
        with pytest.raises(ValueError):
            mgr.save_chunk_stage("missing", 0, "transform", None)

    def test_save_chunk_stage_state_none(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.save_chunk_stage(rid, 0, "in_progress", None)
        loaded = mgr.load_checkpoint(rid)
        assert loaded["chunks"][0]["record_count"] == 0

    def test_get_chunk_resume_index_no_ckpt(self, mgr):
        assert mgr.get_chunk_resume_index("nope") == 0

    def test_get_chunk_resume_index_after_completed(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        mgr.save_checkpoint(rid, 0, {"record_count": 1}, "p")
        mgr.save_checkpoint(rid, 1, {"record_count": 1}, "p")
        assert mgr.get_chunk_resume_index(rid) == 2

    def test_get_chunk_resume_index_no_completed(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        assert mgr.get_chunk_resume_index(rid) == 0

    def test_get_latest_run_id_empty(self, mgr):
        assert mgr.get_latest_run_id() is None

    def test_get_latest_run_id(self, mgr, source_file):
        rid = mgr.create(source_file, [], {})
        assert mgr.get_latest_run_id() == rid
